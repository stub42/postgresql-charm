# Copyright 2011-2015 Canonical Ltd.
#
# This file is part of the PostgreSQL Charm for Juju.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 3, as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranties of
# MERCHANTABILITY, SATISFACTORY QUALITY, or FITNESS FOR A PARTICULAR
# PURPOSE.  See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
import math
import os.path
import re
import subprocess
import time

import yaml

from charmhelpers import context
from charmhelpers.core import hookenv, host, sysctl, templating, unitdata
from charmhelpers.core.hookenv import DEBUG, WARNING
from charms import reactive
from charms.reactive import not_unless, only_once, when, when_not
from charms.reactive.decorators import when_file_changed

from reactive import apt
from reactive import coordinator
from reactive.workloadstatus import status_set

from reactive.postgresql import helpers
from reactive.postgresql import postgresql
from reactive.postgresql import replication
from reactive.postgresql import wal_e

from everyhook import everyhook


@everyhook
def main():
    generate_locale()
    configure_sources()

    # Don't trust this state from the last hook. Daemons may have
    # crashed and servers rebooted since then.
    if reactive.is_state('postgresql.cluster.created'):
        reactive.toggle_state('postgresql.cluster.is_running',
                              postgresql.is_running())

    # Reconfigure PostgreSQL.
    reactive.remove_state('postgresql.cluster.configured')


def emit_deprecated_option_warnings():
    deprecated = sorted(helpers.deprecated_config_in_use())
    if deprecated:
        hookenv.log('Deprecated configuration settings in use: {}'
                    ', '.join(deprecated), WARNING)


# emit_deprecated_option_warnings is called at the end of the hook
# so that the warnings to appear clearly at the end of the logs.
hookenv.atexit(emit_deprecated_option_warnings)


@only_once
def generate_locale():
    '''Ensure that the requested database locale is available.

    The locale cannot be changed post deployment, as this would involve
    completely destroying and recreding the database.
    '''
    config = hookenv.config()
    if config['locale'] != 'C':
        status_set('maintenance',
                   'Generating {} locale'.format(config['locale']))
        subprocess.check_call(['locale-gen',
                               '{}.{}'.format(hookenv.config('locale'),
                                              hookenv.config('encoding'))],
                              universal_newlines=True)


def configure_sources():
    '''Add the apt sources necessary for the configuration options selected.'''
    config = hookenv.config()

    # Shortcut for the PGDG archive.
    if config['pgdg'] and config.changed('pgdg'):
        pgdg_url = 'http://apt.postgresql.org/pub/repos/apt/'
        pgdg_src = 'deb {} {}-pgdg main'.format(pgdg_url,
                                                helpers.distro_codename())
        pgdg_key_path = os.path.join(hookenv.charm_dir(), 'lib', 'pgdg.key')
        with open(pgdg_key_path, 'r') as f:
            hookenv.log('Adding PGDG archive')
            apt.add_source(pgdg_src, f.read())


@when_not('apt.installed.postgresql-common')
@when_not('postgresql.cluster.inhibited')
def inhibit_default_cluster_creation():
    '''Stop the PostgreSQL packages from creating the default cluster.

    We can't use the default cluster as it is likely created with an
    incorrect locale and without options such as data checksumming.
    '''
    path = postgresql.postgresql_conf_path()
    if os.path.exists(path):
        status_set('blocked', 'postgresql.conf already exists')
    else:
        hookenv.log('Inhibiting')
        os.makedirs(os.path.dirname(path), mode=0o755, exist_ok=True)
        with open(path, 'w') as f:
            f.write('# Inhibited')
        reactive.set_state('postgresql.cluster.inhibited')
        hookenv.log('Inhibited == {}'
                    .format(reactive.is_state('postgresql.cluster.inhibited')))


@when('apt.installed.postgresql-common', 'postgresql.cluster.inhibited')
def uninhibit_default_cluster_creation():
    '''Undo inhibit_default_cluster_creation() so manual creation works.'''
    hookenv.log('Removing inhibitions')
    path = postgresql.postgresql_conf_path()
    with open(path, 'r') as f:
        assert f.read() == '# Inhibited', 'Default cluster inhibition failed'
    os.unlink(postgresql.postgresql_conf_path())
    reactive.remove_state('postgresql.cluster.inhibited')


@when('postgresql.cluster.inhibited')
def install_postgresql_packages():
    hookenv.log('Inhibited == {}'
                .format(reactive.is_state('postgresql.cluster.inhibited')))
    apt.queue_install(postgresql.packages())
    install_extra_packages()


@when('apt.installed.postgresql-common')
def install_extra_packages():
    config = hookenv.config()
    packages = set(['rsync'])
    packages.update(set(config['extra_packages'].split()))
    packages.update(set(config['extra-packages'].split()))  # Deprecated.
    apt.queue_install(packages)


@when('apt.installed.postgresql-common')
@only_once
def update_kernel_settings():
    lots_and_lots = pow(1024, 4)  # 1 TB
    sysctl_settings = {'kernel.shmmax': lots_and_lots,
                       'kernel.shmall': lots_and_lots}
    sysctl.create(yaml.dump(sysctl_settings),
                  '/etc/sysctl.d/50-postgresql.conf')


@when('apt.installed.postgresql-common')
@when_not('postgresql.cluster.inhibited')
@when_not('postgresql.cluster.created')
def create_cluster():
    '''Sets the postgresql.cluster.created state.'''
    assert not os.path.exists(postgresql.postgresql_conf_path()), \
        'inhibit_default_cluster_creation() failed'
    assert not os.path.exists(postgresql.data_dir())
    postgresql.create_cluster()
    reactive.set_state('postgresql.cluster.created')


@when_not('leadership.is_leader')
@when_not('leadership.set.nagios_password')
@when_not('workloadstatus.blocked')
def wait_for_leader():
    # We might be running a hook before the leader's install or upgrade-charm
    # hook, so required leadership settings may not be set yet. This state
    # will be hit for example when we upgrade a charm from a release
    # before the nagios password became a leadership setting.
    status_set('waiting', 'Waiting for leader to lead')


@when('postgresql.cluster.created')
@when('postgresql.replication.has_master')
@when_not('postgresql.cluster.configured')
def configure_cluster():
    '''Configure the cluster.'''
    update_pg_ident_conf()
    update_pg_hba_conf()
    try:
        update_postgresql_conf()
        reactive.set_state('postgresql.cluster.configured')
    except InvalidPgConfSetting as x:
        status_set('blocked',
                   'Invalid postgresql.conf setting {}: {}'.format(*x.args))


def update_pg_ident_conf():
    '''Add the charm's required entry to pg_ident.conf'''
    entries = set([('root', 'postgres'),
                   ('postgres', 'postgres')])
    path = postgresql.pg_ident_conf_path()
    with open(path, 'r') as f:
        current_pg_ident = f.read()
    for sysuser, pguser in entries:
        if re.search(r'^\s*juju_charm\s+{}\s+{}\s*$'.format(sysuser, pguser),
                     current_pg_ident, re.M) is None:
            with open(path, 'a') as f:
                f.write('\njuju_charm {} {}'.format(sysuser, pguser))


def update_pg_hba_conf():

    # grab the needed current state
    config = hookenv.config()
    rels = context.Relations()
    path = postgresql.pg_hba_conf_path()
    with open(path, 'r') as f:
        pg_hba = f.read()

    # generate the new state
    pg_hba_content = generate_pg_hba_conf(pg_hba, config, rels)

    # write out the new state
    helpers.rewrite(path, pg_hba_content)


def generate_pg_hba_conf(pg_hba, config, rels):
    '''Update the pg_hba.conf file (host based authentication).'''
    rules = []  # The ordered list, as tuples.

    # local      database  user  auth-method  [auth-options]
    # host       database  user  address  auth-method  [auth-options]
    # hostssl    database  user  address  auth-method  [auth-options]
    # hostnossl  database  user  address  auth-method  [auth-options]
    # host       database  user  IP-address  IP-mask  auth-method  [auth-opts]
    # hostssl    database  user  IP-address  IP-mask  auth-method  [auth-opts]
    # hostnossl  database  user  IP-address  IP-mask  auth-method  [auth-opts]
    def add(*record):
        rules.append(tuple(record))

    # The charm is running as the root user, and needs to be able to
    # connect as the postgres user to all databases.
    add('local', 'all', 'postgres', 'peer', 'map=juju_charm')

    # The local unit needs access to its own database. Let every local
    # user connect to their matching PostgreSQL user, if it exists, and
    # nagios with a password.
    add('local', 'all', nagios.nagios_username(), 'password')
    add('local', 'all', 'all', 'peer')

    # Peers need replication access as the charm replication user.
    if rels.peer:
        for peer, relinfo in rels.peer.items():
            addr = postgresql.addr_to_range(relinfo['private-address'])
            qaddr = postgresql.quote_identifier(addr)
            # Magic replication database, for replication.
            add('host', 'replication', replication.replication_username(),
                qaddr, 'md5', '# {}'.format(relinfo))
            # postgres database, so the leader can query replication status.
            add('host', 'postgres', replication.replication_username(),
                qaddr, 'md5', '# {}'.format(relinfo))

    # Clients need access to the relation database as the relation users.
    for rel in rels['db'].values():
        if 'user' in rel.local:
            for relinfo in rel.values():
                addr = postgresql.addr_to_range(relinfo['private-address'])
                # Quote everything, including the address, to disenchant
                # magic tokens like 'all'.
                add('host',
                    postgresql.quote_identifier(rel.local['database']),
                    postgresql.quote_identifier(rel.local['user']),
                    postgresql.quote_identifier(addr),
                    'md5', '# {}'.format(relinfo))
                add('host',
                    postgresql.quote_identifier(rel.local['database']),
                    postgresql.quote_identifier(rel.local['schema_user']),
                    postgresql.quote_identifier(addr),
                    'md5', '# {}'.format(relinfo))

    # Admin clients need access to all databases as any user, not just the
    # relation user. Most clients will just use the user provided them,
    # but proxies such as pgbouncer need to open connections as the accounts
    # it creates.
    for rel in rels['db-admin'].values():
        if 'user' in rel.local:
            for relinfo in rel.values():
                addr = postgresql.addr_to_range(relinfo['private-address'])
                add('host', 'all', 'all',
                    postgresql.quote_identifier(addr),
                    'md5', '# {}'.format(relinfo))

    # External replication connections. Somewhat different than before
    # as the relation gets its own user to avoid sharing credentials,
    # and logical replication connections will want to specify the
    # database name.
    for rel in rels['master'].values():
        for relinfo in rel.values():
            addr = postgresql.addr_to_range(relinfo['private-address'])
            add('host', 'replication',
                postgresql.quote_identifier(rel.local['user']),
                postgresql.quote_identifier(addr),
                'md5', '# {}'.format(relinfo))
            if 'database' in rel.local:
                add('host',
                    postgresql.quote_identifier(rel.local['database']),
                    postgresql.quote_identifier(rel.local['user']),
                    postgresql.quote_identifier(addr),
                    'md5', '# {}'.format(relinfo))

    # External administrative addresses, if specified by the operator.
    for addr in config['admin_addresses'].split(','):
        if addr:
            add('host', 'all', 'all',
                postgresql.quote_identifier(postgresql.addr_to_range(addr)),
                'md5', '# admin_addresses config')

    # And anything-goes rules, if specified by the operator.
    for line in config['extra_pg_auth'].split(','):
        add(line + '# extra_pg_auth config')

    # Deny everything else
    add('local', 'all', 'all', 'reject', '# Refuse by default')
    add('host', 'all', 'all', 'all', 'reject', '# Refuse by default')

    # Strip out the existing juju managed section
    start_mark = '### BEGIN JUJU SETTINGS ###'
    end_mark = '### END JUJU SETTINGS ###'
    pg_hba = re.sub(r'^\s*{}.*^\s*{}\s*$'.format(re.escape(start_mark),
                                                 re.escape(end_mark)),
                    '', pg_hba, flags=re.I | re.M | re.DOTALL)

    # Comment out any uncommented lines
    pg_hba = re.sub(r'^\s*([^#\s].*)$', r'# juju # \1', pg_hba, flags=re.M)

    # Spit out the updated file
    rules.insert(0, (start_mark,))
    rules.append((end_mark,))
    pg_hba += '\n' + '\n'.join(' '.join(rule) for rule in rules)
    return pg_hba


@when_file_changed(postgresql.pg_ident_conf_path(),
                   postgresql.pg_hba_conf_path())
@when('postgresql.cluster.is_running')
def reload_on_auth_change():
    reactive.set_state('postgresql.cluster.needs_reload')


@when('postgresql.cluster.needs_reload')
@when_not('postgresql.cluster.needs_restart')
@not_unless('postgresql.cluster.is_running')
def reload_config():
    postgresql.reload_config()
    reactive.remove_state('postgresql.cluster.needs_reload')


@when('postgresql.cluster.needs_restart')
def request_restart():
    coordinator.acquire('restart')


@when('postgresql.cluster.needs_restart')
@when('coordinator.granted.restart')
@when('postgresql.cluster.is_running')
def stop():
    status_set('maintenance', 'Stopping PostgreSQL')
    postgresql.stop()
    reactive.remove_state('postgresql.cluster.is_running')
    reactive.remove_state('postgresql.cluster.needs_reload')


@when_not('postgresql.cluster.is_running')
@when('postgresql.cluster.configured')
@when('postgresql.replication.has_master')
@when('postgresql.replication.cloned')
def start():
    status_set('maintenance', 'Starting PostgreSQL')
    postgresql.start()

    while postgresql.is_primary() and postgresql.is_in_recovery():
        status_set('maintenance', 'Startup recovery')
        time.sleep(1)

    store = unitdata.kv()

    open_ports(store.get('postgresql.cluster.pgconf.live.port'),
               store.get('postgresql.cluster.pgconf.current.port') or 5432)

    # Update the 'live' config now we know it is in effect. This
    # is used to detect future config changes that require a restart.
    settings = store.getrange('postgresql.cluster.pgconf.current.')
    store.unsetrange(prefix='postgresql.cluster.pgconf.live.')
    store.update(settings, prefix='postgresql.cluster.pgconf.live.')

    reactive.set_state('postgresql.cluster.is_running')
    reactive.remove_state('postgresql.cluster.needs_restart')
    reactive.remove_state('postgresql.cluster.needs_reload')


def assemble_postgresql_conf():
    '''Assemble postgresql.conf settings and return them as a dictionary.'''
    conf = {}

    # Start with charm defaults.
    conf.update(postgresql_conf_defaults())

    # User overrides from deprecated service config.
    conf.update(postgresql_conf_deprecated_overrides())

    # User overrides from service config.
    conf.update(postgresql_conf_overrides())

    # Ensure minimal settings so the charm can actually work.
    ensure_viable_postgresql_conf(conf)

    # Strip out invalid config and warn.
    validate_postgresql_conf(conf)  # May terminate.

    return conf


def postgresql_conf_defaults():
    '''Return the postgresql.conf defaults, which we parse from config.yaml'''
    # We load defaults from the extra_pg_conf default in config.yaml,
    # which ensures that they never get out of sync.
    raw = helpers.config_yaml()['options']['extra_pg_conf']['default']
    defaults = postgresql.parse_config(raw)

    # And calculate some defaults, which could get out of sync.
    # Settings with mandatory minimums like wal_senders is handled
    # later, in ensure_viable_postgresql_conf().
    ram = int(host.get_total_ram() / (1024 * 1024))  # Working in megabytes.

    # Default shared_buffers to 25% of ram, minimum 16MB, maximum 8GB,
    # per current best practice rules of thumb. Rest is cache.
    shared_buffers = max(min(math.ceil(ram * 0.25), 8192), 16)
    effective_cache_size = max(1, ram - shared_buffers)
    defaults['shared_buffers'] = '{} MB'.format(shared_buffers)
    defaults['effective_cache_size'] = '{} MB'.format(effective_cache_size)

    return defaults


def postgresql_conf_overrides():
    '''User postgresql.conf overrides, from service configuration.'''
    config = hookenv.config()
    return postgresql.parse_config(config['extra_pg_conf'])


def postgresql_conf_deprecated_overrides():
    '''Overrides from deprecated service configuration options.

    There are far too many knobs in postgresql.conf for the charm
    to duplicate each one in config.yaml, and they can change between
    versions. The old options that did this have all been deprecated,
    and users can specify them in the extra_pg_conf option.

    One day this method and the deprecated options will go away.
    '''
    config = hookenv.config()

    # These deprecated options mapped directly to postgresql.conf settings.
    # As you can see, it was unmaintainably long.
    simple_options = frozenset(['max_connections', 'max_prepared_transactions',
                                'ssl', 'log_min_duration_statement',
                                'log_checkpoints', 'log_connections',
                                'log_disconnections', 'log_temp_files',
                                'log_line_prefix', 'log_lock_waits',
                                'log_timezone', 'log_autovacuum_min_duration',
                                'autovacuum', 'autovacuum_analyze_threshold',
                                'autovacuum_vacuum_scale_factor',
                                'autovacuum_analyze_scale_factor',
                                'autovacuum_vacuum_cost_delay', 'search_path',
                                'standard_conforming_strings', 'hot_standby',
                                'hot_standby_feedback', 'wal_level',
                                'max_wal_senders', 'wal_keep_segments',
                                'archive_mode', 'archive_command',
                                'work_mem', 'maintenance_work_mem',
                                'shared_buffers', 'effective_cache_size',
                                'default_statistics_target', 'temp_buffers',
                                'wal_buffers', 'checkpoint_segments',
                                'checkpoint_completion_target',
                                'checkpoint_timeout', 'fsync',
                                'synchronous_commit', 'full_page_writes',
                                'random_page_cost'])

    in_use = helpers.deprecated_config_in_use()

    # The simple deprecated options map directly to postgresql.conf settings.
    settings = {k: config[k] for k in in_use if k in simple_options}

    # The listen_port and collapse_limit options were special.
    config_yaml_options = helpers.config_yaml()['options']
    defaults = {k: config_yaml_options[k]['default']
                for k in config_yaml_options}
    if config['listen_port'] not in (-1, defaults['listen_port']):
        settings['port'] = config['listen_port']
    if config['collapse_limit'] != defaults['collapse_limit']:
        settings['from_collapse_limit'] = config['collapse_limit']
        settings['join_collapse_limit'] = config['collapse_limit']

    return settings


def ensure_viable_postgresql_conf(opts):
    def force(**kw):
        for k, v in kw.items():
            if opts.get(k) != v:
                hookenv.log('Setting {} to {}'.format(k, v), DEBUG)
                opts[k] = v

    config = hookenv.config()
    rels = context.Relations()

    # Number of standby units - count peers and 'master' relations.
    num_standbys = len(rels.peer or {})
    for rel in rels['master'].values():
        num_standbys += len(rel)

    num_clients = 0
    for rel in list(rels['db']) + list(rels['db-admin']):
        num_clients += len(rel)

    # Even without replication, replication slots get used by
    # pg_basebackup(1). Bump up max_wal_senders so things work. It is
    # cheap, so perhaps we should just pump it to several thousand.
    min_wal_senders = num_standbys * 2 + 5
    if min_wal_senders > int(opts.get('max_wal_senders', 0)):
        force(max_wal_senders=min_wal_senders)

    # max_connections. One per client unit, plus replication.
    max_wal_senders = int(opts.get('max_wal_senders', 0))
    assert max_wal_senders > 0
    min_max_connections = max_wal_senders + max(1, num_clients)
    if min_max_connections > int(opts.get('max_connections', 0)):
        force(max_connections=min_max_connections)

    # We want 'hot_standby' at a minimum, as it lets us run
    # pg_basebackup() and it is recommended over the more
    # minimal 'archive'. Is it worth only enabling the higher-still
    # 'logical' level only when necessary? How do we detect that?
    force(hot_standby=True)
    if postgresql.has_version('9.4'):
        force(wal_level='logical')
    else:
        force(wal_level='hot_standby')

    # Having two config options for the one setting is confusing. Perhaps
    # we should deprecate this.
    if num_standbys and (int(config['replicated_wal_keep_segments']) >
                         int(opts.get('wal_keep_segments', 0))):
        force(wal_keep_segments=config['replicated_wal_keep_segments'])

    # Log shipping with WAL-E.
    if config['wal_e_storage_uri']:
        force(archive_mode=True)
        force(archive_command=wal_e.wal_e_archive_command())

    # Log destinations for syslog. This charm only supports standard
    # Debian logging, or Debian + syslog. This will grow more complex in
    # the future, as the local logs are redundant if you are using syslog
    # for log aggregation, and we will want to add csvlog because it is
    # so much easier to parse.
    if context.Relations()['syslog']:
        force(log_destination='stderr,syslog',
              syslog_ident=hookenv.local_unit().replace('/', '_'))


class InvalidPgConfSetting(ValueError):
    pass


def validate_postgresql_conf(conf):
    '''Block the unit and exit the hook if there is invalid configuration.

    We do strict validation to pick up errors in the users pg_extra_conf
    setting. If we put invalid config in postgresql.conf, then config
    reloads will not take effect and restarts will fail.

    I expect this isn't bulletproof and the operator can still shoot
    themselves in the foot with string settings that PostgreSQL cannot
    parse (eg. listen_address="** invalid **").

    It seems preferable to make bad configuration highly visible and
    block, rather than repair the situation with potentially dangerous
    settings and hope the operator notices the log messages.
    '''
    schema = postgresql.pg_settings_schema()
    for k, v in list(conf.items()):
        v = str(v)
        try:
            if k not in schema:
                raise ValueError('Unknown option {}'.format(k))

            r = schema[k]

            if r.vartype == 'bool':
                if v.lower() not in postgresql.VALID_BOOLS:
                    raise ValueError('Invalid boolean {!r}'.format(v, None))

            elif r.vartype == 'enum':
                v = v.lower()
                if v not in r.enumvals:
                    raise ValueError('Must be one of {!r}'.format(r.enumvals))

            elif r.vartype == 'integer':
                if r.unit:
                    try:
                        v = postgresql.convert_unit(v, r.unit)
                    except ValueError:
                        raise ValueError('Invalid integer w/unit {!r}'
                                         ''.format(v))
                else:
                    try:
                        v = int(v)
                    except ValueError:
                        raise ValueError('Invalid integer {!r}'.format(v))

            elif r.vartype == 'real':
                try:
                    v = float(v)
                except ValueError:
                    raise ValueError('Invalid real {!r}'.format(v))

            if r.min_val and v < float(r.min_val):
                raise ValueError('{} below minimum {}'.format(v, r.min_val))
            elif r.max_val and v > float(r.max_val):
                raise ValueError('{} above maximum {}'.format(v, r.maxvalue))

        except ValueError as x:
            raise InvalidPgConfSetting(k, x)


def update_postgresql_conf():
    settings = assemble_postgresql_conf()
    path = postgresql.postgresql_conf_path()

    with open(path, 'r') as f:
        pg_conf = f.read()

    start_mark = '### BEGIN JUJU SETTINGS ###'
    end_mark = '### END JUJU SETTINGS ###'

    # Strip the existing settings section, including the markers.
    pg_conf = re.sub(r'^\s*{}.*^\s*{}\s*$'.format(re.escape(start_mark),
                                                  re.escape(end_mark)),
                     '', pg_conf, flags=re.I | re.M | re.DOTALL)

    for k in settings:
        # Comment out conflicting options. We could just allow later
        # options to override earlier ones, but this is less surprising.
        pg_conf = re.sub(r'^\s*({}[\s=].*)$'.format(re.escape(k)),
                         r'# juju # \1', pg_conf, flags=re.M | re.I)

    # Store the updated charm options. This is compared with the
    # live config to detect if a restart is required.
    store = unitdata.kv()
    current_prefix = 'postgresql.cluster.pgconf.current.'
    store.unsetrange(prefix=current_prefix)
    store.update(settings, prefix=current_prefix)

    # Generate the charm config section, adding it to the end of the
    # config file.
    simple_re = re.compile(r'^[-.\w]+$')
    override_section = [start_mark]
    for k, v in settings.items():
        v = str(v)
        assert '\n' not in v, "Invalid config value {!r}".format(v)
        if simple_re.search(v) is None:
            v = "'{}'".format(v.replace("'", "''"))
        override_section.append('{} = {}'.format(k, v))
    override_section.append(end_mark)
    pg_conf += '\n' + '\n'.join(override_section)

    helpers.rewrite(path, pg_conf)


@when('leadership.changed.replication_password')
@when('postgresql.cluster.created')
def update_pgpass():
    leader = context.Leader()
    accounts = ['root', 'postgres']
    for account in accounts:
        path = os.path.expanduser(os.path.join('~{}'.format(account),
                                               '.pgpass'))
        content = ('# Managed by Juju\n'
                   '*:*:*:{}:{}'.format(replication.replication_username(),
                                        leader.get('replication_password')))
        helpers.write(path, content, mode=0o600, user=account, group=account)


@when_file_changed(postgresql.postgresql_conf_path())
@when('postgresql.cluster.is_running')
@when_not('postgresql.cluster.needs_restart')
def postgresql_conf_changed():
    '''
    After postgresql.conf has been changed, check it to see if
    any changed options require a restart.

    Sets the postgresql.cluster.needs_restart state.
    Sets the postgresql.cluster.needs_reload state.
    '''
    store = unitdata.kv()
    live = store.getrange('postgresql.cluster.pgconf.live.', strip=True)
    current = store.getrange('postgresql.cluster.pgconf.current.', strip=True)

    if not live or not current:
        hookenv.log('PostgreSQL started without current config being saved. '
                    'Was the server rebooted unexpectedly?', WARNING)
        reactive.set_state('postgresql.cluster.needs_restart')
        return

    con = postgresql.connect()
    cur = con.cursor()
    cur.execute("SELECT name FROM pg_settings WHERE context='postmaster'")
    needs_restart = False
    for row in cur.fetchall():
        key = row[0]
        old = live.get(key)
        new = current.get(key)
        if old != new:
            hookenv.log('{} changed from {!r} to {!r}. '
                        'Restart required.'.format(key, old, new))
            needs_restart = True
    reactive.toggle_state('postgresql.cluster.needs_restart', needs_restart)
    reactive.toggle_state('postgresql.cluster.needs_reload', not needs_restart)


def maybe_restart_now():
    if not postgresql.is_running():
        if replication.needs_clone():
            # The unit needs to be cloned. Grab the restart lock
            # to ensure the unit we are cloning isn't restarted
            # during the cloning process.
            coordinator.acquire('restart')
            return
        else:
            # For all other cases there is no need to wait for
            # permission to restart a stopped service.
            return


@when('coordinator.requested.restart')
def wait_for_restart():
    if coordinator.requested('restart') and not coordinator.granted('restart'):
        if replication.needs_clone():
            msg = ('Waiting for permission to clone {}'
                   .format(replication.master()))
        else:
            msg = 'Waiting for permission to restart'
        status_set('waiting', msg)


def open_ports(old_port, new_port):
    if old_port and int(old_port) != int(new_port):
        hookenv.close_port(int(old_port))
    hookenv.open_port(int(new_port))


# def create_ssl_cert(cluster_dir):
#     # PostgreSQL expects SSL certificates in the datadir.
#     server_crt = os.path.join(cluster_dir, 'server.crt')
#     server_key = os.path.join(cluster_dir, 'server.key')
#     if not os.path.exists(server_crt):
#         os.symlink('/etc/ssl/certs/ssl-cert-snakeoil.pem',
#                    server_crt)
#     if not os.path.exists(server_key):
#         os.symlink('/etc/ssl/private/ssl-cert-snakeoil.key',
#                    server_key)


@when('postgresql.cluster.configured')
@when('postgresql.cluster.is_running')
@when('postgresql.replication.has_master')
@when_not('postgresql.cluster.needs_restart')
@when_not('postgresql.cluster.needs_reload')
def set_active():
    if postgresql.is_running():
        if replication.is_master():
            msg = 'Live master'
        elif postgresql.is_primary():
            msg = 'Live primary'
        else:
            msg = 'Live secondary'
        status_set('active', msg)
    else:
        # PostgreSQL crashed! Maybe bad configuration we failed to
        # pick up, or maybe a full disk. The admin will need to diagnose.
        status_set('blocked', 'PostgreSQL unexpectedly shut down')


@when('postgresql.cluster.created')
def install_administrative_scripts():
    scripts_dir = helpers.scripts_dir()
    logs_dir = helpers.logs_dir()
    helpers.makedirs(scripts_dir, mode=0o755)

    # The database backup script. Most of this is redundant now.
    source = os.path.join(hookenv.charm_dir(), 'scripts', 'pgbackup.py')
    destination = os.path.join(scripts_dir, 'dump-pg-db')
    with open(source, 'r') as f:
        helpers.write(destination, f.read(), mode=0o755)

    backups_dir = helpers.backups_dir()
    helpers.makedirs(backups_dir, mode=0o750,
                     user='postgres', group='postgres')

    # Generate a wrapper that invokes the backup script for each
    # database.
    data = dict(logs_dir=logs_dir,
                scripts_dir=scripts_dir,
                # backups_dir probably should be deprecated in favour of
                # a juju storage mount.
                backups_dir=backups_dir)
    destination = os.path.join(helpers.scripts_dir(), 'pg_backup_job')
    templating.render('pg_backup_job.tmpl', destination, data,
                      owner='root', group='postgres', perms=0o755)

    if not os.path.exists(logs_dir):
        helpers.makedirs(logs_dir, mode=0o755)
        # Create the backups.log file used by the backup wrapper if it
        # does not exist, in order to trigger spurious alerts when a
        # unit is installed, per Bug #1329816.
        helpers.write(helpers.backups_log_path(), '', mode=0o644)


@when('postgresql.cluster.is_running')
def update_postgresql_crontab():
    config = hookenv.config()
    data = dict(config)

    data['scripts_dir'] = helpers.scripts_dir()
    data['is_master'] = replication.is_master()
    data['is_primary'] = postgresql.is_primary()

    if config['wal_e_storage_uri']:
        data['wal_e_enabled'] = True
        data['wal_e_backup_command'] = wal_e.wal_e_backup_command()
        data['wal_e_prune_command'] = wal_e.wal_e_prune_command()
    else:
        data['wal_e_enabled'] = False

    destination = os.path.join(helpers.cron_dir(), 'juju-postgresql')
    templating.render('postgres.cron.tmpl', destination, data,
                      owner='root', group='postgres',
                      perms=0o640)


@when_not('postgresql.cluster.is_running')
def remove_postgresql_crontab():
    '''When PostgreSQL is not running, we don't want any cron jobs firing.'''
    path = os.path.join(helpers.cron_dir(), 'juju-postgresql')
    if os.path.exists(path):
        os.unlink(path)
