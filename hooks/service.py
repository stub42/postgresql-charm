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

import yaml

from charmhelpers import context
from charmhelpers.core import hookenv, host, sysctl, templating
from charmhelpers.core.hookenv import DEBUG, WARNING
from charmhelpers import fetch
from charmhelpers.payload import execd

from coordinator import coordinator
from decorators import data_ready_action, leader_only, requirement
import helpers
import postgresql
import replication
import wal_e


@requirement
def valid_config():
    """
    Sanity check charm configuration, blocking the unit if we have
    bogus bogus config values or config changes the charm does not
    yet (or cannot) support.
    """
    valid = True
    config = hookenv.config()

    enums = dict(version=set(['', '9.1', '9.2', '9.3', '9.4']),
                 package_status=set(['install', 'hold']))
    for key, vals in enums.items():
        config[key] = config[key].lower()  # Rewrite to lower case.
        if config[key] not in vals:
            valid = False
            helpers.status_set('blocked',
                               'Invalid value for {} ({!r})'
                               .format(key, config[key]))

    unchangeable_config = ['locale', 'encoding', 'version', 'pgdg',
                           'manual_replication']
    if config._prev_dict is not None:
        for name in unchangeable_config:
            if config.changed(name):
                valid = False
                helpers.status_set('blocked',
                                   'Cannot change {!r} after install '
                                   '(was {!r}).'.format(name,
                                                        config.previous(name)))
    return valid


@data_ready_action
def preinstall():
    '''Invoke charmhelpers.payload.execd.execd_run for site customization.'''
    # Only run the preinstall hooks once, in the first hook. This is
    # either the leader-elected hook or the install hook.
    config = hookenv.config()
    if not config.get('preinstall_done'):
        helpers.status_set('maintenance', 'Running preinstallation hooks')
        try:
            execd.execd_run('charm-pre-install', die_on_error=True)
            config['preinstall_done'] = True
        except SystemExit:
            helpers.block('execd_preinstall failed')
            raise SystemExit(0)


@data_ready_action
def configure_sources():
    config = hookenv.config()

    if not (config.changed('install_sources')
            or config.changed('install_keys')
            or config.changed('pgdg')
            or config.changed('wal_e_storage_uri')):
        hookenv.log('Sources unchanged')
        return

    helpers.status_set(hookenv.status_get(),
                       'Configuring software sources')

    # Shortcut for the PGDG archive.
    if config['pgdg'] and config.changed('pgdg'):
        pgdg_url = 'http://apt.postgresql.org/pub/repos/apt/'
        pgdg_src = 'deb {} {}-pgdg main'.format(pgdg_url,
                                                helpers.distro_codename())
        pgdg_key_path = os.path.join(hookenv.charm_dir(), 'lib', 'pgdg.key')
        with open(pgdg_key_path, 'r') as f:
            hookenv.log('Adding PGDG archive')
            fetch.add_source(pgdg_src, f.read())

    # WAL-E is currently only available from a PPA. This charm and this
    # PPA are maintained by the same person.
    if config['wal_e_storage_uri'] and config.changed('wal_e_storage_uri'):
        hookenv.log('Adding ppa:stub/pgcharm for wal-e packages')
        fetch.add_source('ppa:stub/pgcharm')

    # Standard charm-helpers, using install_sources and install_keys
    # provided by the operator. Called at the end so all previously
    # added sources share the apt update.
    fetch.configure_sources(update=True)


@data_ready_action
def ensure_locale():
    '''Ensure that the requested database locale is available.'''
    config = hookenv.config()
    if hookenv.hook_name() == 'install' and config['locale'] != 'C':
        helpers.status_set('maintenance',
                           'Generating {} locale'.format(config['locale']))
        subprocess.check_call(['locale-gen',
                               '{}.{}'.format(hookenv.config('locale'),
                                              hookenv.config('encoding'))],
                              universal_newlines=True)


@data_ready_action
def install_packages():
    packages = set(['rsync'])
    packages.update(postgresql.packages())
    packages.update(helpers.extra_packages())

    config = hookenv.config()
    config['packages_installed'] = sorted(packages)
    if config.changed('packages_installed'):
        filtered_packages = fetch.filter_installed_packages(packages)
        helpers.status_set(hookenv.status_get(), 'Installing packages')
        try:
            with postgresql.inhibit_default_cluster_creation():
                fetch.apt_install(filtered_packages, fatal=True)
        except subprocess.CalledProcessError:
            helpers.status_set('blocked',
                               'Unable to install packages {!r}'
                               .format(filtered_packages))
            raise SystemExit(0)


@data_ready_action
def ensure_package_status():
    packages = postgresql.packages()
    packages.update(helpers.extra_packages())

    config = hookenv.config()
    config['packages_marked'] = sorted(packages)
    if config.changed('packages_marked') or config.changed('package_status'):
        if config['package_status'] == 'hold':
            helpers.status_set('Holding charm packages')
            mark = 'hold'
        else:
            helpers.status_set('Removing hold on charm packages')
            mark = 'unhold'
        fetch.apt_mark(packages, mark, fatal=True)


@data_ready_action
def emit_deprecated_option_warnings():
    deprecated = sorted(helpers.deprecated_config_in_use())
    if deprecated:
        hookenv.log('Deprecated configuration settings in use: {}'
                    ', '.join(deprecated), WARNING)


@data_ready_action
def ensure_cluster():
    if not os.path.exists(postgresql.postgresql_conf_path()):
        data_dir = postgresql.data_dir()
        assert not os.path.exists(data_dir)
        postgresql.create_cluster()


@leader_only
@data_ready_action
def appoint_master():
    leader = context.Leader()
    master = postgresql.master()
    rel = context.Relations().peer
    local_unit = hookenv.local_unit()

    # TODO: Manual replication mode still needs a master. Detect and pick
    # the first primary.

    if master is None or not rel:
        hookenv.log('Appointing myself master')
        leader['master'] = hookenv.local_unit()
    elif master == local_unit:
        hookenv.log('I will remain master')
    elif master not in rel:
        hookenv.log('Master {} is gone'.format(master), WARNING)

        # Per Bug #1417874, the master doesn't know it is dying until it
        # is too late, and standbys learn about their master dying at
        # different times. We need to wait until all remaining units
        # are aware that the master is gone, which we can see by looking
        # at which units they have authorized. If we fail to do this step,
        # then we risk appointing a new master while some units are still
        # replicating data from the ex-master and we will end up with
        # diverging timelines. Unfortunately, this means failover will
        # not complete until hooks can be run on all remaining units,
        # which could be several hours if maintenance operations are in
        # progress. Once Bug #1417874 is addressed, the departing master
        # can cut off replication to all units simultaneously and we
        # can skip this step and allow failover to occur as soon as the
        # leader learns that the master is gone.
        ready_for_election = True
        for unit, relinfo in rel.items():
            if master in relinfo.get('allowed-units', '').split():
                hookenv.log('Waiting for {} to stop replicating ex-master'
                            ''.format(unit))
                ready_for_election = False
        if ready_for_election:
            new_master = replication.elect_master()
            hookenv.log('Failing over to new master {}'.format(new_master),
                        WARNING)
            leader['master'] = new_master
        else:
            helpers.status_set('Coordinating failover')
            raise SystemExit(0)


@data_ready_action
def update_kernel_settings():
    lots_and_lots = pow(1024, 4)  # 1 TB
    sysctl_settings = {'kernel.shmmax': lots_and_lots,
                       'kernel.shmall': lots_and_lots}
    sysctl.create(yaml.dump(sysctl_settings),
                  '/etc/sysctl.d/50-postgresql.conf')


@data_ready_action
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


@data_ready_action
def update_pg_hba_conf():
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
    # user connect to their matching PostgreSQL user, if it exists.
    add('local', 'all', 'all', 'peer')

    rels = context.Relations()

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

    # Admin clients need access to all databases as the relation users.
    for rel in rels['db-admin'].values():
        if 'user' in rel.local:
            for relinfo in rel.values():
                addr = postgresql.addr_to_range(relinfo['private-address'])
                add('host', 'all',
                    postgresql.quote_identifier(rel.local['user']),
                    postgresql.quote_identifier(addr),
                    'md5', '# {}'.format(relinfo))
                add('host', 'all',
                    postgresql.quote_identifier(rel.local['schema_user']),
                    postgresql.quote_identifier(addr),
                    'md5', '# {}'.format(relinfo))

    # External replication connections. Somewhat different than before
    # as the relation gets its own user to avoid sharing credentials,
    # and logical replication connections will want to specify the
    # database name.
    for rel in rels['master']:
        for relinfo in rel.values():
            addr = postgresql.addr_to_range(relinfo['private-address'])
            add('host', 'replication',
                postgresql.quote_identifier(rel.local['user']),
                postgresql.quote_identifier(addr),
                'md5', '# {}'.format(relinfo))
            if 'database' is rel.local:
                add('host',
                    postgresql.quote_identifier(rel.local['database']),
                    postgresql.quote_identifier(rel.local['user']),
                    postgresql.quote_identifier(addr),
                    'md5', '# {}'.format(relinfo))

    # External administrative addresses, if specified by the operator.
    config = hookenv.config()
    for addr in config['admin_addresses'].split(','):
        if addr:
            add('host', 'all', 'all', postgresql.addr_to_range(addr),
                'md5', '# admin_addresses config')

    # And anything-goes rules, if specified by the operator.
    for line in config['extra_pg_auth'].splitlines():
        add((line, '# extra_pg_auth config'))

    # Deny everything else
    add('local', 'all', 'all', 'reject', '# Refuse by default')
    add('host', 'all', 'all', 'all', 'reject', '# Refuse by default')

    # Load the existing file
    path = postgresql.pg_hba_conf_path()
    with open(path, 'r') as f:
        pg_hba = f.read()

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
    helpers.rewrite(path, pg_hba)


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
    validate_postgresql_conf(conf)

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

    num_standbys = len(rels.peer or {})
    for rel in rels['master'].values():
        num_standbys += len(rel)

    num_clients = 0
    for rel in list(rels['db']) + list(rels['db-admin']):
        num_clients += len(rel)

    # Even without replication, replication slots get used by
    # pg_basebackup(1). Bump up max_wal_senders so things work. It is
    # cheap, so perhaps we should just pump it to several thousand.
    min_wal_senders = num_standbys * 5 + 5
    if min_wal_senders > opts.get('max_wal_senders', 0):
        force(max_wal_senders=min_wal_senders)

    # max_connections. One per client unit, plus replication.
    min_max_connections = min_wal_senders + min(1, num_clients)
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
    if num_standbys and (config['replicated_wal_keep_segments']
                         > opts.get('wal_keep_segments', 0)):
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
            helpers.status_set('blocked',
                               'Invalid postgresql.conf setting {}: {}'
                               ''.format(k, x))
            raise SystemExit(0)


@data_ready_action
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

    # Store the updated charm options, so later handlers can detect
    # if important settings have changed and if PostgreSQL needs to
    # be restarted.
    config = hookenv.config()
    config['postgresql_conf'] = settings

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


@data_ready_action
def update_pgpass():
    leader = context.Leader()
    accounts = ['root', 'postgres']
    for account in accounts:
        path = os.path.expanduser(os.path.join('~{}'.format(account),
                                               '.pgpass'))
        content = ('# Managed by Juju\n'
                   '*:*:*:{}:{}'.format(replication.replication_username(),
                                        leader['replication_password']))
        helpers.write(path, content, mode=0o600, user=account, group=account)


@data_ready_action
def request_restart():
    if coordinator.requested('restart'):
        hookenv.log('Restart already requested')
        return

    # There is no reason to wait for permission from the leader before
    # restarting a stopped server.
    if not postgresql.is_running():
        hookenv.log('PostgreSQL is not running. No need to request restart.')

    # Detect if PostgreSQL settings have changed that require a restart.
    config = hookenv.config()
    if config.previous('postgresql_conf') is None:
        # We special case the first reconfig, as at this point we have
        # never even done a reload and the charm does not yet have access
        # to connect to the database.
        hookenv.log('First reconfig of postgresql.conf. Restart required.')
        coordinator.acquire('restart')
    elif config.changed('postgresql_conf'):
        old_config = config.previous('postgresql_conf')
        new_config = config['postgresql_conf']
        con = postgresql.connect()
        cur = con.cursor()
        cur.execute("SELECT name FROM pg_settings WHERE context='postmaster'")
        for row in cur.fetchall():
            key = row[0]
            old = old_config.get(key)
            new = new_config.get(key)
            if old != new:
                hookenv.log('{} changed from {!r} to {!r}. '
                            'Restart required.'.format(key, old, new))
                # Request permission from the leader to restart. We cannot
                # restart immediately or we risk interrupting operations
                # like backups and replica rebuilds.
                coordinator.acquire('restart')


@data_ready_action
def wait_for_restart():
    if coordinator.requested('restart') and not coordinator.granted('restart'):
        helpers.status_set('waiting', 'Waiting for permission to restart')
        raise SystemExit(0)


@data_ready_action
def restart_or_reload():
    '''Restart if necessary and leader has given permission, or else reload.'''
    if not postgresql.is_running():
        helpers.status_set('maintenance', 'Starting PostgreSQL')
        postgresql.start()
    elif coordinator.granted('restart'):
        helpers.status_set('maintenance', 'Restarting PostgreSQL')
        postgresql.stop()
        postgresql.start()
    else:
        postgresql.reload_config()


@data_ready_action
def reload_config():
    '''Send a reload signal.'''
    postgresql.reload_config()


@data_ready_action
def stop_postgresql():
    if postgresql.is_running():
        postgresql.stop()


@data_ready_action
def open_ports():
    # We can't use the standard Services Framework method of opening
    # our ports, as we don't know what they are when the ServiceManager
    # is instantiated.
    port = postgresql.port()
    config = hookenv.config()
    config['open_port'] = port

    if config.changed('open_port'):
        previous = config.previous('open_port')
        if previous:
            hookenv.close_port(previous)
        hookenv.open_port(port)


@data_ready_action
def close_ports():
    config = hookenv.config()
    port = config.get('open_port')
    if port is not None:
        hookenv.close_port(config['open_port'])
        config['open_port'] = None


# @data_ready_action
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


@data_ready_action
def set_active():
    if postgresql.is_running():
        if postgresql.is_master():
            msg = 'Live master'
        elif postgresql.is_primary():
            msg = 'Live primary'
        else:
            msg = 'Live secondary'
        helpers.status_set('active', msg)
    elif hookenv.status_get() == 'active':
        helpers.status_set('blocked', 'PostgreSQL unexpectedly shut down')
        raise SystemExit(0)


@data_ready_action
def install_administrative_scripts():
    scripts_dir = helpers.scripts_dir()
    logs_dir = helpers.logs_dir()
    helpers.makedirs(scripts_dir, mode=0o755)

    # The database backup script. Most of this is redundant now.
    source = os.path.join(hookenv.charm_dir(), 'scripts', 'pgbackup.py')
    destination = os.path.join(scripts_dir, 'dump-pg-db')
    with open(source, 'r') as f:
        helpers.write(destination, f.read(), mode=0o755)

    backup_dir = hookenv.config()['backup_dir']
    helpers.makedirs(backup_dir, mode=0o750,
                     user='postgres', group='postgres')

    # Generate a wrapper that invokes the backup script for each
    # database.
    data = dict(logs_dir=logs_dir,
                scripts_dir=scripts_dir,
                # backup_dir probably should be deprecated in favour of
                # a juju storage mount.
                backup_dir=hookenv.config()['backup_dir'])
    destination = os.path.join(helpers.scripts_dir(), 'pg_backup_job')
    templating.render('pg_backup_job.tmpl', destination, data,
                      owner='root', group='postgres', perms=0o755)

    if not os.path.exists(logs_dir):
        backups_log = os.path.join(logs_dir, 'backups.log')
        helpers.makedirs(logs_dir, mode=0o755)
        # Create the backups.log file used by the backup wrapper if it
        # does not exist, in order to trigger spurious alerts when a
        # unit is installed, per Bug #1329816.
        helpers.write(backups_log, '', mode=0o644)


@data_ready_action
def update_postgresql_crontab():
    config = hookenv.config()
    data = dict(config)

    data['scripts_dir'] = helpers.scripts_dir()
    data['is_master'] = postgresql.is_master()
    data['is_primary'] = postgresql.is_primary()

    if wal_e.wal_e_enabled():
        data['wal_e_enabled'] = True
        data['wal_e_backup_command'] = wal_e.wal_e_backup_command()
        data['wal_e_prune_command'] = wal_e.wal_e_prune_command()
    else:
        data['wal_e_enabled'] = False

    destination = '/etc/cron.d/juju_postgresql'
    templating.render('postgres.cron.tmpl', destination, data,
                      owner='root', group='postgres',
                      perms=0o640)
