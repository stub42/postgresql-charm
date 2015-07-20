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
import os.path
import re
import subprocess

from charmhelpers.core import hookenv
from charmhelpers.core.hookenv import DEBUG
from charmhelpers import fetch
from charmhelpers.payload import execd

from coordinator import coordinator
from decorators import data_ready_action, requirement
import helpers
import postgresql
import relations


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
                 performance_tuning=set(['dw', 'oltp', 'web', 'mixed',
                                         'desktop', 'manual']),
                 package_status=set(['install', 'hold']))
    for key, vals in enums.items():
        config[key] = config[key].lower()  # Rewrite to lower case.
        if config[key] not in vals:
            valid = False
            helpers.status_set('blocked',
                               'Invalid value for {} ({!r})'
                               .format(key, config[key]))

    unchangeable_config = ['locale', 'encoding', 'version', 'pgdg']
    if config._prev_dict is not None:
        for name in unchangeable_config:
            if config.changed(name):
                valid = False
                helpers.status_set('blocked',
                                   'Cannot change {!r} after install '
                                   '(was {!r}).'.format(name,
                                                        config.previous(name)))

    return valid


@requirement
def has_master():
    # The leader has chosen a master, or we are the leader and about to choose.
    return hookenv.is_leader() or postgresql.master() is not None


@data_ready_action
def preinstall(manager, service_name, event_name):
    '''Invoke charmhelpers.payload.execd.execd_run for site customization.'''
    # Only run the preinstall hooks once, in the first hook. This is
    # either the leader-elected hook or the install hook.
    config = hookenv.config()
    if config.get('preinstall_done'):
        helpers.status_set('maintenance', 'Running preinstallation hooks')
        try:
            execd.execd_run('charm-pre-install', die_on_error=True)
            config['preinstall_done'] = True
        except SystemExit:
            helpers.block('execd_preinstall failed')
            raise SystemExit(0)


@data_ready_action
def configure_sources(manager, service_name, event_name):
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
def ensure_locale(manager, service_name, event_name):
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
def install_packages(manager, service_name, event_name):
    packages = postgresql.packages()
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
def ensure_package_status(manager, service_name, event_name):
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
def ensure_cluster(manager, service_name, event_name):
    if not os.path.exists(postgresql.postgresql_conf_path()):
        postgresql.create_cluster()


@data_ready_action
def appoint_master(manager, service_name, event_name):
    # Underconstruction. First leader is master forever.
    if hookenv.is_leader() and not postgresql.master():
        hookenv.leader_set(master=hookenv.local_unit())


@data_ready_action
def update_pg_ident_conf(manager, service_name, event_name):
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
def update_pg_hba_conf(manager, service_name, event_name):
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

    # # Peers need replication access
    # for peer in helpers.peers():
    #     relinfo = hookenv.relation_get(unit=peer, rid=helpers.peer_relid())
    #     addr = helpers.addr_to_range(relinfo.get('private-address'))
    #     add('host', 'replication', 'postgres', addr, replication_password)

    # Clients need access to the relation database as the relation users.
    service = manager.get_service(service_name)
    clients = [provider for provider in service['provided_data']
               if isinstance(provider, relations.DbRelation)]
    for client in clients:
        for service_name, master_relinfo in sorted(client.master.items()):
            remotes = client.remote[service_name]
            for remote_unit, remote_relinfo in sorted(remotes.items()):
                addr = remote_relinfo['private-address']
                addr = postgresql.addr_to_range(addr)
                # Quote everything, including the address, to # disenchant
                # magic tokens like 'all'.
                add('host',
                    postgresql.quote_identifier(master_relinfo['database']),
                    postgresql.quote_identifier(master_relinfo['user']),
                    postgresql.quote_identifier(addr),
                    'md5', '# {}'.format(remote_unit))

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


@data_ready_action
def update_postgresql_conf(manager, service_name, event_name):
    config = hookenv.config()

    charm_opts = dict(listen_addresses='*')

    path = postgresql.postgresql_conf_path()

    with open(path, 'r') as f:
        pg_conf = f.read()

    start_mark = '### BEGIN JUJU SETTINGS ###'
    end_mark = '### END JUJU SETTINGS ###'

    # Strip the existing settings section, including the markers.
    pg_conf = re.sub(r'^\s*{}.*^\s*{}\s*$'.format(re.escape(start_mark),
                                                  re.escape(end_mark)),
                     '', pg_conf, flags=re.I | re.M | re.DOTALL)

    for k in charm_opts:
        # Comment out conflicting options. We could just allow later
        # options to override earlier ones, but this is less surprising.
        pg_conf = re.sub(r'^\s*({}[\s=].*)$'.format(re.escape(k)),
                         r'# juju # \1', pg_conf, flags=re.M | re.I)

    # Store the updated charm options, so later handlers can detect
    # if important settings have changed and if PostgreSQL needs to
    # be restarted.
    config['postgresql_conf'] = charm_opts

    # Generate the charm config section, adding it to the end of the
    # config file.
    override_section = [start_mark]
    for k, v in charm_opts.items():
        if isinstance(v, str):
            assert '\n' not in v, "Invalid config value {!r}".format(v)
            v = "'{}'".format(v.replace("'", "''"))
        override_section.append('{} = {}'.format(k, v))
    override_section.append(end_mark)
    pg_conf += '\n' + '\n'.join(override_section)

    helpers.rewrite(path, pg_conf)


@data_ready_action
def request_restart(manager, service_name, event_name):
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
                            'Restart required.'.format(old, new))
                # Request permission from the leader to restart. We cannot
                # restart immediately or we risk interrupting operations
                # like backups and replica rebuilds.
                coordinator.acquire('restart')

    # Similarly, if recovery.conf has changed we need to restart.
    # eg. a secondary has been reparented, or the parent IP address
    # has changed.
    if config.changed('recovery_conf'):
        coordinator.acquire('restart')


@data_ready_action
def reload_or_restart(manager, service_name, event_name):
    '''Restart if necessary and leader has given permission, or else reload.'''
    if coordinator.granted('restart') or not postgresql.is_running():
        subprocess.check_call(['pg_ctlcluster', '--mode=fast',
                              postgresql.version(), 'main', 'restart'],
                              universal_newlines=True)
    else:
        subprocess.check_call(['pg_ctlcluster', postgresql.version(),
                               'main', 'reload'],
                              universal_newlines=True)


@data_ready_action
def stop_postgresql(manager, service_name, event_name):
    if postgresql.is_running():
        postgresql.stop()


@data_ready_action
def open_ports(manager, service_name, event_name):
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
def close_ports(manager, service_name, event_name):
    config = hookenv.config()
    hookenv.close_port(config['open_port'])


@data_ready_action
def ensure_client_resources(manager, service_name, event_name):
    if not postgresql.is_master():
        # Only the master manages credentials and creates the database.
        hookenv.log('Not the master, nothing to do.', DEBUG)
        return

    service = manager.get_service(service_name)
    for provider in service['provided_data']:
        if isinstance(provider, relations.DbRelation):
            for remote_service in provider.remote:
                provider.ensure_db_resources(remote_service)


@data_ready_action
def set_active(manager, service_name, event_name):
    if postgresql.is_running():
        if postgresql.is_master():
            msg = 'Live Master'
        elif postgresql.is_primary():
            msg = 'Live Primary'
        else:
            msg = 'Live Secondary'
        helpers.status_set('active', msg)
    elif hookenv.status_get() == 'active':
        helpers.status_set('blocked', 'PostgreSQL unexpectedly shut down')
        raise SystemExit(0)
