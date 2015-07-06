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
import shutil
import subprocess

from charmhelpers.core import hookenv
from charmhelpers.core.hookenv import DEBUG
from charmhelpers import fetch
from charmhelpers.payload import execd

from decorators import data_ready_action, requirement
import helpers
import postgresql


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
        config[key] = config[key].lower()
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


@data_ready_action
def preinstall():
    '''Invoke charmhelpers.payload.execd.execd_run for site customization.'''
    # Only run the preinstall hooks from the actual install hook.
    if hookenv.hook_name() == 'install':
        helpers.status_set('maintenance', 'Running preinstallation hooks')
        try:
            execd.execd_run('charm-pre-install', die_on_error=True)
        except SystemExit:
            helpers.block('execd_preinstall failed')
            raise SystemExit(0)


@data_ready_action
def configure_sources():
    config = hookenv.config()

    if not (config.changed('install_sources')
            or config.changed('install_keys')
            or config.changed('pgdg')
            or config.changed('extra-packages')  # Deprecated.
            or config.changed('extra_packages')
            or config.changed('wal_e_storage_uri')):
        hookenv.log('Sources unchanged')
        return

    helpers.status_set(hookenv.status_get(),
                       'Configuring software sources')

    # Shortcut for the PGDG archive.
    if config['pgdg']:
        pgdg_url = 'http://apt.postgresql.org/pub/repos/apt/'
        pgdg_src = 'deb {} {}-pgdg main'.format(pgdg_url,
                                                helpers.distro_codename())
        pgdg_key_path = os.path.join(hookenv.charm_dir(), 'lib', 'pgdg.key')
        with open(pgdg_key_path, 'r') as f:
            hookenv.log('Adding PGDG archive')
            fetch.add_source(pgdg_src, f.read())

    # WAL-E is currently only available from a PPA. This charm and this
    # PPA are maintained by the same person.
    if config['wal_e_storage_uri']:
        hookenv.log('Adding ppa:stub/pgcharm for wal-e packages')
        fetch.add_source('ppa:stub/pgcharm')

    # Standard charm-helpers, using install_sources and install_keys
    # provided by the operator.
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
    if hookenv.hook_name() == 'install':
        packages = postgresql.packages()
    else:
        packages = set()

    config = hookenv.config()
    if config.changed('extra-packages') or config.changed('extra_packages'):
        packages.update(helpers.extra_packages())

    if config.changed('wal_e_storage_uri'):
        packages.add('wal-e')
        packages.add('daemontools')

    if (config['performance_tuning'].lower() != 'manual'
            and shutil.which('pgtune') is None):
        packages.add('pgtune')

    if packages:
        # Add any new packages to the set of all packages this charm
        # has installed. These are the packages that ensure_package_status
        # will hold or release.
        all_installed_packages = set(config.get('all_installed_packages', []))
        all_installed_packages.update(packages)
        config['all_installed_packages'] = sorted(all_installed_packages)

        filtered_packages = fetch.filter_installed_packages(packages)
        helpers.status_set(hookenv.status_get(), 'Installing packages')
        try:
            fetch.apt_install(filtered_packages, fatal=True)

        except subprocess.CalledProcessError:
            helpers.status_set('blocked',
                               'Unable to install packages {!r}'
                               .format(filtered_packages))
            raise SystemExit(0)


@data_ready_action
def ensure_package_status():
    config = hookenv.config()
    packages = config.get('all_installed_packages', [])
    status = config['package_status']
    if status == 'hold':
        hookenv.log('Holding {!r}'.format(packages), DEBUG)
        fetch.apt_hold(packages, fatal=True)
    else:
        hookenv.log('Unholding {!r}'.format(packages), DEBUG)
        fetch.apt_unhold(packages, fatal=True)


@data_ready_action
def generate_hba_conf():
    '''Generate pg_hba.conf (host based authentication).'''
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

    # The local unit needs access to its own database. Let every local
    # user connect to their matching PostgreSQL user, if it exists.
    add('local', 'all', 'all', 'peer')

    # # Peers need replication access
    # for peer in helpers.peers():
    #     relinfo = hookenv.relation_get(unit=peer, rid=helpers.peer_relid())
    #     addr = helpers.addr_to_range(relinfo.get('private-address'))
    #     add('host', 'replication', 'postgres', addr, replication_password)

    # Clients need access to the relation database as the relation users.
    for relname in ('db', 'db-admin'):
        for relid in hookenv.relation_ids(relname):
            local_relinfo = hookenv.relation_get(unit=hookenv.local_unit(),
                                                 rid=relid)
            for unit in hookenv.related_units(relid):
                remote_relinfo = hookenv.relation_get(unit=unit, rid=relid)
                addr = helpers.addr_to_range(remote_relinfo['private-address'])
                add('host', local_relinfo['database'], local_relinfo['user'],
                    addr, 'md5')

    # External administrative addresses, if specified by the operator.
    config = hookenv.config()
    for addr in config['admin_address'].split(','):
        add('host', 'all', 'all', helpers.addr_to_range(addr), 'md5')

    # And anything-goes rules, if specified by the operator.
    for line in config['extra_pg_auth'].splitlines():
        add(line)

    # Deny everything else
    add('local', 'all', 'all', 'reject')
    add('host', 'all', 'all', 'reject')


@data_ready_action
def stop_postgresql():
    if postgresql.is_running():
        postgresql.stop()
