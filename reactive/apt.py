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

'''
charms.reactive layer and helpers for dealing with Debian packages.

Add debian package sources using add_source(). Queue packages for
installation with install(). Configure your packages once the
apt.installed.{packagename} state is set.
'''
__all__ = ['add_source', 'update',
           'queue_install', 'install_queued', 'installed', 'purge']
import itertools
import subprocess

from charmhelpers import fetch
from charmhelpers.core import hookenv, unitdata
from charms import reactive
from charms.reactive import when, when_not

from reactive.workloadstatus import status_set

from preflight import preflight


def add_source(source, key=None):
    '''Add an apt source.

    Sets the apt.needs_update state.
    '''
    fetch.add_source(source, key)
    reactive.set_state('apt.needs_update')


def queue_install(packages, options=None):
    """Queue for install one or more packages.

    Package is installed when the `apt.installed.{name}` state is set.

    If a package has already been installed it will not be reinstalled.

    If a package has already been queued it will not be requeued, and
    the install options will not be changed.

    Sets the apt.queued_installs state.
    Removes the apt.installed state.
    """
    # Filter installed packages.
    store = unitdata.kv()
    queued_packages = store.getrange('apt.install_queue.', strip=True)
    packages = {package: options for package in packages
                if not (package in queued_packages or
                        reactive.helpers.is_state('apt.installed.' + package))}
    if packages:
        unitdata.kv().update(packages, prefix='apt.install_queue.')
        reactive.set_state('apt.queued_installs')


def installed():
    '''Return the set of packages successfully installed by install_queued()'''
    return set(state.split('.', 2)[2] for state in reactive.bus.get_states()
               if state.startswith('apt.installed.'))


def purge(packages):
    """Purge one or more packages"""
    fetch.apt_purge(packages, fatal=True)
    store = unitdata.kv()
    store.unsetrange(packages, prefix='apt.install_queue.')
    for package in packages:
        reactive.remove_state('apt.installed.{}'.format(package))


@when('apt.needs_update')
def update():
    """Update the apt cache.

    Sets the apt.updated state.
    """
    status_set(None, 'Updating apt cache')
    fetch.apt_update(fatal=True)  # Friends don't let friends set fatal=False
    reactive.remove_state('apt.needs_update')


@when('apt.queued_installs')
@when_not('apt.needs_update')
def install_queued():
    '''Installs queued packages.

    Removes the apt.queued_installs state and sets the apt.installed state.

    On failure, sets the unit's workload state to 'blocked'.

    Sets the apt.installed.{packagename} state for each installed package.
    Failed package installs remain queued.
    '''
    store = unitdata.kv()
    queue = sorted((options, package)
                   for package, options in store.getrange('apt.install_queue.',
                                                          strip=True).items())

    installed = set()
    for options, batch in itertools.groupby(queue, lambda x: x[0]):
        packages = [b[1] for b in batch]
        try:
            status_set(None, 'Installing {}'.format(','.join(packages)))
            fetch.apt_install(packages, options, fatal=True)
            store.unsetrange(packages, prefix='apt.install_queue.')
            installed.update(packages)
        except subprocess.CalledProcessError:
            status_set('blocked',
                       'Unable to install packages {}'
                       .format(','.join(packages)))
            return  # Without setting reactive state.

    for package in installed:
        reactive.set_state('apt.installed.{}'.format(package))

    reactive.remove_state('apt.queued_installs')


@when_not('apt.queued_installs')
def ensure_package_status():
    packages = installed()
    if not packages:
        return
    config = hookenv.config()
    package_status = config['package_status']
    changed = reactive.helpers.data_changed('apt.package_status',
                                            (package_status, sorted(packages)))
    if changed:
        if package_status == 'hold':
            hookenv.log('Holding packages {}'.format(','.join(packages)))
            fetch.apt_hold(packages)
        else:
            hookenv.log('Unholding packages {}'.format(','.join(packages)))
            fetch.apt_unhold(packages)
    reactive.remove_state('apt.needs_hold')


@preflight
def validate_config():
    package_status = hookenv.config().get('package_status')
    if package_status not in ('hold', 'install'):
        status_set('blocked',
                   'Unknown package_status {}'.format(package_status))
        raise SystemExit(0)


@preflight
def configure_sources():
    """Add user specified package sources from the service configuration.

    See charmhelpers.fetch.configure_sources for details.
    """
    config = hookenv.config()
    sources = config.get('install_sources')
    keys = config.get('install_keys')
    if reactive.helpers.data_changed('apt.configure_sources', (sources, keys)):
        fetch.configure_sources(update=False,
                                sources_var='install_sources',
                                keys_var='install_keys')
        reactive.set_state('apt.needs_update')
