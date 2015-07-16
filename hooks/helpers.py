# Copyright 2015 Canonical Ltd.
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

import os
import shutil
import stat
import tempfile

from charmhelpers.core import hookenv, host
from charmhelpers.core.hookenv import INFO, CRITICAL

from coordinator import coordinator


def status_set(status_or_msg, msg=None):
    '''Set the unit status message, and log the change too.'''
    if msg is None:
        msg = status_or_msg
        status = hookenv.status_get()
    else:
        status = status_or_msg

    if status == 'blocked':
        lvl = CRITICAL
    else:
        lvl = INFO
    hookenv.log('{}: {}'.format(status, msg), lvl)
    hookenv.status_set(status, msg)


def distro_codename():
    """Return the distro release code name, eg. 'precise' or 'trusty'."""
    return host.lsb_release()['DISTRIB_CODENAME']


def extra_packages():
    config = hookenv.config()
    packages = set()

    packages.update(set(config['extra_packages'].split()))
    packages.update(set(config['extra-packages'].split()))  # Deprecated.

    if config['wal_e_storage_uri']:
        packages.add('daemontools')
        packages.add('wal-e')

    if config['performance_tuning'] != 'manual':
        packages.add('pgtune')

    return packages


def peer_relid():
    '''Return the peer relation id.'''
    return coordinator.relid


def peers():
    '''Return the set of peers, not including the local unit.'''
    relid = peer_relid()
    return set(hookenv.related_units(relid)) if relid else set()


def maybe_backup(path):
    '''Make a backup of path, if the backup doesn't already exist.'''
    bak = path + '.bak'
    if not os.path.exists(bak):
        shutil.copy2(path, bak)


def rewrite(path, content, mode='w'):
    '''Rewrite a file atomically, preserving ownership and permissions.'''
    attr = os.lstat(path)
    assert stat.S_ISREG(attr.st_mode), '{} not a regular file'.format(path)
    with tempfile.NamedTemporaryFile(mode=mode, delete=False) as f:
        try:
            f.write(content)
            f.flush()
            os.chown(f.name, attr[stat.ST_UID], attr[stat.ST_GID])
            os.chmod(f.name, stat.S_IMODE(attr.st_mode))
            os.replace(f.name, path)
        finally:
            if os.path.exists(f.name):
                os.unlink(f.name)
