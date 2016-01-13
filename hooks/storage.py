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

import os.path
import subprocess
import time

from charmhelpers import context
from charmhelpers.core import hookenv, host
from charmhelpers.core.hookenv import DEBUG

from coordinator import coordinator
from decorators import data_ready_action
import helpers
import postgresql

# Hard coded mount point for the block storage subordinate.
external_volume_mount = "/srv/data"


@data_ready_action
def handle_storage_relation():
    # Remove this once Juju storage is no longer experiemental and
    # everyone has had a chance to upgrade.
    data_rels = context.Relations()['data']
    if len(data_rels) > 1:
        helpers.status_set('blocked',
                           'Too many relations to the storage subordinate')
        raise SystemExit(0)
    elif data_rels:
        relid, rel = list(data_rels.items())[0]
        rel.local['mountpoint'] = external_volume_mount

    if needs_remount():
        # Migrate any data when we can restart.
        coordinator.acquire('restart')


def needs_remount():
    mounted = os.path.isdir(external_volume_mount)
    linked = os.path.islink(postgresql.data_dir())
    return mounted and not linked


def fix_perms(data_dir):
    # The path to data_dir must be world readable, so the postgres user
    # can traverse to it.
    p = data_dir
    while p != '/':
        p = os.path.dirname(p)
        subprocess.check_call(['chmod', 'a+rX', p], universal_newlines=True)

    # data_dir and all of its contents should be owned by the postgres
    # user and group.
    host.chownr(data_dir, 'postgres', 'postgres', follow_links=False)

    # data_dir should not be world readable.
    os.chmod(data_dir, 0o700)


@data_ready_action
def remount():
    if not needs_remount():
        return

    if postgresql.is_running():
        postgresql.stop()

    old_data_dir = postgresql.data_dir()
    new_data_dir = os.path.join(external_volume_mount, 'postgresql',
                                postgresql.version(), 'main')
    backup_data_dir = '{}-{}'.format(old_data_dir, int(time.time()))

    if not os.path.isdir(new_data_dir):
        hookenv.log('Migrating data from {} to {}'.format(old_data_dir,
                                                          new_data_dir))
        helpers.makedirs(new_data_dir, mode=0o700,
                         user='postgres', group='postgres')
        try:
            rsync_cmd = ['rsync', '-av',
                         old_data_dir + '/',
                         new_data_dir + '/']
            hookenv.log('Running {}'.format(' '.join(rsync_cmd)), DEBUG)
            subprocess.check_call(rsync_cmd)
            os.replace(old_data_dir, backup_data_dir)
            os.symlink(new_data_dir, old_data_dir)
            fix_perms(new_data_dir)
        except subprocess.CalledProcessError:
            helpers.status_set('blocked',
                               'Failed to sync data from {} to {}'
                               ''.format(old_data_dir, new_data_dir))
            raise SystemExit(0)
