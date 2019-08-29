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
import shutil
import subprocess
import time

from charmhelpers.core import hookenv, host, unitdata
from charmhelpers.core.hookenv import DEBUG, WARNING
from charms import apt, coordinator, reactive
from charms.reactive import hook, when, when_not

from reactive.postgresql import helpers
from reactive.postgresql import postgresql
from reactive.postgresql import service
from reactive.workloadstatus import status_set


pgdata_mount_key = "postgresql.storage.pgdata.mount"
pgdata_path_key = "postgresql.storage.pgdata.path"


@hook("pgdata-storage-attached")
def attach():
    storageids = hookenv.storage_list("pgdata")
    if not storageids:
        hookenv.status_set("blocked", "Cannot locate attached storage")
        return
    storageid = storageids[0]

    mount = hookenv.storage_get("location", storageid)
    if not mount:
        hookenv.status_set("blocked", "Cannot locate attached storage mount")
        return

    pgdata = os.path.join(mount, postgresql.version(), "main")
    unitdata.kv().set(pgdata_mount_key, mount)
    unitdata.kv().set(pgdata_path_key, pgdata)

    hookenv.log("PGDATA storage attached at {}".format(mount))

    existingdb = os.path.exists(pgdata)
    if os.path.exists(postgresql.data_dir()) and not existingdb:
        required_space = shutil.disk_usage(postgresql.data_dir()).used
        free_space = shutil.disk_usage(mount).free
        if required_space > free_space:
            hookenv.status_set("blocked", "Not enough free space in pgdata storage")
            return

    apt.queue_install(["rsync"])
    coordinator.acquire("restart")
    reactive.set_state("postgresql.storage.pgdata.attached")


@hook("pgdata-storage-detaching")
def detaching():
    if reactive.is_state("postgresql.storage.pgdata.migrated"):
        # We don't attempt to migrate data back to local storage as there
        # is probably not enough of it. And we are most likely destroying
        # the unit, so it would be a waste of time even if there is enough
        # space.
        hookenv.status_set("blocked", "Storage detached. Database destroyed.")
        reactive.set_state("postgresql.cluster.destroyed")
        reactive.remove_state("postgresql.cluster.created")
        reactive.remove_state("postgresql.cluster.configured")
        reactive.remove_state("postgresql.cluster.is_running")
        postgresql.stop()
    else:
        unitdata.kv().unset(pgdata_mount_key)
        unitdata.kv().unset(pgdata_path_key)
        reactive.remove_state("postgresql.storage.pgdata.attached")


@when("postgresql.storage.pgdata.attached")
@when("postgresql.cluster.created")
@when("coordinator.granted.restart")
@when("apt.installed.rsync")
@when_not("postgresql.storage.pgdata.migrated")
def migrate_pgdata():
    """
    Copy the data from /var/lib/postgresql/9.x/main to the
    new path and replace the original PGDATA with a symlink.
    Note that the original may already be a symlink, either from
    the block storage broker or manual changes by admins.
    """
    if reactive.is_state("postgresql.cluster.is_running"):
        # Attempting this while PostgreSQL is live would be really, really bad.
        service.stop()

    old_data_dir = postgresql.data_dir()
    new_data_dir = unitdata.kv().get(pgdata_path_key)

    backup_data_dir = "{}-{}".format(old_data_dir, int(time.time()))

    if os.path.isdir(new_data_dir):
        # This never happens with Juju storage, at least with 2.0,
        # because we have no way of reusing old partitions.
        hookenv.log("Remounting existing database at {}".format(new_data_dir), WARNING)
    else:
        status_set(
            "maintenance",
            "Migrating data from {} to {}".format(old_data_dir, new_data_dir),
        )
        helpers.makedirs(new_data_dir, mode=0o770, user="postgres", group="postgres")
        try:
            rsync_cmd = ["rsync", "-av", old_data_dir + "/", new_data_dir + "/"]
            hookenv.log("Running {}".format(" ".join(rsync_cmd)), DEBUG)
            subprocess.check_call(rsync_cmd, universal_newlines=True)
        except subprocess.CalledProcessError:
            status_set(
                "blocked",
                "Failed to sync data from {} to {}"
                "".format(old_data_dir, new_data_dir),
            )
            return

    os.replace(old_data_dir, backup_data_dir)
    os.symlink(new_data_dir, old_data_dir)
    fix_perms(new_data_dir)
    reactive.set_state("postgresql.storage.pgdata.migrated")


def fix_perms(data_dir):
    # The path to data_dir must be world readable, so the postgres user
    # can traverse to it.
    p = data_dir
    while p != "/":
        p = os.path.dirname(p)
        subprocess.check_call(["chmod", "a+rX", p], universal_newlines=True)

    # data_dir and all of its contents should be owned by the postgres
    # user and group.
    host.chownr(data_dir, "postgres", "postgres", follow_links=False)

    # data_dir should not be world readable.
    os.chmod(data_dir, 0o700)
