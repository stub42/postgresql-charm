# Copyright 2015-2017 Canonical Ltd.
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

"""
Legacy Block Storage Broker mounts.

New installations should use Juju native storage.
"""

import os.path
import subprocess
import time

from charmhelpers.core import hookenv
from charmhelpers.core.hookenv import DEBUG, WARNING
from charms import apt, coordinator, reactive
from charms.reactive import hook, when

import context
from reactive.postgresql import helpers
from reactive.postgresql import postgresql
from reactive.postgresql import service
from reactive.postgresql.storage import fix_perms
from reactive.workloadstatus import status_set


# Hard coded mount point for the block storage subordinate.
external_volume_mount = "/srv/data"


@hook("data-relation-changed")
def handle_storage_relation(dead_chicken):
    # Remove this once Juju storage is no longer experiemental and
    # everyone has had a chance to upgrade.
    data_rels = context.Relations()["data"]
    if len(data_rels) > 1:
        helpers.status_set("blocked", "Too many relations to the storage subordinate")
        return
    elif data_rels:
        relid, rel = list(data_rels.items())[0]
        rel.local["mountpoint"] = external_volume_mount

    if needs_remount():
        reactive.set_state("postgresql.storage.needs_remount")
        apt.queue_install(["rsync"])
        # Migrate any data when we can restart.
        coordinator.acquire("restart")


@hook("{interface:block-storage}-relation-departed")
def depart_storage_relation():
    status_set("blocked", "Unable to continue after departing block storage relation")


def needs_remount():
    mounted = os.path.isdir(external_volume_mount)
    linked = os.path.islink(postgresql.data_dir())
    return mounted and not linked


@when("postgresql.storage.needs_remount")
@when("coordinator.granted.restart")
@when("apt.installed.rsync")
def remount():
    if reactive.is_state("postgresql.cluster.is_running"):
        # Attempting this while PostgreSQL is live would be really, really bad.
        service.stop()

    old_data_dir = postgresql.data_dir()
    new_data_dir = os.path.join(external_volume_mount, "postgresql", postgresql.version(), "main")
    backup_data_dir = "{}-{}".format(old_data_dir, int(time.time()))

    if os.path.isdir(new_data_dir):
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
            subprocess.check_call(rsync_cmd)
        except subprocess.CalledProcessError:
            status_set(
                "blocked",
                "Failed to sync data from {} to {}" "".format(old_data_dir, new_data_dir),
            )
            return

    os.replace(old_data_dir, backup_data_dir)
    os.symlink(new_data_dir, old_data_dir)
    fix_perms(new_data_dir)
    reactive.remove_state("postgresql.storage.needs_remount")
