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

import os.path

from charmhelpers.contrib.charmsupport.nrpe import NRPE
from charmhelpers.core import hookenv, host

from charms import leadership, reactive
from charms.reactive import hook, only_once, when, when_any, when_not

import context
from reactive.postgresql import helpers
from reactive.postgresql import postgresql


@hook("nrpe-external-master-relation-changed", "local-monitors-relation-changed")
def enable_nagios(*dead_chickens):
    if os.path.exists("/var/lib/nagios"):
        reactive.set_state("postgresql.nagios.enabled")
        reactive.set_state("postgresql.nagios.needs_update")


@hook("upgrade-charm")
def upgrade_charm():
    reactive.set_state("postgresql.nagios.needs_update")
    reactive.remove_state("postgresql.nagios.user_ensured")


@when("postgresql.nagios.enabled")
@when_any("config.changed", "leadership.changed.master")
def update_nagios():
    reactive.set_state("postgresql.nagios.needs_update")


def nagios_username():
    return "nagios"


@when("postgresql.nagios.enabled")
@when("leadership.is_leader")
@when_not("leadership.set.nagios_password")
def ensure_nagios_credentials():
    leadership.leader_set(nagios_password=host.pwgen())


@when("postgresql.nagios.enabled")
@when("postgresql.cluster.is_running")
@when("postgresql.replication.is_primary")
@when("leadership.set.nagios_password")
@when_not("postgresql.nagios.user_ensured")
def ensure_nagios_user():
    con = postgresql.connect()
    postgresql.ensure_user(con, nagios_username(), leadership.leader_get("nagios_password"))
    con.commit()
    reactive.set_state("postgresql.nagios.user_ensured")


@when("leadership.changed.nagios_password")
def reensure_nagios_user():
    reactive.remove_state("postgresql.nagios.user_ensured")


def nagios_pgpass_path():
    return os.path.expanduser("~nagios/.pgpass")


@when("postgresql.nagios.enabled")
@when("leadership.changed.nagios_password")
def update_nagios_pgpass():
    leader = context.Leader()
    nagios_password = leader["nagios_password"]
    content = "*:*:*:{}:{}".format(nagios_username(), nagios_password)
    helpers.write(nagios_pgpass_path(), content, mode=0o600, user="nagios", group="nagios")


@when("postgresql.nagios.enabled")
@when("leadership.set.nagios_password")
@only_once
def create_nagios_pgpass():
    update_nagios_pgpass()


@when("postgresql.nagios.enabled")
@when("postgresql.nagios.needs_update")
@when("leadership.set.nagios_password")
def update_nrpe_config():
    update_nagios_pgpass()
    nrpe = NRPE()

    user = nagios_username()
    port = postgresql.port()
    nrpe.add_check(
        shortname="pgsql",
        description="Check pgsql",
        check_cmd="check_pgsql -P {} -l {}".format(port, user),
    )

    # copy the check script which will run cronned as postgres user
    with open("scripts/find_latest_ready_wal.py") as fh:
        check_script = fh.read()

    check_script_path = "{}/{}".format(helpers.scripts_dir(), "find_latest_ready_wal.py")
    helpers.write(check_script_path, check_script, mode=0o755)

    # create an (empty) file with appropriate permissions for the above
    check_output_path = "/var/lib/nagios/postgres-wal-max-age.txt"
    if not os.path.exists(check_output_path):
        helpers.write(check_output_path, b"0\n", mode=0o644, user="postgres", group="postgres")

    # retrieve the threshold values from the charm config
    config = hookenv.config()
    check_warn_threshold = config["wal_archive_warn_threshold"] or 0
    check_crit_threshold = config["wal_archive_crit_threshold"] or 0

    check_cron_path = "/etc/cron.d/postgres-wal-archive-check"
    if check_warn_threshold and check_crit_threshold:
        # create the cron job to run the above
        check_cron = "*/2 * * * * postgres {}".format(check_script_path)
        helpers.write(check_cron_path, check_cron, mode=0o644)

    # copy the nagios plugin which will check the cronned output
    with open("scripts/check_latest_ready_wal.py") as fh:
        check_script = fh.read()
    check_script_path = "{}/{}".format("/usr/local/lib/nagios/plugins", "check_latest_ready_wal.py")
    helpers.write(check_script_path, check_script, mode=0o755)

    # write the nagios check definition
    nrpe.add_check(
        shortname="pgsql_stale_wal",
        description="Check for stale WAL backups",
        check_cmd="{} {} {}".format(check_script_path, check_warn_threshold, check_crit_threshold),
    )

    if reactive.is_state("postgresql.replication.is_master"):
        # TODO: These should be calculated from the backup schedule,
        # which is difficult since that is specified in crontab format.
        warn_age = 172800
        crit_age = 194400
        backups_log = helpers.backups_log_path()
        nrpe.add_check(
            shortname="pgsql_backups",
            description="Check pgsql backups",
            check_cmd=("check_file_age -w {} -c {} -f {}" "".format(warn_age, crit_age, backups_log)),
        )
    else:
        # Standbys don't do backups. We still generate a check though,
        # to ensure alerts get through to monitoring after a failover.
        nrpe.add_check(
            shortname="pgsql_backups",
            description="Check pgsql backups",
            check_cmd=r"check_dummy 0 standby_does_not_backup",
        )
    nrpe.write()
    reactive.remove_state("postgresql.nagios.needs_update")
