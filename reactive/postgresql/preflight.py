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

from charmhelpers.core import hookenv

from reactive.workloadstatus import status_set
from reactive.postgresql import postgresql

from preflight import preflight


@preflight
def block_on_bad_juju():
    if not hookenv.has_juju_version("1.24"):
        status_set("blocked", "Requires Juju 1.24 or higher")
        # Error state, since we don't have 1.24 to give a nice blocked state.
        raise SystemExit(1)


@preflight
def block_on_invalid_config():
    """
    Sanity check charm configuration, blocking the unit if we have
    bogus bogus config values or config changes the charm does not
    yet (or cannot) support.

    We need to do this before the main reactive loop (@preflight),
    or we risk failing to run handlers that rely on @when_file_changed,
    reactive.helpers.data_changed or similar state tied to
    charmhelpers.core.unitdata transactions.
    """
    valid = True
    config = hookenv.config()

    enums = dict(
        version=set(["", "9.3", "9.4", "9.5", "9.6", "10", "11", "12"]),
        package_status=set(["install", "hold"]),
    )
    for key, vals in enums.items():
        config[key] = (config.get(key) or "").lower()
        if config[key] not in vals:
            valid = False
            status_set("blocked", "Invalid value for {} ({!r})".format(key, config[key]))

    unchangeable_config = ["locale", "encoding", "manual_replication"]
    if config._prev_dict is not None:
        for name in unchangeable_config:
            if config.changed(name):
                config[name] = config.previous(name)
                valid = False
                status_set(
                    "blocked",
                    "Cannot change {!r} after install "
                    "(from {!r} to {!r}).".format(name, config.previous(name), config.get("name")),
                )
        if config.changed("version") and (config.previous("version") != postgresql.version()):
            valid = False
            status_set(
                "blocked",
                "Cannot change version after install "
                "(from {!r} to {!r}).".format(config.previous("version"), config["version"]),
            )
            config["version"] = config.previous("version")
            valid = False

    metrics_target = config["metrics_target"].strip()
    if metrics_target:
        if ":" not in metrics_target:
            status_set("blocked", "Invalid metrics_target {}".format(metrics_target))
            valid = False
        metrics_interval = config["metrics_sample_interval"]
        if not metrics_interval:
            status_set(
                "blocked",
                "metrics_sample_interval is required when " "metrics_target is set",
            )
            valid = False

    if not valid:
        raise SystemExit(0)
