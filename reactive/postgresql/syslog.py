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

from charmhelpers.core import hookenv, unitdata
from charms.reactive import when


@when("syslog.available", "postgresql.cluster.configured")
def configure_syslog(syslog):
    programname = hookenv.local_unit().replace("/", "_")
    syslog.configure(programname=programname)

    # Extend the basic syslog interface. Add programname and
    # log_line_prefix, required for consumers such as pgBadger to
    # decode the logs.
    log_line_prefix = get_log_line_prefix()
    for conv in syslog.conversations():
        conv.set_remote("programname", programname)
        conv.set_remote("log_line_prefix", log_line_prefix)


def get_log_line_prefix():
    store = unitdata.kv()
    log_line_prefix_key = "postgresql.cluster.pgconf.current.log_line_prefix"
    return store.get(log_line_prefix_key)
