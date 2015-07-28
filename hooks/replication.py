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

from charmhelpers import context
from charmhelpers.core import host

from decorators import data_ready_action, leader_only, master_only
import postgresql


def replication_username():
    # Leading underscore for 'system' accounts, to avoid an unlikely
    # conflict with a client service named 'repl'.
    return '_juju_repl'


@leader_only
@data_ready_action
def ensure_replication_credentials(manager, service_name, event_name):
    leader = context.Leadership()
    if 'replication_password' not in leader:
        leader['replication_password'] = host.pwgen()


@master_only
@data_ready_action
def ensure_replication_user(manager, service_name, event_name):
    leader = context.Leadership()
    con = postgresql.connect()
    postgresql.ensure_user(con, replication_username(),
                           leader['replication_password'],
                           replication=True)


# @secondary_only
# @data_ready_action
# def clone_master(manager, service_name, event_name):
#     config = hookenv.config()
#     if config['manual_replication']:
#         hookenv.log('manual_replication, nothing to do')
#         return
#
#     master = postgresql.master()
