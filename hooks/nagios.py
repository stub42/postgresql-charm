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

from charmhelpers import context
from charmhelpers.contrib.charmsupport.nrpe import NRPE
from charmhelpers.core import host

from decorators import data_ready_action, leader_only, master_only
import helpers
import postgresql


def nagios_username():
    return '_juju_nagios'


@leader_only
@data_ready_action
def ensure_nagios_credentials():
    leader = context.Leader()
    if 'nagios_password' not in leader:
        leader['nagios_password'] = host.pwgen()


@master_only
@data_ready_action
def ensure_nagios_user():
    leader = context.Leader()
    con = postgresql.connect()
    postgresql.ensure_user(con, nagios_username(), leader['nagios_password'])
    con.commit()


def nagios_pgpass_path():
    return os.path.expanduser('~nagios/.pgpass')


@data_ready_action
def update_nagios_pgpass():
    leader = context.Leader()
    nagios_password = leader['nagios_password']
    content = '*:*:*:{}:{}'.format(nagios_username(), nagios_password)
    helpers.write(nagios_pgpass_path(), content,
                  mode=0o600, user='nagios', group='nagios')


@data_ready_action
def update_nrpe_config():
    nrpe_compat = NRPE()
    nrpe_compat.add_check()
    nrpe_compat.write()
