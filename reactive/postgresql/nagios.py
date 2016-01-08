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

from charms import reactive
from charms.reactive import hook, only_once, when, when_not

from reactive import leadership
from reactive.postgresql import helpers
from reactive.postgresql import postgresql


@hook('{interface:nrpe-external-master}-relation-changed',
      '{interface:local-monitors}-relation-changed')
def enable_nagios():
    reactive.set_state('postgresql.nagios.enabled')


@hook('config-changed',
      '{interface:nrpe-external-master}-relation-changed',
      '{interface:local-monitors}-relation-changed')
def update_nagios():
    reactive.set_state('postgresql.nagios.needs_update')


def nagios_username():
    return '_juju_nagios'


@when('postgresql.nagios.enabled')
@when('leadership.is_leader')
@when_not('leadership.set.nagios_password')
def ensure_nagios_credentials():
    leadership.leader_set(nagios_password=host.pwgen())


@when('postgresql.nagios.enabled')
@when('postgresql.cluster.is_running')
@when('postgresql.replication.is_master')
@when('leadership.set.nagios_password')
@only_once
def ensure_nagios_user():
    con = postgresql.connect()
    postgresql.ensure_user(con, nagios_username(),
                           leadership.leader_get('nagios_password'))
    con.commit()


def nagios_pgpass_path():
    return os.path.expanduser('~nagios/.pgpass')


@when('leadership.changed.nagios_password')
def update_nagios_pgpass():
    if not os.path.isdir(os.path.expanduser('~nagios')):
        return  # Nagios user does not yet exist. Wait for subordinate.
    leader = context.Leader()
    nagios_password = leader['nagios_password']
    content = '*:*:*:{}:{}'.format(nagios_username(), nagios_password)
    helpers.write(nagios_pgpass_path(), content,
                  mode=0o600, user='nagios', group='nagios')


@when('postgresql.nagios.needs_update')
@when('leadership.set.nagios_password')
def update_nrpe_config():
    update_nagios_pgpass()
    nrpe = NRPE()

    user = nagios_username()
    port = postgresql.port()
    nrpe.add_check(shortname='pgsql',
                   description='Check pgsql',
                   check_cmd='check_pgsql -P {} -l {}'.format(port, user))

    # TODO: These should be calcualted from the backup schedule,
    # which is difficult since that is specified in crontab format.
    warn_age = 172800
    crit_age = 194400
    backups_log = helpers.backups_log_path()
    nrpe.add_check(shortname='pgsql_backups',
                   description='Check pgsql backups',
                   check_cmd=('check_file_age -w {} -c {} -f {}'
                              ''.format(warn_age, crit_age, backups_log)))
    nrpe.write()
    reactive.remove_state('postgresql.nagios.needs_update')
