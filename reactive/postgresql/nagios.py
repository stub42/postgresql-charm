# Copyright 2015-2016 Canonical Ltd.
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

from charms import leadership, reactive
from charms.reactive import hook, only_once, when, when_not

from reactive.postgresql import helpers
from reactive.postgresql import postgresql


@hook('nrpe-external-master-relation-changed',
      'local-monitors-relation-changed')
def enable_nagios(*dead_chickens):
    if os.path.exists('/var/lib/nagios'):
        reactive.set_state('postgresql.nagios.enabled')
        reactive.set_state('postgresql.nagios.needs_update')


@hook('upgrade-charm')
def upgrade_charm():
    reactive.set_state('postgresql.nagios.needs_update')
    reactive.remove_state('postgresql.nagios.user_ensured')


@when('postgresql.nagios.enabled')
@when('config.changed')
def update_nagios():
    reactive.set_state('postgresql.nagios.needs_update')


def nagios_username():
    return 'nagios'


@when('postgresql.nagios.enabled')
@when('leadership.is_leader')
@when_not('leadership.set.nagios_password')
def ensure_nagios_credentials():
    leadership.leader_set(nagios_password=host.pwgen())


@when('postgresql.nagios.enabled')
@when('postgresql.cluster.is_running')
@when('postgresql.replication.is_master')
@when('leadership.set.nagios_password')
@when_not('postgresql.nagios.user_ensured')
def ensure_nagios_user():
    con = postgresql.connect()
    postgresql.ensure_user(con, nagios_username(),
                           leadership.leader_get('nagios_password'))
    con.commit()
    reactive.set_state('postgresql.nagios.user_ensured')


def nagios_pgpass_path():
    return os.path.expanduser('~nagios/.pgpass')


@when('postgresql.nagios.enabled')
@when('leadership.changed.nagios_password')
def update_nagios_pgpass():
    leader = context.Leader()
    nagios_password = leader['nagios_password']
    content = '*:*:*:{}:{}'.format(nagios_username(), nagios_password)
    helpers.write(nagios_pgpass_path(), content,
                  mode=0o600, user='nagios', group='nagios')


@when('postgresql.nagios.enabled')
@when('leadership.set.nagios_password')
@only_once
def create_nagios_pgpass():
    update_nagios_pgpass()


@when('postgresql.nagios.enabled')
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
