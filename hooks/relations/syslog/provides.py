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

from charmhelpers.core import hookenv, host, templating

from charms import reactive
from charms.reactive import hook, not_unless, when


class SyslogProvides(reactive.relations.RelationBase):

    @hook('{provides:syslog}-relation-changed')
    def changed(self):
        self.set_state('{relation_name}.available')

    @hook('{provides:syslog}-relation-departed')
    def departed(self):
        path = self.get_local('path')
        if os.path.exists(path):
            os.remove(path)
            reactive.set_state('syslog.needs_restart')
        self.remove_state('{relation_name}.available')
        self.depart()

    def _rsyslog_conf_path(self, remote_unit):
        # Use both the local unit and remote unit in the config file
        # path to avoid conflicts with subordinates.
        rsyslog_conf_dir = '/etc/rsyslog.d'
        local = hookenv.local_unit().replace('/', '_')
        remote = remote_unit.replace('/', '_')
        return os.path.join(rsyslog_conf_dir,
                            'juju-{}-{}.conf'.format(local, remote))

    @not_unless('{provides:syslog}.available')
    def configure(self, programname):
        for conv in self.conversations():
            remote_unit = conv.scope
            remote_addr = conv.get_remote('private-address')

            if reactive.helpers.data_changed('syslog.{}'.format(remote_unit),
                                             (remote_unit, remote_addr)):
                path = self._rsyslog_conf_path(remote_unit)

                templating.render('rsyslog_forward.conf', path,
                                  dict(local_unit=hookenv.local_unit(),
                                       remote_unit=remote_unit,
                                       remote_addr=remote_addr,
                                       programname=programname))
                reactive.set_state('syslog.needs_restart')


@when('syslog.needs_restart')
def restart_rsyslog():
    host.service_restart('rsyslog')
    reactive.remove_state('syslog.needs_restart')
