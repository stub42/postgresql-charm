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

import glob
import os.path

from charmhelpers import context
from charmhelpers.core import hookenv, host, templating

from decorators import data_ready_action, relation_handler


@data_ready_action
def handle_syslog_relations():
    enable_syslog_relations()
    cleanup_syslog_relations()
    host.service_restart('rsyslog')


@relation_handler('syslog')
def enable_syslog_relations(rel):
    config = hookenv.config()
    postgresql_conf = config['postgresql_conf']
    # programname and log_file_prefix are extensions to the syslog
    # interface, required to successfully decode the messages.
    rel.local['log_line_prefix'] = postgresql_conf['log_line_prefix']
    rel.local['programname'] = hookenv.local_unit().replace('/', '_')
    for relinfo in rel.values():
        templating.render('rsyslog_forward.conf',
                          rsyslog_conf_path(hookenv.remote_unit()),
                          dict(rel=rel, relinfo=relinfo))


def cleanup_syslog_relations():
    # If we used a single rsyslog config file, we wouldn't need cleanup.
    wanted_files = set([])
    conf_pattern = ('/etc/rsyslog.d/juju-{}-*.conf'
                    ''.format(hookenv.local_unit().replace('/', '_')))
    existing_files = set(glob.glob(conf_pattern))
    wanted_files = set(rsyslog_conf_path(u)
                       for rel in context.Relations()['syslog']
                       for u in rel.keys())
    for unwanted in (existing_files - wanted_files):
        if os.path.isfile(unwanted):
            os.unlink(unwanted)


def rsyslog_conf_dir():
    return '/etc/rsyslog.d'


def rsyslog_conf_path(remote_unit):
    # Use both the local unit and remote unit in the config file
    # path to avoid conflicts with subordinates.
    local = hookenv.local_unit().replace('/', '_')
    remote = remote_unit.replace('/', '_')
    return os.path.join(rsyslog_conf_dir(),
                        'juju-{}-{}.conf'.format(local, remote))
