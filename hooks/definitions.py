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
from charmhelpers.core import services

import relations
import service


def get_service_definitions():
    config = hookenv.config()
    return [
        dict(service='postgresql',
             required_data=[service.valid_config],
             data_ready=[service.preinstall,
                         service.configure_sources,
                         service.install_packages,
                         service.ensure_package_status],
             provided_data=[relations.DbRelation(),
                            relations.DbAdminRelation()],
             start=[], stop=[]),

        dict(service='postgresql-post-relation',
             required_data=[service.valid_config],
             data_ready=[service.generate_hba_conf],
             ports=[config['listen_port']],
             start=[services.open_ports],
             stop=[service.stop_postgresql, services.close_ports])]


def get_service_manager():
    return services.ServiceManager(get_service_definitions())
