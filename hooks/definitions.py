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

from charmhelpers.core import services

import relations
import service


SERVICE_DEFINITION = [
    dict(service='postgresql',
         required_data=[service.valid_config,
                        service.has_master],
         data_ready=[service.preinstall,
                     service.configure_sources,
                     service.install_packages,
                     service.ensure_package_status,
                     service.update_kernel_settings,
                     service.ensure_cluster,
                     service.appoint_master,
                     relations.publish_db_relations,
                     relations.ensure_db_relation_resources,
                     service.update_pg_ident_conf,
                     service.update_pg_hba_conf,
                     service.update_postgresql_conf,
                     service.request_restart,
                     service.reload_or_restart,
                     service.set_active],
         start=[service.open_ports],
         stop=[service.stop_postgresql, service.close_ports])]


def get_service_manager():
    return services.ServiceManager(SERVICE_DEFINITION)
