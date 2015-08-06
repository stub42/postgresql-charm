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

import clientrel
import nagios
import replication
import service
import syslogrel


SERVICE_DEFINITION = [
    dict(service='postgresql',
         required_data=[service.valid_config],
         data_ready=[service.preinstall,
                     service.configure_sources,
                     service.install_packages,
                     service.ensure_package_status,
                     service.update_kernel_settings,
                     replication.ensure_replication_credentials,
                     nagios.ensure_nagios_credentials,
                     service.appoint_master,

                     replication.wait_for_master,  # Exit if no master.

                     service.ensure_cluster,
                     service.update_pgpass,
                     service.update_pg_hba_conf,
                     service.update_pg_ident_conf,
                     service.update_postgresql_conf,
                     syslogrel.handle_syslog_relations,
                     service.request_restart,

                     service.wait_for_restart,  # Exit if cannot restart yet.

                     replication.promote_master,
                     replication.clone_master,
                     replication.update_recovery_conf,
                     service.restart_or_reload,

                     replication.ensure_replication_user,
                     replication.publish_replication_details,

                     nagios.ensure_nagios_user,

                     clientrel.publish_db_relations,
                     clientrel.ensure_db_relation_resources,

                     service.update_pg_hba_conf,  # Again, after client setup.
                     service.reload_config,

                     service.set_active,

                     # At the end, as people check the end of logs
                     # most frequently.
                     service.emit_deprecated_option_warnings],
         start=[service.open_ports],
         stop=[service.stop_postgresql, service.close_ports])]


def get_service_manager():
    return services.ServiceManager(SERVICE_DEFINITION)
