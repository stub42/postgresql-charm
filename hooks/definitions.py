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

import client
import nagios
import replication
import service
import storage
import syslogrel
import wal_e


SERVICE_DEFINITION = [
    dict(service='postgresql',
         required_data=[service.valid_config],
         data_ready=[service.preinstall,
                     service.configure_sources,
                     service.install_packages,
                     service.ensure_package_status,
                     service.update_kernel_settings,
                     service.appoint_master,
                     nagios.ensure_nagios_credentials,
                     replication.ensure_replication_credentials,
                     replication.publish_replication_details,

                     # Exit if required leader settings are not set.
                     service.wait_for_leader,

                     service.ensure_cluster,
                     service.update_pgpass,
                     service.update_pg_hba_conf,
                     service.update_pg_ident_conf,
                     service.update_postgresql_conf,
                     syslogrel.handle_syslog_relations,
                     storage.handle_storage_relation,
                     wal_e.update_wal_e_env_dir,
                     service.request_restart,

                     service.wait_for_restart,  # Exit if cannot restart yet.

                     replication.promote_master,
                     storage.remount,
                     replication.clone_master,  # Exit if cannot clone yet.
                     replication.update_recovery_conf,
                     service.restart_or_reload,

                     replication.ensure_replication_user,
                     nagios.ensure_nagios_user,
                     service.install_administrative_scripts,
                     service.update_postgresql_crontab,

                     client.publish_db_relations,
                     client.ensure_db_relation_resources,

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
