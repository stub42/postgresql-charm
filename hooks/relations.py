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

from charmhelpers.core import hookenv, host

import helpers
import postgresql


class DbRelation:
    name = 'db'
    superuser = False

    def provide_data(self, remote_service, service_ready):
        if not service_ready:
            return dict()

        data = dict()
        data.update(self.global_data())
        data.update(self.service_data(remote_service))
        data.update(self.unit_data(remote_service))
        return data

    def relid(self, remote_service):
        for relid in hookenv.relation_ids(self.name):
            units = hookenv.related_units(relid)
            if units and units[0].split('/', 1)[0] == remote_service:
                return relid
        return None

    def global_data(self):
        return dict(version=postgresql.version())

    def service_data(self, remote_service):
        # The master is responsible for creating accounts and generating
        # credentials.
        if postgresql.is_master():
            self._update_service_data(remote_service)
        return self._master_service_data(remote_service)

    def unit_data(self, remote_service):
        relid = self.relid(remote_service)
        allowed_units = ' '.join(sorted(hookenv.related_units(relid)))
        if postgresql.is_primary():
            if hookenv.is_leader() and len(helpers.peers()) == 0:
                state = 'standalone'
            else:
                state = 'master'
        else:
            state = 'hot standby'
        return {'allowed-units': allowed_units,
                'host': hookenv.unit_private_ip(),
                'port': postgresql.port(),
                'state': state}

    def _master_service_data(self, remote_service):
        service_keys = frozenset(['user', 'password', 'roles', 'database',
                                  'schema_user', 'schema_password'])
        relid = self.relid(remote_service)
        master = postgresql.master()
        full_master_data = hookenv.relation_get(unit=master, rid=relid)
        return dict((k, v) for k, v in full_master_data.items()
                    if k in service_keys)

    def _update_service_data(self, remote_service):
        master_data = self._master_service_data(remote_service)
        relid = self.relid(remote_service)
        remote_unit = sorted(hookenv.related_units(relid))[0]
        remote_data = hookenv.relation_get(unit=remote_unit, rid=relid)

        # Ensure the requested database exists, or provide one
        # named after the remote service.
        if 'database' in remote_data:
            master_data['database'] = remote_data['database']
        elif 'database' not in master_data:
            # Older versions of the charm have different database names,
            # so don't override the existing setting if it exists.
            master_data['database'] = remote_service
        postgresql.ensure_database(master_data['database'])

        # Generate credentials if they don't already exist.
        if 'user' not in master_data:
            master_data['user'] = postgresql.username(remote_service,
                                                      self.superuser)
            master_data['password'] = host.pwgen()
            master_data['schema_user'] = master_data['user'] + '_schema'
            master_data['schema_password'] = host.pwgen()

            postgresql.ensure_user(master_data['user'],
                                   master_data['password'],
                                   superuser=self.superuser)
            postgresql.ensure_user(master_data['schema_user'],
                                   master_data['schema_password'])

        # Reset the roles granted to the user as requested.
        postgresql.reset_user_roles(master_data['user'],
                                    remote_data.get('roles', []))

        # Ensure the user can connect to the db.
        postgresql.grant_database_privilege('CONNECT',
                                            master_data['database'],
                                            master_data['user'])

        # Grant the schema_user all privileges, which is dangerous
        # so we should deprecate this interface.
        postgresql.grant_database_privilege('ALL PRIVILEGES',
                                            master_data['database'],
                                            master_data['schema_user'])
        return master_data


class DbAdminRelation(DbRelation):
    name = 'db-admin'
    superuser = True
