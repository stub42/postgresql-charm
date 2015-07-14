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

    def __init__(self):
        self.local = {}
        self.remote = {}
        self.master = {}
        self.relid = {}
        master_unit = postgresql.master()
        if master_unit is not None:
            for relid in hookenv.relation_ids(self.name):
                remote_unit = hookenv.related_units(relid)[0]
                remote_service = remote_unit.split('/', 1)[0]
                local_rel = hookenv.relation_get(rid=relid,
                                                 unit=hookenv.local_unit())
                remote_rel = hookenv.relation_get(rid=relid,
                                                  unit=remote_unit)
                master_rel = hookenv.relation_get(rid=relid,
                                                  unit=master_unit)
                self.local[remote_service] = local_rel
                self.remote[remote_service] = remote_rel
                self.master[remote_service] = master_rel
                self.relid[remote_service] = relid

    def provide_data(self, remote_service, service_ready):
        if not service_ready:
            return {}

        hookenv.log('** Providing {} ({})'.format(remote_service,
                                                  self.relid[remote_service]))
        data = self.local[remote_service]
        data.update(self.global_data())
        data.update(self.service_data(remote_service))
        data.update(self.unit_data(remote_service))
        return data

    def global_data(self):
        return dict(version=postgresql.version())

    def service_data(self, remote_service):
        # Copy the relevant keys from the master (possibly ourself)
        service_keys = frozenset(['user', 'password', 'roles',
                                  'schema_user', 'schema_password',
                                  'database', 'extensions'])
        return dict((k, v) for k, v in self.master[remote_service].items()
                    if k in service_keys)

    def unit_data(self, remote_service):
        relid = self.relid[remote_service]

        # We allowed access to all related units when we regenerated
        # pg_hba.conf
        allowed_units = ' '.join(sorted(hookenv.related_units(relid)))

        # Calulate the state of this unit. standalone will disappear
        # in a future version of this interface, as this state was
        # only needed to deal with race conditions now solved by
        # Juju leadership.
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

    def ensure_db_resources(self, remote_service):
        # A data_ready handler will invoke this on the master,
        # ensuring that the necessary credentials and requested
        # database environment get setup.
        assert postgresql.is_master(), 'Not the master'

        remote_data = self.remote[remote_service]

        # We update the master data in this instance, and it will later
        # be returned by provide_data() for publishing to the relation.
        master_data = self.master[remote_service]

        # The requested database name, the existing database name,
        # or use the remote service name as the database name.
        if 'database' in remote_data:
            master_data['database'] = remote_data['database']
        elif 'database' not in master_data:
            master_data['database'] = remote_service
        postgresql.ensure_database(master_data['database'])

        # The rest of resource creation can be done in a transaction.
        con = postgresql.connect(database=master_data['database'])

        # Ensure requested extensions have been created in the database.
        master_data['extensions'] = remote_data.get('extensions')  # Reflect
        if master_data['extensions']:
            extensions = filter(None, master_data['extensions'].split(','))
            postgresql.ensure_extensions(con, extensions)

        # Generate credentials if they don't already exist.
        if 'user' not in master_data:
            master_data['user'] = postgresql.username(remote_service,
                                                      superuser=self.superuser)
            master_data['password'] = host.pwgen()
            postgresql.ensure_user(con,
                                   master_data['user'],
                                   master_data['password'],
                                   superuser=self.superuser)

            # schema_user has never been documented and is deprecated.
            master_data['schema_user'] = master_data['user'] + '_schema'
            master_data['schema_password'] = host.pwgen()
            postgresql.ensure_user(con,
                                   master_data['schema_user'],
                                   master_data['schema_password'])

        # Reset the roles granted to the user as requested.
        master_data['roles'] = remote_data.get('roles')  # Reflect back.
        if master_data['roles'] is not None:
            roles = filter(None, master_data['roles'].split(','))
            postgresql.reset_user_roles(con, master_data['user'], roles)

        # Grant specified privileges on the database to the user.
        # This comes from the PostgreSQL service configuration, as
        # allowing the relation to specify how much access it gets
        # is insecure.
        config = hookenv.config()
        privs = set(config['relation_database_privileges'].split(','))
        postgresql.grant_database_privileges(con,
                                             master_data['user'],
                                             master_data['database'],
                                             privs)
        postgresql.grant_database_privileges(con,
                                             master_data['schema_user'],
                                             master_data['database'],
                                             privs)
        con.commit()
        return master_data


class DbAdminRelation(DbRelation):
    name = 'db-admin'
    superuser = True
