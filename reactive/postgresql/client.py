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

import json

from charmhelpers import context
from charmhelpers.core import hookenv, host
from charms import leadership, reactive
from charms.reactive import hook, not_unless, when, when_any, when_not

from reactive.postgresql import postgresql


CLIENT_RELNAMES = frozenset(['db', 'db-admin', 'master'])


@hook('leader-elected')
def leader_elected():
    reactive.remove_state('postgresql.client.leader_published')


@when_any('db.connected', 'db-admin.conected', 'master.connected')
def client_relation_changed(*ignored):
    reactive.remove_state('postgresql.client.leader_published')


@hook('{interface:pgsql}-relation-changed', 'config-changed')
def update_client_conninfo(*ignored):
    reactive.remove_state('postgresql.client.clients_published')


@when('leadership.changed.client_conninfo')
def leader_conninfo_changed():
    reactive.remove_state('postgresql.client.clients_published')


def get_conninfos():
    return json.loads(leadership.leader_get('client_conninfo') or '{}')


def set_conninfos(conninfos):
    leadership.leader_set(client_conninfo=json.dumps(conninfos))


@when('leadership.is_leader')
@when_not('postgresql.client.leader_published')
def publish_leader_conninfo():
    '''Leader generates usernames and passwords.

    Note that this may not be happening in a relation hook, as no
    unit may have been leader when the relation hooks were last run.
    '''
    conninfos = get_conninfos()
    rels = context.Relations()
    relids = set()
    for relname in CLIENT_RELNAMES:
        # Master declares common connection details.
        for rel in rels[relname].values():
            if not rel:
                continue
            relids.add(rel.relid)
            superuser, replication = _credential_types(rel)
            user = postgresql.username(rel.service,
                                       superuser=superuser,
                                       replication=replication)

            # Use the requested database name, or the existing database
            # from the relation for backwards compatibility. Otherwise,
            # default to the service name. We no longer use the
            # relation id for the database name or usernames, as when a
            # database dump is restored into a new Juju environment we
            # are more likely to have matching service names than relation
            # ids and less likely to have to perform manual permission and
            # ownership cleanups.
            for remote in rel.values():
                break
            if 'database' in remote:
                database = remote['database']
            elif 'database' in rel.local:
                database = rel.local['database']
            else:
                database = remote.service

            hookenv.log('Publishing leader connection info for {}'
                        ''.format(rel.relid))
            conninfos[rel.relid] = dict(user=user, password=host.pwgen(),
                                        database=database)

    # Remove conninfos for relations that no longer exist.
    for relid in set(conninfos.keys()):
        if relid not in relids:
            del conninfos[relid]

    set_conninfos(conninfos)
    reactive.set_state('postgresql.client.leader_published')


def _credential_types(rel):
    superuser = (rel.relname in ('db-admin', 'master'))
    replication = (rel.relname == 'master')
    return (superuser, replication)


@when('postgresql.cluster.configured')
@when_not('postgresql.client.clients_published')
def publish_client_conninfos():
    conninfos = get_conninfos()
    rels = context.Relations()
    for relname in CLIENT_RELNAMES:
        for rel in rels[relname].values():
            if rel.relid not in conninfos:
                hookenv.log('Connection info for {} not yet available'
                            ''.format(rel.relid))
                continue
            publish_client_conninfo(rel, conninfos[rel.relid])
            reactive.remove_state('postgresql.client.resources_ensured')
    reactive.set_state('postgresql.client.clients_published')


def publish_client_conninfo(rel, conninfo):
    hookenv.log('Publishing client connection info to {}'.format(rel.relid))
    local = rel.local
    for remote in rel.values():
        break

    # Username, password and database from the leader settings.
    local['user'] = conninfo['user']
    local['password'] = conninfo['password']
    local['database'] = conninfo['database']

    # schema_user and schema_password are deprecated and will one
    # day be removed.
    local['schema_user'] = conninfo['user']
    local['schema_password'] = conninfo['password']

    # Roles to be granted to generated users, requested by the client.
    local['roles'] = remote.get('roles')

    # Extensions to add to the database, requested by the client. This
    # likely needs packages installed via the extra_packages configuration
    # setting to work.
    local['extensions'] = remote.get('extensions')

    # Version number, allowing clients to adjust or block if their
    # expectations are not met.
    local['version'] = postgresql.version()

    # Calculate the state of this unit. 'standalone' will disappear
    # in a future version of this interface, as this state was
    # only needed to deal with race conditions now solved by
    # Juju leadership. We check for is_primary() rather than
    # the postgresql.replication.is_master reactive state to
    # publish the correct state when we are using manual replication
    # (there might be multiple independent masters, possibly useful for
    # sharding, or perhaps this is a multi master BDR setup).
    if postgresql.is_primary():
        if reactive.helpers.is_state('postgresql.replication.has_peers'):
            local['state'] = 'master'
        else:
            local['state'] = 'standalone'
    else:
        local['state'] = 'hot standby'

    # Host is the private ip address, but this might change and
    # become the address of an attached proxy or alternative peer
    # if this unit is in maintenance.
    local['host'] = hookenv.unit_private_ip()

    # Port will be 5432, unless the user has overridden it or
    # something odd happened when the packages where installed.
    local['port'] = str(postgresql.port())

    # The list of remote units on this relation granted access.
    # This is to avoid the race condition where a new client unit
    # joins an existing client relation and sees valid credentials,
    # before we have had a chance to grant it access.
    local['allowed-units'] = ' '.join(unit for unit, relinfo in rel.items()
                                      if 'private-address' in relinfo)


@when('postgresql.cluster.configured')
@when('postgresql.cluster.is_running')
@when('postgresql.replication.is_primary')
@when('postgresql.client.clients_published')
@when_not('postgresql.client.resources_ensured')
def ensure_resources():
    rels = context.Relations()
    for relname in CLIENT_RELNAMES:
        for rel in rels[relname].values():
            if 'database' not in rel.local:
                continue  # Should not happen?
            ensure_db_relation_resources(rel)
    reactive.set_state('postgresql.client.resources_ensured')


@not_unless('postgresql.replication.is_primary')
def ensure_db_relation_resources(rel):
    '''Create the database resources needed for the relation.'''

    master = rel.local

    hookenv.log('Ensuring database {!r} and user {!r} exist for {}'
                ''.format(master['database'], master['user'], rel))

    # First create the database, if it isn't already.
    postgresql.ensure_database(master['database'])

    # Next, connect to the database to create the rest in a transaction.
    con = postgresql.connect(database=master['database'])

    superuser, replication = _credential_types(rel)
    postgresql.ensure_user(con, master['user'], master['password'],
                           superuser=superuser, replication=replication)
    if not superuser:
        postgresql.ensure_user(con,
                               master['schema_user'],
                               master['schema_password'])

    # Grant specified privileges on the database to the user. This comes
    # from the PostgreSQL service configuration, as allowing the
    # relation to specify how much access it gets is insecure.
    config = hookenv.config()
    privs = set(filter(None,
                       config['relation_database_privileges'].split(',')))
    postgresql.grant_database_privileges(con, master['user'],
                                         master['database'], privs)
    if not superuser:
        postgresql.grant_database_privileges(con, master['schema_user'],
                                             master['database'], privs)

    # Reset the roles granted to the user as requested.
    if 'roles' in master:
        roles = filter(None, master.get('roles', '').split(','))
        postgresql.grant_user_roles(con, master['user'], roles)

    # Create requested extensions. We never drop extensions, as there
    # may be dependent objects.
    if 'extensions' in master:
        extensions = filter(None, master.get('extensions', '').split(','))
        postgresql.ensure_extensions(con, extensions)

    con.commit()  # Don't throw away our changes.
