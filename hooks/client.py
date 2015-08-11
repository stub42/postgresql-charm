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

from decorators import relation_handler, master_only
import helpers
import postgresql


@relation_handler('db', 'db-admin')
def publish_db_relations(rel):
    if postgresql.is_master():
        superuser = (rel.relname == 'db-admin')
        db_relation_master(rel, superuser=superuser)
    else:
        db_relation_mirror(rel)
    db_relation_common(rel)


def db_relation_master(rel, superuser):
    '''The master generates credentials and negotiates resources.'''
    master = rel.local
    # Pick one remote unit as representative. They should all converge.
    for remote in rel.values():
        break

    # The requested database name, the existing database name, or use
    # the remote service name as a default. We no longer use the
    # relation id for the database name or usernames, as when a
    # database dump is restored into a new Juju environment we
    # are more likely to have matching service names than relation ids
    # and less likely to have to perform manual permission and ownership
    # cleanups.
    if 'database' in remote:
        master['database'] = remote['database']
    elif 'database' not in master:
        master['database'] = remote.service

    if 'user' not in master:
        user = postgresql.username(remote.service, superuser=superuser)
        master['user'] = user
        master['password'] = host.pwgen()

        # schema_user has never been documented and is deprecated.
        master['schema_user'] = user + '_schema'
        master['schema_password'] = host.pwgen()

    hookenv.log('** Master providing {} ({}/{})'.format(rel,
                                                        master['database'],
                                                        master['user']))

    # Reflect these settings back so the client knows when they have
    # taken effect.
    master['roles'] = remote.get('roles')
    master['extensions'] = remote.get('extensions')


def db_relation_mirror(rel):
    '''Non-masters mirror relation information from the master.'''
    master = postgresql.master()
    master_keys = ['database', 'user', 'password', 'roles',
                   'schema_user', 'schema_password', 'extensions']
    master_info = rel.peers.get(master)
    if master_info is None:
        hookenv.log('Waiting for {} to join {}'.format(rel))
        return
    hookenv.log('Mirroring {} database credentials from {}'.format(rel,
                                                                   master))
    rel.local.update({k: master_info.get(k) for k in master_keys})


def db_relation_common(rel):
    '''Publish unit specific relation details.'''
    local = rel.local
    if 'database' not in local:
        return  # Not yet ready.

    # Version number, allowing clients to adjust or block if their
    # expectations are not met.
    local['version'] = postgresql.version()

    # Calculate the state of this unit. 'standalone' will disappear
    # in a future version of this interface, as this state was
    # only needed to deal with race conditions now solved by
    # Juju leadership.
    if postgresql.is_primary():
        if hookenv.is_leader() and len(helpers.peers()) == 0:
            local['state'] = 'standalone'
        else:
            local['state'] = 'master'
    else:
        local['state'] = 'hot standby'

    # Host is the private ip address, but this might change and
    # become the address of an attached proxy or alternative peer
    # if this unit is in maintenance.
    local['host'] = hookenv.unit_private_ip()

    # Port will be 5432, unless the user has overridden it or
    # something very weird happened when the packages where installed.
    local['port'] = str(postgresql.port())

    # The list of remote units on this relation granted access.
    # This is to avoid the race condition where a new client unit
    # joins an existing client relation and sees valid credentials,
    # before we have had a chance to grant it access.
    local['allowed-units'] = ' '.join(rel.keys())


@master_only
@relation_handler('db', 'db-admin')
def ensure_db_relation_resources(rel):
    '''Create the database resources needed for the relation.'''
    superuser = (rel.relname == 'db-admin')
    master = rel.local

    hookenv.log('Ensuring database {!r} and user {!r} exist for {}'
                ''.format(master['database'], master['user'], rel))

    # First create the database, if it isn't already.
    postgresql.ensure_database(master['database'])

    # Next, connect to the database to create the rest in a transaction.
    con = postgresql.connect(database=master['database'])

    postgresql.ensure_user(con, master['user'], master['password'],
                           superuser=superuser)
    postgresql.ensure_user(con,
                           master['schema_user'], master['schema_password'])

    # Grant specified privileges on the database to the user. This comes
    # from the PostgreSQL service configuration, as allowing the
    # relation to specify how much access it gets is insecure.
    config = hookenv.config()
    privs = set(filter(None,
                       config['relation_database_privileges'].split(',')))
    postgresql.grant_database_privileges(con, master['user'],
                                         master['database'], privs)
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
