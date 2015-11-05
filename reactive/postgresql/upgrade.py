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

from charmhelpers import context
from charmhelpers.core import hookenv, host
from charms import reactive
from charms.reactive import hook

from reactive import leadership
from reactive import workloadstatus
from reactive.postgresql import postgresql
from reactive.postgresql import replication


@hook('upgrade-charm')
def upgrade_charm():
    workloadstatus.status_set('maintenance', 'Upgrading charm')

    rels = context.Relations()

    # The master is now appointed by the leader.
    if hookenv.is_leader():
        master = replication.get_master()
        if not master:
            master = hookenv.local_unit()
            if rels.peer:
                for peer_relinfo in rels.peer.values():
                    if peer_relinfo.get('state') == 'master':
                        master = peer_relinfo.unit
                        break
            hookenv.log('Discovered {} is the master'.format(master))
            leadership.leader_set(master=master)

    # The name of this crontab has changed. It will get regenerated.
    if os.path.exists('/etc/cron.d/postgresql'):
        hookenv.log('Removing old crontab')
        os.unlink('/etc/cron.d/postgresql')

    # Older generated usernames where generated from the relation id,
    # and really old ones contained random components. This made it
    # problematic to restore a database into a fresh environment,
    # because the new usernames would not match the old usernames and
    # done of the database permissions would match. We now generate
    # usernames using just the client service name, so restoring a
    # database into a fresh environment will work provided the service
    # names match. We want to update the old usernames in upgraded
    # services to the new format to improve their disaster recovery
    # story.
    for relname, superuser in [('db', False), ('db-admin', True)]:
        for client_rel in rels[relname].values():
            hookenv.log('Migrating database users for {}'.format(client_rel))
            password = client_rel.local.get('password', host.pwgen())
            old_username = client_rel.local.get('user')
            new_username = postgresql.username(client_rel.service,
                                               superuser, False)
            if old_username and old_username != new_username:
                migrate_user(old_username, new_username, password, superuser)
                client_rel.local['user'] = new_username
                client_rel.local['password'] = password

            old_username = client_rel.local.get('schema_user')
            if old_username and old_username != new_username:
                migrate_user(old_username, new_username, password, superuser)
                client_rel.local['schema_user'] = new_username
                client_rel.local['schema_password'] = password

    # Admin relations used to get 'all' published as the database name,
    # which was bogus.
    for client_rel in rels['db-admin'].values():
        if client_rel.local.get('database') == 'all':
            client_rel.local['database'] = client_rel.service

    # Reconfigure PostgreSQL and republish client relations.
    reactive.remove_state('postgresql.cluster.configured')
    reactive.remove_state('postgresql.client.published')

    # Set the postgresql.replication.cloned flag, so we don't rebuild
    # standbys when upgrading the charm from a pre-reactive version.
    reactive.set_state('postgresql.replication.cloned')


def migrate_user(old_username, new_username, password, superuser=False):
    if postgresql.is_primary():
        # We do this on any primary, as the master is
        # appointed later. It also works if we have
        # a weird setup with manual_replication and
        # multiple primaries.
        con = postgresql.connect()
        postgresql.ensure_user(con, new_username, password,
                               superuser=superuser)
        cur = con.cursor()
        hookenv.log('Granting old role {} to new role {}'
                    ''.format(old_username, new_username))
        cur.execute('GRANT %s TO %s',
                    (postgresql.pgidentifier(old_username),
                        postgresql.pgidentifier(new_username)))
        con.commit()
    else:
        hookenv.log('Primary must map role {!r} to {!r}'
                    ''.format(old_username, new_username))
