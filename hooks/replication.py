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

from functools import wraps
import os.path
import shutil
import subprocess

import psycopg2

from charmhelpers import context
from charmhelpers.core import host, hookenv, templating
from charmhelpers.core.hookenv import DEBUG, ERROR, WARNING

from coordinator import coordinator
from decorators import leader_only, master_only, not_master
import helpers
import postgresql
import wal_e


def replication_username():
    # Leading underscore for 'system' accounts, to avoid an unlikely
    # conflict with a client service named 'repl'.
    return '_juju_repl'


def replication_data_ready_action(func):
    '''A replication specific data_ready action wrapper.

    Action skipped when the manual_replication config option is True
    or if we are not ready to deal with replication.
    '''
    @wraps(func)
    def wrapper(service_name):
        if hookenv.config()['manual_replication']:
            hookenv.log('Manual replication - skipping')
            return

        peer_rel = context.Relations().peer
        if peer_rel is None:
            hookenv.log('Not yet joined peer relation - skipping')
            return

        if 'following' in peer_rel.local:
            # Master was available, replication working or in failover.
            return func()

        master = postgresql.master()
        if master in peer_rel or master == hookenv.local_unit():
            # Master is available.
            return func()

        hookenv.log('Not yet joined peer relation with {} - skipping'
                    ''.format(postgresql.master()))
        return
    return wrapper


@leader_only
@replication_data_ready_action
def ensure_replication_credentials():
    leader = context.Leader()
    if 'replication_password' not in leader:
        leader['replication_password'] = host.pwgen()


@master_only
@replication_data_ready_action
def ensure_replication_user():
    leader = context.Leader()
    con = postgresql.connect()
    postgresql.ensure_user(con, replication_username(),
                           leader['replication_password'],
                           replication=True)
    con.commit()


@replication_data_ready_action
def publish_replication_details():
    peer = context.Relations().peer
    if peer is not None:
        peer.local['host'] = hookenv.unit_private_ip()
        peer.local['port'] = str(postgresql.port())
        peer.local['allowed-units'] = ' '.join(sorted(peer.keys()))


def following():
    peer_rel = context.Relations().peer
    if peer_rel:
        return peer_rel.local.get('following')
    return None


def needs_clone():
    if postgresql.is_master():
        return False
    peer_rel = context.Relations().peer
    if peer_rel is None or not peer_rel.local.get('following'):
        return True


def wait_for_master_auth():
    '''Wait until the master has authorized us.

    If not, the unit is put into 'waiting' state and the hook exits.
    '''
    # Check if the master has listed this unit as allowed.
    master = postgresql.master()
    master_relinfo = context.Relations().peer[master]
    allowed = master_relinfo.get('allowed-units', '').split()

    # If we are running upgrade-charm, this standby might be running the
    # upgrade-charm hook before the master, and the connection details
    # not yet available. It may remain this way for several hooks, while
    # siblings trigger peer relation hooks and whatnot. So we need to
    # handle this case here. If the master has not yet published its
    # connection details, it is the equivalent of not allowed.
    master_creds_available = ('host' in master_relinfo)

    if hookenv.local_unit() in allowed and master_creds_available:
        return
    helpers.status_set('waiting',
                       'Waiting for master {} to authorize'.format(master))
    raise SystemExit(0)


@not_master
@replication_data_ready_action
def clone_master():
    if not needs_clone():
        hookenv.log('Does not need cloning')
        return

    master = postgresql.master()
    peer_rel = context.Relations().peer
    local_relinfo = peer_rel.local
    master_relinfo = peer_rel[master]

    assert not postgresql.is_running()

    # If this unit joined during failover, then we might not have yet
    # joined the peer relation. Wait until the master is available before
    # attempting to clone it.
    wait_for_master_auth()  # Terminates if the master is not yet available.

    data_dir = postgresql.data_dir()
    if os.path.exists(data_dir):
        hookenv.log('Removing {} in preparation for clone'.format(data_dir))
        shutil.rmtree(data_dir)
    helpers.makedirs(data_dir, mode=0o700, user='postgres', group='postgres')

    cmd = ['sudo', '-H',  # -H needed to locate $HOME/.pgpass
           '-u', 'postgres', 'pg_basebackup',
           '-D', postgresql.data_dir(),
           '-h', master_relinfo['host'],
           '-p', master_relinfo['port'],
           '--checkpoint=fast', '--progress',
           '--no-password', '--username=_juju_repl']
    if postgresql.has_version('9.2'):
        cmd.append('--xlog-method=stream')
    else:
        cmd.append('--xlog')
    hookenv.log('Cloning {} with {}'.format(master, ' '.join(cmd)))
    helpers.status_set('maintenance', 'Cloning {}'.format(master))
    try:
        # Switch to a directory the postgres user can access.
        with helpers.switch_cwd('/tmp'):
            subprocess.check_call(cmd, universal_newlines=True)
        local_relinfo['following'] = master
    except subprocess.CalledProcessError as x:
        hookenv.log('Clone failed with {}'.format(x), ERROR)
        # We failed, and the local cluster is broken.
        helpers.status_set('blocked', 'Failed to clone {}'.format(master))
        postgresql.drop_cluster()
        raise SystemExit(0)

    ensure_ssl_certs()


def ensure_ssl_certs():
    if postgresql.has_version('9.2'):
        hookenv.log('Nothing to do with PostgreSQL {}'
                    ''.format(postgresql.version()))
        return

    # Ensure the SSL certificates exist in $DATA_DIR, where PostgreSQL
    # expects to find them.
    data_dir = postgresql.data_dir()
    server_crt = os.path.join(data_dir, 'server.crt')
    server_key = os.path.join(data_dir, 'server.key')
    if not os.path.exists(server_crt):
        hookenv.log('Linking snakeoil certificate')
        os.symlink('/etc/ssl/certs/ssl-cert-snakeoil.pem', server_crt)
    if not os.path.exists(server_key):
        hookenv.log('Linking snakeoil key')
        os.symlink('/etc/ssl/private/ssl-cert-snakeoil.key', server_key)
    hookenv.log('SSL certificates exist', DEBUG)


@master_only
@replication_data_ready_action
def promote_master():
    if postgresql.is_secondary():
        hookenv.log("I've been promoted to master", WARNING)
        postgresql.promote()
        rels = context.Relations()
        del rels.peer.local['following']
    else:
        hookenv.log("I'm already master and remaining so.", DEBUG)

    # Update the cached copy used to detect changes.
    hookenv.config()['recovery_conf'] = None


@not_master
@replication_data_ready_action
def update_recovery_conf():
    master = postgresql.master()
    path = postgresql.recovery_conf_path()

    peer_rel = context.Relations().peer
    master_relinfo = peer_rel.get(master)
    if master_relinfo is None:
        # This pathalogical case should only happen when a new unit
        # is added during failover. The new master may be appointed by
        # the leader before this unit has joined the peer relation with
        # the new master.
        hookenv.log('Waiting for new master {} to join peer relation'
                    ''.format(master))
        return

    following = peer_rel.local.get('following')
    leader = context.Leader()
    config = hookenv.config()

    if master != following:
        hookenv.log('Following new master {} (was {})'.format(master,
                                                              following))
    else:
        # Even though the master is unchanged, we still regenerate
        # recovery.conf in case connection details such as IP addresses
        # have changed.
        hookenv.log('Continuing to follow {}'.format(master))

    data = dict(streaming_replication=config['streaming_replication'],
                host=master_relinfo['host'],
                port=master_relinfo['port'],
                user=replication_username(),
                password=leader['replication_password'])
    if wal_e.wal_e_enabled():
        data['restore_command'] = wal_e.wal_e_restore_command()
    templating.render('recovery.conf.tmpl', path, data,
                      owner='postgres', group='postgres',
                      perms=0o600)

    # We stuff a copy into the config to easily tell if it has changed
    # and we need to restart.
    with open(path, 'r') as f:
        config['recovery_conf'] = f.read()
        if config.changed('recovery_conf'):
            coordinator.acquire('restart')


def elect_master():
    '''Elect a new master after the old one has departed.

    The new master is the secondary that has received the most
    WAL data. There must be no hot standbys still replicating
    data from the previous master, or we may end up with diverged
    timelines.
    '''
    rel = context.Relations().peer
    assert rel is not None, 'Attempting to elect master with no peer rel'

    local_unit = hookenv.local_unit()

    # The unit with the most advanced WAL offset should be the new master.
    if postgresql.is_running():
        local_offset = postgresql.wal_received_offset(postgresql.connect())
        offsets = [(local_offset, local_unit)]
    else:
        offsets = []

    for unit, relinfo in rel.items():
        try:
            con = postgresql.connect(user=replication_username(), unit=unit)
            offsets.append((postgresql.wal_received_offset(con), unit))
        except (psycopg2.Error, postgresql.InvalidConnection) as x:
            hookenv.log('Unable to query replication state of {}: {}'
                        ''.format(unit, x), WARNING)
            # TODO: Signal re-cloning required. Or autodetect
            # based on timeline switch. Or PG9.3+ could use pg_rewind.

    offsets.sort()
    if not offsets:
        # This should only happen if we failover before replication has
        # been setup, like a test suite destroying units without waiting
        # for the initial deployment to complete.
        helpers.status_set('blocked', 'No candidates for master found!')
        raise SystemExit(0)
    elected_master = offsets[0][1]
    return elected_master
