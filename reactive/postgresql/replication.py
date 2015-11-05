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
import shutil
import subprocess
import time

import psycopg2

from charmhelpers import context
from charmhelpers.core import host, hookenv, templating
from charmhelpers.core.hookenv import DEBUG, ERROR, WARNING
from charms import reactive
from charms.reactive import hook, not_unless, when, when_not, when_file_changed

from reactive.leadership import leader_get, leader_set
from reactive.workloadstatus import status_set
from reactive.postgresql import helpers
from reactive.postgresql import postgresql
from reactive.postgresql import wal_e


@hook
def set_replication_state():
    '''
    Called for every hook. Set the following states appropriately:

        postgresql.replication.has_peers

            This unit has peers.

        postgresql.replication.had_peers

            This unit once had peers, but may not any more.

        postgresql.replication.master.peered

            This unit is peered with the master. It is not the master.

        postgresql.replication.master.authorized

            This unit is peered with and authorized by the master. It is
            not the master.

        postgresql.replication.is_master

            This unit is the master.

        postgresql.replication.has_master

            This unit is the master, or it is peered with and
            authorized by the master.

        postgresql.replication.cloned

            This unit is on the master's timeline. It has been cloned from
            the master, or is the master.
    '''
    peer_relid = hookenv.peer_relation_id()
    peers = hookenv.related_units(peer_relid)
    reactive.toggle_state('postgresql.replication.has_peers', bool(peers))
    if peers:
        reactive.set_state('postgresql.replication.had_peers')

    master = leader_get('master')
    reactive.toggle_state('postgresql.replication.is_master',
                          master == hookenv.local_unit())
    reactive.toggle_state('postgresql.replication.master.peered',
                          master in peers)
    reactive.toggle_state('postgresql.replication.master.authorized',
                          master in peers and _authorized_by(master))
    ready = (reactive.is_state('postgresql.replication.is_master') or
             reactive.is_state('postgresql.replication.master.authorized'))
    reactive.toggle_state('postgresql.replication.has_master', ready)


def _authorized_by(unit):
    # Ensure that this unit is listed as authorized by unit, and the
    # given unit has provided connection details. The check for
    # connection details is needed in case we are upgrading from an
    # older charm and the remote unit has not yet run its upgrade-charm
    # hook and provided the necessary information.
    peer = context.Relations().peer
    authorized = set(peer[unit].get('allowed-units').split())
    return 'host' in peer[unit] and unit in authorized


@when_not('leadership.is_leader')
@when_not('postgresql.replication.had_peers')
@when_not('workloadstatus.blocked')
def wait_for_peers():
    """Wait if there are no peers and we are not the master."""
    status_set('waiting', 'Waiting for peers')


@when('leadership.set.master')
@when('postgresql.replication.has_peers')
@when_not('leadership.replication.has_master')
@when_not('workloadstatus.blocked')
def wait_for_master():
    """Master appointed but not available to this unit."""
    status_set('waiting', 'Waiting for master')


def get_master():
    '''Return the appointed master unit.'''
    return leader_get('master')


def is_master():
    return get_master() == hookenv.local_unit()


@when('leadership.is_leader')
@when_not('leadership.set.master')
def initial_deployment_appoint_master():
    '''I am the leader and nobody is declared master. Declare myself master.'''
    standalone_unit_appoint_master()


@when('leadership.is_leader')
@when('postgresql.replication.had_peers')
@when_not('postgresql.replication.has_peers')
@when_not('postgresql.replication.is_master')
def standalone_unit_appoint_master():
    '''I am the leader and have no peers. Declare myself master.'''
    leader_set(master=hookenv.local_unit())
    reactive.set_state('postgresql.replication.is_master')
    reactive.set_state('postgresql.replication.has_master')
    reactive.set_state('postgresql.replication.cloned')


@when('leadership.is_leader')
@when('postgresql.replication.has_peers')
@when_not('postgresql.replication.is_master')
@when_not('postgresql.replication.master.peered')
def appoint_master():
    '''The master has been destroyed. Trigger the failover process.'''
    master = get_master()
    rel = context.Relations().peer

    hookenv.log('Master {} is gone'.format(master), WARNING)

    # Per Bug #1417874, the master doesn't know it is dying until it
    # is too late, and standbys learn about their master dying at
    # different times. We need to wait until all remaining units
    # are aware that the master is gone, which we can see by looking
    # at which units they have authorized. If we fail to do this step,
    # then we risk appointing a new master while some units are still
    # replicating data from the ex-master and we will end up with
    # diverging timelines. Unfortunately, this means failover will
    # not complete until hooks can be run on all remaining units,
    # which could be several hours if maintenance operations are in
    # progress. Once Bug #1417874 is addressed, the departing master
    # can cut off replication to all units simultaneously and we
    # can skip this step and allow failover to occur as soon as the
    # leader learns that the master is gone. Or can we? A network
    # partition could stop the controller seeing the master, and
    # any about-to-depart hooks will not be triggered, with the same
    # problem detailed above. pg_rewind and repmgr may also offer
    # alternatives, repairing the diverged timeline rather than
    # avoiding it.
    ready_for_election = True
    for unit, relinfo in rel.items():
        if master in relinfo.get('allowed-units', '').split():
            hookenv.log('Waiting for {} to stop replicating ex-master'
                        ''.format(unit))
            ready_for_election = False
    if ready_for_election:
        new_master = elect_master()
        hookenv.log('Failing over to new master {}'.format(new_master),
                    WARNING)
        leader_set(master=new_master)
    else:
        status_set(None, 'Coordinating failover')


def replication_username():
    # Leading underscore for 'system' accounts, to avoid an unlikely
    # conflict with a client service named 'repl'.
    return '_juju_repl'


# def replication_data_ready_action(func):
#     '''A replication specific data_ready action wrapper.
#
#     Action skipped when the manual_replication config option is True
#     or if we are not ready to deal with replication.
#     '''
#     @wraps(func)
#     def wrapper(service_name):
#         if hookenv.config()['manual_replication']:
#             hookenv.log('Manual replication - skipping')
#             return
#
#         peer_rel = context.Relations().peer
#         if peer_rel is None:
#             hookenv.log('Not yet joined peer relation - skipping')
#             return
#
#         if 'following' in peer_rel.local:
#             # Master was available, replication working or in failover.
#             return func()
#
#         master = postgresql.master()
#         if master in peer_rel or master == hookenv.local_unit():
#             # Master is available.
#             return func()
#
#         hookenv.log('Not yet joined peer relation with {} - skipping'
#                     ''.format(postgresql.master()))
#         return
#     return wrapper


@when('leadership.is_leader')
@when_not('leadership.set.replication_password')
def ensure_replication_credentials():
    leader_set(replication_password=host.pwgen())


@when('postgresql.replication.is_master')
@when('postgresql.cluster.is_running')
@when('leadership.set.replication_password')
def ensure_replication_user():
    con = postgresql.connect()
    postgresql.ensure_user(con, replication_username(),
                           leader_get('replication_password'),
                           replication=True)
    con.commit()


@when('postgresql.replication.has_peers')
def publish_replication_details():
    peer = context.Relations().peer
    if peer is not None:
        peer.local['host'] = hookenv.unit_private_ip()
        peer.local['port'] = str(postgresql.port())
        peer.local['allowed-units'] = ' '.join(sorted(peer.keys()))


@when('postgresql.cluster.configured')
@when('postgresql.replication.master.authorized')
@when_not('postgresql.replication.cloned')
def clone_master():
    master = postgresql.master()
    peer_rel = context.Relations().peer
    local_relinfo = peer_rel.local
    master_relinfo = peer_rel[master]

    # Be paranoid since we are about to destroy data.
    assert not reactive.helpers.is_state('postgresql.replication.is_master')
    assert not reactive.helpers.is_state('postgresql.cluster.is_running')

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
    except subprocess.CalledProcessError as x:
        hookenv.log('Clone failed with {}'.format(x), ERROR)
        # We failed, and the local cluster is broken.
        status_set('blocked', 'Failed to clone {}'.format(master))
        postgresql.drop_cluster()
        reactive.remove_state('postgresql.cluster.configured')
        reactive.remove_state('postgresql.cluster.created')
        # Terminate. We need this hook to exit, rather than enter a loop.
        raise SystemExit(0)

    ensure_ssl_certs()
    update_recovery_conf()

    local_relinfo['following'] = master
    reactive.set_state('postgresql.replication.cloned')


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


@when('postgresql.replication.is_master')
def promote_master():
    rels = context.Relations()
    if 'following' in rels.peer.local:
        if postgresql.is_secondary():
            hookenv.status_set('maintenance', 'Promoting to master')
            postgresql.promote()
        while postgresql.is_in_recovery():
            time.sleep(1)
        del rels.peer.local['following']


@when_file_changed(postgresql.recovery_conf_path())
def restart_on_master_change():
    reactive.set_state('postgresql.cluster.needs_restart')


@when('postgresql.replication.master.authorized')
@when('postgresql.replication.cloned')
@not_unless('postgresql.replication.master.peered')
def update_recovery_conf():
    master = get_master()
    path = postgresql.recovery_conf_path()

    peer_rel = context.Relations().peer
    master_relinfo = peer_rel.get(master)

    if not (master_relinfo and 'host' in master_relinfo):
        hookenv.log('Master {} has no published connection details'
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

    if reactive.helpers.is_state('postgresql.wal_e.enabled'):
        data['restore_command'] = wal_e.wal_e_restore_command()

    templating.render('recovery.conf.tmpl', path, data,
                      owner='postgres', group='postgres',
                      perms=0o600)


@not_unless('leadership.is_leader')
@not_unless('postgresql.replication.has_peers')
def elect_master():
    '''Elect a new master after the old one has departed.

    The new master is the secondary that has received the most
    WAL data. There must be no hot standbys still replicating
    data from the previous master, or we may end up with diverged
    timelines.
    '''
    rel = context.Relations().peer
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
