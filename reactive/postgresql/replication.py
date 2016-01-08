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
from charmhelpers.core import host, hookenv, templating, unitdata
from charmhelpers.core.hookenv import DEBUG, ERROR, WARNING
from charms import reactive
from charms.reactive import not_unless, when, when_not

from everyhook import everyhook
from reactive.leadership import leader_get, leader_set
from reactive.workloadstatus import status_set
from reactive.postgresql import helpers
from reactive.postgresql import postgresql
from reactive.postgresql import wal_e


@everyhook
def replication_states():
    '''Update the replication state every hook, or risk failures
       when leadership or peer relation settings are visible before
       the leadership or peer relation hooks are fired.
    '''
    update_replication_states()


def update_replication_states():
    '''
    Set the following states appropriately:

        postgresql.replication.has_peers

            This unit has peers.

        postgresql.replication.had_peers

            This unit once had peers, but may not any more. The peer
            relation exists.

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
            the master, or is the master. Undefined with manual replication.

        postgresql.replication.manual

            Manual replication mode has been selected and the charm
            must not do any replication setup or maintenance.

        postgresql.replication.is_primary

            The unit is writable. It is either the master or manual
            replication mode is in effect.
    '''
    peers = context.Relations().peer
    reactive.toggle_state('postgresql.replication.has_peers', peers)
    if peers:
        reactive.set_state('postgresql.replication.had_peers')

    reactive.toggle_state('postgresql.replication.manual',
                          hookenv.config()['manual_replication'])

    master = get_master()  # None if postgresql.replication.manual state.
    reactive.toggle_state('postgresql.replication.is_master',
                          master == hookenv.local_unit())
    reactive.toggle_state('postgresql.replication.master.peered',
                          peers and master in peers)
    reactive.toggle_state('postgresql.replication.master.authorized',
                          peers and master in peers and authorized_by(master))
    ready = (reactive.is_state('postgresql.replication.is_master') or
             reactive.is_state('postgresql.replication.master.authorized'))
    reactive.toggle_state('postgresql.replication.has_master', ready)

    reactive.toggle_state('postgresql.replication.is_primary',
                          postgresql.is_primary())

    if reactive.is_state('postgresql.replication.is_primary'):
        if reactive.is_state('postgresql.replication.is_master'):
            # If the unit is a primary and the master, it is on the master
            # timeline by definition and gets the 'cloned' state.
            reactive.set_state('postgresql.replication.cloned')
        else:
            # If the unit is a primary and not the master, it is on a
            # divered timeline and needs to lose the 'cloned' state.
            reactive.remove_state('postgresql.replication.cloned')


def authorized_by(unit):
    # Ensure that this unit is listed as authorized by the given unit,
    # and the given unit has provided connection details. The check for
    # connection details is needed in case we are upgrading from an
    # older charm and the remote unit has not yet run its upgrade-charm
    # hook and provided the necessary information.
    peer = context.Relations().peer
    if peer is None or unit not in peer:
        return False
    authorized = set(peer[unit].get('allowed-units', '').split())
    return 'host' in peer[unit] and hookenv.local_unit() in authorized


@when_not('leadership.is_leader')
@when_not('postgresql.replication.had_peers')
@when_not('workloadstatus.blocked')
@when_not('postgresql.replication.manual')
def wait_for_peers():
    """Wait if there are no peers and we are not the master."""
    status_set('waiting', 'Waiting for peers')


@when('leadership.set.master')
@when('postgresql.replication.has_peers')
@when_not('postgresql.replication.has_master')
@when_not('workloadstatus.blocked')
@when_not('postgresql.replication.manual')
def wait_for_master():
    """Master appointed but not available to this unit."""
    status_set('waiting', 'Waiting for master {}'.format(get_master()))


def get_master():
    '''Return the appointed master unit.'''
    if reactive.is_state('manual_replication'):
        return None
    return leader_get('master')


@not_unless('leadership.is_leader')
def set_master(master):
    leader_set(master=master)
    update_replication_states()


def is_master():
    return get_master() == hookenv.local_unit()


@when('leadership.is_leader')
@when_not('leadership.set.master')
@when_not('postgresql.replication.manual')
def initial_deployment_appoint_master():
    '''I am the leader and nobody is declared master. Declare myself master.'''
    set_master(hookenv.local_unit())


@when('leadership.is_leader')
@when('postgresql.replication.had_peers')
@when_not('postgresql.replication.has_peers')
@when_not('postgresql.replication.is_master')
@when_not('postgresql.replication.manual')
def standalone_unit_appoint_master():
    '''I am the leader and have no peers. Declare myself master.'''
    set_master(hookenv.local_unit())


@when('leadership.is_leader')
@when('postgresql.replication.has_peers')
@when_not('postgresql.replication.is_master')
@when_not('postgresql.replication.master.peered')
@when_not('postgresql.replication.manual')
def failover():
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
    waiting_on = set()
    for unit, relinfo in rel.items():
        if master in relinfo.get('allowed-units', '').split():
            hookenv.log('Waiting for {} to stop replicating ex-master'
                        ''.format(unit))
            waiting_on.add(unit)
    if not waiting_on:
        new_master = elect_master()
        hookenv.log('Failing over to new master {}'.format(new_master),
                    WARNING)
        set_master(new_master)
    else:
        status_set(None,
                   'Coordinating failover. Waiting on {}'
                   ''.format(',').join(sorted(waiting_on)))


def replication_username():
    # Leading underscore for 'system' accounts, to avoid an unlikely
    # conflict with a client service named 'repl'.
    return '_juju_repl'


@when('leadership.is_leader')
@when_not('leadership.set.replication_password')
def ensure_replication_credentials():
    leader_set(replication_password=host.pwgen())


@when('postgresql.replication.is_master')
@when('postgresql.replication.is_primary')
@when('postgresql.cluster.is_running')
@when('leadership.set.replication_password')
@when_not('postgresql.replication.manual')
@when_not('postgresql.replication.replication_user_created')
def create_replication_user():
    username = replication_username()
    hookenv.log('Creating replication user {}'.format(username))
    con = postgresql.connect()
    postgresql.ensure_user(con, username,
                           leader_get('replication_password'),
                           replication=True)
    con.commit()
    reactive.set_state('postgresql.replication.replication_user_created')


@when('postgresql.replication.has_peers')
@when_not('postgresql.replication.manual')
def publish_replication_details():
    peer = context.Relations().peer
    if peer is not None:
        peer.local['host'] = hookenv.unit_private_ip()
        peer.local['port'] = str(postgresql.port())
        peer.local['allowed-units'] = ' '.join(sorted(peer.keys()))


@when('coordinator.requested.restart')
@when_not('coordinator.granted.restart')
@when_not('postgresql.replication.cloned')
@when_not('postgresql.replication.manual')
@when_not('workloadstatus.blocked')
def wait_for_clone():
    status_set('waiting',
               'Waiting for permission to clone {}'.format(get_master()))


@when('postgresql.cluster.configured')
@when('postgresql.replication.master.authorized')
@when_not('postgresql.replication.cloned')
@when_not('postgresql.replication.manual')
def clone_master():
    master = get_master()
    peer_rel = context.Relations().peer
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
    status_set('maintenance', 'Cloning {}'.format(master))
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

    reactive.set_state('postgresql.replication.cloned')


def ensure_ssl_certs():
    if postgresql.has_version('9.2'):
        hookenv.log('No SSL cleanup with PostgreSQL {}'
                    ''.format(postgresql.version()), DEBUG)
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
@when('postgresql.replication.cloned')
@when('postgresql.cluster.is_running')
@when_not('postgresql.replication.is_primary')
@when_not('postgresql.replication.manual')
def promote_to_master():
    status_set('maintenance', 'Promoting to master')
    postgresql.promote()

    while postgresql.is_in_recovery():
        status_set('maintenance', 'Waiting for startup')
        time.sleep(1)

    update_replication_states()


@when('postgresql.replication.master.authorized')
@when('postgresql.replication.cloned')
@when_not('postgresql.replication.manual')
def update_recovery_conf():
    master = get_master()
    assert master != hookenv.local_unit()

    path = postgresql.recovery_conf_path()

    peer_rel = context.Relations().peer
    master_relinfo = peer_rel.get(master)

    following = unitdata.kv().get('postgresql.replication.following')
    leader = context.Leader()
    config = hookenv.config()

    if master != following:
        status_set('maintenance', 'Following new master {}'.format(master))
        unitdata.kv().set('postgresql.replication.following', master)

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

    # Use @when_file_changed for this when Issue #44 is resolved.
    if reactive.helpers.any_file_changed([path]):
        reactive.set_state('postgresql.cluster.needs_restart')


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
        status_set('blocked', 'No candidates for master found!')
        raise SystemExit(0)
    elected_master = offsets[0][1]
    return elected_master
