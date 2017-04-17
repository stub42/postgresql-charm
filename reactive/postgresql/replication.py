# Copyright 2015-2017 Canonical Ltd.
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
from textwrap import dedent
import time

import psycopg2

from charmhelpers.core import host, hookenv, templating, unitdata
from charmhelpers.core.hookenv import DEBUG, ERROR, WARNING
from charms import coordinator, reactive
from charms.leadership import leader_get, leader_set
from charms.reactive import not_unless, when, when_not

from everyhook import everyhook
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

        postgresql.replication.switchover

            In switchover to a new master. A switchover is a controlled
            failover, where the existing master is available.

        postgresql.replication.is_anointed

            In switchover and this unit is anointed to be the new master.
    '''
    peers = helpers.get_peer_relation()
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

    anointed = get_anointed()
    reactive.toggle_state('postgresql.replication.switchover',
                          anointed is not None and anointed != master)
    reactive.toggle_state('postgresql.replication.is_anointed',
                          anointed is not None and anointed != master and
                          anointed == hookenv.local_unit())

    reactive.toggle_state('postgresql.replication.is_primary',
                          postgresql.is_primary())

    if reactive.is_state('postgresql.replication.is_primary'):
        if reactive.is_state('postgresql.replication.is_master'):
            # If the unit is a primary and the master, it is on the master
            # timeline by definition and gets the 'cloned' state.
            reactive.set_state('postgresql.replication.cloned')
        elif reactive.is_state('postgresql.replication.is_anointed'):
            # The anointed unit retains its 'cloned' state.
            pass
        else:
            # If the unit is a primary and not the master, it is on a
            # divered timeline and needs to lose the 'cloned' state.
            reactive.remove_state('postgresql.replication.cloned')

    cloned = reactive.is_state('postgresql.replication.cloned')
    reactive.toggle_state('postgresql.replication.failover',
                          master != hookenv.local_unit() and
                          peers and cloned and (master not in peers))


def authorized_by(unit):
    # Ensure that this unit is listed as authorized by the given unit,
    # and the given unit has provided connection details. The check for
    # connection details is needed in case we are upgrading from an
    # older charm and the remote unit has not yet run its upgrade-charm
    # hook and provided the necessary information.
    peer = helpers.get_peer_relation()
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
@when_not('postgresql.replication.failover')
@when_not('workloadstatus.blocked')
@when_not('postgresql.replication.manual')
def wait_for_master():
    """Master appointed but not available to this unit."""
    status_set('waiting', 'Waiting for master {}'.format(get_master()))


@when('postgresql.replication.failover')
@when_not('workloadstatus.blocked')
@when_not('postgresql.replication.manual')
def wait_for_failover():
    """Failover in progress."""
    status_set('waiting', 'Failover from {}'.format(get_master()))


def get_master():
    '''Return the master unit.'''
    if reactive.is_state('postgresql.replication.manual'):
        return None
    return leader_get('master')


@not_unless('leadership.is_leader')
def set_master(master):
    leader_set(master=master)
    update_replication_states()


def is_master():
    return get_master() == hookenv.local_unit()


def get_anointed():
    """The unit anointed to become master in switchover (not failover)"""
    if reactive.is_state('postgresql.replication.manual'):
        return None
    anointed = leader_get('anointed_master')
    if anointed == hookenv.local_unit():
        return anointed
    peer_rel = helpers.get_peer_relation()
    if peer_rel and anointed in peer_rel:
        return anointed
    # If this unit is being torn down, there is the perverse
    # case where the anointed master is no longer in the
    # peer relation. This probably will never happen outside
    # of test suites.
    return None


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
@when('postgresql.replication.failover')
@when_not('postgresql.replication.manual')
def coordinate_failover():
    '''The master has been destroyed. Trigger the failover process.'''
    master = get_master()
    rel = helpers.get_peer_relation()

    hookenv.log('Master {} is gone'.format(master), WARNING)

    # Per Bug #1417874, the master doesn't know it is dying until it
    # is too late, and standbys learn about their master dying at
    # different times. We need to wait until all remaining units
    # are aware that the master is gone, and are no longer following
    # it. If we fail to do this step, then we risk appointing a new
    # master while some units are still replicating data from
    # the ex-master and we will end up with diverged timelines.
    # Unfortunately, this means failover will not complete until
    # hooks can be run on all remaining units, which could be several
    # hours if maintenance operations are in progress. Once
    # Bug #1417874 is addressed, the departing master
    # can cut off replication to all units simultaneously and we
    # can skip this step and allow failover to occur as soon as the
    # leader learns that the master is gone. Or can we? A network
    # partition could stop the controller seeing the master, and
    # any about-to-depart hooks will not be triggered, with the same
    # problem detailed above. pg_rewind and repmgr may also offer
    # alternatives, repairing the diverged timeline rather than
    # avoiding it. But pg_rewind only copes with timeline switches
    # in PG9.6+, which means we can't promote, which risks wal shipping
    # collisions between the old and new masters.
    waiting_on = set()
    for unit, relinfo in rel.items():
        if relinfo.get('following'):
            hookenv.log('Waiting for {} to stop replicating ex-master'
                        ''.format(unit))
            waiting_on.add(unit)
    if rel.local.get('following'):
        # following from the relation, rather than get_following(),
        # to ensure that the change has been applied.
        hookenv.log('Waiting for me to stop replicating ex-master')
        waiting_on.add(hookenv.local_unit())
    if not waiting_on:
        new_master = elect_master()
        hookenv.log('Failing over to new master {}'.format(new_master),
                    WARNING)
        set_master(new_master)
    else:
        status_set(None,
                   'Coordinating failover. Waiting on {}'
                   ''.format(','.join(sorted(waiting_on))))


@when('postgresql.replication.failover')
@when_not('postgresql.cluster.needs_restart')
@when_not('postgresql.replication.manual')
def failover():
    if get_following() is None:
        hookenv.log('Failover already in progress', DEBUG)
        return

    # Stop replicating the doomed master, or we risk diverging
    # timelines.
    helpers.rewrite(postgresql.recovery_conf_path(),
                    dedent('''\
                           # Managed by Juju. Failover in progress.
                           standby_mode = on
                           recovery_target_timeline = latest
                           '''))

    # Kick off a rolling restart to apply the change.
    reactive.set_state('postgresql.cluster.needs_restart')

    # Publish the change after the restart.
    set_following(None)
    reactive.set_state('postgresql.replication.publish_following')


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
    peer = helpers.get_peer_relation()
    if peer is not None:
        peer.local['host'] = hookenv.unit_private_ip()
        peer.local['port'] = str(postgresql.port())
        peer.local['allowed-units'] = ' '.join(sorted(peer.keys()))


@when_not('coordinator.requested.restart')
@when_not('coordinator.granted.restart')
@when_not('postgresql.replication.cloned')
@when_not('postgresql.replication.manual')
@when('postgresql.cluster.configured')
@when('postgresql.replication.master.authorized')
def need_clone_lock():
    # We need to grab the restart lock before cloning, to ensure
    # that the master is not restarted during the process.
    coordinator.acquire('restart')


@when('coordinator.requested.restart')
@when_not('coordinator.granted.restart')
@when_not('postgresql.replication.cloned')
@when_not('postgresql.replication.manual')
@when_not('workloadstatus.blocked')
def wait_for_clone():
    status_set('waiting',
               'Waiting for permission to clone {}'.format(get_master()))


@when('postgresql.cluster.is_running')
@when('postgresql.replication.is_primary')
@when_not('postgresql.replication.cloned')
@when_not('postgresql.replication.manual')
def diverged_timeline():
    status_set('maintenance', 'Diverged timeline')
    # Don't shutdown without the coordinator lock. Most likely,
    # this unit is being destroyed and shouldn't reclone.
    reactive.set_state('postgresql.cluster.needs_restart')


@when('postgresql.cluster.configured')
@when('postgresql.replication.master.authorized')
@when('coordinator.granted.restart')
@when_not('postgresql.cluster.is_running')
@when_not('postgresql.replication.cloned')
@when_not('postgresql.replication.manual')
def clone_master():
    master = get_master()
    peer_rel = helpers.get_peer_relation()
    master_relinfo = peer_rel[master]

    # Be paranoid since we are about to destroy data.
    assert not reactive.helpers.is_state('postgresql.replication.is_master')
    assert not reactive.helpers.is_state('postgresql.cluster.is_running')

    # We use realpath on data_dir as it may have been replaced with
    # a symbolic link, so we empty and recreate the actual directory
    # and the links remain in place.
    data_dir = os.path.realpath(postgresql.data_dir())

    if os.path.exists(data_dir):
        hookenv.log('Removing {} in preparation for clone'.format(data_dir))
        shutil.rmtree(data_dir)
    helpers.makedirs(data_dir, mode=0o700, user='postgres', group='postgres')

    cmd = ['sudo', '-H',  # -H needed to locate $HOME/.pgpass
           '-u', 'postgres', 'pg_basebackup',
           '-D', postgresql.data_dir(),
           '-h', master_relinfo['host'],
           '-p', master_relinfo['port'],
           '--checkpoint=fast', '--progress', '--xlog-method=stream',
           '--no-password', '--username=_juju_repl']
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

    update_recovery_conf(follow=master)

    reactive.set_state('postgresql.replication.cloned')
    update_replication_states()


@when('postgresql.replication.is_master')
@when('postgresql.replication.cloned')
@when('postgresql.cluster.is_running')
@when_not('postgresql.replication.is_primary')
@when_not('postgresql.replication.manual')
@when_not('postgresql.replication.switchover')
def promote_to_master():
    status_set('maintenance', 'Promoting to master')
    postgresql.promote()

    set_following(None)
    publish_following()

    while postgresql.is_in_recovery():
        status_set('maintenance', 'Waiting for startup')
        time.sleep(1)

    assert not os.path.exists(postgresql.recovery_conf_path()), \
        'recovery.conf still exists after promotion'

    update_replication_states()
    helpers.ping_peers()


@when('postgresql.replication.master.authorized')
@when('postgresql.replication.cloned')
@when_not('postgresql.replication.switchover')
@when_not('postgresql.replication.failover')
@when_not('postgresql.replication.manual')
def follow_master():
    update_recovery_conf(follow=get_master())


@when('postgresql.replication.switchover')
@when('postgresql.replication.cloned')
@when_not('postgresql.replication.is_anointed')
@when_not('postgresql.replication.failover')
@when_not('postgresql.replication.manual')
def follow_anointed():
    anointed = get_anointed()
    if anointed is not None:
        update_recovery_conf(follow=anointed)
    switchover_status()


def update_recovery_conf(follow):
    assert follow != hookenv.local_unit()

    path = postgresql.recovery_conf_path()

    peer_rel = helpers.get_peer_relation()
    follow_relinfo = peer_rel.get(follow)
    assert follow_relinfo is not None, 'Invalid upstream {}'.format(follow)

    current_follow = get_following()

    if follow != current_follow:
        status_set('maintenance', 'Following new unit {}'.format(follow))
        set_following(follow)
        # Setting the state to defer publication until after restart.
        reactive.set_state('postgresql.replication.publish_following')

    else:
        # Even though the master is unchanged, we still regenerate
        # recovery.conf in case connection details such as IP addresses
        # have changed.
        hookenv.log('Continuing to follow {}'.format(follow))

    config = hookenv.config()

    data = dict(streaming_replication=config['streaming_replication'],
                host=follow_relinfo['host'],
                port=follow_relinfo['port'],
                user=replication_username(),
                password=leader_get('replication_password'))

    if reactive.helpers.is_state('postgresql.wal_e.enabled'):
        data['restore_command'] = wal_e.wal_e_restore_command()

    templating.render('recovery.conf.tmpl', path, data,
                      owner='postgres', group='postgres',
                      perms=0o600)

    # Use @when_file_changed for this when Issue #44 is resolved.
    if reactive.helpers.any_file_changed([path]):
        reactive.set_state('postgresql.cluster.needs_restart')
        if reactive.is_state('postgresql.replication.cloned'):
            reactive.set_state('postgresql.replication.check_following')


def get_following():
    return unitdata.kv().get('postgresql.replication.following')


def set_following(master):
    if master == get_following():
        hookenv.log('Following {}, unchanged'.format(master), DEBUG)
    else:
        unitdata.kv().set('postgresql.replication.following', master)
        hookenv.log('Will follow {} next restart'.format(master), DEBUG)


@when('postgresql.replication.publish_following')
@when_not('postgresql.cluster.needs_restart')
def publish_following():
    # Advertise the unit we are following, in the hook that we actually
    # restart and this change actually takes effect. This pings any
    # anointed master during switchover, allowing it to proceed onto
    # the promotion step.
    peer_rel = helpers.get_peer_relation()
    following = get_following()
    if peer_rel is not None:
        peer_rel.local['following'] = following
        reactive.remove_state('postgresql.replication.publish_following')
    if following is None:
        reactive.remove_state('postgresql.replication.check_following')
    if reactive.is_state('postgresql.replication.switchover'):
        switchover_status()


@when('postgresql.replication.check_following')
@when('coordinator.granted.restart')
@when_not('postgresql.cluster.needs_restart')
def check_following():
    peer_rel = helpers.get_peer_relation()
    following = get_following()
    if peer_rel is None or following is None:
        reactive.remove_state('postgresql.replication.check_following')
        return
    if postgresql.is_replicating(following, user=replication_username()):
        hookenv.log('Replication of {} is confirmed'.format(following))
        reactive.remove_state('postgresql.replication.check_following')
    else:
        status_set('blocked',
                   'Replication of {} has failed'.format(following))


@when('postgresql.replication.is_anointed')
@when('postgresql.cluster.is_running')
@when_not('postgresql.replication.is_primary')
@when_not('postgresql.replication.manual')
@when_not('coordinator.requested.restart')
@when_not('coordinator.granted.restart')
def drain_master_and_promote_anointed():
    # Wait until this anointed unit is fully in-sync with the
    # master, and then promote it to master. But first we
    # need to ensure that the master is following us, and that we
    # have no outstanding requests on the restart lock to avoid deadlocking
    # the cluster.
    peer_rel = helpers.get_peer_relation()
    master = get_master()
    if peer_rel is None or master is None:
        return  # Peers all gone? Other handlers will promote.

    if peer_rel[master].get('following') != hookenv.local_unit():
        status_set('waiting',
                   'Waiting for master to follow me, its anointed successor')
        return  # Try again next hook

    # Drain the master
    while True:
        local_offset = postgresql.wal_received_offset(postgresql.connect())
        if local_offset is None:
            # Huh? Should not happen unless the server was unexpectedly
            # restarted.
            break

        try:
            remote_con = postgresql.connect(user=replication_username(),
                                            unit=master)
            remote_offset = postgresql.wal_received_offset(remote_con)
            if remote_offset is None:
                # Huh? Should not happen either, since the master published
                # that it is following us.
                break
        except (psycopg2.Error, postgresql.InvalidConnection) as x:
            status_set('waiting',
                       'Waiting to query replication state of {}: {}'
                       ''.format(master, x))
            time.sleep(1)
            continue

        if local_offset >= remote_offset:
            break  # In sync. Proceed to promotion.

        status_set('waiting',
                   '{} bytes to sync before promotion'
                   ''.format(remote_offset - local_offset))
        time.sleep(1)

    # Promote the anointed to master
    promote_to_master()
    switchover_status()


@not_unless('leadership.is_leader')
@not_unless('postgresql.replication.has_peers')
def elect_master():
    '''Elect a new master after the old one has departed.

    The new master is the secondary that has replayed the most
    WAL data. There must be no hot standbys still replicating
    data from the previous master, or we may end up with diverged
    timelines.

    Note we check replayed wal instead of received wal, because the
    servers have just been restarted with no master and information
    about received wal lost.
    '''
    rel = helpers.get_peer_relation()
    local_unit = hookenv.local_unit()

    # The unit with the most advanced WAL offset should be the new master.
    if postgresql.is_running():
        local_offset = postgresql.wal_replay_offset(postgresql.connect())
        offsets = [(local_offset, local_unit)]
    else:
        offsets = []

    for unit, relinfo in rel.items():
        try:
            con = postgresql.connect(user=replication_username(), unit=unit)
            offsets.append((postgresql.wal_replay_offset(con), unit))
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


@when('action.switchover')
@when_not('leadership.is_leader')
def switchover_action_requires_leader():
    hookenv.action_fail("switchover must be run on the leader unit")
    reactive.remove_state('action.switchover')


@when('action.switchover')
@when('leadership.is_leader')
@when('leadership.set.anointed_master')
def switchover_in_progress():
    hookenv.action_fail("switchover to {} already in progress"
                        "".format(get_anointed()))
    reactive.remove_state('action.switchover')


@when('action.switchover')
@when('leadership.is_leader')
@when_not('leadership.set.anointed_master')
def switchover_action():
    try:
        params = hookenv.action_get()
        anointed = params['master']
        master = get_master()

        if not master:
            hookenv.action_fail("There is no master. Cannot switchover")
            return

        if not anointed:
            hookenv.action_fail("anointed master was not specified")
            return

        if master == anointed:
            hookenv.action_set(dict(result='{} is already master'
                                           ''.format(anointed)))
            return

        peer_rel = helpers.get_peer_relation()
        if anointed != hookenv.local_unit() and anointed not in peer_rel:
            hookenv.action_fail("Invalid unit name {}".format(anointed))
            return

        leader_set(anointed_master=anointed)
        update_replication_states()

        switchover_status()

        hookenv.action_set(dict(result='Initiated switchover of master to {}'
                                       ''.format(anointed)))

    finally:
        reactive.remove_state('action.switchover')


@when('leadership.is_leader')
@when('leadership.set.anointed_master')
def check_switchover_complete():
    peer_rel = helpers.get_peer_relation()
    anointed = get_anointed()

    if anointed is None:
        # switchover target is gone. Hopefully the service
        # is being torn down, because this otherwise shouldn't happen.
        # Reverting to the existing master should work
        leader_set(anointed_master=None)
        update_replication_states()
        return

    if anointed == hookenv.local_unit():
        anointed_relinfo = peer_rel.local
    else:
        anointed_relinfo = peer_rel[anointed]
    if anointed_relinfo.get('following') is None:
        leader_set(master=anointed, anointed_master=None)
        hookenv.log('Switchover to {} complete'.format(anointed))
        update_replication_states()
    else:
        hookenv.log('Switchover to {} continues'.format(anointed))

    switchover_status()


@when('postgresql.replication.switchover')
def switchover_status():
    update_replication_states()
    anointed = get_anointed()

    # From the peer relation, to match what is published after restart.
    # unitdata copy is set before restart.
    peer_rel = helpers.get_peer_relation()
    following = peer_rel.local.get('following')

    mode = ('Primary'
            if reactive.is_state('postgresql.replication.is_primary')
            else 'Secondary')

    hookenv.status_set('maintenance',
                       'Switchover to {}. {} following {}'
                       ''.format(anointed, mode, str(following)))
