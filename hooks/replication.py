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
import subprocess

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

    Action skipped when the manual_replication config option is True.
    '''
    @wraps(func)
    def wrapper(service_name):
        config = hookenv.config()
        if config['manual_replication']:
            hookenv.log('Manual replication - skipping')
            return
        return func()
    return wrapper


@replication_data_ready_action
def wait_for_master():
    '''Wait until the master has not authorized us.

    If not, the unit is put into 'waiting' state and the hook exits.
    '''
    master = postgresql.master()
    local = hookenv.local_unit()
    if master == local:
        return

    peer_rel = context.Relations().peer
    if peer_rel and master and master in peer_rel:
        relinfo = peer_rel[master]
        allowed = relinfo.get('allowed-units', '').split()
        if local in allowed:
            return

    helpers.status_set('waiting', 'Waiting for master')
    raise SystemExit(0)


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


@not_master
@replication_data_ready_action
def clone_master():
    master = postgresql.master()
    peer_rel = context.Relations().peer
    local_relinfo = peer_rel.local
    master_relinfo = peer_rel[master]

    if 'following' in local_relinfo:
        hookenv.log('Already cloned {}'.format(local_relinfo['following']),
                    DEBUG)
        return

    data_dir = postgresql.data_dir()

    if os.path.exists(data_dir):
        # End users should never see this. Both pg_basebackup and
        # pg_dropcluster would need to fail.
        helpers.status_set('blocked',
                           'Cannot clone master while local cluster exists. '
                           'Run pg_dropcluster {} main'
                           ''.format(postgresql.version()))
        raise SystemExit(0)
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


@replication_data_ready_action
def update_recovery_conf():
    master = postgresql.master()
    local_unit = hookenv.local_unit()
    path = postgresql.recovery_conf_path()

    if master == local_unit:
        # Remove recovery.conf if this unit has been promoted to master.
        if os.path.exists(path):
            hookenv.log("I've been promoted to master", WARNING)
            os.unlink(path)
        return  # Am master. No need to continue here.

    peer = context.Relations().peer
    master_relinfo = peer[master]
    leader = context.Leader()
    config = hookenv.config()

    hookenv.log('Following master {}'.format(master))
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
    rel = context.Relations().peer
    assert rel is not None, 'Attempting to elect master with no peer rel'
    local_unit = hookenv.local_unit()

    # The unit with the most advanced WAL offset should be the new master.
    local_offset = postgresql.wal_received_offset(postgresql.connect())
    offsets = [(local_offset, local_unit)]

    for unit, relinfo in rel.items():
        # If the remote unit hasn't yet authorized us, it is not
        # suitable to become master.
        if local_unit not in relinfo.get('allowed-units', '').split():
            # TODO: Signal potential clone required. Or autodetect
            # based on timeline switch.
            break

        # If the remote unit is restarting, it is not suitable to become
        # master.
        if coordinator.grants.get(unit, {}).get('restart'):
            # TODO: Signal potential clone required. Or autodetect
            # based on timeline switch.
            break

        con = postgresql.connect(user=replication_username(), unit=unit)
        offsets.append((postgresql.wal_received_offset(con), unit))

    offsets.sort()
    elected_master = offsets[0][1]
    return elected_master
