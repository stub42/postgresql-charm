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

import csv
from io import StringIO
import os.path
import shutil
import subprocess
import tempfile
from textwrap import dedent
import time
from urllib.parse import urlparse

from charmhelpers.core import hookenv
from charmhelpers.core.hookenv import ERROR
from charms import reactive
from charms.reactive import when, when_any, when_not
from charms.layer import snap

from reactive.postgresql import helpers
from reactive.postgresql import postgresql
from reactive.postgresql import replication
from reactive.workloadstatus import status_set


@when('config.changed.wal_e_storage_uri')
def main():
    storage_uri = hookenv.config()['wal_e_storage_uri'].strip()
    reactive.helpers.toggle_state('postgresql.wal_e.enabled', storage_uri)
    reactive.helpers.toggle_state('postgresql.wal_e.swift',
                                  storage_uri.startswith('swift:'))
    reactive.remove_state('postgresql.wal_e.configured')


@when_any('config.changed.os_username',
          'config.changed.os_password',
          'config.changed.os_auth_url',
          'config.changed.os_tenant_name',
          'config.changed.aws_access_key_id',
          'config.changed.aws_secret_access_key',
          'config.changed.aws_region',
          'config.changed.wabs_account_name',
          'config.changed.wabs_access_key')
@when_not('snap.installed.wal-e')
def install():
    # Install WAL-E via snap package
    status_set(None, 'Installing wal-e snap')
    snap.install('wal-e', classic=True)


def wal_e_env_dir():
    '''The envdir(1) environment location used to drive WAL-E.'''
    return os.path.join(postgresql.config_dir(), 'wal-e.env')


@when('postgresql.cluster.created')
@when('snap.installed.wal-e')
@when('config.set.wal_e_storage_uri')
@when_not('postgresql.wal_e.configured')
def update_default_wal_e_env_dir():
    update_wal_e_env_dir(wal_e_env_dir(),
                         hookenv.config()['wal_e_storage_uri'])
    reactive.set_state('postgresql.wal_e.configured')


def update_wal_e_env_dir(dirpath, storage_uri):
    '''Regenerate the envdir(1) environment used to drive WAL-E.

    We do this even if wal-e is not enabled to ensure we destroy
    any secrets potentially left around from when it was enabled.
    '''
    config = hookenv.config()
    env = dict(
        # wal-e Swift creds
        SWIFT_AUTHURL=config.get('os_auth_url', ''),
        SWIFT_TENANT=config.get('os_tenant_name', ''),
        SWIFT_USER=config.get('os_username', ''),
        SWIFT_PASSWORD=config.get('os_password', ''),

        # wal-e AWS creds
        AWS_ACCESS_KEY_ID=config.get('aws_access_key_id', ''),
        AWS_SECRET_ACCESS_KEY=config.get('aws_secret_access_key', ''),
        AWS_REGION=config.get('aws_region', ''),

        # wal-e Azure cred
        WABS_ACCOUNT_NAME=config.get('wabs_account_name', ''),
        WABS_ACCESS_KEY=config.get('wabs_access_key', ''),

        # OpenStack creds for swift(1) cli tool
        OS_AUTH_URL=config.get('os_auth_url', ''),
        OS_USERNAME=config.get('os_username', ''),
        OS_PASSWORD=config.get('os_password', ''),
        OS_TENANT_NAME=config.get('os_tenant_name', ''),

        WALE_SWIFT_PREFIX='',
        WALE_S3_PREFIX='',
        WALE_WABS_PREFIX='')

    uri = storage_uri
    if uri:
        required_env = []
        parsed_uri = urlparse(uri)
        if parsed_uri.scheme == 'swift':
            env['WALE_SWIFT_PREFIX'] = uri
            required_env = ['SWIFT_AUTHURL', 'SWIFT_TENANT',
                            'SWIFT_USER', 'SWIFT_PASSWORD']
        elif parsed_uri.scheme == 's3':
            env['WALE_S3_PREFIX'] = uri
            required_env = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
                            'AWS_REGION']
        elif parsed_uri.scheme == 'wabs':
            env['WALE_WABS_PREFIX'] = uri
            required_env = ['WABS_ACCOUNT_NAME', 'WABS_ACCESS_KEY']
        else:
            hookenv.log('Invalid wal_e_storage_uri {}'.format(uri), ERROR)

        for env_key in required_env:
            if not env[env_key].strip():
                hookenv.log('Missing {}'.format(env_key), ERROR)

    # Regenerate the envdir(1) environment recommended by WAL-E.
    # All possible keys are rewritten to ensure we remove old secrets.
    helpers.makedirs(dirpath, mode=0o750,
                     user='postgres', group='postgres')
    for k, v in env.items():
        helpers.write(os.path.join(dirpath, k), v.strip(),
                      mode=0o640, user='postgres', group='postgres')


@when('postgresql.wal_e.swift')
@when('postgresql.wal_e.configured')
@when('snap.installed.wal-e')
def ensure_swift_container():
    uri = hookenv.config().get('wal_e_storage_uri', None).strip()
    if reactive.helpers.data_changed('postgresql.wal_e.uri', uri):
        container = urlparse(uri).netloc
        hookenv.log('Creating Swift container {}'.format(container))
        cmd = ['/snap/bin/wal-e.envdir', wal_e_env_dir(),
               '/snap/bin/wal-e.swift', 'post', container]
        subprocess.check_call(cmd, universal_newlines=True)


def wal_e_archive_command():
    '''Return the archive_command needed in postgresql.conf.'''
    return '/snap/bin/wal-e.envdir {} /snap/bin/wal-e wal-push %p'.format(
        wal_e_env_dir())


def wal_e_restore_command(envdir=None):
    return ('/snap/bin/wal-e.envdir {} /snap/bin/wal-e '
            'wal-fetch "%f" "%p"'.format(envdir or wal_e_env_dir()))


def wal_e_backup_command():
    return '/snap/bin/wal-e.envdir {} /snap/bin/wal-e backup-push {}'.format(
        wal_e_env_dir(), postgresql.data_dir())


def wal_e_prune_command():
    config = hookenv.config()
    return ('/snap/bin/wal-e.envdir {} /snap/bin/wal-e '
            'delete --confirm retain {}'
            ''.format(wal_e_env_dir(), config['wal_e_backup_retention']))


def wal_e_run(args, envdir=None, timeout=None):
    """Run a wal-e command.

    Returns stdout output. On failure, raises CalledProcessError
    with output on x.output and returncode on x.returncode. stderr goes
    to stderr, and likely the juju logs.
    """
    cmd = ['/snap/bin/wal-e.envdir',
           envdir or wal_e_env_dir(),
           '/snap/bin/wal-e'] + args
    # wal-e spits diagnostics to stderr, so leave them there for the juju logs.
    return subprocess.check_output(cmd, universal_newlines=True,
                                   timeout=timeout)


def wal_e_list_backups(envdir=None):
    raw = wal_e_run(['backup-list', '--detail'], envdir=envdir)
    r = list(csv.reader(StringIO(raw), dialect='excel-tab'))
    details = [{r[0][i]: r[j][i] for i in range(len(r[0]))}
               for j in range(1, len(r))]
    return details


@when('action.wal-e-restore')
def wal_e_restore():
    reactive.remove_state('action.wal-e-restore')
    params = hookenv.action_get()
    backup = params['backup-name'].strip().replace('-', '_')
    storage_uri = params['storage-uri'].strip()

    ship_uri = hookenv.config().get('wal_e_storage_uri')
    if storage_uri == ship_uri:
        hookenv.action_fail('The storage-uri parameter is identical to '
                            'the wal_e_storage_uri config setting. Your '
                            'restoration source cannot be the same as the '
                            'folder you are archiving too to avoid corrupting '
                            'the backups.')
        return

    if not params['confirm']:
        m = 'Recovery from {}.'.format(storage_uri)
        if ship_uri:
            m += '\nContents of {} will be destroyed.'.format(ship_uri)
        m += '\nExisting local database will be destroyed.'
        m += "\nRerun action with 'confirm=true' to proceed."
        hookenv.action_set({"info": m})
        return

    with tempfile.TemporaryDirectory(prefix='wal-e',
                                     suffix='envdir') as envdir:
        update_wal_e_env_dir(envdir, storage_uri)

        # Confirm there is a backup to restore
        backups = wal_e_list_backups(envdir)
        if not backups:
            hookenv.action_fail('No backups found at {}'.format(storage_uri))
            return
        if backup != 'LATEST' and backup not in (b['name'] for b in backups):
            hookenv.action_fail('Backup {} not found'.format(backup))
            return

        # Shutdown PostgreSQL. Note we want this action to run synchronously,
        # so there is no opportunity to ask permission from the leader. If
        # there are other units cloning this database, those clone operations
        # will fail. Which seems preferable to blocking a recovery operation
        # in any case, because if we are doing disaster recovery we generally
        # want to do it right now.
        status_set('maintenance', 'Stopping PostgreSQL for backup restoration')
        postgresql.stop()

        # Trash the existing database. Its dangerous to do this first, but
        # we probably need the space.
        data_dir = postgresql.data_dir()  # May be a symlink
        for content in os.listdir(data_dir):
            cpath = os.path.join(data_dir, content)
            if os.path.isdir(cpath) and not os.path.islink(cpath):
                shutil.rmtree(cpath)
            else:
                os.remove(cpath)

        # WAL-E recover
        status_set('maintenance', 'Restoring backup {}'.format(backup))
        wal_e_run(['backup-fetch', data_dir, backup], envdir=envdir)

        # Create recovery.conf to complete recovery
        is_master = replication.is_master()
        standby_mode = 'off' if is_master else 'on'
        if params.get('target-time'):
            target_time = ("recovery_target_time='{}'"
                           "".format(params['target-time']))
        else:
            target_time = ''
        target_action = 'promote' if is_master else 'shutdown'
        immediate = "" if is_master else "recovery_target='immediate'"
        helpers.write(postgresql.recovery_conf_path(),
                      dedent('''\
                             # Managed by Juju. PITR in progress.
                             standby_mode = {}
                             restore_command='{}'
                             recovery_target_timeline = {}
                             recovery_target_action = {}
                             {}
                             {}
                             ''').format(standby_mode,
                                         wal_e_restore_command(envdir=envdir),
                                         params['target-timeline'],
                                         target_action,
                                         target_time,
                                         immediate),
                      mode=0o600, user='postgres', group='postgres')

        if replication.is_master():
            # If master, trash the configured wal-e storage. This may
            # contain WAL and backups from the old cluster which will
            # conflict with the new cluster. Hopefully it does not
            # contain anything important, because we have no way to
            # prompt the user for confirmation.
            wal_e_run(['delete', '--confirm', 'everything'])

            # Then, wait for recovery and promotion.
            postgresql.start()
            con = postgresql.connect()
            cur = con.cursor()
            while True:
                cur.execute('''SELECT pg_is_in_recovery(),
                                      pg_last_xlog_replay_location()''')
                in_rec, loc = cur.fetchone()
                if not in_rec:
                    break
                status_set('maintenance', 'Recovery at {}'.format(loc))
                time.sleep(10)
        else:
            # If standby, startup and wait for recovery to complete and
            # shutdown.
            status_set('maintenance', 'Recovery')
            # Startup might shutdown immediately and look like a failure.
            postgresql.start(ignore_failure=True)
            # No recovery point status yet for standbys, as we would need
            # to handle connection failures when the DB shuts down. We
            # should do this.
            while postgresql.is_running():
                time.sleep(5)
            replication.update_recovery_conf(follow=replication.get_master())

    # Reactive handlers will deal with the rest of the cleanup.
    # eg. ensuring required users and roles exist
    replication.update_replication_states()
    reactive.remove_state('postgresql.cluster.configured')
    reactive.toggle_state('postgresql.cluster.is_running',
                          postgresql.is_running())
