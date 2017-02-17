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
import subprocess
from urllib.parse import urlparse

from charmhelpers.core import hookenv
from charmhelpers.core.hookenv import ERROR
from charms import reactive
from charms.reactive import hook, when, when_not
from charms.layer import snap

from reactive.postgresql import helpers
from reactive.postgresql import postgresql
from reactive.workloadstatus import status_set


@hook('config-changed')
def main():
    storage_uri = hookenv.config()['wal_e_storage_uri'].strip()
    reactive.helpers.toggle_state('postgresql.wal_e.enabled', storage_uri)
    reactive.helpers.toggle_state('postgresql.wal_e.swift',
                                  storage_uri.startswith('swift:'))
    reactive.remove_state('postgresql.wal_e.configured')


@when('postgresql.wal_e.enabled')
@when_not('snap.installed.wal-e')
def install():
    # Install WAL-E via snap package
    status_set(None, 'Installing wal-e snap')
    snap.install('wal-e', classic=True, channel='beta')


def wal_e_env_dir():
    '''The envdir(1) environment location used to drive WAL-E.'''
    return os.path.join(postgresql.config_dir(), 'wal-e.env')


@when('postgresql.cluster.created')
@when('snap.installed.wal-e')
@when_not('postgresql.wal_e.configured')
def update_wal_e_env_dir():
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

    uri = config.get('wal_e_storage_uri', None)
    if uri:
        required_env = []
        parsed_uri = urlparse(uri)
        if parsed_uri.scheme == 'swift':
            env['WALE_SWIFT_PREFIX'] = uri
            required_env = ['SWIFT_AUTHURL', 'SWIFT_TENANT',
                            'SWIFT_USER', 'SWIFT_PASSWORD']
        elif parsed_uri.scheme == 's3':
            env['WALE_S3_PREFIX'] = uri
            required_env = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
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
    helpers.makedirs(wal_e_env_dir(), mode=0o750,
                     user='postgres', group='postgres')
    for k, v in env.items():
        helpers.write(os.path.join(wal_e_env_dir(), k), v.strip(),
                      mode=0o640, user='postgres', group='postgres')

    reactive.set_state('postgresql.wal_e.configured')


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


def wal_e_restore_command():
    return ('/snap/bin/wal-e.envdir {} /snap/bin/wal-e '
            'wal-fetch "%f" "%p"'.format(wal_e_env_dir()))


def wal_e_backup_command():
    return '/snap/bin/wal-e.envdir {} /snap/bin/wal-e backup-push {}'.format(
        wal_e_env_dir(), postgresql.data_dir())


def wal_e_prune_command():
    config = hookenv.config()
    return ('/snap/bin/wal-e.envdir {} /snap/bin/wal-e '
            'delete --confirm retain {}'
            ''.format(wal_e_env_dir(), config['wal_e_backup_retention']))
