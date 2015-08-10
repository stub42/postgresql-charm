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
from urllib.parse import urlparse

from charmhelpers.core import hookenv
from charmhelpers.core.hookenv import ERROR

import helpers
import postgresql


def wal_e_enabled():
    config = hookenv.config()
    return bool(config['wal_e_storage_uri'])


def wal_e_env_dir():
    '''The envdir(1) environment location used to drive WAL-E.'''
    return os.path.join(postgresql.config_dir(), 'wal-e.env')


@data_ready_action
def create_wal_e_env_dir():
    '''Regenerate the envdir(1) environment used to drive WAL-E.
   
    We do this even if wal-e is not enabled to ensure we destroy
    any secrets perhaps left around from when it was enabled.
    '''
    config = hookenv.config()
    env = dict(
        SWIFT_AUTHURL=config.get('os_auth_url', ''),
        SWIFT_TENANT=config.get('os_tenant_name', ''),
        SWIFT_USER=config.get('os_username', ''),
        SWIFT_PASSWORD=config.get('os_password', ''),
        AWS_ACCESS_KEY_ID=config.get('aws_access_key_id', ''),
        AWS_SECRET_ACCESS_KEY=config.get('aws_secret_access_key', ''),
        WABS_ACCOUNT_NAME=config.get('wabs_account_name', ''),
        WABS_ACCESS_KEY=config.get('wabs_access_key', ''),
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
            ensure_swift_container(parsed_uri.netloc)
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


def wal_e_archive_command():
    '''Return the archive_command needed in postgresql.conf.'''
    return 'envdir {} wal-e wal-push %p'.format(wal_e_env_dir())


def wal_e_restore_command():
    return 'envdir {} wal-e wal-fetch "%f" "%p"'.format(wal_e_env_dir())


def wal_e_backup_command():
    return 'envdir {} wal-e backup-push {}'.format(wal_e_env_dir(),
                                                   postgresql.data_dir())


def wal_e_prune_command():
    config = hookenv.config()
    return ('envdir {} wal-e delete --confirm retain {}'
            ''.format(wal_e_env_dir(), config['wal_e_backup_retention']))


def ensure_swift_container(container):
    from swiftclient import client as swiftclient
    config = hookenv.config()
    con = swiftclient.Connection(
        authurl=config.get('os_auth_url', ''),
        user=config.get('os_username', ''),
        key=config.get('os_password', ''),
        tenant_name=config.get('os_tenant_name', ''),
        auth_version='2.0',
        retries=0)
    try:
        con.head_container(container)
    except swiftclient.ClientException:
        con.put_container(container)
