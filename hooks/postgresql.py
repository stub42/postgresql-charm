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
import re

import psycopg2

from charmhelpers.core import hookenv

import helpers


def version():
    '''PostgreSQL version. major.minor, as a string.'''
    # We use the charm configuration here, as multiple versions
    # of PostgreSQL may be installed.
    version = hookenv.config()['version']
    if version:
        return version

    # If the version wasn't set, we are using the default version for
    # the distro release.
    version_map = dict(precise='9.1', trusty='9.3')
    return version_map[helpers.distro_codename()]


def con():
    return psycopg2.connect(user=username(hookenv.local_unit()),
                            database='postgres',
                            port=port())


def username(unit):
    '''Return the username to use for connections from the given unit.'''
    return 'juju_{}'.format(unit.split('/', 1)[0])


def port():
    '''The port PostgreSQL is listening on.'''
    path = postgresql_conf_path()
    with open(path, 'r') as f:
        m = re.search(r'^port\s*=\*(\d+)', f.read(), re.I | re.M)
        assert m is not None, 'No port configured in {!r}'.format(path)
        return int(m.group(1))


def packages():
    ver = version()
    return set(['postgresql-{}'.format(ver),
                'postgresql-common',
                'postgresql-contrib-{}'.format(ver),
                'postgresql-client-{}'.format(ver)])


def postgresql_conf_path():
    return '/etc/postgresql/{}/main/postgresql.conf'.format(version())


def recovery_conf_path():
    return '/var/lib/postgresql/{}/recovery.conf'.format(version())


def is_in_recovery():
    '''True if the local cluster is in recovery.

    The unit may be a hot standby, or it may be a primary that is still
    starting up.
    '''
    cur = con().cursor()
    cur.execute('SELECT pg_is_in_recovery()')
    return cur.fetchone()[0]


def is_primary():
    '''True if the unit is a primary.

    It may be possible for there to be multiple primaries in the service,
    or none at all.
    '''
    return not is_secondary()


def is_secondary():
    '''True if the unit is a hot standby.'''
    return is_in_recovery() and os.path.exists(recovery_conf_path())
