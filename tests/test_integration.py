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

from contextlib import suppress
import os.path
import shutil
import signal
import socket
import subprocess
import sys
import time
import unittest

import psycopg2

HERE = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.join(HERE, os.pardir)))

from testing.amuletfixture import AmuletFixture


SERIES = os.environ.get('SERIES', 'trusty').strip()
CLIENT_CHARMDIR = os.path.abspath(os.path.join(HERE, os.pardir,
                                               'lib', 'pgclient'))
assert os.path.isdir(CLIENT_CHARMDIR)


def skip_if_swift_is_unavailable():
    os_keys = set(['OS_TENANT_NAME', 'OS_AUTH_URL',
                   'OS_USERNAME', 'OS_PASSWORD'])
    for os_key in os_keys:
        if os_key not in os.environ:
            return unittest.skip('Swift is unavailable')
    return lambda x: x


def skip_if_s3_is_unavailable():
    os_keys = set(['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY'])
    for os_key in os_keys:
        if os_key not in os.environ:
            return unittest.skip('S3 is unavailable')
    return lambda x: x


def skip_if_wabs_is_unavailable():
    os_keys = set(['WABS_ACCOUNT_NAME', 'WABS_ACCESS_KEY'])
    for os_key in os_keys:
        if os_key not in os.environ:
            return unittest.skip('WABS is unavailable')
    return lambda x: x


class PostgreSQLCharmBaseTestCase(object):
    deployment = None  # Module scoped AmuletFixture()

    common_config = dict()

    # Override these in subclasses to run these tests multiple times
    # for different PostgreSQL versions.
    test_config = None
    num_units = None

    @classmethod
    def setUpClass(cls):
        super(PostgreSQLCharmBaseTestCase, cls).setUpClass()

        # Set up the AmuletFixture. It would be nice to share some
        # of this setup with a module level fixture, but unfortunately
        # Amulet does not let us add services after the initial deploy.
        deployment = AmuletFixture(series=SERIES)
        deployment.setUp()
        cls.deployment = deployment

        # Add the helper charm. We need this to act as a proxy,
        # so connections come from a unit the PostgreSQL charm recognizes.
        deployment.add('client', CLIENT_CHARMDIR)

        # Add and configure the PostgreSQL units.
        deployment.add('postgresql', units=cls.num_units,
                       constraints=dict(mem="512M"))
        deployment.expose('postgresql')
        config = dict(cls.common_config)
        config.update(cls.test_config)
        deployment.configure('postgresql', config)

        # Relate it to the client service.
        cls.deployment.relate('postgresql:db', 'client:db')
        cls.deployment.relate('postgresql:db-admin', 'client:db-admin')

        try:
            cls.deployment.deploy()
        except Exception:
            with suppress(Exception):
                cls.deployment.tearDown()
            raise

    @classmethod
    def tearDownClass(cls):
        if cls.deployment is not None:
            cls.deployment.tearDown()
        super(PostgreSQLCharmBaseTestCase, cls).setUpClass()

    def connect(self, unit=None, admin=False, database=None):
        '''
        A psycopg2 connection to a PostgreSQL unit via our client.

        A db-admin relation is used if database is specified. Otherwise,
        a standard db relation is used.
        '''
        # 'db' or 'db-admin' relation?
        rel_name = 'db-admin' if admin else 'db'
        to_rel = 'client:{}'.format(rel_name)

        # Which PostgreSQL unit we want to talk to.
        if unit is None:  # Any unit
            postgres_sentry = self.deployment.sentry['postgresql'][0]
            relinfo = postgres_sentry.relation(rel_name, to_rel)
        else:
            postgres_sentry = self.deployment.sentry[unit]
            relinfo = postgres_sentry.relation(rel_name, to_rel)

        if database is None:
            database = relinfo['database']

        # Choose a local port for our tunnel.
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("", 0))
        local_port = s.getsockname()[1]
        s.close()

        # Open the tunnel and wait for it to come up.
        # The new process group is to ensure we can reap all the ssh
        # tunnels, as simply killing the 'juju ssh' process doesn't seem
        # to be enough.
        tunnel_cmd = [
            'juju', 'ssh', 'client/0', '-q', '-N', '-L',
            '{}:{}:{}'.format(local_port, relinfo['host'], relinfo['port'])]
        tunnel_proc = subprocess.Popen(
            tunnel_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, preexec_fn=os.setpgrp)
        tunnel_proc.stdin.close()

        self.addCleanup(os.killpg, tunnel_proc.pid, signal.SIGTERM)
        self.addCleanup(tunnel_proc.kill)  # Holds a reference too.

        timeout = time.time() + 60
        while True:
            time.sleep(1)
            assert tunnel_proc.poll() is None, 'Tunnel died {!r}'.format(
                tunnel_proc.stdout)
            try:
                socket.create_connection(('localhost', local_port)).close()
                break
            except socket.error:
                if time.time() > timeout:
                    # Its not going to work. Per Bug #802117, this
                    # is likely an invalid host key forcing
                    # tunnelling to be disabled.
                    # TODO: We could use nc instead of an ssh tunnel.
                    raise

        return psycopg2.connect(
            port=local_port, host='localhost', database=database,
            user=relinfo['user'], password=relinfo['password'])

    def test_db_relation(self):
        con = self.connect()
        cur = con.cursor()
        cur.execute('SELECT TRUE')
        cur.fetchone()

    def test_db_admin_relation(self):
        con = self.connect(admin=True)
        con.autocommit = True
        cur = con.cursor()
        cur.execute('CREATE DATABASE newone')

        # db-admin relations can connect to any database.
        # (use postgres rather than the one we just created to avoid
        # PostgreSQL races).
        con = self.connect(admin=True, database='postgres')
        cur = con.cursor()
        cur.execute('SELECT TRUE')
        cur.fetchone()



class PG93Tests(PostgreSQLCharmBaseTestCase, unittest.TestCase):
    test_config = dict(version=(None if SERIES == 'trusty' else '9.3'),
                       pgdg=(False if SERIES == 'trusty' else True))
    num_units = 1


class PG94Tests(PostgreSQLCharmBaseTestCase, unittest.TestCase):
    test_config = dict(version='9.4',
                       pgdg=True)
    num_units = 1


def setUpModule():
    # Mirror charmhelpers into our support charms, since charms
    # can't symlink out of their subtree.
    main_charmhelpers = os.path.abspath(os.path.join(HERE, os.pardir,
                                                     'hooks', 'charmhelpers'))
    test_client_charmhelpers = os.path.join(CLIENT_CHARMDIR,
                                            'hooks', 'charmhelpers')
    if os.path.exists(test_client_charmhelpers):
        shutil.rmtree(test_client_charmhelpers)
    shutil.copytree(main_charmhelpers, test_client_charmhelpers)


def tearDownModule():
    test_client_charmhelpers = os.path.join(CLIENT_CHARMDIR,
                                            'hooks', 'charmhelpers')
    if os.path.exists(test_client_charmhelpers):
        shutil.rmtree(test_client_charmhelpers)
