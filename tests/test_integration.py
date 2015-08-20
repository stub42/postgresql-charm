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
from datetime import datetime
import os.path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

import psycopg2
import yaml

HERE = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.join(HERE, os.pardir)))

from testing.amuletfixture import AmuletFixture


SERIES = os.environ.get('SERIES', 'trusty').strip()
CLIENT_CHARMDIR = os.path.abspath(os.path.join(HERE, os.pardir,
                                               'lib', 'pgclient'))
assert os.path.isdir(CLIENT_CHARMDIR)


def skip_if_swift_is_unavailable(f):
    os_keys = set(['OS_TENANT_NAME', 'OS_AUTH_URL',
                   'OS_USERNAME', 'OS_PASSWORD'])
    for os_key in os_keys:
        if os_key not in os.environ:
            return unittest.skip('Swift is unavailable - '
                                 '{} envvar is unset'.format(os_key))
    return f


def skip_if_s3_is_unavailable(f):
    os_keys = set(['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY'])
    for os_key in os_keys:
        if os_key not in os.environ:
            return unittest.skip('S3 is unavailable')
    return f


def skip_if_wabs_is_unavailable(f):
    os_keys = set(['WABS_ACCOUNT_NAME', 'WABS_ACCESS_KEY'])
    for os_key in os_keys:
        if os_key not in os.environ:
            return unittest.skip('WABS is unavailable')
    return f


class PGBaseTestCase(object):
    deployment = None  # Module scoped AmuletFixture()

    common_config = dict()

    # Override these in subclasses to run these tests multiple times
    # for different PostgreSQL versions.
    test_config = None
    num_units = 1
    nagios_subordinate = False
    storage_subordinate = False

    @classmethod
    def setUpClass(cls, postgresql_charm_dir=None):
        super(PGBaseTestCase, cls).setUpClass()

        # Set up the AmuletFixture. It would be nice to share some
        # of this setup with a module level fixture, but unfortunately
        # Amulet does not let us add services after the initial deploy.
        deployment = AmuletFixture(series=SERIES)
        deployment.charm_dir = postgresql_charm_dir
        deployment.setUp()
        cls.deployment = deployment

        # Add the helper charm. We need this to act as a proxy,
        # so connections come from a unit the PostgreSQL charm recognizes.
        deployment.add('client', CLIENT_CHARMDIR)

        # Add and configure the PostgreSQL units.
        deployment.add('postgresql', postgresql_charm_dir,
                       units=cls.num_units,
                       constraints=dict(mem="512M"))
        deployment.expose('postgresql')
        config = dict(cls.common_config)
        config.update(cls.test_config)
        deployment.configure('postgresql', config)

        # Relate it to the client service.
        deployment.relate('postgresql:db', 'client:db')
        deployment.relate('postgresql:db-admin', 'client:db-admin')

        # Add the nagios subordinate to exercise the nrpe hooks.
        if cls.nagios_subordinate:
            deployment.add('nrpe', 'cs:trusty/nrpe')
            deployment.relate('postgresql:nrpe-external-master',
                              'nrpe:nrpe-external-master')

        # Add a storage subordinate. Defaults just use local disk.
        # We need to use an unofficial branch, as there is not yet
        # an official branch of the storage charm for trusty.
        if cls.storage_subordinate:
            deployment.add('storage', 'lp:~stub/charms/trusty/storage/trunk')
            deployment.relate('postgresql:data', 'storage:data')

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
        super(PGBaseTestCase, cls).setUpClass()

    def _get_config(self):
        raw = subprocess.check_output(['juju', 'get', 'postgresql'],
                                      universal_newlines=True)
        settings = yaml.safe_load(raw)['settings']
        return {k: settings[k]['value'] for k in settings.keys()}

    def setUp(self):
        starting_config = self._get_config()

        def _maybe_reset_config():
            # Reset any changed configuration.
            current_config = self._get_config()
            if current_config != starting_config:
                conf = {k: v for k, v in starting_config.items()
                        if starting_config[k] != current_config[k]}
                self.deployment.configure('postgresql', conf)
                self.deployment.wait()

        self.addCleanup(_maybe_reset_config)

    @property
    def master(self):
        status = self.deployment.get_status()
        for unit, info in status['services']['postgresql']['units'].items():
            status_message = info['workload-status']['message']
            if status_message == 'Live master':
                return unit
        self.fail("There is no master")

    @property
    def secondaries(self):
        status = self.deployment.get_status()
        units = status['services']['postgresql']['units']
        return set([unit for unit, info in units.items()
                    if info['workload-status']['message'] == 'Live master'])

    @property
    def secondary(self):
        secondaries = self.secondaries
        if secondaries:
            return list(secondaries)[0]
        return None

    @property
    def units(self):
        status = self.deployment.get_status()
        return set(status['services']['postgresql']['units'].keys())

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
                    raise

        return psycopg2.connect(
            port=local_port, host='localhost', database=database,
            user=relinfo['user'], password=relinfo['password'])

    def test_db_relation(self):
        for unit in self.units:
            with self.subTest(unit=unit):
                con = self.connect(unit)
                cur = con.cursor()
                cur.execute('SELECT TRUE')
                cur.fetchone()

    def test_db_admin_relation(self):
        for unit in self.units:
            with self.subTest(unit=unit):
                con = self.connect(unit, admin=True)
                con.autocommit = True
                cur = con.cursor()
                cur.execute('SELECT * FROM pg_stat_activity')

                # db-admin relations can connect to any database.
                con = self.connect(unit, admin=True, database='postgres')
                cur = con.cursor()
                cur.execute('SELECT * FROM pg_stat_activity')
                cur.fetchone()


class PGMultiBaseTestCase(PGBaseTestCase):
    num_units = 2

    def _replication_test(self):
        con = self.connect(self.master)
        con.autocommit = True
        cur = con.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS tokens (x text)')
        token = str(uuid.uuid1())
        cur.execute('INSERT INTO tokens(x) VALUES (%s)', (token,))

        for secondary in self.secondaries:
            with self.subTest(secondary=secondary):
                con = self.connect(secondary)
                con.autocommit = True
                cur = con.cursor()
                timeout = time.time() + 10
                while True:
                    try:
                        cur.execute('SELECT TRUE FROM tokens WHERE x=%s',
                                    (token,))
                        break
                    except psycopg2.Error:
                        if time.time() > timeout:
                            raise
                self.assertTrue(cur.fetchone()[0])

    def test_replication(self):
        self._replication_test()

    def test_failover(self):
        self.deployment.destroy_unit(self.master)
        self.deployment.add_unit('postgresql')
        self.deployment.wait()
        self._replication_test()

    @skip_if_swift_is_unavailable
    def test_wal_e_swift_logshipping(self):
        now = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        container = '_juju_pg_tests'

        config = dict(streaming_replication=False,
                      wal_e_storage_uri='swift://{}/{}'.format(container, now))

        # OpenStack credentials
        os_keys = set(['OS_TENANT_NAME', 'OS_AUTH_URL',
                       'OS_USERNAME', 'OS_PASSWORD'])
        for os_key in os_keys:
            config[os_key.lower()] = os.environ[os_key]

        # The swift command line tool uses the same environment variables
        # as this test suite.
        self.addCleanup(subprocess.call,
                        ['swift', 'delete', container],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.STDOUT,
                        universal_newlines=True)

        self.deployment.configure('postgresql', config)
        self.deployment.wait()

        # Confirm that the slave has not opened a streaming
        # replication connection.
        con = self.connect(self.master, admin=True)
        con.autocommit = True
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM pg_stat_replication")
        self.assertEqual(cur.fetchone()[0], 0, 'Streaming connection found')

        # Confirm that replication is actually happening.
        # Create a table and force a WAL change.
        cur.execute("CREATE TABLE wale AS SELECT generate_series(0,100)")
        cur.execute("SELECT pg_switch_xlog()")
        self.addCleanup(cur.execute, 'DROP TABLE wale')

        con = self.connect(self.secondary, admin=True)
        con.autocommit = True
        cur = con.cursor()
        timeout = time.time() + 120
        table_found = False
        while time.time() < timeout and not table_found:
            time.sleep(1)
            cur.execute("SELECT COUNT(*) FROM pg_class WHERE relname='wale'")
            table_found = cur.fetchone()[0] == 1
        self.assertTrue(table_found, "Replication not replicating")


class PG91Tests(PGBaseTestCase, unittest.TestCase):
    test_config = dict(version=(None if SERIES == 'precise' else '9.1'),
                       pgdg=(False if SERIES == 'precise' else True))


class PG91MultiTests(PGMultiBaseTestCase, unittest.TestCase):
    test_config = dict(version=(None if SERIES == 'precise' else '9.1'),
                       pgdg=(False if SERIES == 'precise' else True))


class PG92Tests(PGBaseTestCase, unittest.TestCase):
    test_config = dict(version='9.2', pgdg=True)


class PG92MultiTests(PGBaseTestCase, unittest.TestCase):
    test_config = dict(version='9.2', pgdg=True)


class PG93Tests(PGBaseTestCase, unittest.TestCase):
    test_config = dict(version=(None if SERIES == 'trusty' else '9.3'),
                       pgdg=(False if SERIES == 'trusty' else True))


class PG93MultiTests(PGMultiBaseTestCase, unittest.TestCase):
    storage_subordinate = True
    nagios_subordinate = True
    test_config = dict(version=(None if SERIES == 'trusty' else '9.3'),
                       pgdg=(False if SERIES == 'trusty' else True))


class PG94Tests(PGBaseTestCase, unittest.TestCase):
    test_config = dict(version=(None if SERIES == 'wily' else '9.4'),
                       pgdg=(False if SERIES == 'wily' else True))


class PG94MultiTests(PGMultiBaseTestCase, unittest.TestCase):
    num_units = 3
    test_config = dict(version=(None if SERIES == 'wily' else '9.4'),
                       pgdg=(False if SERIES == 'wily' else True))


class UpgradedCharmTests(PGBaseTestCase, unittest.TestCase):
    num_units = 2  # Old charm only supported 2 unit initial deploy.
    test_config = dict(version=None)
    storage_subordinate = True
    nagios_subordinate = False  # Nagios was broken with the old revision.

    @classmethod
    def setUpClass(cls):
        # Ensure an old version of the charm is first installed (but not
        # too old!). This version was what we internally recommended
        # before the rewrite to support Juju leadership and unit status,
        # and you can tell the correct version is deployed as the unit
        # status will remain 'unknown'.
        old_charm_dir = tempfile.mkdtemp(suffix='.charm')
        try:
            subprocess.check_call(['bzr', 'checkout', '-q', '--lightweight',
                                   '-r', '127', 'lp:charms/trusty/postgresql',
                                   old_charm_dir])
            super(UpgradedCharmTests, cls).setUpClass(old_charm_dir)
        finally:
            shutil.rmtree(old_charm_dir)

        # Replace the pre-leadership charm in the repo with this version,
        # so we can upgrade.
        cls.deployment.charm_dir = None
        cls.deployment.repackage_charm()
        repo_path = os.path.join(os.environ['JUJU_REPOSITORY'], SERIES,
                                 'postgresql')
        shutil.rmtree(repo_path)
        shutil.copytree(cls.deployment.charm_dir, repo_path)

        # Upgrade.
        subprocess.check_call(['juju', 'upgrade-charm', 'postgresql'],
                              stdout=subprocess.DEVNULL,
                              universal_newlines=True)
        cls.deployment.wait()


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
