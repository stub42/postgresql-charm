#!/usr/bin/python

"""
Test the PostgreSQL charm.

Usage:
    juju bootstrap
    TEST_TIMEOUT=900 ./test.py -v
    juju destroy-environment
"""

import os.path
import socket
import subprocess
import time
import unittest

import fixtures
import psycopg2
import testtools
from testtools.content import text_content
import yaml

from testing.jujufixture import JujuFixture, run


SERIES = 'precise'
TEST_CHARM = 'local:postgresql'
PSQL_CHARM = 'local:postgresql-psql'


class PostgreSQLCharmBaseTestCase(object):

    VERSION = '9.1'

    # We don't destroy databases (and their data) when destroying a unit
    # or service. We may end up with old PG databases still configured,
    # from tests of previous PG versions run on the same machine.
    # To work around this, we specify a unique port per PG version.
    PORT = 5432

    def setUp(self):
        super(PostgreSQLCharmBaseTestCase, self).setUp()

        # Generate a basic config for all PostgreSQL charm deploys.
        # Tests may add or change options.
        self.pg_config = dict(version=self.VERSION, listen_port=self.PORT)

        # If we are not using the default version of PostgreSQL for
        # the Ubuntu release we are testing on, add the PostgreSQL
        # Global Development Group APT archive to make the official
        # backports available.
        if SERIES == 'precise' and self.VERSION == '9.1':
            self.pg_config['pgdg'] = False
        elif SERIES == 'trusty' and self.VERSION == '9.3':
            self.pg_config['pgdg'] = False
        else:
            self.pg_config['pgdg'] = True

        self.juju = self.useFixture(JujuFixture(
            reuse_machines=True,
            do_teardown='TEST_DONT_TEARDOWN_JUJU' not in os.environ))

        # If the charms fail, we don't want tests to hang indefinitely.
        timeout = int(os.environ.get('TEST_TIMEOUT', 900))
        if timeout > 0:
            self.useFixture(fixtures.Timeout(timeout, gentle=True))

    def sql(self, sql, postgres_unit=None, psql_unit=None, dbname=None):
        '''Run some SQL on postgres_unit from psql_unit.

        Uses a random psql_unit and postgres_unit if not specified.

        postgres_unit may be set to an explicit unit name, 'master' or
        'hot standby'.

        A db-admin relation is used if dbname is specified. Otherwise,
        a standard db relation is used.
        '''
        if psql_unit is None:
            psql_unit = (
                self.juju.status['services']['psql']['units'].keys()[0])

        # The psql statements we are going to execute.
        sql = sql.strip()
        if not sql.endswith(';'):
            sql += ';'
        sql += '\n\\q\n'

        # The command we run to connect psql to the desired database.
        if postgres_unit is None:
            postgres_unit = (
                self.juju.status['services']['postgresql']['units'].keys()[0])
        elif postgres_unit == 'hot standby':
            postgres_unit = 'hot-standby'  # Munge for generating script name.
        if dbname is None:
            psql_cmd = [
                'psql-db-{}'.format(postgres_unit.replace('/', '-'))]
        else:
            psql_cmd = [
                'psql-db-admin-{}'.format(
                    postgres_unit.replace('/', '-')), '-d', dbname]
        psql_args = [
            '--quiet', '--tuples-only', '--no-align', '--no-password',
            '--field-separator=,', '--file=-']
        cmd = [
            'juju', 'ssh', psql_unit,
            # Due to Bug #1191079, we need to send the whole remote command
            # as a single argument.
            ' '.join(psql_cmd + psql_args)]
        out = run(self, cmd, input=sql)
        result = [line.split(',') for line in out.splitlines()]
        self.addDetail('sql', text_content(repr((sql, result))))
        return result

    def pg_ctlcluster(self, unit, command):
        cmd = [
            'juju', 'ssh', unit,
            # Due to Bug #1191079, we need to send the whole remote command
            # as a single argument.
            'sudo pg_ctlcluster 9.1 main -force {}'.format(command)]
        run(self, cmd)

    def test_basic(self):
        '''Connect to a a single unit service via the db relationship.'''
        self.juju.deploy(TEST_CHARM, 'postgresql', config=self.pg_config)
        self.juju.deploy(PSQL_CHARM, 'psql')
        self.juju.do(['add-relation', 'postgresql:db', 'psql:db'])
        self.juju.wait_until_ready()

        result = self.sql('SELECT TRUE')
        self.assertEqual(result, [['t']])

    def test_basic_admin(self):
        '''Connect to a single unit service via the db-admin relationship.'''
        self.juju.deploy(TEST_CHARM, 'postgresql', config=self.pg_config)
        self.juju.deploy(PSQL_CHARM, 'psql')
        self.juju.do(['add-relation', 'postgresql:db-admin', 'psql:db-admin'])
        self.juju.wait_until_ready()

        result = self.sql('SELECT TRUE', dbname='postgres')
        self.assertEqual(result, [['t']])

    def is_master(self, postgres_unit, dbname=None):
        is_master = self.sql(
            'SELECT NOT pg_is_in_recovery()',
            postgres_unit, dbname=dbname)[0][0]
        return (is_master == 't')

    def test_failover(self):
        """Set up a multi-unit service and perform failovers."""
        self.juju.deploy(
            TEST_CHARM, 'postgresql', num_units=3, config=self.pg_config)
        self.juju.deploy(PSQL_CHARM, 'psql')
        self.juju.do(['add-relation', 'postgresql:db', 'psql:db'])
        self.juju.wait_until_ready()

        # Even on a freshly setup service, we have no idea which unit
        # will become the master as we have no control over which two
        # units join the peer relation first.
        units = sorted(
            (self.is_master(unit), unit) for unit in
            self.juju.status['services']['postgresql']['units'].keys())
        self.assertFalse(units[0][0])
        self.assertFalse(units[1][0])
        self.assertTrue(units[2][0])
        standby_unit_1 = units[0][1]
        standby_unit_2 = units[1][1]
        master_unit = units[2][1]

        self.sql('CREATE TABLE Token (x int)', master_unit)

        # Some simple helper to send data via the master and check if it
        # was replicated to the hot standbys.
        _counter = [0]

        def send_token(unit):
            _counter[0] += 1
            self.sql("INSERT INTO Token VALUES (%d)" % _counter[0], unit)

        def token_received(unit):
            # async replocation can lag, so retry for a little while to
            # give the databases a chance to get their act together.
            start = time.time()
            timeout = start + 60
            while time.time() <= timeout:
                r = self.sql(
                    "SELECT TRUE FROM Token WHERE x=%d" % _counter[0], unit)
                if r == [['t']]:
                    return True
            return False

        # Confirm that replication is actually happening.
        send_token(master_unit)
        self.assertIs(True, token_received(standby_unit_1))
        self.assertIs(True, token_received(standby_unit_2))

        # Remove the master unit.
        self.juju.do(['remove-unit', master_unit])
        self.juju.wait_until_ready()

        # When we failover, the unit that has received the most WAL
        # information from the old master (most in sync) is elected the
        # new master.
        standby_unit_1_is_master = self.is_master(standby_unit_1)
        standby_unit_2_is_master = self.is_master(standby_unit_2)
        self.assertNotEqual(
            standby_unit_1_is_master, standby_unit_2_is_master)

        if standby_unit_1_is_master:
            master_unit = standby_unit_1
            standby_unit = standby_unit_2
        else:
            master_unit = standby_unit_2
            standby_unit = standby_unit_1

        # Confirm replication is still working.
        send_token(master_unit)
        self.assertIs(True, token_received(standby_unit))

        # Remove the master again, leaving a single unit.
        self.juju.do(['remove-unit', master_unit])
        self.juju.wait_until_ready()

        # Last unit is a working, standalone database.
        self.is_master(standby_unit)
        send_token(standby_unit)

        # We can tell it is correctly reporting that it is standalone by
        # seeing if the -master and -hot-standby scripts no longer exist
        # on the psql unit.
        self.assertRaises(
            subprocess.CalledProcessError,
            self.sql, 'SELECT TRUE', 'master')
        self.assertRaises(
            subprocess.CalledProcessError,
            self.sql, 'SELECT TRUE', 'hot standby')

    def test_failover_election(self):
        """Ensure master elected in a failover is the best choice"""
        self.juju.deploy(
            TEST_CHARM, 'postgresql', num_units=3, config=self.pg_config)
        self.juju.deploy(PSQL_CHARM, 'psql')
        self.juju.do(['add-relation', 'postgresql:db-admin', 'psql:db-admin'])
        self.juju.wait_until_ready()

        # Even on a freshly setup service, we have no idea which unit
        # will become the master as we have no control over which two
        # units join the peer relation first.
        units = sorted(
            (self.is_master(unit, 'postgres'), unit) for unit in
            self.juju.status['services']['postgresql']['units'].keys())
        self.assertFalse(units[0][0])
        self.assertFalse(units[1][0])
        self.assertTrue(units[2][0])
        standby_unit_1 = units[0][1]
        standby_unit_2 = units[1][1]
        master_unit = units[2][1]

        # Shutdown PostgreSQL on standby_unit_1 and ensure
        # standby_unit_2 will have received more WAL information from
        # the master.
        self.pg_ctlcluster(standby_unit_1, 'stop')
        self.sql("SELECT pg_switch_xlog()", master_unit, dbname='postgres')

        # Break replication so when we bring standby_unit_1 up, it has
        # no way of catching up.
        self.sql(
            "ALTER ROLE juju_replication NOREPLICATION",
            master_unit, dbname='postgres')
        self.pg_ctlcluster(master_unit, 'restart')

        # Restart standby_unit_1 now it has no way or resyncing.
        self.pg_ctlcluster(standby_unit_1, 'start')

        # Failover.
        self.juju.do(['remove-unit', master_unit])
        self.juju.wait_until_ready()

        # Fix replication.
        self.sql(
            "ALTER ROLE juju_replication REPLICATION",
            standby_unit_2, dbname='postgres')

        # Ensure the election went as predicted.
        self.assertIs(True, self.is_master(standby_unit_2, 'postgres'))
        self.assertIs(False, self.is_master(standby_unit_1, 'postgres'))

    def test_admin_addresses(self):
        self.juju.deploy(TEST_CHARM, 'postgresql', config=self.pg_config)
        self.juju.deploy(PSQL_CHARM, 'psql')
        self.juju.do(['add-relation', 'postgresql:db-admin', 'psql:db-admin'])
        self.juju.wait_until_ready()

        # We need to determine the IP address that the unit will see.
        unit = self.juju.status['services']['postgresql']['units'].keys()[0]
        unit_ip = self.juju.status['services']['postgresql']['units'][
            unit]['public-address']
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((unit_ip, self.PORT))
        my_ip = s.getsockname()[0]
        del s

        # We also need to set a password.
        self.sql(
            "ALTER USER postgres ENCRYPTED PASSWORD 'foo'", dbname='postgres')

        # Direct connection string to the unit's database.
        conn_str = 'dbname=postgres user=postgres password=foo host={}'.format(
            unit_ip)

        # Direct database connections should fail at the moment.
        self.assertRaises(
            psycopg2.OperationalError, psycopg2.connect, conn_str)

        # Connections should work after setting the admin-addresses.
        self.juju.do([
            'set', 'postgresql', 'admin_addresses={}'.format(my_ip)])
        self.juju.wait_until_ready()
        con = psycopg2.connect(conn_str)
        cur = con.cursor()
        cur.execute('SELECT 1')
        self.assertEquals(1, cur.fetchone()[0])

    def test_explicit_database(self):
        self.juju.deploy(TEST_CHARM, 'postgresql', config=self.pg_config)
        self.juju.deploy(PSQL_CHARM, 'psql')
        self.juju.do(['set', 'psql', 'database=explicit'])
        self.juju.do(['add-relation', 'postgresql:db', 'psql:db'])
        self.juju.wait_until_ready()

        result = self.sql('SELECT current_database()')
        self.assertEqual(result, [['explicit']])


    def test_roles_granted(self):
        self.juju.deploy(TEST_CHARM, 'postgresql', config=self.pg_config)
        self.juju.deploy(PSQL_CHARM, 'psql')
        self.juju.do(['set', 'psql', 'roles=role_a'])
        self.juju.do(['add-relation', 'postgresql:db', 'psql:db'])
        self.juju.wait_until_ready()

        result = self.sql('''
            SELECT pg_has_role(current_user, 'role_a', 'MEMBER')
            ''')
        self.assertEqual(result, [['t']])

        self.juju.do(['set', 'psql', 'roles=role_a,role_b'])
        self.juju.wait_until_ready()

        result = self.sql('''
            SELECT
                pg_has_role(current_user, 'role_a', 'MEMBER'),
                pg_has_role(current_user, 'role_b', 'MEMBER')
            ''')
        self.assertEqual(result, [['t', 't']])

    def test_roles_revoked(self):
        self.juju.deploy(TEST_CHARM, 'postgresql', config=self.pg_config)
        self.juju.deploy(PSQL_CHARM, 'psql')
        self.juju.do(['set', 'psql', 'roles=role_a,role_b'])
        self.juju.do(['add-relation', 'postgresql:db', 'psql:db'])
        self.juju.wait_until_ready()

        result = self.sql('''
            SELECT
                pg_has_role(current_user, 'role_a', 'MEMBER'),
                pg_has_role(current_user, 'role_b', 'MEMBER')
            ''')
        self.assertEqual(result, [['t', 't']])

        self.juju.do(['set', 'psql', 'roles=role_c'])
        self.juju.wait_until_ready()

        result = self.sql('''
            SELECT
                pg_has_role(current_user, 'role_a', 'MEMBER'),
                pg_has_role(current_user, 'role_b', 'MEMBER'),
                pg_has_role(current_user, 'role_c', 'MEMBER')
            ''')
        self.assertEqual(result, [['f', 'f', 't']])

        self.juju.do(['unset', 'psql', 'roles'])
        self.juju.wait_until_ready()

        result = self.sql('''
            SELECT
                pg_has_role(current_user, 'role_a', 'MEMBER'),
                pg_has_role(current_user, 'role_b', 'MEMBER'),
                pg_has_role(current_user, 'role_c', 'MEMBER')
            ''')
        self.assertEqual(result, [['f', 'f', 'f']])


class PG91Tests(
        PostgreSQLCharmBaseTestCase,
        testtools.TestCase, fixtures.TestWithFixtures):
    VERSION = '9.1'
    PORT = 5431


class PG92Tests(
        PostgreSQLCharmBaseTestCase,
        testtools.TestCase, fixtures.TestWithFixtures):
    VERSION = '9.2'
    PORT = 5432


class PG93Tests(
        PostgreSQLCharmBaseTestCase,
        testtools.TestCase, fixtures.TestWithFixtures):
    VERSION = '9.3'
    PORT = 5433


def unit_sorted(units):
    """Return a correctly sorted list of unit names."""
    return sorted(
        units, lambda a, b: cmp(int(a.split('/')[-1]), int(b.split('/')[-1])))


if __name__ == '__main__':
    raise SystemExit(unittest.main())
