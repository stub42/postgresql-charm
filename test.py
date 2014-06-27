#!/usr/bin/python

"""
Test the PostgreSQL charm.

Usage:
    juju bootstrap
    TEST_TIMEOUT=900 ./test.py -v
    juju destroy-environment
"""

import os.path
import signal
import socket
import subprocess
import time
import unittest
import uuid

import fixtures
import psycopg2
import testtools

from testing.jujufixture import JujuFixture, run


SERIES = os.environ.get('SERIES', 'precise').strip()
TEST_CHARM = 'local:{}/postgresql'.format(SERIES)
PSQL_CHARM = 'local:{}/postgresql-psql'.format(SERIES)


class NotReady(Exception):
    pass


class PostgreSQLCharmBaseTestCase(object):

    # Override these in subclasses to run these tests multiple times
    # for different PostgreSQL versions.

    # PostgreSQL version for tests. One of the subclasses leaves the
    # VERSION as None to test automatic version selection.
    VERSION = None

    # Use the PGDG Apt archive or not. One of the subclasses sets this
    # to False to test the Ubuntu main packages. The rest set this to
    # True to pull packages from the PGDG (only one PostgreSQL version
    # exists in main).
    PGDG = None

    def setUp(self):
        super(PostgreSQLCharmBaseTestCase, self).setUp()

        # Generate a basic config for all PostgreSQL charm deploys.
        # Tests may add or change options.
        self.pg_config = dict(version=self.VERSION, pgdg=self.PGDG)

        self.juju = self.useFixture(JujuFixture(
            series=SERIES, reuse_machines=True,
            do_teardown='TEST_DONT_TEARDOWN_JUJU' not in os.environ))

        # If the charms fail, we don't want tests to hang indefinitely.
        timeout = int(os.environ.get('TEST_TIMEOUT', 900))
        if timeout > 0:
            self.useFixture(fixtures.Timeout(timeout, gentle=True))

    def wait_until_ready(self, pg_units, relation=True):

        # Per Bug #1200267, it is impossible to know when a juju
        # environment is actually ready for testing. Instead, we do the
        # best we can by inspecting all the relation state, and, if it
        # is at this particular instant in the expected state, hoping
        # that the system is stable enough to continue testing.

        timeout = time.time() + 180
        pg_units = frozenset(pg_units)

        # The list of PG units we expect to be related to the psql unit.
        if relation:
            rel_pg_units = frozenset(pg_units)
        else:
            rel_pg_units = frozenset()

        while True:
            try:
                self.juju.wait_until_ready(0)  # Also refreshes status

                status_pg_units = set(self.juju.status[
                    'services']['postgresql']['units'].keys())

                if pg_units != status_pg_units:
                    # Confirm the PG units reported by 'juju status'
                    # match the list we expect.
                    raise NotReady('units not yet added/removed')

                for psql_unit in self.juju.status['services']['psql']['units']:
                    self.confirm_psql_unit_ready(psql_unit, rel_pg_units)

                for pg_unit in pg_units:
                    peers = [u for u in pg_units if u != pg_unit]
                    self.confirm_postgresql_unit_ready(pg_unit, peers)

                return
            except NotReady:
                if time.time() > timeout:
                    raise
                time.sleep(3)

    def confirm_psql_unit_ready(self, psql_unit, pg_units):
        # Confirm the db and db-admin relations are all in a useful
        # state.
        psql_rel_info = self.juju.relation_info(psql_unit)
        if pg_units and not psql_rel_info:
            raise NotReady('{} waiting for relations'.format(psql_unit))
        elif not pg_units and psql_rel_info:
            raise NotReady('{} waiting to drop relations'.format(psql_unit))
        elif not pg_units and not psql_rel_info:
            return

        psql_service = psql_unit.split('/', 1)[0]

        # The set of PostgreSQL units related to the psql unit. They
        # might be related via several db or db-admin relations.
        all_rel_pg_units = set()

        for rel_name in psql_rel_info:
            for rel_id, rel_info in psql_rel_info[rel_name].items():

                # The database this relation has requested to use, if any.
                requested_db = rel_info[psql_unit].get('database', None)

                rel_pg_units = (
                    [u for u in rel_info if not u.startswith(psql_service)])
                all_rel_pg_units = all_rel_pg_units.union(rel_pg_units)

                num_masters = 0

                for unit in rel_pg_units:
                    unit_rel_info = rel_info[unit]

                    # PG unit must be presenting the correct database.
                    if 'database' not in unit_rel_info:
                        raise NotReady(
                            '{} has no database'.format(unit))
                    if requested_db and (
                            unit_rel_info['database'] != requested_db):
                        raise NotReady(
                            '{} not using requested db {}'.format(
                                unit, requested_db))

                    # PG unit must be in a valid state.
                    state = unit_rel_info.get('state', None)
                    if not state:
                        raise NotReady(
                            '{} has no state'.format(unit))
                    elif state == 'standalone':
                        if len(rel_pg_units) > 1:
                            raise NotReady(
                                '{} is standalone'.format(unit))
                    elif state == 'master':
                        num_masters += 1
                    elif state != 'hot standby':
                        # Failover state or totally broken.
                        raise NotReady(
                            '{} in {} state'.format(unit, state))

                    # PG unit must have authorized this psql client.
                    allowed_units = unit_rel_info.get(
                        'allowed-units', '').split()
                    if psql_unit not in allowed_units:
                        raise NotReady(
                            '{} not yet authorized by {} ({})'.format(
                                psql_unit, unit, allowed_units))

                # We must not have multiple masters in this relation.
                if len(rel_pg_units) > 1 and num_masters != 1:
                    raise NotReady(
                        '{} masters'.format(num_masters))

        if pg_units != all_rel_pg_units:
            raise NotReady(
                'Expected PG units {} != related units {}'.format(
                    pg_units, all_rel_pg_units))

    def confirm_postgresql_unit_ready(self, pg_unit, peers=()):
        pg_rel_info = self.juju.relation_info(pg_unit)
        if not pg_rel_info:
            raise NotReady('{} has no relations'.format(pg_unit))

        try:
            rep_rel_id = pg_rel_info['replication'].keys()[0]
            actual_peers = set([
                u for u in pg_rel_info['replication'][rep_rel_id].keys()
                if u != pg_unit])
        except (IndexError, KeyError):
            if peers:
                raise NotReady('Peer relation does not exist')
            rep_rel_id = None
            actual_peers = set()

        if actual_peers != set(peers):
            raise NotReady('Expecting {} peers, found {}'.format(
                peers, actual_peers))

        if not peers:
            return

        pg_rep_rel_info = pg_rel_info['replication'][rep_rel_id].get(
            pg_unit, None)
        if not pg_rep_rel_info:
            raise NotReady('{} has not yet joined the peer relation'.format(
                pg_unit))

        state = pg_rep_rel_info.get('state', None)

        if not state:
            raise NotReady('{} has no state'.format(pg_unit))

        if state == 'standalone' and peers:
            raise NotReady('{} is standalone but has peers'.format(pg_unit))

        if state not in ('standalone', 'master', 'hot standby'):
            raise NotReady('{} reports failover in progress'.format(pg_unit))

        num_masters = 1 if state in ('master', 'standalone') else 0

        for peer in peers:
            peer_rel_info = pg_rel_info['replication'][rep_rel_id][peer]
            peer_state = peer_rel_info.get('state', None)
            if not peer_state:
                raise NotReady('{} has no peer state'.format(peer))
            if peer_state == 'master':
                num_masters += 1
            elif peer_state != 'hot standby':
                raise NotReady('Peer {} in state {}'.format(peer, peer_state))

        if num_masters != 1:
            raise NotReady('No masters seen from {}'.format(pg_unit))

    def sql(self, sql, postgres_unit=None, psql_unit=None, dbname=None):
        '''Run some SQL on postgres_unit from psql_unit.

        Uses a random psql_unit and postgres_unit if not specified.

        postgres_unit may be set to an explicit unit name, 'master' or
        'hot standby'.

        A db-admin relation is used if dbname is specified. Otherwise,
        a standard db relation is used.
        '''
        # Which psql unit we are going to query from.
        if psql_unit is None:
            psql_unit = (
                self.juju.status['services']['psql']['units'].keys()[0])

        full_rel_info = self.juju.relation_info(psql_unit)

        # 'db' or 'db-admin' relation?
        rel_name = 'db-admin' if dbname else 'db'

        # Which PostgreSQL unit we want to talk to.
        if postgres_unit is None:
            postgres_unit = (
                self.juju.status['services']['postgresql']['units'].keys()[0])
        elif postgres_unit in ('master', 'hot standby'):
            for rel_id, rel_info in full_rel_info[rel_name].items():
                for rel_unit, rel_unit_info in rel_info.items():
                    if rel_unit_info.get('state') == postgres_unit:
                        postgres_unit = rel_unit
        assert postgres_unit not in (None, 'master', 'hot standby'), (
            'Unable to determine postgresql unit to use')

        # PostgreSQL unit relation info
        rel_info = None
        for rel_id in full_rel_info[rel_name]:
            if postgres_unit in full_rel_info[rel_name][rel_id]:
                rel_info = full_rel_info[rel_name][rel_id][postgres_unit]
                break
        assert rel_info is not None, 'Unable to find pg rel info {!r}'.format(
            full_rel_info[rel_name])

        if dbname is None:
            dbname = rel_info['database']

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
            'juju', 'ssh', psql_unit, '-N', '-L',
            '{}:{}:{}'.format(local_port, rel_info['host'], rel_info['port'])]
        tunnel_proc = subprocess.Popen(
            tunnel_cmd, stdin=subprocess.PIPE, preexec_fn=os.setpgrp)
            # Don't disable stdout, so we can see when there are SSH
            # failures like bad host keys.
            #stdout=open('/dev/null', 'ab'), stderr=subprocess.STDOUT)
        tunnel_proc.stdin.close()

        try:
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

            # Execute the query
            con = psycopg2.connect(
                database=dbname, port=local_port, host='localhost',
                user=rel_info['user'], password=rel_info['password'])
            cur = con.cursor()
            cur.execute(sql)
            if cur.description is None:
                rv = None
            else:
                rv = cur.fetchall()
            con.commit()
            con.close()
            return rv
        finally:
            os.killpg(tunnel_proc.pid, signal.SIGTERM)
            tunnel_proc.kill()
            tunnel_proc.wait()

    def pg_ctlcluster(self, unit, command):
        cmd = [
            'juju', 'run', '--unit', unit,
            'sudo pg_ctlcluster 9.1 main -force {}'.format(command)]
        run(self, cmd)

    def test_basic(self):
        '''Connect to a a single unit service via the db relationship.'''
        self.juju.deploy(TEST_CHARM, 'postgresql', config=self.pg_config)
        self.juju.deploy(PSQL_CHARM, 'psql')
        self.juju.do(['add-relation', 'postgresql:db', 'psql:db'])
        self.wait_until_ready(['postgresql/0'])

        result = self.sql('SELECT TRUE')
        self.assertEqual(result, [(True,)])

        # Confirm that the relation tears down without errors.
        self.juju.do(['destroy-relation', 'postgresql:db', 'psql:db'])
        self.wait_until_ready(['postgresql/0'], relation=False)

    def test_streaming_replication(self):
        self.juju.deploy(
            TEST_CHARM, 'postgresql', num_units=2, config=self.pg_config)
        self.juju.deploy(PSQL_CHARM, 'psql')
        self.juju.do(['add-relation', 'postgresql:db', 'psql:db'])
        self.wait_until_ready(['postgresql/0', 'postgresql/1'])

        # Confirm that the slave has successfully opened a streaming
        # replication connection.
        num_slaves = self.sql(
            'SELECT COUNT(*) FROM pg_stat_replication',
            postgres_unit='master')[0][0]

        self.assertEqual(num_slaves, 1, 'Slave not connected')

    def test_basic_admin(self):
        '''Connect to a single unit service via the db-admin relationship.'''
        self.juju.deploy(TEST_CHARM, 'postgresql', config=self.pg_config)
        self.juju.deploy(PSQL_CHARM, 'psql')
        self.juju.do(['add-relation', 'postgresql:db-admin', 'psql:db-admin'])
        self.juju.do(['expose', 'postgresql'])
        self.wait_until_ready(['postgresql/0'])

        result = self.sql('SELECT TRUE', dbname='postgres')
        self.assertEqual(result, [(True,)])

        # Confirm that the relation tears down without errors.
        self.juju.do([
            'destroy-relation', 'postgresql:db-admin', 'psql:db-admin'])
        self.wait_until_ready(['postgresql/0'], relation=False)

    def is_master(self, postgres_unit, dbname=None):
        is_master = self.sql(
            'SELECT NOT pg_is_in_recovery()',
            postgres_unit, dbname=dbname)[0][0]
        return is_master

    def test_failover(self):
        """Set up a multi-unit service and perform failovers."""
        # Per Bug #1258485, creating a 3 unit service will often fail.
        # Instead, create a 2 unit service, wait for it to be ready,
        # then add a third unit.
        self.juju.deploy(PSQL_CHARM, 'psql')
        self.juju.deploy(
            TEST_CHARM, 'postgresql', num_units=2, config=self.pg_config)
        self.juju.do(['add-relation', 'postgresql:db', 'psql:db'])
        self.wait_until_ready(['postgresql/0', 'postgresql/1'])
        self.juju.add_unit('postgresql')
        self.wait_until_ready(['postgresql/0', 'postgresql/1', 'postgresql/2'])

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
            timeout = start + 30
            while time.time() <= timeout:
                r = self.sql(
                    "SELECT COUNT(*) FROM Token WHERE x=%d" % _counter[0],
                    unit)[0][0]
                if r >= 1:
                    return True
            return False

        # Confirm that replication is actually happening.
        send_token(master_unit)
        self.assertIs(True, token_received(standby_unit_1))
        self.assertIs(True, token_received(standby_unit_2))

        # Remove the master unit.
        self.juju.do(['remove-unit', master_unit])
        self.wait_until_ready([standby_unit_1, standby_unit_2])

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
        self.wait_until_ready([standby_unit])

        # Last unit is a working, standalone database.
        self.is_master(standby_unit)
        send_token(standby_unit)

        # Confirm that it is actually reporting as 'standalone' rather
        # than 'master'
        full_relation_info = self.juju.relation_info('psql/0')
        for rel_info in full_relation_info['db'].values():
            for unit, unit_rel_info in rel_info.items():
                if unit == 'psql/0':
                    pass
                elif unit == standby_unit:
                    self.assertEqual(unit_rel_info['state'], 'standalone')
                else:
                    raise RuntimeError('Unknown unit {}'.format(unit))

    def test_failover_election(self):
        """Ensure master elected in a failover is the best choice"""
        # Per Bug #1258485, creating a 3 unit service will often fail.
        # Instead, create a 2 unit service, wait for it to be ready,
        # then add a third unit.
        self.juju.deploy(
            TEST_CHARM, 'postgresql', num_units=2, config=self.pg_config)
        self.juju.deploy(PSQL_CHARM, 'psql')
        self.juju.do(['add-relation', 'postgresql:db-admin', 'psql:db-admin'])
        self.wait_until_ready(['postgresql/0', 'postgresql/1'])
        self.juju.add_unit('postgresql')
        self.wait_until_ready(['postgresql/0', 'postgresql/1', 'postgresql/2'])

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
        self.wait_until_ready([standby_unit_1, standby_unit_2])

        # Fix replication.
        self.sql(
            "ALTER ROLE juju_replication REPLICATION",
            'master', dbname='postgres')

        # Ensure the election went as predicted.
        self.assertIs(True, self.is_master(standby_unit_2, 'postgres'))
        self.assertIs(False, self.is_master(standby_unit_1, 'postgres'))

    def test_admin_addresses(self):

        # This test also tests explicit port assignment. We need
        # a different port for each PostgreSQL version we might be
        # testing, because clusters from previous tests of different
        # versions may be hanging around.
        port = 7400 + int((self.VERSION or '66').replace('.', ''))
        self.pg_config['listen_port'] = port

        self.juju.deploy(TEST_CHARM, 'postgresql', config=self.pg_config)
        self.juju.deploy(PSQL_CHARM, 'psql')
        self.juju.do(['add-relation', 'postgresql:db-admin', 'psql:db-admin'])
        self.wait_until_ready(['postgresql/0'])

        # Determine the IP address that the unit will see.
        unit = self.juju.status['services']['postgresql']['units'].keys()[0]
        unit_ip = self.juju.status['services']['postgresql']['units'][
            unit]['public-address']
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((unit_ip, port))
        my_ip = s.getsockname()[0]
        del s

        # We also need to set a password.
        self.sql(
            "ALTER USER postgres ENCRYPTED PASSWORD 'foo'", dbname='postgres')

        # Direct connection string to the unit's database.
        conn_str = (
            'dbname=postgres user=postgres password=foo '
            'host={} port={}'.format(unit_ip, port))

        # Direct database connections should fail at the moment.
        self.assertRaises(
            psycopg2.OperationalError, psycopg2.connect, conn_str)

        # Connections should work after setting the admin-addresses.
        self.juju.do([
            'set', 'postgresql', 'admin_addresses={}'.format(my_ip)])
        timeout = time.time() + 30
        while True:
            try:
                con = psycopg2.connect(conn_str)
                break
            except psycopg2.OperationalError:
                if time.time() > timeout:
                    raise
                time.sleep(0.25)
        cur = con.cursor()
        cur.execute('SELECT 1')
        self.assertEquals(1, cur.fetchone()[0])

    def test_explicit_database(self):
        # Two units to ensure both masters and hot standbys
        # present the correct credentials.
        self.juju.deploy(
            TEST_CHARM, 'postgresql', num_units=2, config=self.pg_config)
        self.juju.deploy(PSQL_CHARM, 'psql')
        self.juju.do(['set', 'psql', 'database=explicit'])
        self.juju.do(['add-relation', 'postgresql:db', 'psql:db'])

        pg_units = ['postgresql/0', 'postgresql/1']
        self.wait_until_ready(pg_units)

        for unit in pg_units:
            result = self.sql('SELECT current_database()', unit)[0][0]
            self.assertEqual(
                result, 'explicit',
                '{} reports incorrect db {}'.format(unit, result))

    def test_roles_granted(self):
        # We use two units to confirm that there is no attempt to
        # grant roles on the hot standby.
        self.juju.deploy(
            TEST_CHARM, 'postgresql', num_units=2, config=self.pg_config)
        self.juju.deploy(PSQL_CHARM, 'psql', config={'roles': 'role_a'})
        self.juju.do(['add-relation', 'postgresql:db', 'psql:db'])
        pg_units = ['postgresql/0', 'postgresql/1']
        self.wait_until_ready(pg_units)

        has_role_a = self.sql('''
            SELECT pg_has_role(current_user, 'role_a', 'MEMBER')
            ''')[0][0]
        self.assertTrue(has_role_a)

        self.juju.do(['set', 'psql', 'roles=role_a,role_b'])
        self.wait_until_ready(pg_units)

        # Retry this for a while. Per Bug #1200267, we can't tell when
        # the hooks have finished running and the role has been granted.
        # We could make the PostgreSQL charm provide feedback on when
        # the role has actually been granted and wait for that, but that
        # is complex as hot standbys need to wait until the master has
        # performed the grant and the grant has replicated.
        timeout = time.time() + 60
        while True:
            try:
                has_role_a, has_role_b = self.sql('''
                    SELECT
                        pg_has_role(current_user, 'role_a', 'MEMBER'),
                        pg_has_role(current_user, 'role_b', 'MEMBER')
                    ''')[0]
                break
            except psycopg2.ProgrammingError:
                if time.time() > timeout:
                    raise
        self.assertTrue(has_role_a)
        self.assertTrue(has_role_b)

    def test_roles_revoked(self):
        # We use two units to confirm that there is no attempts to
        # grant roles on the hot standby.
        self.juju.deploy(
            TEST_CHARM, 'postgresql', num_units=2, config=self.pg_config)
        self.juju.deploy(PSQL_CHARM, 'psql', config={'roles': 'role_a,role_b'})
        self.juju.do(['add-relation', 'postgresql:db', 'psql:db'])
        pg_units = ['postgresql/0', 'postgresql/1']
        self.wait_until_ready(pg_units)

        has_role_a, has_role_b = self.sql('''
            SELECT
                pg_has_role(current_user, 'role_a', 'MEMBER'),
                pg_has_role(current_user, 'role_b', 'MEMBER')
            ''')[0]
        self.assertTrue(has_role_a)
        self.assertTrue(has_role_b)

        self.juju.do(['set', 'psql', 'roles=role_c'])
        self.wait_until_ready(pg_units)

        # Per Bug #1200267, we have to retry a while here and hope.
        # We have of knowing when the pending role changes have
        # actually been applied.
        timeout = time.time() + 60
        while time.time() < timeout:
            has_role_a, has_role_b, has_role_c = self.sql('''
                SELECT
                    pg_has_role(current_user, 'role_a', 'MEMBER'),
                    pg_has_role(current_user, 'role_b', 'MEMBER'),
                    pg_has_role(current_user, 'role_c', 'MEMBER')
                ''')[0]
            if has_role_c:
                break
        self.assertFalse(has_role_a)
        self.assertFalse(has_role_b)
        self.assertTrue(has_role_c)

        self.juju.do(['unset', 'psql', 'roles'])
        self.wait_until_ready(pg_units)

        timeout = time.time() + 60
        while True:
            has_role_a, has_role_b, has_role_c = self.sql('''
                SELECT
                    pg_has_role(current_user, 'role_a', 'MEMBER'),
                    pg_has_role(current_user, 'role_b', 'MEMBER'),
                    pg_has_role(current_user, 'role_c', 'MEMBER')
                ''')[0]
            if not has_role_c:
                break
        self.assertFalse(has_role_a)
        self.assertFalse(has_role_b)
        self.assertFalse(has_role_c)

    def test_syslog(self):
        # Deploy 2 PostgreSQL units and 2 rsyslog units to ensure that
        # log messages from every source reach every sink.
        self.pg_config['log_min_duration_statement'] = 0  # Log all statements
        self.juju.deploy(
            TEST_CHARM, 'postgresql', num_units=2, config=self.pg_config)
        self.juju.deploy(PSQL_CHARM, 'psql')
        self.juju.do(['add-relation', 'postgresql:db', 'psql:db'])
        self.juju.deploy('cs:rsyslog', 'rsyslog', num_units=2)
        self.juju.do([
            'add-relation', 'postgresql:syslog', 'rsyslog:aggregator'])
        pg_units = ['postgresql/0', 'postgresql/1']
        self.wait_until_ready(pg_units)

        token = str(uuid.uuid1())

        self.sql("SELECT 'master {}'".format(token), 'master')
        self.sql("SELECT 'hot standby {}'".format(token), 'hot standby')
        time.sleep(2)

        for runit in ['rsyslog/0', 'rsyslog/1']:
            cmd = ['juju', 'run', '--unit', runit, 'tail -100 /var/log/syslog']
            out = run(self, cmd)
            self.failUnless('master {}'.format(token) in out)
            self.failUnless('hot standby {}'.format(token) in out)

        # Confirm that the relation tears down correctly.
        self.juju.do(['destroy-service', 'rsyslog'])
        timeout = time.time() + 120
        while time.time() < timeout:
            status = self.juju.refresh_status()
            if 'rsyslog' not in status['services']:
                break
        self.assert_(
            'rsyslog' not in status['services'], 'rsyslog failed to die')
        self.wait_until_ready(pg_units)


class PG91Tests(
        PostgreSQLCharmBaseTestCase,
        testtools.TestCase, fixtures.TestWithFixtures):
    # Test automatic version selection under precise.
    VERSION = None if SERIES == 'precise' else '9.1'
    PGDG = False if SERIES == 'precise' else True


class PG92Tests(
        PostgreSQLCharmBaseTestCase,
        testtools.TestCase, fixtures.TestWithFixtures):
    VERSION = '9.2'
    PGDG = True


class PG93Tests(
        PostgreSQLCharmBaseTestCase,
        testtools.TestCase, fixtures.TestWithFixtures):
    # Test automatic version selection under trusty.
    VERSION = None if SERIES == 'trusty' else '9.3'
    PGDG = False if SERIES == 'trusty' else True


class PG94Tests(
        PostgreSQLCharmBaseTestCase,
        testtools.TestCase, fixtures.TestWithFixtures):
    # 9.4 is still in beta, with packages only available in the PGDG
    # archive.
    VERSION = '9.4'
    PGDG = True


def unit_sorted(units):
    """Return a correctly sorted list of unit names."""
    return sorted(
        units, lambda a, b: cmp(int(a.split('/')[-1]), int(b.split('/')[-1])))


if __name__ == '__main__':
    raise SystemExit(unittest.main())
