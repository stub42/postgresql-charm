#!/usr/bin/python

"""
Test the PostgreSQL charm.

Usage:
    juju bootstrap
    TEST_DEBUG_FILE=test-debug.log TEST_TIMEOUT=900 ./test.py -v
    juju destroy-environment
"""

import fixtures
import json
import os.path
import subprocess
import testtools
from testtools.content import text_content
import time
import unittest


SERIES = 'precise'
TEST_CHARM = 'local:postgresql'
PSQL_CHARM = 'local:postgresql-psql'


def DEBUG(msg):
    """Allow us to watch these slow tests as they run."""
    debug_file = os.environ.get('TEST_DEBUG_FILE', '')
    if debug_file:
        with open(debug_file, 'a') as f:
            f.write('{}> {}\n'.format(time.ctime(), msg))
            f.flush()


def _run(detail_collector, cmd, input=''):
    DEBUG("Running {}".format(' '.join(cmd)))
    try:
        proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
    except subprocess.CalledProcessError, x:
        DEBUG("exception: {!r}".format(x))
        DEBUG("stderr: {}".format(proc.stderr.read()))
        raise

    (out, err) = proc.communicate(input)
    if out:
        DEBUG("stdout: {}".format(out))
        detail_collector.addDetail('stdout', text_content(out))
    if err:
        DEBUG("stderr: {}".format(err))
        detail_collector.addDetail('stderr', text_content(err))
    if proc.returncode != 0:
        DEBUG("rv: {}".format(proc.returncode))
        raise subprocess.CalledProcessError(
            proc.returncode, cmd, err)
    return out


class JujuFixture(fixtures.Fixture):
    """Interact with juju. Assumes juju environment is bootstrapped."""
    _deployed_charms = set()

    def do(self, cmd):
        cmd = ['juju'] + cmd
        _run(self, cmd)

    def get_result(self, cmd):
        cmd = ['juju'] + cmd + ['--format=json']
        out = _run(self, cmd)
        if out:
            return json.loads(out)
        return None

    def deploy(self, charm, name=None, num_units=1):
        # The first time we deploy a local: charm in the test run, it
        # needs to deploy with --update to ensure we are testing the
        # desired revision of the charm. Subsequent deploys we do not
        # use --update to avoid overhead and needless incrementing of the
        # revision number.
        if not charm.startswith('local:') or charm in self._deployed_charms:
            cmd = ['deploy']
        else:
            cmd = ['deploy', '-u']
            self._deployed_charms.add(charm)

        if num_units > 1:
            cmd.extend(['-n', str(num_units)])

        cmd.append(charm)

        if name:
            cmd.append(name)

        self.do(cmd)

    # The most recent environment status, updated by refresh_status()
    status = None

    def refresh_status(self):
        self.status = self.get_result(['status'])
        return self.status

    def wait_until_ready(self, extra=45):
        ready = False
        while not ready:
            self.refresh_status()
            ready = True
            for service in self.status['services']:
                if self.status['services'][service].get('life', '') == 'dying':
                    ready = False
                units = self.status['services'][service].get('units', {})
                for unit in units.keys():
                    agent_state = units[unit].get('agent-state', '')
                    if agent_state == 'error':
                        raise RuntimeError('{} error: {}'.format(
                            unit, units[unit].get('agent-state-info','')))
                    if agent_state != 'started':
                        ready = False
        # Unfortunately, there is no way to tell when a system is
        # actually ready for us to test. Juju only tells us that a
        # relation has started being setup, and that no errors have been
        # encountered yet. It utterly fails to inform us when the
        # cascade of hooks this triggers has finished and the
        # environment is in a stable and actually testable state.
        # So as a work around for Bug #1200267, we need to sleep long
        # enough that our system is probably stable. This means we have
        # extremely slow and flaky tests, but that is possibly better
        # than no tests.
        time.sleep(extra)

    def setUp(self):
        DEBUG("JujuFixture.setUp()")
        super(JujuFixture, self).setUp()
        self.reset()
        # Optionally, don't teardown services and machines after running
        # a test. If a subsequent test is run, they will be torn down at
        # that point. This option is only useful when running a single
        # test, or when the test harness is set to abort after the first
        # failed test.
        if not os.environ.get('TEST_DONT_TEARDOWN_JUJU', False):
            self.addCleanup(self.reset)

    def reset(self):
        DEBUG("JujuFixture.reset()")
        # Tear down any services left running.
        found_services = False
        self.refresh_status()
        for service in self.status['services']:
            found_services = True
            # It is an error to destroy a dying service.
            if self.status['services'][service].get('life', '') != 'dying':
                self.do(['destroy-service', service])

        # Per Bug #1190250 (WONTFIX), we need to wait for dying services
        # to die before we can continue.
        if found_services:
            self.wait_until_ready(0)

        # We shouldn't reuse machines, as we have no guarantee they are
        # still in a usable state, so tear them down too. Per
        # Bug #1190492 (INVALID), in the future this will be much nicer
        # when we can use containers for isolation and can happily reuse
        # machines.
        dirty_machines = [
            m for m in self.status['machines'].keys() if m != '0']
        if dirty_machines:
            self.do(['terminate-machine'] + dirty_machines)


class LocalCharmRepositoryFixture(fixtures.Fixture):
    """Create links so the given directory can be deployed as a charm."""
    def __init__(self, path=None):
        if path is None:
            path = os.getcwd()
        self.local_repo_path = os.path.abspath(path)

    def setUp(self):
        super(LocalCharmRepositoryFixture, self).setUp()

        series_dir = os.path.join(self.local_repo_path, SERIES)
        charm_dir = os.path.join(series_dir, TEST_CHARM)

        if not os.path.exists(series_dir):
            os.mkdir(series_dir, 0o700)
            self.addCleanup(os.rmdir, series_dir)

        if not os.path.exists(charm_dir):
            os.symlink(self.local_repo_path, charm_dir)
            self.addCleanup(os.remove, charm_dir)

        self.useFixture(fixtures.EnvironmentVariable(
            'JUJU_REPOSITORY', self.local_repo_path))


class PostgreSQLCharmTestCase(testtools.TestCase, fixtures.TestWithFixtures):

    def setUp(self):
        super(PostgreSQLCharmTestCase, self).setUp()

        self.juju = self.useFixture(JujuFixture())

        ## Disabled until postgresql-psql is in the charm store.
        ## Otherwise, we need to make the local:postgresql-psql charm
        ## discoverable.
        ## self.useFixture(LocalCharmRepositoryFixture())

        # If the charms fail, we don't want tests to hang indefinitely.
        # We might need to increase this in some environments or if the
        # environment doesn't have enough machines warmed up.
        timeout = int(os.environ.get('TEST_TIMEOUT', 900))
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
        DEBUG("SQL {}".format(sql))
        out = _run(self, cmd, input=sql)
        DEBUG("OUT {}".format(out))
        result = [line.split(',') for line in out.splitlines()]
        self.addDetail('sql', text_content(repr((sql, result))))
        return result

    def pg_ctlcluster(self, unit, command):
        cmd = [
            'juju', 'ssh', unit,
            # Due to Bug #1191079, we need to send the whole remote command
            # as a single argument.
            'sudo pg_ctlcluster 9.1 main -force {}'.format(command)]
        _run(self, cmd)

    def test_basic(self):
        '''Connect to a a single unit service via the db relationship.'''
        self.juju.deploy(TEST_CHARM, 'postgresql')
        self.juju.deploy(PSQL_CHARM, 'psql')
        self.juju.do(['add-relation', 'postgresql:db', 'psql:db'])
        self.juju.wait_until_ready()

        # There a race condition here, as hooks may still be running
        # from adding the relation. I'm protected here as 'juju status'
        # takes about 25 seconds to run from here to my test cloud but
        # others might not be so 'lucky'.
        result = self.sql('SELECT TRUE')
        self.assertEqual(result, [['t']])

    def test_basic_admin(self):
        '''Connect to a single unit service via the db-admin relationship.'''
        self.juju.deploy(TEST_CHARM, 'postgresql')
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
        self.juju.deploy(TEST_CHARM, 'postgresql', num_units=3)
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
        self.juju.deploy(TEST_CHARM, 'postgresql', num_units=3)
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


def unit_sorted(units):
    """Return a correctly sorted list of unit names."""
    return sorted(
        units, lambda a, b: cmp(int(a.split('/')[-1]), int(b.split('/')[-1])))


if __name__ == '__main__':
    raise SystemExit(unittest.main())
