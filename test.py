#!/usr/bin/python

"""
TEST_DEBUG_FILE=test-debug.log TEST_TIMEOUT=600 ./test.py -vv
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
        # The first time we deploy a charm in the test run, it needs to
        # deploy with --update to ensure we are testing the desired
        # revision of the charm. Subsequent deploys we do not use
        # --update to avoid overhead and needless incrementing of the
        # revision number.
        if charm not in self._deployed_charms:
            cmd = ['deploy', '-u']
            self._deployed_charms.add(charm)
        else:
            cmd = ['deploy']

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

    def wait_until_ready(self):
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

    def setUp(self):
        DEBUG("JujuFixture.setUp()")
        super(JujuFixture, self).setUp()
        self.reset()
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
            self.wait_until_ready()

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

    def sql(self, sql, psql_unit=None, postgres_unit=None, dbname=None):
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
        out = _run(self, cmd, input=sql)
        result = [line.split(',') for line in out.splitlines()]
        self.addDetail('sql', text_content(repr((sql, result))))
        return result

    def test_basic(self):
        '''Set up a single unit service'''
        self.juju.deploy(TEST_CHARM, 'postgresql')
        self.juju.deploy(PSQL_CHARM, 'psql')
        self.juju.do(['add-relation', 'postgresql:db', 'psql:db'])
        self.juju.wait_until_ready()

        # There a race condition here, as hooks may still be running
        # from adding the relation. I'm protected here as 'juju status'
        # takes about 25 seconds to run from here to my test cloud but
        # others might not be so 'lucky'.
        self.addDetail('status', text_content(repr(self.juju.status)))
        result = self.sql('SELECT TRUE')
        self.assertEqual(result, [['t']])

    def is_master(self, postgres_unit):
         is_master = self.sql(
             'SELECT NOT pg_is_in_recovery()',
             postgres_unit=postgres_unit)[0][0]
         return (is_master == 't')

    def test_failover(self):
        """Set up a multi-unit service and perform failovers."""
        self.juju.deploy(TEST_CHARM, 'postgresql', num_units=4)
        self.juju.deploy(PSQL_CHARM, 'psql')
        self.juju.do(['add-relation', 'postgresql:db', 'psql:db'])
        self.juju.wait_until_ready()

        units = unit_sorted(
            self.juju.status['services']['postgresql']['units'].keys())
        master_unit, standby_unit_1, standby_unit_2, standby_unit_3 = units

        # Confirm units agree on their roles. On a freshly setup
        # service, lowest numbered unit is always the master.
        self.assertIs(True, self.is_master(master_unit))
        self.assertIs(False, self.is_master(standby_unit_1))
        self.assertIs(False, self.is_master(standby_unit_2))
        self.assertIs(False, self.is_master(standby_unit_3))

        # Remove the master unit.
        self.juju.do(['remove-unit', master_unit])
        self.juju.wait_until_ready()

        # All hot standbys were fully in sync with the master, so the
        # lowest numbered standby will be the new master.
        self.assertIs(True, self.is_master(standby_unit_1))
        self.assertIs(False, self.is_master(standby_unit_2))
        self.assertIs(False, self.is_master(standby_unit_3))

        self.sql('CREATE TABLE Foo (integer bar)', postgres_unit='master')

        # Pause replication on unit 2, to allow it to get out of sync
        # with the master. Then make some DB changes.
        self.sql('SELECT pg_xlog_replay_pause()', standby_unit_2)
        self.sql('INSERT INTO Foo VALUES (1)', standby_unit_1)

        # Remove the master unit. In this case, unit 3 will end up as
        # the new master because it is more in sync.
        self.juju.do(['remove-unit', standby_unit_1])
        self.juju.wait_until_ready()

        # Confirm unit 2 is a standby, and unit 3 is the master.
        self.assertIs(False, self.is_master(standby_unit_2))
        self.assertIs(True, self.is_master(standby_unit_3))

        # Confirm sync status.
        result_2 = self.sql('SELECT COUNT(*) FROM Foo', standby_unit_2)
        self.assertEqual(result_2, [['0']])
        result_3 = self.sql('SELECT COUNT(*) FROM Foo', standby_unit_3)
        self.assertEqual(result_3, [['1']])

        # Reenable replication on unit 2, and confirm it works.
        self.sql('SELECT pg_xlog_replay_resume()', standby_unit_2)
        result_2 = self.sql('SELECT COUNT(*) FROM Foo', standby_unit_2)
        self.assertEqual(result_2, [['1']])

        # Remove the master (standby_unit_3). The last remaining standby
        # will endup standalone.
        self.juju.do(['remove-unit', standby_unit_3])
        self.juju.wait_until_ready()

        self.assertIs(True, self.is_master(standby_unit_2))

        # TODO: We need to extend the postgresql-psql charm to allow us
        # to inspect the status attribute on the relation. It should no
        # longer be 'master', but instead 'standalone'.


def unit_sorted(units):
    """Return a correctly sorted list of unit names."""
    return sorted(
        units, lambda a,b:
            cmp(int(a.split('/')[-1]), int(b.split('/')[-1])))


if __name__ == '__main__':
    raise SystemExit(unittest.main())
