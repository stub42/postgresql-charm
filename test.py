#!/usr/bin/python

import fixtures
import json
import os.path
import subprocess
import testtools
import unittest


SERIES = 'precise'
TEST_CHARM = 'local:postgresql'
PSQL_CHARM = 'local:postgresql-psql'


class JujuFixture(fixtures.Fixture):
    """Interact with juju. Assumes juju environment is bootstrapped."""
    def do(self, cmd):
        cmd = ['juju', '--log=/dev/null'] + cmd
        subprocess.check_call(cmd)

    def get_result(self, cmd):
        cmd = ['juju', '--log=/dev/null'] + cmd + ['--format=json']
        json_result = subprocess.check_output(cmd)
        if json_result:
            return json.loads(json_result)
        return None

    # The most recent environment status, updated by refresh_status()
    status = None

    def refresh_status(self):
        self.status = self.get_result(['status'])

    def wait_until_ready(self):
        ready = False
        while not ready:
            self.refresh_status()
            ready = True
            for unit in self.status['services']['units']:
                agent_state = (
                    self.status['services']['units'][unit]['agent-state'])
                if agent_state != 'started':
                    ready = False
                    break

    def setUp(self):
        super(JujuFixture, self).setUp()
        self.addCleanup(self.reset)

    def reset(self):
        # Tear down any services left running.
        for service in status['services']:
            self.do(['destroy-service', service])
        # We unfortunately cannot reuse machines, as we have no
        # guarantee they are still in a usable state. Tear them down
        # too.
        dirty_machines = [m for m in status['machines'].keys() if m != '0']
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

    @classmethod
    def setUpClass(cls):
        # Should we bootstrap an environment here if it isn't already
        # setup? How do we do this? We would also need to tear it down
        # if we bootstrapped it.
        cls.juju = JujuFixture()
        cls.juju.setUp()

    @classmethod
    def tearDownClass(cls):
        cls.juju.cleanUp()

    def setUp(self):
        super(PostgreSQLCharmTestCase, self).setUp()
        self.useFixture(LocalCharmRepositoryFixture())
        self.juju.reset()

    def sql(self):
        units = self.juju.status["services"]["psql"]

    def test_basic(self):
        '''Set up a single unit service'''
        self.juju.do(['deploy', TEST_CHARM, 'postgresql'])
        self.juju.do(['deploy', PSQL_CHARM, 'psql'])
        self.juju.do(['add-relation', 'postgresql:db', 'psql:db'])
        self.juju.wait_until_ready()


if __name__ == '__main__':
    raise SystemExit(unittest.main())
