#!/usr/bin/python

import fixtures
import json
import os.path
import subprocess
import testtools
import unittest


SERIES = 'precise'
CHARM = 'postgresql'


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

    def status(self):
        return self.get_result(['status'])

    def setUp(self):
        super(JujuFixture, self).setUp()
        self.addCleanup(self.reset)

    def reset(self):
        # Tear down any services left running.
        status = self.status()
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
        charm_dir = os.path.join(series_dir, CHARM)

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
        # It would be nice to bootstrap an environment, but SSH key
        # handling makes that problematic.
        cls.juju = JujuFixture()
        cls.juju.setUp()

    @classmethod
    def tearDownClass(cls):
        cls.juju.cleanUp()

    def setUp(self):
        super(PostgreSQLCharmTestCase, self).setUp()
        self.useFixture(LocalCharmRepositoryFixture())
        self.juju.reset()

    def test_basic(self):
        '''Set up a single unit service'''
        self.juju.do(['deploy', 'local:{}'.format(CHARM)])


if __name__ == '__main__':
    raise SystemExit(unittest.main())
