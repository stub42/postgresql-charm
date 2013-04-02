#!/usr/bin/python

import fixtures
import json
import subprocess
import testtools
import unittest


class JujuFixture(fixtures.Fixture):
    def do(self, cmd):
        cmd = ['juju', '--log=/dev/null'] + cmd
        subprocess.check_call(cmd)

    def get_result(self, cmd):
        cmd = ['juju', '--log=/dev/null'] + cmd + ['--format=json']
        return subprocess.check_result(cmd)

    def status(self):
        cmd = ['juju', '--log=/dev/null', 'status', '--format=json']
        json_result = subprocess.check_output(cmd)
        if json:
            return json.loads(json_result)
        return None

    def cleanUp(self):
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


class PostgreSQLCharmTestCase(testtools.TestCase, fixtures.TestWithFixtures):

    @classmethod
    def setUpClass(cls):
        JujuFixture().cleanUp()

    def setUp(self):
        super(PostgreSQLCharmTestCase, self).setUp()
        self.juju = self.useFixture(JujuFixture())

    def test_foo(self):
        pass


if __name__ == '__main__':
    unittest.main()
