import json
import time

import fixtures

from run import run


__all__ = ['JujuFixture']


class JujuFixture(fixtures.Fixture):
    """Interact with juju.

    Assumes juju environment is bootstrapped."""

    def __init__(self, do_teardown=True):
        super(JujuFixture, self).__init__()

        self._deployed_charms = set()

        # Optionally, don't teardown services and machines after running
        # a test. If a subsequent test is run, they will be torn down at
        # that point. This option is only useful when running a single
        # test, or when the test harness is set to abort after the first
        # failed test.
        self.do_teardown = do_teardown

    def do(self, cmd):
        cmd = ['juju'] + cmd
        run(self, cmd)

    def get_result(self, cmd):
        cmd = ['juju'] + cmd + ['--format=json']
        out = run(self, cmd)
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
        super(JujuFixture, self).setUp()
        self.reset()
        if self.do_teardown:
            self.addCleanup(self.reset)

    def reset(self):
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



