import json
import subprocess
import time

import fixtures
from testtools.content import text_content


__all__ = ['JujuFixture', 'run']


class JujuFixture(fixtures.Fixture):
    """Interact with juju.

    Assumes juju environment is bootstrapped.
    """

    def __init__(self, reuse_machines=False, do_teardown=True):
        super(JujuFixture, self).__init__()

        self._deployed_charms = set()

        self.reuse_machines = reuse_machines

        # Optionally, don't teardown services and machines after running
        # a test. If a subsequent test is run, they will be torn down at
        # that point. This option is only useful when running a single
        # test, or when the test harness is set to abort after the first
        # failed test.
        self.do_teardown = do_teardown

        self._deployed_services = set()

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

        cmd.append(charm)

        if name is None:
            name = charm.split(':', 1)[-1]

        cmd.append(name)
        self._deployed_services.add(name)

        if self.reuse_machines and self._free_machines:
            cmd.extend(['--to', str(self._free_machines.pop())])
            self.do(cmd)
            if num_units > 1:
                self.add_unit(charm, name, num_units - 1)
        else:
            cmd.extend(['-n', str(num_units)])
            self.do(cmd)

    def add_unit(self, charm, name=None, num_units=1):
        if name is None:
            name = charm.split(':', 1)[-1]

        num_units_spawned = 0
        while self.reuse_machines and self._free_machines:
            cmd = ['add-unit', '--to', str(self._free_machines.pop()), name]
            self.do(cmd)
            num_units_spawned += 1
            if num_units_spawned == num_units:
                return

        cmd = ['add-unit', '-n', str(num_units - num_units_spawned), name]
        self.do(cmd)

    # The most recent environment status, updated by refresh_status()
    status = None

    def refresh_status(self):
        self.status = self.get_result(['status'])

        self._free_machines = set(
            int(k) for k in self.status['machines'].keys()
            if k != '0')
        for service in self.status.get('services', {}).values():
            for unit in service.get('units', []):
                if 'machine' in unit:
                    self._free_machines.remove(int(unit['machine']))

        return self.status

    def wait_until_ready(self, extra=60):
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
                            unit, units[unit].get('agent-state-info', '')))
                    if agent_state != 'started':
                        ready = False
            time.sleep(1)
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
        # Tear down any services left running that we know we spawned.
        while True:
            found_services = False
            self.refresh_status()

            # Kill any services started by the deploy() method.
            for service_name, service in self.status.get(
                    'services', {}).items():
                if service_name in self._deployed_services:
                    found_services = True
                    if service.get('life', '') != 'dying':
                        self.do(['destroy-service', service_name])
                    # If any units have failed hooks, unstick them.
                    for unit_name, unit in service.get('units', {}).items():
                        if unit.get('agent-state', None) == 'error':
                            self.do(['resolved', unit_name])
            if not found_services:
                break
            time.sleep(1)

        self._deployed_services = set()

        # We need to wait for dying services
        # to die before we can continue.
        if found_services:
            self.wait_until_ready(0)

        # We shouldn't reuse machines, as we have no guarantee they are
        # still in a usable state, so tear them down too. Per
        # Bug #1190492 (INVALID), in the future this will be much nicer
        # when we can use containers for isolation and can happily reuse
        # machines.
        if not self.reuse_machines:
            self.do(['terminate-machine'] + list(self._free_machines))


def run(detail_collector, cmd, input=''):
    try:
        proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
    except subprocess.CalledProcessError:
        raise

    (out, err) = proc.communicate(input)
    if out:
        detail_collector.addDetail('stdout', text_content(out))
    if err:
        detail_collector.addDetail('stderr', text_content(err))
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(
            proc.returncode, cmd, err)
    return out
