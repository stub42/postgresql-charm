import json
import os.path
import subprocess
import time

import fixtures
from testtools.content import text_content
import yaml


__all__ = ['JujuFixture', 'run']


class JujuFixture(fixtures.Fixture):
    """Interact with juju.

    Assumes juju environment is bootstrapped.
    """

    def __init__(self, series, reuse_machines=False, do_teardown=True):
        super(JujuFixture, self).__init__()

        self.series = series

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

    def deploy(self, charm, name=None, num_units=1, config=None):
        cmd = ['deploy']

        if config:
            config_path = os.path.join(
                self.useFixture(fixtures.TempDir()).path, 'config.yaml')
            cmd.append('--config={}'.format(config_path))
            config = yaml.safe_dump({name: config}, default_flow_style=False)
            open(config_path, 'w').write(config)
            self.addDetail('pgconfig', text_content(config))

        cmd.append(charm)

        if name is None:
            name = charm.split(':', 1)[-1]

        cmd.append(name)
        self._deployed_services.add(name)

        if self.reuse_machines and self._free_machines:
            cmd.extend(['--to', str(self._free_machines.pop())])
            self.do(cmd)
            if num_units > 1:
                self.add_unit(name, num_units - 1)
        else:
            cmd.extend(['-n', str(num_units)])
            self.do(cmd)

    def add_unit(self, name, num_units=1):
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
            int(k) for k, m in self.status['machines'].items()
            if k != '0'
            and m.get('life', None) not in ('dead', 'dying')
            and m.get('series', None) == self.series
            and m.get('agent-state', 'pending') in ('started', 'ready'))
        for service in self.status.get('services', {}).values():
            for unit in service.get('units', {}).values():
                if 'machine' in unit:
                    self._free_machines.discard(int(unit['machine']))

        return self.status

    def relation_info(self, unit):
        '''Return all the relation information accessible from a unit.

        relation_info('foo/0')[relation_name][relation_id][unit][key]
        '''
        # Get the possible relation names heuristically, per Bug #1298819
        relation_names = []
        for service_name, service_info in self.status['services'].items():
            if service_name == unit.split('/')[0]:
                relation_names = service_info.get('relations', {}).keys()
                break

        res = {}
        juju_run_cmd = ['juju', 'run', '--unit', unit]
        for rel_name in relation_names:
            try:
                relation_ids = run(
                    self, juju_run_cmd + [
                        'relation-ids {}'.format(rel_name)]).split()
            except subprocess.CalledProcessError:
                # Per Bug #1298819, we can't ask the unit which relation
                # names are active so we need to use the relation names
                # reported by 'juju status'. This may cause us to
                # request relation information that the unit is not yet
                # aware of.
                continue
            res[rel_name] = {}
            for rel_id in relation_ids:
                res[rel_name][rel_id] = {}
                relation_units = [unit] + run(
                    self, juju_run_cmd + [
                        'relation-list -r {}'.format(rel_id)]).split()
                for rel_unit in relation_units:
                    try:
                        json_rel_info = run(
                            self, juju_run_cmd + [
                                'relation-get --format=json -r {} - {}'.format(
                                    rel_id, rel_unit)])
                        res[rel_name][rel_id][rel_unit] = json.loads(
                            json_rel_info)
                    except subprocess.CalledProcessError as x:
                        if x.returncode == 2:
                            res[rel_name][rel_id][rel_unit] = None
                        else:
                            raise
        return res

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
                    if service.get('life', '') not in ('dying', 'dead'):
                        self.do(['destroy-service', service_name])
                    # If any units have failed hooks, unstick them.
                    for unit_name, unit in service.get('units', {}).items():
                        if unit.get('agent-state', None) == 'error':
                            try:
                                self.do(['resolved', unit_name])
                            except subprocess.CalledProcessError:
                                # More race conditions in juju. A
                                # previous 'resolved' call make cause a
                                # subsequent one to fail if it is still
                                # being processed. However, we need to
                                # keep retrying because after a
                                # successful resolution a subsequent hook
                                # may cause an error state.
                                pass
            if not found_services:
                break
            time.sleep(1)

        self._deployed_services = set()

        # We shouldn't reuse machines, as we have no guarantee they are
        # still in a usable state, so tear them down too. Per
        # Bug #1190492 (INVALID), in the future this will be much nicer
        # when we can use containers for isolation and can happily reuse
        # machines.
        if self.reuse_machines:
            # If we are reusing machines, wait until pending machines
            # are ready and dying machines are dead.
            while True:
                for k, machine in self.status['machines'].items():
                    if (k != 0 and machine.get('agent-state', 'pending')
                            not in ('ready', 'started')):
                        time.sleep(1)
                        self.refresh_status()
                        continue
                break
        else:
            self.do(['terminate-machine'] + list(self._free_machines))


_run_seq = 0


def run(detail_collector, cmd, input=''):
    global _run_seq
    _run_seq = _run_seq + 1

    # This helper primarily exists to capture the subprocess details,
    # but this is failing. Details are being captured, but those added
    # inside the actual test (not setup) are not being reported.

    out, err, returncode = None, None, None
    try:
        proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        (out, err) = proc.communicate(input)
        returncode = proc.returncode
        if returncode != 0:
            raise subprocess.CalledProcessError(returncode, cmd, err)
        return out
    except subprocess.CalledProcessError as x:
        returncode = x.returncode
        raise
    finally:
        if detail_collector is not None:
            m = {
                'cmd': ' '.join(cmd),
                'rc': returncode,
                'stdout': out,
                'stderr': err,
            }
            detail_collector.addDetail(
                'run_{}'.format(_run_seq),
                text_content(yaml.safe_dump(m, default_flow_style=False)))
