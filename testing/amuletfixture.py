# Copyright 2015 Canonical Ltd.
#
# This file is part of the PostgreSQL Charm for Juju.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 3, as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranties of
# MERCHANTABILITY, SATISFACTORY QUALITY, or FITNESS FOR A PARTICULAR
# PURPOSE.  See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import json
import os
import shutil
import subprocess
import tempfile
import time

import amulet
import yaml


class AmuletFixture(amulet.Deployment):
    def __init__(self, series, charm_dir=None):
        self.charm_dir = charm_dir  # If None, reset by repackage_charm()
        # We use a wrapper around juju-deployer so we can adjust how it is
        # invoked. In particular, only make noise on failure.
        juju_deployer = os.path.abspath(os.path.join(
            os.path.dirname(__file__), os.pardir, 'lib',
            'juju-deployer-wrapper.py'))
        super(AmuletFixture, self).__init__(series=series,
                                            juju_deployer=juju_deployer)

    def setUp(self, keep=None):
        self._temp_dirs = []

        if keep:
            self.reset_environment(keep=keep)
        else:
            self.reset_environment(force=True)

        # Repackage our charm to a temporary directory, allowing us
        # to strip our virtualenv symlinks that would otherwise cause
        # juju to abort. We also strip the .bzr directory, working
        # around Bug #1394078.
        if self.charm_dir is None:
            self.repackage_charm()

        # Fix amulet.Deployment so it doesn't depend on environment
        # variables or the current working directory, but rather the
        # environment we have introspected.
        with open(os.path.join(self.charm_dir, 'metadata.yaml'), 'r') as s:
            self.charm_name = yaml.safe_load(s)['name']
        self.charm_cache.test_charm = None
        self.charm_cache.fetch(self.charm_name, self.charm_dir,
                               series=self.series)

        # Explicitly reset $JUJU_REPOSITORY to ensure amulet and
        # juju-deployer does not mess with the real one, per Bug #1393792
        self.org_repo = os.environ.get('JUJU_REPOSITORY', None)
        temp_repo = tempfile.mkdtemp(suffix='.repo')
        self._temp_dirs.append(temp_repo)
        os.environ['JUJU_REPOSITORY'] = temp_repo
        os.makedirs(os.path.join(temp_repo, self.series), mode=0o700)

    def tearDown(self, reset_environment=True, keep=None):
        if reset_environment:
            self.reset_environment(keep=keep)
        if self.org_repo is None:
            del os.environ['JUJU_REPOSITORY']
        else:
            os.environ['JUJU_REPOSITORY'] = self.org_repo

    def deploy(self, timeout=None, keep=None):
        '''Deploying or updating the configured system.

        Invokes amulet.Deployer.setup with a nicer name and standard
        timeout handling.
        '''
        # First, ensure any existing environment is completely
        # torn down. juju-deployer seems to forget to deploy
        # services if there is an existing service in the environment
        # in the process of being destroyed.
        self.reset_environment(keep=keep)
        if timeout is None:
            timeout = int(os.environ.get('AMULET_TIMEOUT', 900))

        # If setUp fails, tearDown is never called leaving the
        # environment setup. This is useful for debugging.
        self.setup(timeout=timeout)
        self.wait(timeout=timeout)

    def __del__(self):
        for temp_dir in self._temp_dirs:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)

    def get_status(self):
        try:
            raw = subprocess.check_output(['juju', 'status', '--format=json'],
                                          universal_newlines=True)
        except subprocess.CalledProcessError as x:
            print(x.output)
            raise
        if raw:
            return json.loads(raw)
        return None

    def wait(self, timeout=None):
        '''Wait until the environment has reached a stable state.'''
        cmd = ['juju', 'wait', '-q']
        if timeout is None:
            timeout = int(os.environ.get('AMULET_TIMEOUT', 900))
        cmd = ['timeout', str(timeout)] + cmd
        try:
            subprocess.check_output(cmd, universal_newlines=True)
        except subprocess.CalledProcessError as x:
            print(x.output)
            raise

    def reset_environment(self, force=False, keep=None):
        if keep is None:
            keep = frozenset()
        if force:
            status = self.get_status()
            machines = [m for m in status.get('machines', {}).keys()
                        if m != '0']
            if machines:
                subprocess.call(['juju', 'destroy-machine',
                                 '--force'] + machines,
                                stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL)

        fails = dict()
        keep_machines = set(["0"])
        while True:
            status = self.get_status()
            service_items = status.get('services', {}).items()

            for service_name, service in service_items:
                if service_name in keep:
                    # Don't mess with this service.
                    keep_machines.update([unit['machine'] for unit
                                          in service['units'].values()])
                    continue

                if service.get('life', '') not in ('dying', 'dead'):
                    subprocess.call(['juju', 'destroy-service', service_name],
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT)

                for unit_name, unit in service.get('units', {}).items():
                    if unit.get('agent-state', None) == 'error':
                        fails[unit_name] = unit

            services = set(k for k, v in service_items
                           if k not in keep)
            if not services:
                break

            time.sleep(1)

        harvest_machines = []
        for machine, state in status.get('machines', {}).items():
            if machine not in keep_machines and (state.get('life')
                                                 not in ('dying', 'dead')):
                harvest_machines.append(machine)

        if harvest_machines:
            cmd = ['juju', 'remove-machine', '--force'] + harvest_machines
            subprocess.check_output(cmd, stderr=subprocess.STDOUT)

        if fails and not force:
            raise Exception("Teardown failed", fails)

    def repackage_charm(self):
        """Mirror the charm into a staging area.

        We do this to work around issues with Amulet, juju-deployer
        and juju. In particular:
            - symlinks in the Python virtual env pointing outside of the
            charm directory.
            - odd bzr interactions, such as tests being run on the committed
            version of the charm, rather than the working tree.

        Returns the test charm directory.
        """
        # Find the charm_dir we are testing
        src_charm_dir = os.path.dirname(__file__)
        while True:
            if os.path.exists(os.path.join(src_charm_dir,
                                           'metadata.yaml')):
                break
            assert src_charm_dir != os.sep, 'metadata.yaml not found'
            src_charm_dir = os.path.abspath(os.path.join(src_charm_dir,
                                                         os.pardir))

        with open(os.path.join(src_charm_dir, 'metadata.yaml'), 'r') as s:
            self.charm_name = yaml.safe_load(s)['name']

        repack_root = tempfile.mkdtemp(suffix='.charm')
        self._temp_dirs.append(repack_root)

        self.charm_dir = os.path.join(repack_root, self.charm_name)

        # Ignore .bzr to work around weird bzr interactions with
        # juju-deployer, per Bug #1394078, and ignore .venv
        # due to a) it containing symlinks juju will reject and b) to avoid
        # infinite recursion.
        shutil.copytree(src_charm_dir, self.charm_dir, symlinks=True,
                        ignore=shutil.ignore_patterns('.venv?', '.bzr'))
