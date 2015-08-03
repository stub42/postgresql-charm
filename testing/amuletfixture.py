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
    def __init__(self, series):
        # We use a wrapper around juju-deployer so we can fix how it is
        # invoked. In particular, turn off all the noise so we can
        # actually read our test output.
        juju_deployer = os.path.abspath(os.path.join(
            os.path.dirname(__file__), os.pardir, 'lib',
            'juju-deployer-wrapper.py'))
        super(AmuletFixture, self).__init__(series=series,
                                            juju_deployer=juju_deployer)

    def setUp(self):
        self._temp_dirs = []

        self.reset_environment(force=True)

        # Repackage our charm to a temporary directory, allowing us
        # to strip our virtualenv symlinks that would otherwise cause
        # juju to abort. We also strip the .bzr directory, working
        # around Bug #1394078.
        self.repackage_charm()

        # Fix amulet.Deployment so it doesn't depend on environment
        # variables or the current working directory, but rather the
        # environment we have introspected.
        with open(os.path.join(self.charm_dir, 'metadata.yaml'), 'r') as s:
            self.charm_name = yaml.safe_load(s)['name']
        self.charm_cache.test_charm = None
        self.charm_cache.fetch(self.charm_name, self.charm_dir, self.series)

        # Explicitly reset $JUJU_REPOSITORY to ensure amulet and
        # juju-deployer does not mess with the real one, per Bug #1393792
        self.org_repo = os.environ.get('JUJU_REPOSITORY', None)
        temp_repo = tempfile.mkdtemp(suffix='.repo')
        self._temp_dirs.append(temp_repo)
        os.environ['JUJU_REPOSITORY'] = temp_repo
        os.makedirs(os.path.join(temp_repo, self.series), mode=0o700)

    def tearDown(self, reset_environment=True):
        if reset_environment:
            self.reset_environment()
        if self.org_repo is None:
            del os.environ['JUJU_REPOSITORY']
        else:
            os.environ['JUJU_REPOSITORY'] = self.org_repo

    def deploy(self, timeout=None):
        '''Deploying or updating the configured system.

        Invokes amulet.Deployer.setup with a nicer name and standard
        timeout handling.
        '''
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
        cmd = ['juju', 'wait']
        if timeout:
            cmd = ['timeout', str(timeout)] + cmd
        try:
            subprocess.check_output(cmd, universal_newlines=True)
        except subprocess.CalledProcessError as x:
            print(x.output)
            raise

    def reset_environment(self, force=False):
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
        while True:
            status = self.get_status()
            service_items = status.get('services', {}).items()
            if not service_items:
                break
            for service_name, service in service_items:
                if service.get('life', '') not in ('dying', 'dead'):
                    subprocess.call(['juju', 'destroy-service', service_name],
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT)
                for unit_name, unit in service.get('units', {}).items():
                    if unit.get('agent-state', None) == 'error':
                        if force:
                            # If any units have failed hooks, unstick them.
                            # This should no longer happen now we are
                            # using the 'destroy-machine --force' command
                            # earlier.
                            try:
                                subprocess.check_output(
                                    ['juju', 'resolved', unit_name],
                                    stderr=subprocess.STDOUT)
                            except subprocess.CalledProcessError:
                                # A previous 'resolved' call make cause a
                                # subsequent one to fail if it is still
                                # being processed. However, we need to keep
                                # retrying because after a successful
                                # resolution a subsequent hook may cause an
                                # error state.
                                pass
                        else:
                            fails[unit_name] = unit
            time.sleep(1)

        harvest_machines = []
        for machine, state in status.get('machines', {}).items():
            if machine != "0" and state.get('life') not in ('dying', 'dead'):
                harvest_machines.append(machine)

        if harvest_machines:
            cmd = ['juju', 'remove-machine', '--force'] + harvest_machines
            subprocess.check_output(cmd, stderr=subprocess.STDOUT)

        if fails:
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
