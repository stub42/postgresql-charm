# Copyright 2015-2017 Canonical Ltd.
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

from contextlib import suppress
from datetime import datetime
from distutils.version import LooseVersion
import os.path
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

import amulet
import psycopg2
import yaml

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(1, ROOT)
sys.path.insert(2, os.path.join(ROOT, "lib"))
sys.path.insert(3, os.path.join(ROOT, "lib", "pypi"))

from testing.amuletfixture import AmuletFixture


SERIES = os.environ.get("SERIES", "xenial").strip()
CLIENT_CHARMDIR = os.path.abspath(os.path.join(ROOT, "lib", "pgclient"))
assert os.path.isdir(CLIENT_CHARMDIR)


def has_swift():
    os_keys = set(["OS_TENANT_NAME", "OS_AUTH_URL", "OS_USERNAME", "OS_PASSWORD"])
    for os_key in os_keys:
        if os_key not in os.environ:
            return False
    return True


def has_s3():
    os_keys = set(["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"])
    for os_key in os_keys:
        if os_key not in os.environ:
            return False
    return True


def has_wabs():
    os_keys = set(["WABS_ACCOUNT_NAME", "WABS_ACCESS_KEY"])
    for os_key in os_keys:
        if os_key not in os.environ:
            return False
    return True


class PGBaseTestCase(object):
    deployment = None  # Module scoped AmuletFixture()

    common_config = dict()

    # Override these in subclasses to run these tests multiple times
    # for different PostgreSQL versions.
    version = None
    test_config = None
    num_units = 1
    nagios_subordinate = False
    storage_subordinate = False

    keep = set()

    @classmethod
    def setUpClass(cls, postgresql_charm_dir=None):
        super(PGBaseTestCase, cls).setUpClass()

        # Set up the AmuletFixture. It would be nice to share some
        # of this setup with a module level fixture, but unfortunately
        # Amulet does not let us add services after the initial deploy.
        deployment = AmuletFixture(series=SERIES)
        deployment.charm_dir = postgresql_charm_dir
        deployment.setUp(keep=cls.keep)
        cls.deployment = deployment

        # Add the helper charm. We need this to act as a proxy,
        # so connections come from a unit the PostgreSQL charm recognizes.
        deployment.add("client", CLIENT_CHARMDIR)
        cls.keep.add("client")

        # Add and configure the PostgreSQL units.
        deployment.add(
            "postgresql", postgresql_charm_dir, units=cls.num_units, constraints=dict(mem="512M"),
        )
        deployment.expose("postgresql")
        config = dict(cls.common_config)
        config.update(cls.test_config)
        deployment.configure("postgresql", config)

        # Relate it to the client service.
        deployment.relate("postgresql:db", "client:db")
        deployment.relate("postgresql:db-admin", "client:db-admin")

        # Add the nagios subordinate to exercise the nrpe hooks.
        if cls.nagios_subordinate:
            deployment.add("nrpe", "cs:nrpe")
            deployment.relate("postgresql:nrpe-external-master", "nrpe:nrpe-external-master")

        # Add a storage subordinate. Defaults just use local disk.
        # We need to use an unofficial branch, as there is not yet
        # an official branch of the storage charm for trusty.
        if cls.storage_subordinate:
            deployment.add("storage", "lp:~stub/charms/trusty/storage/trunk")
            deployment.relate("postgresql:data", "storage:data")

        # Per Bug #1489237, wait until juju-deployer can no longer see
        # the ghost of serivices we want to redeploy.
        for service in ["postgresql", "nagios", "storage"]:
            while True:
                cmd = ["juju-deployer", "-f", service]
                rv = subprocess.call(cmd, stderr=subprocess.STDOUT, stdout=subprocess.DEVNULL, universal_newlines=True,)
                if rv == 1:
                    break  # Its gone according to juju-deployer.
                time.sleep(1)
        # But also per Bug #1489237, that waiting isn't enough so I'm
        # just going to have to sleep for a bit for things to clear before
        # attempting the deploy.
        time.sleep(10)

        try:
            cls.deployment.deploy(keep=cls.keep)
            if not cls.storage_subordinate:
                cls.add_juju_storage()
        except Exception:
            with suppress(Exception):
                cls.deployment.tearDown()
            raise

    @classmethod
    def add_juju_storage(cls):
        if cls.deployment.has_juju_version("2.0"):
            cmd = ["add-storage"]
        else:
            cmd = ["storage", "add"]

        for sentry in cls.deployment.sentry["postgresql"]:
            unit = sentry.info["unit_name"]
            amulet.helpers.juju(cmd + [unit, "pgdata=5M"])
        cls.deployment.wait()

    @classmethod
    def tearDownClass(cls):
        if cls.deployment is not None:
            cls.deployment.tearDown(keep=cls.keep)
        super(PGBaseTestCase, cls).setUpClass()

    def _get_config(self):
        if self.deployment.has_juju_version("2.0"):
            cmd = ["juju", "config", "postgresql"]
        else:
            cmd = ["juju", "get", "postgresql"]
        raw = subprocess.check_output(cmd, universal_newlines=True)
        settings = yaml.safe_load(raw)["settings"]
        return {k: settings[k]["value"] for k in settings.keys()}

    def setUp(self):
        starting_config = self._get_config()

        def _maybe_reset_config():
            # Reset any changed configuration.
            current_config = self._get_config()
            if current_config != starting_config:
                conf = {k: v for k, v in starting_config.items() if starting_config[k] != current_config[k]}
                self.deployment.configure("postgresql", conf)
                self.deployment.wait()

        self.addCleanup(_maybe_reset_config)

    @property
    def master(self):
        status = self.deployment.get_status()
        messages = []
        for unit, info in status["services"]["postgresql"]["units"].items():
            status_message = info["workload-status"].get("message")
            if status_message.startswith("Live master"):
                return unit
            messages.append(status_message)
        self.fail("There is no master. Got {!r}".format(messages))

    @property
    def secondaries(self):
        status = self.deployment.get_status()
        units = status["services"]["postgresql"]["units"]
        return set(
            unit for unit, info in units.items() if info["workload-status"]["message"].startswith("Live secondary")
        )

    @property
    def secondary(self):
        secondaries = self.secondaries
        if secondaries:
            return list(secondaries)[0]
        return None

    @property
    def units(self):
        status = self.deployment.get_status()
        return set(status["services"]["postgresql"]["units"].keys())

    @property
    def leader(self):
        status = self.deployment.get_status()
        for unit, d in status["services"]["postgresql"]["units"].items():
            if d.get("leader"):
                return unit
        return None

    def connect(self, unit=None, admin=False, database=None, user=None, password=None):
        """
        A psycopg2 connection to a PostgreSQL unit via our client.

        A db-admin relation is used if database is specified. Otherwise,
        a standard db relation is used.
        """
        # 'db' or 'db-admin' relation?
        rel_name = "db-admin" if admin else "db"
        to_rel = "client:{}".format(rel_name)

        # Which PostgreSQL unit we want to talk to.
        if unit is None:  # Any unit
            postgres_sentry = self.deployment.sentry["postgresql"][0]
            relinfo = postgres_sentry.relation(rel_name, to_rel)
        else:
            postgres_sentry = self.deployment.sentry[unit]
            relinfo = postgres_sentry.relation(rel_name, to_rel)

        self.assertIn("database", relinfo, "Client relation not setup")

        if database is None:
            database = relinfo["database"]

        # Choose a local port for our tunnel.
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(("", 0))
        local_port = s.getsockname()[1]
        s.close()

        # Open the tunnel and wait for it to come up.
        # The new process group is to ensure we can reap all the ssh
        # tunnels, as simply killing the 'juju ssh' process doesn't seem
        # to be enough.
        client_unit = self.deployment.sentry["client"][0].info["unit_name"]
        tunnel_cmd = [
            "juju",
            "ssh",
            client_unit,
            "-q",
            "-N",
            "-L",
            "{}:{}:{}".format(local_port, relinfo["host"], relinfo["port"]),
        ]
        tunnel_proc = subprocess.Popen(
            tunnel_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setpgrp,
        )
        tunnel_proc.stdin.close()

        self.addCleanup(os.killpg, tunnel_proc.pid, signal.SIGTERM)
        self.addCleanup(tunnel_proc.kill)  # Holds a reference too.

        timeout = time.time() + 60
        while True:
            time.sleep(1)
            assert tunnel_proc.poll() is None, "Tunnel died {!r}".format(tunnel_proc.stdout)
            try:
                socket.create_connection(("localhost", local_port)).close()
                break
            except socket.error:
                if time.time() > timeout:
                    # Its not going to work. Per Bug #802117, this
                    # is likely an invalid host key forcing
                    # tunnelling to be disabled.
                    raise

        return psycopg2.connect(
            port=local_port,
            host="localhost",
            database=database,
            user=user or relinfo["user"],
            password=password or relinfo["password"],
        )

    def has_version(self, ver):
        return LooseVersion(self.ver) >= LooseVersion(ver)

    def test_db_relation(self):
        for unit in self.units:
            with self.subTest(unit=unit):
                con = self.connect(unit)
                cur = con.cursor()
                cur.execute("SELECT TRUE")
                cur.fetchone()

    def test_db_admin_relation(self):
        # Create a user with a known password for subsequent tests.
        # We can't use the 'postgres' user as we don't know the password.
        con = self.connect(self.master, admin=True, database="postgres")
        cur = con.cursor()
        newuser = str(uuid.uuid1())
        newpass = str(uuid.uuid1())
        cur.execute("""CREATE USER "{}" SUPERUSER PASSWORD '{}'""".format(newuser, newpass))
        con.commit()

        for unit in self.units:
            with self.subTest(unit=unit):
                con = self.connect(unit, admin=True)
                cur = con.cursor()
                cur.execute("SELECT * FROM pg_stat_activity")

                # db-admin relations can connect to any database.
                con = self.connect(unit, admin=True, database="postgres")
                cur = con.cursor()
                cur.execute("select * from pg_stat_activity")

                # db-admin relations can connect as any user to any database.
                con = self.connect(unit, admin=True, database="postgres", user=newuser, password=newpass,)
                cur = con.cursor()
                cur.execute("select * from pg_stat_activity")
                cur.fetchone()

    def test_admin_addresses(self):
        # admin_addresses grants password authenticated access, so we need
        # to set a password on the postgres user.
        pw = str(uuid.uuid1())
        con = self.connect(self.master, admin=True)
        cur = con.cursor()
        cur.execute("ALTER USER postgres ENCRYPTED PASSWORD %s", (pw,))
        con.commit()

        status = self.deployment.get_status()
        unit_infos = status["services"]["postgresql"]["units"]

        # Calculate our libpq direct connection strings.
        conn_strs = {}
        for unit, unit_info in unit_infos.items():
            with self.subTest(unit=unit):
                unit_ip = unit_info["public-address"]
                port = int(unit_info["open-ports"][0].split("/")[0])
                conn_str = " ".join(
                    [
                        "dbname=postgres",
                        "user=postgres",
                        "password='{}'".format(pw),
                        "host={}".format(unit_ip),
                        "port={}".format(port),
                    ]
                )
                conn_strs[unit] = conn_str

        # Confirm that we cannot connect at the moment. This also
        # helpfully gives the IP address PostgreSQL sees the connection
        # coming from in the error message, which we will use since I
        # can't find any other reliable cross cloud method of obtaining it.
        reject_pattern = r'pg_hba.conf rejects connection for host "([\d.]+)"'
        reject_re = re.compile(reject_pattern)
        my_ips = set()
        for unit, conn_str in conn_strs.items():
            with self.subTest(unit=unit):
                with self.assertRaisesRegex(psycopg2.OperationalError, reject_re) as x:
                    psycopg2.connect(conn_str)
                m = reject_re.search(str(x.exception))
                my_ips.add(m.group(1))

        # Connections should work after setting the admin-addresses.
        if self.deployment.has_juju_version("2.0"):
            subcmd = "config"
        else:
            subcmd = "set"
        subprocess.check_call(
            ["juju", subcmd, "postgresql", "admin_addresses={}".format(",".join(my_ips)),], universal_newlines=True,
        )
        self.deployment.wait()

        for unit, conn_str in conn_strs.items():
            with self.subTest(unit=unit):
                con = psycopg2.connect(conn_str)
                cur = con.cursor()
                cur.execute("SELECT 1")
                self.assertEquals(1, cur.fetchone()[0])

    def test_explicit_database(self):
        client_unit = self.deployment.sentry["client"][0].info["unit_name"]
        relid = subprocess.check_output(
            ["juju", "run", "--unit", client_unit, "relation-ids db"],
            stderr=subprocess.DEVNULL,
            universal_newlines=True,
        ).strip()
        subprocess.check_call(
            ["juju", "run", "--unit", client_unit, "relation-set -r {} database=explicit" "".format(relid),],
            stderr=subprocess.DEVNULL,
            universal_newlines=True,
        )
        self.deployment.wait()

        for unit in self.units:
            with self.subTest(unit=unit):
                con = self.connect(unit, database="explicit")
                cur = con.cursor()
                cur.execute("SELECT 1")
                self.assertEqual(cur.fetchone()[0], 1)

    def test_mount(self):
        ver = self.version
        client_unit = self.deployment.sentry["postgresql"][0].info["unit_name"]
        details = subprocess.check_output(
            [
                "juju",
                "run",
                "--unit",
                client_unit,
                'stat --format "%A %U %G %N" ' "/var/lib/postgresql/{}/main" "".format(ver),
            ],
            stderr=subprocess.DEVNULL,
            universal_newlines=True,
        ).strip()
        if self.storage_subordinate:
            mount = "/srv/data/postgresql"
        else:
            mount = "/srv/pgdata"
        self.assertEqual(
            details, "lrwxrwxrwx root root " "'/var/lib/postgresql/{}/main' -> " "'{}/{}/main'".format(ver, mount, ver),
        )

        details = subprocess.check_output(
            ["juju", "run", "--unit", client_unit, 'stat --format "%A %U %G %N" ' "{}/{}/main".format(mount, ver),],
            stderr=subprocess.DEVNULL,
            universal_newlines=True,
        ).strip()
        self.assertEqual(details, "drwx------ postgres postgres " "'{}/{}/main'".format(mount, ver))


class PGMultiBaseTestCase(PGBaseTestCase):
    num_units = 2

    def _replication_test(self):
        con = self.connect(self.master)
        con.autocommit = True
        cur = con.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS tokens (x text)")
        token = str(uuid.uuid1())
        cur.execute("INSERT INTO tokens(x) VALUES (%s)", (token,))

        for secondary in self.secondaries:
            with self.subTest(secondary=secondary):
                con = self.connect(secondary)
                con.autocommit = True
                cur = con.cursor()
                timeout = time.time() + 10
                while True:
                    try:
                        cur.execute("SELECT TRUE FROM tokens WHERE x=%s", (token,))
                        break
                    except psycopg2.Error:
                        if time.time() > timeout:
                            raise
                self.assertTrue(cur.fetchone()[0])

    def test_replication(self):
        self._replication_test()

    def test_failover(self):
        # Destroy the master in a stable environment.
        self.deployment.add_unit("postgresql")
        self.deployment.wait()
        self.deployment.destroy_unit(self.master)

        # It can take some time after destroying the leader for a new
        # leader to be appointed. We need to wait enough time for the
        # hooks to kick in.
        time.sleep(60)
        self.deployment.wait()
        timeout = time.time() + 300
        while timeout > time.time():
            try:
                self.master
                break
            except AssertionError:
                time.sleep(3)
        self.deployment.wait()
        self.master  # Asserts there is a master db

        self._replication_test()

    def test_switchover(self):
        # The switchover action is run on the leader
        leader = self.leader
        self.assertIsNotNone(leader)
        new_master = self.secondary
        self.assertIsNotNone(new_master)

        action_id = amulet.actions.run_action(leader, "switchover", dict(master=new_master))
        result = amulet.actions.get_action_output(action_id, raise_on_timeout=True)
        self.assertEqual(
            result["result"], "Initiated switchover of master to {}" "".format(new_master),
        )

        self.deployment.wait()
        self.assertEqual(self.master, new_master)
        self._replication_test()

    @unittest.skipUnless(has_swift(), "Swift storage is unavailable")
    def test_wal_e_swift_logshipping(self):
        now = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        container = "_juju_pg_tests"

        config = dict(streaming_replication=False, wal_e_storage_uri="swift://{}/{}".format(container, now),)

        # OpenStack credentials
        os_keys = set(["OS_TENANT_NAME", "OS_AUTH_URL", "OS_USERNAME", "OS_PASSWORD"])
        for os_key in os_keys:
            config[os_key.lower()] = os.environ[os_key]
        # Required PPA listed by default in config.yaml.
        # config['install_sources'] = '["ppa:stub/pgcharm"]'
        # config['install_keys'] = '[null]'

        # The swift command line tool uses the same environment variables
        # as this test suite.
        self.addCleanup(
            subprocess.call,
            ["swift", "delete", container],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
        )

        self.deployment.configure("postgresql", config)
        self.deployment.wait()

        # Confirm that the slave has not opened a streaming
        # replication connection.
        con = self.connect(self.master, admin=True)
        con.autocommit = True
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM pg_stat_replication")
        self.assertEqual(cur.fetchone()[0], 0, "Streaming connection found")

        # Confirm that replication is actually happening.
        # Create a table and force a WAL change.
        cur.execute("CREATE TABLE wale AS SELECT generate_series(0,100)")
        if self.has_version("10"):
            cur.execute("SELECT pg_switch_wal()")
        else:
            cur.execute("SELECT pg_switch_xlog()")
        self.addCleanup(cur.execute, "DROP TABLE wale")

        con = self.connect(self.secondary, admin=True)
        con.autocommit = True
        cur = con.cursor()
        timeout = time.time() + 120
        table_found = False
        while time.time() < timeout and not table_found:
            time.sleep(1)
            cur.execute("SELECT COUNT(*) FROM pg_class WHERE relname='wale'")
            table_found = cur.fetchone()[0] == 1
        self.assertTrue(table_found, "Replication not replicating")


class PG93Tests(PGBaseTestCase, unittest.TestCase):
    version = "9.3"
    test_config = dict(
        version=("" if SERIES == "trusty" else "9.3"),
        pgdg=(False if SERIES == "trusty" else True),
        max_connections=150,
    )
    storage_subordinate = True if SERIES == "trusty" else False
    nagios_subordinate = True if SERIES == "trusty" else False

    def test_deprecated_overrides(self):
        con = self.connect()
        cur = con.cursor()
        cur.execute("show max_connections")
        max_connections = cur.fetchone()[0]
        self.assertEqual(int(max_connections), 150)


class PG93MultiTests(PGMultiBaseTestCase, unittest.TestCase):
    # Alas, the subordinates do not yet support Xenial so we cannot
    # test with them.
    storage_subordinate = True if SERIES == "trusty" else False
    nagios_subordinate = True if SERIES == "trusty" else False

    version = "9.3"
    test_config = dict(version=("" if SERIES == "trusty" else "9.3"), pgdg=(False if SERIES == "trusty" else True),)


class PG95Tests(PGBaseTestCase, unittest.TestCase):
    # checkpoint_segments to test Bug #1588072
    version = "9.5"
    test_config = dict(
        version=("" if SERIES == "xenial" else "9.5"),
        pgdg=(False if SERIES == "xenial" else True),
        checkpoint_segments=10,
    )
    nagios_subordinate = True if SERIES == "xenial" else False


class PG95MultiTests(PGMultiBaseTestCase, unittest.TestCase):
    num_units = 3
    version = "9.6"
    test_config = dict(version=("" if SERIES == "xenial" else "9.5"), pgdg=(False if SERIES == "xenial" else True),)


class PG10Tests(PGBaseTestCase, unittest.TestCase):
    version = "10"
    test_config = dict(version=("" if SERIES == "bionic" else "10"), pgdg=(False if SERIES == "bionic" else True),)


class PG10MultiTests(PGMultiBaseTestCase, unittest.TestCase):
    num_units = 2
    version = "10"
    test_config = dict(version=("" if SERIES == "bionic" else "10"), pgdg=(False if SERIES == "bionic" else True),)


class UpgradedCharmTests(PGBaseTestCase, unittest.TestCase):
    num_units = 2  # Old charm only supported 2 unit initial deploy.
    version = "9.3"
    test_config = dict(version="9.3")
    # Storage subordinate does not yet work with Xenial.
    storage_subordinate = True if SERIES == "trusty" else False
    nagios_subordinate = False  # Nagios was broken with the old revision.

    @classmethod
    def setUpClass(cls):
        # Ensure an old version of the charm is first installed (but not
        # too old!). This version was what we internally recommended
        # before the rewrite to support Juju leadership and unit status,
        # and you can tell the correct version is deployed as the unit
        # status will remain 'unknown'.
        old_charm_dir = tempfile.mkdtemp(suffix=".charm")
        try:
            subprocess.check_call(
                ["bzr", "checkout", "-q", "--lightweight", "-r", "127", "lp:charms/trusty/postgresql", old_charm_dir,]
            )
            super(UpgradedCharmTests, cls).setUpClass(old_charm_dir)
        finally:
            shutil.rmtree(old_charm_dir)

        # Replace the pre-leadership charm in the repo with this version,
        # so we can upgrade.
        cls.deployment.charm_dir = None
        cls.deployment.repackage_charm()
        repo_path = os.path.join(os.environ["JUJU_REPOSITORY"], SERIES, "postgresql")
        if os.path.exists(repo_path):
            shutil.rmtree(repo_path)
        shutil.copytree(cls.deployment.charm_dir, repo_path)

        # Upgrade.
        if cls.deployment.has_juju_version("2.0"):
            cmd = [
                "juju",
                "upgrade-charm",
                "--switch",
                cls.deployment.charm_dir,
                "postgresql",
            ]
        else:
            cmd = ["juju", "upgrade-charm", "postgresql"]
        subprocess.check_call(
            cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, universal_newlines=True,
        )

        # Sleep. upgrade-charm first needs to distribute the updated
        # charm to the units before the hooks get invoked, and this takes
        # some time. During this period, the system looks completely idle
        # and 'juju wait' will consider the environment quiescent.
        time.sleep(10)

        # Now wait for the upgrade and fallout to finish, having hopefully
        # left enough time for the upgrade to actually start.
        cls.deployment.wait()

    def test_username(self):
        # We change the generated usernames to make disaster recovery
        # easier. Old usernames based on the relation id and perhaps
        # with a random component are GRANTed to the new usernames
        # so that database permissions are not lost.
        for admin, expected_username in [
            (False, "juju_client"),
            (True, "jujuadmin_client"),
        ]:
            with self.subTest(admin=admin):
                con = self.connect(admin=admin)
                cur = con.cursor()
                cur.execute("show session_authorization")
                username = cur.fetchone()[0]
                self.assertEqual(username, expected_username)
                cur.execute(
                    """
                            select count(*)
                            from
                                pg_user as role, pg_user as member,
                                pg_auth_members
                            where role.usesysid = pg_auth_members.roleid
                            and member.usesysid = pg_auth_members.member
                            and member.usename = %s
                            """,
                    (username,),
                )
                # The new username has been granted permissions of both
                # the old user and the old schema user (if there was an
                # old schema user)
                self.assertGreaterEqual(cur.fetchone()[0], 1)


# Now installed by the Makefile.
#
# def setUpModule():
#     # Mirror charmhelpers into our support charms, since charms
#     # can't symlink out of their subtree.
#     main_charmhelpers = os.path.abspath(os.path.join(ROOT, 'lib',
#                                                      'charmhelpers'))
#     test_client_charmhelpers = os.path.join(CLIENT_CHARMDIR,
#                                             'hooks', 'charmhelpers')
#     if os.path.exists(test_client_charmhelpers):
#         shutil.rmtree(test_client_charmhelpers)
#     shutil.copytree(main_charmhelpers, test_client_charmhelpers)
#
#
# def tearDownModule():
#     test_client_charmhelpers = os.path.join(CLIENT_CHARMDIR,
#                                             'hooks', 'charmhelpers')
#     if os.path.exists(test_client_charmhelpers):
#         shutil.rmtree(test_client_charmhelpers)
