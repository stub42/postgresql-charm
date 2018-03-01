# Copyright 2015-2018 Canonical Ltd.
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
import os.path
import subprocess
import sys
import tempfile
from textwrap import dedent
import unittest
from unittest.mock import ANY, call, MagicMock, patch, sentinel

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(1, ROOT)
sys.path.insert(2, os.path.join(ROOT, 'lib'))
sys.path.insert(3, os.path.join(ROOT, 'lib', 'testdeps'))

from charmhelpers.core import hookenv
from charmhelpers.core import unitdata

from reactive import workloadstatus
from reactive.postgresql import helpers
from reactive.postgresql import postgresql


class TestPostgresql(unittest.TestCase):
    @patch.object(hookenv, 'config')
    @patch.object(helpers, 'distro_codename')
    def test_version(self, codename, config):

        def clear_cache():
            unitdata.kv().unset('postgresql.pg_version')

        # Explicit version in config.
        config.return_value = {'version': '23'}
        clear_cache()
        self.assertEqual(postgresql.version(), '23')

        config.return_value = {'version': ''}

        # Trusty default
        codename.return_value = 'trusty'
        clear_cache()
        self.assertEqual(postgresql.version(), '9.3')

        # Xenial default
        codename.return_value = 'xenial'
        clear_cache()
        self.assertEqual(postgresql.version(), '9.5')

        # Bionic default
        codename.return_value = 'bionic'
        clear_cache()
        self.assertEqual(postgresql.version(), '10')

        # No other fallbacks, yet.
        codename.return_value = 'whatever'
        clear_cache()
        with self.assertRaises(NotImplementedError):
            postgresql.version()

    @patch('subprocess.check_output')
    @patch.object(postgresql, 'postgres_path')
    def test_point_version(self, postgres_path, check_output):
        postgres_path.return_value = sentinel.postgres_path
        check_output.return_value = 'postgres (PostgreSQL) 9.8.765-2\n'
        self.assertEqual(postgresql.point_version(), '9.8.765')
        check_output.assert_called_once_with([sentinel.postgres_path, '-V'],
                                             universal_newlines=True)

    @patch.object(postgresql, 'version')
    def test_has_version(self, version):
        version.return_value = '9.4'
        self.assertTrue(postgresql.has_version('9.1'))
        self.assertTrue(postgresql.has_version('9.4'))
        self.assertFalse(postgresql.has_version('9.5'))

        # New version scheme starting PostgreSQL 10
        version.return_value = '10'
        self.assertTrue(postgresql.has_version('9.6'))
        self.assertFalse(postgresql.has_version('11'))

    @patch.object(hookenv, 'local_unit')
    @patch.object(helpers, 'get_peer_relation')
    @patch.object(postgresql, 'port')
    @patch('psycopg2.connect')
    def test_connect(self, psycopg2_connect, port, peer_rel, local_unit):
        psycopg2_connect.return_value = sentinel.connection
        port.return_value = sentinel.local_port

        self.assertEqual(postgresql.connect(), sentinel.connection)
        psycopg2_connect.assert_called_once_with(user='postgres',
                                                 database='postgres',
                                                 host=None,
                                                 port=sentinel.local_port)

        psycopg2_connect.reset_mock()
        local_unit.return_value = sentinel.local_unit
        peer_rel.return_value = {
            sentinel.local_unit: {'host': sentinel.local_host,
                                  'port': sentinel.local_port},
            sentinel.remote_unit: {'host': sentinel.remote_host,
                                   'port': sentinel.remote_port}}

        self.assertEqual(postgresql.connect(sentinel.user, sentinel.db,
                                            sentinel.local_unit),
                         sentinel.connection)
        psycopg2_connect.assert_called_once_with(user=sentinel.user,
                                                 database=sentinel.db,
                                                 host=None,
                                                 port=sentinel.local_port)

        psycopg2_connect.reset_mock()
        self.assertEqual(postgresql.connect(sentinel.user, sentinel.db,
                                            sentinel.remote_unit),
                         sentinel.connection)
        psycopg2_connect.assert_called_once_with(user=sentinel.user,
                                                 database=sentinel.db,
                                                 host=sentinel.remote_host,
                                                 port=sentinel.remote_port)

    def test_username(self):
        # Calculate the client username for the given service or unit
        # to use.
        self.assertEqual(postgresql.username('hello', False, False),
                         'juju_hello')
        self.assertEqual(postgresql.username('hello/0', False, False),
                         'juju_hello')
        self.assertEqual(postgresql.username('hello',
                                             superuser=True,
                                             replication=False),
                         'jujuadmin_hello')
        self.assertEqual(postgresql.username('hello/2', True, False),
                         'jujuadmin_hello')
        self.assertEqual(postgresql.username('hello', False, True),
                         'jujurepl_hello')

    def test_username_truncation(self):
        # Usernames need to be truncated to 63 characters, while remaining
        # unique.
        service = 'X' * 70
        too_long = 'juju_{}'.format(service)
        truncated = too_long[:31] + 'd83abbe4d9ddcab942fe8fe92d387470'
        self.assertEqual(len(truncated), 63)
        self.assertEqual(postgresql.username(service, False, False), truncated)

    @patch.object(postgresql, 'postgresql_conf_path')
    def test_port(self, pgconf_path):
        # Pull the configured port from postgresql.conf.
        with tempfile.NamedTemporaryFile('w') as pgconf:
            pgconf.write('# Some rubbish\n')
            pgconf.write(' Port = 1234 # Picked by pg_createcluster(1)\n')
            pgconf.flush()
            pgconf_path.return_value = pgconf.name
            self.assertEqual(postgresql.port(), 1234)

        with tempfile.NamedTemporaryFile('w') as pgconf:
            pgconf.write("port='1235'\n")
            pgconf.write('# Some rubbish\n')
            pgconf.flush()
            pgconf_path.return_value = pgconf.name
            self.assertEqual(postgresql.port(), 1235)

        with tempfile.NamedTemporaryFile('w') as pgconf:
            pgconf_path.return_value = pgconf.name
            self.assertEqual(postgresql.port(), 5432)  # Fallback to default.

    @patch.object(postgresql, 'version')
    def test_packages(self, version):
        version.return_value = '9.9'
        expected = set(['postgresql-9.9', 'postgresql-common',
                        'postgresql-client-common',
                        'postgresql-contrib-9.9', 'postgresql-client-9.9'])
        self.assertSetEqual(postgresql.packages(), expected)

    @patch('os.makedirs')
    @patch.object(postgresql, 'postgresql_conf_path')
    def test_inhibit_default_cluster_creation(self, pgconf_path, makedirs):
        # If the postgresql.conf file already exists for the default
        # cluster, package installation will not recreate the default
        # cluster.
        with tempfile.NamedTemporaryFile() as f:
            pgconf_path.return_value = f.name  # File already exists, noop.
            with postgresql.inhibit_default_cluster_creation():
                pass
            self.assertFalse(makedirs.called)

        with tempfile.NamedTemporaryFile(delete=False) as f:
            pgconf_path.return_value = f.name  # File already exists
            os.unlink(f.name)  # Remove it, to trigger creation

            with postgresql.inhibit_default_cluster_creation():
                self.assertTrue(os.path.isfile(pgconf_path()))  # It is back

            self.assertFalse(os.path.isfile(pgconf_path()))  # And gone again.

            # We ensured the path to the config file was created.
            makedirs.assert_called_once_with(os.path.dirname(f.name),
                                             mode=0o755, exist_ok=True)

    @patch.object(postgresql, 'version')
    def test_simple_paths(self, version):
        # We have a pile of trivial helpers to get directory and file
        # paths. We use these for consistency and ease of mocking.
        version.return_value = '9.9'
        self.assertEqual(postgresql.config_dir(),
                         '/etc/postgresql/9.9/main')
        self.assertEqual(postgresql.data_dir(),
                         '/var/lib/postgresql/9.9/main')
        self.assertEqual(postgresql.postgresql_conf_path(),
                         '/etc/postgresql/9.9/main/postgresql.conf')
        self.assertEqual(postgresql.pg_hba_conf_path(),
                         '/etc/postgresql/9.9/main/pg_hba.conf')
        self.assertEqual(postgresql.pg_ident_conf_path(),
                         '/etc/postgresql/9.9/main/pg_ident.conf')
        self.assertEqual(postgresql.recovery_conf_path(),
                         '/var/lib/postgresql/9.9/main/recovery.conf')
        self.assertEqual(postgresql.pg_ctl_path(),
                         '/usr/lib/postgresql/9.9/bin/pg_ctl')
        self.assertEqual(postgresql.postgres_path(),
                         '/usr/lib/postgresql/9.9/bin/postgres')

    @patch.object(postgresql, 'connect')
    def test_is_in_recovery(self, connect):
        connect().cursor().fetchone.return_value = [sentinel.flag]
        connect().cursor.reset_mock()
        connect.reset_mock()

        self.assertEqual(postgresql.is_in_recovery(), sentinel.flag)

        connect.assert_called_once_with()
        connect().cursor.assert_called_once_with()
        connect().cursor().execute.assert_called_once_with(
            'SELECT pg_is_in_recovery()')

    @patch.object(postgresql, 'is_secondary')
    def test_is_primary(self, is_secondary):
        is_secondary.return_value = True
        self.assertFalse(postgresql.is_primary())
        is_secondary.return_value = False
        self.assertTrue(postgresql.is_primary())

    @patch.object(postgresql, 'recovery_conf_path')
    def test_is_secondary(self, recovery_path):
        # if recovery.conf exists, we are a secondary.
        with tempfile.NamedTemporaryFile() as f:
            recovery_path.return_value = f.name
            self.assertTrue(postgresql.is_secondary())
        self.assertFalse(postgresql.is_secondary())

    @patch.object(hookenv, 'local_unit')
    @patch.object(postgresql, 'master')
    def is_master(self, master, local_unit):
        master.return_value = sentinel.master
        local_unit.return_value = sentinel.other
        self.assertFalse(postgresql.is_master())
        local_unit.return_value = sentinel.master
        self.assertTrue(postgresql.is_master())

    @patch.object(hookenv, 'leader_get')
    def master(self, leader_get):
        # The master is whoever the leader says it is.
        leader_get.return_value = sentinel.master
        self.assertEqual(postgresql.master(), sentinel.master)
        leader_get.assert_called_once_with('master')

    def test_quote_identifier(self):
        eggs = [('hello', '"hello"'),
                ('Hello', '"Hello"'),
                ("""'""", '''"'"'''),
                ('"', '''""""'''),
                ('\\', r'''"\"'''),
                (r'\"', r'''"\"""'''),
                # Unicode too, not that anything this odd should get through.
                ('\\ aargh \u0441\u043b\u043e\u043d',
                 r'U&"\\ aargh \0441\043b\043e\043d"')]
        for raw, quote in eggs:
            with self.subTest(raw=raw):
                self.assertEqual(postgresql.quote_identifier(raw), quote)

    def test_pgidentifier(self):
        a = postgresql.pgidentifier('magic')
        self.assertEqual(a, postgresql.AsIs('"magic"'))

    @patch('subprocess.check_call')
    @patch.object(hookenv, 'config')
    @patch.object(postgresql, 'version')
    def test_create_cluster(self, version, config, check_call):
        version.return_value = '9.9'
        config.return_value = {'locale': sentinel.locale,
                               'encoding': sentinel.encoding}
        postgresql.create_cluster()
        check_call.assert_called_once_with(['pg_createcluster',
                                            '-e', sentinel.encoding,
                                            '--locale', sentinel.locale,
                                            '9.9', 'main',
                                            '--', '--data-checksums'],
                                           universal_newlines=True)

        # No data checksums with earlier PostgreSQL versions.
        version.return_value = '9.2'
        config.return_value = {'locale': sentinel.locale,
                               'encoding': sentinel.encoding}
        check_call.reset_mock()
        postgresql.create_cluster()
        check_call.assert_called_once_with(['pg_createcluster',
                                            '-e', sentinel.encoding,
                                            '--locale', sentinel.locale,
                                            '9.2', 'main'],
                                           universal_newlines=True)

    @patch('subprocess.check_call')
    @patch.object(postgresql, 'version')
    def test_drop_cluster(self, version, check_call):
        version.return_value = '9.9'
        postgresql.drop_cluster()
        check_call.assert_called_once_with(['pg_dropcluster', '9.9', 'main'],
                                           universal_newlines=True)

    @patch.object(postgresql, 'connect')
    def test_ensure_database(self, connect):
        cur = connect().cursor()

        # If the database exists, nothing happens.
        cur.fetchone.return_value = sentinel.something
        postgresql.ensure_database('hello')
        cur.execute.assert_has_calls([
            call('SELECT datname FROM pg_database WHERE datname=%s',
                 ('hello',))])

        # If the database does not exist, it is created.
        cur.fetchone.return_value = None
        postgresql.ensure_database('hello')
        cur.execute.assert_has_calls([
            call('SELECT datname FROM pg_database WHERE datname=%s',
                 ('hello',)),
            call('CREATE DATABASE %s', (ANY,))])
        # The database name in that last call was correctly quoted.
        quoted_dbname = cur.execute.call_args[0][1][0]
        self.assertIsInstance(quoted_dbname, postgresql.AsIs)
        self.assertEqual(str(quoted_dbname), '"hello"')

    @patch.object(postgresql, 'pgidentifier')
    @patch.object(postgresql, 'role_exists')
    def test_ensure_user(self, role_exists, pgidentifier):
        con = MagicMock()
        cur = con.cursor()

        # Create a new boring user
        role_exists.return_value = False
        pgidentifier.return_value = sentinel.quoted_user
        postgresql.ensure_user(con, sentinel.user, sentinel.secret)
        pgidentifier.assert_called_once_with(sentinel.user)
        cur.execute.assert_called_once_with(
            'CREATE ROLE %s WITH LOGIN NOSUPERUSER NOREPLICATION PASSWORD %s',
            (sentinel.quoted_user, sentinel.secret))

        # Ensure an existing user is a superuser
        role_exists.return_value = True
        cur.execute.reset_mock()
        postgresql.ensure_user(con, sentinel.user, sentinel.secret,
                               superuser=True)
        cur.execute.assert_called_once_with(
            'ALTER ROLE %s WITH LOGIN SUPERUSER NOREPLICATION PASSWORD %s',
            (sentinel.quoted_user, sentinel.secret))

        # Create a new user with replication permissions.
        role_exists.return_value = False
        cur.execute.reset_mock()
        postgresql.ensure_user(con, sentinel.user, sentinel.secret,
                               replication=True)
        cur.execute.assert_called_once_with(
            'CREATE ROLE %s WITH LOGIN NOSUPERUSER REPLICATION PASSWORD %s',
            (sentinel.quoted_user, sentinel.secret))

    def test_role_exists(self):
        con = MagicMock()
        cur = con.cursor()

        # Exists
        cur.fetchone.return_value = sentinel.something
        self.assertTrue(postgresql.role_exists(con, sentinel.role))
        cur.execute.assert_called_once_with(
            "SELECT TRUE FROM pg_roles WHERE rolname=%s", (sentinel.role,))

        # Does not exist
        cur.fetchone.return_value = None
        cur.execute.reset_mock()
        self.assertFalse(postgresql.role_exists(con, sentinel.role))
        cur.execute.assert_called_once_with(
            "SELECT TRUE FROM pg_roles WHERE rolname=%s", (sentinel.role,))

    def test_grant_database_privileges(self):
        con = MagicMock()
        cur = con.cursor()
        privs = ['privA', 'privB']
        postgresql.grant_database_privileges(con, 'a_Role', 'a_DB', privs)

        cur.execute.assert_has_calls([
            call("GRANT %s ON DATABASE %s TO %s",
                 (postgresql.AsIs('privA'),  # Unquoted. Its a keyword.
                  postgresql.AsIs('"a_DB"'), postgresql.AsIs('"a_Role"'))),
            call("GRANT %s ON DATABASE %s TO %s",
                 (postgresql.AsIs('privB'),
                  postgresql.AsIs('"a_DB"'), postgresql.AsIs('"a_Role"')))])

    @patch.object(hookenv, 'log')
    @patch.object(postgresql, 'ensure_role')
    @patch.object(postgresql, 'pgidentifier')
    def test_grant_user_roles(self, pgidentifier, ensure_role, log):
        pgidentifier.side_effect = lambda d: 'q_{}'.format(d)

        existing_roles = set(['roleA', 'roleB'])
        wanted_roles = set(['roleB', 'roleC'])

        con = MagicMock()
        cur = con.cursor()
        cur.fetchall.return_value = [(r,) for r in existing_roles]

        postgresql.grant_user_roles(con, 'fred', wanted_roles)

        # A new role was ensured. The others we know exist.
        ensure_role.assert_called_once_with(con, 'roleC')

        role_query = dedent("""\
            SELECT role.rolname
            FROM
                pg_roles AS role,
                pg_roles AS member,
                pg_auth_members
            WHERE
                member.oid = pg_auth_members.member
                AND role.oid = pg_auth_members.roleid
                AND member.rolname = %s
            """)
        cur.execute.assert_has_calls([
            call(role_query, ('fred',)),
            call('GRANT %s TO %s', ('q_roleC', 'q_fred'))])

    @patch.object(postgresql, 'pgidentifier')
    def test_ensure_role(self, pgidentifier):
        con = MagicMock()
        cur = con.cursor()

        pgidentifier.side_effect = lambda d: 'q_{}'.format(d)

        # If the role already exists, nothing happens.
        cur.fetchone.return_value = sentinel.something
        postgresql.ensure_role(con, 'roleA')
        cur.execute.assert_called_once_with(
            "SELECT TRUE FROM pg_roles WHERE rolname=%s", ('roleA',))

        # If the role does not exist, it is created.
        cur.fetchone.return_value = None
        postgresql.ensure_role(con, 'roleA')
        cur.execute.assert_has_calls([call("CREATE ROLE %s INHERIT NOLOGIN",
                                           ('q_roleA',))])

    @patch.object(hookenv, 'log')
    @patch.object(postgresql, 'pgidentifier')
    def test_ensure_extensions(self, pgidentifier, log):
        con = MagicMock()
        cur = con.cursor()

        pgidentifier.side_effect = lambda d: 'q_{}'.format(d)

        existing_extensions = set(['extA', 'extB'])
        wanted_extensions = set(['extB', 'extC'])

        cur.fetchall.return_value = [[x] for x in existing_extensions]
        postgresql.ensure_extensions(con, wanted_extensions)
        cur.execute.assert_has_calls([
            call('SELECT extname FROM pg_extension'),
            call('CREATE EXTENSION %s', ('q_extC',))])

    def test_addr_to_range(self):
        eggs = [('hostname', 'hostname'),
                ('192.168.1.1', '192.168.1.1/32'),
                ('192.168.1.0/24', '192.168.1.0/24'),
                ('::whatever::', '::whatever::/128'),
                ('::whatever::/64', '::whatever::/64'),
                ('unparseable nonsense', 'unparseable nonsense')]
        for addr, addr_range in eggs:
            with self.subTest(addr=addr):
                self.assertEqual(postgresql.addr_to_range(addr), addr_range)

    @patch.object(postgresql, 'version')
    @patch.object(postgresql, 'data_dir')
    @patch.object(postgresql, 'pg_ctl_path')
    @patch('subprocess.check_call')
    def test_is_running(self, check_call, pg_ctl_path, data_dir, version):
        version.return_value = '9.2'
        pg_ctl_path.return_value = '/path/to/pg_ctl'
        data_dir.return_value = '/path/to/DATADIR'
        self.assertTrue(postgresql.is_running())
        check_call.assert_called_once_with(['sudo', '-u', 'postgres',
                                            '/path/to/pg_ctl', 'status',
                                            '-D', '/path/to/DATADIR'],
                                           universal_newlines=True,
                                           stdout=subprocess.DEVNULL)

        # Exit code 3 is pg_ctl(1) speak for 'not running'
        check_call.side_effect = subprocess.CalledProcessError(3, 'whoops')
        self.assertFalse(postgresql.is_running())

        # Exit code 4 is pg_ctl(1) speak for 'wtf is the $DATADIR', PG9.4+
        version.return_value = '9.4'
        check_call.side_effect = subprocess.CalledProcessError(4, 'whoops')
        self.assertFalse(postgresql.is_running())
        version.return_value = '9.3'
        check_call.side_effect = subprocess.CalledProcessError(4, 'whoops')
        with self.assertRaises(subprocess.CalledProcessError) as x:
            postgresql.is_running()
        self.assertEqual(x.exception.returncode, 4)

        # Other failures bubble up, not that they should occur.
        check_call.side_effect = subprocess.CalledProcessError(42, 'whoops')
        with self.assertRaises(subprocess.CalledProcessError) as x:
            postgresql.is_running()
        self.assertEqual(x.exception.returncode, 42)

    @patch.object(postgresql, 'emit_pg_log')
    @patch.object(workloadstatus, 'status_set')
    @patch('subprocess.check_call')
    @patch.object(postgresql, 'version')
    def test_start(self, version, check_call, status_set, emit_pg_log):
        version.return_value = '9.9'

        # When it works, it works.
        postgresql.start()
        # Both -w and -t options are required to wait for startup.
        # We wait a long time, as startup might take a long time.
        # Maybe we should wait a lot longer.
        check_call.assert_called_once_with(['pg_ctlcluster', '9.9', 'main',
                                            'start', '--', '-w',
                                            '-t', '86400'],
                                           universal_newlines=True)
        self.assertFalse(emit_pg_log.called)

        # If it is already running, pg_ctlcluster returns code 2.
        # We block, and terminate whatever hook is running.
        check_call.side_effect = subprocess.CalledProcessError(2, 'whoops')
        check_call.reset_mock()
        postgresql.start()
        check_call.assert_called_once_with(['pg_ctlcluster', '9.9', 'main',
                                            'start', '--', '-w',
                                            '-t', '86400'],
                                           universal_newlines=True)

        # Other failures block the unit. Perhaps it is just taking too
        # perform recovery after a power outage.
        check_call.side_effect = subprocess.CalledProcessError(42, 'whoops')
        with self.assertRaises(SystemExit) as x:
            postgresql.start()
        status_set.assert_called_once_with('blocked', ANY)  # Set blocked.
        self.assertEqual(x.exception.code, 0)  # Terminated without error
        emit_pg_log.assert_called_once_with()  # Tail of log emitted to logs.

    @patch.object(hookenv, 'log')
    @patch.object(workloadstatus, 'status_set')
    @patch('subprocess.check_call')
    @patch.object(postgresql, 'version')
    def test_stop(self, version, check_call, status_set, log):
        version.return_value = '9.9'

        # Normal shutdown shuts down.
        postgresql.stop()
        # -t option is required to wait for shutdown to complete. -w not
        # required unlike 'start', but lets be explicit.
        check_call.assert_called_once_with(['pg_ctlcluster',
                                            '--mode', 'fast', '9.9', 'main',
                                            'stop', '--', '-w', '-t', '300'],
                                           universal_newlines=True)

        # If the server is not running, pg_ctlcluster(1) signals this with
        # returncode 2.
        check_call.side_effect = subprocess.CalledProcessError(2, 'whoops')
        check_call.reset_mock()
        postgresql.stop()
        # -t option is required to wait for shutdown to complete. -w not
        # required unlike 'start', but lets be explicit.
        check_call.assert_called_once_with(['pg_ctlcluster',
                                            '--mode', 'fast', '9.9', 'main',
                                            'stop', '--', '-w', '-t', '300'],
                                           universal_newlines=True)

        # If 'fast' shutdown fails, we retry with an 'immediate' shutdown
        check_call.side_effect = iter([subprocess.CalledProcessError(42, 'x'),
                                       None])
        check_call.reset_mock()
        postgresql.stop()
        check_call.assert_has_calls([
            call(['pg_ctlcluster', '--mode', 'fast', '9.9', 'main',
                  'stop', '--', '-w', '-t', '300'],
                 universal_newlines=True),
            call(['pg_ctlcluster', '--mode', 'immediate', '9.9', 'main',
                  'stop', '--', '-w', '-t', '300'],
                 universal_newlines=True)])

        # If both fail, we block the unit.
        check_call.side_effect = subprocess.CalledProcessError(42, 'x')
        with self.assertRaises(SystemExit) as x:
            postgresql.stop()
        status_set.assert_called_once_with('blocked', ANY)
        self.assertEqual(x.exception.code, 0)  # Exit cleanly

    @patch('subprocess.check_call')
    @patch.object(postgresql, 'version')
    def test_reload_config(self, version, check_call):
        version.return_value = '9.9'
        postgresql.reload_config()
        check_call.assert_called_once_with(['pg_ctlcluster', '9.9', 'main',
                                            'reload'])

    def test_parse_config(self):
        valid = [(r'# A comment', dict()),
                 (r'key_1 = value', dict(key_1='value')),
                 (r"key_2 ='quoted valu3'", dict(key_2='quoted valu3')),
                 (r"""key_3= 'foo "bar"'""", dict(key_3='foo "bar"')),
                 (r"""key_4='''bar\''""", dict(key_4="'bar'")),
                 (r"key_5=''", dict(key_5='')),
                 (r"", dict()),
                 (r'  # Another comment ', dict()),
                 (r"key_6='#'", dict(key_6='#')),
                 (r"key_7=42", dict(key_7='42')),
                 (r"key_8=3.142", dict(key_8='3.142')),
                 (r'key_9=-1', dict(key_9='-1'))]

        # The above examples all parse correctly.
        for raw, expected in valid:
            with self.subTest(raw=raw):
                self.assertDictEqual(postgresql.parse_config(raw), expected)

        # Concatenating them parses correctly to.
        combined_raw = []
        combined_expected = {}
        for raw, expected in valid:
            combined_raw.append(raw)
            combined_expected.update(expected)
        self.assertDictEqual(postgresql.parse_config('\n'.join(combined_raw)),
                             combined_expected)

        with self.assertRaises(SyntaxError) as x:
            postgresql.parse_config("=")
        self.assertEqual(str(x.exception), 'Missing key (line 1)')
        self.assertEqual(x.exception.lineno, 1)
        self.assertEqual(x.exception.text, "=")

        # We could be lazy here, since we are dealing with trusted input,
        # but meaningful error messages are helpful.
        with self.assertRaises(SyntaxError) as x:
            postgresql.parse_config('# comment\nkey=')
        self.assertEqual(str(x.exception), 'Missing value (line 2)')

        with self.assertRaises(SyntaxError) as x:
            postgresql.parse_config("key='unterminated")
        self.assertEqual(str(x.exception), 'Badly quoted value (line 1)')

        with self.assertRaises(SyntaxError) as x:
            postgresql.parse_config("key='unterminated 2 # comment")
        self.assertEqual(str(x.exception), 'Badly quoted value (line 1)')

        with self.assertRaises(SyntaxError) as x:
            postgresql.parse_config("key='unte''''")
        self.assertEqual(str(x.exception), 'Badly quoted value (line 1)')

        with self.assertRaises(SyntaxError) as x:
            postgresql.parse_config(r"key='\'")
        self.assertEqual(str(x.exception), 'Badly quoted value (line 1)')

    def test_convert_unit(self):
        c = postgresql.convert_unit
        self.assertEqual(c('10 kB', 'kB'), 10.0)
        self.assertEqual(c('10 MB', 'GB'), 10.0 / 1024)
        self.assertEqual(c('800 kB', '8kB'), 800.0 / 8)
        self.assertEqual(c('1TB', 'MB'), 1.0 * 1024 * 1024)
        self.assertEqual(c('42', 'MB'), 42.0)

        self.assertEqual(c('1s', 'ms'), 1000.0)
        self.assertEqual(c('1d', 'h'), 24.0)
        self.assertEqual(c('1 min', 'd'), 1.0 / (24 * 60))

        with self.assertRaises(ValueError) as x:
            c('10 MB', 'd')
        self.assertEqual(x.exception.args[0], '10 MB')
        self.assertEqual(x.exception.args[1], 'Cannot convert MB to d')

        with self.assertRaises(ValueError) as x:
            c('MB', 'MB')
        self.assertEqual(x.exception.args[0], 'MB')
        self.assertEqual(x.exception.args[1], 'Invalid number or unit')

        with self.assertRaises(ValueError) as x:
            c('1 KB', 'kB')  # Fail due to case sensitivity, per pg docs.
        self.assertEqual(x.exception.args[0], '1 KB')
        self.assertEqual(x.exception.args[1],
                         "Unknown conversion unit 'KB'. "
                         "Units are case sensitive.")

        with self.assertRaises(ValueError) as x:
            c('10.5MB', 'kB')  # Floats with units fail in postgresql.conf
        self.assertEqual(x.exception.args[0], '10.5MB')
        self.assertEqual(x.exception.args[1], 'Invalid number or unit')
