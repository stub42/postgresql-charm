import mocker
import os.path
import hooks


class TestJujuHost(object):
    """
    Testing object to intercept charmhelper calls and inject data, or make sure
    certain data is set.
    """
    def write_file(self, file_path, contents, owner=None, group=None,
                   perms=None):
        """
        Only write the file as requested. owner, group and perms untested.
        """
        with open(file_path, 'w') as target:
            target.write(contents)

    def mkdir(self, dir_path, owner, group, perms):
        """Not yet tested"""
        pass

    def service_start(self, service_name):
        """Not yet tested"""
        pass

    def service_reload(self, service_name):
        """Not yet tested"""
        pass

    def service_pwgen(self, service_name):
        """Not yet tested"""
        return ""

    def service_stop(self, service_name):
        """Not yet tested"""
        pass


class TestJuju(object):
    """
    Testing object to intercept juju calls and inject data, or make sure
    certain data is set.
    """

    _relation_data = {}
    _relation_ids = {}
    _relation_list = ("postgres/0",)

    def __init__(self):
        self._config = {
            "admin_addresses": "",
            "locale": "C",
            "encoding": "UTF-8",
            "extra_packages": "",
            "dumpfile_location": "None",
            "config_change_command": "reload",
            "version": "9.1",
            "cluster_name": "main",
            "listen_ip": "*",
            "listen_port": "5432",
            "max_connections": "100",
            "ssl": "True",
            "log_min_duration_statement": -1,
            "log_checkpoints": False,
            "log_connections": False,
            "log_disconnections": False,
            "log_line_prefix": "%t ",
            "log_lock_waits": False,
            "log_timezone": "UTC",
            "autovacuum": True,
            "log_autovacuum_min_duration": -1,
            "autovacuum_analyze_threshold": 50,
            "autovacuum_vacuum_scale_factor": 0.2,
            "autovacuum_analyze_scale_factor": 0.1,
            "autovacuum_vacuum_cost_delay": "20ms",
            "search_path": "\"$user\",public",
            "standard_conforming_strings": True,
            "hot_standby": False,
            "hot_standby_feedback": False,
            "wal_level": "minimal",
            "max_wal_senders": 0,
            "wal_keep_segments": 0,
            "replicated_wal_keep_segments": 5000,
            "archive_mode": False,
            "archive_command": "",
            "work_mem": "1MB",
            "maintenance_work_mem": "1MB",
            "performance_tuning": "auto",
            "kernel_shmall": 0,
            "kernel_shmmax": 0,
            "shared_buffers": "",
            "effective_cache_size": "",
            "temp_buffers": "1MB",
            "wal_buffers": "-1",
            "checkpoint_segments": 3,
            "random_page_cost": 4.0,
            "volume_ephemeral_storage": True,
            "volume_map": "",
            "volume_dev_regexp": "/dev/db[b-z]",
            "backup_dir": "/var/lib/postgresql/backups",
            "backup_schedule": "13 4 * * *",
            "backup_retention_count": 7,
            "nagios_context": "juju",
            "extra_archives": "",
            "advisory_lock_restart_key": 765}

    def relation_set(self, *args, **kwargs):
        """
        Capture result of relation_set into _relation_data, which
        can then be checked later.
        """
        if "relation_id" in kwargs:
            del kwargs["relation_id"]
        self._relation_data = dict(self._relation_data, **kwargs)
        for arg in args:
            (key, value) = arg.split("=")
            self._relation_data[key] = value

    def relation_ids(self, relation_name="db-admin"):
        """
        Return expected relation_ids for tests.  Feel free to expand
        as more tests are added.
        """
        return [self._relation_ids[name] for name in self._relation_ids.keys()
                if name.find(relation_name) == 0]

    def related_units(self, relid="db-admin:5"):
        """
        Return expected relation_ids for tests.  Feel free to expand
        as more tests are added.
        """
        return [name for name, value in self._relation_ids.iteritems()
                if value == relid]

    def relation_list(self):
        """
        Hardcode expected relation_list for tests.  Feel free to expand
        as more tests are added.
        """
        return list(self._relation_list)

    def unit_get(self, *args):
        """
        for now the only thing this is called for is "public-address",
        so it's a simplistic return.
        """
        return "localhost"

    def local_unit(self):
        return hooks.os.environ["JUJU_UNIT_NAME"]

    def charm_dir(self):
        return hooks.os.environ["CHARM_DIR"]

    def juju_log(self, *args, **kwargs):
        pass

    def log(self, *args, **kwargs):
        pass

    def config_get(self, scope=None):
        if scope is None:
            return self.config
        else:
            return self.config[scope]

    def relation_get(self, scope=None, unit_name=None, relation_id=None):
        pass


class TestHooks(mocker.MockerTestCase):

    def setUp(self):
        hooks.hookenv = TestJuju()
        hooks.host = TestJujuHost()
        hooks.juju_log_dir = self.makeDir()
        hooks.hookenv.config = lambda: hooks.hookenv._config
        #hooks.hookenv.localunit = lambda: "localhost"
        hooks.os.environ["JUJU_UNIT_NAME"] = "landscape/1"
        hooks.os.environ["CHARM_DIR"] = os.path.abspath(
            os.path.join(os.path.dirname(__file__), os.pardir))
        hooks.postgresql_sysctl = self.makeFile()
        hooks._get_system_ram = lambda: 1024   # MB
        hooks._get_page_size = lambda: 1024 * 1024  # bytes
        hooks._run_sysctl = lambda x: ""
        self.maxDiff = None

    def assertFileContains(self, filename, lines):
        """Make sure strings exist in a file."""
        with open(filename, "r") as fp:
            contents = fp.read()
        for line in lines:
            self.assertIn(line, contents)

    def assertNotFileContains(self, filename, lines):
        """Make sure strings do not exist in a file."""
        with open(filename, "r") as fp:
            contents = fp.read()
        for line in lines:
            self.assertNotIn(line, contents)

    def assertFilesEqual(self, file1, file2):
        """Given two filenames, compare them."""
        with open(file1, "r") as fp1:
            contents1 = fp1.read()
        with open(file2, "r") as fp2:
            contents2 = fp2.read()
        self.assertEqual(contents1, contents2)


class TestHooksService(TestHooks):

    def test_create_postgresql_config_wal_no_replication(self):
        """
        When postgresql is in C{standalone} mode, and participates in no
        C{replication} relations, default wal settings will be present.
        """
        config_outfile = self.makeFile()
        _run_sysctl = self.mocker.replace(hooks._run_sysctl)
        _run_sysctl(hooks.postgresql_sysctl)
        self.mocker.result(True)
        self.mocker.replay()
        hooks.create_postgresql_config(config_outfile)
        self.assertFileContains(
            config_outfile,
            ["wal_level = minimal", "max_wal_senders = 0",
             "wal_keep_segments = 0"])

    def test_create_postgresql_config_wal_with_replication(self):
        """
        When postgresql is in C{replicated} mode, and participates in a
        C{replication} relation, C{hot_standby} will be set to C{on},
        C{wal_level} will be enabled as C{hot_standby} and the
        C{max_wall_senders} will match the count of replication relations.
        The value of C{wal_keep_segments} will be the maximum of the configured
        C{wal_keep_segments} and C{replicated_wal_keep_segments}.
        """
        self.addCleanup(
            setattr, hooks.hookenv, "_relation_ids", {})
        hooks.hookenv._relation_ids = {
            "replication/0": "db-admin:5", "replication/1": "db-admin:6"}
        config_outfile = self.makeFile()
        _run_sysctl = self.mocker.replace(hooks._run_sysctl)
        _run_sysctl(hooks.postgresql_sysctl)
        self.mocker.result(True)
        self.mocker.replay()
        hooks.create_postgresql_config(config_outfile)
        self.assertFileContains(
            config_outfile,
            ["hot_standby = True", "wal_level = hot_standby",
             "max_wal_senders = 2", "wal_keep_segments = 5000"])

    def test_create_postgresql_config_wal_with_replication_max_override(self):
        """
        When postgresql is in C{replicated} mode, and participates in a
        C{replication} relation, C{hot_standby} will be set to C{on},
        C{wal_level} will be enabled as C{hot_standby}. The written value for
        C{max_wal_senders} will be the maximum of replication slave count and
        the configuration value for C{max_wal_senders}.
        The written value of C{wal_keep_segments} will be
        the maximum of the configuration C{wal_keep_segments} and
        C{replicated_wal_keep_segments}.
        """
        self.addCleanup(
            setattr, hooks.hookenv, "_relation_ids", ())
        hooks.hookenv._relation_ids = {
            "replication/0": "db-admin:5", "replication/1": "db-admin:6"}
        hooks.hookenv._config["max_wal_senders"] = "3"
        hooks.hookenv._config["wal_keep_segments"] = 1000
        hooks.hookenv._config["replicated_wal_keep_segments"] = 999
        config_outfile = self.makeFile()
        _run_sysctl = self.mocker.replace(hooks._run_sysctl)
        _run_sysctl(hooks.postgresql_sysctl)
        self.mocker.result(True)
        self.mocker.replay()
        hooks.create_postgresql_config(config_outfile)
        self.assertFileContains(
            config_outfile,
            ["hot_standby = True", "wal_level = hot_standby",
             "max_wal_senders = 3", "wal_keep_segments = 1000"])

    def test_postgresql_config_pgtune(self):
        """
        When automatic performance tuning is specified, pgtune will
        modify postgresql.conf. Automatic performance tuning is the default.
        """
        config_outfile = self.makeFile()
        _run_sysctl = self.mocker.replace(hooks._run_sysctl)
        _run_sysctl(hooks.postgresql_sysctl)
        self.mocker.result(True)
        self.mocker.replay()

        hooks.create_postgresql_config(config_outfile)

        raw_config = open(config_outfile, 'r').read()
        self.assert_('# pgtune wizard' in raw_config)

    def test_create_postgresql_config_performance_tune_auto_large_ram(self):
        """
        When configuration attribute C{performance_tune} is set to C{auto} and
        total RAM on a system is > 1023MB. It will automatically calculate
        values for the following attributes if these attributes were left as
        default values:
           - C{effective_cache_size} set to 75% of total RAM in MegaBytes
           - C{shared_buffers} set to 25% of total RAM in MegaBytes
           - C{kernel_shmmax} set to total RAM in bytes
           - C{kernel_shmall} equal to kernel_shmmax in pages
        """
        config_outfile = self.makeFile()
        _run_sysctl = self.mocker.replace(hooks._run_sysctl)
        _run_sysctl(hooks.postgresql_sysctl)
        self.mocker.result(True)
        self.mocker.replay()
        hooks.create_postgresql_config(config_outfile)
        self.assertFileContains(
            config_outfile,
            ["shared_buffers = 256MB", "effective_cache_size = 768MB"])
        self.assertFileContains(
            hooks.postgresql_sysctl,
            ["kernel.shmall = 1025\nkernel.shmmax = 1073742848"])

    def test_create_postgresql_config_performance_tune_auto_small_ram(self):
        """
        When configuration attribute C{performance_tune} is set to C{auto} and
        total RAM on a system is <= 1023MB. It will automatically calculate
        values for the following attributes if these attributes were left as
        default values:
           - C{effective_cache_size} set to 75% of total RAM in MegaBytes
           - C{shared_buffers} set to 15% of total RAM in MegaBytes
           - C{kernel_shmmax} set to total RAM in bytes
           - C{kernel_shmall} equal to kernel_shmmax in pages
        """
        hooks._get_system_ram = lambda: 1023   # MB
        config_outfile = self.makeFile()
        _run_sysctl = self.mocker.replace(hooks._run_sysctl)
        _run_sysctl(hooks.postgresql_sysctl)
        self.mocker.result(True)
        self.mocker.replay()
        hooks.create_postgresql_config(config_outfile)
        self.assertFileContains(
            config_outfile,
            ["shared_buffers = 153MB", "effective_cache_size = 767MB"])
        self.assertFileContains(
            hooks.postgresql_sysctl,
            ["kernel.shmall = 1024\nkernel.shmmax = 1072694272"])

    def test_create_postgresql_config_performance_tune_auto_overridden(self):
        """
        When configuration attribute C{performance_tune} is set to C{auto} any
        non-default values for the configuration parameters below will be used
        instead of the automatically calculated values.
           - C{effective_cache_size}
           - C{shared_buffers}
           - C{kernel_shmmax}
           - C{kernel_shmall}
        """
        hooks.hookenv._config["effective_cache_size"] = "999MB"
        hooks.hookenv._config["shared_buffers"] = "101MB"
        hooks.hookenv._config["kernel_shmmax"] = 50000
        hooks.hookenv._config["kernel_shmall"] = 500
        hooks._get_system_ram = lambda: 1023   # MB
        config_outfile = self.makeFile()
        _run_sysctl = self.mocker.replace(hooks._run_sysctl)
        _run_sysctl(hooks.postgresql_sysctl)
        self.mocker.result(True)
        self.mocker.replay()
        hooks.create_postgresql_config(config_outfile)
        self.assertFileContains(
            config_outfile,
            ["shared_buffers = 101MB", "effective_cache_size = 999MB"])
        self.assertFileContains(
            hooks.postgresql_sysctl,
            ["kernel.shmall = 1024\nkernel.shmmax = 1072694272"])
