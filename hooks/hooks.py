#!/usr/bin/env python
# vim: et ai ts=4 sw=4:

from contextlib import contextmanager
import commands
import cPickle as pickle
from distutils.version import StrictVersion
import glob
from grp import getgrnam
import os
from pwd import getpwnam
import re
import shutil
import socket
import subprocess
import sys
from tempfile import NamedTemporaryFile
from textwrap import dedent
import time
import urlparse

from charmhelpers import fetch
from charmhelpers.core import hookenv, host
from charmhelpers.core.hookenv import (
    CRITICAL, ERROR, WARNING, INFO, DEBUG)

try:
    import psycopg2
    from jinja2 import Template
except ImportError:
    fetch.apt_update(fatal=True)
    fetch.apt_install(['python-psycopg2', 'python-jinja2'], fatal=True)
    import psycopg2
    from jinja2 import Template

from psycopg2.extensions import AsIs
from jinja2 import Environment, FileSystemLoader


hooks = hookenv.Hooks()


class State(dict):
    """Encapsulate state common to the unit for republishing to relations."""
    def __init__(self, state_file):
        super(State, self).__init__()
        self._state_file = state_file
        self.load()

    def load(self):
        '''Load stored state from local disk.'''
        if os.path.exists(self._state_file):
            state = pickle.load(open(self._state_file, 'rb'))
        else:
            state = {}
        self.clear()

        self.update(state)

    def save(self):
        '''Store state to local disk.'''
        state = {}
        state.update(self)
        old_mask = os.umask(0o077)  # This file contains database passwords!
        try:
            pickle.dump(state, open(self._state_file, 'wb'))
        finally:
            os.umask(old_mask)

    def publish(self):
        """Publish relevant unit state to relations"""

        def add(state_dict, key):
            if key in self:
                state_dict[key] = self[key]

        client_state = {}
        add(client_state, 'state')

        for relid in hookenv.relation_ids('db'):
            hookenv.relation_set(relid, client_state)

        for relid in hookenv.relation_ids('db-admin'):
            hookenv.relation_set(relid, client_state)

        replication_state = dict(client_state)

        add(replication_state, 'replication_password')
        add(replication_state, 'port')
        add(replication_state, 'wal_received_offset')
        add(replication_state, 'following')
        add(replication_state, 'client_relations')

        authorized = self.get('authorized', None)
        if authorized:
            replication_state['authorized'] = ' '.join(sorted(authorized))

        for relid in hookenv.relation_ids('replication'):
            hookenv.relation_set(relid, replication_state)

        for relid in hookenv.relation_ids('master'):
            hookenv.relation_set(relid, state=self.get('state'))

        log('saving local state', DEBUG)
        self.save()


def volume_get_all_mounted():
    command = ("mount |egrep %s" % external_volume_mount)
    status, output = commands.getstatusoutput(command)
    if status != 0:
        return None
    return output


def postgresql_autostart(enabled):
    postgresql_config_dir = _get_postgresql_config_dir()
    startup_file = os.path.join(postgresql_config_dir, 'start.conf')
    if enabled:
        log("Enabling PostgreSQL startup in {}".format(startup_file))
        mode = 'auto'
    else:
        log("Disabling PostgreSQL startup in {}".format(startup_file))
        mode = 'manual'
    template_file = "{}/templates/start_conf.tmpl".format(hookenv.charm_dir())
    contents = Template(open(template_file).read()).render({'mode': mode})
    host.write_file(
        startup_file, contents, 'postgres', 'postgres', perms=0o644)


def create_postgresql_config(config_file):
    '''Create the postgresql.conf file'''
    if config_data["performance_tuning"].lower() != "manual":
        total_ram = _get_system_ram()
        config_data["kernel_shmmax"] = (int(total_ram) * 1024 * 1024) + 1024
        config_data["kernel_shmall"] = config_data["kernel_shmmax"]

    tune_postgresql_config(config_file)


def tune_postgresql_config(config_file):
    tune_workload = hookenv.config('performance_tuning').lower()
    if tune_workload == "manual":
        return  # Requested no autotuning.

    if tune_workload == "auto":
        tune_workload = "mixed"  # Pre-pgtune backwards compatibility.

    with NamedTemporaryFile() as tmp_config:
        run(['pgtune', '-i', config_file, '-o', tmp_config.name,
             '-T', tune_workload,
             '-c', str(hookenv.config('max_connections'))])
        host.write_file(
            config_file, open(tmp_config.name, 'r').read(),
            owner='postgres', group='postgres', perms=0o600)


def install_postgresql_crontab(output_file):
    '''Create the postgres user's crontab'''
    config_data = hookenv.config()
    config_data['scripts_dir'] = postgresql_scripts_dir
    config_data['swiftwal_backup_command'] = swiftwal_backup_command()
    config_data['swiftwal_prune_command'] = swiftwal_prune_command()
    config_data['wal_e_backup_command'] = wal_e_backup_command()
    config_data['wal_e_prune_command'] = wal_e_prune_command()

    charm_dir = hookenv.charm_dir()
    template_file = "{}/templates/postgres.cron.tmpl".format(charm_dir)
    crontab_template = Template(
        open(template_file).read()).render(config_data)
    host.write_file(output_file, crontab_template, perms=0600)


def create_recovery_conf(master_host, master_port, restart_on_change=False):
    if hookenv.config('manual_replication'):
        log('manual_replication; should not be here', CRITICAL)
        raise RuntimeError('manual_replication; should not be here')

    version = pg_version()
    cluster_name = hookenv.config('cluster_name')
    postgresql_cluster_dir = os.path.join(
        postgresql_data_dir, version, cluster_name)

    recovery_conf_path = os.path.join(postgresql_cluster_dir, 'recovery.conf')
    if os.path.exists(recovery_conf_path):
        old_recovery_conf = open(recovery_conf_path, 'r').read()
    else:
        old_recovery_conf = None

    charm_dir = hookenv.charm_dir()
    streaming_replication = hookenv.config('streaming_replication')
    template_file = "{}/templates/recovery.conf.tmpl".format(charm_dir)
    params = dict(
        host=master_host, port=master_port,
        password=local_state['replication_password'],
        streaming_replication=streaming_replication)
    if hookenv.config('wal_e_storage_uri'):
        params['restore_command'] = wal_e_restore_command()
    elif hookenv.config('swiftwal_log_shipping'):
        params['restore_command'] = swiftwal_restore_command()
    recovery_conf = Template(open(template_file).read()).render(params)
    log(recovery_conf, DEBUG)
    host.write_file(
        os.path.join(postgresql_cluster_dir, 'recovery.conf'),
        recovery_conf, owner="postgres", group="postgres", perms=0o600)

    if restart_on_change and old_recovery_conf != recovery_conf:
        log("recovery.conf updated. Restarting to take effect.")
        postgresql_restart()


def validate_config():
    """
    Sanity check charm configuration, aborting the script if
    we have bogus config values or config changes the charm does not yet
    (or cannot) support.
    """
    valid = True
    config_data = hookenv.config()

    valid_workloads = [
        'dw', 'oltp', 'web', 'mixed', 'desktop', 'manual', 'auto']
    requested_workload = config_data['performance_tuning'].lower()
    if requested_workload not in valid_workloads:
        valid = False
        log('Invalid performance_tuning setting {}'.format(requested_workload),
            CRITICAL)
    if requested_workload == 'auto':
        log("'auto' performance_tuning deprecated. Using 'mixed' tuning",
            WARNING)


# -----------------------------------------------------------------------------
# Core logic for permanent storage changes:
# NOTE the only 2 "True" return points:
#   1) symlink already pointing to existing storage (no-op)
#   2) new storage properly initialized:
#     - if fresh new storage dir: rsync existing data
#     - manipulate /var/lib/postgresql/VERSION/CLUSTER symlink
# -----------------------------------------------------------------------------
def config_changed_volume_apply(mount_point):
    version = pg_version()
    cluster_name = hookenv.config('cluster_name')
    data_directory_path = os.path.join(
        postgresql_data_dir, version, cluster_name)

    assert(data_directory_path)

    if not os.path.exists(data_directory_path):
        log(
            "postgresql data dir {} not found, "
            "not applying changes.".format(data_directory_path),
            CRITICAL)
        return False

    new_pg_dir = os.path.join(mount_point, "postgresql")
    new_pg_version_cluster_dir = os.path.join(
        new_pg_dir, version, cluster_name)
    if not mount_point:
        log(
            "invalid mount point = {}, "
            "not applying changes.".format(mount_point), ERROR)
        return False

    if ((os.path.islink(data_directory_path) and
         os.readlink(data_directory_path) == new_pg_version_cluster_dir and
         os.path.isdir(new_pg_version_cluster_dir))):
        log(
            "postgresql data dir '{}' already points "
            "to {}, skipping storage changes.".format(
                data_directory_path, new_pg_version_cluster_dir))
        log(
            "existing-symlink: to fix/avoid UID changes from "
            "previous units, doing: "
            "chown -R postgres:postgres {}".format(new_pg_dir))
        run("chown -R postgres:postgres %s" % new_pg_dir)
        return True

    # Create a directory structure below "new" mount_point as
    #   external_volume_mount/postgresql/9.1/main
    for new_dir in [new_pg_dir,
                    os.path.join(new_pg_dir, version),
                    new_pg_version_cluster_dir]:
        if not os.path.isdir(new_dir):
            log("mkdir %s".format(new_dir))
            host.mkdir(new_dir, owner="postgres", perms=0o700)
    # Carefully build this symlink, e.g.:
    # /var/lib/postgresql/9.1/main ->
    # external_volume_mount/postgresql/9.1/main
    # but keep previous "main/"  directory, by renaming it to
    # main-$TIMESTAMP
    if not postgresql_stop() and postgresql_is_running():
        log("postgresql_stop() failed - can't migrate data.", ERROR)
        return False
    if not os.path.exists(os.path.join(
            new_pg_version_cluster_dir, "PG_VERSION")):
        log("migrating PG data {}/ -> {}/".format(
            data_directory_path, new_pg_version_cluster_dir), WARNING)
        # void copying PID file to perm storage (shouldn't be any...)
        command = "rsync -a --exclude postmaster.pid {}/ {}/".format(
            data_directory_path, new_pg_version_cluster_dir)
        log("run: {}".format(command))
        run(command)
    try:
        os.rename(data_directory_path, "{}-{}".format(
            data_directory_path, int(time.time())))
        log("NOTICE: symlinking {} -> {}".format(
            new_pg_version_cluster_dir, data_directory_path))
        os.symlink(new_pg_version_cluster_dir, data_directory_path)
        run("chown -h postgres:postgres {}".format(data_directory_path))
        log(
            "after-symlink: to fix/avoid UID changes from "
            "previous units, doing: "
            "chown -R postgres:postgres {}".format(new_pg_dir))
        run("chown -R postgres:postgres {}".format(new_pg_dir))
        return True
    except OSError:
        log("failed to symlink {} -> {}".format(
            data_directory_path, mount_point), CRITICAL)
        return False


def reset_manual_replication_state():
    '''In manual replication mode, the state of the local database cluster
    is outside of Juju's control. We need to detect and update the charm
    state to match reality.
    '''
    if hookenv.config('manual_replication'):
        if os.path.exists('recovery.conf'):
            local_state['state'] = 'hot standby'
        elif slave_count():
            local_state['state'] = 'master'
        else:
            local_state['state'] = 'standalone'
        local_state.publish()


@hooks.hook()
def config_changed(force_restart=False, mount_point=None):
    validate_config()
    config_data = hookenv.config()
    update_repos_and_packages()

    if mount_point is not None:
        # config_changed_volume_apply will stop the service if it finds
        # it necessary, ie: new volume setup
        if config_changed_volume_apply(mount_point=mount_point):
            postgresql_autostart(True)
        else:
            postgresql_autostart(False)
            postgresql_stop()
            mounts = volume_get_all_mounted()
            if mounts:
                log("current mounted volumes: {}".format(mounts))
            log(
                "Disabled and stopped postgresql service "
                "(config_changed_volume_apply failure)", ERROR)
            sys.exit(1)

    reset_manual_replication_state()

    postgresql_config_dir = _get_postgresql_config_dir(config_data)
    postgresql_config = os.path.join(postgresql_config_dir, "postgresql.conf")
    postgresql_hba = os.path.join(postgresql_config_dir, "pg_hba.conf")
    postgresql_ident = os.path.join(postgresql_config_dir, "pg_ident.conf")

    create_postgresql_config(postgresql_config)
    create_postgresql_ident(postgresql_ident)  # Do this before pg_hba.conf.
    generate_postgresql_hba(postgresql_hba)
    create_ssl_cert(os.path.join(
        postgresql_data_dir, pg_version(), config_data['cluster_name']))
    create_swiftwal_config()
    create_wal_e_envdir()
    update_service_port()
    update_nrpe_checks()
    write_metrics_cronjob('/usr/local/bin/postgres_to_statsd.py',
                          '/etc/cron.d/postgres_metrics')

    # If an external mountpoint has caused an old, existing DB to be
    # mounted, we need to ensure that all the users, databases, roles
    # etc. exist with known passwords.
    if local_state['state'] in ('standalone', 'master'):
        client_relids = (
            hookenv.relation_ids('db') + hookenv.relation_ids('db-admin'))
        for relid in client_relids:
            rel = hookenv.relation_get(rid=relid, unit=hookenv.local_unit())
            client_rel = None
            for unit in hookenv.related_units(relid):
                client_rel = hookenv.relation_get(unit=unit, rid=relid)
            if not client_rel:
                continue  # No client units - in between departed and broken?

            database = rel.get('database')
            if database is None:
                continue  # The relation exists, but we haven't joined it yet.

            roles = filter(None, (client_rel.get('roles') or '').split(","))
            user = rel.get('user')
            if user:
                admin = relid.startswith('db-admin')
                password = create_user(user, admin=admin)
                reset_user_roles(user, roles)
                hookenv.relation_set(relid, password=password)

            schema_user = rel.get('schema_user')
            if schema_user:
                schema_password = create_user(schema_user)
                hookenv.relation_set(relid, schema_password=schema_password)

            if user and schema_user and not (
                    database is None or database == 'all'):
                ensure_database(user, schema_user, database)

    if force_restart:
        postgresql_restart()
    postgresql_reload_or_restart()

    # In case the log_line_prefix has changed, inform syslog consumers.
    for relid in hookenv.relation_ids('syslog'):
        hookenv.relation_set(
            relid, log_line_prefix=hookenv.config('log_line_prefix'))


@hooks.hook()
def install(run_pre=True, force_restart=True):
    if run_pre:
        for f in glob.glob('exec.d/*/charm-pre-install'):
            if os.path.isfile(f) and os.access(f, os.X_OK):
                subprocess.check_call(['sh', '-c', f])

    validate_config()

    config_data = hookenv.config()
    update_repos_and_packages()
    if 'state' not in local_state:
        log('state not in {}'.format(local_state.keys()), DEBUG)
        # Fresh installation. Because this function is invoked by both
        # the install hook and the upgrade-charm hook, we need to guard
        # any non-idempotent setup. We should probably fix this; it
        # seems rather fragile.
        local_state.setdefault('state', 'standalone')
        log(repr(local_state.keys()), DEBUG)

        # Drop the cluster created when the postgresql package was
        # installed, and rebuild it with the requested locale and encoding.
        version = pg_version()
        for ver, name in lsclusters(slice(2)):
            if version == ver and name == 'main':
                run("pg_dropcluster --stop {} main".format(version))
        listen_port = config_data.get('listen_port', None)
        if listen_port:
            port_opt = "--port={}".format(config_data['listen_port'])
        else:
            port_opt = ''
        createcluster()
        assert (
            not port_opt
            or get_service_port() == config_data['listen_port']), (
            'allocated port {!r} != {!r}'.format(
                get_service_port(), config_data['listen_port']))
        local_state['port'] = get_service_port()
        log('publishing state', DEBUG)
        local_state.publish()

    postgresql_backups_dir = (
        config_data['backup_dir'].strip() or
        os.path.join(postgresql_data_dir, 'backups'))

    host.mkdir(postgresql_backups_dir, owner="postgres", perms=0o755)
    host.mkdir(postgresql_scripts_dir, owner="postgres", perms=0o755)
    host.mkdir(postgresql_logs_dir, owner="postgres", perms=0o755)
    paths = {
        'base_dir': postgresql_data_dir,
        'backup_dir': postgresql_backups_dir,
        'scripts_dir': postgresql_scripts_dir,
        'logs_dir': postgresql_logs_dir,
    }
    charm_dir = hookenv.charm_dir()
    template_file = "{}/templates/pg_backup_job.tmpl".format(charm_dir)
    backup_job = Template(open(template_file).read()).render(paths)
    host.write_file(
        os.path.join(postgresql_scripts_dir, 'dump-pg-db'),
        open('scripts/pgbackup.py', 'r').read(), perms=0o755)
    host.write_file(
        os.path.join(postgresql_scripts_dir, 'pg_backup_job'),
        backup_job, perms=0755)
    install_postgresql_crontab(postgresql_crontab)

    # Create this empty log file on installation to avoid triggering
    # spurious monitoring system alerts, per Bug #1329816.
    if not os.path.exists(backup_log):
        host.write_file(backup_log, '', 'postgres', 'postgres', 0664)

    hookenv.open_port(get_service_port())

    # Ensure at least minimal access granted for hooks to run.
    # Reload because we are using the default cluster setup and started
    # when we installed the PostgreSQL packages.
    config_changed(force_restart=force_restart)

    snapshot_relations()


@hooks.hook()
def upgrade_charm():
    """Handle saving state during an upgrade-charm hook.

    When upgrading from an installation using volume-map, we migrate
    that installation to use the storage subordinate charm by remounting
    a mountpath that the storage subordinate maintains. We exit(1) only to
    raise visibility to manual procedure that we log in juju logs below for the
    juju admin to finish the migration by relating postgresql to the storage
    and block-storage-broker services. These steps are generalised in the
    README as well.
    """
    install(run_pre=False, force_restart=False)
    snapshot_relations()
    version = pg_version()
    cluster_name = hookenv.config('cluster_name')
    data_directory_path = os.path.join(
        postgresql_data_dir, version, cluster_name)
    if (os.path.islink(data_directory_path)):
        link_target = os.readlink(data_directory_path)
        if "/srv/juju" in link_target:
            # Then we just upgraded from an installation that was using
            # charm config volume_map definitions. We need to stop postgresql
            # and remount the device where the storage subordinate expects to
            # control the mount in the future if relations/units change
            volume_id = link_target.split("/")[3]
            unit_name = hookenv.local_unit()
            new_mount_root = external_volume_mount
            new_pg_version_cluster_dir = os.path.join(
                new_mount_root, "postgresql", version, cluster_name)
            if not os.exists(new_mount_root):
                os.mkdir(new_mount_root)
            log("\n"
                "WARNING: %s unit has external volume id %s mounted via the\n"
                "deprecated volume-map and volume-ephemeral-storage\n"
                "configuration parameters.\n"
                "These parameters are no longer available in the postgresql\n"
                "charm in favor of using the volume_map parameter in the\n"
                "storage subordinate charm.\n"
                "We are migrating the attached volume to a mount path which\n"
                "can be managed by the storage subordinate charm. To\n"
                "continue using this volume_id with the storage subordinate\n"
                "follow this procedure.\n-----------------------------------\n"
                "1. cat > storage.cfg <<EOF\nstorage:\n"
                "  provider: block-storage-broker\n"
                "  root: %s\n"
                "  volume_map: \"{%s: %s}\"\nEOF\n2. juju deploy "
                "--config storage.cfg storage\n"
                "3. juju deploy block-storage-broker\n4. juju add-relation "
                "block-storage-broker storage\n5. juju resolved --retry "
                "%s\n6. juju add-relation postgresql storage\n"
                "-----------------------------------\n" %
                (unit_name, volume_id, new_mount_root, unit_name, volume_id,
                 unit_name), WARNING)
            postgresql_stop()
            os.unlink(data_directory_path)
            log("Unmounting external storage due to charm upgrade: %s" %
                link_target)
            try:
                subprocess.check_output(
                    "umount /srv/juju/%s" % volume_id, shell=True)
                # Since e2label truncates labels to 16 characters use only the
                # first 16 characters of the volume_id as that's what was
                # set by old versions of postgresql charm
                subprocess.check_call(
                    "mount -t ext4 LABEL=%s %s" %
                    (volume_id[:16], new_mount_root), shell=True)
            except subprocess.CalledProcessError, e:
                log("upgrade-charm mount migration failed. %s" % str(e), ERROR)
                sys.exit(1)

            log("NOTICE: symlinking {} -> {}".format(
                new_pg_version_cluster_dir, data_directory_path))
            os.symlink(new_pg_version_cluster_dir, data_directory_path)
            run("chown -h postgres:postgres {}".format(data_directory_path))
            postgresql_start()  # Will exit(1) if issues
            log("Remount and restart success for this external volume.\n"
                "This current running installation will break upon\n"
                "add/remove postgresql units or relations if you do not\n"
                "follow the above procedure to ensure your external\n"
                "volumes are preserved by the storage subordinate charm.",
                WARNING)
            # So juju admins can see the hook fail and note the steps to fix
            # per our WARNINGs above
            sys.exit(1)


@contextmanager
def pgpass():
    passwords = {}

    # Replication.
    # pg_basebackup only works with the password in .pgpass, or entered
    # at the command prompt.
    if 'replication_password' in local_state:
        passwords['juju_replication'] = local_state['replication_password']

    pgpass_contents = '\n'.join(
        "*:*:*:{}:{}".format(username, password)
        for username, password in passwords.items())
    pgpass_file = NamedTemporaryFile()
    pgpass_file.write(pgpass_contents)
    pgpass_file.flush()
    os.chown(pgpass_file.name, getpwnam('postgres').pw_uid, -1)
    os.chmod(pgpass_file.name, 0o400)
    org_pgpassfile = os.environ.get('PGPASSFILE', None)
    os.environ['PGPASSFILE'] = pgpass_file.name
    try:
        yield pgpass_file.name
    finally:
        if org_pgpassfile is None:
            del os.environ['PGPASSFILE']
        else:
            os.environ['PGPASSFILE'] = org_pgpassfile


def authorized_by(unit):
    '''Return True if the peer has authorized our database connections.'''
    for relid in hookenv.relation_ids('replication'):
        relation = hookenv.relation_get(unit=unit, rid=relid)
        authorized = relation.get('authorized', '').split()
        return hookenv.local_unit() in authorized


def promote_database():
    '''Take the database out of recovery mode.'''
    config_data = hookenv.config()
    version = pg_version()
    cluster_name = config_data['cluster_name']
    postgresql_cluster_dir = os.path.join(
        postgresql_data_dir, version, cluster_name)
    recovery_conf = os.path.join(postgresql_cluster_dir, 'recovery.conf')
    if os.path.exists(recovery_conf):
        # Rather than using 'pg_ctl promote', we do the promotion
        # this way to avoid creating a timeline change. Switch this
        # to using 'pg_ctl promote' once PostgreSQL propagates
        # timeline changes via streaming replication.
        os.unlink(recovery_conf)
        postgresql_restart()


def follow_database(master):
    '''Connect the database as a streaming replica of the master.'''
    master_relation = hookenv.relation_get(unit=master)
    create_recovery_conf(
        master_relation['private-address'],
        master_relation['port'], restart_on_change=True)


def elected_master():
    """Return the unit that should be master, or None if we don't yet know."""
    if local_state['state'] == 'master':
        log("I am already the master", DEBUG)
        return hookenv.local_unit()

    if local_state['state'] == 'hot standby':
        log("I am already following {}".format(
            local_state['following']), DEBUG)
        return local_state['following']

    replication_relid = hookenv.relation_ids('replication')[0]
    replication_units = hookenv.related_units(replication_relid)

    if local_state['state'] == 'standalone':
        log("I'm a standalone unit wanting to participate in replication")
        existing_replication = False
        for unit in replication_units:
            # If another peer thinks it is the master, believe it.
            remote_state = hookenv.relation_get(
                'state', unit, replication_relid)
            if remote_state == 'master':
                log("{} thinks it is the master, believing it".format(
                    unit), DEBUG)
                return unit

            # If we find a peer that isn't standalone, we know
            # replication has already been setup at some point.
            if remote_state != 'standalone':
                existing_replication = True

        # If we are joining a peer relation where replication has
        # already been setup, but there is currently no master, wait
        # until one of the remaining participating units has been
        # promoted to master. Only they have the data we need to
        # preserve.
        if existing_replication:
            log("Peers participating in replication need to elect a master",
                DEBUG)
            return None

        # There are no peers claiming to be master, and there is no
        # election in progress, so lowest numbered unit wins.
        units = replication_units + [hookenv.local_unit()]
        master = unit_sorted(units)[0]
        if master == hookenv.local_unit():
            log("I'm Master - lowest numbered unit in new peer group")
            return master
        else:
            log("Waiting on {} to declare itself Master".format(master), DEBUG)
            return None

    if local_state['state'] == 'failover':
        former_master = local_state['following']
        log("Failover from {}".format(former_master))

        units_not_in_failover = set()
        candidates = set()
        for unit in replication_units:
            if unit == former_master:
                log("Found dying master {}".format(unit), DEBUG)
                continue

            relation = hookenv.relation_get(unit=unit, rid=replication_relid)

            if relation['state'] == 'master':
                log("{} says it already won the election".format(unit),
                    INFO)
                return unit

            if relation['state'] == 'failover':
                candidates.add(unit)

            elif relation['state'] != 'standalone':
                units_not_in_failover.add(unit)

        if units_not_in_failover:
            log("{} unaware of impending election. Deferring result.".format(
                " ".join(unit_sorted(units_not_in_failover))))
            return None

        log("Election in progress")
        winner = None
        winning_offset = -1
        candidates.add(hookenv.local_unit())
        # Sort the unit lists so we get consistent results in a tie
        # and lowest unit number wins.
        for unit in unit_sorted(candidates):
            relation = hookenv.relation_get(unit=unit, rid=replication_relid)
            if int(relation['wal_received_offset']) > winning_offset:
                winner = unit
                winning_offset = int(relation['wal_received_offset'])

        # All remaining hot standbys are in failover mode and have
        # reported their wal_received_offset. We can declare victory.
        if winner == hookenv.local_unit():
            log("I won the election, announcing myself winner")
            return winner
        else:
            log("Waiting for {} to announce its victory".format(winner),
                DEBUG)
            return None


@hooks.hook('replication-relation-joined', 'replication-relation-changed')
def replication_relation_joined_changed():
    config_changed()  # Ensure minimal replication settings.

    # Now that pg_hba.conf has been regenerated and loaded, inform related
    # units that they have been granted replication access.
    authorized_units = set()
    for unit in hookenv.related_units():
        authorized_units.add(unit)
    local_state['authorized'] = authorized_units

    if hookenv.config('manual_replication'):
        log('manual_replication, nothing to do')
        return

    master = elected_master()

    # Handle state changes:
    #  - Fresh install becoming the master
    #  - Fresh install becoming a hot standby
    #  - Hot standby being promoted to master

    if master is None:
        log("Master is not yet elected. Deferring.")

    elif master == hookenv.local_unit():
        if local_state['state'] != 'master':
            log("I have elected myself master")
            promote_database()
            if 'following' in local_state:
                del local_state['following']
            if 'wal_received_offset' in local_state:
                del local_state['wal_received_offset']
            if 'paused_at_failover' in local_state:
                del local_state['paused_at_failover']
            local_state['state'] = 'master'

            # Publish credentials to hot standbys so they can connect.
            replication_password = create_user(
                'juju_replication', replication=True)
            local_state['replication_password'] = replication_password
            local_state['client_relations'] = ' '.join(
                hookenv.relation_ids('db') + hookenv.relation_ids('db-admin'))
            local_state.publish()

        else:
            log("I am master and remain master")

    elif not authorized_by(master):
        log("I need to follow {} but am not yet authorized".format(master))

    else:
        log("Syncing replication_password from {}".format(master), DEBUG)
        local_state['replication_password'] = hookenv.relation_get(
            'replication_password', master)

        if 'following' not in local_state:
            log("Fresh unit. I will clone {} and become a hot standby".format(
                master))

            master_ip = hookenv.relation_get('private-address', master)
            master_port = hookenv.relation_get('port', master)
            assert master_port is not None, 'No master port set'

            clone_database(master, master_ip, master_port)

            local_state['state'] = 'hot standby'
            local_state['following'] = master
            if 'wal_received_offset' in local_state:
                del local_state['wal_received_offset']

        elif local_state['following'] == master:
            log("I am a hot standby already following {}".format(master))

            # Replication connection details may have changed, so
            # ensure we are still following.
            follow_database(master)

        else:
            log("I am a hot standby following new master {}".format(master))
            follow_database(master)
            if not local_state.get("paused_at_failover", None):
                run_sql_as_postgres("SELECT pg_xlog_replay_resume()")
            local_state['state'] = 'hot standby'
            local_state['following'] = master
            del local_state['wal_received_offset']
            del local_state['paused_at_failover']

        publish_hot_standby_credentials()
        postgresql_hba = os.path.join(
            _get_postgresql_config_dir(), "pg_hba.conf")
        generate_postgresql_hba(postgresql_hba)

    # Swift container name make have changed, so regenerate the SwiftWAL
    # config. This can go away when we have real leader election and can
    # safely share a single container.
    create_swiftwal_config()
    create_wal_e_envdir()

    local_state.publish()


def publish_hot_standby_credentials():
        # Block until users and database has replicated, so we know the
        # connection details we publish are actually valid. This will
        # normally be pretty much instantaneous. Do not block if we are
        # running in manual replication mode, as it is outside of juju's
        # control when replication is actually setup and running.
        if not hookenv.config('manual_replication'):
            timeout = 60
            start = time.time()
            while time.time() < start + timeout:
                cur = db_cursor(autocommit=True)
                cur.execute('select datname from pg_database')
                if cur.fetchone() is not None:
                    break
                del cur
                log('Waiting for database {} to be replicated'.format(
                    connection_settings['database']))
                time.sleep(10)


@hooks.hook()
def replication_relation_departed():
    '''A unit has left the replication peer group.'''
    remote_unit = hookenv.remote_unit()

    assert remote_unit is not None

    log("{} has left the peer group".format(remote_unit))

    # If we are the last unit standing, we become standalone
    remaining_peers = set(hookenv.related_units(hookenv.relation_id()))
    remaining_peers.discard(remote_unit)  # Bug #1192433

    # True if we were following the departed unit.
    following_departed = (local_state.get('following', None) == remote_unit)

    if remaining_peers and not following_departed:
        log("Remaining {}".format(local_state['state']))

    elif remaining_peers and following_departed:
        # If the unit being removed was our master, prepare for failover.
        # We need to suspend replication to ensure that the replay point
        # remains consistent throughout the election, and publish that
        # replay point. Once all units have entered this steady state,
        # we can identify the most up to date hot standby and promote it
        # to be the new master.
        log("Entering failover state")
        cur = db_cursor(autocommit=True)
        cur.execute("SELECT pg_is_xlog_replay_paused()")
        already_paused = cur.fetchone()[0]
        local_state["paused_at_failover"] = already_paused
        if not already_paused:
            cur.execute("SELECT pg_xlog_replay_pause()")
        # Switch to failover state. Don't cleanup the 'following'
        # setting because having access to the former master is still
        # useful.
        local_state['state'] = 'failover'
        local_state['wal_received_offset'] = postgresql_wal_received_offset()

    else:
        log("Last unit standing. Switching from {} to standalone.".format(
            local_state['state']))
        promote_database()
        local_state['state'] = 'standalone'
        if 'following' in local_state:
            del local_state['following']
        if 'wal_received_offset' in local_state:
            del local_state['wal_received_offset']
        if 'paused_at_failover' in local_state:
            del local_state['paused_at_failover']

    config_changed()
    local_state.publish()


@hooks.hook()
def replication_relation_broken():
    # This unit has been removed from the service.
    promote_database()
    config_changed()


            # Change directory the postgres user can read, and need
            # .pgpass too.
            with switch_cwd('/tmp'), pgpass():
                # Clone the master with pg_basebackup.
                output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            log(output, DEBUG)
            # SSL certificates need to exist in the datadir.
            create_ssl_cert(postgresql_cluster_dir)
            create_recovery_conf(master_host, master_port)
        except subprocess.CalledProcessError as x:
            # We failed, and this cluster is broken. Rebuild a
            # working cluster so start/stop etc. works and we
            # can retry hooks again. Even assuming the charm is
            # functioning correctly, the clone may still fail
            # due to eg. lack of disk space.
            log(x.output, ERROR)
            log("Clone failed, local db destroyed", ERROR)
            if os.path.exists(postgresql_cluster_dir):
                shutil.rmtree(postgresql_cluster_dir)
            if os.path.exists(postgresql_config_dir):
                shutil.rmtree(postgresql_config_dir)
            createcluster()
            config_changed()
            raise
        finally:
            postgresql_start()
            wait_for_db()


def slave_count():
    num_slaves = 0
    for relid in hookenv.relation_ids('replication'):
        num_slaves += len(hookenv.related_units(relid))
    for relid in hookenv.relation_ids('master'):
        num_slaves += len(hookenv.related_units(relid))
    return num_slaves


def postgresql_is_in_backup_mode():
    version = pg_version()
    cluster_name = hookenv.config('cluster_name')
    postgresql_cluster_dir = os.path.join(
        postgresql_data_dir, version, cluster_name)

    return os.path.exists(
        os.path.join(postgresql_cluster_dir, 'backup_label'))


def pg_basebackup_is_running():
    cur = db_cursor(autocommit=True)
    cur.execute("""
        SELECT count(*) FROM pg_stat_activity
        WHERE usename='juju_replication' AND application_name='pg_basebackup'
        """)
    return cur.fetchone()[0] > 0


def postgresql_wal_received_offset():
    """How much WAL we have.

    WAL is replicated asynchronously from the master to hot standbys.
    The more WAL a hot standby has received, the better a candidate it
    makes for master during failover.

    Note that this is not quite the same as how in sync the hot standby is.
    That depends on how much WAL has been replayed. WAL is replayed after
    it is received.
    """
    cur = db_cursor(autocommit=True)
    cur.execute('SELECT pg_is_in_recovery(), pg_last_xlog_receive_location()')
    is_in_recovery, xlog_received = cur.fetchone()
    if is_in_recovery:
        return wal_location_to_bytes(xlog_received)
    return None


def wal_location_to_bytes(wal_location):
    """Convert WAL + offset to num bytes, so they can be compared."""
    logid, offset = wal_location.split('/')
    return int(logid, 16) * 16 * 1024 * 1024 * 255 + int(offset, 16)


def wait_for_db(
        timeout=120, db='postgres', user='postgres', host=None, port=None):
    '''Wait until the db is fully up.'''
    db_cursor(db=db, user=user, host=host, port=port, timeout=timeout)


def delete_metrics_cronjob(cron_path):
    try:
        os.unlink(cron_path)
    except OSError:
        pass


def write_metrics_cronjob(script_path, cron_path):
    config_data = hookenv.config()

    # need the following two configs to be valid
    metrics_target = config_data['metrics_target'].strip()
    metrics_sample_interval = config_data['metrics_sample_interval']
    if (not metrics_target
            or ':' not in metrics_target
            or not metrics_sample_interval):
        log("Required config not found or invalid "
            "(metrics_target, metrics_sample_interval), "
            "disabling statsd metrics", DEBUG)
        delete_metrics_cronjob(cron_path)
        return

    charm_dir = os.environ['CHARM_DIR']
    statsd_host, statsd_port = metrics_target.split(':', 1)
    metrics_prefix = config_data['metrics_prefix'].strip()
    metrics_prefix = metrics_prefix.replace(
        "$UNIT", hookenv.local_unit().replace('.', '-').replace('/', '-'))

    # ensure script installed
    charm_script = os.path.join(charm_dir, 'files', 'metrics',
                                'postgres_to_statsd.py')
    host.write_file(script_path, open(charm_script, 'rb').read(), perms=0755)

    # write the crontab
    with open(cron_path, 'w') as cronjob:
        cronjob.write(render_template("metrics_cronjob.template", {
            'interval': config_data['metrics_sample_interval'],
            'script': script_path,
            'metrics_prefix': metrics_prefix,
            'metrics_sample_interval': metrics_sample_interval,
            'statsd_host': statsd_host,
            'statsd_port': statsd_port,
        }))


@hooks.hook('nrpe-external-master-relation-changed')
def update_nrpe_checks():
    config_data = hookenv.config()
    try:
        nagios_uid = getpwnam('nagios').pw_uid
        nagios_gid = getgrnam('nagios').gr_gid
    except Exception:
        hookenv.log("Nagios user not set up.", hookenv.DEBUG)
        return

    try:
        nagios_password = create_user('nagios')
        pg_pass_entry = '*:*:*:nagios:%s' % (nagios_password)
        with open('/var/lib/nagios/.pgpass', 'w') as target:
            os.fchown(target.fileno(), nagios_uid, nagios_gid)
            os.fchmod(target.fileno(), 0400)
            target.write(pg_pass_entry)
    except psycopg2.InternalError:
        if config_data['manual_replication']:
            log("update_nrpe_checks(): manual_replication: "
                "ignoring psycopg2.InternalError caught creating 'nagios' "
                "postgres role; assuming we're already replicating")
        else:
            raise

    relids = hookenv.relation_ids('nrpe-external-master')
    relations = []
    for relid in relids:
        for unit in hookenv.related_units(relid):
            relations.append(hookenv.relation_get(unit=unit, rid=relid))

    if len(relations) == 1 and 'nagios_hostname' in relations[0]:
        nagios_hostname = relations[0]['nagios_hostname']
        log("update_nrpe_checks: Obtained nagios_hostname ({}) "
            "from nrpe-external-master relation.".format(nagios_hostname))
    else:
        unit = hookenv.local_unit()
        unit_name = unit.replace('/', '-')
        nagios_hostname = "%s-%s" % (config_data['nagios_context'], unit_name)
        log("update_nrpe_checks: Deduced nagios_hostname ({}) from charm "
            "config (nagios_hostname not found in nrpe-external-master "
            "relation, or wrong number of relations "
            "found)".format(nagios_hostname))

    nrpe_service_file = \
        '/var/lib/nagios/export/service__{}_check_pgsql.cfg'.format(
            nagios_hostname)
    nagios_logdir = '/var/log/nagios'
    if not os.path.exists(nagios_logdir):
        os.mkdir(nagios_logdir)
        os.chown(nagios_logdir, nagios_uid, nagios_gid)
    for f in os.listdir('/var/lib/nagios/export/'):
        if re.search('.*check_pgsql.cfg', f):
            os.remove(os.path.join('/var/lib/nagios/export/', f))

    # --- exported service configuration file
    servicegroups = [config_data['nagios_context']]
    additional_servicegroups = config_data['nagios_additional_servicegroups']
    if additional_servicegroups != '':
        servicegroups.extend(
            servicegroup.strip() for servicegroup
            in additional_servicegroups.split(',')
        )
    templ_vars = {
        'nagios_hostname': nagios_hostname,
        'nagios_servicegroup': ', '.join(servicegroups),
    }
    template = render_template('nrpe_service.tmpl', templ_vars)
    with open(nrpe_service_file, 'w') as nrpe_service_config:
        nrpe_service_config.write(str(template))

    # --- nrpe configuration
    # pgsql service
    nrpe_check_file = '/etc/nagios/nrpe.d/check_pgsql.cfg'
    with open(nrpe_check_file, 'w') as nrpe_check_config:
        nrpe_check_config.write("# check pgsql\n")
        nrpe_check_config.write(
            "command[check_pgsql]=/usr/lib/nagios/plugins/check_pgsql -P {}"
            .format(get_service_port()))
    # pgsql backups
    nrpe_check_file = '/etc/nagios/nrpe.d/check_pgsql_backups.cfg'
    # XXX: these values _should_ be calculated from the backup schedule
    #      perhaps warn = backup_frequency * 1.5, crit = backup_frequency * 2
    warn_age = 172800
    crit_age = 194400
    with open(nrpe_check_file, 'w') as nrpe_check_config:
        nrpe_check_config.write("# check pgsql backups\n")
        nrpe_check_config.write(
            "command[check_pgsql_backups]=/usr/lib/nagios/plugins/\
check_file_age -w {} -c {} -f {}".format(warn_age, crit_age, backup_log))

    if os.path.isfile('/etc/init.d/nagios-nrpe-server'):
        host.service_reload('nagios-nrpe-server')


@hooks.hook('data-relation-changed')
def data_relation_changed():
    """Listen for configured mountpoint from storage subordinate relation"""
    if not hookenv.relation_get("mountpoint"):
        hookenv.log("Waiting for mountpoint from the relation: %s"
                    % external_volume_mount, hookenv.DEBUG)
    else:
        hookenv.log("Storage ready and mounted", hookenv.DEBUG)
        config_changed(mount_point=external_volume_mount)


@hooks.hook('data-relation-joined')
def data_relation_joined():
    """Request mountpoint from storage subordinate by setting mountpoint"""
    hookenv.log("Setting mount point in the relation: %s"
                % external_volume_mount, hookenv.DEBUG)
    hookenv.relation_set(mountpoint=external_volume_mount)


@hooks.hook('data-relation-departed')
def stop_postgres_on_data_relation_departed():
    hookenv.log("Data relation departing. Stopping PostgreSQL",
                hookenv.DEBUG)
    postgresql_stop()


@hooks.hook('master-relation-joined', 'master-relation-changed')
def master_relation_joined_changed():
    local_relation = hookenv.relation_get(unit=hookenv.local_unit())

    # Relation settings both master and standbys can set now.
    allowed_units = sorted(hookenv.related_units())  # Bug #1458754
    hookenv.relation_set(
        relation_settings={'allowed-units': ' '.join(allowed_units),
                           'host': hookenv.unit_private_ip(),
                           'port': get_service_port(),
                           'state': local_state['state'],
                           'version': pg_version()})

    if local_state['state'] == 'hot standby':
        # Hot standbys cannot create credentials. Publish them from the
        # master if they are available, or defer until a peer-relation-changed
        # hook when they are.
        publish_hot_standby_credentials()
        config_changed()
        return

    user = local_relation.get('user') or user_name(hookenv.relation_id(),
                                                   hookenv.remote_unit())
    password = local_relation.get('password') or create_user(user,
                                                             admin=True,
                                                             replication=True)
    hookenv.relation_set(user=user, password=password)

    # For logical replication, the standby service may request an explicit
    # database.
    database = hookenv.relation_get('database')
    if database:
        ensure_database(user, user, database)
        hookenv.relation_set(database=database)  # Signal database is ready

    # We may need to bump the number of replication connections and
    # restart, and we will certainly need to regenerate pg_hba.conf
    # and reload.
    config_changed()  # Must be called after db & user are created.


@hooks.hook()
def master_relation_departed():
    config_changed()
    allowed_units = hookenv.relation_get('allowed-units',
                                         hookenv.local_unit()).split()
    if hookenv.remote_unit() in allowed_units:
        allowed_units.remove(hookenv.remote_unit())
    hookenv.relation_set(relation_settings={
        'allowed-units': ' '.join(allowed_units)})


def _get_postgresql_config_dir(config_data=None):
    """ Return the directory path of the postgresql configuration files. """
    if config_data is None:
        config_data = hookenv.config()
    version = pg_version()
    cluster_name = config_data['cluster_name']
    return os.path.join("/etc/postgresql", version, cluster_name)


###############################################################################
# Global variables
###############################################################################
postgresql_data_dir = "/var/lib/postgresql"
postgresql_scripts_dir = os.path.join(postgresql_data_dir, 'scripts')
postgresql_logs_dir = os.path.join(postgresql_data_dir, 'logs')

postgresql_sysctl = "/etc/sysctl.d/50-postgresql.conf"
postgresql_crontab = "/etc/cron.d/postgresql"
postgresql_service_config_dir = "/var/run/postgresql"
local_state = State('local_state.pickle')
hook_name = os.path.basename(sys.argv[0])
juju_log_dir = "/var/log/juju"
external_volume_mount = "/srv/data"

backup_log = os.path.join(postgresql_logs_dir, "backups.log")


if __name__ == '__main__':
    # Hook and context overview. The various replication and client
    # hooks interact in complex ways.
    log("Running {} hook".format(hook_name))
    if hookenv.relation_id():
        log("Relation {} with {}".format(
            hookenv.relation_id(), hookenv.remote_unit()))
    hooks.execute(sys.argv)
    log("Completed {} hook".format(hook_name))
