#!/usr/bin/env python
# vim: et ai ts=4 sw=4:

from contextlib import contextmanager
import commands
import cPickle as pickle
import glob
from grp import getgrnam
import os.path
from pwd import getpwnam
import re
import shutil
import socket
import subprocess
import sys
from tempfile import NamedTemporaryFile
import time

from charmhelpers import fetch
from charmhelpers.core import hookenv, host
from charmhelpers.core.hookenv import (
    CRITICAL, ERROR, WARNING, INFO, DEBUG,
    )

hooks = hookenv.Hooks()


def Template(*args, **kw):
    """jinja2.Template with deferred jinja2 import.

    jinja2 may not be importable until the install hook has installed the
    required packages.
    """
    from jinja2 import Template
    return Template(*args, **kw)


def log(msg, lvl=INFO):
    '''Log a message.

    Per Bug #1208787, log messages sent via juju-log are being lost.
    Spit messages out to a log file to work around the problem.
    It is also rather nice to have the log messages we explicitly emit
    in a separate log file, rather than just mashed up with all the
    juju noise.
    '''
    myname = hookenv.local_unit().replace('/', '-')
    ts = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
    with open('{}/{}-debug.log'.format(juju_log_dir, myname), 'a') as f:
        f.write('{} {}: {}\n'.format(ts, lvl, msg))
    hookenv.log(msg, lvl)


def pg_version():
    '''Return pg_version to use.

    Return "version" config item if set, else use version from "postgresql"
    package candidate, saving it in local_state for later.
    '''
    config_data = hookenv.config()
    if 'pg_version' in local_state:
        version = local_state['pg_version']
    elif 'version' in config_data:
        version = config_data['version']
    else:
        log("map version from distro release ...")
        version_map = {'precise': '9.1',
                       'trusty': '9.3'}
        version = version_map.get(distro_codename())
        if not version:
            log("No PG version map for distro_codename={}, "
                "you'll need to explicitly set it".format(distro_codename()),
                CRITICAL)
            sys.exit(1)
        log("version={} from distro_codename='{}'".format(
            version, distro_codename()))
        # save it for later
        local_state.setdefault('pg_version', version)
        local_state.save()

    assert version, "pg_version couldn't find a version to use"
    return version


def distro_codename():
    """Return the distro release code name, eg. 'precise' or 'trusty'."""
    return host.lsb_release()['DISTRIB_CODENAME']


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
        pickle.dump(state, open(self._state_file, 'wb'))

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


def run(command, exit_on_error=True, quiet=False):
    '''Run a command and return the output.'''
    if not quiet:
        log("Running {!r}".format(command), DEBUG)
    p = subprocess.Popen(
        command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        shell=isinstance(command, basestring))
    p.stdin.close()
    lines = []
    for line in p.stdout:
        if line:
            # LP:1274460 & LP:1259490 mean juju-log is no where near as
            # useful as we would like, so just shove a copy of the
            # output to stdout for logging.
            # log("> {}".format(line), DEBUG)
            if not quiet:
                print line
            lines.append(line)
        elif p.poll() is not None:
            break

    p.wait()

    if p.returncode == 0:
        return '\n'.join(lines)

    if p.returncode != 0 and exit_on_error:
        log("ERROR: {}".format(p.returncode), ERROR)
        sys.exit(p.returncode)

    raise subprocess.CalledProcessError(
        p.returncode, command, '\n'.join(lines))


def postgresql_is_running():
    '''Return true if PostgreSQL is running.'''
    for version, name, _, status in lsclusters(slice(4)):
        if (version, name) == (pg_version(), hookenv.config('cluster_name')):
            if 'online' in status.split(','):
                log('PostgreSQL is running', DEBUG)
                return True
            else:
                log('PostgreSQL is not running', DEBUG)
                return False
    assert False, 'Cluster {} {} not found'.format(
        pg_version(), hookenv.config('cluster_name'))


def postgresql_stop():
    '''Shutdown PostgreSQL.'''
    if postgresql_is_running():
        run([
            'pg_ctlcluster', '--force',
            pg_version(), hookenv.config('cluster_name'), 'stop'])
        log('PostgreSQL shut down')


def postgresql_start():
    '''Start PostgreSQL if it is not already running.'''
    if not postgresql_is_running():
        run([
            'pg_ctlcluster', pg_version(),
            hookenv.config('cluster_name'), 'start'])
        log('PostgreSQL started')


def postgresql_restart():
    '''Restart PostgreSQL, or start it if it is not already running.'''
    if postgresql_is_running():
        with restart_lock(hookenv.local_unit(), True):
            run([
                'pg_ctlcluster', '--force',
                pg_version(), hookenv.config('cluster_name'), 'restart'])
            log('PostgreSQL restarted')
    else:
        postgresql_start()

    assert postgresql_is_running()

    # Store a copy of our known live configuration so
    # postgresql_reload_or_restart() can make good choices.
    if 'saved_config' in local_state:
        local_state['live_config'] = local_state['saved_config']
        local_state.save()


def postgresql_reload():
    '''Make PostgreSQL reload its configuration.'''
    # reload returns a reliable exit status
    if postgresql_is_running():
        # I'm using the PostgreSQL function to avoid as much indirection
        # as possible.
        success = run_select_as_postgres('SELECT pg_reload_conf()')[1][0][0]
        assert success, 'Failed to reload PostgreSQL configuration'
        log('PostgreSQL configuration reloaded')
    return postgresql_start()


def requires_restart():
    '''Check for configuration changes requiring a restart to take effect.'''
    if not postgresql_is_running():
        return True

    saved_config = local_state.get('saved_config', None)
    if not saved_config:
        # No record of postgresql.conf state, perhaps an upgrade.
        # Better restart.
        return True

    live_config = local_state.setdefault('live_config', {})

    # Pull in a list of PostgreSQL settings.
    cur = db_cursor()
    cur.execute("SELECT name, context FROM pg_settings")
    restart = False
    for name, context in cur.fetchall():
        live_value = live_config.get(name, None)
        new_value = saved_config.get(name, None)

        if new_value != live_value:
            if live_config:
                log("Changed {} from {!r} to {!r}".format(
                    name, live_value, new_value), DEBUG)
            if context == 'postmaster':
                # A setting has changed that requires PostgreSQL to be
                # restarted before it will take effect.
                restart = True
                log('{} changed from {} to {}. Restart required.'.format(
                    name, live_value, new_value), DEBUG)
    return restart


def postgresql_reload_or_restart():
    """Reload PostgreSQL configuration, restarting if necessary."""
    if requires_restart():
        log("Configuration change requires PostgreSQL restart", WARNING)
        postgresql_restart()
        assert not requires_restart(), "Configuration changes failed to apply"
    else:
        postgresql_reload()

    local_state['saved_config'] = local_state['live_config']
    local_state.save()


def get_service_port():
    '''Return the port PostgreSQL is listening on.'''
    for version, name, port in lsclusters(slice(3)):
        if (version, name) == (pg_version(), hookenv.config('cluster_name')):
            return int(port)

    assert False, 'No port found for {!r} {!r}'.format(
        pg_version(), hookenv.config['cluster_name'])


def lsclusters(s=slice(0, -1)):
    for line in run('pg_lsclusters', quiet=True).splitlines()[1:]:
        if line:
            yield line.split()[s]


def _get_system_ram():
    """ Return the system ram in Megabytes """
    import psutil
    return psutil.phymem_usage()[0] / (1024 ** 2)


def _get_page_size():
    """ Return the operating system's configured PAGE_SIZE """
    return int(run("getconf PAGE_SIZE"))   # frequently 4096


def create_postgresql_config(config_file):
    '''Create the postgresql.conf file'''
    config_data = hookenv.config()
    if not config_data.get('listen_port', None):
        config_data['listen_port'] = get_service_port()
    if config_data["performance_tuning"] == "auto":
        # Taken from:
        # http://wiki.postgresql.org/wiki/Tuning_Your_PostgreSQL_Server
        # num_cpus is not being used ... commenting it out ... negronjl
        #num_cpus = run("cat /proc/cpuinfo | grep processor | wc -l")
        total_ram = _get_system_ram()
        if not config_data["effective_cache_size"]:
            config_data["effective_cache_size"] = \
                "%sMB" % (int(int(total_ram) * 0.75),)
        if not config_data["shared_buffers"]:
            if total_ram > 1023:
                config_data["shared_buffers"] = \
                    "%sMB" % (int(int(total_ram) * 0.25),)
            else:
                config_data["shared_buffers"] = \
                    "%sMB" % (int(int(total_ram) * 0.15),)
        config_data["kernel_shmmax"] = (int(total_ram) * 1024 * 1024) + 1024
        config_data["kernel_shmall"] = config_data["kernel_shmmax"]

    # XXX: This is very messy - should probably be a subordinate charm
    lines = ["kernel.sem = 250 32000 100 1024\n"]
    if config_data["kernel_shmall"] > 0:
        # Convert config kernel_shmall (bytes) to pages
        page_size = _get_page_size()
        num_pages = config_data["kernel_shmall"] / page_size
        if (config_data["kernel_shmall"] % page_size) > 0:
            num_pages += 1
        lines.append("kernel.shmall = %s\n" % num_pages)
    if config_data["kernel_shmmax"] > 0:
        lines.append("kernel.shmmax = %s\n" % config_data["kernel_shmmax"])
    host.write_file(postgresql_sysctl, ''.join(lines), perms=0600)
    run("sysctl -p {}".format(postgresql_sysctl))

    # If we are replicating, some settings may need to be overridden to
    # certain minimum levels.
    num_slaves = slave_count()
    if num_slaves > 0:
        log('{} hot standbys in peer relation.'.format(num_slaves))
        log('Ensuring minimal replication settings')
        config_data['hot_standby'] = True
        config_data['wal_level'] = 'hot_standby'
        config_data['max_wal_senders'] = max(
            num_slaves, config_data['max_wal_senders'])
        config_data['wal_keep_segments'] = max(
            config_data['wal_keep_segments'],
            config_data['replicated_wal_keep_segments'])

    # Send config data to the template
    # Return it as pg_config
    charm_dir = hookenv.charm_dir()
    template_file = "{}/templates/postgresql.conf.tmpl".format(charm_dir)
    if not config_data.get('version', None):
        config_data['version'] = pg_version()
    pg_config = Template(
        open(template_file).read()).render(config_data)
    host.write_file(
        config_file, pg_config,
        owner="postgres",  group="postgres", perms=0600)

    local_state['saved_config'] = config_data
    local_state.save()


def create_postgresql_ident(output_file):
    '''Create the pg_ident.conf file.'''
    ident_data = {}
    charm_dir = hookenv.charm_dir()
    template_file = "{}/templates/pg_ident.conf.tmpl".format(charm_dir)
    pg_ident_template = Template(open(template_file).read())
    host.write_file(
        output_file, pg_ident_template.render(ident_data),
        owner="postgres", group="postgres", perms=0600)


def generate_postgresql_hba(
        output_file, user=None, schema_user=None, database=None):
    '''Create the pg_hba.conf file.'''

    # Per Bug #1117542, when generating the postgresql_hba file we
    # need to cope with private-address being either an IP address
    # or a hostname.
    def munge_address(addr):
        # http://stackoverflow.com/q/319279/196832
        try:
            socket.inet_aton(addr)
            return "%s/32" % addr
        except socket.error:
            # It's not an IP address.
            # XXX workaround for MAAS bug
            # https://bugs.launchpad.net/maas/+bug/1250435
            # If it's a CNAME, use the A record it points to.
            # If it fails for some reason, return the original address
            try:
                output = run("dig +short -t CNAME %s" % addr, True).strip()
            except:
                return addr
            if len(output) != 0:
                return output.rstrip(".")  # trailing dot
            return addr

    config_data = hookenv.config()
    allowed_units = set()
    relation_data = []
    relids = hookenv.relation_ids('db') + hookenv.relation_ids('db-admin')
    for relid in relids:
        local_relation = hookenv.relation_get(
            unit=hookenv.local_unit(), rid=relid)

        # We might see relations that have not yet been setup enough.
        # At a minimum, the relation-joined hook needs to have been run
        # on the server so we have information about the usernames and
        # databases to allow in.
        if 'user' not in local_relation:
            continue

        for unit in hookenv.related_units(relid):
            relation = hookenv.relation_get(unit=unit, rid=relid)

            relation['relation-id'] = relid
            relation['unit'] = unit

            if relid.startswith('db-admin:'):
                relation['user'] = 'all'
                relation['database'] = 'all'
            elif relid.startswith('db:'):
                relation['user'] = local_relation.get('user', user)
                relation['schema_user'] = local_relation.get('schema_user',
                                                             schema_user)
                relation['database'] = local_relation.get('database', database)

                if ((relation['user'] is None
                     or relation['schema_user'] is None
                     or relation['database'] is None)):
                    # Missing info in relation for this unit, so skip it.
                    continue
            else:
                raise RuntimeError(
                    'Unknown relation type {}'.format(repr(relid)))

            allowed_units.add(unit)
            relation['private-address'] = munge_address(
                relation['private-address'])
            relation_data.append(relation)

    log(str(relation_data), INFO)

    # Replication connections. Each unit needs to be able to connect to
    # every other unit's postgres database and the magic replication
    # database. It also needs to be able to connect to its own postgres
    # database.
    for relid in hookenv.relation_ids('replication'):
        for unit in hookenv.related_units(relid):
            relation = hookenv.relation_get(unit=unit, rid=relid)
            remote_addr = munge_address(relation['private-address'])
            remote_replication = {'database': 'replication',
                                  'user': 'juju_replication',
                                  'private-address': remote_addr,
                                  'relation-id': relid,
                                  'unit': unit,
                                  }
            relation_data.append(remote_replication)
            remote_pgdb = {'database': 'postgres',
                           'user': 'juju_replication',
                           'private-address': remote_addr,
                           'relation-id': relid,
                           'unit': unit,
                           }
            relation_data.append(remote_pgdb)

    # Hooks need permissions too to setup replication.
    for relid in hookenv.relation_ids('replication'):
        local_replication = {'database': 'postgres',
                             'user': 'juju_replication',
                             'private-address': munge_address(
                                 hookenv.unit_private_ip()),
                             'relation-id': relid,
                             'unit': hookenv.local_unit(),
                             }
        relation_data.append(local_replication)

    # Admin IP addresses for people using tools like pgAdminIII in a local JuJu
    # We accept a single IP or a comma separated list of IPs, these are added
    # to the list of relations that end up in pg_hba.conf thus granting
    # the IP addresses socket access to the postgres server.
    if config_data["admin_addresses"] != '':
        if "," in config_data["admin_addresses"]:
            admin_ip_list = config_data["admin_addresses"].split(",")
        else:
            admin_ip_list = [config_data["admin_addresses"]]

        for admin_ip in admin_ip_list:
            admin_host = {
                'database': 'all',
                'user': 'all',
                'private-address': munge_address(admin_ip)}
            relation_data.append(admin_host)

    template_file = "{}/templates/pg_hba.conf.tmpl".format(hookenv.charm_dir())
    pg_hba_template = Template(open(template_file).read())
    host.write_file(
        output_file, pg_hba_template.render(access_list=relation_data),
        owner="postgres", group="postgres", perms=0600)
    postgresql_reload()

    # Loop through all db relations, making sure each knows what are the list
    # of allowed hosts that were just added. lp:#1187508
    # We sort the list to ensure stability, probably unnecessarily.
    for relid in hookenv.relation_ids('db') + hookenv.relation_ids('db-admin'):
        hookenv.relation_set(
            relid, {"allowed-units": " ".join(unit_sorted(allowed_units))})


def install_postgresql_crontab(output_file):
    '''Create the postgres user's crontab'''
    config_data = hookenv.config()
    crontab_data = {
        'backup_schedule': config_data["backup_schedule"],
        'scripts_dir': postgresql_scripts_dir,
        'backup_days': config_data["backup_retention_count"],
    }
    charm_dir = hookenv.charm_dir()
    template_file = "{}/templates/postgres.cron.tmpl".format(charm_dir)
    crontab_template = Template(
        open(template_file).read()).render(crontab_data)
    host.write_file(output_file, crontab_template, perms=0600)


def create_recovery_conf(master_host, master_port, restart_on_change=False):
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
    recovery_conf = Template(open(template_file).read()).render({
        'host': master_host,
        'port': master_port,
        'password': local_state['replication_password'],
        'streaming_replication': streaming_replication})
    log(recovery_conf, DEBUG)
    host.write_file(
        os.path.join(postgresql_cluster_dir, 'recovery.conf'),
        recovery_conf, owner="postgres", group="postgres", perms=0o600)

    if restart_on_change and old_recovery_conf != recovery_conf:
        log("recovery.conf updated. Restarting to take effect.")
        postgresql_restart()


#------------------------------------------------------------------------------
# load_postgresql_config:  Convenience function that loads (as a string) the
#                          current postgresql configuration file.
#                          Returns a string containing the postgresql config or
#                          None
#------------------------------------------------------------------------------
def load_postgresql_config(config_file):
    if os.path.isfile(config_file):
        return(open(config_file).read())
    else:
        return(None)


def update_service_port():
    old_port = local_state.get('listen_port', None)
    new_port = get_service_port()
    if old_port != new_port:
        if new_port:
            hookenv.open_port(new_port)
        if old_port:
            hookenv.close_port(old_port)
        local_state['listen_port'] = new_port
        local_state.save()


def create_ssl_cert(cluster_dir):
    # Debian by default expects SSL certificates in the datadir.
    server_crt = os.path.join(cluster_dir, 'server.crt')
    server_key = os.path.join(cluster_dir, 'server.key')
    if not os.path.exists(server_crt):
        os.symlink('/etc/ssl/certs/ssl-cert-snakeoil.pem',
                   server_crt)
    if not os.path.exists(server_key):
        os.symlink('/etc/ssl/private/ssl-cert-snakeoil.key',
                   server_key)


def set_password(user, password):
    if not os.path.isdir("passwords"):
        os.makedirs("passwords")
    old_umask = os.umask(0o077)
    try:
        with open("passwords/%s" % user, "w") as pwfile:
            pwfile.write(password)
    finally:
        os.umask(old_umask)


def get_password(user):
    try:
        with open("passwords/%s" % user) as pwfile:
            return pwfile.read()
    except IOError:
        return None


def db_cursor(autocommit=False, db='postgres', user='postgres',
              host=None, port=None, timeout=30):
    import psycopg2
    if port is None:
        port = get_service_port()
    if host:
        conn_str = "dbname={} host={} port={} user={}".format(
            db, host, port, user)
    else:
        conn_str = "dbname={} port={} user={}".format(db, port, user)
    # There are often race conditions in opening database connections,
    # such as a reload having just happened to change pg_hba.conf
    # settings or a hot standby being restarted and needing to catch up
    # with its master. To protect our automation against these sorts of
    # race conditions, by default we always retry failed connections
    # until a timeout is reached.
    start = time.time()
    while True:
        try:
            with pgpass():
                conn = psycopg2.connect(conn_str)
            break
        except psycopg2.Error, x:
            if time.time() > start + timeout:
                log("Database connection {!r} failed".format(
                    conn_str), CRITICAL)
                raise
            log("Unable to open connection ({}), retrying.".format(x))
            time.sleep(1)
    conn.autocommit = autocommit
    return conn.cursor()


def run_sql_as_postgres(sql, *parameters):
    import psycopg2
    cur = db_cursor(autocommit=True)
    try:
        cur.execute(sql, parameters)
        return cur.statusmessage
    except psycopg2.ProgrammingError:
        log(sql, CRITICAL)
        raise


def run_select_as_postgres(sql, *parameters):
    cur = db_cursor()
    cur.execute(sql, parameters)
    # NB. Need to suck in the results before the rowcount is valid.
    results = cur.fetchall()
    return (cur.rowcount, results)


def validate_config():
    """
    Sanity check charm configuration, aborting the script if
    we have bogus config values or config changes the charm does not yet
    (or cannot) support.
    """
    valid = True
    config_data = hookenv.config()

    version = config_data.get('version', None)
    if version:
        if version not in ('9.1', '9.2', '9.3'):
            valid = False
            log("Invalid or unsupported version {!r} requested".format(
                version), CRITICAL)

    if config_data['cluster_name'] != 'main':
        valid = False
        log("Cluster names other than 'main' do not work per LP:1271835",
            CRITICAL)

    if config_data['listen_ip'] != '*':
        valid = False
        log("listen_ip values other than '*' do not work per LP:1271837",
            CRITICAL)

    unchangeable_config = [
        'locale', 'encoding', 'version', 'cluster_name', 'pgdg']

    for name in unchangeable_config:
        if (name in local_state
                and local_state[name] != config_data.get(name, None)):
            valid = False
            log("Cannot change {!r} setting after install.".format(name))
        local_state[name] = config_data.get(name, None)
    local_state.save()

    if not valid:
        sys.exit(99)


#------------------------------------------------------------------------------
# Core logic for permanent storage changes:
# NOTE the only 2 "True" return points:
#   1) symlink already pointing to existing storage (no-op)
#   2) new storage properly initialized:
#     - if fresh new storage dir: rsync existing data
#     - manipulate /var/lib/postgresql/VERSION/CLUSTER symlink
#------------------------------------------------------------------------------
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


def token_sql_safe(value):
    # Only allow alphanumeric + underscore in database identifiers
    if re.search('[^A-Za-z0-9_]', value):
        return False
    return True


@hooks.hook()
def config_changed(force_restart=False, mount_point=None):
    validate_config()
    config_data = hookenv.config()
    update_repos_and_packages()

    if mount_point is not None:
        ## config_changed_volume_apply will stop the service if it finds
        ## it necessary, ie: new volume setup
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

    postgresql_config_dir = _get_postgresql_config_dir(config_data)
    postgresql_config = os.path.join(postgresql_config_dir, "postgresql.conf")
    postgresql_hba = os.path.join(postgresql_config_dir, "pg_hba.conf")
    postgresql_ident = os.path.join(postgresql_config_dir, "pg_ident.conf")

    create_postgresql_config(postgresql_config)
    create_postgresql_ident(postgresql_ident)  # Do this before pg_hba.conf.
    generate_postgresql_hba(postgresql_hba)
    create_ssl_cert(os.path.join(
        postgresql_data_dir, pg_version(), config_data['cluster_name']))
    update_service_port()
    update_nrpe_checks()
    if force_restart:
        postgresql_restart()
    postgresql_reload_or_restart()


@hooks.hook()
def install(run_pre=True):
    if run_pre:
        for f in glob.glob('exec.d/*/charm-pre-install'):
            if os.path.isfile(f) and os.access(f, os.X_OK):
                subprocess.check_call(['sh', '-c', f])

    validate_config()

    config_data = hookenv.config()
    update_repos_and_packages()
    if not 'state' in local_state:
        # Fresh installation. Because this function is invoked by both
        # the install hook and the upgrade-charm hook, we need to guard
        # any non-idempotent setup. We should probably fix this; it
        # seems rather fragile.
        local_state.setdefault('state', 'standalone')

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
        with switch_cwd('/tmp'):
            create_cmd = [
                "pg_createcluster",
                "--locale", config_data['locale'],
                "-e", config_data['encoding']]
            if listen_port:
                create_cmd.extend(["-p", str(config_data['listen_port'])])
            create_cmd.append(pg_version())
            create_cmd.append(config_data['cluster_name'])
            run(create_cmd)
        assert (
            not port_opt
            or get_service_port() == config_data['listen_port']), (
            'allocated port {!r} != {!r}'.format(
                get_service_port(), config_data['listen_port']))
        local_state['port'] = get_service_port()
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
    template_file = "{}/templates/dump-pg-db.tmpl".format(charm_dir)
    dump_script = Template(open(template_file).read()).render(paths)
    template_file = "{}/templates/pg_backup_job.tmpl".format(charm_dir)
    backup_job = Template(open(template_file).read()).render(paths)
    host.write_file(
        '{}/dump-pg-db'.format(postgresql_scripts_dir),
        dump_script, perms=0755)
    host.write_file(
        '{}/pg_backup_job'.format(postgresql_scripts_dir),
        backup_job, perms=0755)
    install_postgresql_crontab(postgresql_crontab)
    hookenv.open_port(get_service_port())

    # Ensure at least minimal access granted for hooks to run.
    # Reload because we are using the default cluster setup and started
    # when we installed the PostgreSQL packages.
    config_changed(force_restart=True)

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
    install(run_pre=False)
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


@hooks.hook()
def start():
    postgresql_reload_or_restart()


@hooks.hook()
def stop():
    if postgresql_is_running():
        with restart_lock(hookenv.local_unit(), True):
            postgresql_stop()


def quote_identifier(identifier):
    r'''Quote an identifier, such as a table or role name.

    In SQL, identifiers are quoted using " rather than ' (which is reserved
    for strings).

    >>> print(quote_identifier('hello'))
    "hello"

    Quotes and Unicode are handled if you make use of them in your
    identifiers.

    >>> print(quote_identifier("'"))
    "'"
    >>> print(quote_identifier('"'))
    """"
    >>> print(quote_identifier("\\"))
    "\"
    >>> print(quote_identifier('\\"'))
    "\"""
    >>> print(quote_identifier('\\ aargh \u0441\u043b\u043e\u043d'))
    U&"\\ aargh \0441\043b\043e\043d"
    '''
    try:
        return '"%s"' % identifier.encode('US-ASCII').replace('"', '""')
    except UnicodeEncodeError:
        escaped = []
        for c in identifier:
            if c == '\\':
                escaped.append('\\\\')
            elif c == '"':
                escaped.append('""')
            else:
                c = c.encode('US-ASCII', 'backslashreplace')
                # Note Python only supports 32 bit unicode, so we use
                # the 4 hexdigit PostgreSQL syntax (\1234) rather than
                # the 6 hexdigit format (\+123456).
                if c.startswith('\\u'):
                    c = '\\' + c[2:]
                escaped.append(c)
        return 'U&"%s"' % ''.join(escaped)


def sanitize(s):
    s = s.replace(':', '_')
    s = s.replace('-', '_')
    s = s.replace('/', '_')
    s = s.replace('"', '_')
    s = s.replace("'", '_')
    return s


def user_name(relid, remote_unit, admin=False, schema=False):
    # Per Bug #1160530, don't append the remote unit number to the user name.
    components = [sanitize(relid), sanitize(re.split("/", remote_unit)[0])]
    if admin:
        components.append("admin")
    elif schema:
        components.append("schema")
    return "_".join(components)


def user_exists(user):
    sql = "SELECT rolname FROM pg_roles WHERE rolname = %s"
    if run_select_as_postgres(sql, user)[0] > 0:
        return True
    else:
        return False


def create_user(user, admin=False, replication=False):
    from psycopg2.extensions import AsIs

    password = get_password(user)
    if password is None:
        password = host.pwgen()
        set_password(user, password)
    if user_exists(user):
        log("Updating {} user".format(user))
        action = ["ALTER ROLE"]
    else:
        log("Creating {} user".format(user))
        action = ["CREATE ROLE"]
    action.append('%s WITH LOGIN')
    if admin:
        action.append('SUPERUSER')
    else:
        action.append('NOSUPERUSER')
    if replication:
        action.append('REPLICATION')
    else:
        action.append('NOREPLICATION')
    action.append('PASSWORD %s')
    sql = ' '.join(action)
    run_sql_as_postgres(sql, AsIs(quote_identifier(user)), password)
    return password


def reset_user_roles(user, roles):
    from psycopg2.extensions import AsIs

    wanted_roles = set(roles)

    sql = """
        SELECT role.rolname
        FROM
            pg_roles AS role,
            pg_roles AS member,
            pg_auth_members
        WHERE
            member.oid = pg_auth_members.member
            AND role.oid = pg_auth_members.roleid
            AND member.rolname = %s
        """
    existing_roles = set(r[0] for r in run_select_as_postgres(sql, user)[1])

    roles_to_grant = wanted_roles.difference(existing_roles)

    for role in roles_to_grant:
        ensure_role(role)

    if roles_to_grant:
        log("Granting {} to {}".format(",".join(roles_to_grant), user), INFO)

    for role in roles_to_grant:
        run_sql_as_postgres(
            "GRANT %s TO %s",
            AsIs(quote_identifier(role)), AsIs(quote_identifier(user)))

    roles_to_revoke = existing_roles.difference(wanted_roles)

    if roles_to_revoke:
        log("Revoking {} from {}".format(",".join(roles_to_grant), user), INFO)

    for role in roles_to_revoke:
        run_sql_as_postgres(
            "REVOKE %s FROM %s",
            AsIs(quote_identifier(role)), AsIs(quote_identifier(user)))


def ensure_role(role):
    from psycopg2.extensions import AsIs

    sql = "SELECT oid FROM pg_roles WHERE rolname = %s"
    if run_select_as_postgres(sql, role)[0] == 0:
        sql = "CREATE ROLE %s INHERIT NOLOGIN"
        run_sql_as_postgres(sql, AsIs(quote_identifier(role)))


def ensure_database(user, schema_user, database):
    from psycopg2.extensions import AsIs

    sql = "SELECT datname FROM pg_database WHERE datname = %s"
    if run_select_as_postgres(sql, database)[0] != 0:
        # DB already exists
        pass
    else:
        sql = "CREATE DATABASE %s"
        run_sql_as_postgres(sql, AsIs(quote_identifier(database)))
    sql = "GRANT ALL PRIVILEGES ON DATABASE %s TO %s"
    run_sql_as_postgres(sql, AsIs(quote_identifier(database)),
                        AsIs(quote_identifier(schema_user)))
    sql = "GRANT CONNECT ON DATABASE %s TO %s"
    run_sql_as_postgres(sql, AsIs(quote_identifier(database)),
                        AsIs(quote_identifier(user)))


def snapshot_relations():
    '''Snapshot our relation information into local state.

    We need this information to be available in -broken
    hooks letting us actually clean up properly. Bug #1190996.
    '''
    log("Snapshotting relations", DEBUG)
    local_state['relations'] = hookenv.relations()
    local_state.save()


# Each database unit needs to publish connection details to the
# client. This is problematic, because 1) the user and database are
# only created on the master unit and this is replicated to the
# slave units outside of juju and 2) we have no control over the
# order that units join the relation.
#
# The simplest approach of generating usernames and passwords in
# the master units db-relation-joined hook fails because slave
# units may well have already run their hooks and found no
# connection details to republish. When the master unit publishes
# the connection details it only triggers relation-changed hooks
# on the client units, not the relation-changed hook on other peer
# units.
#
# A more complex approach is for the first database unit that joins
# the relation to generate the usernames and passwords and publish
# this to the relation. Subsequent units can retrieve this
# information and republish it. Of course, the master unit also
# creates the database and users when it joins the relation.
# This approach should work reliably on the server side. However,
# there is a window from when a slave unit joins a client relation
# until the master unit has joined that relation when the
# credentials published by the slave unit are invalid. These
# credentials will only become valid after the master unit has
# actually created the user and database.
#
# The implemented approach is for the master unit's
# db-relation-joined hook to create the user and database and
# publish the connection details, and in addition update a list
# of active relations to the service's peer 'replication' relation.
# After the master unit has updated the peer relationship, the
# slave unit's peer replication-relation-changed hook will
# be triggered and it will have an opportunity to republish the
# connection details. Of course, it may not be able to do so if the
# slave unit's db-relation-joined hook has yet been run, so we must
# also attempt to to republish the connection settings there.
# This way we are guaranteed at least one chance to republish the
# connection details after the database and user have actually been
# created and both the master and slave units have joined the
# relation.
#
# The order of relevant hooks firing may be:
#
# master db-relation-joined (publish)
# slave db-relation-joined (republish)
# slave replication-relation-changed (noop)
#
# slave db-relation-joined (noop)
# master db-relation-joined (publish)
# slave replication-relation-changed (republish)
#
# master db-relation-joined (publish)
# slave replication-relation-changed (noop; slave not yet joined db rel)
# slave db-relation-joined (republish)


@hooks.hook('db-relation-joined', 'db-relation-changed')
def db_relation_joined_changed():
    if local_state['state'] == 'hot standby':
        publish_hot_standby_credentials()
        return

    # By default, we create a database named after the remote
    # servicename. The remote service can override this by setting
    # the database property on the relation.
    database = hookenv.relation_get('database')
    if not database:
        database = hookenv.remote_unit().split('/')[0]

    # Generate a unique username for this relation to use.
    user = user_name(hookenv.relation_id(), hookenv.remote_unit())

    roles = filter(None, (hookenv.relation_get('roles') or '').split(","))

    log('{} unit publishing credentials'.format(local_state['state']))

    password = create_user(user)
    reset_user_roles(user, roles)
    schema_user = "{}_schema".format(user)
    schema_password = create_user(schema_user)
    ensure_database(user, schema_user, database)
    host = hookenv.unit_private_ip()
    port = get_service_port()
    state = local_state['state']  # master, hot standby, standalone

    # Publish connection details.
    connection_settings = dict(
        user=user, password=password,
        schema_user=schema_user, schema_password=schema_password,
        host=host, database=database, port=port, state=state)
    log("Connection settings {!r}".format(connection_settings), DEBUG)
    hookenv.relation_set(relation_settings=connection_settings)

    # Update the peer relation, notifying any hot standby units
    # to republish connection details to the client relation.
    local_state['client_relations'] = ' '.join(sorted(
        hookenv.relation_ids('db') + hookenv.relation_ids('db-admin')))
    log("Client relations {}".format(local_state['client_relations']))
    local_state.publish()

    postgresql_hba = os.path.join(_get_postgresql_config_dir(), "pg_hba.conf")
    generate_postgresql_hba(postgresql_hba, user=user,
                            schema_user=schema_user,
                            database=database)

    snapshot_relations()


@hooks.hook('db-admin-relation-joined', 'db-admin-relation-changed')
def db_admin_relation_joined_changed():
    if local_state['state'] == 'hot standby':
        publish_hot_standby_credentials()
        return

    user = user_name(
        hookenv.relation_id(), hookenv.remote_unit(), admin=True)

    log('{} unit publishing credentials'.format(local_state['state']))

    password = create_user(user, admin=True)
    host = hookenv.unit_private_ip()
    port = get_service_port()
    state = local_state['state']  # master, hot standby, standalone

    # Publish connection details.
    connection_settings = dict(
        user=user, password=password,
        host=host, database='all', port=port, state=state)
    log("Connection settings {!r}".format(connection_settings), DEBUG)
    hookenv.relation_set(relation_settings=connection_settings)

    # Update the peer relation, notifying any hot standby units
    # to republish connection details to the client relation.
    local_state['client_relations'] = ' '.join(
        hookenv.relation_ids('db') + hookenv.relation_ids('db-admin'))
    log("Client relations {}".format(local_state['client_relations']))
    local_state.publish()

    postgresql_hba = os.path.join(_get_postgresql_config_dir(), "pg_hba.conf")
    generate_postgresql_hba(postgresql_hba)

    snapshot_relations()


@hooks.hook()
def db_relation_broken():
    from psycopg2.extensions import AsIs

    relid = hookenv.relation_id()
    if relid not in local_state['relations']['db']:
        # This was to be a hot standby, but it had not yet got as far as
        # receiving and handling credentials from the master.
        log("db-relation-broken called before relation finished setup", DEBUG)
        return

    # The relation no longer exists, so we can't pull the database name
    # we used from there. Instead, we have to persist this information
    # ourselves.
    relation = local_state['relations']['db'][relid]
    unit_relation_data = relation[hookenv.local_unit()]

    if local_state['state'] in ('master', 'standalone'):
        user = unit_relation_data.get('user', None)
        database = unit_relation_data['database']

        # We need to check that the database still exists before
        # attempting to revoke privileges because the local PostgreSQL
        # cluster may have been rebuilt by another hook.
        sql = "SELECT datname FROM pg_database WHERE datname = %s"
        if run_select_as_postgres(sql, database)[0] != 0:
            sql = "REVOKE ALL PRIVILEGES ON DATABASE %s FROM %s"
            run_sql_as_postgres(sql, AsIs(quote_identifier(database)),
                                AsIs(quote_identifier(user)))
            run_sql_as_postgres(sql, AsIs(quote_identifier(database)),
                                AsIs(quote_identifier(user + "_schema")))

    postgresql_hba = os.path.join(_get_postgresql_config_dir(), "pg_hba.conf")
    generate_postgresql_hba(postgresql_hba)

    # Cleanup our local state.
    snapshot_relations()


@hooks.hook()
def db_admin_relation_broken():
    from psycopg2.extensions import AsIs

    if local_state['state'] in ('master', 'standalone'):
        user = hookenv.relation_get('user', unit=hookenv.local_unit())
        if user:
            # We need to check that the user still exists before
            # attempting to revoke privileges because the local PostgreSQL
            # cluster may have been rebuilt by another hook.
            sql = "SELECT usename FROM pg_user WHERE usename = %s"
            if run_select_as_postgres(sql, user)[0] != 0:
                sql = "ALTER USER %s NOSUPERUSER"
                run_sql_as_postgres(sql, AsIs(quote_identifier(user)))

    postgresql_hba = os.path.join(_get_postgresql_config_dir(), "pg_hba.conf")
    generate_postgresql_hba(postgresql_hba)

    # Cleanup our local state.
    snapshot_relations()


def update_repos_and_packages():
    need_upgrade = False

    # Add the PGDG APT repository if it is enabled. Setting this boolean
    # is simpler than requiring the magic URL and key be added to
    # install_sources and install_keys. In addition, per Bug #1271148,
    # install_keys is likely a security hole for this sort of remote
    # archive. Instead, we keep a copy of the signing key in the charm
    # and can add it securely.
    pgdg_list = '/etc/apt/sources.list.d/pgdg_{}.list'.format(
        sanitize(hookenv.local_unit()))
    pgdg_key = 'ACCC4CF8'

    if hookenv.config('pgdg'):
        if not os.path.exists(pgdg_list):
            # We need to upgrade, as if we have Ubuntu main packages
            # installed they may be incompatible with the PGDG ones.
            # This is unlikely to ever happen outside of the test suite,
            # and never if you don't reuse machines.
            need_upgrade = True
            run("apt-key add lib/{}.asc".format(pgdg_key))
            open(pgdg_list, 'w').write('deb {} {}-pgdg main'.format(
                'http://apt.postgresql.org/pub/repos/apt/', distro_codename()))
    elif os.path.exists(pgdg_list):
        log(
            "PGDG apt source not requested, but already in place in this "
            "container", WARNING)
        # We can't just remove a source, as we may have packages
        # installed that conflict with ones from the other configured
        # sources. In particular, if we have postgresql-common installed
        # from the PGDG Apt source, PostgreSQL packages from Ubuntu main
        # will fail to install.
        # os.unlink(pgdg_list)

    # Try to optimize our calls to fetch.configure_sources(), as it
    # cannot do this itself due to lack of state.
    if (need_upgrade
        or local_state.get('install_sources', None)
            != hookenv.config('install_sources')
        or local_state.get('install_keys', None)
            != hookenv.config('install_keys')):
        # Support the standard mechanism implemented by charm-helpers. Pulls
        # from the default 'install_sources' and 'install_keys' config
        # options. This also does 'apt-get update', pulling in the PGDG data
        # if we just configured it.
        fetch.configure_sources(True)
        local_state['install_sources'] = hookenv.config('install_sources')
        local_state['install_keys'] = hookenv.config('install_keys')
        local_state.save()

    # Ensure that the desired database locale is possible.
    if hookenv.config('locale') != 'C':
        run(["locale-gen", "{}.{}".format(
            hookenv.config('locale'), hookenv.config('encoding'))])

    if need_upgrade:
        run("apt-get -y upgrade")

    version = pg_version()
    # It might have been better for debversion and plpython to only get
    # installed if they were listed in the extra-packages config item,
    # but they predate this feature.
    packages = ["python-psutil",  # to obtain system RAM from python
                "libc-bin",       # for getconf
                "postgresql-{}".format(version),
                "postgresql-contrib-{}".format(version),
                "postgresql-plpython-{}".format(version),
                "python-jinja2", "syslinux", "python-psycopg2"]
    # PGDG currently doesn't have debversion for 9.3. Put this back when
    # it does.
    if not (hookenv.config('pgdg') and version == '9.3'):
        "postgresql-{}-debversion".format(version)
    packages.extend((hookenv.config('extra-packages') or '').split())
    packages = fetch.filter_installed_packages(packages)
    fetch.apt_install(packages, fatal=True)


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
    relation = hookenv.relation_get(unit=unit)
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
        os.unlink(os.path.join(postgresql_cluster_dir, 'recovery.conf'))
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

    local_state.publish()


def publish_hot_standby_credentials():
    '''
    If a hot standby joins a client relation before the master
    unit, it is unable to publish connection details. However,
    when the master does join it updates the client_relations
    value in the peer relation causing the replication-relation-changed
    hook to be invoked. This gives us a second opertunity to publish
    connection details.

    This function is invoked from both the client and peer
    relation-changed hook. One of these will work depending on the order
    the master and hot standby joined the client relation.
    '''
    master = local_state['following']

    client_relations = hookenv.relation_get(
        'client_relations', master, hookenv.relation_ids('replication')[0])

    if client_relations is None:
        log("Master {} has not yet joined any client relations".format(
            master), DEBUG)
        return

    # Build the set of client relations that both the master and this
    # unit have joined.
    active_client_relations = set(
        hookenv.relation_ids('db') + hookenv.relation_ids('db-admin')
        ).intersection(set(client_relations.split()))

    for client_relation in active_client_relations:
        # We need to pull the credentials from the master unit's
        # end of the client relation. This is problematic as we
        # have no way of knowing if the master unit has joined
        # the relation yet. We use the exception handler to detect
        # this case per Bug #1192803.
        log('Hot standby republishing credentials from {} to {}'.format(
            master, client_relation))

        connection_settings = hookenv.relation_get(
            unit=master, rid=client_relation)

        # Override unit specific connection details
        connection_settings['host'] = hookenv.unit_private_ip()
        connection_settings['port'] = get_service_port()
        connection_settings['state'] = local_state['state']
        requested_db = hookenv.relation_get('database')
        # A hot standby might have seen a database name change before
        # the master, so override. This is no problem because we block
        # until this database has been created on the master and
        # replicated through to this unit.
        if requested_db:
            connection_settings['database'] = requested_db

        # Block until users and database has replicated, so we know the
        # connection details we publish are actually valid. This will
        # normally be pretty much instantaneous.
        timeout = 900
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

        log("Relation {} connection settings {!r}".format(
            client_relation, connection_settings), DEBUG)
        hookenv.relation_set(
            client_relation, relation_settings=connection_settings)


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


@contextmanager
def switch_cwd(new_working_directory):
    org_dir = os.getcwd()
    os.chdir(new_working_directory)
    try:
        yield new_working_directory
    finally:
        os.chdir(org_dir)


@contextmanager
def restart_lock(unit, exclusive):
    '''Aquire the database restart lock on the given unit.

    A database needing a restart should grab an exclusive lock before
    doing so. To block a remote database from doing a restart, grab a shared
    lock.
    '''
    import psycopg2
    key = long(hookenv.config('advisory_lock_restart_key'))
    if exclusive:
        lock_function = 'pg_advisory_lock'
    else:
        lock_function = 'pg_advisory_lock_shared'
    q = 'SELECT {}({})'.format(lock_function, key)

    # We will get an exception if the database is rebooted while waiting
    # for a shared lock. If the connection is killed, we retry a few
    # times to cope.
    num_retries = 3

    for count in range(0, num_retries):
        try:
            if unit == hookenv.local_unit():
                cur = db_cursor(autocommit=True)
            else:
                host = hookenv.relation_get('private-address', unit)
                port = hookenv.relation_get('port', unit)
                cur = db_cursor(
                    autocommit=True, db='postgres', user='juju_replication',
                    host=host, port=port)
            cur.execute(q)
            break
        except psycopg2.Error:
            if count == num_retries - 1:
                raise

    try:
        yield
    finally:
        # Close our connection, swallowing any exceptions as the database
        # may be being rebooted now we have released our lock.
        try:
            del cur
        except psycopg2.Error:
            pass


def clone_database(master_unit, master_host, master_port):
    with restart_lock(master_unit, False):
        postgresql_stop()
        log("Cloning master {}".format(master_unit))

        config_data = hookenv.config()
        version = pg_version()
        cluster_name = config_data['cluster_name']
        postgresql_cluster_dir = os.path.join(
            postgresql_data_dir, version, cluster_name)
        postgresql_config_dir = _get_postgresql_config_dir(config_data)
        cmd = [
            'sudo', '-E',  # -E needed to locate pgpass file.
            '-u', 'postgres', 'pg_basebackup', '-D', postgresql_cluster_dir,
            '--xlog', '--checkpoint=fast', '--no-password',
            '-h', master_host, '-p', master_port,
            '--username=juju_replication']
        log(' '.join(cmd), DEBUG)

        if os.path.isdir(postgresql_cluster_dir):
            shutil.rmtree(postgresql_cluster_dir)

        try:
            # Change directory the postgres user can read, and need
            # .pgpass too.
            with switch_cwd('/tmp'), pgpass():
                # Clone the master with pg_basebackup.
                output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            log(output, DEBUG)
            # Debian by default expects SSL certificates in the datadir.
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
            run('pg_createcluster {} main'.format(version))
            config_changed()
            raise
        finally:
            postgresql_start()
            wait_for_db()


def slave_count():
    num_slaves = 0
    for relid in hookenv.relation_ids('replication'):
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


def unit_sorted(units):
    """Return a sorted list of unit names."""
    return sorted(
        units, lambda a, b: cmp(int(a.split('/')[-1]), int(b.split('/')[-1])))


@hooks.hook('nrpe-external-master-relation-changed')
def update_nrpe_checks():
    config_data = hookenv.config()
    try:
        nagios_uid = getpwnam('nagios').pw_uid
        nagios_gid = getgrnam('nagios').gr_gid
    except Exception:
        hookenv.log("Nagios user not set up.", hookenv.DEBUG)
        return
    nagios_password = create_user('nagios')
    pg_pass_entry = '*:*:*:nagios:%s' % (nagios_password)
    with open('/var/lib/nagios/.pgpass', 'w') as target:
        os.fchown(target.fileno(), nagios_uid, nagios_gid)
        os.fchmod(target.fileno(), 0400)
        target.write(pg_pass_entry)

    unit_name = hookenv.local_unit().replace('/', '-')
    nagios_hostname = "%s-%s" % (config_data['nagios_context'], unit_name)
    nagios_logdir = '/var/log/nagios'
    nrpe_service_file = \
        '/var/lib/nagios/export/service__{}_check_pgsql.cfg'.format(
            nagios_hostname)
    if not os.path.exists(nagios_logdir):
        os.mkdir(nagios_logdir)
        os.chown(nagios_logdir, nagios_uid, nagios_gid)
    for f in os.listdir('/var/lib/nagios/export/'):
        if re.search('.*check_pgsql.cfg', f):
            os.remove(os.path.join('/var/lib/nagios/export/', f))

    # --- exported service configuration file
    from jinja2 import Environment, FileSystemLoader
    template_env = Environment(
        loader=FileSystemLoader(
            os.path.join(os.environ['CHARM_DIR'], 'templates')))
    templ_vars = {
        'nagios_hostname': nagios_hostname,
        'nagios_servicegroup': config_data['nagios_context'],
    }
    template = \
        template_env.get_template('nrpe_service.tmpl').render(templ_vars)
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
    backup_log = "{}/backups.log".format(postgresql_logs_dir)
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
replication_relation_types = ['master', 'slave', 'replication']
local_state = State('local_state.pickle')
hook_name = os.path.basename(sys.argv[0])
juju_log_dir = "/var/log/juju"
external_volume_mount = "/srv/data"


if __name__ == '__main__':
    # Hook and context overview. The various replication and client
    # hooks interact in complex ways.
    log("Running {} hook".format(hook_name))
    if hookenv.relation_id():
        log("Relation {} with {}".format(
            hookenv.relation_id(), hookenv.remote_unit()))
    hooks.execute(sys.argv)
