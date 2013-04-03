#!/usr/bin/env python
# vim: et ai ts=4 sw=4:

import cPickle as pickle
import json
import yaml
import os
import glob
import random
import re
import shutil
import string
import socket
import subprocess
import sys
from textwrap import dedent
import time
from yaml.constructor import ConstructorError
import commands
from pwd import getpwnam
from grp import getgrnam


# jinja2 may not be importable until the install hook has installed the
# required packages.
def Template(*args, **kw):
    from jinja2 import Template
    return Template(*args, **kw)


###############################################################################
# Supporting functions
###############################################################################
MSG_CRITICAL = "CRITICAL"
MSG_DEBUG = "DEBUG"
MSG_INFO = "INFO"
MSG_ERROR = "ERROR"
MSG_WARNING = "WARNING"


def juju_log(level, msg):
    subprocess.call(['juju-log', '-l', level, msg])


class State(dict):
    """Encapsulate state common to the unit for republishing to relations."""
    def __init__(self, state_file):
        self._state_file = state_file
        self.load()

    def load(self):
        if os.path.exists(self._state_file):
            state = pickle.load(open(self._state_file, 'rb'))
        else:
            state = {
                'cluster_name': os.environ['JUJU_UNIT_NAME'].replace('/','_')}
        self.clear()
        self.update(state)

    def save(self):
        state = {}
        state.update(self)
        pickle.dump(state, open(self._state_file, 'wb'))

    def publish(self):
        """Publish relevant unit state to relations"""

        def add(state_dict, key):
            if self.has_key(key):
                state_dict[key] = self[key]

        client_state = {}
        add(client_state, 'state')

        for relid in relation_ids(relation_types=['db', 'db-admin']):
            relation_set(client_state, relid)

        replication_state = dict(client_state)

        add(replication_state, 'public_ssh_key')
        add(replication_state, 'ssh_host_key')
        add(replication_state, 'replication_password')

        authorized = self.get('authorized', None)
        if authorized:
            replication_state['authorized'] = ' '.join(sorted(authorized))

        for relid in relation_ids(relation_types=replication_relation_types):
            relation_set(replication_state, relid)

        self.save()


###############################################################################

# Volume managment
###############################################################################
#------------------------------
# Get volume-id from juju config "volume-map" dictionary as
#     volume-map[JUJU_UNIT_NAME]
# @return  volid
#
#------------------------------
def volume_get_volid_from_volume_map():
    volume_map = {}
    try:
        volume_map = yaml.load(config_data['volume-map'])
        if volume_map:
            return volume_map.get(os.environ['JUJU_UNIT_NAME'])
    except ConstructorError as e:
        juju_log(MSG_WARNING, "invalid YAML in 'volume-map': %s", e)
    return None


# Is this volume_id permanent ?
# @returns  True if volid set and not --ephemeral, else:
#           False
def volume_is_permanent(volid):
    if volid and volid != "--ephemeral":
        return True
    return False


#------------------------------
# Returns a mount point from passed vol-id, e.g. /srv/juju/vol-000012345
#
# @param  volid          volume id (as e.g. EBS volid)
# @return mntpoint_path  eg /srv/juju/vol-000012345
#------------------------------
def volume_mount_point_from_volid(volid):
    if volid and volume_is_permanent(volid):
        return "/srv/juju/%s" % volid
    return None


# Do we have a valid storage state?
# @returns  volid
#           None    config state is invalid - we should not serve
def volume_get_volume_id():
    ephemeral_storage = config_data['volume-ephemeral-storage']
    volid = volume_get_volid_from_volume_map()
    juju_unit_name = os.environ['JUJU_UNIT_NAME']
    if ephemeral_storage in [True, 'yes', 'Yes', 'true', 'True']:
        if volid:
            juju_log(MSG_ERROR, "volume-ephemeral-storage is True, but " +
                     "volume-map['%s'] -> %s" % (juju_unit_name, volid))
            return None
        else:
            return "--ephemeral"
    else:
        if not volid:
            juju_log(MSG_ERROR, "volume-ephemeral-storage is False, but " +
                     "no volid found for volume-map['%s']" % (juju_unit_name))
            return None
    return volid


# Initialize and/or mount permanent storage, it straightly calls
# shell helper
def volume_init_and_mount(volid):
    command = ("scripts/volume-common.sh call " +
              "volume_init_and_mount %s" % volid)
    output = run(command)
    if output.find("ERROR") >= 0:
        return False
    return True


def volume_get_all_mounted():
    command = ("mount |egrep /srv/juju")
    status, output = commands.getstatusoutput(command)
    if status != 0:
        return None
    return output


#------------------------------------------------------------------------------
# Enable/disable service start by manipulating policy-rc.d
#------------------------------------------------------------------------------
def enable_service_start(service):
    ### NOTE: doesn't implement per-service, this can be an issue
    ###       for colocated charms (subordinates)
    juju_log(MSG_INFO, "NOTICE: enabling %s start by policy-rc.d" % service)
    if os.path.exists('/usr/sbin/policy-rc.d'):
        os.unlink('/usr/sbin/policy-rc.d')
        return True
    return False


def disable_service_start(service):
    juju_log(MSG_INFO, "NOTICE: disabling %s start by policy-rc.d" % service)
    policy_rc = '/usr/sbin/policy-rc.d'
    policy_rc_tmp = "%s.tmp" % policy_rc
    open('%s' % policy_rc_tmp, 'w').write("""#!/bin/bash
[[ "$1"-"$2" == %s-start ]] && exit 101
exit 0
EOF
""" % service)
    os.chmod(policy_rc_tmp, 0755)
    os.rename(policy_rc_tmp, policy_rc)


#------------------------------------------------------------------------------
# run: Run a command, return the output
#------------------------------------------------------------------------------
def run(command, exit_on_error=True):
    try:
        juju_log(MSG_DEBUG, command)
        return subprocess.check_output(
            command, stderr=subprocess.STDOUT, shell=True)
    except subprocess.CalledProcessError, e:
        juju_log(MSG_ERROR, "status=%d, output=%s" % (e.returncode, e.output))
        if exit_on_error:
            sys.exit(e.returncode)
        else:
            raise


#------------------------------------------------------------------------------
# install_file: install a file resource. overwites existing files.
#------------------------------------------------------------------------------
def install_file(contents, dest, owner="root", group="root", mode=0600):
    uid = getpwnam(owner)[2]
    gid = getgrnam(group)[2]
    dest_fd = os.open(dest, os.O_WRONLY | os.O_TRUNC | os.O_CREAT, mode)
    os.fchown(dest_fd, uid, gid)
    with os.fdopen(dest_fd, 'w') as destfile:
        destfile.write(str(contents))


#------------------------------------------------------------------------------
# install_dir: create a directory
#------------------------------------------------------------------------------
def install_dir(dirname, owner="root", group="root", mode=0700):
    command = \
    '/usr/bin/install -o {} -g {} -m {} -d {}'.format(owner, group, oct(mode),
        dirname)
    return run(command)


#------------------------------------------------------------------------------
# postgresql_stop, postgresql_start, postgresql_is_running:
# wrappers over invoke-rc.d, with extra check for postgresql_is_running()
#------------------------------------------------------------------------------
def postgresql_is_running():
    # init script always return true (9.1), add extra check to make it useful
    status, output = commands.getstatusoutput("invoke-rc.d postgresql status")
    if status != 0:
        return False
    # e.g. output: "Running clusters: 9.1/main"
    vc = "%s/%s" % (config_data["version"], local_state['cluster_name'])
    return vc in output.decode('utf8').split()


def postgresql_stop():
    status, output = commands.getstatusoutput("invoke-rc.d postgresql stop")
    if status != 0:
        return False
    return not postgresql_is_running()


def postgresql_start():
    status, output = commands.getstatusoutput("invoke-rc.d postgresql start")
    if status != 0:
        juju_log(MSG_CRITICAL, output)
        return False
    return postgresql_is_running()


def postgresql_restart():
    if postgresql_is_running():
        # If the database is in backup mode, we don't want to restart
        # PostgreSQL and abort the procedure. This may be another unit being
        # cloned, or a filesystem level backup is being made. There is no
        # timeout here, as backups can take hours or days. Instead, keep
        # logging so admins know wtf is going on.
        last_warning = time.time()
        while postgresql_is_in_backup_mode():
            if time.time() + 120 > last_warning:
                juju_log(
                    MSG_WARNING,
                    "In backup mode. PostgreSQL restart blocked.")
                juju_log(
                    MSG_INFO,
                    "Run \"psql -U postgres -c 'SELECT pg_stop_backup()'\""
                    "to cancel backup mode and forcefully unblock this hook.")
                last_warning = time.time()
            time.sleep(5)

        status, output = \
            commands.getstatusoutput("invoke-rc.d postgresql restart")
        if status != 0:
            return False
    else:
        postgresql_start()

    # Store a copy of our known live configuration so
    # postgresql_reload_or_restart() can make good choices.
    if local_state.has_key('saved_config'):
        local_state['live_config'] = local_state['saved_config']
        local_state.save()

    return postgresql_is_running()


def postgresql_reload():
    # reload returns a reliable exit status
    status, output = commands.getstatusoutput("invoke-rc.d postgresql reload")
    return (status == 0)


def postgresql_reload_or_restart():
    """Reload PostgreSQL configuration, restarting if necessary."""
    # Pull in current values of settings that can only be changed on
    # server restart.
    if not postgresql_is_running():
        return postgresql_restart()

    # Suck in the config last written to postgresql.conf.
    saved_config = local_state.get('saved_config', None)
    if not saved_config:
        # No record of postgresql.conf state, perhaps an upgrade.
        # Better restart.
        return postgresql_restart()

    # Suck in our live config from last time we restarted.
    live_config = local_state.setdefault('live_config', {})

    # Pull in a list of PostgreSQL settings.
    cur = db_cursor()
    cur.execute("SELECT name, context FROM pg_settings")
    requires_restart = False
    for name, context in cur.fetchall():
        live_value = live_config.get(name, None)
        new_value = saved_config.get(name, None)

        if new_value != live_value:
            if live_config:
                juju_log(
                    MSG_DEBUG, "Changed {} from {} to {}".format(
                        name, repr(live_value), repr(new_value)))
            if context == 'postmaster':
                # A setting has changed that requires PostgreSQL to be
                # restarted before it will take effect.
                requires_restart = True

    if requires_restart:
        # A change has been requested that requires a restart.
        juju_log(
            MSG_WARNING,
            "Configuration change requires PostgreSQL restart. "
            "Restarting.")
        rc = postgresql_restart()
    else:
        juju_log(
            MSG_DEBUG, "PostgreSQL reload, config changes taking effect.")
        rc = postgresql_reload()  # No pending need to bounce, just reload.

    if rc == 0 and local_state.has_key('saved_config'):
        local_state['live_config'] = local_state['saved_config']
        local_state.save()

    return rc


#------------------------------------------------------------------------------
# config_get:  Returns a dictionary containing all of the config information
#              Optional parameter: scope
#              scope: limits the scope of the returned configuration to the
#                     desired config item.
#------------------------------------------------------------------------------
def config_get(scope=None):
    try:
        config_cmd_line = ['config-get']
        if scope is not None:
            config_cmd_line.append(scope)
        config_cmd_line.append('--format=json')
        config_data = json.loads(subprocess.check_output(config_cmd_line))
    except:
        config_data = None
    finally:
        return(config_data)


#------------------------------------------------------------------------------
# get_service_port:   Convenience function that scans the existing postgresql
#                     configuration file and returns a the existing port
#                     being used.  This is necessary to know which port(s)
#                     to open and close when exposing/unexposing a service
#------------------------------------------------------------------------------
def get_service_port(postgresql_config):
    postgresql_config = load_postgresql_config(postgresql_config)
    if postgresql_config is None:
        return(None)
    port = re.search("port.*=(.*)", postgresql_config).group(1).strip()
    try:
        return int(port)
    except:
        return None


#------------------------------------------------------------------------------
# relation_json:  Returns json-formatted relation data
#                Optional parameters: scope, relation_id
#                scope:        limits the scope of the returned data to the
#                              desired item.
#                unit_name:    limits the data ( and optionally the scope )
#                              to the specified unit
#                relation_id:  specify relation id for out of context usage.
#------------------------------------------------------------------------------
def relation_json(scope=None, unit_name=None, relation_id=None):
    command = ['relation-get', '--format=json']
    if relation_id is not None:
        command.extend(('-r', relation_id))
    if scope is not None:
        command.append(scope)
    else:
        command.append('-')
    if unit_name is not None:
        command.append(unit_name)
    output = subprocess.check_output(command, stderr=subprocess.STDOUT)
    return output or None


#------------------------------------------------------------------------------
# relation_get:  Returns a dictionary containing the relation information
#                Optional parameters: scope, relation_id
#                scope:        limits the scope of the returned data to the
#                              desired item.
#                unit_name:    limits the data ( and optionally the scope )
#                              to the specified unit
#------------------------------------------------------------------------------
def relation_get(scope=None, unit_name=None, relation_id=None):
    j = relation_json(scope, unit_name, relation_id)
    if j:
        return json.loads(j)
    else:
        return None


def relation_set(keyvalues, relation_id=None):
    args = []
    if relation_id:
        args.extend(['-r', relation_id])
    args.extend(["{}='{}'".format(k, v or '') for k, v in keyvalues.items()])
    run("relation-set {}".format(' '.join(args)))

    ## Posting json to relation-set doesn't seem to work as documented?
    ## Bug #1116179
    ##
    ## cmd = ['relation-set']
    ## if relation_id:
    ##     cmd.extend(['-r', relation_id])
    ## p = Popen(
    ##     cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
    ##     stderr=subprocess.PIPE)
    ## (out, err) = p.communicate(json.dumps(keyvalues))
    ## if p.returncode:
    ##     juju_log(MSG_ERROR, err)
    ##     sys.exit(1)
    ## juju_log(MSG_DEBUG, "relation-set {}".format(repr(keyvalues)))


def relation_list(relation_id=None):
    """Return the list of units participating in the relation."""
    if relation_id is None:
        relation_id = os.environ['JUJU_RELATION_ID']
    cmd = ['relation-list', '--format=json', '-r', relation_id]
    json_units = subprocess.check_output(cmd).strip()
    if json_units:
        return json.loads(subprocess.check_output(cmd))
    return []


#------------------------------------------------------------------------------
# relation_ids:  Returns a list of relation ids
#                optional parameters: relation_type
#                relation_type: return relations only of this type
#------------------------------------------------------------------------------
def relation_ids(relation_types=('db',)):
    # accept strings or iterators
    if isinstance(relation_types, basestring):
        reltypes = [relation_types, ]
    else:
        reltypes = relation_types
    relids = []
    for reltype in reltypes:
        relid_cmd_line = ['relation-ids', '--format=json', reltype]
        json_relids = subprocess.check_output(relid_cmd_line).strip()
        if json_relids:
            relids.extend(json.loads(json_relids))
    return relids


#------------------------------------------------------------------------------
# relation_get_all:  Returns a dictionary containing the relation information
#                optional parameters: relation_type
#                relation_type: limits the scope of the returned data to the
#                               desired item.
#------------------------------------------------------------------------------
def relation_get_all(*args, **kwargs):
    relation_data = []
    relids = relation_ids(*args, **kwargs)
    for relid in relids:
        units_cmd_line = ['relation-list', '--format=json', '-r', relid]
        json_units = subprocess.check_output(units_cmd_line).strip()
        if json_units:
            for unit in json.loads(json_units):
                unit_data = \
                    json.loads(relation_json(relation_id=relid,
                        unit_name=unit))
                for key in unit_data:
                    if key.endswith('-list'):
                        unit_data[key] = unit_data[key].split()
                unit_data['relation-id'] = relid
                unit_data['unit'] = unit
                relation_data.append(unit_data)
    return relation_data


#------------------------------------------------------------------------------
# apt_get_install( packages ):  Installs package(s)
#------------------------------------------------------------------------------
def apt_get_install(packages=None):
    if packages is None:
        return(False)
    cmd_line = ['apt-get', '-y', 'install', '-qq']
    cmd_line.extend(packages)
    return(subprocess.call(cmd_line))


#------------------------------------------------------------------------------
# create_postgresql_config:   Creates the postgresql.conf file
#------------------------------------------------------------------------------
def create_postgresql_config(postgresql_config):
    if config_data["performance_tuning"] == "auto":
        # Taken from:
        # http://wiki.postgresql.org/wiki/Tuning_Your_PostgreSQL_Server
        # num_cpus is not being used ... commenting it out ... negronjl
        #num_cpus = run("cat /proc/cpuinfo | grep processor | wc -l")
        total_ram = run("free -m | grep Mem | awk '{print $2}'")
        config_data["effective_cache_size"] = \
            "%sMB" % (int(int(total_ram) * 0.75),)
        if total_ram > 1023:
            config_data["shared_buffers"] = \
                "%sMB" % (int(int(total_ram) * 0.25),)
        else:
            config_data["shared_buffers"] = \
                "%sMB" % (int(int(total_ram) * 0.15),)
        # XXX: This is very messy - should probably be a subordinate charm
        # file overlaps with __builtin__.file ... renaming to conf_file
        # negronjl
        conf_file = open("/etc/sysctl.d/50-postgresql.conf", "w")
        conf_file.write("kernel.sem = 250 32000 100 1024\n")
        conf_file.write("kernel.shmall = %s\n" %
            ((int(total_ram) * 1024 * 1024) + 1024),)
        conf_file.write("kernel.shmmax = %s\n" %
            ((int(total_ram) * 1024 * 1024) + 1024),)
        conf_file.close()
        run("sysctl -p /etc/sysctl.d/50-postgresql.conf")

    # If we are replicating, some settings may need to be overridden to
    # certain minimum levels.
    num_slaves = slave_count()
    if num_slaves > 0:
        juju_log(
            MSG_INFO, 'Master replicated to {} hot standbys.'.format(
                num_slaves))
        juju_log(MSG_INFO, 'Ensuring minimal replication settings')
        config_data['hot_standby'] = 'on'
        config_data['wal_level'] = 'hot_standby'
        config_data['max_wal_senders'] = max(
            num_slaves, config_data['max_wal_senders'])
        config_data['wal_keep_segments'] = max(
            config_data['wal_keep_segments'],
            config_data['replicated_wal_keep_segments'])

    # Send config data to the template
    # Return it as pg_config
    pg_config = Template(
            open("templates/postgresql.conf.tmpl").read()).render(config_data)
    install_file(pg_config, postgresql_config)

    local_state['saved_config'] = config_data
    local_state.save()


#------------------------------------------------------------------------------
# create_postgresql_ident:  Creates the pg_ident.conf file
#------------------------------------------------------------------------------
def create_postgresql_ident(postgresql_ident):
    ident_data = {}
    pg_ident_template = \
        Template(
            open("templates/pg_ident.conf.tmpl").read()).render(ident_data)
    with open(postgresql_ident, 'w') as ident_file:
        ident_file.write(str(pg_ident_template))


#------------------------------------------------------------------------------
# generate_postgresql_hba:  Creates the pg_hba.conf file
#------------------------------------------------------------------------------
def generate_postgresql_hba(postgresql_hba):

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
            return addr

    relation_data = relation_get_all(relation_types=['db', 'db-admin'])
    for relation in relation_data:
        relation_id = relation['relation-id']
        if relation_id.startswith('db-admin:'):
            relation['user'] = 'all'
            relation['database'] = 'all'
        elif relation_id.startswith('db:'):
            relation['user'] = user_name(relation['relation-id'],
                                         relation['unit'])
            relation['schema_user'] = user_name(relation['relation-id'],
                                                relation['unit'],
                                                schema=True)
        else:
            raise RuntimeError(
                'Unknown relation type {}'.format(repr(relation_id)))

        relation['private-address'] = munge_address(
            relation['private-address'])

    juju_log(MSG_INFO, str(relation_data))

    # Replication connections. Each unit needs to be able to connect to
    # every other unit's postgres database and the magic replication
    # database. It also needs to be able to connect to its own postgres
    # database.
    for relid in relation_ids(relation_types=replication_relation_types):
        for unit in relation_list(relid):
            replicated = True
            relation = relation_get(unit_name=unit, relation_id=relid)
            remote_addr = munge_address(relation['private-address'])
            remote_replication = {
                'database': 'replication', 'user': 'juju_replication',
                'private-address': remote_addr,
                'relation-id': relid,
                'unit': unit,
                }
            relation_data.append(remote_replication)
            remote_pgdb = {
                'database': 'postgres', 'user': 'juju_replication',
                'private-address': remote_addr,
                'relation-id': relid,
                'unit': unit,
                }
            relation_data.append(remote_pgdb)

    # Hooks need permissions too to setup replication.
    for relid in relation_ids(relation_types=['replication']):
        local_replication = {
            'database': 'postgres', 'user': 'juju_replication',
            'private-address': munge_address(get_unit_host()),
            'relation-id': relid,
            'unit': os.environ['JUJU_UNIT_NAME'],
            }
        relation_data.append(local_replication)

    pg_hba_template = Template(
        open("templates/pg_hba.conf.tmpl").read()).render(
            access_list=relation_data)
    with open(postgresql_hba, 'w') as hba_file:
        hba_file.write(str(pg_hba_template))
    postgresql_reload()



#------------------------------------------------------------------------------
# install_postgresql_crontab:  Creates the postgresql crontab file
#------------------------------------------------------------------------------
def install_postgresql_crontab(postgresql_ident):
    crontab_data = {
        'backup_schedule': config_data["backup_schedule"],
        'scripts_dir': postgresql_scripts_dir,
        'backup_days': config_data["backup_retention_count"],
    }
    crontab_template = Template(
        open("templates/postgres.cron.tmpl").read()).render(crontab_data)
    install_file(str(crontab_template), "/etc/cron.d/postgres", mode=0644)


#------------------------------------------------------------------------------
# load_postgresql_config:  Convenience function that loads (as a string) the
#                          current postgresql configuration file.
#                          Returns a string containing the postgresql config or
#                          None
#------------------------------------------------------------------------------
def load_postgresql_config(postgresql_config):
    if os.path.isfile(postgresql_config):
        return(open(postgresql_config).read())
    else:
        return(None)


#------------------------------------------------------------------------------
# open_port:  Convenience function to open a port in juju to
#             expose a service
#------------------------------------------------------------------------------
def open_port(port=None, protocol="TCP"):
    if port is None:
        return(None)
    return(subprocess.call(['open-port', "%d/%s" %
        (int(port), protocol)]))


#------------------------------------------------------------------------------
# close_port:  Convenience function to close a port in juju to
#              unexpose a service
#------------------------------------------------------------------------------
def close_port(port=None, protocol="TCP"):
    if port is None:
        return(None)
    return(subprocess.call(['close-port', "%d/%s" %
        (int(port), protocol)]))


#------------------------------------------------------------------------------
# update_service_ports:  Convenience function that evaluate the old and new
#                        service ports to decide which ports need to be
#                        opened and which to close
#------------------------------------------------------------------------------
def update_service_port(old_service_port=None, new_service_port=None):
    if old_service_port is None or new_service_port is None:
        return(None)
    if new_service_port != old_service_port:
        close_port(old_service_port)
        open_port(new_service_port)


#------------------------------------------------------------------------------
# pwgen:  Generates a random password
#         pwd_length:  Defines the length of the password to generate
#                      default: 20
#------------------------------------------------------------------------------
def pwgen(pwd_length=None):
    if pwd_length is None:
        pwd_length = random.choice(range(20, 30))
    alphanumeric_chars = [l for l in (string.letters + string.digits)
        if l not in 'Iil0oO1']
    random_chars = [random.choice(alphanumeric_chars)
        for i in range(pwd_length)]
    return(''.join(random_chars))


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


def db_cursor(
    autocommit=False, db='template1', user='postgres', host=None, timeout=120):
    import psycopg2
    if host:
        conn_str = "dbname={} host={} user={}".format(db, host, user)
    else:
        conn_str = "dbname={} user={}".format(db, user)
    # There are often race conditions in opening database connections,
    # such as a reload having just happened to change pg_hba.conf
    # settings or a hot standby being restarted and needing to catch up
    # with its master. To protect our automation against these sorts of
    # race conditions, by default we always retry failed connections
    # until a timeout is reached.
    start = time.time()
    while True:
        try:
            conn = psycopg2.connect(conn_str)
            break
        except psycopg2.Error:
            if time.time() > start + timeout:
                juju_log(
                    MSG_CRITICAL, "Database connection {!r} failed".format(
                        conn_str))
                raise
        time.sleep(0.3)
    conn.autocommit = autocommit
    return conn.cursor()


def run_sql_as_postgres(sql, *parameters):
    import psycopg2
    cur = db_cursor(autocommit=True)
    try:
        cur.execute(sql, parameters)
        return cur.statusmessage
    except psycopg2.ProgrammingError:
        juju_log(MSG_CRITICAL, sql)
        raise


def run_select_as_postgres(sql, *parameters):
    cur = db_cursor()
    cur.execute(sql, parameters)
    # NB. Need to suck in the results before the rowcount is valid.
    results = cur.fetchall()
    return (cur.rowcount, results)


#------------------------------------------------------------------------------
# Core logic for permanent storage changes:
# NOTE the only 2 "True" return points:
#   1) symlink already pointing to existing storage (no-op)
#   2) new storage properly initialized:
#     - volume: initialized if not already (fdisk, mkfs),
#       mounts it to e.g.:  /srv/juju/vol-000012345
#     - if fresh new storage dir: rsync existing data
#     - manipulate /var/lib/postgresql/VERSION/CLUSTER symlink
#------------------------------------------------------------------------------
def config_changed_volume_apply():
    data_directory_path = postgresql_cluster_dir
    assert(data_directory_path)
    volid = volume_get_volume_id()
    if volid:
        if volume_is_permanent(volid):
            if not volume_init_and_mount(volid):
                juju_log(MSG_ERROR, "volume_init_and_mount failed, " +
                     "not applying changes")
                return False

        if not os.path.exists(data_directory_path):
            juju_log(MSG_CRITICAL, ("postgresql data dir = %s not found, " +
                     "not applying changes.") % data_directory_path)
            return False

        mount_point = volume_mount_point_from_volid(volid)
        new_pg_dir = os.path.join(mount_point, "postgresql")
        new_pg_version_cluster_dir = os.path.join(new_pg_dir,
            config_data["version"], local_state['cluster_name'])
        if not mount_point:
            juju_log(MSG_ERROR, "invalid mount point from volid = \"%s\", " +
                     "not applying changes." % mount_point)
            return False

        if (os.path.islink(data_directory_path) and
            os.readlink(data_directory_path) == new_pg_version_cluster_dir and
            os.path.isdir(new_pg_version_cluster_dir)):
            juju_log(MSG_INFO,
                "NOTICE: postgresql data dir '%s' already points to '%s', \
                skipping storage changes." %
                (data_directory_path, new_pg_version_cluster_dir))
            juju_log(MSG_INFO,
                "existing-symlink: to fix/avoid UID changes from previous "
                "units, doing: chown -R postgres:postgres %s" % new_pg_dir)
            run("chown -R postgres:postgres %s" % new_pg_dir)
            return True

        # Create a directory structure below "new" mount_point, as e.g.:
        #   /srv/juju/vol-000012345/postgresql/9.1/main  , which "mimics":
        #   /var/lib/postgresql/9.1/main
        curr_dir_stat = os.stat(data_directory_path)
        for new_dir in [new_pg_dir,
                    os.path.join(new_pg_dir, config_data["version"]),
                    new_pg_version_cluster_dir]:
            if not os.path.isdir(new_dir):
                juju_log(MSG_INFO, "mkdir %s" % new_dir)
                os.mkdir(new_dir)
                # copy permissions from current data_directory_path
                os.chown(new_dir, curr_dir_stat.st_uid, curr_dir_stat.st_gid)
                os.chmod(new_dir, curr_dir_stat.st_mode)
        # Carefully build this symlink, e.g.:
        # /var/lib/postgresql/9.1/main ->
        # /srv/juju/vol-000012345/postgresql/9.1/main
        # but keep previous "main/"  directory, by renaming it to
        # main-$TIMESTAMP
        if not postgresql_stop():
            juju_log(MSG_ERROR,
                "postgresql_stop() returned False - can't migrate data.")
            return False
        if not os.path.exists(os.path.join(new_pg_version_cluster_dir,
            "PG_VERSION")):
            juju_log(MSG_WARNING, "migrating PG data %s/ -> %s/" % (
                     data_directory_path, new_pg_version_cluster_dir))
            # void copying PID file to perm storage (shouldn't be any...)
            command = "rsync -a --exclude postmaster.pid %s/ %s/" % \
                (data_directory_path, new_pg_version_cluster_dir)
            juju_log(MSG_INFO, "run: %s" % command)
            #output = run(command)
            run(command)
        try:
            os.rename(data_directory_path, "%s-%d" % (
                          data_directory_path, int(time.time())))
            juju_log(MSG_INFO, "NOTICE: symlinking %s -> %s" %
                (new_pg_version_cluster_dir, data_directory_path))
            os.symlink(new_pg_version_cluster_dir, data_directory_path)
            juju_log(MSG_INFO,
                "after-symlink: to fix/avoid UID changes from previous "
                "units, doing: chown -R postgres:postgres %s" % new_pg_dir)
            run("chown -R postgres:postgres %s" % new_pg_dir)
            return True
        except OSError:
            juju_log(MSG_CRITICAL, "failed to symlink \"%s\" -> \"%s\"" % (
                          data_directory_path, mount_point))
            return False
    else:
        juju_log(MSG_ERROR, "ERROR: Invalid volume storage configuration, " +
                 "not applying changes")
    return False


###############################################################################
# Hook functions
###############################################################################
def config_changed(postgresql_config, force_restart=False):

    add_extra_repos()

    # Trigger volume initialization logic for permanent storage
    volid = volume_get_volume_id()
    if not volid:
        ## Invalid configuration (whether ephemeral, or permanent)
        disable_service_start("postgresql")
        postgresql_stop()
        mounts = volume_get_all_mounted()
        if mounts:
            juju_log(MSG_INFO, "FYI current mounted volumes: %s" % mounts)
        juju_log(MSG_ERROR,
            "Disabled and stopped postgresql service, \
            because of broken volume configuration - check \
            'volume-ephermeral-storage' and 'volume-map'")
        sys.exit(1)

    if volume_is_permanent(volid):
        ## config_changed_volume_apply will stop the service if it founds
        ## it necessary, ie: new volume setup
        if config_changed_volume_apply():
            enable_service_start("postgresql")
            force_restart = True
        else:
            disable_service_start("postgresql")
            postgresql_stop()
            mounts = volume_get_all_mounted()
            if mounts:
                juju_log(MSG_INFO, "FYI current mounted volumes: %s" % mounts)
            juju_log(MSG_ERROR,
                "Disabled and stopped postgresql service \
                (config_changed_volume_apply failure)")
            sys.exit(1)
    current_service_port = get_service_port(postgresql_config)
    create_postgresql_config(postgresql_config)
    generate_postgresql_hba(postgresql_hba)
    create_postgresql_ident(postgresql_ident)
    updated_service_port = config_data["listen_port"]
    update_service_port(current_service_port, updated_service_port)
    update_nrpe_checks()
    generate_pgpass()
    if force_restart:
        return postgresql_restart()
    return postgresql_reload_or_restart()


def token_sql_safe(value):
    # Only allow alphanumeric + underscore in database identifiers
    if re.search('[^A-Za-z0-9_]', value):
        return False
    return True


def install(run_pre=True):
    if run_pre:
        for f in glob.glob('exec.d/*/charm-pre-install'):
            if os.path.isfile(f) and os.access(f, os.X_OK):
                subprocess.check_call(['sh', '-c', f])

    add_extra_repos()

    postgresql_already_installed = package_installed('postgresql')

    packages = ["postgresql", "pwgen", "python-jinja2", "syslinux",
                "python-psycopg2", "postgresql-contrib", "postgresql-plpython",
                "postgresql-%s-debversion" % config_data["version"]]
    packages.extend(config_data["extra-packages"].split())
    apt_get_install(packages)

    if not local_state.has_key('state'):
        # Fresh installation. Because this function is invoked by both
        # the install hook and the upgrade-charm hook, we need to guard
        # any non-idempotent setup. We should probably fix this; it
        # seems rather fragile.
        local_state.setdefault('state', 'standalone')
        local_state.publish()

    if not postgresql_already_installed:
        # We just installed PostgreSQL, and a database cluster was
        # created that we don't want. Nuke it. Note that if we didn't
        # just install the postgresql package, a cohosted charm already
        # in this container may be using it.
        run("pg_dropcluster --stop 9.1 {}".format(local_state['cluster_name']))

    if not pg_cluster_exists():
        pg_createcluster()

    install_dir(postgresql_backups_dir, owner="postgres", mode=0755)
    install_dir(postgresql_scripts_dir, owner="postgres", mode=0755)
    install_dir(postgresql_logs_dir, owner="postgres", mode=0755)
    paths = {
        'base_dir': postgresql_data_dir,
        'backup_dir': postgresql_backups_dir,
        'scripts_dir': postgresql_scripts_dir,
        'logs_dir': postgresql_logs_dir,
    }
    dump_script = Template(
        open("templates/dump-pg-db.tmpl").read()).render(paths)
    backup_job = Template(
        open("templates/pg_backup_job.tmpl").read()).render(paths)
    install_file(dump_script, '{}/dump-pg-db'.format(postgresql_scripts_dir),
        mode=0755)
    install_file(backup_job, '{}/pg_backup_job'.format(postgresql_scripts_dir),
        mode=0755)
    install_postgresql_crontab(postgresql_crontab)
    open_port(5432)

    # Ensure at least minimal access granted for hooks to run.
    # Reload because we are using the default cluster setup and started
    # when we installed the PostgreSQL packages.
    config_changed(postgresql_config, force_restart=True)


def upgrade_charm():
    # Detect if we are upgrading from the old charm that used repmgr for
    # replication.
    from_repmgr = not local_state.has_key('juju_replication')

    # Handle renaming of the repmgr user to juju_replication.
    if from_repmgr and local_state['state'] == 'master':
        replication_password = create_user(
            'juju_replication', admin=True, replication=True)
        generate_pgpass()
        local_state['replication_password'] = replication_password
        juju_log(MSG_INFO, "Updating replication connection details")
        local_state.publish()
        drop_database('repmgr')

    if from_repmgr and local_state['state'] == 'hot standby':
        for relid in relation_ids(relation_types=['replication']):
            for unit in relation_list(relid):
                relation = relation_get(unit_name=unit, relation_id=relid)
                if relation.get('state', None) == 'master':
                    recovery_conf = Template(
                        open("templates/recovery.conf.tmpl").read()).render({
                            'host': relation['private-address'],
                            'password': local_state['replication_password']})
                    juju_log(MSG_DEBUG, recovery_conf)
                    install_file(
                        recovery_conf,
                        os.path.join(postgresql_cluster_dir, 'recovery.conf'),
                        owner="postgres", group="postgres")
                    postgresql_restart()
                    break

    if not local_state.has_key('cluster_name'):
        local_state['cluster_name'] = 'main'
        local_state.save()


def user_name(relid, remote_unit, admin=False, schema=False):
    def sanitize(s):
        s = s.replace(':', '_')
        s = s.replace('-', '_')
        s = s.replace('/', '_')
        s = s.replace('"', '_')
        s = s.replace("'", '_')
        return s
    # Per Bug #1160530, don't append the remote unit number to the user name.
    components = [sanitize(relid), sanitize(re.split("/",remote_unit)[0])]
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
    password = get_password(user)
    if password is None:
        password = pwgen()
        set_password(user, password)
    if user_exists(user):
        action = ["ALTER ROLE"]
    else:
        action = ["CREATE ROLE"]
    action.append('"{}"'.format(user))
    action.append('WITH LOGIN')
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
    run_sql_as_postgres(sql, password)
    return password


def grant_roles(user, roles):
    # Delete previous roles
    sql = ("DELETE FROM pg_auth_members WHERE member IN ("
           "SELECT oid FROM pg_roles WHERE rolname = %s)")
    run_sql_as_postgres(sql, user)

    for role in roles:
        ensure_role(role)
        sql = "GRANT {} to {}".format(role, user)
        run_sql_as_postgres(sql)


def ensure_role(role):
    sql = "SELECT oid FROM pg_roles WHERE rolname = %s"
    if run_select_as_postgres(sql, role)[0] != 0:
        # role already exists
        pass
    else:
        sql = "CREATE ROLE {} INHERIT NOLOGIN".format(role)
        run_sql_as_postgres(sql)


def ensure_database(user, schema_user, database):
    sql = "SELECT datname FROM pg_database WHERE datname = %s"
    if run_select_as_postgres(sql, database)[0] != 0:
        # DB already exists
        pass
    else:
        sql = "CREATE DATABASE {}".format(database)
        run_sql_as_postgres(sql)
    sql = "GRANT ALL PRIVILEGES ON DATABASE {} TO {}".format(database,
        schema_user)
    run_sql_as_postgres(sql)
    sql = "GRANT CONNECT ON DATABASE {} TO {}".format(database, user)
    run_sql_as_postgres(sql)


def get_relation_host():
    remote_host = run("relation-get ip")
    if not remote_host:
        # remote unit $JUJU_REMOTE_UNIT uses deprecated 'ip=' component of
        # interface.
        remote_host = run("relation-get private-address")
    return remote_host


def get_unit_host():
    this_host = run("unit-get private-address")
    return this_host.strip()


def db_relation_joined_changed(user, database, roles):
    if not user_exists(user):
        password = create_user(user)
        run("relation-set user='%s' password='%s'" % (user, password))
    grant_roles(user, roles)
    schema_user = "{}_schema".format(user)
    if not user_exists(schema_user):
        schema_password = create_user(schema_user)
        run("relation-set schema_user='%s' schema_password='%s'" % (
            schema_user, schema_password))
    ensure_database(user, schema_user, database)
    config_data = config_get()
    host = get_unit_host()
    run("relation-set host='%s' database='%s' port='%s'" % (
        host, database, config_data["listen_port"]))
    generate_postgresql_hba(postgresql_hba)


def db_admin_relation_joined_changed(user, database='all'):
    if not user_exists(user):
        password = create_user(user, admin=True)
        run("relation-set user='%s' password='%s'" % (user, password))
    host = get_unit_host()
    config_data = config_get()
    run("relation-set host='%s' port='%s'" % (
        host, config_data["listen_port"]))
    generate_postgresql_hba(postgresql_hba)


def db_relation_broken(user, database):
    sql = "REVOKE ALL PRIVILEGES ON {} FROM {}".format(database, user)
    run_sql_as_postgres(sql)
    sql = "REVOKE ALL PRIVILEGES ON {} FROM {}_schema".format(database, user)
    run_sql_as_postgres(sql)


def db_admin_relation_broken(user):
    sql = "ALTER USER {} NOSUPERUSER".format(user)
    run_sql_as_postgres(sql)
    generate_postgresql_hba(postgresql_hba)


def TODO(msg):
    juju_log(MSG_WARNING, 'TODO> %s' % msg)


def add_extra_repos():
    extra_repos = config_get('extra_archives')
    extra_repos_added = local_state.setdefault('extra_repos_added', set())
    if extra_repos:
        repos_added = False
        for repo in extra_repos.split():
            if repo not in extra_repos_added:
                run("add-apt-repository --yes '{}'".format(repo))
                extra_repos_added.add(repo)
                repos_added = True
        if repos_added:
            run('apt-get update')
            local_state.save()


def ensure_local_ssh():
    """Generate SSH keys for postgres user.

    The public key is stored in public_ssh_key on the relation.

    Bidirectional SSH access is required by repmgr.
    """
    comment = 'repmgr key for {}'.format(os.environ['JUJU_UNIT_NAME'])
    if not os.path.isdir(postgres_ssh_dir):
        install_dir(postgres_ssh_dir, "postgres", "postgres", 0700)
    if not os.path.exists(postgres_ssh_private_key):
        run("sudo -u postgres -H ssh-keygen -q -t rsa -C '{}' -N '' "
            "-f '{}'".format(comment, postgres_ssh_private_key))
    public_key = open(postgres_ssh_public_key, 'r').read().strip()
    host_key = open('/etc/ssh/ssh_host_ecdsa_key.pub').read().strip()
    local_state['public_ssh_key'] = public_key
    local_state['ssh_host_key'] = host_key
    local_state.publish()


def authorize_remote_ssh():
    """Generate the SSH authorized_keys file."""
    authorized_units = set()
    authorized_keys = set()
    known_hosts = set()
    for relid in relation_ids(relation_types=replication_relation_types):
        for unit in relation_list(relid):
            relation = relation_get(unit_name=unit, relation_id=relid)
            public_key = relation.get('public_ssh_key', None)
            if public_key:
                authorized_units.add(unit)
                authorized_keys.add(public_key)
                known_hosts.add('{} {}'.format(
                    relation['private-address'], relation['ssh_host_key']))

    # Generate known_hosts
    install_file(
        '\n'.join(known_hosts), postgres_ssh_known_hosts,
        owner="postgres", group="postgres", mode=0o644)

    # Generate authorized_keys
    install_file(
        '\n'.join(authorized_keys), postgres_ssh_authorized_keys,
        owner="postgres", group="postgres", mode=0o400)

    # Publish details, so relation knows they have been granted access.
    local_state['authorized'] = authorized_units
    local_state.publish()


def generate_pgpass():
    passwords = {}

    # Replication
    for relid in relation_ids(relation_types=['replication', 'master']):
        for unit in relation_list(relid):
            relation = relation_get(unit_name=unit, relation_id=relid)

            if relation.get('state', None) == 'master':
                replication_password = relation.get('replication_password', '')
                if replication_password:
                    passwords['juju_replication'] = replication_password

    if passwords:
        pgpass = '\n'.join(
            "*:*:*:{}:{}".format(username, password)
                for username, password in passwords.items())
        install_file(
            pgpass, charm_pgpass,
            owner="postgres", group="postgres", mode=0o400)


def drop_database(dbname, warn=True):
    import psycopg2
    timeout = 120
    now = time.time()
    while True:
        try:
            db_cursor(autocommit=True).execute(
                'DROP DATABASE IF EXISTS "{}"'.format(dbname))
        except psycopg2.Error:
            if time.time() > now + timeout:
                if warn:
                    juju_log(
                        MSG_WARNING, "Unable to drop database %s" % dbname)
                else:
                    raise
            time.sleep(0.5)
        else:
            break


def replication_gc():
    """Remove old nodes from the repmgr database, tear down if no slaves"""
    wanted_units = []
    for relid in relation_ids(replication_relation_types):
        wanted_units.extend(relation_list(relid))

    # If there are replication relationships, trash the local repmgr setup.
    if not wanted_units:
        # Restore a hot standby to a standalone configuration.
        if postgresql_is_in_recovery():
            pg_ctl = os.path.join(postgresql_bin_dir, 'pg_ctl')
            run("sudo -u postgres {} promote -D '{}'".format(
                pg_ctl, postgresql_cluster_dir))

        if os.path.exists(charm_pgpass):
            os.unlink(charm_pgpass)

        local_state['state'] = 'standalone'


def is_master():
    '''True if we are, or should be, the master.

    Return True if I am the active master, or if neither myself nor
    the remote unit is and I win an election.
    '''
    master_relation_ids = relation_ids(relation_types=['master'])
    slave_relation_ids = relation_ids(relation_types=['slave'])
    if master_relation_ids and slave_relation_ids:
        # Both master and slave relations, so an attempt has been made
        # to set up cascading replication. This is not yet supported in
        # PostgreSQL, so we cannot support it either. Unfortunately,
        # there is no way yet to inform juju about this so we just have
        # to leave the impossible relation in a broken state.
        juju_log(
            MSG_CRITICAL,
            "Unable to create relationship. "
            "Cascading replication not supported.")
        raise SystemExit(1)

    if slave_relation_ids:
        # I'm explicitly the slave in a master/slave relationship.
        # No units in my service can be a master.
        juju_log(MSG_DEBUG, "In a slave relation, so I'm a slave")
        return False

    # Do I think I'm the master?
    if local_state['state'] == 'master':
        juju_log(MSG_DEBUG, "I already believe I am the master")
        return True

    # Lets see what out peer group thinks.
    peer_units = set()
    peer_host = {}  # Cache of addresses for peer units.
    peer_authorized = {}  # True if the peer unit has authorized us.
    for relid in relation_ids(relation_types=['replication']):
        # If there are any other peers claiming to be the master, then I am
        # not the master.
        for unit in relation_list(relid):
            relation = relation_get(unit_name=unit, relation_id=relid)
            peer_units.add(unit)
            peer_host[unit] = relation['private-address']
            peer_authorized[unit] = False
            for a in relation.get('authorized', '').split():
                if a == os.environ['JUJU_UNIT_NAME']:
                    peer_authorized[unit] = True
                    break
            if relation.get('state', None) == 'master':
                juju_log(MSG_DEBUG, "Found a master in peers, so I'm a slave")
                return False

    # Are there other units? Maybe we are the only one left in the
    # various master/slave/replication relationships.
    alone = True
    for relid in relation_ids(relation_types=replication_relation_types):
        if relation_list(relid):
            alone = False
            break
    if alone:
        juju_log(MSG_DEBUG, "I am alone, no point being a master")
        return False

    # If the peer group has lost a master, the hot standby with the
    # least lag should be the new master. Perhaps that is me?
    my_offset = postgresql_wal_received_offset(
        host=None, db='postgres', user='postgres')
    if my_offset is not None:
        # Store the offset, unit number & unit in a tuple for easy
        # sorting.
        my_unit = os.environ['JUJU_UNIT_NAME']
        offsets = set([(my_offset, int(my_unit.split('/')[1]), my_unit)])
        for unit in peer_units:
            if peer_authorized[unit]:
                # If the peer has not yet got as far as authorizing us,
                # it will not be further in sync with the master than
                # us.
                host = peer_host[unit]
                offset = postgresql_wal_received_offset(host)
                if offset is not None:
                    if offset < my_offset:
                        juju_log(
                            MSG_DEBUG,
                            "A peer is less lagged than me, so I'm a slave")
                        return False  # Short circuit.
                    offsets.add((offset, int(unit.split('/')[1]), unit))
            else:
                juju_log(
                    MSG_DEBUG,
                    "Unable to check {} wal offset - unauthorized".format(
                        unit))
        best_unit = sorted(offsets)[0][2]  # Lowest number wins a tie.
        if best_unit == my_unit:
            juju_log(MSG_DEBUG, "I won the lag tie breaker and am the master")
            return True
        else:
            juju_log(MSG_DEBUG, "I lost the lag tie breaker and am a slave")
            return False

    # There are no masters, so we need an election within this peer
    # relation. Lowest unit number wins and gets to be the master.
    remote_nums = sorted(int(unit.split('/')[1]) for unit in peer_units)
    if not remote_nums:
        return True  # Only unit in a service in a master relationship.
    my_num = int(os.environ['JUJU_UNIT_NAME'].split('/')[1])
    if my_num < remote_nums[0]:
        juju_log(MSG_DEBUG, "Lowest unit so I'm the master")
        return True
    else:
        juju_log(MSG_DEBUG, "Not the lowest unit so I'm a slave")
        return False


def replication_relation_changed():
    ## Without repmgr, we no longer need SSH authorization
    ## Leaving the code around for now in case we want it as the log
    ## shipping transport.
    ##
    ## ensure_local_ssh()  # Generate SSH key and publish details
    ## authorize_remote_ssh()  # Authorize relationship SSH keys.
    config_changed(postgresql_config)  # Ensure minimal replication settings.

    # Now that pg_hba.conf has been regenerated and loaded, inform related
    # units that they have been granted replication access.
    authorized_units = set()
    for relid in relation_ids(relation_types=replication_relation_types):
        for unit in relation_list(relid):
            authorized_units.add(unit)
    local_state['authorized'] = authorized_units
    local_state.publish()

    relation = relation_get()

    juju_log(MSG_INFO, "Current state is {}".format(local_state['state']))

    if is_master():
        if local_state['state'] == 'standalone':  # Initial setup of a master.
            juju_log(MSG_INFO, "I am standalone and becoming the master")
            # The juju_replication user connects as both a streaming
            # replication connection and as a superuser to check
            # replication status.
            # TODO: Does it? We can use explicit grants to remove the
            # superuser requirement now.
            replication_password = create_user(
                'juju_replication', admin=True, replication=True)
            local_state['state'] = 'master'
            local_state['replication_password'] = replication_password
            juju_log(
                MSG_INFO,
                "Publishing replication connection details to hot standbys")
            local_state.publish()

        elif local_state['state'] == 'master':  # Already the master.
            juju_log(MSG_INFO, "I am the master")
            replication_gc()

        elif local_state['state'] == 'hot standby':  # I've been promoted
            juju_log(MSG_INFO, "I am a hot standby being promoted to master")
            # Rather than using 'pg_ctl promote', we do the promotion
            # this way to avoid creating a timeline change. Switch this
            # to using 'pg_ctl promote' once PostgreSQL propagates
            # timeline changes via streaming replication.
            os.unlink(os.path.join(postgresql_cluster_dir, 'recovery.conf'))
            postgresql_restart()
            local_state['state'] = 'master'
            local_state.publish()
            replication_gc()

        else:
            raise AssertionError(
                "Unknown state {}".format(local_state['state']))

    else:  # A hot standby, now or soon.
        remote_is_master = (relation.get('state', '') == 'master')

        remote_has_authorized = False
        for unit in relation.get('authorized', '').split():
            if unit == os.environ['JUJU_UNIT_NAME']:
                remote_has_authorized = True

        if remote_is_master and remote_has_authorized:
            replication_password = relation['replication_password']
            if local_state.get(
                'replication_password', None) != replication_password:
                local_state['replication_password'] = replication_password
                generate_pgpass()

            slave_relation_ids = relation_ids(relation_types=['slave'])
            if local_state['state'] == 'standalone' or slave_relation_ids:
                # Building a fresh hot standby. Either a new node
                # ('standalone'), or a unit in a service that is being
                # attached as a slave.
                juju_log(MSG_INFO, "I am becoming a hot standby")
                # Republish the replication password in case we failover to
                # being the master in the future. Bug #806098.
                local_state[
                    'replication_password'] = relation['replication_password']
                local_state.publish()

                # We are just joining replication, and have found a
                # master. Clone and follow it.
                generate_pgpass()

                # Before we start destroying anything, ensure that the
                # master is contactable.
                wait_for_db(
                    db='postgres', user='juju_replication',
                    host=relation['private-address'])

                clone(
                    os.environ['JUJU_REMOTE_UNIT'],
                    relation['private-address'])

                local_state['state'] = 'hot standby'
                local_state['following'] = os.environ['JUJU_REMOTE_UNIT']
                local_state.publish()

            elif local_state['state'] == 'hot standby':
                juju_log(MSG_INFO, "I am a hot standby")
                if local_state['following'] != os.environ['JUJU_REMOTE_UNIT']:
                    juju_log(
                        MSG_INFO, "New master {} found. Following".format(
                            os.environ['JUJU_REMOTE_UNIT']))
                    recovery_conf = dedent("""\
                        standby_mode = on
                        primary_conninfo = 'host={} user=juju_replication'
                        """.format(relation['private-address']))
                    juju_log(MSG_DEBUG, recovery_conf)
                    install_file(
                        recovery_conf,
                        os.path.join(postgresql_cluster_dir, 'recovery.conf'),
                        owner="postgres", group="postgres")
                    postgresql_restart()
                    local_state['following'] = os.environ['JUJU_REMOTE_UNIT']
                    local_state.save()

            else:
                raise AssertionError(
                    "Unknown state {}".format(local_state['state']))

        elif remote_is_master:
            juju_log(
                MSG_INFO,
                "I am waiting for a master to authorize me")


def replication_relation_broken():
    config_changed(postgresql_config)
    authorize_remote_ssh()


def clone(master_unit, master_host):
    postgresql_stop()
    juju_log(MSG_INFO, "Cloning master {}".format(master_unit))

    cmd = [
        'sudo', '-E', '-u', 'postgres',  # -E needed to locate pgpass file.
        'pg_basebackup', '-D', postgresql_cluster_dir,
        '--xlog', '--checkpoint=fast', '--no-password',
        '-h', master_host, '-p', '5432', '--username=juju_replication',
        ]
    juju_log(MSG_DEBUG, ' '.join(cmd))
    if os.path.isdir(postgresql_cluster_dir):
        shutil.rmtree(postgresql_cluster_dir)
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        juju_log(MSG_DEBUG, output)
        # Debian by default expects SSL certificates in the datadir.
        os.symlink(
            '/etc/ssl/certs/ssl-cert-snakeoil.pem',
            os.path.join(postgresql_cluster_dir, 'server.crt'))
        os.symlink(
            '/etc/ssl/private/ssl-cert-snakeoil.key',
            os.path.join(postgresql_cluster_dir, 'server.key'))
        recovery_conf = Template(
            open("templates/recovery.conf.tmpl").read()).render({
                'host': master_host,
                'password': local_state['replication_password']})
        juju_log(MSG_DEBUG, recovery_conf)
        install_file(
            recovery_conf,
            os.path.join(postgresql_cluster_dir, 'recovery.conf'),
            owner="postgres", group="postgres")
    except subprocess.CalledProcessError, x:
        # We failed, and this cluster is broken. Rebuild a
        # working cluster so start/stop etc. works and we
        # can retry hooks again. Even assuming the charm is
        # functioning correctly, the clone may still fail
        # due to eg. lack of disk space.
        juju_log(MSG_ERROR, "Clone failed, db cluster destroyed")
        juju_log(MSG_ERROR, x.output)
        if os.path.exists(postgresql_cluster_dir):
            shutil.rmtree(postgresql_cluster_dir)
        if os.path.exists(postgresql_config_dir):
            shutil.rmtree(postgresql_config_dir)
        pg_createcluster()
        config_changed(postgresql_config)
        raise
    finally:
        postgresql_start()
        wait_for_db()


def slave_count():
    num_slaves = 0
    for relid in relation_ids(relation_types=replication_relation_types):
        num_slaves += len(relation_list(relid))
    return num_slaves

def pg_cluster_exists():
    raise NotImplementedError()

def pg_createcluster():
    run("pg_createcluster --locale='{}' --encoding='{}' {} {}".format(
        config_data['locale'], config_data['encoding'],
        version, local_state['cluster_name']))


def postgresql_is_in_recovery():
    cur = db_cursor(autocommit=True)
    cur.execute("SELECT pg_is_in_recovery()")
    return cur.fetchone()[0]


def postgresql_is_in_backup_mode():
    return os.path.exists(
        os.path.join(postgresql_cluster_dir, 'backup_label'))


def postgresql_wal_received_offset(
    host, db='postgres', user='juju_replication'):
    cur = db_cursor(autocommit=True, db=db, user=user, host=host)
    cur.execute('SELECT pg_is_in_recovery(), pg_last_xlog_receive_location()')
    is_in_recovery, xlog_received = cur.fetchone()
    if is_in_recovery:
        return wal_location_to_bytes(xlog_received)
    return None


def wal_location_to_bytes(wal_location):
    """Convert WAL + offset to num bytes, so they can be compared."""
    logid, offset = wal_location.split('/')
    return int(logid, 16) * 16 * 1024 * 1024 * 255 + int(offset, 16)


def wait_for_db(timeout=120, db='template1', user='postgres', host=None):
    '''Wait until the db is fully up.'''
    db_cursor(db=db, user=user, host=host, timeout=timeout)


def update_nrpe_checks():
    config_data = config_get()
    try:
        nagios_uid = getpwnam('nagios').pw_uid
        nagios_gid = getgrnam('nagios').gr_gid
    except:
        juju_log(MSG_DEBUG, "Nagios user not set up. Exiting.")
        return

    unit_name = os.environ['JUJU_UNIT_NAME'].replace('/', '-')
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
        loader=FileSystemLoader(os.path.join(os.environ['CHARM_DIR'],
        'templates')))
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
            "command[check_pgsql]=/usr/lib/nagios/plugins/check_pgsql -p {}"
            .format(config_data['listen_port']))
    # pgsql backups
    nrpe_check_file = '/etc/nagios/nrpe.d/check_pgsql_backups.cfg'
    backup_log = "%s/backups.log".format(postgresql_logs_dir)
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
        subprocess.call(['service', 'nagios-nrpe-server', 'reload'])

###############################################################################
# Global variables
###############################################################################
config_data = config_get()
local_state = State('local_state.pickle')
version = config_data['version']
postgresql_data_dir = "/var/lib/postgresql"
postgresql_cluster_dir = os.path.join(
    postgresql_data_dir, version, local_state['cluster_name'])
postgresql_bin_dir = os.path.join('/usr/lib/postgresql', version, 'bin')
postgresql_config_dir = os.path.join(
    "/etc/postgresql", version, local_state['cluster_name'])
postgresql_config = os.path.join(postgresql_config_dir, "postgresql.conf")
postgresql_ident = os.path.join(postgresql_config_dir, "pg_ident.conf")
postgresql_hba = os.path.join(postgresql_config_dir, "pg_hba.conf")
postgresql_crontab = "/etc/cron.d/postgresql"
postgresql_service_config_dir = "/var/run/postgresql"
postgresql_scripts_dir = os.path.join(postgresql_data_dir, 'scripts')
postgresql_backups_dir = os.path.join(postgresql_data_dir, 'backups')
postgresql_logs_dir = os.path.join(postgresql_data_dir, 'logs')
postgres_ssh_dir = os.path.expanduser('~postgres/.ssh')
postgres_ssh_public_key = os.path.join(postgres_ssh_dir, 'id_rsa.pub')
postgres_ssh_private_key = os.path.join(postgres_ssh_dir, 'id_rsa')
postgres_ssh_authorized_keys = os.path.join(postgres_ssh_dir, 'authorized_keys')
postgres_ssh_known_hosts = os.path.join(postgres_ssh_dir, 'known_hosts')
hook_name = os.path.basename(sys.argv[0])
replication_relation_types = ['master', 'slave', 'replication']
local_state = State('local_state.pickle')
charm_pgpass = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', 'pgpass'))

# Hooks, running as root, need to be pointed at the correct .pgpass.
os.environ['PGPASSFILE'] = charm_pgpass


###############################################################################
# Main section
###############################################################################
def main():
    juju_log(MSG_INFO, "Running {} hook".format(hook_name))
    if hook_name == "install":
        install()

    elif hook_name == "config-changed":
        config_changed(postgresql_config)

    elif hook_name == "upgrade-charm":
        install(run_pre=False)
        upgrade_charm()

    elif hook_name == "start":
        if not postgresql_restart():
            raise SystemExit(1)

    elif hook_name == "stop":
        if not postgresql_stop():
            raise SystemExit(1)

    elif hook_name in ["db-relation-joined", "db-relation-changed"]:
        roles = filter(None, relation_get('roles').split(","))
        database = relation_get('database')
        if database == '':
            # Missing some information. We expect it to appear in a
            # future call to the hook.
            juju_log(MSG_WARNING, "No database set in relation, exiting")
            sys.exit(0)
        user = \
            user_name(os.environ['JUJU_RELATION_ID'],
                os.environ['JUJU_REMOTE_UNIT'])
        if user != '' and database != '':
            db_relation_joined_changed(user, database, roles)

    elif hook_name == "db-relation-broken":
        database = relation_get('database')
        user = \
            user_name(os.environ['JUJU_RELATION_ID'],
                os.environ['JUJU_REMOTE_UNIT'])
        db_relation_broken(user, database)

    elif hook_name in [
        "db-admin-relation-joined", "db-admin-relation-changed"]:
        user = user_name(os.environ['JUJU_RELATION_ID'],
            os.environ['JUJU_REMOTE_UNIT'], admin=True)
        db_admin_relation_joined_changed(user, 'all')

    elif hook_name == "db-admin-relation-broken":
        # XXX: Fix: relation is not set when it is already broken
        # cannot determine the user name
        user = user_name(os.environ['JUJU_RELATION_ID'],
            os.environ['JUJU_REMOTE_UNIT'], admin=True)
        db_admin_relation_broken(user)

    elif hook_name == "nrpe-external-master-relation-changed":
        update_nrpe_checks()

    elif hook_name in (
        'master-relation-joined', 'master-relation-changed',
        'slave-relation-joined', 'slave-relation-changed',
        'replication-relation-joined', 'replication-relation-changed'):
        replication_relation_changed()

    elif hook_name in (
        'master-relation-broken', 'slave-relation-broken',
        'replication-relation-broken', 'replication-relation-departed'):
        replication_relation_broken()

    #-------- persistent-storage-relation-joined,
    #         persistent-storage-relation-changed
    #elif hook_name in ["persistent-storage-relation-joined",
    #    "persistent-storage-relation-changed"]:
    #    persistent_storage_relation_joined_changed()
    #-------- persistent-storage-relation-broken
    #elif hook_name == "persistent-storage-relation-broken":
    #    persistent_storage_relation_broken()
    else:
        print "Unknown hook {}".format(hook_name)
        raise SystemExit(1)


if __name__ == '__main__':
    raise SystemExit(main())
