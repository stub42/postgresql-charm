#!/usr/bin/env python
# vim: et ai ts=4 sw=4:

import commands
import cPickle as pickle
import glob
from grp import getgrnam
import json
import os.path
from pwd import getpwnam
import random
import re
import shutil
import socket
import string
import subprocess
import sys
from textwrap import dedent
import time
import yaml
from yaml.constructor import ConstructorError

from charmhelpers.core import hookenv

from charmhelpers.core.hookenv import (
    log, CRITICAL, ERROR, WARNING, INFO, DEBUG)


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
    log(msg, level)


class State(dict):
    """Encapsulate state common to the unit for republishing to relations."""
    def __init__(self, state_file):
        self._state_file = state_file
        self.load()

    def load(self):
        if os.path.exists(self._state_file):
            state = pickle.load(open(self._state_file, 'rb'))
        else:
            state = {}
        self.clear()

        self.update(state)

    def save(self):
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

        for relid in relation_ids(relation_types=['db', 'db-admin']):
            relation_set(client_state, relid)

        replication_state = dict(client_state)

        add(replication_state, 'public_ssh_key')
        add(replication_state, 'ssh_host_key')
        add(replication_state, 'replication_password')
        add(replication_state, 'wal_received_offset')
        add(replication_state, 'following')

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
    command = '/usr/bin/install -o {} -g {} -m {} -d {}'.format(
        owner, group, oct(mode), dirname)
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
    vc = "%s/%s" % (config_data["version"], config_data["cluster_name"])
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
    if 'saved_config' in local_state:
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

    if rc == 0 and 'saved_config' in local_state:
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
        data = json.loads(json_units)
        if data is not None:
            return data
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
def generate_postgresql_hba(postgresql_hba, user=None,
                            schema_user=None, database=None):

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

    relation_data = []
    for relid in relation_ids(relation_types=['db', 'db-admin']):
        local_relation = relation_get(
            unit_name=os.environ['JUJU_UNIT_NAME'], relation_id=relid)

        # We might see relations that have not yet been setup enough.
        # At a minimum, the relation-joined hook needs to have been run
        # on the server so we have information about the usernames and
        # databases to allow in.
        if 'user' not in local_relation:
            continue

        for unit in relation_list(relid):
            relation = relation_get(unit_name=unit, relation_id=relid)

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

            relation['private-address'] = munge_address(
                relation['private-address'])
            relation_data.append(relation)

    juju_log(MSG_INFO, str(relation_data))

    # Replication connections. Each unit needs to be able to connect to
    # every other unit's postgres database and the magic replication
    # database. It also needs to be able to connect to its own postgres
    # database.
    for relid in relation_ids(relation_types=replication_relation_types):
        for unit in relation_list(relid):
            relation = relation_get(unit_name=unit, relation_id=relid)
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
    for relid in relation_ids(relation_types=['replication']):
        local_replication = {'database': 'postgres',
                             'user': 'juju_replication',
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


def db_cursor(autocommit=False, db='template1', user='postgres',
              host=None, timeout=120):
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
        except psycopg2.Error, x:
            if time.time() > start + timeout:
                juju_log(
                    MSG_CRITICAL, "Database connection {!r} failed".format(
                        conn_str))
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
                juju_log(MSG_ERROR,
                         "volume_init_and_mount failed, "
                         "not applying changes")
                return False

        if not os.path.exists(data_directory_path):
            juju_log(MSG_CRITICAL,
                     "postgresql data dir = %s not found, "
                     "not applying changes." % data_directory_path)
            return False

        mount_point = volume_mount_point_from_volid(volid)
        new_pg_dir = os.path.join(mount_point, "postgresql")
        new_pg_version_cluster_dir = os.path.join(
            new_pg_dir, config_data["version"], config_data["cluster_name"])
        if not mount_point:
            juju_log(MSG_ERROR,
                     "invalid mount point from volid = \"%s\", "
                     "not applying changes." % mount_point)
            return False

        if ((os.path.islink(data_directory_path) and
             os.readlink(data_directory_path) == new_pg_version_cluster_dir and
             os.path.isdir(new_pg_version_cluster_dir))):
            juju_log(MSG_INFO,
                     "NOTICE: postgresql data dir '%s' already points "
                     "to '%s', skipping storage changes." %
                     (data_directory_path, new_pg_version_cluster_dir))
            juju_log(MSG_INFO,
                     "existing-symlink: to fix/avoid UID changes from "
                     "previous units, doing: "
                     "chown -R postgres:postgres %s" % new_pg_dir)
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
                     "after-symlink: to fix/avoid UID changes from "
                     "previous units, doing: "
                     "chown -R postgres:postgres %s" % new_pg_dir)
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
                 "Disabled and stopped postgresql service, "
                 "because of broken volume configuration - check "
                 "'volume-ephemeral-storage' and 'volume-map'")
        sys.exit(1)

    if volume_is_permanent(volid):
        ## config_changed_volume_apply will stop the service if it founds
        ## it necessary, ie: new volume setup
        if config_changed_volume_apply():
            enable_service_start("postgresql")
        else:
            disable_service_start("postgresql")
            postgresql_stop()
            mounts = volume_get_all_mounted()
            if mounts:
                juju_log(MSG_INFO, "FYI current mounted volumes: %s" % mounts)
            juju_log(MSG_ERROR,
                     "Disabled and stopped postgresql service "
                     "(config_changed_volume_apply failure)")
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

    packages = ["postgresql", "pwgen", "python-jinja2", "syslinux",
                "python-psycopg2", "postgresql-contrib", "postgresql-plpython",
                "postgresql-%s-debversion" % config_data["version"]]
    packages.extend(config_data["extra-packages"].split())
    apt_get_install(packages)

    if not 'state' in local_state:
        # Fresh installation. Because this function is invoked by both
        # the install hook and the upgrade-charm hook, we need to guard
        # any non-idempotent setup. We should probably fix this; it
        # seems rather fragile.
        local_state.setdefault('state', 'standalone')
        local_state.publish()

        # Drop the cluster created when the postgresql package was
        # installed, and rebuild it with the requested locale and encoding.
        run("pg_dropcluster --stop 9.1 main")
        run("pg_createcluster --locale='{}' --encoding='{}' 9.1 main".format(
            config_data['locale'], config_data['encoding']))

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

    snapshot_relations()


def upgrade_charm():
    # Detect if we are upgrading from the old charm that used repmgr for
    # replication.
    from_repmgr = not 'juju_replication' in local_state

    # Handle renaming of the repmgr user to juju_replication.
    if from_repmgr and local_state['state'] == 'master':
        replication_password = create_user(
            'juju_replication', admin=True, replication=True)
        local_state['replication_password'] = replication_password
        generate_pgpass()
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

    snapshot_relations()


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
        password = pwgen()
        set_password(user, password)
    if user_exists(user):
        action = ["ALTER ROLE"]
    else:
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


def grant_roles(user, roles):
    from psycopg2.extensions import AsIs

    # Delete previous roles
    sql = ("DELETE FROM pg_auth_members WHERE member IN ("
           "SELECT oid FROM pg_roles WHERE rolname = %s)")
    run_sql_as_postgres(sql, user)

    for role in roles:
        ensure_role(role)
        sql = "GRANT %s to %s"
        run_sql_as_postgres(sql, AsIs(quote_identifier(role)),
                            AsIs(quote_identifier(user)))


def ensure_role(role):
    from psycopg2.extensions import AsIs

    sql = "SELECT oid FROM pg_roles WHERE rolname = %s"
    if run_select_as_postgres(sql, role)[0] != 0:
        # role already exists
        pass
    else:
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

def db_relation_joined_changed(user, database, roles):
    if local_state['state'] not in ('master', 'standalone'):
        return

    log('{} unit publishing credentials'.format(local_state['state']))

    password = create_user(user)
    grant_roles(user, roles)
    schema_user = "{}_schema".format(user)
    schema_password = create_user(schema_user)
    ensure_database(user, schema_user, database)
    host = get_unit_host()
    port = config_get()["listen_port"]
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
    client_relations = ' '.join(
        hookenv.relation_ids('db') + hookenv.relation_ids('db-admin'))
    log("Client relations {}".format(client_relations))
    for relid in hookenv.relation_ids('replication'):
        hookenv.relation_set(relid, client_relations=client_relations)

    generate_postgresql_hba(postgresql_hba, user=user,
                            schema_user=schema_user,
                            database=database)

    snapshot_relations()


def db_admin_relation_joined_changed(user):
    if local_state['state'] not in ('master', 'standalone'):
        return

    log('{} unit publishing credentials'.format(local_state['state']))

    password = create_user(user, admin=True)
    host = get_unit_host()
    port = config_get()["listen_port"]
    state = local_state['state']  # master, hot standby, standalone

    # Publish connection details.
    connection_settings = dict(
        user=user, password=password,
        host=host, database='all', port=port, state=state)
    log("Connection settings {!r}".format(connection_settings), DEBUG)
    hookenv.relation_set(relation_settings=connection_settings)

    # Update the peer relation, notifying any hot standby units
    # to republish connection details to the client relation.
    client_relations = ' '.join(
        hookenv.relation_ids('db') + hookenv.relation_ids('db-admin'))
    log("Client relations {}".format(client_relations))
    for relid in hookenv.relation_ids('replication'):
        hookenv.relation_set(relid, client_relations=client_relations)

    generate_postgresql_hba(postgresql_hba)

    snapshot_relations()


def db_relation_broken():
    from psycopg2.extensions import AsIs

    relid = os.environ['JUJU_RELATION_ID']
    if relid not in local_state['relations']['db']:
        # This was to be a hot standby, but it had not yet got as far as
        # receiving and handling credentials from the master.
        log("db-relation-broken called before relation finished setup", DEBUG)
        return

    # The relation no longer exists, so we can't pull the database name
    # we used from there. Instead, we have to persist this information
    # ourselves.
    relation = local_state['relations']['db'][relid]
    unit_relation_data = relation[os.environ['JUJU_UNIT_NAME']]

    if local_state['state'] in ('master', 'standalone'):
        user = unit_relation_data.get('user', None)
        database = unit_relation_data['database']

        sql = "REVOKE ALL PRIVILEGES ON DATABASE %s FROM %s"
        run_sql_as_postgres(sql, AsIs(quote_identifier(database)),
                            AsIs(quote_identifier(user)))
        run_sql_as_postgres(sql, AsIs(quote_identifier(database)),
                            AsIs(quote_identifier(user + "_schema")))

    generate_postgresql_hba(postgresql_hba)

    # Cleanup our local state.
    snapshot_relations()


def db_admin_relation_broken():
    from psycopg2.extensions import AsIs

    if local_state['state'] in ('master', 'standalone'):
        user = hookenv.relation_get('user', unit=hookenv.local_unit())
        if user:
            sql = "ALTER USER %s NOSUPERUSER"
            run_sql_as_postgres(sql, AsIs(quote_identifier(user)))

    generate_postgresql_hba(postgresql_hba)

    # Cleanup our local state.
    snapshot_relations()


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


def authorized_by(unit):
    '''Return True if the peer has authorized our database connections.'''
    relation = hookenv.relation_get(unit=unit)
    authorized = relation.get('authorized', '').split()
    return hookenv.local_unit() in authorized


def promote_database():
    '''Take the database out of recovery mode.'''
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

    recovery_conf = dedent("""\
        standby_mode = on
        primary_conninfo = 'host={} user=juju_replication'
        """.format(master_relation['private-address']))
    log(recovery_conf, DEBUG)
    install_file(
        recovery_conf,
        os.path.join(postgresql_cluster_dir, 'recovery.conf'),
        owner="postgres", group="postgres")
    postgresql_restart()


def elected_master():
    """Return the unit that should be master, or None if we don't yet know."""
    if local_state['state'] == 'master':
        log("I am already the master", DEBUG)
        return hookenv.local_unit()

    if local_state['state'] == 'failover':
        former_master = local_state['following']
        log("Failover from {}".format(former_master))

        units_not_in_failover = set()
        for relid in hookenv.relation_ids('replication'):
            for unit in hookenv.related_units(relid):
                if unit == former_master:
                    log("Found dying master {}".format(unit), DEBUG)
                    continue

                relation = hookenv.relation_get(unit=unit, rid=relid)

                if relation['state'] == 'master':
                    log(
                        "{} says it already won the election".format(unit),
                        INFO)
                    return unit

                if relation['state'] != 'failover':
                    units_not_in_failover.add(unit)

        if units_not_in_failover:
            log("{} unaware of impending election. Deferring result.".format(
                " ".join(unit_sorted(units_not_in_failover))))
            return None

        log("Election in progress")
        winner = None
        winning_offset = -1
        for relid in hookenv.relation_ids('replication'):
            candidates = set(hookenv.related_units(relid))
            candidates.add(hookenv.local_unit())
            candidates.discard(former_master)
            # Sort the unit lists so we get consistent results in a tie
            # and lowest unit number wins.
            for unit in unit_sorted(candidates):
                relation = hookenv.relation_get(unit=unit, rid=relid)
                if int(relation['wal_received_offset']) > winning_offset:
                    winner = unit
                    winning_offset = int(relation['wal_received_offset'])

        # All remaining hot standbys are in failover mode and have
        # reported their wal_received_offset. We can declare victory.
        log("{} won the election as is the new master".format(winner))
        return winner

    # Maybe another peer thinks it is the master?
    for relid in hookenv.relation_ids('replication'):
        for unit in hookenv.related_units(relid):
            if hookenv.relation_get('state', unit, relid) == 'master':
                return unit

    # New peer group. Lowest numbered unit will be the master.
    for relid in hookenv.relation_ids('replication'):
        units = hookenv.related_units(relid) + [hookenv.local_unit()]
        master = unit_sorted(units)[0]
        log("New peer group. {} is the master".format(master))
        return master


def replication_relation_joined_changed():
    config_changed(postgresql_config)  # Ensure minimal replication settings.

    # Now that pg_hba.conf has been regenerated and loaded, inform related
    # units that they have been granted replication access.
    authorized_units = set()
    for relid in relation_ids(relation_types=replication_relation_types):
        for unit in relation_list(relid):
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
            local_state['state'] = 'master'

            # Publish credentials to hot standbys so they can connect.
            replication_password = create_user(
                'juju_replication', replication=True)
            local_state['replication_password'] = replication_password

        else:
            log("I am master and remain master")

    elif not authorized_by(master):
        log("I need to follow {} but am not yet authorized".format(master))

    elif 'following' not in local_state:
        log("Fresh unit. I will clone {} and become a hot standby".format(
            master))

        local_state['replication_password'] = hookenv.relation_get(
            'replication_password', master)
        generate_pgpass()

        # Before we start destroying anything, ensure that the
        # master is contactable.
        master_ip = hookenv.relation_get('private-address', master)
        wait_for_db(db='postgres', user='juju_replication', host=master_ip)

        clone_database(master, master_ip)

        local_state['state'] = 'hot standby'
        local_state['following'] = master
        if 'wal_received_offset' in local_state:
            del local_state['wal_received_offset']

    elif local_state['following'] == master:
        log("I am a hot standby already following {}".format(master))

    else:
        log("I am a hot standby following new master {}".format(master))
        local_state['replication_password'] = hookenv.relation_get(
            'replication_password', master)
        generate_pgpass()
        follow_database(master)
        if not local_state["paused_at_failover"]:
            run_sql_as_postgres("SELECT pg_xlog_replay_resume()")
        local_state['state'] = 'hot standby'
        local_state['following'] = master
        del local_state['wal_received_offset']
        del local_state['paused_at_failover']

    if local_state['state'] == 'hot standby':
        publish_hot_standby_credentials()
        generate_postgresql_hba(postgresql_hba)

    local_state.publish()


def publish_hot_standby_credentials():
    '''
    If a hot standby joins a client relation before the master
    unit, it was unable to publish connection details. However,
    when the master does join it updates the client_relations
    value in the peer relation causing the
    replication-relation-changed hook to be invoked. This gives us
    a second opertunity to publish connection details.

    This function is invoked from both the client and peer
    relation-changed hook. One of these will work depending on the order
    the master and hot standby joined the client relation.
    '''
    master = local_state['following']

    client_relations = hookenv.relation_get(
        'client_relations', master, relation_ids('replication')[0])

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
        connection_settings['host'] = get_unit_host()
        connection_settings['port'] = config_get()["listen_port"]
        connection_settings['state'] = local_state['state']

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

        log("Connection settings {!r}".format(connection_settings), DEBUG)
        hookenv.relation_set(
            client_relation, relation_settings=connection_settings)


def replication_relation_departed():
    '''A unit has left the replication peer group.'''
    remote_unit = hookenv.remote_unit()
    remote_relation = hookenv.relation_get()
    remote_state = remote_relation['state']

    assert remote_unit is not None

    log("{} {} has left the peer group".format(remote_state, remote_unit))

    # If the unit being removed was our master, we need to failover.
    if local_state.get('following', None) == remote_unit:

        # Prepare for failover. We need to suspend replication to ensure
        # that the replay point remains consistent throughout the
        # election, and publish that replay point. By comparing these
        # replay points, the most up to date hot standby can be
        # identified and promoted to the new master.
        cur = db_cursor(autocommit=True)
        cur.execute(
            "SELECT pg_is_xlog_replay_paused()")
        already_paused = cur.fetchone()[0]
        local_state["paused_at_failover"] = already_paused
        if not already_paused:
            cur.execute("SELECT pg_xlog_replay_pause()")
        local_state['state'] = 'failover'
        local_state['wal_received_offset'] = postgresql_wal_received_offset()

        # Now do nothing. We can't elect a new master until all the
        # remaining peers are in a steady state and have published their
        # wal_received_offset. Only then can we select a node to be
        # master.
        pass

    config_changed(postgresql_config)
    local_state.publish()


def replication_relation_broken():
    promote_database()
    local_state['state'] = 'standalone'
    local_state.save()
    if os.path.exists(charm_pgpass):
        os.unlink(charm_pgpass)
    config_changed(postgresql_config)


def clone_database(master_unit, master_host):
    postgresql_stop()
    juju_log(MSG_INFO, "Cloning master {}".format(master_unit))

    cmd = ['sudo', '-E', '-u', 'postgres',  # -E needed to locate pgpass file.
           'pg_basebackup', '-D', postgresql_cluster_dir,
           '--xlog', '--checkpoint=fast', '--no-password',
           '-h', master_host, '-p', '5432', '--username=juju_replication']
    juju_log(MSG_DEBUG, ' '.join(cmd))
    if os.path.isdir(postgresql_cluster_dir):
        shutil.rmtree(postgresql_cluster_dir)
    try:
        output = subprocess.check_output(cmd)
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
        run('pg_createcluster {} main'.format(version))
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


def postgresql_is_in_backup_mode():
    return os.path.exists(
        os.path.join(postgresql_cluster_dir, 'backup_label'))


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


def wait_for_db(timeout=120, db='template1', user='postgres', host=None):
    '''Wait until the db is fully up.'''
    db_cursor(db=db, user=user, host=host, timeout=timeout)


def unit_sorted(units):
    """Return a sorted list of unit names."""
    return sorted(
        units, lambda a, b: cmp(int(a.split('/')[-1]), int(b.split('/')[-1])))


def update_nrpe_checks():
    config_data = config_get()
    try:
        nagios_uid = getpwnam('nagios').pw_uid
        nagios_gid = getgrnam('nagios').gr_gid
    except:
        hookenv.log("Nagios user not set up.", hookenv.DEBUG)
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
            .format(config_data['listen_port']))
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
        subprocess.call(['service', 'nagios-nrpe-server', 'reload'])

###############################################################################
# Global variables
###############################################################################
config_data = config_get()
version = config_data['version']
cluster_name = config_data['cluster_name']
postgresql_data_dir = "/var/lib/postgresql"
postgresql_cluster_dir = os.path.join(
    postgresql_data_dir, version, cluster_name)
postgresql_bin_dir = os.path.join('/usr/lib/postgresql', version, 'bin')
postgresql_config_dir = os.path.join("/etc/postgresql", version, cluster_name)
postgresql_config = os.path.join(postgresql_config_dir, "postgresql.conf")
postgresql_ident = os.path.join(postgresql_config_dir, "pg_ident.conf")
postgresql_hba = os.path.join(postgresql_config_dir, "pg_hba.conf")
postgresql_crontab = "/etc/cron.d/postgresql"
postgresql_service_config_dir = "/var/run/postgresql"
postgresql_scripts_dir = os.path.join(postgresql_data_dir, 'scripts')
postgresql_backups_dir = (
    config_data['backup_dir'].strip() or
    os.path.join(postgresql_data_dir, 'backups'))
postgresql_logs_dir = os.path.join(postgresql_data_dir, 'logs')
postgres_ssh_dir = os.path.expanduser('~postgres/.ssh')
postgres_ssh_public_key = os.path.join(postgres_ssh_dir, 'id_rsa.pub')
postgres_ssh_private_key = os.path.join(postgres_ssh_dir, 'id_rsa')
postgres_ssh_authorized_keys = os.path.join(postgres_ssh_dir,
                                            'authorized_keys')
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
    # Hook and context overview. The various replication and client
    # hooks interact in complex ways.
    log("Running {} hook".format(hook_name), INFO)
    if 'JUJU_RELATION_NAME' in os.environ:
        log("Relation {} with {}".format(
            os.environ['JUJU_RELATION_NAME'],
            ' '.join(hookenv.related_units)), INFO)
        if os.environ.get('JUJU_REMOTE_UNIT', None):
            log(
                "Remote unit is {}".format(os.environ['JUJU_REMOTE_UNIT']),
                INFO)
        else:
            log("There is no remote unit")

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

    elif hook_name == "db-relation-joined":
        # By default, we create a database named after the remote
        # servicename. The remote service can override this by setting
        # the database property on the relation.
        database = os.environ['JUJU_REMOTE_UNIT'].split('/')[0]

        # Generate a unique username for this relation to use.
        user = user_name(
            os.environ['JUJU_RELATION_ID'], os.environ['JUJU_REMOTE_UNIT'])

        db_relation_joined_changed(user, database, [])  # No roles yet.

    elif hook_name == "db-relation-changed":
        roles = filter(None, (relation_get('roles') or '').split(","))

        # If the remote service has requested we use a particular database
        # name, honour that request.
        database = relation_get('database')
        if not database:
            database = relation_get('database', os.environ['JUJU_UNIT_NAME'])

        user = relation_get('user', os.environ['JUJU_UNIT_NAME'])
        if not user:
            user = user_name(
                os.environ['JUJU_RELATION_ID'], os.environ['JUJU_REMOTE_UNIT'])
        db_relation_joined_changed(user, database, roles)

    elif hook_name == "db-relation-broken":
        db_relation_broken()

    elif hook_name in ("db-admin-relation-joined",
                       "db-admin-relation-changed"):
        user = user_name(os.environ['JUJU_RELATION_ID'],
                         os.environ['JUJU_REMOTE_UNIT'], admin=True)
        db_admin_relation_joined_changed(user)

    elif hook_name == "db-admin-relation-broken":
        db_admin_relation_broken()

    elif hook_name == "nrpe-external-master-relation-changed":
        update_nrpe_checks()

    elif hook_name.startswith('master') or hook_name.startswith('slave'):
        raise NotImplementedError(hook_name)

    elif hook_name == 'replication-relation-joined':
        replication_relation_joined_changed()

    elif hook_name == 'replication-relation-changed':
        replication_relation_joined_changed()

    elif hook_name == 'replication-relation-departed':
        replication_relation_departed()

    elif hook_name == 'replication-relation-broken':
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
