#!/usr/bin/env python

import json
import os
import random
import re
import string
import subprocess
import sys
from pwd import getpwnam
from grp import getgrnam
# these modules are installed during the install hook
try:
    import psycopg2
    from jinja2 import Template
except ImportError:
    pass


###############################################################################
# Supporting functions
###############################################################################

#------------------------------------------------------------------------------
# run: Run a command, return the output
#------------------------------------------------------------------------------
def run(command):
    try:
        return subprocess.check_output(command, shell=True)
    except subprocess.CalledProcessError, e:
        sys.exit(e.returncode)


#------------------------------------------------------------------------------
# install_file: install a file resource. overwites existing files.
#------------------------------------------------------------------------------
def install_file(contents, dest, owner="root", group="root", mode=0600):
        uid = getpwnam(owner)[2]
        gid = getgrnam(group)[2]
        dest_fd = os.open(dest, os.O_WRONLY|os.O_CREAT, mode)
        os.fchown(dest_fd,uid,gid)
        with os.fdopen(dest_fd,'w') as destfile:
            destfile.write(str(contents))

#------------------------------------------------------------------------------
# install_dir: create a directory
#------------------------------------------------------------------------------
def install_dir(dirname, owner="root", group="root", mode=0700):
    command = ['/usr/bin/install']
    command.extend(['-o',owner])
    command.extend(['-g',group])
    command.extend(['-m',oct(mode)])
    command.extend(['-d', dirname])
    return run(command)

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
# relation_get:  Returns a dictionary containing the relation information
#                Optional parameters: scope, relation_id
#                scope:        limits the scope of the returned data to the
#                              desired item.
#                unit_name:    limits the data ( and optionally the scope )
#                              to the specified unit
#------------------------------------------------------------------------------
def relation_get(scope=None, unit_name=None):
    try:
        relation_cmd_line = ['relation-get', '--format=json']
        if scope is not None:
            relation_cmd_line.append(scope)
        else:
            relation_cmd_line.append('')
        if unit_name is not None:
            relation_cmd_line.append(unit_name)
        relation_data = json.loads(subprocess.check_output(relation_cmd_line))
    except:
        relation_data = None
    finally:
        return(relation_data)

#------------------------------------------------------------------------------
# apt_get_install( package ):  Installs a package
#------------------------------------------------------------------------------
def apt_get_install(packages=None):
    if packages is None:
        return(False)
    cmd_line = ['apt-get', '-y', 'install', '-qq']
    cmd_line.append(packages)
    return(subprocess.call(cmd_line))


#------------------------------------------------------------------------------
# create_postgresql_config:   Creates the postgresql.conf file
#------------------------------------------------------------------------------
def create_postgresql_config(postgresql_config):
    if config_data["performance_tuning"] == "auto":
        # Taken from http://wiki.postgresql.org/wiki/Tuning_Your_PostgreSQL_Server
        num_cpus = run("cat /proc/cpuinfo | grep processor | wc -l")
        total_ram = run("free -m | grep Mem | awk '{print $2}'")
        config_data["effective_cache_size"] = "%sMB" % (int( int(total_ram) * 0.75 ), )
        if total_ram > 1023:
            config_data["shared_buffers"] = "%sMB" % (int( int(total_ram) * 0.25 ), )
        else:
            config_data["shared_buffers"] = "%sMB" % (int( int(total_ram) * 0.15 ), )
        # XXX: This is very messy - should probably be a subordinate charm
        file = open("/etc/sysctl.d/50-postgresql.conf", "w")
        file.write("kernel.sem = 250 32000 100 1024\n")
        file.write("kernel.shmall = %s\n" % ((int(total_ram) * 1024 * 1024) + 1024),)
        file.write("kernel.shmmax = %s\n" % ((int(total_ram) * 1024 * 1024) + 1024),)
        file.close()
        run("sysctl -p /etc/sysctl.d/50-postgresql.conf")
    # Send config data to the template
    # Return it as pg_config
    pg_config = Template(open("templates/postgresql.conf.tmpl").read()).render(config_data)
    install_file(pg_config, postgresql_config)


#------------------------------------------------------------------------------
# create_postgresql_ident:  Creates the pg_ident.conf file
#------------------------------------------------------------------------------
def create_postgresql_ident(postgresql_ident):
    ident_data = {}
    pg_ident_template = Template(open("templates/pg_ident.conf.tmpl").read()).render(ident_data)
    with open(postgresql_ident, 'w') as ident_file:
        ident_file.write(str(pg_ident_template))


#------------------------------------------------------------------------------
# create_postgresql_hba:  Creates the pg_hba.conf file
#------------------------------------------------------------------------------
def create_postgresql_hba(postgresql_hba):
    hba_data = {}
    pg_hba_template = Template(open("templates/pg_hba.conf.tmpl").read()).render(hba_data)
    with open(postgresql_hba, 'w') as hba_file:
        hba_file.write(str(pg_hba_template))


#------------------------------------------------------------------------------
# update_postgresql_crontab:  Creates the postgresql crontab file
#------------------------------------------------------------------------------
def update_postgresql_crontab(postgresql_ident):
    crontab_data = {
        'backup_schedule': config_data["backup_schedule"],
        'scripts_dir': postgresql_scripts_dir,
        'databases': " ".join(database_names()),
    }
    crontab_template = Template(open("templates/postgres.cron.tmpl").read()).render(crontab_data)
    install_file(str(crontab_template),"/etc/cron.d/postgres", mode=0644)

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
    return(subprocess.call(['/usr/bin/open-port', "%d/%s" % \
    (int(port), protocol)]))


#------------------------------------------------------------------------------
# close_port:  Convenience function to close a port in juju to
#              unexpose a service
#------------------------------------------------------------------------------
def close_port(port=None, protocol="TCP"):
    if port is None:
        return(None)
    return(subprocess.call(['/usr/bin/close-port', "%d/%s" % \
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
        pwd_length = random.choice(range(20,30))
    alphanumeric_chars = [l for l in (string.letters + string.digits) \
    if l not in 'Iil0oO1']
    random_chars = [random.choice(alphanumeric_chars) \
    for i in range(pwd_length)]
    return(''.join(random_chars))

def db_cursor(autocommit=False):
    conn = psycopg2.connect("dbname=template1 user=postgres")
    conn.autocommit = autocommit
    try:
        return conn.cursor()
    except psycopg2.ProgrammingError:
        print sql
        raise


def run_sql_as_postgres(sql, *parameters):
    cur = db_cursor(autocommit=True)
    try:
        cur.execute(sql, parameters)
        return cur.statusmessage
    except psycopg2.ProgrammingError:
        print sql
        raise

def run_select_as_postgres(sql, *parameters):
    cur = db_cursor()
    cur.execute(sql, parameters)
    return (cur.rowcount, cur.fetchall())

###############################################################################
# Hook functions
###############################################################################
def config_changed(postgresql_config):
    current_service_port = get_service_port(postgresql_config)
    create_postgresql_config(postgresql_config)
    create_postgresql_hba(postgresql_hba)
    create_postgresql_ident(postgresql_ident)
    updated_service_port = config_data["listen_port"]
    update_service_port(current_service_port, updated_service_port)
    if config_data["config_change_command"] in ["reload", "restart"]:
        subprocess.call(['service', 'postgresql', config_data["config_change_command"]])

def token_sql_safe(value):
    # Only allow alphanumeric + underscore in database identifiers
    if re.search('[^A-Za-z0-9_]', value):
        return False
    return True

def install():
    for package in ["postgresql", "pwgen", "python-jinja2", "syslinux", "python-psycopg2"]:
        apt_get_install(package)
    from jinja2 import Template
    install_dir(postgresql_backups_dir,mode=0755)
    install_dir(postgresql_scripts_dir,mode=0755)
    paths = {
        'base_dir': postgresql_data_dir,
        'backup_dir': postgresql_backups_dir,
        'scripts_dir': postgresql_scripts_dir,
        'logs_dir': postgresql_logs_dir,
    }
    dump_script = Template(open("templates/dump-pg-db.tmpl").read()).render(paths)
    backup_job = Template(open("templates/pg_backup_job.tmpl").read()).render(paths)
    install_file(dump_script,'{}/dump-pg-db'.format(postgresql_scripts_dir),mode=0755)
    install_file(backup_job,'{}/pg_backup_job'.format(postgresql_scripts_dir),mode=0755)
    open_port(5432)

def user_name(admin=False):
    components = []
    components.append(os.environ['JUJU_RELATION_ID'].replace(":","_"))
    components.append(os.environ['JUJU_REMOTE_UNIT'].replace("/","_"))
    if admin:
        components.append("admin")
    return "_".join(components)

def database_names(admin=False):
    omit_tables = ['template0','template1']
    sql = "SELECT datname FROM pg_database WHERE datname NOT IN (" + ",".join(["%s"] * len(omit_tables)) + ")"
    return [t for (t,) in run_select_as_postgres(sql, *omit_tables)[1]]


def ensure_user(user, admin=False):
    sql = "SELECT rolname FROM pg_roles WHERE rolname = %s"
    password = pwgen()
    action = "CREATE"
    if run_select_as_postgres(sql, user)[0] != 0:
        action = "ALTER"
    if admin:
        sql = "{} USER {} SUPERUSER PASSWORD %s".format(action, user)
    else:
        sql = "{} USER {} PASSWORD %s".format(action, user)
    run_sql_as_postgres(sql, password)
    return password

def ensure_database(user, schema_user, database):
    sql = "SELECT datname FROM pg_database WHERE datname = %s"
    if run_select_as_postgres(sql, database)[0] != 0:
        # DB already exists
        pass
    else:
        sql = "CREATE DATABASE {} OWNER {}".format(database, schema_user)
        run_sql_as_postgres(sql)
    sql = "GRANT ALL PRIVILEGES ON DATABASE {} TO {}".format(database, schema_user)
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

def db_relation_joined_changed(user, database):
    password = ensure_user(user)
    schema_user = "{}_schema".format(user)
    schema_password = ensure_user(schema_user)
    ensure_database(user, schema_user, database)
    update_postgresql_crontab(postgresql_crontab)
    host = get_relation_host()
    run("relation-set host='{}' user='{}' password='{}' schema_user='{}' schema_password='{}' database='{}'".format(
                      host,     user,     password,     schema_user,     schema_password,     database))

def db_admin_relation_joined_changed(user):
    password = ensure_user(user)
    host = get_relation_host()
    run("relation-set host='{}' user='{}' password='{}'".format(
                      host,     user,     password))

def db_relation_broken(user, database):
    # Need to handle "all" value
    sql = "REVOKE ALL PRIVILEGES FROM {}_schema".format(user)
    run_sql_as_postgres(sql)
    sql = "REVOKE ALL PRIVILEGES FROM {}".format(user)
    run_sql_as_postgres(sql)
    update_postgresql_crontab(postgresql_crontab)

def db_admin_relation_broken(user):
    sql = "REVOKE ALL PRIVILEGES FROM {}".format(user)
    run_sql_as_postgres(sql)


###############################################################################
# Global variables
###############################################################################
config_data = config_get()
version = config_data['version']
# We need this to evaluate if we're on a version greater than a given number
config_data['version_float'] = float(version)
cluster_name = config_data['cluster_name']
postgresql_data_dir = "/var/lib/postgresql"
postgresql_config_dir = "/etc/postgresql"
postgresql_config = "%s/%s/%s/postgresql.conf" % (postgresql_config_dir, version, cluster_name)
postgresql_ident = "%s/%s/%s/pg_ident.conf" % (postgresql_config_dir, version, cluster_name)
postgresql_hba = "%s/%s/%s/pg_hba.conf" % (postgresql_config_dir, version, cluster_name)
postgresql_crontab = "/etc/cron.d/postgresql"
postgresql_service_config_dir = "/var/run/postgresql"
postgresql_scripts_dir = '{}/scripts'.format(postgresql_data_dir)
postgresql_backups_dir = '{}/backups'.format(postgresql_data_dir)
postgresql_logs_dir = '{}/logs'.format(postgresql_data_dir)
hook_name = os.path.basename(sys.argv[0])

###############################################################################
# Main section
###############################################################################
if hook_name == "install":
    install()
#-------- config-changed
elif hook_name == "config-changed":
    config_changed(postgresql_config)
#-------- start
elif hook_name == "start":
    try:
        subprocess.check_output(["service","postgresql","restart"])
    except subprocess.CalledProcessError, e:
        try:
            subprocess.check_output(["service","postgresql","start"])
        except subprocess.CalledProcessError, e:
            sys.exit(e.returncode)
#-------- stop
elif hook_name == "stop":
    try:
        subprocess.check_output(["service","postgresql","stop"])
    except subprocess.CalledProcessError, e:
        sys.exit(e.returncode)
#-------- db-relation-joined, db-relation-changed
elif hook_name in ["db-relation-joined","db-relation-changed"]:
    database = relation_get('database')
    if database == '':
        # Missing some information. We expect it to appear in a
        # future call to the hook.
        sys.exit(0)
    user = user_name()
    if user != '' and database != '':
        db_relation_joined_changed(user, database)
#-------- db-relation-broken
elif hook_name == "db-relation-broken":
    db_relation_broken(user, database)
#-------- db-admin-relation-joined, db-admin-relation-changed
elif hook_name in ["db-admin-relation-joined","db-admin-relation-changed"]:
    user = user_name(admin=True)
    db_admin_relation_joined_changed(user, "all")
#-------- db-admin-relation-broken
elif hook_name == "db-admin-relation-broken":
    user = user_name(admin=True)
    db_admin_relation_broken(user, "all")
#-------- persistent-storage-relation-joined, persistent-storage-relation-changed
elif hook_name in ["persistent-storage-relation-joined","persistent-storage-relation-changed"]:
    persistent_storage_relation_joined_changed()
#-------- persistent-storage-relation-broken
elif hook_name == "persistent-storage-relation-broken":
    persistent_storage_relation_broken()
else:
    print "Unknown hook"
    sys.exit(1)
