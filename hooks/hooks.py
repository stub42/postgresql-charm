#!/usr/bin/env python

from shelltoolbox import su
from helpers import (
    log,
    log_entry,
    log_exit,
    run,
)
import commands
import json
import glob
import os
import random
import re
import socket
import string
import subprocess
import sys
import yaml


###############################################################################
# Supporting functions
###############################################################################


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
    pg_config = Template(open("templates/postgresql.conf.tmpl").read(), searchList=[config_data])
    with open(postgresql_config, 'w') as postgres_config:
        postgres_config.write(str(pg_config))


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
def pwgen(pwd_length=20):
    alphanumeric_chars = [l for l in (string.letters + string.digits) \
    if l not in 'Iil0oO1']
    random_chars = [random.choice(alphanumeric_chars) \
    for i in range(pwd_length)]
    return(''.join(random_chars))

def run_sql_as_postgres(sql)
    run("""sudo -su postgres psql -c "%s" """ % sql)

###############################################################################
# Hook functions
###############################################################################
def config_changed(postgresql_config):
    current_service_port = get_service_port(postgresql_config)
    create_postgresql_config(postgresql_config)
    updated_service_port = config_data["listen_port"]
    update_service_port(current_service_port, updated_service_port)
    if config_data["config_change_command"] in ["reload", "restart"]:
        retVal = subprocess.call(['service', 'postgresql', config_data["config_change_command"]])

def install():
    for package in ["postgresql", "pwgen", "python-cheetah", "syslinux"]:
        apt_get_install(package)
    open_port(5432)

def get_db_password(user, database)
    if os.path.exists("/var/lib/juju/pgsql.%s.%s.password" % (database, user)):
        f = open("/var/lib/juju/pgsql.%s.%s.password" % (database, user))
        return f.read()
    else:
        password = pwgen()
        password_file = open("/var/lib/juju/pgsql.%s.%s.password" % (database, user), "w")
        password_file.write(password)
        password_file.close()
        return password

def ensure_db_user(user, password):
    sql = "SELECT rolname FROM pg_roles WHERE rolname = '%s'" % (user, )
    if run_sql_as_postgres(sql):
        # User already exists
        pass
    else:
        sql = "CREATE USER %s WITH SUPERUSER PASSWORD '%s'" % (user, password)
        run_sql_as_postgres(sql)

def ensure_database(user, database):
    sql = "SELECT datname FROM pg_database WHERE datname = '%s'" % (database)
    if run_sql_as_postgres(sql):
        # DB already exists
        pass
    else:
        sql = "CREATE DATABASE %s OWNER %s" % (database, user)
        run_sql_as_postgres(sql)
        sql = "GRANT ALL PRIVILEGES ON %s TO %s" % (database, user)
        run_sql_as_postgres(sql)

def db_relation_joined(user, database):
    password = get_db_password(user, database)
    ensure_db_user(user, password)
    ensure_database(user, database)
    host = run("gethostip $ip | awk '{print $2}'")
    run("relation-set host=%s user=%s password=%s database=%s" % (host, user,
        password, database))

def db_relation_broken(user, database):
    # Need to handle "all" value
    sql = "REVOKE ALL PRIVILEGES ON DATABASE %s FROM %s" % (user, database)
    run_sql_as_postgres(sql)

def db_relation_broken_admin(user):
    sql = "ALTER USER %s NOCREATEDB NOCREATEUSER" % (user, )
    run_sql_as_postgres(sql)

def db_relation_changed(user, database):
    remote_host = run("relation-get ip")
    if not remote_host:
        # remote unit $JUJU_REMOTE_UNIT uses deprecated 'ip=' component of
        # interface.
        remote_host = run("relation-get private-address")
    if not remote_host:
        # remote host not set yet
        sys.exit(0)
    remote_ip = run("gethostip $remote_host | awk '{print $2}'")
    if re.search("%s.*%s.*%s" % (database, user, remote_ip), postgresql_hba):
        # we already have access
        pass
    else:
        pg_hba = Template(open("templates/pg_hba.conf.tmpl").read(), searchList=[config_data])
        with open(postgresql_hba, 'w') as postgres_hba:
        postgres_hba.write(str(pg_hba)) 
        

###############################################################################
# Global variables
###############################################################################
config_data = config_get()
version = config_data['version']
# We need this to evaluate if we're on a version greater than a given number
config_data['version_float'] = float(version)
cluster_name = config_data['cluster_name']
postgresql_config_dir = "/etc/postgresql"
postgresql_config = "%s/%s/%s/postgresql.conf" % (postgresql_config_dir, version, cluster_name)
postgresql_hba = "%s/%s/%s/hba.conf" % (postgresql_config_dir, version, cluster_name)
postgresql_service_config_dir = "/var/run/postgresql"
hook_name = os.path.basename(sys.argv[0])

###############################################################################
# Main section
###############################################################################
if hook_name == "install":
    install()
elif hook_name == "config-changed":
    from Cheetah.Template import Template
    config_changed(postgresql_config)
elif hook_name == "start":
    status, output = commands.getstatusoutput("service postgresql restart")
    if status != 0:
        status, output = commands.getstatusoutput("service postgresql start")
        if status != 0:
            sys.exit(status)
elif hook_name == "stop":
    status, output = commands.getstatusoutput("service postgresql stop")
    if status != 0:
        sys.exit(status)
elif hook_name == "db-relation-joined":
    db_relation_joined(user, database)
elif hook_name == "db-admin-relation-joined":
    db_relation_joined(user, "all")
elif hook_name == "db-relation-changed":
    db_relation_changed(user, database)
elif hook_name == "db-admin-relation-changed":
    db_relation_changed(user, "all")
elif hook_name == "db-relation-broken":
    db_relation_broken(user, database)
elif hook_name == "db-admin-relation-broken":
    db_relation_broken(user, "all")
    db_relation_broken_admin(user)
else:
    print "Unknown hook"
    sys.exit(1)
