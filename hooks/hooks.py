#!/usr/bin/env python

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
from Cheetah.Template import Template


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
def get_service_port(postgresql_config_file="/etc/postgresql/9.1/main/postgresql.conf"):
    postgresql_config = load_postgresql_config(postgresql_config_file)
    if postgresql_config is None:
        return(None)
    return(re.findall("port.*=(.*)", haproxy_config))


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
def create_postgresql_config():
    # Send config data to the template
    # Return it as pg_config
    pg_config = Template(open("templates/postgresql.conf.tmp").read(), config_data)
    with open(postgresql_config, 'w') as postgres_config:
        postgres_config.write(pg_config)


#------------------------------------------------------------------------------
# load_postgresql_config:  Convenience function that loads (as a string) the
#                          current postgresql configuration file.
#                          Returns a string containing the postgresql config or
#                          None
#------------------------------------------------------------------------------
def load_postgresql_config():
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
def update_service_ports(old_service_ports=None, new_service_ports=None):
    if old_service_ports is None or new_service_ports is None:
        return(None)
    for port in old_service_ports:
        if port not in new_service_ports:
            close_port(port)
    for port in new_service_ports:
        if port not in old_service_ports:
            open_port(port)


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


#------------------------------------------------------------------------------
# construct_haproxy_config:  Convenience function to write haproxy.cfg
#                            haproxy_globals, haproxy_defaults,
#                            haproxy_monitoring, haproxy_services
#                            are all strings that will be written without
#                            any checks.
#                            haproxy_monitoring and haproxy_services are
#                            optional arguments
#------------------------------------------------------------------------------
def construct_haproxy_config(haproxy_globals=None,
                         haproxy_defaults=None,
                         haproxy_monitoring=None,
                         haproxy_services=None):
    if haproxy_globals is None or \
       haproxy_defaults is None:
        return(None)
    with open(default_haproxy_config, 'w') as haproxy_config:
        haproxy_config.write(haproxy_globals)
        haproxy_config.write("\n")
        haproxy_config.write("\n")
        haproxy_config.write(haproxy_defaults)
        haproxy_config.write("\n")
        haproxy_config.write("\n")
        if haproxy_monitoring is not None:
            haproxy_config.write(haproxy_monitoring)
            haproxy_config.write("\n")
            haproxy_config.write("\n")
        if haproxy_services is not None:
            haproxy_config.write(haproxy_services)
            haproxy_config.write("\n")
            haproxy_config.write("\n")


#------------------------------------------------------------------------------
# service_haproxy:  Convenience function to start/stop/restart/reload
#                   the haproxy service
#------------------------------------------------------------------------------
def service_haproxy(action=None, haproxy_config=default_haproxy_config):
    if action is None or haproxy_config is None:
        return(None)
    elif action == "check":
        retVal = subprocess.call(\
        ['/usr/sbin/haproxy', '-f', haproxy_config, '-c'])
        if retVal == 1:
            return(False)
        elif retVal == 0:
            return(True)
        else:
            return(False)
    else:
        retVal = subprocess.call(['service', 'haproxy', action])
        if retVal == 0:
            return(True)
        else:
            return(False)


###############################################################################
# Hook functions
###############################################################################
def install_hook():
    return (apt_get_install("postgresql-%s" % version) == True)


def config_changed():
    current_service_port = get_service_port()
    create_postgresql_config()
    updated_service_port = config_data["listen_port"]
    update_service_port(current_service_port, updated_service_port)
    service_postgresql("reload")


def start_hook():
    if service_postgresql("status"):
        return(service_postgresql("restart"))
    else:
        return(service_postgresql("start"))


def stop_hook():
    if service_postgresql("status"):
        return(service_postgresql("stop"))


def reverseproxy_interface(hook_name=None):
    if hook_name is None:
        return(None)
    if hook_name == "changed":
        config_changed()


def website_interface(hook_name=None):
    if hook_name is None:
        return(None)
    my_fqdn = socket.getfqdn(socket.gethostname())
    default_port = 80
    relation_data = relation_get()
    if hook_name == "joined":
        subprocess.call(['relation-set', 'port=%d' % \
        default_port, 'hostname=%s' % my_fqdn])
    elif hook_name == "changed":
        if 'is-proxy' in relation_data:
            service_name = "%s__%d" % \
            (relation_data['hostname'], relation_data['port'])
            open("%s/%s.is.proxy" % \
            (default_haproxy_service_config_dir, service_name), 'a').close()


###############################################################################
# Global variables
###############################################################################
config_data = config_get()
version = config_data['version']
cluster_name = config_data['cluster_name']
postgresql_config_dir = "/etc/postgresql"
postgresql_config = "%s/%s/%s/postgresql.conf" % (version, cluster_name, postgreql_config_dir)
postgresql_service_config_dir = "/var/run/postgresql"
hook_name = os.path.basename(sys.argv[0])

###############################################################################
# Main section
###############################################################################
if hook_name == "install":
    install_hook()
elif hook_name == "config-changed":
    config_changed()
elif hook_name == "start":
    start_hook()
elif hook_name == "stop":
    stop_hook()
elif hook_name == "reverseproxy-relation-broken":
    config_changed()
elif hook_name == "reverseproxy-relation-changed":
    reverseproxy_interface("changed")
elif hook_name == "website-relation-joined":
    website_interface("joined")
elif hook_name == "website-relation-changed":
    website_interface("changed")
else:
    print "Unknown hook"
    sys.exit(1)
