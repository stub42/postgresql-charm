# Copyright 2015 Canonical Ltd.
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
import re
import subprocess

import psycopg2
from psycopg2.extensions import AsIs

from charmhelpers.core import hookenv
from charmhelpers.core.hookenv import DEBUG, WARNING

import helpers


def version():
    '''PostgreSQL version. major.minor, as a string.'''
    # We use the charm configuration here, as multiple versions
    # of PostgreSQL may be installed.
    version = hookenv.config()['version']
    if version:
        return version

    # If the version wasn't set, we are using the default version for
    # the distro release.
    version_map = dict(precise='9.1', trusty='9.3')
    return version_map[helpers.distro_codename()]


def connect(database='postgres'):
    return psycopg2.connect(user=username(hookenv.local_unit()),
                            database=database, port=port())


def username(unit_or_service, superuser=False):
    '''Return the username to use for connections from the unit or service.'''
    servicename = unit_or_service.split('/', 1)[0]
    if superuser:
        username = 'juju_{}_admin'.format(servicename)
    else:
        username = 'juju_{}'.format(servicename)
    return username


def port():
    '''The port PostgreSQL is listening on.'''
    path = postgresql_conf_path()
    with open(path, 'r') as f:
        m = re.search(r'^port\s*=\*(\d+)', f.read(), re.I | re.M)
        assert m is not None, 'No port configured in {!r}'.format(path)
        return int(m.group(1))


def packages():
    ver = version()
    return set(['postgresql-{}'.format(ver),
                'postgresql-common',
                'postgresql-contrib-{}'.format(ver),
                'postgresql-client-{}'.format(ver)])


def postgresql_conf_path():
    return '/etc/postgresql/{}/main/postgresql.conf'.format(version())


def datadir_path():
    return '/var/lib/postgresql/{}/main'.format(version())


def recovery_conf_path():
    return os.path.join(datadir_path(), 'recovery.conf'.format(version()))


def is_in_recovery():
    '''True if the local cluster is in recovery.

    The unit may be a hot standby, or it may be a primary that is still
    starting up.
    '''
    cur = connect().cursor()
    cur.execute('SELECT pg_is_in_recovery()')
    return cur.fetchone()[0]


def is_primary():
    '''True if the unit is a primary.

    It may be possible for there to be multiple primaries in the service,
    or none at all. Primaries are writable replicas, and will include the
    master.
    '''
    return not is_secondary()


def is_secondary():
    '''True if the unit is a hot standby.

    Hot standbys are read only replicas.
    '''
    return is_in_recovery() and os.path.exists(recovery_conf_path())


def is_master():
    '''True if the unit is the master.

    The master unit is responsible for creating objects in the database,
    and must be a primary. The service has at most one master, and may
    have no master during transitional states like failover.
    '''
    return (hookenv.leader_get('master') == hookenv.local_unit()
            and is_primary())


def master():
    '''Return the master unit.

    May return None if there we are in a transitional state and there
    is currently no master.
    '''
    return hookenv.leader_get('master')


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


def pgidentifier(token):
    '''Wrap a string for interpolation by psycopg2 as an SQL identifier'''
    return AsIs(quote_identifier(token))


def ensure_database(database):
    '''Create the database if it doesn't already exist.

    This is done outside of a transaction.
    '''
    con = connect()
    con.autocommit = True
    cur = con.cursor()
    cur.execute("SELECT datname FROM pg_database WHERE datname=%s",
                (database,))
    if cur.fetchone() is None:
        cur.execute('CREATE DATABASE %s', (pgidentifier(database),))


def ensure_user(con, username, password, superuser=False):
    if role_exists(con, username):
        cmd = ["ALTER ROLE"]
    else:
        cmd = ["CREATE ROLE"]
    cmd.append("%s WITH LOGIN")
    cmd.append("SUPERUSER" if superuser else "NOSUPERUSER")
    cmd.append("PASSWORD %s")
    cur = con.cursor()
    cur.execute(' '.join(cmd), (pgidentifier(username), password))


def role_exists(con, role):
    '''True if the database role exists.'''
    cur = con.cursor()
    cur.execute("SELECT TRUE FROM pg_roles WHERE rolname=%s", (role,))
    return cur.fetchone() is not None


def grant_database_privileges(con, role, database, privs):
    cur = con.cursor()
    for priv in privs:
        cur.execure("GRANT %s ON DATABASE %s TO %s",
                    (pgidentifier(priv), pgidentifier(database),
                     pgidentifier(role)))


def reset_user_roles(con, username, roles):
    wanted_roles = set(roles)

    cur = con.cursor()
    cur.execute("""
        SELECT role.rolname
        FROM
            pg_roles AS role,
            pg_roles AS member,
            pg_auth_members
        WHERE
            member.oid = pg_auth_members.member
            AND role.oid = pg_auth_members.roleid
            AND member.rolname = %s
        """, (username,))
    existing_roles = set(r[0] for r in cur.fetchall())

    roles_to_grant = wanted_roles.difference(existing_roles)

    if roles_to_grant:
        hookenv.log("Granting {} to {}".format(",".join(roles_to_grant),
                                               username))
        for role in roles_to_grant:
            ensure_role(con, role)
            cur.execute("GRANT %s TO %s",
                        (pgidentifier(role), pgidentifier(username)))

    roles_to_revoke = existing_roles.difference(wanted_roles)

    if roles_to_revoke:
        hookenv.log("Revoking {} from {}".format(",".join(roles_to_grant),
                                                 username))
        for role in roles_to_revoke:
            cur.execute("REVOKE %s FROM %s",
                        (pgidentifier(role), pgidentifier(role)))


def ensure_role(con, role):
    # Older PG versions don't have 'CREATE ROLE IF NOT EXISTS'
    cur = con.cursor()
    cur.execute("SELECT TRUE FROM pg_roles WHERE rolname=%s",
                (role,))
    if cur.fetchone() is None:
        cur.execute("CREATE ROLE %s INHERIT NOLOGIN",
                    (pgidentifier(role),))


def ensure_extensions(con, extensions):
    cur = con.cursor()
    cur.execute('SELECT extname FROM pg_extension')
    installed_extensions = frozenset(x[0] for x in cur.fetchall())
    hookenv.log("ensure_extensions({}), have {}"
                .format(extensions, installed_extensions), DEBUG)
    extensions_set = frozenset(extensions)
    extensions_to_create = extensions_set.difference(installed_extensions)
    for ext in extensions_to_create:
        hookenv.log("creating extension {}".format(ext), DEBUG)
        cur.execute('CREATE EXTENSION %s',
                    (pgidentifier(ext),))


def addr_to_range(addr):
    '''Convert an address to a format suitable for pg_hba.conf.

    IPv4 and IPv6 ranges are passed through unchanged, as are hostnames.
    Individual IPv4 and IPv6 addresses have a hostmask appended.
    '''
    if re.search(r'^(?:\d{1,3}\.){3}\d{1,3}$', addr, re.A) is not None:
        addr += '/32'
    elif ':' in addr and '/' not in addr:  # IPv6
        addr += '/128'
    return addr


def is_running():
    try:
        subprocess.check_call(['pg_ctl', 'status', '-s', '-D', datadir_path()],
                              universal_newlines=True)
        return True
    except subprocess.CalledProcessError as x:
        if x.code == 3:
            return False
        raise


# Slow or busy systems can take much longer than the default 60 seconds
# to startup or shutdown. If a database needs to go through recovery,
# it could take days.
STARTUP_TIMEOUT = 24 * 60 * 60

# If we can't perform a fast shutdown in 5 minutes, the system is in
# a bit of a mess so proceed to immediate shutdown (and causing the
# subsequent startup to be slow).
SHUTDOWN_TIMEOUT = 5 * 60


def start():
    try:
        subprocess.check_call(['pg_ctlcluster',
                               version(), 'main', 'start',
                               '--', '-t', str(STARTUP_TIMEOUT)],
                              universal_newlines=True)
    except subprocess.CalledProcessError as x:
        if x.code == 2:
            return  # The server is already running.
        raise


def stop():
    # First try a 'fast' shutdown.
    try:
        subprocess.check_call(['pg_ctlcluster', '--mode', 'fast',
                               version(), 'main', 'stop',
                               '--', '-t', str(SHUTDOWN_TIMEOUT)],
                              universal_newlines=True)
        return
    except subprocess.CalledProcessError as x:
        if x.code == 2:
            return  # The server was not running.

    # If the 'fast' shutdown failed, try an 'immediate' shutdown.
    try:
        hookenv.log('Fast shutdown failed. Attempting immediate shutdown.',
                    WARNING)
        subprocess.check_call(['pg_ctlcluster', '--mode', 'immediate',
                               version(), 'main', 'stop',
                               '--', '-t', str(SHUTDOWN_TIMEOUT)],
                              universal_newlines=True)
        return
    except subprocess.CalledProcessError as x:
        if x.code == 2:
            return  # The server was not running.
        helpers.status_set('blocked', 'Unable to shutdown PostgreSQL')
        raise SystemExit(0)
