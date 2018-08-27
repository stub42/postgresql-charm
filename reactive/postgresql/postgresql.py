# Copyright 2015-2017 Canonical Ltd.
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

from collections import namedtuple, OrderedDict
from contextlib import contextmanager
from distutils.version import LooseVersion
import functools
import hashlib
import itertools
import json
import os.path
import re
import subprocess
import sys
from textwrap import dedent
import time

import psycopg2
import psycopg2.extensions
import psycopg2.extras

from charmhelpers.core import hookenv, unitdata
from charmhelpers.core.hookenv import DEBUG, WARNING
from charms import reactive

# This module is unit tested, so we can't use not_unless until we
# deal with charms.reactive Issue #46.
# from charms.reactive import not_unless

from reactive.postgresql import helpers
from reactive import workloadstatus


@functools.total_ordering
class AsIs(psycopg2.extensions.ISQLQuote):
    '''An extension of psycopg2.extensions.AsIs

    The comparison operators make it usable in unittests and
    stable no matter the psycopg2 version.
    '''
    def getquoted(self):
        return str(self._wrapped).encode('UTF8')

    def __conform__(self, protocol):
        if protocol is psycopg2.extensions.ISQLQuote:
            return self

    def __eq__(self, other):
        return self._wrapped == other

    def __lt__(self, other):
        return self._wrapped < other

    def __str__(self):
        return str(self._wrapped)

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, self._wrapped)


def version():
    '''PostgreSQL version. major.minor, as a string.'''
    # Use a cached version if available, to ensure this
    # method returns the same version consistently, even
    # across OS release upgrades.
    version = unitdata.kv().get('postgresql.pg_version')
    if version:
        return version

    # We use the charm configuration here, as multiple versions
    # of PostgreSQL may be installed.
    config = hookenv.config()
    version = config.get('version')
    if version:
        unitdata.kv().set('postgresql.pg_version', version)
        return version

    # If the version wasn't set, we are using the default version for
    # the distro release.
    version_map = dict(trusty='9.3', xenial='9.5', bionic='10')
    try:
        version = version_map[helpers.distro_codename()]
    except KeyError:
        raise NotImplementedError("No default version for distro {}".format(
            helpers.distro_codename()))
    unitdata.kv().set('postgresql.pg_version', version)
    return version


def point_version():
    '''PostgreSQL version. major.minor.patch or major.patch, as a string.'''
    output = subprocess.check_output([postgres_path(), '-V'],
                                     universal_newlines=True)
    return re.search(r'[\d\.]+', output).group(0)
    return output.split()[-1]


def has_version(ver):
    return LooseVersion(version()) >= LooseVersion(ver)


class InvalidConnection(Exception):
    '''Raised when we attempt to connect to a unit not yet ready.'''


def connect(user='postgres', database='postgres', unit=None):
    if unit is None or unit == hookenv.local_unit():
        host = None
        port_ = port()
    else:
        relinfo = helpers.get_peer_relation()[unit]
        if 'host' not in relinfo or 'port' not in relinfo:
            raise InvalidConnection('{} has not published connection details'
                                    ''.format(unit))
        host = relinfo['host']
        port_ = relinfo['port']
    return psycopg2.connect(user=user, database=database,
                            host=host, port=port_)


def username(unit_or_service, superuser, replication):
    '''Return the username to use for connections from the unit or service.'''
    servicename = unit_or_service.split('/', 1)[0]
    # The prefixes ensure that a client service can use all three relation
    # types to a single PostgreSQL service. And names starting with 'juju'
    # should not conflict with manually created roles.
    if replication:
        username = 'jujurepl_{}'.format(servicename)
    elif superuser:
        username = 'jujuadmin_{}'.format(servicename)
    else:
        username = 'juju_{}'.format(servicename)
    if len(username) > 63:
        h = hashlib.md5(username.encode('UTF8')).hexdigest()
        username = username[:31] + h
    return username


def port():
    '''The port PostgreSQL is listening on.'''
    path = postgresql_conf_path()
    if os.path.exists(path):
        with open(path, 'r') as f:
            m = re.search(r"^\s*port\s*=\s*'?(\d+)", f.read(), re.I | re.M)
            if m is not None:
                return int(m.group(1))
    return 5432  # Default port.


def packages():
    ver = version()
    p = set(['postgresql-{}'.format(ver),
             'postgresql-common', 'postgresql-client-common',
             'postgresql-client-{}'.format(ver)])
    if not has_version('10'):
        p.add('postgresql-contrib-{}'.format(ver))
    return p


@contextmanager
def inhibit_default_cluster_creation():
    '''Stop the PostgreSQL packages from creating the default cluster.

    We can't use the default cluster as it is likely created with an
    incorrect locale and without options such as data checksumming.
    '''
    path = postgresql_conf_path()
    if os.path.exists(path):
        yield
    else:
        os.makedirs(os.path.dirname(path), mode=0o755, exist_ok=True)
        with open(path, 'w'):
            pass
        try:
            yield
        finally:
            os.unlink(path)


def config_dir():
    return '/etc/postgresql/{}/main'.format(version())


def data_dir():
    return '/var/lib/postgresql/{}/main'.format(version())


def postgresql_conf_path():
    return os.path.join(config_dir(), 'postgresql.conf')


def pg_hba_conf_path():
    return os.path.join(config_dir(), 'pg_hba.conf')


def pg_ident_conf_path():
    return os.path.join(config_dir(), 'pg_ident.conf')


def recovery_conf_path():
    return os.path.join(data_dir(), 'recovery.conf')


def pg_ctl_path():
    return '/usr/lib/postgresql/{}/bin/pg_ctl'.format(version())


def pg_controldata_path():
    return '/usr/lib/postgresql/{}/bin/pg_controldata'.format(version())


def postgres_path():
    return '/usr/lib/postgresql/{}/bin/postgres'.format(version())


def pg_rewind_path():
    if has_version('9.5'):
        return '/usr/lib/postgresql/{}/bin/pg_rewind'.format(version())
    return None


def pid_path():
    return '/var/run/postgresql/{}-main.pid'.format(version())


def pg_log_path():
    return '/var/log/postgresql/postgresql-{}-main.log'.format(version())


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
    return os.path.exists(recovery_conf_path())


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
        identifier.encode('US-ASCII')
        return '"{}"'.format(identifier.replace('"', '""'))
    except UnicodeEncodeError:
        escaped = []
        for c in identifier:
            if c == '\\':
                escaped.append('\\\\')
            elif c == '"':
                escaped.append('""')
            else:
                c = c.encode('US-ASCII', 'backslashreplace').decode('US-ASCII')
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


def create_cluster():
    config = hookenv.config()
    cmd = ['pg_createcluster', '-e', config['encoding'],
           '--locale', config['locale'], version(), 'main']
    # With 9.3+, we make an opinionated decision to always enable
    # data checksums. This seems to be best practice. We could
    # turn this into a configuration item if there is need. There
    # is no way to enable this option on existing clusters.
    if has_version('9.3'):
        cmd.extend(['--', '--data-checksums'])
    subprocess.check_call(cmd, universal_newlines=True)


def drop_cluster(stop=False):
    cmd = ['pg_dropcluster', version(), 'main']
    if stop:
        cmd.append('--stop')
    subprocess.check_call(cmd, universal_newlines=True)


# @not_unless('postgresql.replication.is_primary')
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


# @not_unless('postgresql.replication.is_primary')
def ensure_user(con, username, password, superuser=False, replication=False):
    if role_exists(con, username):
        cmd = ["ALTER ROLE"]
    else:
        cmd = ["CREATE ROLE"]
    cmd.append("%s WITH LOGIN")
    cmd.append("SUPERUSER" if superuser else "NOSUPERUSER")
    cmd.append("REPLICATION" if replication else "NOREPLICATION")
    cmd.append("PASSWORD %s")
    cur = con.cursor()
    cur.execute(' '.join(cmd), (pgidentifier(username), password))


def role_exists(con, role):
    '''True if the database role exists.'''
    cur = con.cursor()
    cur.execute("SELECT TRUE FROM pg_roles WHERE rolname=%s", (role,))
    return cur.fetchone() is not None


# @not_unless('postgresql.replication.is_primary')
def grant_database_privileges(con, role, database, privs):
    cur = con.cursor()
    for priv in privs:
        cur.execute("GRANT %s ON DATABASE %s TO %s",
                    (AsIs(priv), pgidentifier(database), pgidentifier(role)))


# @not_unless('postgresql.replication.is_primary')
def grant_user_roles(con, username, roles):
    wanted_roles = set(roles)

    cur = con.cursor()
    cur.execute(dedent("""\
        SELECT role.rolname
        FROM
            pg_roles AS role,
            pg_roles AS member,
            pg_auth_members
        WHERE
            member.oid = pg_auth_members.member
            AND role.oid = pg_auth_members.roleid
            AND member.rolname = %s
        """), (username,))
    existing_roles = set(r[0] for r in cur.fetchall())

    roles_to_grant = wanted_roles.difference(existing_roles)

    if roles_to_grant:
        hookenv.log("Granting {} to {}".format(",".join(roles_to_grant),
                                               username))
        for role in roles_to_grant:
            ensure_role(con, role)
            cur.execute("GRANT %s TO %s",
                        (pgidentifier(role), pgidentifier(username)))

    # We no longer revoke roles, as this interferes with manually
    # granted permissions.
    # roles_to_revoke = existing_roles.difference(wanted_roles)
    # if roles_to_revoke:
    #     hookenv.log("Revoking {} from {}".format(",".join(roles_to_grant),
    #                                              username))
    #     for role in roles_to_revoke:
    #         cur.execute("REVOKE %s FROM %s",
    #                     (pgidentifier(role), pgidentifier(username)))


# @not_unless('postgresql.replication.is_primary')
def ensure_role(con, role):
    # Older PG versions don't have 'CREATE ROLE IF NOT EXISTS'
    cur = con.cursor()
    cur.execute("SELECT TRUE FROM pg_roles WHERE rolname=%s",
                (role,))
    if cur.fetchone() is None:
        cur.execute("CREATE ROLE %s INHERIT NOLOGIN",
                    (pgidentifier(role),))


# @not_unless('postgresql.replication.is_primary')
def ensure_extensions(con, extensions):
    '''extensions is a list of (name, schema) tuples'''
    cur = con.cursor()
    cur.execute('''SELECT extname,nspname FROM pg_extension,pg_namespace
                   WHERE pg_namespace.oid = extnamespace''')
    installed_extensions = frozenset((x[0], x[1]) for x in cur.fetchall())
    hookenv.log("ensure_extensions({}), have {}"
                .format(extensions, installed_extensions), DEBUG)
    extensions_set = frozenset(set(extensions))
    extensions_to_create = extensions_set.difference(installed_extensions)
    for ext, schema in extensions_to_create:
        hookenv.log("creating extension {}".format(ext), DEBUG)
        if schema != 'public':
            cur.execute('CREATE SCHEMA IF NOT EXISTS %s',
                        (pgidentifier(schema),))
        cur.execute('CREATE EXTENSION %s WITH SCHEMA %s',
                    (pgidentifier(ext), pgidentifier(schema)))


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
        subprocess.check_call(['sudo', '-u', 'postgres',
                               pg_ctl_path(), 'status',
                               '-D', data_dir()],
                              universal_newlines=True,
                              stdout=subprocess.DEVNULL)
        return True
    except subprocess.CalledProcessError as x:
        if x.returncode == 3:
            return False  # PostgreSQL not running
        elif x.returncode == 4 and has_version('9.4'):
            return False  # $DATA_DIR inaccessible, PG 9.4+
        raise  # Unexpected failure.


# Slow or busy systems can take much longer than the default 60 seconds
# to startup or shutdown. If a database needs to go through recovery,
# it could take days.
STARTUP_TIMEOUT = 24 * 60 * 60

# If we can't perform a fast shutdown in 5 minutes, the system is in
# a bit of a mess so proceed to immediate shutdown (and causing the
# subsequent startup to be slow).
SHUTDOWN_TIMEOUT = 5 * 60


def start(ignore_failure=False):
    try:
        subprocess.check_call(['pg_ctlcluster',
                               version(), 'main', 'start',
                               # These extra options cause pg_ctl to wait
                               # for startup to finish, so we don't have to.
                               '--', '-w', '-t', str(STARTUP_TIMEOUT)],
                              universal_newlines=True)
    except subprocess.CalledProcessError as x:
        if ignore_failure:
            return
        if x.returncode == 2:
            return  # The server is already running.
        workloadstatus.status_set('blocked', 'PostgreSQL failed to start')
        emit_pg_log()  # For debugging inscruitable pg_ctlcluster failures.
        raise SystemExit(0)


def stop():
    # First try a 'fast' shutdown.
    try:
        subprocess.check_call(['pg_ctlcluster', '--mode', 'fast',
                               version(), 'main', 'stop',
                               '--', '-w', '-t', str(SHUTDOWN_TIMEOUT)],
                              universal_newlines=True)
        return
    except subprocess.CalledProcessError as x:
        if x.returncode == 2:
            return  # The server was not running.

    # If the 'fast' shutdown failed, try an 'immediate' shutdown.
    try:
        hookenv.log('Fast shutdown failed. Attempting immediate shutdown.',
                    WARNING)
        subprocess.check_call(['pg_ctlcluster', '--mode', 'immediate',
                               version(), 'main', 'stop',
                               '--', '-w', '-t', str(SHUTDOWN_TIMEOUT)],
                              universal_newlines=True)
        return
    except subprocess.CalledProcessError as x:
        if x.returncode == 2:
            return  # The server was not running.
        workloadstatus.status_set('blocked', 'Unable to shutdown PostgreSQL')
        raise SystemExit(0)


def emit_pg_log(lines=100):
    '''Dump the end of the PostgreSQL log file to stdout'''
    rec_conf = recovery_conf_path()
    if os.path.exists(rec_conf):
        print(open(rec_conf, 'r').read())
        sys.stdout.flush()
    subprocess.call([pg_controldata_path(), '-D', data_dir()],
                    universal_newlines=True)
    subprocess.call(['tail', '-{:d}'.format(lines), pg_log_path()],
                    universal_newlines=True)


def reload_config():
    '''Send a reload signal to a running PostgreSQL.

    Alas, there is no easy way to confirm that the reload succeeded.
    '''
    subprocess.check_call(['pg_ctlcluster', version(), 'main', 'reload'])


def parse_config(unparsed_config, fatal=True):
    '''Parse a postgresql.conf style string, returning a dictionary.

    This is a simple key=value format, per section 18.1.2 at
    http://www.postgresql.org/docs/9.4/static/config-setting.html
    '''
    scanner = re.compile(r"""^\s*
                         (                       # key=value (1)
                           (?:
                              (\w+)              # key (2)
                              (?:\s*=\s*|\s+)    # separator
                           )?
                           (?:
                              ([-.\w]+) |        # simple value (3) or
                              '(                 # quoted value (4)
                                (?:[^']|''|\\')*
                               )(?<!\\)'(?!')
                           )?
                           \s* ([^\#\s].*?)?     # badly quoted value (5)
                         )?
                         (?:\s*\#.*)?$           # comment
                         """, re.X)
    parsed = OrderedDict()
    for lineno, line in zip(itertools.count(1), unparsed_config.splitlines()):
        try:
            m = scanner.search(line)
            if m is None:
                raise SyntaxError('Invalid line')
            keqv, key, value, q_value, bad_value = m.groups()
            if not keqv:
                continue
            if key is None:
                raise SyntaxError('Missing key'.format(keqv))
            if bad_value is not None:
                raise SyntaxError('Badly quoted value'.format(bad_value))
            assert value is None or q_value is None
            if q_value is not None:
                value = re.sub(r"''|\\'", "'", q_value)
            if value is not None:
                parsed[key.lower()] = value
            else:
                raise SyntaxError('Missing value')
        except SyntaxError as x:
            if fatal:
                x.lineno = lineno
                x.text = line
                raise x
            workloadstatus.status_set('blocked',
                                      '{} line {}: {}'.format(x, lineno, line))
            raise SystemExit(0)
    return parsed


def pg_settings_schema():
    '''Server setting definitions as a dictionary of records.

    Alas, --describe-cluster doesn't provide us with everything
    we need, and pg_settings is only available when the server is
    running and correctly configured, so we load a copy of pg_settings
    cached in $CHARM_DIR/lib.

    Generate the file using lib/cache_settings.py.
    '''
    cache = os.path.join(hookenv.charm_dir(), 'lib',
                         'pg_settings_{}.json'.format(version()))
    assert os.path.exists(cache), 'No pg_settings cache {}'.format(cache)
    with open(cache, 'r') as f:
        schema = json.load(f)

    # Convert to namedtuples.
    for item in schema.values():
        keys = sorted(item.keys())
        break
    rec = namedtuple('pg_settings', keys)
    return {k: rec(**schema[k]) for k in schema.keys()}


# def live_pg_settings():
#     '''Return the pg_settings system view as a dictionary of records.
#
#     Returns the same information as pg_settings_schema, but with current
#     live settings. PostgreSQL must be running.
#     '''
#     con = connect()
#     cur = con.cursor(cursor_factory=NamedTupleCursor)
#     cur.execute('SELECT * FROM pg_settings')
#     return {record.name: record for record in cur.fetchall()}


def convert_unit(value_with_unit, dest_unit):
    '''Convert a number with a unit like '16MB' to the given unit.

    Input is a string. Returns a integer.

    If the source does not specify a unit, it is passed through
    unmodified.

    Units are case sensitive, per the postgresql documentation.
    '''
    m = re.search(r'^([-\d]+)\s*(\w+)?\s*$', value_with_unit)
    if m is None:
        raise ValueError(value_with_unit, 'Invalid number or unit')
    v, source_unit = m.groups()
    v = int(v)
    if source_unit is None:
        return v

    mem_conv = {'kB': 1024,
                '8kB': 1024 * 8,  # Output only, for postgresql.conf
                'MB': 1024 * 1024,
                'GB': 1024 * 1024 * 1024,
                'TB': 1024 * 1024 * 1024 * 1024}

    time_conv = {'ms': 1,
                 's': 1000,
                 'min': 1000 * 60,
                 'h': 1000 * 60 * 60,
                 'd': 1000 * 60 * 60 * 24}

    for conv in (mem_conv, time_conv):
        if source_unit in conv:
            if dest_unit in conv:
                return v * conv[source_unit] / conv[dest_unit]
            else:
                raise ValueError(value_with_unit,
                                 'Cannot convert {} to {}'.format(source_unit,
                                                                  dest_unit))
    raise ValueError(value_with_unit,
                     'Unknown conversion unit {!r}. '
                     'Units are case sensitive.'.format(source_unit))


# VALID_BOOLS is the set of unique prefixes accepted as valid boolean values.
VALID_BOOLS = ['on', 'off', 'true', 'false', 'yes', 'no', '0', '1']
VALID_BOOLS = frozenset(prefix
                        for word in VALID_BOOLS
                        for prefix in [word[:i + 1]
                                       for i in range(0, len(word))]
                        if len([w for w in VALID_BOOLS
                                if w.startswith(prefix)]) == 1)


def wal_received_offset(con):
    """How much WAL a hot standby has received.

    Coverts PostgreSQL's pg_last_xlog_receive_location() to a number.
    The higher the number, the more advanced in the timeline the unit
    is. When we failover, the unit with the highest offset should become
    the new master to minimize dataloss and avoid unnecessary rebuilds
    of the remaining hot standbys.

    Returns None if run against a primary.
    """
    cur = con.cursor()
    if has_version('10'):
        cur.execute(
            'SELECT pg_is_in_recovery(), pg_last_wal_receive_lsn()')
    else:
        cur.execute(
            'SELECT pg_is_in_recovery(), pg_last_xlog_receive_location()')
    is_in_recovery, xlog_received = cur.fetchone()
    if is_in_recovery:
        return wal_location_to_bytes(xlog_received)
    return None


def wal_replay_offset(con):
    """How much WAL a hot standby has replayed.

    Coverts PostgreSQL's pg_last_xlog_replay_location() to a number.
    The higher the number, the more advanced in the timeline the unit
    is.

    This method waits until the replay location stops changing, and
    should not be run against a standby connected to an active master
    (ie. only during failover, after disconnecting from the doomed master)
    """
    cur = con.cursor()
    prev_xlog_replayed = None
    while True:
        if has_version('10'):
            cur.execute('''SELECT pg_is_in_recovery(),
                                  pg_last_wal_replay_lsn()''')
        else:
            cur.execute('''SELECT pg_is_in_recovery(),
                                  pg_last_xlog_replay_location()''')
        is_in_recovery, xlog_replayed = cur.fetchone()
        assert is_in_recovery, 'Unit is not in recovery mode'
        if xlog_replayed is not None and xlog_replayed == prev_xlog_replayed:
            return wal_location_to_bytes(xlog_replayed)
        prev_xlog_replayed = xlog_replayed
        hookenv.log('WAL replay position {}'.format(xlog_replayed))
        time.sleep(1.5)


def wal_location_to_bytes(wal_location):
    """Convert WAL + offset to num bytes, so they can be compared."""
    logid, offset = wal_location.split('/')
    return int(logid, 16) * 16 * 1024 * 1024 * 255 + int(offset, 16)


def promote():
    assert is_secondary(), 'Cannot promote primary'
    assert is_running(), 'Attempting to promote a stopped server'

    wal_e_enabled = reactive.is_state('postgresql.wal_e.enabled')

    if wal_e_enabled or has_version('9.3'):
        # If we have PostgreSQL 9.3 or WAL archiving enabled, promote
        # and do a timeline switch. We have to assume WAL-E is configured
        # properly and is working.
        rc = subprocess.call(['sudo', '-u', 'postgres', '-H',
                              pg_ctl_path(), 'promote', '-D', data_dir()],
                             universal_newlines=True)
        if rc != 0:
            helpers.status_set('blocked', 'Failed to promote to primary')
            raise SystemExit(0)
    else:
        # Removing recovery.conf will promote the unit to master without
        # a timeline switch when PostgreSQL is restarted.
        os.unlink(recovery_conf_path())
        # Restart now. No need to request permission, as this unit was
        # previously a secondary and not being used by anything. This
        # also ensures the unit is a functioning primary before returning,
        # like the newer 'promote' code path above.
        stop()
        start()


def is_replicating(parent, ip=None, user=None):
    '''Return True if the ip address is replicating from the parent unit'''
    # We can get away with using unit_private_ip() here because we only
    # replicate to peers and not cross model. TODO: Use egress subnets anyway.
    if ip is None:
        ip = hookenv.unit_private_ip()
    ip = helpers.ensure_ip(ip)
    con = connect(user=user, unit=parent)
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute('SELECT * FROM pg_stat_replication WHERE client_addr=%s',
                (ip,))
    found = False
    for row in cur.fetchall():
        hookenv.log('Replication details: {}'.format(row), DEBUG)
        found = True
    return found
