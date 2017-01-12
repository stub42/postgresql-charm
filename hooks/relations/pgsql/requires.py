# Copyright 2016 Canonical Ltd.
#
# This file is part of the PostgreSQL Client Interface for Juju charms.reactive
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

from collections import OrderedDict
import ipaddress
import itertools
import re
import urllib.parse

from charmhelpers import context
from charmhelpers.core import hookenv
from charms.reactive import hook, scopes, RelationBase


# This data structure cannot be in an external library,
# as interfaces have no way to declare dependencies
# (https://github.com/juju/charm-tools/issues/243).
# It also must be defined in this file
# (https://github.com/juju-solutions/charms.reactive/pull/51)
#
class ConnectionString(str):
    """A libpq connection string.

    >>> c = ConnectionString(host='1.2.3.4', dbname='mydb',
    ...                      port=5432, user='anon', password='secret')
    ...
    >>> c
    'host=1.2.3.4 dbname=mydb port=5432 user=anon password=secret
    >>> ConnectionString(str(c), dbname='otherdb')
    'host=1.2.3.4 dbname=otherdb port=5432 user=anon password=secret

    Components may be accessed as attributes.

    >>> c.dbname
    'mydb'
    >>> c.host
    '1.2.3.4'
    >>> c.port
    '5432'

    The standard URI format is also accessible:

    >>> c.uri
    'postgresql://anon:secret@1.2.3.4:5432/mydb'

    """
    def __new__(self, conn_str=None, **kw):

        # Parse libpq key=value style connection string. Components
        # passed by keyword argument override. If the connection string
        # is invalid, some components may be skipped (but in practice,
        # where database and usernames don't contain whitespace,
        # quotes or backslashes, this doesn't happen).
        if conn_str is not None:
            r = re.compile(r"""(?x)
                               (\w+) \s* = \s*
                               (?:
                                 '((?:.|\.)*?)' |
                                 (\S*)
                               )
                               (?=(?:\s|\Z))
                           """)
            for key, v1, v2 in r.findall(conn_str):
                if key not in kw:
                    kw[key] = v1 or v2

        def quote(x):
            q = str(x).replace("\\", "\\\\").replace("'", "\\'")
            q = q.replace('\n', ' ')  # \n is invalid in connection strings
            if ' ' in q:
                q = "'" + q + "'"
            return q

        c = " ".join("{}={}".format(k, quote(v))
                     for k, v in sorted(kw.items())
                     if v)
        c = str.__new__(self, c)

        for k, v in kw.items():
            setattr(c, k, v)

        self._keys = set(kw.keys())

        # Construct the documented PostgreSQL URI for applications
        # that use this format. PostgreSQL docs refer to this as a
        # URI so we do do, even though it meets the requirements the
        # more specific term URL.
        fmt = ['postgresql://']
        d = {k: urllib.parse.quote(v, safe='') for k, v in kw.items() if v}
        if 'user' in d:
            if 'password' in d:
                fmt.append('{user}:{password}@')
            else:
                fmt.append('{user}@')
        if 'host' in kw:
            try:
                hostaddr = ipaddress.ip_address(kw.get('hostaddr') or
                                                kw.get('host'))
                if isinstance(hostaddr, ipaddress.IPv6Address):
                    d['hostaddr'] = '[{}]'.format(hostaddr)
                else:
                    d['hostaddr'] = str(hostaddr)
            except ValueError:
                # Not an IP address, but hopefully a resolvable name.
                d['hostaddr'] = d['host']
            del d['host']
            fmt.append('{hostaddr}')
        if 'port' in d:
            fmt.append(':{port}')
        if 'dbname' in d:
            fmt.append('/{dbname}')
        main_keys = frozenset(['user', 'password',
                               'dbname', 'hostaddr', 'port'])
        extra_fmt = ['{}={{{}}}'.format(extra, extra)
                     for extra in sorted(d.keys()) if extra not in main_keys]
        if extra_fmt:
            fmt.extend(['?', '&'.join(extra_fmt)])
        c.uri = ''.join(fmt).format(**d)

        return c

    host = None
    dbname = None
    port = None
    user = None
    password = None
    uri = None

    def keys(self):
        return iter(self._keys)

    def items(self):
        return {k: self[k] for k in self.keys()}.items()

    def values(self):
        return iter(self[k] for k in self.keys())

    def __getitem__(self, key):
        if isinstance(key, int):
            return super(ConnectionString, self).__getitem__(key)
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key)


class ConnectionStrings(OrderedDict):
    """Collection of :class:`ConnectionString` for a relation.

    :class:`ConnectionString` may be accessed as a dictionary
    lookup by unit name, or more usefully by the master and
    standbys attributes. Note that the dictionary lookup may
    return None, when the database is not ready for use.
    """
    relname = None
    relid = None

    def __init__(self, relid):
        super(ConnectionStrings, self).__init__()
        self.relname = relid.split(':', 1)[0]
        self.relid = relid
        relations = context.Relations()
        relation = relations[self.relname][relid]
        for unit, reldata in relation.items():
            self[unit] = _cs(reldata)

    @property
    def master(self):
        """The :class:`ConnectionString` for the master, or None."""
        relation = context.Relations()[self.relname][self.relid]

        if not self._authorized(relation):
            return None

        # New v2 protocol, each unit advertises the master connection.
        for reldata in relation.values():
            if reldata.get('master'):
                return ConnectionString(reldata['master'])

        # Fallback to v1 protocol.
        masters = [unit for unit, reldata in relation.items()
                   if reldata.get('state') in ('master', 'standalone')]
        if len(masters) == 1:
            return self[masters[0]]  # One, and only one.
        else:
            # None ready, or multiple due to failover in progress.
            return None

    @property
    def standbys(self):
        """list of :class:`ConnectionString` for active hot standbys."""
        relation = context.Relations()[self.relname][self.relid]

        if not self._authorized(relation):
            return None

        # New v2 protocol, each unit advertises all standbys.
        for reldata in relation.values():
            if reldata.get('standbys'):
                return [ConnectionString(s)
                        for s in reldata['standbys'].splitlines() if s]

        # Fallback to v1 protocol.
        s = []
        for unit, reldata in relation.items():
            if reldata.get('state') == 'hot standby':
                conn_str = self[unit]
                if conn_str:
                    s.append(conn_str)
        return s

    @property
    def version(self):
        """PostgreSQL major version (eg. `9.5`)."""
        for relation in context.Relations()[self.relname][self.relid].values():
            if relation.get('version'):
                return relation['version']
        return None

    def _authorized(self, relation):
        local_unit = hookenv.local_unit()
        for reldata in relation.values():
            # Ignore new PostgreSQL units that are not yet providing
            # connection details. ie. all remote units that have not
            # yet run their -relation-joined hook and are yet unaware
            # of this client. This prevents authorization 'flapping' when
            # new remote units are added.
            if 'master' not in reldata and 'standbys' not in reldata:
                continue

            # If any units have not yet authorized this unit, we can't
            # trust that the service is usable yet.
            if local_unit not in reldata.get('allowed-units', '').split():
                return False

            # Similarly, if any units do not yet match the requested database
            # configuration, we can't trust that the service is usable yet.
            for c in ['database', 'roles', 'extensions']:
                requested = relation.local.get(c, '')
                if requested and requested != reldata.get(c, ''):
                    return False
        return True


class PostgreSQLClient(RelationBase):
    """
    PostgreSQL client interface.

    A client may be related to one or more PostgreSQL services.

    In most cases, a charm will only use a single PostgreSQL
    service being related for each relation defined in metadata.yaml
    (so one per relation name). To access the connection strings, use
    the master and standbys attributes::

        @when('productdb.master.available')
        def setup_database(pgsql):
            conn_str = pgsql.master  # A ConnectionString.
            update_db_conf(conn_str)

        @when('productdb.standbys.available')
        def setup_cache_databases(pgsql):
            set_cache_db_list(pgsql.standbys)  # set of ConnectionString.

    In somecases, a relation name may be related to several PostgreSQL
    services. You can also access the ConnectionStrings for a particular
    service by relation id or by iterating over all of them::

        @when('db.master.available')
        def set_dbs(pgsql):
            update_monitored_dbs(cs.master
                                 for cs in pgsql  # ConnectionStrings.
                                 if cs.master)
    """
    scope = scopes.SERVICE

    @hook('{requires:pgsql}-relation-joined')
    def joined(self):
        # There is at least one named relation
        self.set_state('{relation_name}.connected')
        hookenv.log('Joined {} relation'.format(hookenv.relation_id()))

    @hook('{requires:pgsql}-relation-{joined,changed,departed}')
    def changed(self):
        relid = hookenv.relation_id()
        cs = self[relid]

        # There is a master in this relation.
        self.toggle_state('{relation_name}.master.available',
                          cs.master)

        # There is at least one standby in this relation.
        self.toggle_state('{relation_name}.standbys.available',
                          cs.standbys)

        # There is at least one database in this relation.
        self.toggle_state('{relation_name}.database.available',
                          cs.master or cs.standbys)

        # Ideally, we could turn logging off using a layer option
        # but that is not available for interfaces.
        if cs.master and cs.standbys:
            hookenv.log('Relation {} has master and standby '
                        'databases available'.format(relid))
        elif cs.master:
            hookenv.log('Relation {} has a master database available, '
                        'but no standbys'.format(relid))
        elif cs.standbys:
            hookenv.log('Relation {} only has standby databases '
                        'available'.format(relid))
        else:
            hookenv.log('Relation {} has no databases available'.format(relid))

    @hook('{requires:pgsql}-relation-departed')
    def departed(self):
        if not any(u for u in hookenv.related_units() or []
                   if u != hookenv.remote_unit()):
            self.remove_state('{relation_name}.connected')
            self.conversation().depart()
            hookenv.log('Departed {} relation'.format(hookenv.relation_id()))

    # @hook('{requires:pgsql}-relation-broken')
    # def broken(self):
    #     if not hookenv.relation_ids(self.relation_name):
    #         for conversation in self.conversations():
    #             conversation.remove_state('{relation_name}.connected')
    #             conversation.remove_state('{relation_name}.master.available')
    #             conversation.remove_state('{relation_name}.standbys.available')
    #             conversation.remove_state('{relation_name}.database.available')

    def set_database(self, dbname, relid=None):
        """Set the database that the named relations connect to.

        The PostgreSQL service will create the database if necessary. It
        will never remove it.

        :param dbname: The database name. If unspecified, the local service
                       name is used.

        :param relid: relation id to send the database name setting to.
                      If unset, the setting is broadcast to all relations
                      sharing the relation name.

        """
        for c in self.conversations():
            if relid is None or c.namespace == relid:
                c.set_remote('database', dbname)

    def set_roles(self, roles, relid=None):
        """Provide a set of roles to be granted to the database user.

        Granting permissions to roles allows you to grant database
        access to other charms.

        The PostgreSQL service will create the roles if necessary.
        """
        if isinstance(roles, str):
            roles = [roles]
        roles = ','.join(sorted(roles))
        for c in self.conversations():
            if relid is None or c.namespace == relid:
                c.set_remote('roles', roles)

    def set_extensions(self, extensions, relid=None):
        """Provide a set of extensions to be installed into the database.

        The PostgreSQL service will attempt to install the requested
        extensions into the database. Extensions not bundled with
        PostgreSQL are normally installed onto the PostgreSQL service
        using the `extra_packages` config setting.
        """
        if isinstance(extensions, str):
            extensions = [extensions]
        extensions = ','.join(sorted(extensions))
        for c in self.conversations():
            if relid is None or c.namespace == relid:
                c.set_remote('extensions', extensions)

    def __getitem__(self, relid):
        """:returns: :class:`ConnectionStrings` for the relation id."""
        return ConnectionStrings(relid)

    def __iter__(self):
        """:returns: Iterator of :class:`ConnectionStrings` for this named
                     relation, one per relation id.
        """
        return iter(self[relid]
                    for relid in context.Relations()[self.relation_name])

    @property
    def master(self):
        ''':class:`ConnectionString` to the master, or None.

        If multiple PostgreSQL services are related using this relation
        name then the first master found is returned.
        '''
        for cs in self:
            if cs.master:
                return cs.master

    @property
    def standbys(self):
        '''Set of class:`ConnectionString` to the read-only hot standbys.

        If multiple PostgreSQL services are related using this relation
        name then all standbys found are returned.
        '''
        return set(itertools.chain(*(cs.standbys for cs in self)))

    def connection_string(self, unit=None):
        ''':class:`ConnectionString` to the remote unit, or None.

        unit defaults to the active remote unit.

        You should normally use the master or standbys attributes rather
        than this method.

        If the unit is related multiple times using the same relation
        name, the first one found is returned.
        '''
        if unit is None:
            unit = hookenv.remote_unit()

        relations = context.Relations()
        found = False
        for relation in relations[self.relation_name].values():
            if unit in relation:
                found = True
                conn_str = _cs(relation[unit])
                if conn_str:
                    return conn_str
        if found:
            return None  # unit found, but not yet ready.
        raise LookupError(unit)  # unit not related.


def _cs(reldata):
    """Generate a ConnectionString from :class:``context.RelationInfo``"""
    if not reldata:
        return None
    d = dict(host=reldata.get('host'),
             port=reldata.get('port'),
             dbname=reldata.get('database'),
             user=reldata.get('user'),
             password=reldata.get('password'))
    if not all(d.values()):
        return None
    local_unit = hookenv.local_unit()
    if local_unit not in reldata.get('allowed-units', '').split():
        return None  # Not yet authorized
    locdata = context.Relations()[reldata.relname][reldata.relid].local
    if 'database' in locdata and locdata['database'] != d['dbname']:
        return None  # Requested database does not match yet
    if 'roles' in locdata and locdata['roles'] != reldata.get('roles'):
        return None  # Requested roles have not yet been assigned
    if 'extensions' in locdata and (locdata['extensions'] !=
                                    reldata.get('extensions')):
        return None  # Requested extensions have not yet been installed
    return ConnectionString(**d)
