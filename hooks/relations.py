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

from collections import OrderedDict, UserDict
from functools import wraps

from charmhelpers.core import hookenv, host

from decorators import master_only
import helpers
import postgresql


def peer_relid():
    md = hookenv.metadata()
    section = md.get('peers')
    if section:
        for key in section:
            relids = hookenv.relation_ids(key)
            if relids:
                return relids[0]
            break
    return None


class Relations(UserDict):
    '''Mapping relation name -> relid -> Relation(relid).

    >>> rels = Relations()
    >>> rels['sprog']['sprog:12']['client/6']['widget']
    'remote widget'
    >>> rels['sprog']['sprog:12'].local['widget'] = 'local widget'
    >>> rels['sprog']['sprog:12'].local['widget']
    'local widget'
    '''
    def __init__(self):
        super(Relations, self).__init__(
            {relname: {relid: Relation(relid)
                       for relid in hookenv.relation_ids(relname)}
             for relname in hookenv.relation_types()})


class Relation(OrderedDict):
    '''Mapping of unit -> RelationInfo for a relation.

    This is an OrderedDict mapping, ordered numerically by
    by unit number.

    >>> r = Relation('sprog:12')
    >>> r.keys()
    ['client/9', 'client/10']     # Ordered numerically
    >>> r['client/10']['widget']  # A remote RelationInfo
    'remote widget'
    >>> r.local['widget']         # The local RelationInfo
    'local widget'
    '''
    relid = None  # The relation id.
    relname = None  # The relation name (also known as relation type).
    service = None  # The remote service name, if known.

    local = None  # The local end's RelationInfo.

    # Mapping of peer -> RelationInfo. Peers must have joined a peer
    # relation before they appear here. None if there is no peer relation
    # defined.
    peers = None

    def __init__(self, relid):
        remote_units = hookenv.related_units(relid)
        if remote_units:
            remote_units.sort(key=lambda u: int(u.split('/', 1)[-1]))
            data = [(unit, RelationInfo(relid, unit))
                    for unit in remote_units]
        else:
            data = []

        super(Relation, self).__init__(data)

        self.service = data[0][1].service if data else None
        self.relname = relid.split(':', 1)[0]
        self.relid = relid
        self.local = RelationInfo(relid, hookenv.local_unit())

        # If we have peers, and they have joined both the provided peer
        # relation and this relation, we can peek at their data too.
        # This is useful for creating consensus without leadership.
        p_relid = peer_relid()
        if p_relid:
            peers = hookenv.related_units(p_relid)
            if peers:
                peers.sort(key=lambda u: int(u.split('/', 1)[-1]))
                self.peers = OrderedDict((peer, RelationInfo(relid, peer))
                                         for peer in peers)
            else:
                self.peers = OrderedDict()
        else:
            self.peers = None

    def __str__(self):
        return '{} ({})'.format(self.relid, self.service)


class RelationInfo(UserDict):
    '''The bag of data at an end of a relation.

    Every unit participating in a relation has a single bag of
    data associated with that relation. This is that bag.

    The bag of data for the local unit may be updated. Remote data
    is immutable and will remain static for the duration of the hook.

    Changes made to the local units relation data only become visible
    to other units after the hook completes successfully. If the hook
    does not complete successfully, the changes are rolled back.

    Unlike standard Python mappings, setting an item to None is the
    same as deleting it.

    >>> relinfo = RelationInfo('db:12')  # Default is the local unit.
    >>> relinfo['user'] = 'fred'
    >>> relinfo['user']
    'fred'
    >>> relinfo['user'] = None
    >>> 'fred' in relinfo
    False
    '''
    relid = None    # The relation id.
    relname = None  # The relation name (also know as the relation type).
    unit = None     # The unit id.
    number = None   # The unit number (integer).
    service = None  # The service name.

    def __init__(self, relid, unit):
        self.relname = relid.split(':', 1)[0]
        self.relid = relid
        self.unit = unit
        self.service, num = self.unit.split('/', 1)
        self.number = int(num)

    def __str__(self):
        return '{} ({})'.format(self.relid, self.unit)

    @property
    def data(self):
        return hookenv.relation_get(rid=self.relid, unit=self.unit)

    def __setitem__(self, key, value):
        if self.unit != hookenv.local_unit():
            raise TypeError('Attempting to set {} on remote unit {}'
                            ''.format(key, self.unit))
        if value is not None and not isinstance(value, str):
            # We don't do implicit casting. A mechanism to allow
            # automatic serialization to JSON may be useful for
            # non-strings, but should it be the default or always on?
            raise ValueError('Only string values supported')
        hookenv.relation_set(self.relid, {key: value})

    def __delitem__(self, key):
        # Deleting a key and setting it to null is the same thing in
        # Juju relations.
        self[key] = None


def relations():
    return Relations()


def relation_handler(*relnames):
    '''Invoke the decorated function once per matching relation.

    The decorated function should accept the Relation() instance
    as its single parameter.
    '''
    assert relnames, 'relation names required'
    def decorator(func):
        @wraps(func)
        def wrapper(servicename):
            rels = relations()
            for relname in relnames:
                for rel in rels[relname].values():
                    func(rel)
        return wrapper
    return decorator


@relation_handler('db', 'db-admin')
def publish_db_relations(rel):
    if postgresql.is_master():
        superuser = (rel.relname == 'db-admin')
        db_relation_master(rel, superuser=superuser)
    else:
        db_relation_mirror(rel)
    db_relation_common(rel)


def db_relation_master(rel, superuser):
    '''The master generates credentials and negotiates resources.'''
    master = rel.local
    # Pick one remote unit as representative. They should all converge.
    for remote in rel.values():
        break

    # The requested database name, the existing database name, or use
    # the remote service name as a default. We no longer use the
    # relation id for the database name or usernames, as when a
    # database dump is restored into a new Juju environment we
    # are more likely to have matching service names than relation ids
    # and less likely to have to perform manual permission and ownership
    # cleanups.
    if 'database' in remote:
        master['database'] = remote['database']
    elif 'database' not in master:
        master['database'] = remote.service

    if 'user' not in master:
        user = postgresql.username(remote.service, superuser=superuser)
        master['user'] = user
        master['password'] = host.pwgen()

        # schema_user has never been documented and is deprecated.
        master['schema_user'] = user + '_schema'
        master['schema_password'] = host.pwgen()

    hookenv.log('** Master providing {} ({}/{})'.format(rel,
                                                        master['database'],
                                                        master['user']))

    # Reflect these settings back so the client knows when they have
    # taken effect.
    master['roles'] = remote.get('roles')
    master['extensions'] = remote.get('extensions')


def db_relation_mirror(rel):
    '''Non-masters mirror relation information from the master.'''
    master = postgresql.master()
    master_keys = ['database', 'user', 'password', 'roles',
                   'schema_user', 'schema_password', 'extensions']
    master_info = rel.peers.get(master)
    if master_info is None:
        hookenv.log('Waiting for {} to join {}'.format(rel))
        return
    hookenv.log('Mirroring {} database credentials from {}'.format(rel,
                                                                   master))
    rel.local.update({k: master.get(k) for k in master_keys})


def db_relation_common(rel):
    '''Publish unit specific relation details.'''
    local = rel.local
    if 'database' not in local:
        return  # Not yet ready.

    # Version number, allowing clients to adjust or block if their
    # expectations are not met.
    local['version'] = postgresql.version()

    # Calculate the state of this unit. 'standalone' will disappear
    # in a future version of this interface, as this state was
    # only needed to deal with race conditions now solved by
    # Juju leadership.
    if postgresql.is_primary():
        if hookenv.is_leader() and len(helpers.peers()) == 0:
            local['state'] = 'standalone'
        else:
            local['state'] = 'master'
    else:
        local['state'] = 'hot standby'

    # Host is the private ip address, but this might change and
    # become the address of an attached proxy or alternative peer
    # if this unit is in maintenance.
    local['host'] = hookenv.unit_private_ip()

    # Port will be 5432, unless the user has overridden it or
    # something very weird happened when the packages where installed.
    local['port'] = str(postgresql.port())

    # The list of remote units on this relation granted access.
    # This is to avoid the race condition where a new client unit
    # joins an existing client relation and sees valid credentials,
    # before we have had a chance to grant it access.
    local['allowed-units'] = ' '.join(rel.keys())


@master_only
@relation_handler('db', 'db-admin')
def ensure_db_relation_resources(rel):
    '''Create the database resources needed for the relation.'''
    superuser = (rel.relname == 'db-admin')
    master = rel.local

    hookenv.log('Ensuring database {!r} and user {!r} exist for {}'
                ''.format(master['database'], master['user'], rel))

    # First create the database, if it isn't already.
    postgresql.ensure_database(master['database'])

    # Next, connect to the database to create the rest in a transaction.
    con = postgresql.connect(database=master['database'])

    postgresql.ensure_user(con, master['user'], master['password'],
                           superuser=superuser)
    postgresql.ensure_user(con,
                           master['schema_user'], master['schema_password'])

    # Grant specified privileges on the database to the user. This comes
    # from the PostgreSQL service configuration, as allowing the
    # relation to specify how much access it gets is insecure.
    config = hookenv.config()
    privs = set(filter(None,
                       config['relation_database_privileges'].split(',')))
    postgresql.grant_database_privileges(con, master['user'],
                                         master['database'], privs)
    postgresql.grant_database_privileges(con, master['schema_user'],
                                         master['database'], privs)

    # Reset the roles granted to the user as requested.
    if 'roles' in master:
        roles = filter(None, master.get('roles', '').split(','))
        postgresql.reset_user_roles(con, master['user'], roles)

    # Create requested extensions. We never drop extensions, as there
    # may be dependent objects.
    if 'extensions' in master:
        extensions = filter(None, master.get('extensions', '').split(','))
        postgresql.ensure_extensions(con, extensions)

    con.commit()  # Don't throw away our changes.


# class SyslogRelation(RelationContext):
#     name = 'syslog'
#     interface = 'syslog'
#
#     def get_data(self):
#         self.programname = hookenv.local_unit().replace('/', '_')
#         return super(SyslogRelation, self).get_data()
#
#     def provide_data(self, remote_service, service_ready):
#         config = hookenv.config()
#         pg_conf = config['postgresql_conf']
#         return dict(log_line_prefix=pg_conf['log_line_prefix'],
#                     programname=self.programname)
#
#
# @hooks.hook()
# def syslog_relation_changed():
#     configure_log_destination(_get_postgresql_config_dir())
#     postgresql_reload()
#
#     # We extend the syslog interface by exposing the log_line_prefix.
#     # This is required so consumers of the PostgreSQL logs can decode
#     # them. Consumers not smart enough to cope with arbitrary prefixes
#     # can at a minimum abort if they detect it is set to something they
#     # cannot support. Similarly, inform the consumer of the programname
#     # we are using so they can tell one units log messages from another.
#     hookenv.relation_set(
#         log_line_prefix=hookenv.config('log_line_prefix'),
#         programname=sanitize(hookenv.local_unit()))
#
#     template_path = "{0}/templates/rsyslog_forward.conf".format(
#         hookenv.charm_dir())
#     rsyslog_conf = Template(open(template_path).read()).render(
#         local_unit=sanitize(hookenv.local_unit()),
#         raw_local_unit=hookenv.local_unit(),
#         raw_remote_unit=hookenv.remote_unit(),
#         remote_addr=hookenv.relation_get('private-address'))
#     host.write_file(rsyslog_conf_path(hookenv.remote_unit()), rsyslog_conf)
#     run(['service', 'rsyslog', 'restart'])
#
#
# @hooks.hook()
# def syslog_relation_departed():
#     configure_log_destination(_get_postgresql_config_dir())
#     postgresql_reload()
#     os.unlink(rsyslog_conf_path(hookenv.remote_unit()))
#     run(['service', 'rsyslog', 'restart'])
#
#
# def configure_log_destination(config_dir):
#     """Set the log_destination PostgreSQL config flag appropriately"""
#     # We currently support either 'standard' logs (the files in
#     # /var/log/postgresql), or syslog + 'standard' logs. This should
#     # grow more complex in the future, as the local logs will be
#     # redundant if you are using syslog for log aggregation, and we
#     # probably want to add csvlog in the future. Note that csvlog
#     # requires switching from 'Debian' log redirection and rotation to
#     # the PostgreSQL builtin facilities.
#     logdest_conf_path = os.path.join(config_dir, 'juju_logdest.conf')
#     logdest_conf = open(logdest_conf_path, 'w')
#     if hookenv.relation_ids('syslog'):
#         # For syslog, we change the ident from the default of 'postgres'
#         # to the unit name to allow remote services to easily identify
#         # and filter which unit messages are from. We don't use IP
#         # address for this as it is not necessarily unique.
#         logdest_conf.write(dedent("""\
#                 log_destination='stderr,syslog'
#                 syslog_ident={0}
#                 """).format(sanitize(hookenv.local_unit())))
#     else:
#         open(logdest_conf_path, 'w').write("log_destination='stderr'")
#
#
# def rsyslog_conf_path(remote_unit):
#     return '/etc/rsyslog.d/juju-{0}-{1}.conf'.format(
#         sanitize(hookenv.local_unit()), sanitize(remote_unit))
#
