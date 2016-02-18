# Overview

*excerpt from http://www.postgresql.org/about/*

PostgreSQL is a powerful, open source object-relational database system.  It
has more than 15 years of active development and a proven architecture that has
earned it a strong reputation for reliability, data integrity, and correctness.
It is fully ACID compliant, has full support for foreign keys, joins, views,
triggers, and stored procedures (in multiple languages). It includes most
SQL:2008 data types, including INTEGER, NUMERIC, BOOLEAN, CHAR, VARCHAR, DATE,
INTERVAL, and TIMESTAMP.  It also supports storage of binary large objects,
including pictures, sounds, or video. It has native programming interfaces for
C/C++, Java, .Net, Perl, Python, Ruby, Tcl, ODBC, among others, and
[exceptional documentation](http://www.postgresql.org/docs/manuals/).

An enterprise class database, PostgreSQL boasts sophisticated features such as
Multi-Version Concurrency Control (MVCC), point in time recovery, tablespaces,
asynchronous replication, nested transactions (savepoints), online/hot backups,
a sophisticated query planner/optimizer, and write ahead logging for fault
tolerance. It supports international character sets, multibyte character
encodings, Unicode, and it is locale-aware for sorting, case-sensitivity, and
formatting. It is highly scalable both in the sheer quantity of data it can
manage and in the number of concurrent users it can accommodate.  There are
active PostgreSQL systems in production environments that manage in excess of 4
terabytes of data.

# Usage

This charm can deploy a single standalone PostgreSQL unit, or a service
containing a single master unit and one or more replicas.

To setup a single 'standalone' service::

    juju deploy postgresql pg-a


## Scale Out Usage

To add a replica to an existing service::

    juju add-unit pg-a

To deploy a new service containing a master and two hot standby replicas::

    juju deploy -n 3 postgresql pg-b

You can remove units as normal. If the master unit is removed, failover occurs
and the most up to date hot standby is promoted to the master.  The
'db-relation-changed' and 'db-admin-relation-changed' hooks are fired,
letting clients adjust::

    juju remove-unit pg-b/0


To setup a client using a PostgreSQL database, in this case a vanilla Django
installation listening on port 8080::

    juju deploy postgresql
    juju deploy python-django
    juju deploy gunicorn
    juju add-relation python-django postgresql:db
    juju add-relation python-django gunicorn
    juju expose python-django


## Known Limitations and Issues

⚠ Do not attempt to relate client charms to a PostgreSQL service containing
  multiple units unless you know the charm supports a replicated service.

⚠ To host multiple units on a single server, you must use an lxc
container.


# Interacting with the Postgresql Service

At a minimum, you just need to join a the `db` relation, and a user and
database will be created for you.  For more complex environments, you can
provide the `database` name allowing multiple services to share the same
database. A client may also wish to defer its setup until the unit name is
listed in `allowed-units`, to avoid attempting to connect to a database before
it has been authorized.

The `db-admin` relation may be used similarly to the `db` relation.  The
automatically generated user for `db-admin` relations is a PostgreSQL
superuser.

## Database Permissions and Disaster Recovery

⚠ These two topics are entwined, because failing to follow best
  practice with your database permissions will make your life difficult
  when you need to recover after failure.

PostgreSQL has comprehensive database security, including ownership
and permissions on database objects. By default, any objects a client
service creates will be owned by a user with the same name as the
client service and inaccessible to other users. To share data, it
is best to create new roles, grant the relevant permissions and object
ownership to the new roles and finally grant these roles to the users
your services can connect as. This also makes disaster recovery easier.
If you restore a database into an indentical Juju environment, then
the service names and usernames will be the same and database permissions
will match. However, if you restore a database into an environment
with different client service names then the usernames will not match
and the new users not have access to your data.

Learn about the SQL `GRANT` statement in the excellect [PostgreSQL
reference guide][3].


### block-storage-broker

If you are using external storage provided by the block storage broker,
recovery or a failed unit is simply a matter of ensuring the old unit
is fully shut down, and then bringing up a fresh unit with the old
external storage mounted. The charm will see the old database there
and use it.

If you are unable or do not wish to to simply remount the same
filesystem, you can of course copy all the data from the old filesystem
to the new one before bringing up the new unit.

### dump/restore

PostgreSQL dumps, such as those that can be scheduled in the charm, can
be recovered on a new unit by using 'juju ssh' to connect to the new unit
and using the standard PostgreSQL `pg_restore(1)` tool. This new unit must
be standalone, or the master unit. Any hot standbys will replicate the
recovered data from the master.

You will need to use `pg_restore(1)` with the `--no-owner` option, as
users that existed in the old service will not exist in the new
service.

### PITR

If you had configured WAL-E, you can recover a WAL-E backup and replay
to a point in time of your choosing using the `wal-e` tool. This
will recover the whole database cluster, so all databases will be
replaced.

If there are any hot standby units, they will need to be destroyed
and recreated after the PITR recovery.


## During db-relation-joined

### the client service provides:

- `database`: Optional. The name of the database to use. The postgresql service
  will create it if necessary. If your charm sets this, then it must wait
  until a matching `database` value is presented on the PostgreSQL side of
  the relation (ie. `relation-get database` returns the value you set).
- `roles`: Deprecated. A comma separated list of database roles to grant the
  database user. Typically these roles will have been granted permissions to
  access the tables and other database objects.
- `extensions`: Optional. A comma separated list of required postgresql
  extensions.

## During db-relation-changed

### the postgresql service provides:

- `host`: the host to contact.
- `database`: a regular database.
- `port`: the port PostgreSQL is listening on.
- `user`: a regular user authorized to read the database.
- `password`: the password for `user`.
- `state`: 'standalone', 'master' or 'hot standby'.
- `allowed-units`: space separated list of allowed clients (unit name).  You
  should check this to determine if you can connect to the database yet.

## During db-admin-relation-changed

### the postgresql service provides:

- `host`: the host to contact
- `port`: the port PostgreSQL is listening on
- `user`: a created super user
- `password`: the password for `user`
- `state`: 'standalone', 'master' or 'hot standby'
- `allowed-units`: space separated list of allowed clients (unit name).  You
  should check this to determine if you can connect to the database yet.

## During syslog-relation-changed

### the postgresql service provides:

- `programname`: the syslog 'programname' identifying this unit's
  PostgreSQL logs.
- `log_line_prefix`: the `log_line_prefix` setting for the PostgreSQL
  service.


## For replicated database support

A PostgreSQL service may contain multiple units (a single master, and
optionally one or more hot standbys). The client charm can tell which
unit in a relation is the master and which are hot standbys by
inspecting the 'state' property on the relation.

The 'standalone' state is deprecated, and when a unit advertises itself
as 'standalone' you should treat it like a 'master'. The state only exists
for backwards compatibility and will be removed soon.

Writes must go to the unit identifying itself as 'master' or 'standalone'.
If you sent writes to a 'hot standby', they will fail.

Reads may go to any unit. Ideally they should be load balanced over
the 'hot standby' units. If you need to ensure consistency, you may
need to read from the 'master'.

Units in any other state, including no state, should not be used and
connections to them will likely fail. These units may still be setting
up, or performing a maintenance operation such as a failover.

The client charm needs to watch for state changes in its
relation-changed hook. During failover, one of the existing 'hot standby' 
units will change into a 'master'.


## Example client hooks

Python::

    import sys
    from charmhelpers.core.hookenv import (
        Hooks, config, relation_set, relation_get,
        local_unit, related_units, remote_unit)

    hooks = Hooks()
    hook = hooks.hook

    @hook
    def db_relation_joined():
        relation_set('database', config('database'))  # Explicit database name

    @hook('db-relation-changed', 'db-relation-departed')
    def db_relation_changed():
        # Rather than try to merge in just this particular database
        # connection that triggered the hook into our existing connections,
        # it is easier to iterate over all active related databases and
        # reset the entire list of connections.
        conn_str_tmpl = "dbname={dbname} user={user} host={host} port={port}"
        master_conn_str = None
        slave_conn_strs = []
        for db_unit in related_units():
            if relation_get('database', db_unit) != config('database'):
                continue  # Not yet acknowledged requested database name.

            allowed_units = relation_get('allowed-units') or ''  # May be None
            if local_unit() not in allowed_units.split():
                continue  # Not yet authorized.

            conn_str = conn_str_tmpl.format(**relation_get(unit=db_unit)
            remote_state = relation_get('state', db_unit)

            if remote_state in ('master', 'standalone'):
                master_conn_str = conn_str
            elif relation_state == 'hot standby':
                slave_conn_strs.append(conn_str)

        update_my_db_config(master=master_conn_str, slaves=slave_conn_strs)

    if __name__ == '__main__':
        hooks.execute(sys.argv)


# Point In Time Recovery

The PostgreSQL charm has support for log shipping and point in time
recovery. This feature uses the wal-e[2] tool, which will be
installed from the Launchpad PPA ppa:stub/pgcharm. This feature
requires access to either Amazon S3, Microsoft Azure Block Storage or
Swift. This feature is experimental because it has only been tested with
Swift. The charm can be configured to perform regular filesystem backups
and ship WAL files to the object store. Hot standbys will make use of
the archived WAL files, allowing them to resync after extended netsplits
or even let you turn off streaming replication entirely.

With a base backup and the WAL archive you can perform point in time
recovery, but this is still a manual process and the charm does not
yet help you do it. The simplest approach would be to create a new
PostgreSQL service containing a single unit, 'juju ssh' in and use
wal-e to replace the database after shutting it down, create a
recovery.conf to replay the archived WAL files using wal-e, restart the
database and wait for it to recover. Once recovered, new hot standby
units can be added and client services related to the new database
service.

To enable the experimental wal-e support with Swift, you will need to
and set the service configuration settings similar to the following::

    postgresql:
        wal_e_storage_uri: swift://mycontainer
        os_username: my_swift_username
        os_password: my_swift_password
        os_auth_url: https://keystone.auth.url.example.com:8080/v2/
        os_tenant_name: my_tenant_name
        install_sources: |
            - ppa:stub/pgcharm
            - cloud:icehouse


# Development and Contributions

The PostgreSQL Charm is maintained on Launchpad[4] using git. The 'master'
branch is a Reactive Framework Layer, and generates a deployable Charm
using the 'charm build' command provided by charm-tools.

The latest stable source layer is in the 'master' branch in the
git+ssh://git.launchpad.net/postgresql-charm repository. Merge proposals
should be made against the 'master' branch. Do not make merge proposals
against the old Bazaar branches or the 'built' branch.


# Support

Bug reports can be made at https://bugs.launchpad.net/postgresql-charm.
Queries can be made in any of the major Juju forums, such as the main
Juju mailing list or the #juju channel on Freenode IRC.


## Latest Stable

The latest tested, stable and deployable charm is stored in the 'built'
branch in the git+ssh://git.launchpad.net/postgresql-charm repository::

    mkdir trusty
    git clone -b built \
        https://git.launchpad.net/postgresql-charm trusty/postgresql
    JUJU_REPOSITORY=. juju deploy local:postgresql


# References 

- [PostgreSQL website](http://www.postgresql.org/)
- [PostgreSQL Mailing List](http://www.postgresql.org/list/)

  [1]: https://bugs.launchpad.net/charms/+source/postgresql/+bug/1258485
  [2]: https://github.com/wal-e/wal-e
  [3]: http://www.postgresql.org/docs/9.3/static/sql-grant.html
  [4]: https://launchpad.net/postgresql-charm
