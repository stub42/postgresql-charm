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

This charm supports several deployment models:

 - A single service containing one unit. This provides a 'standalone'
   environment.

 - A service containing multiple units. One unit will be a 'master', and every
   other unit is a 'hot standby'. The charm sets up and maintains replication
for you, using standard PostgreSQL streaming replication.

To setup a single 'standalone' service::

    juju deploy postgresql pg-a


## Scale Out Usage

To replicate this 'standalone' database to a 'hot standby', turning the
existing unit into a 'master'::

    juju add-unit pg-a

To deploy a new service containing a 'master' and two 'hot standbys'::

    juju deploy -n 2 postgresql pg-b
    [ ... wait until units are stable ... ]
    juju add-unit pg-b

You can remove units as normal. If the master unit is removed, failover occurs
and the most up to date 'hot standby' is promoted to 'master'.  The
'db-relation-changed' and 'db-admin-relation-changed' hooks are fired, letting
clients adjust::

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

⚠ Due to current [limitations][1] with juju, you cannot reliably
create a service initially containing more than 2 units (eg. juju deploy
-n 3 postgresql). Instead, you must first create a service with 2 units.
Once the environment is stable and all the hooks have finished running,
you may add more units.

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

## During db-relation-joined

### the client service provides:

- `database`: Optional. The name of the database to use. The postgresql service
  will create it if necessary. If your charm sets this, then it must wait
  until a matching `database` value is presented on the PostgreSQL side of
  the relation (ie. `relation-get database` returns the value you set).
- `roles`: Optional. A comma separated list of database roles to grant the
  database user. Typically these roles will have been granted permissions to
access the tables and other database objects.  Do not grant permissions
directly to juju generated database users, as the charm may revoke them.

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
inspecting the 'state' property on the relation, and it needs to be
aware of how many units are in the relation by using the 'relation-list'
hook tool.

If there is a single PostgreSQL unit related, the state will be
'standalone'. All database connections of course go to this unit.

If there is more than one PostgreSQL unit related, the client charm
must only use units with state set to 'master' or 'hot standby'.
The unit with 'master' state can accept read and write connections. The
units with 'hot standby' state can accept read-only connections, and
any attempted writes will fail. Units with any other state must not be
used and should be ignored ('standalone' units are new units joining the
service that are not yet setup, and 'failover' state will occur when the
master unit is being shutdown and a new master is being elected).

The client charm needs to watch for state changes in its
relation-changed hook. New units may be added to a single unit service,
and the client charm must stop using existing 'standalone' unit and wait
for 'master' and 'hot standby' units to appear. Units may be removed,
possibly causing a 'hot standby' unit to be promoted to a master, or
even having the service revert to a single 'standalone' unit.


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
        relation_set('roles', 'reporting,standard')  # DB roles required

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

            if remote_state == 'standalone' and len(active_db_units) == 1:
                master_conn_str = conn_str
            elif relation_state == 'master':
                master_conn_str = conn_str
            elif relation_state == 'hot standby':
                slave_conn_strs.append(conn_str)

        update_my_db_config(master=master_conn_str, slaves=slave_conn_strs)

    if __name__ == '__main__':
        hooks.execute(sys.argv)


## Upgrade-charm hook notes

The PostgreSQL charm has deprecated volume-map and volume-ephemeral-storage
configuration options in favor of using the storage subordinate charm for
general external storage management. If the installation being upgraded is
using these deprecated options, there are a couple of manual steps necessary 
to finish migration and continue using the current external volumes.
Even though all data will remain intact, and PostgreSQL service will continue
running, the upgrade-charm hook will intentionally fail and exit 1 as well to
raise awareness of the manual procedure which will also be documented in the
juju logs on the PostgreSQL units.

The following steps must be additionally performed to continue using external
volume maps for the PostgreSQL units once juju upgrade-charm is run from the
command line:
  1. cat > storage.cfg <<EOF
     storage:
       provider:block-storage-broker
       root: /srv/data
       volume_map: "{postgresql/0: your-vol-id, postgresql/1: your-2nd-vol-id}"
     EOF
  2. juju deploy --config storage.cfg storage
  3. juju deploy block-storage-broker
  4. juju add-relation block-storage-broker storage
  5. juju resolved --retry postgresql/0   # for each postgresql unit running
  6. juju add-relation postgresql storage


# Contact Information

## PostgreSQL 

- [PostgreSQL website](http://www.postgresql.org/)
- [PostgreSQL bug submission
  guidelines](http://www.postgresql.org/docs/9.2/static/bug-reporting.html)
- [PostgreSQL Mailing List](http://www.postgresql.org/list/)

  [1]: https://bugs.launchpad.net/charms/+source/postgresql/+bug/1258485
