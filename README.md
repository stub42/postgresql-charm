# Overview

## PostgreSQL

*excerpt from http://www.postgresql.org/about/*

PostgreSQL is a powerful, open source object-relational database system.
It has more than 15 years of active development and a proven
architecture that has earned it a strong reputation for reliability,
data integrity, and correctness. It is fully ACID compliant, has full
support for foreign keys, joins, views, triggers, and stored procedures
(in multiple languages). It includes most SQL:2008 data types, including
INTEGER, NUMERIC, BOOLEAN, CHAR, VARCHAR, DATE, INTERVAL, and TIMESTAMP.
It also supports storage of binary large objects, including pictures,
sounds, or video. It has native programming interfaces for C/C++, Java,
.Net, Perl, Python, Ruby, Tcl, ODBC, among others, and [exceptional
documentation](http://www.postgresql.org/docs/manuals/).

An enterprise class database, PostgreSQL boasts sophisticated features
such as Multi-Version Concurrency Control (MVCC), point in time
recovery, tablespaces, asynchronous replication, nested transactions
(savepoints), online/hot backups, a sophisticated query
planner/optimizer, and write ahead logging for fault tolerance. It
supports international character sets, multibyte character encodings,
Unicode, and it is locale-aware for sorting, case-sensitivity, and
formatting. It is highly scalable both in the sheer quantity of data it
can manage and in the number of concurrent users it can accommodate.
There are active PostgreSQL systems in production environments that
manage in excess of 4 terabytes of data.


# Usage

This charm supports several deployment models:

 - A single service containing one unit. This provides a 'standalone'
   environment.

 - A service containing multiple units. One unit will be a 'master',
   and every other unit is a 'hot standby'. The charm sets up and
   maintains replication for you, using standard PostgreSQL streaming
   replication.


To setup a single 'standalone' service::

    juju deploy postgresql pg-a


To replicate this 'standalone' database to a 'hot standby', turning the
existing unit into a 'master'::

    juju add-unit pg-a

To deploy a new service containing a 'master' and two 'hot standbys'::

    juju deploy -n 3 postgresql pg-b

You can remove units as normal. If the master unit is removed, failover
occurs and the most up to date 'hot standby' is promoted to 'master'.
The 'db-relation-changed' and 'db-admin-relation-changed' hooks are
fired, letting clients adjust::

    juju remove-unit pg-b/0


To setup a client using a PostgreSQL database, in this case a vanilla
Django installation listening on port 8080::

    juju deploy postgresql
    juju deploy python-django
    juju deploy gunicorn
    juju add-relation python-django postgresql:db
    juju add-relation python-django gunicorn
    juju expose python-django


## Restrictions

- Do not attempt to relate client charms to a PostgreSQL service
  containing multiple units unless you know the charm supports
  a replicated service.

- You cannot host multiple units in a single juju container. This is
  problematic as some PostgreSQL features, such as tablespaces, use
  user specified absolute paths.

# Interacting with the Postgresql Service

At a minimum, you just need to join a the `db` relation, and a user and
database will be created for you.  For more complex environments, 
you can provide the `database` name allowing multiple services to share
the same database. A client may also wish to defer its setup until the
unit name is listed in `allowed-units`, to avoid attempting to connect
to a database before it has been authorized.

The `db-admin` relation may be used similarly to the `db` relation.
The automatically generated user for `db-admin` relations is a
PostgreSQL superuser.

## During db-relation-joined

### the client service provides:

- `database`: Optional. The name of the database to use. The postgresql
              service will create it if necessary.
- `roles`: Optional. A comma separated list of database roles to grant
           the database user. Typically these roles will have been granted
           permissions to access the tables and other database objects.
           Do not grant permissions directly to juju generated database
           users, as the charm may revoke them.

## During db-relation-changed

### the postgresql service provides:

- `host`: the host to contact.
- `database`: a regular database.
- `port`: the port PostgreSQL is listening on.
- `user`: a regular user authorized to read the database.
- `password`: the password for `user`.
- `state`: 'standalone', 'master' or 'hot standby'.
- `allowed-units`: space separated list of allowed clients (unit name).
  You should check this to determine if you can connect to the database yet.

## During db-admin-relation-changed

### the postgresql service provides:

- `host`: the host to contact
- `port`: the port PostgreSQL is listening on
- `user`: a created super user
- `password`: the password for `user`
- `state`: 'standalone', 'master' or 'hot standby'
- `allowed-units`: space separated list of allowed clients (unit name).
  You should check this to determine if you can connect to the database yet.

## For replicated database support

A PostgreSQL service may contain multiple units (a single master, and
optionally one or more hot standbys). The client charm can tell which
unit in a relation is the master and which are hot standbys by
inspecting the 'state' property on the relation, and it needs to be
aware of how many units are in the relation by using the 'relation-list'
hook tool.

If there is a single PostgreSQL unit related, the state will be
'standalone'. All database connections of course go to this unit.

If there are more than one PostgreSQL unit related, the client charm
must only use units with state set to 'master' or 'hot standby'.
The unit with 'master' state can accept read and write connections. The
units with 'hot standby' state can accept read-only connections, and
any attempted writes will fail. Units with any other state must not be
used and should be ignored ('standalone' units are new units joining the
service that are not yet setup, and 'failover' state will occur when the
master unit is being shutdown and a new master is being elected).

The client charm needs to watch for state changes in it's
relation-changed hook. New units may be added to a single unit service,
and the client charm must stop using existing 'standalone' unit and wait
for 'master' and 'hot standby' units to appear. Units may be removed,
possibly causing a 'hot standby' unit to be promoted to a master, or
even having the service revert to a single 'standalone' unit.
