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

## During db-relation-changed

### the postgresql service provides:

- `host`: the host to contact
- `database`: a regular database
- `port`: the port PostgreSQL is listening on
- `user`: a regular user authorized to read the database
- `password`: the password for `user`
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

### For clustered support
In order for client charms to support replication:
  - client will need to be aware when relation-list reports > 1 unit of postgresql related
  - When > 1 postgresql units are related:
    - if the client charm needs database write access, they will ignore
      all "standalone", "hot standby" and "failover" states as those will
      likely come from a standby unit (read-only) during standby install,
      setup or teardown
    - If read-only access is needed for a client, acting on
      db-admin-relation-changed "hot standby" state will provide you with a
      readonly replicated copy of the db
  - When 1 postgresql unit is related:
    - watch for updates to the db-admin-relation-changed with "standalone" state
