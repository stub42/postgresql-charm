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

 - Multiple services linked using 'master'/'slave' relationships. A
   single service can be the 'master', and multiple services connected
   to this master in a 'slave' role. Each service can contain multiple
   units; the 'master' service will contain a single 'master' unit and
   remaining units all 'hot standby'. The 'slave' services will only
   contain 'hot standby' units. 'Cascading replication is not
   supported', so do not attempt to relate an existing 'slave' service
   as a 'master' to another service.


To setup a single 'standalone' service::

    juju deploy postgresql pg-a


To replicate this 'standalone' database to a 'hot standby', turning the
existing unit into a 'master'::

    juju add-unit pg-a

To deploy a new service containing a 'master' and a 'hot standby'::

    juju deploy -n 2 postgresql pg-b


To relate a PostgreSQL service as a 'slave' of another PostgreSQL service.
**Caution** - this destroys the existing databases in the pg-b service::

    juju add-relation pg-a:master pg-b:slave


To setup a client using a PostgreSQL database, in this case OpenERP and
its web front end. Note that OpenERP requires an administrative level
connection::

    juju deploy postgresql
    juju deploy postgresql pg-standby
    juju deploy openerp-web
    juju deploy openerp-server

    juju add-relation postgresql:master pg-standby:slave
    juju add-relation openerp-server:db postgresql:db-admin
    juju add-relation openerp-web openerp-server

    juju expose openerp-web
    juju expose openerp-server


## Restrictions

- Do not attempt to relate client charms to a PostgreSQL service
  containing multiple units unless you know the charm supports
  a replicated service. You can use a 'master'/'slave' relationship
  to create a redundant copy of your database until the client charms
  are updated.

- You cannot host multiple units in a single juju container. This is
  problematic as some PostgreSQL features, such as tablespaces, use
  user specified absolute paths.

# Interacting with the Postgresql Service

Typically, you just need to join a the `db` relation, and a user and database
will be created for you.  For more advanced uses, you can join the `db-admin`
relation, and a super user will be created.  Using this account, you can
manipulate all other aspects of the database.

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
