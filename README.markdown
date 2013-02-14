
# Postgresql Service

Typically, you just need to join a the `db` relation, and a user and database
will be created for you.  For more advanced uses, you can join the `db-admin`
relation, and a super user will be created.  Using this account, you can
manipulate all other aspects of the database.

## During db-relation-joined, db-relation-changed

### the postgresql service provides:

- `host`: the host to contact
- `user`: a regular user authorized to read the database
- `database`: a regular database
- `password`: the password for 'user'

### and accepts

- `ip`: deprecated way to specify the client ip address to enable
        access from. This is no longer necessary, you can rely on the
        implicit 'private-address' relation component.

Here's an example client hook providing that

    #!/bin/sh
    relation-set ip=`unit-get private-address`


## During db-admin-relation-joined, db-admin-relation-changed

### the postgresql service provides:

Similar to db-relation-joined/changed, but a super user is created instead.  
No initial database will be created, and all user names are authorized from
the client computer connection.

- `host`: the host to contact
- `user`: A created super user.
- `password`: the super user password
