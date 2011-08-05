
# Postgresql Service


## During db-relation-joined,


### the postgresql service provides:

- `host`
- `user`
- `database`
- `password`

### and requires

- `ip`: the client ip address to enable access

Here's an example client hook providing that

    #!/bin/sh
    IP=`ifconfig  | grep 'inet addr:'| grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $1}'|head -n 1`
    echo setting ip to $IP
    relation-set ip=$IP


## During db-relation-changed,

### provides

### accepts

- `ip`: the client ip address to enable access

