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
import json
from itertools import chain
import re

from charmhelpers.core import hookenv, host
from charms import leadership
from charms import reactive
from charms.reactive import not_unless, when, when_not

import context
from reactive.postgresql import helpers
from reactive.postgresql import replication
from reactive.postgresql import postgresql
from reactive.postgresql.service import incoming_addresses
from relations.pgsql.requires import ConnectionString

from everyhook import everyhook


# @hook('{interface:pgsql}-relation-changed',
#       'replication-relation-changed',
#       'leader-settings-changed',
#       'leader-elected',
#       'config-changed',
#       'upgrade-charm')
@everyhook
def publish_client_relations():
    reactive.remove_state("postgresql.client.published")
    reactive.remove_state("postgresql.client.passwords_set")


CLIENT_RELNAMES = frozenset(["db", "db-admin", "master"])


@when("leadership.is_leader")
@when_not("postgresql.client.passwords_set")
def set_client_passwords():
    """The leader chooses passwords for client connections.

    Storing the passwords in the leadership settings is the most
    reliable way of distributing them to peers.
    """
    raw = leadership.leader_get("client_passwords")
    pwds = json.loads(raw) if raw else {}
    rels = context.Relations()
    updated = False
    for relname in CLIENT_RELNAMES:
        for rel in rels[relname].values():
            superuser, replication = _credential_types(rel)
            for remote in rel.values():
                user = postgresql.username(remote.service, superuser=superuser, replication=replication)
                if user not in pwds:
                    password = host.pwgen()
                    pwds[user] = password
                    updated = True
    if updated:
        leadership.leader_set(client_passwords=json.dumps(pwds, sort_keys=True))
    reactive.set_state("postgresql.client.passwords_set")


def get_client_password(username):
    raw = leadership.leader_get("client_passwords")
    pwds = json.loads(raw) if raw else {}
    return pwds.get(username)


@when("postgresql.replication.is_master")
@when("postgresql.replication.is_primary")
@when("postgresql.cluster.is_running")
@when_not("postgresql.client.published")
def master_provides():
    """The master publishes client connection details.

    Note that this may not be happening in the -relation-changed
    hook, as this unit may not have been the master when the relation
    was joined.
    """
    rels = context.Relations()
    for relname in CLIENT_RELNAMES:
        for rel in rels[relname].values():
            if len(rel):
                db_relation_master(rel)
                db_relation_common(rel)
                ensure_db_relation_resources(rel)
    reactive.set_state("postgresql.client.published")
    # Now we know the username and database, ensure pg_hba.conf gets
    # regenerated to match and the clients can actually login.
    reactive.remove_state("postgresql.cluster.configured")


@when("postgresql.replication.master.authorized")
@when("postgresql.cluster.is_running")
@when_not("postgresql.client.published")
def mirror_master():
    """A standby mirrors client connection details from the master.

    The master pings its peers using the peer relation to ensure a hook
    is invoked and this handler called after the credentials have been
    published.
    """
    rels = context.Relations()
    for relname in CLIENT_RELNAMES:
        for rel in rels[relname].values():
            db_relation_mirror(rel)
            db_relation_common(rel)
    reactive.set_state("postgresql.client.published")
    # Now we know the username and database, ensure pg_hba.conf gets
    # regenerated to match and the clients can actually login.
    reactive.remove_state("postgresql.cluster.configured")


def _credential_types(rel):
    superuser = rel.relname in ("db-admin", "master")
    replication = rel.relname == "master"
    return (superuser, replication)


@not_unless("postgresql.replication.is_master")
def db_relation_master(rel):
    """The master generates credentials and negotiates resources."""
    master = rel.local
    org_master = dict(master)

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
    if "database" in remote:
        master["database"] = remote["database"]
    elif "database" not in master:
        master["database"] = remote.service

    superuser, replication = _credential_types(rel)

    if "user" not in master:
        user = postgresql.username(remote.service, superuser=superuser, replication=replication)
        password = get_client_password(user)
        if not password:
            hookenv.log("** Master waiting for {} password".format(user))
            return
        master["user"] = user
        master["password"] = password

        # schema_user has never been documented and is deprecated.
        if not superuser:
            master["schema_user"] = user
            master["schema_password"] = password

    hookenv.log("** Master providing {} ({}/{})".format(rel, master["database"], master["user"]))

    # Reflect these settings back so the client knows when they have
    # taken effect.
    if not replication:
        master["roles"] = remote.get("roles")
        master["extensions"] = remote.get("extensions")

    # If things have changed, ping peers so they can remirror.
    if dict(master) != org_master:
        ping_standbys()


def db_relation_mirror(rel):
    """Non-masters mirror relation information from the master."""
    master = replication.get_master()
    master_keys = [
        "database",
        "user",
        "password",
        "roles",
        "schema_user",
        "schema_password",
        "extensions",
    ]
    master_info = rel.peers.get(master)
    if master_info is None:
        hookenv.log("Waiting for {} to join {}".format(master, rel))
        return
    hookenv.log("Mirroring {} database credentials from {}".format(rel, master))
    rel.local.update({k: master_info.get(k) for k in master_keys})


def db_relation_common(rel):
    """Publish unit specific relation details."""
    local = rel.local
    if "database" not in local:
        return  # Not yet ready.

    # Version number, allowing clients to adjust or block if their
    # expectations are not met.
    local["version"] = postgresql.version()

    # Calculate the state of this unit. 'standalone' will disappear
    # in a future version of this interface, as this state was
    # only needed to deal with race conditions now solved by
    # Juju leadership. We check for is_primary() rather than
    # the postgresql.replication.is_master reactive state to
    # publish the correct state when we are using manual replication
    # (there might be multiple independent masters, possibly useful for
    # sharding, or perhaps this is a multi master BDR setup).
    if postgresql.is_primary():
        if reactive.helpers.is_state("postgresql.replication.has_peers"):
            local["state"] = "master"
        else:
            local["state"] = "standalone"
    else:
        local["state"] = "hot standby"

    # Host is the private ip address, but this might change and
    # become the address of an attached proxy or alternative peer
    # if this unit is in maintenance.
    local["host"] = ingress_address(local.relname, local.relid)

    # Port will be 5432, unless the user has overridden it or
    # something very weird happened when the packages where installed.
    local["port"] = str(postgresql.port())

    # The list of remote units on this relation granted access.
    # This is to avoid the race condition where a new client unit
    # joins an existing client relation and sees valid credentials,
    # before we have had a chance to grant it access.
    local["allowed-units"] = " ".join(unit for unit, relinfo in rel.items() if len(incoming_addresses(relinfo)) > 0)

    # The list of IP address ranges on this relation granted access.
    # This will replace allowed-units, which does not work with cross
    # model ralations due to the anonymization of the external client.
    local["allowed-subnets"] = ",".join(
        sorted({r: True for r in chain(*[incoming_addresses(relinfo) for relinfo in rel.values()])}.keys())
    )

    # v2 protocol. Publish connection strings for this unit and its peers.
    # Clients should use these connection strings in favour of the old
    # host, port, database settings. A single proxy unit can thus
    # publish several end points to clients.
    master = replication.get_master()
    if replication.is_master():
        master_relinfo = local
    else:
        master_relinfo = rel.peers.get(master)
    local["master"] = relinfo_to_cs(master_relinfo)
    if rel.peers:
        all_relinfo = rel.peers.values()
    all_relinfo = list(rel.peers.values()) if rel.peers else []
    all_relinfo.append(rel.local)
    standbys = filter(
        None,
        [relinfo_to_cs(relinfo) for relinfo in all_relinfo if relinfo.unit != master],
    )
    local["standbys"] = "\n".join(sorted(standbys)) or None


def relinfo_to_cs(relinfo):
    """Generate a ConnectionString from :class:``context.RelationInfo``"""
    if relinfo is None:
        return None
    d = dict(
        host=relinfo.get("host"),
        port=relinfo.get("port"),
        dbname=relinfo.get("database"),
        user=relinfo.get("user"),
        password=relinfo.get("password"),
    )
    if not all(d.values()):
        return None
    return ConnectionString(**d)


def ping_standbys():
    helpers.ping_peers()


@not_unless("postgresql.replication.is_primary")
def ensure_db_relation_resources(rel):
    """Create the database resources needed for the relation."""

    master = rel.local

    if "password" not in master:
        return

    hookenv.log("Ensuring database {!r} and user {!r} exist for {}" "".format(master["database"], master["user"], rel))

    # First create the database, if it isn't already.
    postgresql.ensure_database(master["database"])

    # Next, connect to the database to create the rest in a transaction.
    con = postgresql.connect(database=master["database"])

    superuser, replication = _credential_types(rel)
    postgresql.ensure_user(
        con,
        master["user"],
        master["password"],
        superuser=superuser,
        replication=replication,
    )
    if not superuser:
        postgresql.ensure_user(con, master["schema_user"], master["schema_password"])

    # Grant specified privileges on the database to the user. This comes
    # from the PostgreSQL service configuration, as allowing the
    # relation to specify how much access it gets is insecure.
    config = hookenv.config()
    privs = set(filter(None, config["relation_database_privileges"].split(",")))
    postgresql.grant_database_privileges(con, master["user"], master["database"], privs)
    if not superuser:
        postgresql.grant_database_privileges(con, master["schema_user"], master["database"], privs)

    # Reset the roles granted to the user as requested.
    if "roles" in master:
        roles = filter(None, master.get("roles", "").split(","))
        postgresql.grant_user_roles(con, master["user"], roles)

    # Create requested extensions. We never drop extensions, as there
    # may be dependent objects.
    if "extensions" in master:
        extensions = list(filter(None, master.get("extensions", "").split(",")))
        # Convert to the (extension, schema) tuple expected by
        # postgresql.ensure_extensions
        for i in range(0, len(extensions)):
            m = re.search(r"^\s*([^(\s]+)\s*(?:\((\w+)\))?", extensions[i])
            if m is None:
                raise RuntimeError("Invalid extension {}".format(extensions[i]))
            extensions[i] = (m.group(1), m.group(2) or "public")
        postgresql.ensure_extensions(con, extensions)

    con.commit()  # Don't throw away our changes.


def ingress_address(endpoint, relid):
    # Work around https://github.com/juju/charm-helpers/issues/112
    if not hookenv.has_juju_version("2.3"):
        return hookenv.unit_private_ip()

    try:
        d = hookenv.network_get(endpoint, relid)
        return d["ingress-addresses"][0]
    except NotImplementedError:
        # Warn, although this is normal with older Juju.
        hookenv.log(
            "Unable to determine ingress address, " "falling back to private ip",
            hookenv.WARNING,
        )
        return hookenv.unit_private_ip()
