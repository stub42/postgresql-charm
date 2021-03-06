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
from contextlib import contextmanager
import os
import re
import shutil
import socket
import stat
import tempfile
import uuid

import yaml

from charmhelpers.core import hookenv, host
from charmhelpers.core.hookenv import WARNING

import context


def distro_codename():
    """Return the distro release code name, eg. 'precise' or 'trusty'."""
    return host.lsb_release()["DISTRIB_CODENAME"]


def get_peer_relation():
    """Return the peer class:`context.Relation`

    We can't use context.Relations().peer to find the peer relation,
    because with multiple peer relations the one it returns is unstable.
    """
    for rel in context.Relations()["replication"].values():
        return rel


def peers():
    """Return the set of peers, not including the local unit."""
    rel = get_peer_relation()
    return frozenset(rel.keys()) if rel else frozenset()


def rewrite(path, content):
    """Rewrite a file atomically, preserving ownership and permissions."""
    attr = os.lstat(path)
    write(
        path,
        content,
        mode=stat.S_IMODE(attr.st_mode),
        user=attr[stat.ST_UID],
        group=attr[stat.ST_GID],
    )


def write(path, content, mode=0o640, user="root", group="root"):
    """Write a file atomically."""
    open_mode = "wb" if isinstance(content, bytes) else "w"
    with tempfile.NamedTemporaryFile(mode=open_mode, delete=False) as f:
        try:
            f.write(content)
            f.flush()
            shutil.chown(f.name, user, group)
            os.chmod(f.name, mode)
            shutil.move(f.name, path)
            # shutil.move fails to preserve ownership if crossing
            # filesystem bounaries, so reset the ownership here.
            # We also do it above to remove the race condition for
            # the normal case.
            shutil.chown(path, user, group)
        finally:
            if os.path.exists(f.name):
                os.unlink(f.name)


def makedirs(path, mode=0o750, user="root", group="root"):
    if os.path.exists(path):
        assert os.path.isdir(path), "{} is not a directory"
    else:
        # Don't specify mode here, to ensure parent dirs are traversable.
        os.makedirs(path)
    shutil.chown(path, user, group)
    os.chmod(path, mode)


@contextmanager
def switch_cwd(new_working_directory="/tmp"):
    "Switch working directory."
    org_dir = os.getcwd()
    os.chdir(new_working_directory)
    try:
        yield new_working_directory
    finally:
        os.chdir(org_dir)


def config_yaml():
    config_yaml_path = os.path.join(hookenv.charm_dir(), "config.yaml")
    with open(config_yaml_path, "r") as f:
        return yaml.safe_load(f)


def deprecated_config_in_use():
    options = config_yaml()["options"]
    config = hookenv.config()
    deprecated = [
        key
        for key in options
        if ("DEPRECATED" in options[key]["description"] and config[key] != options[key]["default"])
    ]
    return set(deprecated)


def cron_dir():
    """Where we put crontab files."""
    return "/etc/cron.d"


def scripts_dir():
    """Where the charm puts adminstrative scripts."""
    return "/var/lib/postgresql/scripts"


def logs_dir():
    """Where the charm administrative scripts log their output."""
    return "/var/lib/postgresql/logs"


def backups_dir():
    """Where pg_dump backups are stored."""
    return hookenv.config()["backup_dir"]


def backups_log_path():
    return os.path.join(logs_dir(), "backups.log")


def split_extra_pg_auth(raw_extra_pg_auth):
    """Yield the extra_pg_auth stanza line by line.

    Uses the input as a multi-line string if valid, or falls
    back to comma separated for backwards compatibility.
    """
    # Lines in a pg_hba.conf file must be comments, whitespace, or begin
    # with 'local' or 'host'.
    valid_re = re.compile(r"^\s*(host.*|local.*|#.*)?\s*$")

    def valid_line(ln):
        return valid_re.search(ln) is not None

    lines = list(raw_extra_pg_auth.split(","))
    if len(lines) > 1 and all(valid_line(ln) for ln in lines):
        hookenv.log("Falling back to comma separated extra_pg_auth", WARNING)
        return lines
    else:
        return raw_extra_pg_auth.splitlines()


def ping_peers():
    peer_rel = get_peer_relation()
    if peer_rel:
        peer_rel.local["ping"] = str(uuid.uuid4())


def ensure_ip(addr):
    """If addr is a hostname, resolve it to an IP address"""
    if not addr:
        return None
    # We need to use socket.getaddrinfo for IPv6 support.
    info = socket.getaddrinfo(addr, None)
    if info is None:
        # Should never happen
        raise ValueError("Invalid result None from getaddinfo")
    try:
        return info[0][4][0]
    except IndexError:
        # Should never happen
        raise ValueError("Invalid result {!r} from getaddinfo".format(info))
