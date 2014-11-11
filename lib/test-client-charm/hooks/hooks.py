#!/usr/bin/env python

import os.path
import re
import shutil
import sys
from textwrap import dedent

from charmhelpers.core import hookenv, host
from charmhelpers.core.hookenv import log, DEBUG, INFO
from charmhelpers import fetch


CLIENT_RELATION_TYPES = frozenset(['db', 'db-admin'])

DATA_DIR = os.path.join(
    '/var/lib/units', hookenv.local_unit().replace('/', '-'))
SCRIPT_DIR = os.path.join(DATA_DIR, 'bin')
PGPASS_DIR = os.path.join(DATA_DIR, 'pgpass')


def update_system_path():
    org_lines = open('/etc/environment', 'rb').readlines()
    env_lines = []

    for line in org_lines:
        if line.startswith('PATH=') and SCRIPT_DIR not in line:
            line = re.sub(
                """(['"]?)$""",
                ":{}\\1".format(SCRIPT_DIR),
                line, 1)
        env_lines.append(line)

    if org_lines != env_lines:
        content = '\n'.join(env_lines)
        host.write_file('/etc/environment', content, perms=0o644)


def all_relations(relation_types=CLIENT_RELATION_TYPES):
    for reltype in relation_types:
        for relid in hookenv.relation_ids(reltype):
            for unit in hookenv.related_units(relid):
                yield reltype, relid, unit, hookenv.relation_get(
                    unit=unit, rid=relid)


def rebuild_all_relations():
    config = hookenv.config()

    # Clear out old scripts and pgpass files
    if os.path.exists(SCRIPT_DIR):
        shutil.rmtree(SCRIPT_DIR)
    if os.path.exists(PGPASS_DIR):
        shutil.rmtree(PGPASS_DIR)
    host.mkdir(DATA_DIR, perms=0o755)
    host.mkdir(SCRIPT_DIR, perms=0o755)
    host.mkdir(PGPASS_DIR, group='ubuntu', perms=0o750)

    for _, relid, unit, relation in all_relations(relation_types=['db']):
        log("{} {} {!r}".format(relid, unit, relation), DEBUG)

        def_str = '<DEFAULT>'
        if config['database'] != relation.get('database', ''):
            log("Switching from database {} to {}".format(
                relation.get('database', '') or def_str,
                config['database'] or def_str), INFO)

        if config['roles'] != relation.get('roles', ''):
            log("Updating granted roles from {} to {}".format(
                relation.get('roles', '') or def_str,
                config['roles'] or def_str))

        hookenv.relation_set(
            relid, database=config['database'], roles=config['roles'])

        if 'user' in relation:
            rebuild_relation(relid, unit, relation)

    for _, relid, unit, relation in all_relations(relation_types=['db-admin']):
        log("{} {} {!r}".format(relid, unit, relation), DEBUG)
        if 'user' in relation:
            rebuild_relation(relid, unit, relation)


def rebuild_relation(relid, unit, relation):
    relname = relid.split(':')[0]
    unitname = unit.replace('/', '-')
    this_unit = hookenv.local_unit()

    allowed_units = relation.get('allowed-units', '')
    if this_unit not in allowed_units.split():
        log("Not yet authorized on {}".format(relid), INFO)
        return

    script_name = 'psql-{}-{}'.format(relname, unitname)
    build_script(script_name, relation)
    state = relation.get('state', None)
    if state in ('master', 'hot standby'):
        script_name = 'psql-{}-{}'.format(relname, state.replace(' ', '-'))
        build_script(script_name, relation)


def build_script(script_name, relation):
    # Install a wrapper to psql that connects it to the desired database
    # by default. One wrapper per unit per relation.
    script_path = os.path.abspath(os.path.join(SCRIPT_DIR, script_name))
    pgpass_path = os.path.abspath(os.path.join(PGPASS_DIR, script_name))
    script = dedent("""\
        #!/bin/sh
        exec env \\
            PGHOST={host} PGPORT={port} PGDATABASE={database} \\
            PGUSER={user} PGPASSFILE={pgpass} \\
            psql $@
        """).format(
            host=relation['host'],
            port=relation['port'],
            database=relation.get('database', ''),  # db-admin has no database
            user=relation['user'],
            pgpass=pgpass_path)
    log("Generating wrapper {}".format(script_path), INFO)
    host.write_file(
        script_path, script, owner="ubuntu", group="ubuntu", perms=0o700)

    # The wrapper requires access to the password, stored in a .pgpass
    # file so it isn't exposed in an environment variable or on the
    # command line.
    pgpass = "*:*:*:{user}:{password}".format(
        user=relation['user'], password=relation['password'])
    host.write_file(
        pgpass_path, pgpass, owner="ubuntu", group="ubuntu", perms=0o400)


hooks = hookenv.Hooks()


@hooks.hook()
def install():
    fetch.apt_install(
        ['language-pack-en', 'postgresql-client', 'python-psycopg2'],
        fatal=True)
    update_system_path()


@hooks.hook()
def upgrade_charm():
    # Per Bug #1205286, we can't store scripts and passwords in the
    # charm directory.
    if os.path.exists('bin'):
        shutil.rmtree('bin')
    if os.path.exists('pgpass'):
        shutil.rmtree('pgpass')
    update_system_path()
    return rebuild_all_relations()


@hooks.hook(
    'config-changed', 'db-admin-relation-broken',
    'db-admin-relation-changed', 'db-admin-relation-joined',
    'db-relation-broken', 'db-relation-changed', 'db-relation-joined')
def rebuild_hook():
    return rebuild_all_relations()


if __name__ == '__main__':
    hooks.execute(sys.argv)
