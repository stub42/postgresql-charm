#!/usr/bin/env python

from __future__ import print_function


from contextlib import contextmanager
from distutils.version import StrictVersion
import psycopg2
import sys


EXCLUDE_DBS = ['postgres', 'template0', 'template1']


STATS = {
    'per-server': [
        {
            'table': 'pg_stat_bgwriter',
            'exclude_columns': ['stats_reset'],
        },
        # Some very interesting things, but not easy to create a key for,
        #  perhaps need to build something to sample the information here
        #  and summarize
        # pg_stat_activity

        # Not sure what the key would be, as I'm not sure what would be
        #  unique. Maybe it would have to be pid, which isn't great for
        #  graphing
        # pg_stat_replication
    ],
    'per-db': [
        {
            'query': 'SELECT * FROM pg_stat_database WHERE datname=%(dbname)s;',
            'exclude_columns': ['datid', 'datname', 'stats_reset'],
        },
        {
            'query': 'SELECT * FROM pg_stat_database_conflicts WHERE datname=%(dbname)s;',
            'exclude_columns': ['datid', 'datname'],
        },
        {
            'table': 'pg_stat_user_tables',
            'key': ['schemaname', 'relname'],
            'alias': 'tables',
            'exclude_columns': ['relid', 'last_vacuum', 'last_autovacuum',
                                'last_analyze', 'last_autoanalyze'],
        },
        {
            'table': 'pg_stat_user_indexes',
            'key': ['schemaname', 'relname', 'indexrelname'],
            'alias': 'indexes',
            'exclude_columns': ['relid', 'indexrelid'],
        },
        {
            'table': 'pg_statio_user_tables',
            'key': ['schemaname', 'relname'],
            'alias': 'tables',
            'exclude_columns': ['relid'],
        },
        {
            'table': 'pg_statio_user_indexes',
            'key': ['schemaname', 'relname', 'indexrelname'],
            'alias': 'indexes',
            'exclude_columns': ['relid', 'indexrelid'],
        },
        {
            'table': 'pg_statio_user_sequences',
            'key': ['schemaname', 'relname'],
            'alias': 'sequences',
            'exclude_columns': ['relid'],
        },
        {
            'table': 'pg_stat_user_functions',
            'key': ['schemaname', 'funcname'],
            'alias': 'user_functions',
            'exclude_columns': ['funcid'],
        },
        # Do we really care about sending all of the system table information?
        # pg_stat_sys_tables
        # pg_stat_sys_indexes
        # pg_statio_sys_tables
        # pg_statio_sys_indexes
        # pg_statio_sys_sequences
        {
           'query': 'select COUNT(*) as connections from pg_stat_activity where datname=%(dbname)s;',
        },
        {
           'query': 'select COUNT(*) as connections_waiting from pg_stat_activity where datname=%(dbname)s and waiting=true;',
        },
        {
           'query': "select COUNT(*) as connections_active from pg_stat_activity where datname=%(dbname)s and state='active';",
           'version_required': '9.3',
        },
        {
           'query': "select COUNT(*) as connections_idle from pg_stat_activity where datname=%(dbname)s and state='idle';",
           'version_required': '9.3',
        },
        {
           'query': "select COUNT(*) as connections_idle_in_transaction from pg_stat_activity where datname=%(dbname)s and state='idle in transaction';",
           'version_required': '9.3',
        },
        {
           'query': "select COUNT(*) as connections_idle_in_transaction_aborted from pg_stat_activity where datname=%(dbname)s and state='idle in transaction (aborted)';",
           'version_required': '9.3',
        },
        {
           'query': "select COUNT(*) as connections_fastpath_function_call from pg_stat_activity where datname=%(dbname)s and state='fastpath function call';",
           'version_required': '9.3',
        },
        {
           'query': "select COUNT(*) as connections_disabled from pg_stat_activity where datname=%(dbname)s and state='disabled';",
           'version_required': '9.3',
        },
    ],
}


def execute_query(description, cur, dbname=None):
    if 'table' in description:
        query = "SELECT * from {};".format(description['table'])
    else:
        query = description['query']
    cur.execute(query, dict(dbname=dbname))
    return cur.fetchall(), [desc.name for desc in cur.description]


def get_key_indices(description, column_names):
    key = description['key']
    if isinstance(key, list):
        key_indices = [column_names.index(k) for k in key]
    else:
        key_indices = [column_names.index(key)]
    return key_indices


def get_stats(description, conn, dbname=None):
    if 'version_required' in description:
        cur = conn.cursor()
        cur.execute('show SERVER_VERSION;')
        server_version = StrictVersion(cur.fetchone()[0])
        required = StrictVersion(description['version_required'])
        if server_version < required:
            return []
    cur = conn.cursor()
    rows, column_names = execute_query(description, cur, dbname=dbname)
    name_parts = []
    if dbname:
        name_parts.extend(['databases', dbname])
    if 'alias' in description:
        name_parts.append(description['alias'])
    elif 'table' in description:
        name_parts.append(description['table'])
    row_keys = []
    key_indices = []
    if 'key' in description:
        key_indices = get_key_indices(description, column_names)
        for row in rows:
            row_keys.append(".".join([row[i] for i in key_indices]))
    stats = []
    for i, row in enumerate(rows):
        row_name_parts = name_parts[:]
        if row_keys:
            row_name_parts.append(row_keys[i])
        for j, value in enumerate(row):
            cell_name_parts = row_name_parts[:]
            if key_indices and j in key_indices:
                continue
            column = column_names[j]
            if column in description.get('exclude_columns', []):
                continue
            cell_name_parts.append(column)
            if value is None:
                value = 0
            stats.append((".".join(cell_name_parts), value))
    return stats


def list_dbnames(conn):
    cur = conn.cursor()
    cur.execute("SELECT datname from pg_stat_database;")
    return [r[0] for r in cur.fetchall() if r[0] not in EXCLUDE_DBS]


@contextmanager
def connect_to_db(dbname):
    conn = psycopg2.connect("dbname={}".format(dbname))
    try:
        yield conn
    finally:
        conn.close()


def statsdify(stat):
    return "{}:{}|g'".format(stat[0], stat[1])


def write_stats(stats):
    map(print, map(statsdify, stats))


def main(args):
    stats = []
    with connect_to_db('postgres') as conn:
        for description in STATS['per-server']:
            stats.extend(get_stats(description, conn))
        dbnames = list_dbnames(conn)
    for dbname in dbnames:
        with connect_to_db(dbname) as conn:
            for description in STATS['per-db']:
                stats.extend(get_stats(description, conn, dbname))
    write_stats(stats)


def run():
    sys.exit(main(sys.argv[1:]))


if __name__ == '__main__':
    run()
