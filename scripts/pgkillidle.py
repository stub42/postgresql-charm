#!/usr/bin/python3
# Copyright 2007-2016 Canonical Ltd.
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
"""Kill <IDLE> in transaction connections that have hung around for too long.
"""

from optparse import OptionParser
import psycopg2
import sys


def main():
    parser = OptionParser()
    parser.add_option(
        '-c', '--connection', type='string', dest='connect_string',
        default='', help="Psycopg connection string",
        )
    parser.add_option(
        '-s', '--max-idle-seconds', type='int',
        dest='max_idle_seconds', default=10*60, metavar="SECS",
        help='Connections idling in a transaction more than SECS seconds '
             'will be killed. If 0, all connections are killed even if not '
             'in a transaction.')
    parser.add_option(
        '-q', '--quiet', action='store_true', dest="quiet",
        default=False, help='Silence output',
        )
    parser.add_option(
        '-n', '--dry-run', action='store_true', default=False,
        dest='dryrun', help="Dry run - don't kill anything",
        )
    parser.add_option(
        '-i', '--ignore', action='append', dest='ignore',
        help='Ignore connections by USER', metavar='USER')
    options, args = parser.parse_args()
    if len(args) > 0:
        parser.error('Too many arguments')

    ignore_sql = ' AND %s not in (usename, application_name)' * len(
        options.ignore or [])

    con = psycopg2.connect(options.connect_string)

    cur = con.cursor()

    cur.execute('show server_version_num')
    ver = int(cur.fetchone()[0])

    if ver >= 90200:
        is_idle_sql = "pid <> pg_backend_pid()"
        if options.max_idle_seconds != 0:
            is_idle_sql += """
                AND state = 'idle in transaction'
                AND state_change < CURRENT_TIMESTAMP - '%d seconds'::interval
                """ % options.max_idle_seconds
        cur.execute("""
            SELECT
                usename, application_name, datname, pid,
                backend_start, state_change, AGE(NOW(), state_change) AS age
            FROM pg_stat_activity
            WHERE %s %s
            ORDER BY age
            """ % (is_idle_sql, ignore_sql), options.ignore)
    else:
        is_idle_sql = "procpid <> pg_backend_pid()"
        if options.max_idle_seconds != 0:
            is_idle_sql += """
                AND current_query = '<IDLE> in transaction'
                    AND query_start < CURRENT_TIMESTAMP
                        - '%d seconds'::interval
                """ % options.max_idle_seconds
        cur.execute("""
            SELECT
                usename, application_name, datname, procpid,
                backend_start, query_start, AGE(NOW(), query_start) AS age
            FROM pg_stat_activity
            WHERE %s %s
            ORDER BY age
            """ % (is_idle_sql, ignore_sql), options.ignore)

    rows = cur.fetchall()

    if len(rows) == 0:
        if not options.quiet:
            print('No IDLE transactions to kill')
        return 0

    for usename, appname, datname, pid, backend, state, age in rows:
        print(80 * "=")
        print('Killing %s(%d) %s from %s:' % (usename, pid, appname, datname))
        print('    backend start: %s' % (backend,))
        print('    idle start:    %s' % (state,))
        print('    age:           %s' % (age,))
        if not options.dryrun:
            cur.execute('SELECT pg_terminate_backend(%s)', (pid,))
    cur.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
