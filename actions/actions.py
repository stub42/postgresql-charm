#!/usr/bin/python
# Copyright 2015 Canonical Ltd.
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
import os.path
import sys
import traceback


hooks_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                         '..', 'hooks'))
if hooks_dir not in sys.path:
    sys.path.append(hooks_dir)

from charmhelpers.core import hookenv
import postgresql


def replication_pause(params):
    if not postgresql.is_secondary():
        hookenv.action_fail('Not a hot standby')
        return

    offset = postgresql.wal_received_offset()
    hookenv.action_set(dict(offset=offset))

    con = postgresql.connect()
    con.autocommit = True
    cur = con.cursor()
    cur.execute('SELECT pg_is_xlog_replay_paused()')
    if cur.fetchone()[0] is True:
        hookenv.action_fail('Already paused')
        return
    cur.execute('SELECT pg_xlog_replay_pause()')
    hookenv.action_set(dict(result='Paused'))


def replication_resume(params):
    if not postgresql.is_secondary():
        hookenv.action_fail('Not a hot standby')
        return

    offset = postgresql.wal_received_offset()
    hookenv.action_set(dict(offset=offset))

    con = postgresql.connect()
    con.autocommit = True
    cur = con.cursor()
    cur.execute('SELECT pg_is_xlog_replay_paused()')
    if cur.fetchone()[0] is False:
        hookenv.action_fail('Already resumed')
        return
    cur.execute('SELECT pg_xlog_replay_resume()')
    hookenv.action_set(dict(result='Resumed'))


def main(argv):
    action = os.path.basename(argv[0])
    params = hookenv.action_get()
    try:
        if action == 'replication-pause':
            replication_pause(params)
        elif action == 'replication-resume':
            replication_resume(params)
        else:
            hookenv.action_fail('Action {} not implemented'.format(action))
    except Exception:
        hookenv.action_fail('Unhandled exception')
        hookenv.action_set(dict(traceback=traceback.format_exc()))


if __name__ == '__main__':
    main(sys.argv)
