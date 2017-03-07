#!/usr/bin/python3
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
libs_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                        '..', 'lib'))
if libs_dir not in sys.path:
    sys.path.append(libs_dir)


from charmhelpers.core import hookenv
from charms import reactive

from reactive.postgresql import postgresql


def replication_pause(params):
    if not postgresql.is_secondary():
        hookenv.action_fail('Not a hot standby')
        return

    con = postgresql.connect()
    con.autocommit = True

    offset = postgresql.wal_received_offset(con)
    hookenv.action_set(dict(offset=offset))

    cur = con.cursor()
    cur.execute('SELECT pg_is_xlog_replay_paused()')
    if cur.fetchone()[0] is True:
        # Not a failure, per lp:1670613
        hookenv.action_set(dict(result='Already paused'))
        return
    cur.execute('SELECT pg_xlog_replay_pause()')
    hookenv.action_set(dict(result='Paused'))


def replication_resume(params):
    if not postgresql.is_secondary():
        hookenv.action_fail('Not a hot standby')
        return

    con = postgresql.connect()
    con.autocommit = True

    offset = postgresql.wal_received_offset(con)
    hookenv.action_set(dict(offset=offset))

    cur = con.cursor()
    cur.execute('SELECT pg_is_xlog_replay_paused()')
    if cur.fetchone()[0] is False:
        # Not a failure, per lp:1670613
        hookenv.action_set(dict(result='Already resumed'))
        return
    cur.execute('SELECT pg_xlog_replay_resume()')
    hookenv.action_set(dict(result='Resumed'))


# Revisit this when actions are more mature. Per Bug #1483525, it seems
# impossible to return filenames in our results.
#
# def backup(params):
#     assert params['type'] == 'dump'
#     script = os.path.join(helpers.scripts_dir(), 'pg_backup_job')
#     cmd = ['sudo', '-u', 'postgres', '-H', script, str(params['retention'])]
#     hookenv.action_set(dict(command=' '.join(cmd)))
#
#     try:
#         subprocess.check_call(cmd)
#     except subprocess.CalledProcessError as x:
#         hookenv.action_fail(str(x))
#         return
#
#     backups = {}
#     for filename in os.listdir(backups_dir):
#         path = os.path.join(backups_dir, filename)
#         if not is.path.isfile(path):
#             continue
#         backups['{}.{}'.format(filename
#         backups[filename] = dict(name=filename,
#                                  size=os.path.getsize(path),
#                                  path=path,
#                                  scp_path='{}:{}'.format(unit, path))
#     hookenv.action_set(dict(backups=backups))


def reactive_action(state):
    reactive.set_state(state)
    reactive.main()


def main(argv):
    action = os.path.basename(argv[0])
    params = hookenv.action_get()
    try:
        if action == 'replication-pause':
            replication_pause(params)
        elif action == 'replication-resume':
            replication_resume(params)
        elif action == 'switchover':
            reactive_action('actions.switchover')
        else:
            hookenv.action_fail('Action {} not implemented'.format(action))
    except Exception:
        hookenv.action_fail('Unhandled exception')
        hookenv.action_set(dict(traceback=traceback.format_exc()))


if __name__ == '__main__':
    main(sys.argv)
