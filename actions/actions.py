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
import subprocess
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
from reactive.postgresql import wal_e


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


def wal_e_backup(params):
    if not postgresql.is_primary():
        hookenv.action_fail('Not a primary. Run this action on the master')
        return

    backup_cmd = wal_e.wal_e_backup_command()
    if params['prune']:
        prune_cmd = wal_e.wal_e_prune_command()
    else:
        prune_cmd = None

    hookenv.action_set({"wal-e-backup-cmd": backup_cmd,
                        "wal-e-prune-cmd": prune_cmd})

    try:
        hookenv.log('Running wal-e backup')
        hookenv.log(backup_cmd)
        out = subprocess.check_output('sudo -Hu postgres -- ' + backup_cmd,
                                      stderr=subprocess.STDOUT,
                                      shell=True, universal_newlines=True)
        # hookenv.action_set({"backup-output": out,
        #                     "backup-return-code": 0})
    except subprocess.CalledProcessError as x:
        hookenv.action_set({"backup-output": x.output,
                            "backup-return-code": x.returncode})
        hookenv.action_fail('Backup failed')
        return

    if prune_cmd is None:
        return

    try:
        hookenv.log('Running wal-e prune')
        hookenv.log(prune_cmd)
        out = subprocess.check_output('sudo -Hu postgres -- ' + prune_cmd,
                                      stderr=subprocess.STDOUT,
                                      shell=True, universal_newlines=True)
        # hookenv.action_set({"prune-output": out,
        #                     "prune-return-code": 0})
    except subprocess.CalledProcessError as x:
        hookenv.action_set({"prune-output": x.output,
                            "prune-return-code": x.returncode})
        hookenv.action_fail('Backup succeeded, pruning failed')
        return


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
        elif action == 'wal-e-backup':
            wal_e_backup(params)
        elif action == 'switchover':
            reactive_action('actions.switchover')
        else:
            hookenv.action_fail('Action {} not implemented'.format(action))
    except Exception:
        hookenv.action_fail('Unhandled exception')
        tb = traceback.format_exc()
        hookenv.action_set(dict(traceback=tb))
        hookenv.log('Unhandled exception in action {}'.format(action))
        print(tb)


if __name__ == '__main__':
    main(sys.argv)
