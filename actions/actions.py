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

from charmhelpers.core import hookenv


def replication(params):
    hookenv.action_set(result=True)


def main(argv):
    action = os.path.basename(argv[0])
    params = hookenv.action_get()
    try:
        if action == 'replication':
            replication(params)
        else:
            hookenv.action_fail('Action {} not implemented'.format(action))
    except Exception:
        hookenv.action_fail('Unhandled exception')
        hookenv.action_set(traceback=traceback.format_exc())


if __name__ == '__main__':
    main(sys.argv)
