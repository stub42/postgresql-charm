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

from charmhelpers import context
from charmhelpers.core import hookenv

import helpers
import postgresql


def upgrade_charm():
    helpers.status_set('maintenance', 'Upgrading charm')

    config = hookenv.config()

    # We can no longer run preinstall 'only in the install hook',
    # because the first hook may now be a leader hook or a storage hook.
    # Set the new flag so it doesn't run.
    config['preinstall_done'] = True

    rels = context.Relations()

    # The master is now appointed by the leader.
    if hookenv.is_leader():
        master = postgresql.master()
        if not master:
            master = hookenv.local_unit()
            if rels.peer:
                for peer_relinfo in rels.peer.values():
                    if peer_relinfo.get('state') == 'master':
                        master = peer_relinfo.unit
                        break
            hookenv.leader_set(master=master)

    # The name of this crontab has changed. It will get regenerated in
    # config-changed.
    if os.path.exists('/etc/cron.d/postgresql'):
        os.unlink('/etc/cron.d/postgresql')
