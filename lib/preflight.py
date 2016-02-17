# Copyright 2011-2015 Canonical Ltd.
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

from charmhelpers.core import hookenv
from charms.reactive.bus import _short_action_id


def preflight(action):
    '''
    Decorator to run a handler before the main hook phase.

    preflight hooks are used to initialize state for use by the
    main hooks (including hooks that exist in other layers). They
    can also be used to validate the environment, blocking the unit
    and aborting the hook if something this layer is responsible for
    is broken (eg. a service configuration option set to an invalid
    value). We need abort before the main reactive loop, or we
    risk failing to run handlers that rely on @when_file_changed,
    reactive.helpers.data_changed or other state tied to
    charmhelpers.core.unitdata transactions.
    '''
    _id = _short_action_id(action)
    hookenv.atstart(hookenv.log,
                    'preflight handler: {}'.format(_id))
    hookenv.atstart(action)
    return action
