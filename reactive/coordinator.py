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

from charmhelpers.core import hookenv
from charms import reactive

from preflight import preflight

# If this is moved to a layer, then the BaseCoordinator class should
# not be hardcoded. It will need to be imported from a module that may
# be overridden somehow. Or maybe just hardcode Serial or TrueSerial
# since that is all anyone has used so far, and maybe the customization
# is overkill.
from charmhelpers.coordinator import Serial

coordinator = Serial()


def acquire(lock):
    """
    Sets either the coordinator.granted.{lockname} or
    coordinator.requested.{lockname} state.

    Returns True if the lock could be immediately granted.
    """
    if coordinator.acquire(lock):
        reactive.set_state('coordinator.granted.{}'.format(lock))
        return True
    else:
        reactive.set_state('coordinator.requested.{}'.format(lock))
        return False


@preflight
def initialize_coordinator_state():
    '''
    The coordinator.granted.{lockname} state will be set for every lock
    granted to the currently running hook.
    '''
    # Remove reactive state for locks that have been released.
    granted = set(coordinator.grants.get(hookenv.local_unit(), {}).keys())
    previously_granted = set(state.split('.', 2)[2]
                             for state in reactive.bus.get_states()
                             if state.startswith('coordinator.granted.'))
    for released in (previously_granted - granted):
        reactive.remove_state('coordinator.granted.{}'.format(released))
    for state in granted:
        reactive.set_state('coordinator.granted.{}'.format(state))

    requested = set(coordinator.requests.get(hookenv.local_unit(), {}).keys())
    previously_requested = set(state.split('.', 2)[2]
                               for state in reactive.bus.get_states()
                               if state.startswith('coordinator.requested.'))
    for dropped in (previously_requested - requested):
        reactive.remove_state('coordinator.requested.{}'.format(dropped))
    for state in requested:
        reactive.set_state('coordinator.requested.{}'.format(state))
