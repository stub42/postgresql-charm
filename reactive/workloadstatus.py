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

__all__ = ['status_get', 'status_set']

from charmhelpers.core import hookenv
from charmhelpers.core.hookenv import INFO, WARNING

from charms import reactive

from preflight import preflight


VALID_STATES = ('maintenance', 'blocked', 'waiting', 'active')


def status_set(state, message):
    """Set the unit's workload status.

    Set state == None to keep the same state and just change the message.

    Toggles the workloadstatus.{maintenance,blocked,waiting,active,unknown}
    states.
    """
    if state is None:
        state = hookenv.status_get()[0]
        if state == 'unknown':
            state = 'maintenance'  # Guess
    assert state in VALID_STATES, 'Invalid state {}'.format(state)
    if state in ('error', 'blocked'):
        lvl = WARNING
    else:
        lvl = INFO
    hookenv.status_set(state, message)
    hookenv.log('{}: {}'.format(state, message), lvl)
    initialize_workloadstatus_state(state)


def status_get():
    """Returns (workload_status, message) for this unit."""
    return hookenv.status_get()


@preflight
def initialize_workloadstatus_state(state=None):
    if state is None:
        state = status_get()[0]
    for s in VALID_STATES + ('unknown',):
        reactive.helpers.toggle_state('workloadstatus.{}'.format(s),
                                      s == state)
