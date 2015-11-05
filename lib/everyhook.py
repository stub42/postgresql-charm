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

from charmhelpers.core import unitdata
from charms.reactive.bus import Handler


def everyhook(action):
    '''
    Decorator to run a handler each and every hook, during the hook phase.

    Charms should to minimize code that can only run in a specific hook
    to avoid race conditions. For example, consider a simple service
    configuration change. This will queue the config-changed hook on
    each unit, but if there is already a queue of hooks being processed
    by a unit then these hooks will see the changed configuration in the
    hook environment before the config-changed hook has had a chance to
    run. This can be fatal, with hooks crashing attempting to run code
    paths before the config-changed hook has set things up so they can
    be run successfully. Similar races can be described for leadership
    and relations. The simplest way of avoiding this entire class of
    races is to have a single, general hook instead of several specific
    ones tied to particular events.
    '''
    def in_hook_phase():
        dispatch_phase = unitdata.kv().get('reactive.dispatch.phase')
        return dispatch_phase == 'hooks'

    handler = Handler.get(action)
    handler.add_predicate(in_hook_phase)
    return action
