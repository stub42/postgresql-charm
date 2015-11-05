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

__all__ = ['leader_get', 'leader_set']

from charmhelpers.core import hookenv
from charmhelpers.core import unitdata

from charms import reactive
from charms.reactive import not_unless

from preflight import preflight


@not_unless('leadership.is_leader')
def leader_set(settings=None, **kw):
    '''Change leadership settings, per charmhelpers.core.hookenv.leader_set.

    The leadership.set.{key} state will be set while the leadership
    setting remains set.

    Changed leadership settings will set the leadership.changed.{key}
    state. This state will remain set until the following hook.

    These state changes take effect immediately on the leader, and
    in future hooks run on non-leaders. In this way both leaders and
    non-leaders can share handlers, waiting on these states.
    '''
    settings = settings or {}
    settings.update(kw)
    previous = unitdata.kv().getrange('leadership.settings.', strip=True)

    for key, value in settings.items():
        if value != previous.get(key):
            reactive.set_state('leadership.changed.{}'.format(key))
        reactive.helpers.toggle_state('leadership.set.{}'.format(key),
                                      value is not None)
    hookenv.leader_set(settings)
    unitdata.kv().update(settings, prefix='leadership.settings.')


def leader_get(attribute=None):
    return hookenv.leader_get(attribute)


@preflight
def initialize_leadership_state():
    '''Sets the leadership.is_leader state if this unit is the leader.'''
    hookenv.log('Setting leadership.is_leader to {}'
                .format(hookenv.is_leader()))
    reactive.helpers.toggle_state('leadership.is_leader',
                                  hookenv.is_leader())

    previous = unitdata.kv().getrange('leadership.settings.', strip=True)
    current = hookenv.leader_get()

    assert type(previous) is dict
    assert type(list(previous.keys())) is list
    assert type(set(list(previous.keys()))) is set

    # Handle deletions.
    for key in set(previous.keys()) - set(current.keys()):
        current[key] = None

    for key, value in current.items():
        reactive.helpers.toggle_state('leadership.changed.{}'.format(key),
                                      value != previous.get(key))
        reactive.helpers.toggle_state('leadership.set.{}'.format(key),
                                      value is not None)

    unitdata.kv().update(current, prefix='leadership.settings.')
