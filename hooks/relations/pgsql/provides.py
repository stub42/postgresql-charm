# Copyright 2016 Canonical Ltd.
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

from charms.reactive import hook
from charms.reactive.bus import State, StateList
from charms.reactive.relations import RelationBase, scopes


class PgsqlProvides(RelationBase):

    scope = scopes.SERVICE

    class states(StateList):
        connected = State('{relation_name}.connected')

    @hook('{provides:pgsql}-relation-changed')
    def changed(self):
        self.set_state(self.states.connected)

    @hook('{provides:pgsql}-relation-departed')
    def departed(self):
        self.remove_state(self.states.connected)
