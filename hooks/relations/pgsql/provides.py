# Copyright 2016 Canonical Ltd.
#
# This file is part of the PostgreSQL Client Interface for Juju charms.reactive
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
from charms.reactive import hook, scopes, RelationBase


class PostgreSQLServer(RelationBase):
    """
    PostgreSQL partial server side interface.

    A client may be related to a PostgreSQL service multiple times.
    All clients on a relation should eventually agree.

    This is just a skeleton to set a reactive state. The actual
    server side protocol cannot be modelled as a charms.reactive
    relation as coordination requires both the client relation
    and on the peer relation. And in the case of a proxy like pgbouncer,
    with the backend service relation.
    """
    scope = scopes.SERVICE

    @hook('{provides:pgsql}-relation-joined')
    def joined(self):
        '''Set the {relation_name}.connected state'''
        # There is at least one named relation
        self.set_state('{relation_name}.connected')
        hookenv.log('Joined {} relation'.format(hookenv.relation_id()))
