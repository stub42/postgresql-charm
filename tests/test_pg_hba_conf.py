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
from collections import defaultdict
import os
import sys
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(1, ROOT)
sys.path.insert(2, os.path.join(ROOT, 'lib'))
sys.path.insert(3, os.path.join(ROOT, 'lib', 'pypi'))

from reactive.postgresql.service import generate_pg_hba_conf


# set of classes to stub out the context relation interface
class Relations(defaultdict):
    def __init__(self, *args, **kwargs):
        # set RelationSet as default factory
        super(Relations, self).__init__(RelationSet, *args, **kwargs)
        self.peer = {}


class RelationSet(dict):
    def add_unit(self, unit, data=None, local=None):
        rel = RelationData()
        rel[unit] = {'private-address': '1.2.3.4'}
        if local is not None:
            rel.local.update(**local)
        if data is not None:
            rel[unit].update(**data)
        self[unit] = rel


class RelationData(dict):
    def __init__(self, *args, **kwargs):
        super(RelationData, self).__init__(*args, **kwargs)
        self.local = defaultdict(str)


class TestPgHbaConf(unittest.TestCase):

    def test_no_relations_or_config(self):
        content = generate_pg_hba_conf('', defaultdict(str), Relations())
        self.assertIn('local all postgres peer map=juju_charm', content)
        self.assertIn('local all all peer', content)
        self.assertIn('local all all reject', content)
        self.assertIn('host all all all reject', content)

    def test_peer_relation(self):
        rels = Relations()
        rels.peer = {
            'unit/1': {'private-address': '1.2.3.4'},
        }
        content = generate_pg_hba_conf('', defaultdict(str), rels)
        self.assertIn('host replication _juju_repl "1.2.3.4/32" md5', content)
        self.assertIn('host postgres _juju_repl "1.2.3.4/32" md5', content)

    def test_db_relation(self):
        rels = Relations()
        rels['db'].add_unit('unit/1', local={
            'user': 'user',
            'database': 'database',
            'schema_user': 'schema_user',
        })
        content = generate_pg_hba_conf('', defaultdict(str), rels)
        self.assertIn('host "database" "user" "1.2.3.4/32" md5', content)
        self.assertIn(
            'host "database" "schema_user" "1.2.3.4/32" md5', content)

    def test_db_admin_relation(self):
        rels = Relations()
        rels['db-admin'].add_unit('unit/1', local=({'user': 'user'}))
        content = generate_pg_hba_conf('', defaultdict(str), rels)
        self.assertIn('host all all "1.2.3.4/32" md5', content)

    def test_master_relation(self):
        rels = Relations()
        rels['master'].add_unit('unit/1', local=({
            'user': 'user',
            'database': 'database',
        }))
        content = generate_pg_hba_conf('', defaultdict(str), rels)
        self.assertIn('host replication "user" "1.2.3.4/32" md5', content)
        self.assertIn('host "database" "user" "1.2.3.4/32" md5', content)

    def test_admin_addresses_config(self):
        rels = Relations()
        config = defaultdict(str)
        config['admin_addresses'] = '192.168.1.0/24,10.0.0.0/8,1.2.3.4'
        content = generate_pg_hba_conf('', config, rels)
        self.assertIn('host all all "192.168.1.0/24" md5', content)
        self.assertIn('host all all "10.0.0.0/8" md5', content)
        self.assertIn('host all all "1.2.3.4/32" md5', content)

    def test_extra_pg_auth(self):
        rels = Relations()
        config = defaultdict(str)
        config['extra_pg_auth'] = 'local all sso md5,local all ssoadmin md5'
        content = generate_pg_hba_conf('', config, rels)
        self.assertIn('\nlocal all sso md5', content)
        self.assertIn('\nlocal all ssoadmin md5', content)
