#!/usr/bin/python3
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

import json
import os.path

import psycopg2
import psycopg2.extras

con = psycopg2.connect('user=postgres dbname=postgres')

cur = con.cursor()
cur.execute('show server_version')
ver = cur.fetchone()[0]
ver = '.'.join(ver.split('.')[:2])

cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute('''
            SELECT name, unit, category, short_desc, extra_desc,
                   context, vartype, min_val, max_val, enumvals,
                   boot_val
            FROM pg_settings
            WHERE context <> 'internal'
            ''')

cache = os.path.join(os.path.dirname(__file__),
                     'pg_settings_{}.json'.format(ver))
with open(cache, 'w') as f:
    json.dump({d['name'].lower(): d for d in cur.fetchall()}, f,
              ensure_ascii=True, indent=4, sort_keys=True)
