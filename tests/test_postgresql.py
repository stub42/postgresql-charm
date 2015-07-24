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
import sys
import unittest

sys.path.append(os.path.join(os.path.dirname(__file__), os.pardir, 'hooks'))

import postgresql


class TestPostgresql(unittest.TestCase):
    def test_parse_config(self):
        valid = [(r'# A comment', dict()),
                 (r'key_1 = value', dict(key_1='value')),
                 (r"key_2 ='quoted valu3'", dict(key_2='quoted valu3')),
                 (r"""key_3= 'foo "bar"'""", dict(key_3='foo "bar"')),
                 (r"""key_4='''bar\''""", dict(key_4="'bar'")),
                 (r"key_5=''", dict(key_5='')),
                 (r"", dict()),
                 (r'  # Another comment ', dict()),
                 (r"key_6='#'", dict(key_6='#')),
                 (r"key_7=42", dict(key_7='42')),
                 (r"key_8=3.142", dict(key_8='3.142')),
                 (r'key_9=-1', dict(key_9='-1'))]

        # The above examples all parse correctly.
        for raw, expected in valid:
            with self.subTest(raw=raw):
                self.assertDictEqual(postgresql.parse_config(raw), expected)

        # Concatenating them parses correctly to.
        combined_raw = []
        combined_expected = {}
        for raw, expected in valid:
            combined_raw.append(raw)
            combined_expected.update(expected)
        self.assertDictEqual(postgresql.parse_config('\n'.join(combined_raw)),
                             combined_expected)

        with self.assertRaises(SyntaxError) as x:
            postgresql.parse_config("=")
        self.assertEqual(str(x.exception), 'Missing key (line 1)')
        self.assertEqual(x.exception.lineno, 1)
        self.assertEqual(x.exception.text, "=")

        # We could be lazy here, since we are dealing with trusted input,
        # but meaningful error messages are helpful.
        with self.assertRaises(SyntaxError) as x:
            postgresql.parse_config('# comment\nkey=')
        self.assertEqual(str(x.exception), 'Missing value (line 2)')

        with self.assertRaises(SyntaxError) as x:
            postgresql.parse_config("key='unterminated")
        self.assertEqual(str(x.exception), 'Badly quoted value (line 1)')

        with self.assertRaises(SyntaxError) as x:
            postgresql.parse_config("key='unterminated 2 # comment")
        self.assertEqual(str(x.exception), 'Badly quoted value (line 1)')

        with self.assertRaises(SyntaxError) as x:
            postgresql.parse_config("key='unte''''")
        self.assertEqual(str(x.exception), 'Badly quoted value (line 1)')

        with self.assertRaises(SyntaxError) as x:
            postgresql.parse_config(r"key='\'")
        self.assertEqual(str(x.exception), 'Badly quoted value (line 1)')
