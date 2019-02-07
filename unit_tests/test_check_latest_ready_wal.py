# Copyright 2019 Canonical Ltd.
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
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(1, os.path.join(ROOT, 'scripts'))

from check_latest_ready_wal import make_nice_age

class TestCheckLatestReadyWal(unittest.TestCase):
    def test_make_nice_age(self):
        seconds = 123456
        self.assertEqual(
            make_nice_age(seconds),
            '1 days, 10 hours, 17 minutes and 36 seconds'
        )

if __name__ == '__main__':
    unittest.main()
