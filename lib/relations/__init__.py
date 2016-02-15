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
import os.path
from pkgutil import extend_path
import sys


charm_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
hooks_dir = os.path.join(charm_dir, 'hooks')
if hooks_dir not in sys.path:
    sys.path.append(hooks_dir)


__path__ = extend_path(__path__, __name__)
