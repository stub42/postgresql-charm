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

import os.path
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.append(os.path.join(os.path.dirname(__file__), os.pardir, 'lib'))

from charmhelpers import fetch  # NOQA: flake8
from charmhelpers.core import hookenv  # NOQA: flake8
from charms.reactive import main  # NOQA: flake8


def bootstrap():
    try:
        import psycopg2  # NOQA: flake8
        import jinja2  # NOQA: flake8
    except ImportError:
        packages = ['python3-psycopg2', 'python3-jinja2']
        fetch.apt_install(packages, fatal=True)
        import psycopg2  # NOQA: flake8


def block_on_bad_juju():
    if not hookenv.has_juju_version('1.24'):
        hookenv.status_set('blocked', 'Requires Juju 1.24 or higher')
        # Error state, since we don't have 1.24 to give a nice blocked state.
        raise SystemExit(1)


def default_hook():
    hookenv.log('*** Start {!r} hook'.format(hookenv.hook_name()))
    block_on_bad_juju()
    bootstrap()
    main()
    hookenv.log('*** End {!r} hook'.format(hookenv.hook_name()))


if __name__ == '__main__':
    default_hook()
