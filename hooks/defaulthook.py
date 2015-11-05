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

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
sys.path.insert(1, ROOT)
sys.path.insert(2, os.path.join(ROOT, 'lib'))
sys.path.insert(3, os.path.join(ROOT, 'lib', 'pypi'))

from charmhelpers import fetch  # NOQA: flake8
from charmhelpers.core import hookenv  # NOQA: flake8
from charmhelpers.core.hookenv import WARNING  # NOQA: flake8
from charms.reactive import main  # NOQA: flake8

# Work around https://github.com/juju-solutions/charms.reactive/issues/33
import reactive.apt  # NOQA: flake8
import reactive.workloadstatus  # NOQA: flake8
import preflight  # NOQA: flake8
import everyhook  # NOQA: flake8


def bootstrap():
    try:
        import psycopg2  # NOQA: flake8
        import jinja2  # NOQA: flake8
    except ImportError:
        packages = ['python3-psycopg2', 'python3-jinja2']
        fetch.apt_install(packages, fatal=True)
        import psycopg2  # NOQA: flake8
        import jinja2  # NOQA: flake8


def default_hook():
    hookenv.log('*** Start {!r} hook'.format(hookenv.hook_name()))
    bootstrap()

    # Kick off the charms.reactive reactor.
    try:
        main()
        hookenv.log('*** {!r} hook completed'.format(hookenv.hook_name()))
    except SystemExit as x:
        hookenv.log('*** {!r} hook aborted code {}'.format(hookenv.hook_name(),
                                                           x.code), WARNING)
        raise


if __name__ == '__main__':
    default_hook()
