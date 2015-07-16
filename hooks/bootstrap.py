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

from charmhelpers import fetch
from charmhelpers.core import hookenv


def bootstrap():
    try:
        import psycopg2  # NOQA: flake8
    except ImportError:
        packages = ['python3-psycopg2']
        fetch.apt_install(packages, fatal=True)
        import psycopg2  # NOQA: flake8


def default_hook():
    if not hookenv.has_juju_version('1.24'):
        hookenv.status_set('blocked', 'Requires Juju 1.24 or higher')
        # Error state, since we don't have 1.24 to give a nice blocked state.
        raise SystemExit(1)

    # These need to be imported after bootstrap() or required Python
    # packages may not have been installed.
    import definitions

    hookenv.log('*** Start {!r} hook'.format(hookenv.hook_name()))
    sm = definitions.get_service_manager()
    sm.manage()
    hookenv.log('*** End {!r} hook'.format(hookenv.hook_name()))
