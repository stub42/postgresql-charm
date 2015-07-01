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

from charmhelpers.core import hookenv
from charmhelpers.core.hookenv import INFO, CRITICAL


def status_set(status, msg):
    '''Set the unit status message, and log the change too.'''
    if status is None:
        status = hookenv.status_get()
    if status == 'blocked':
        lvl = CRITICAL
    else:
        lvl = INFO
    hookenv.log('{}: {}'.format(status, msg), lvl)
    hookenv.status_set(status, msg)


def distro_codename():
    """Return the distro release code name, eg. 'precise' or 'trusty'."""
    return host.lsb_release()['DISTRIB_CODENAME']


def extra_packages():
    config = hookenv.config()
    return set(config['extra-packages'].split()
               + config['extra_packages'].split())
