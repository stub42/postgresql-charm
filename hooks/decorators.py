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
from functools import wraps

from charmhelpers.core import hookenv
from charmhelpers.core.hookenv import CRITICAL

import helpers


def data_ready_action(func):
    '''Decorate func to be used as a data_ready item.
    
    Log and call func, stripping the unused servicename argument.
    '''
    @wraps(func)
    def wrapper(servicename, *args, **kw):
        if hookenv.remote_unit():
            hookenv.log("** Action {}/{} ({})".format(hookenv.hook_name(),
                                                      func.__name__,
                                                      hookenv.remote_unit()))
        else:
            hookenv.log("** Action {}/{}".format(hookenv.hook_name(),
                                                 func.__name__))
        return func(*args, **kw)
    return wrapper


class requirement:
    '''Decorate a function so it can be used as a required_data item.

    Function must True if requirements are met. Sets the unit state
    to blocked if requirements are not met and the unit not already blocked.
    '''
    def __init__(self, func):
        self._func = func

    def __bool__(self):
        name = self._func.__name__
        if self._func():
            hookenv.log('** Requirement {} passed'.format(name))
            return True
        else:
            block('Requirement {} failed'.format(name))
            return False
