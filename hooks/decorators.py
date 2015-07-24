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

from charmhelpers.core import hookenv, services
from charmhelpers.core.hookenv import DEBUG

import helpers
import postgresql


class ManagerCallback(services.ManagerCallback):
    def __init__(self, callback=None):
        self._cb = callback

    def __call__(self, manager, service_name, event_name):
        if self._cb is None:
            raise NotImplementedError()

        return self._cb(manager, service_name, event_name)


def data_ready_action(func):
    '''Decorate func to be used as a data_ready item.

    Func must accept the 3 extended arguments per
    charmhelpers.core.services.base.ManagerCallback
        manager - the ServiceManager instance in play
        service_name - the 'service' key of the service definition in play.
        event_name - data_ready, data_lost, start, stop

    Func is wrapped, adding logging, and with ManagerCallback so the
    Services Framework invokes it with the desired paramter list.
    '''
    @wraps(func)
    def wrapper(manager, service_name, event_name):
        if hookenv.remote_unit():
            hookenv.log("** Action {}/{} ({})".format(hookenv.hook_name(),
                                                      func.__name__,
                                                      hookenv.remote_unit()))
        else:
            hookenv.log("** Action {}/{}".format(hookenv.hook_name(),
                                                 func.__name__))
        return func(manager, service_name, event_name)

    return ManagerCallback(wrapper)


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
            if hookenv.status_get() != 'blocked':
                helpers.status_set('blocked',
                                   'Requirement {} failed'.format(name))
            return False


def leader_only(func):
    '''Only run on the service leader.'''
    @wraps(func)
    def wrapper(*args, **kw):
        if hookenv.is_leader():
            return func(*args, **kw)
        else:
            hookenv.log('Not the leader', DEBUG)


def master_only(func):
    '''Only run on the appointed master.'''
    @wraps(func)
    def wrapper(*args, **kw):
        if postgresql.is_master():
            return func(*args, **kw)
        else:
            hookenv.log('Not the master', DEBUG)
    return wrapper


def secondary_only(func):
    @wraps(func)
    def wrapper(*args, **kw):
        if postgresql.is_secondary():
            return func(*args, **kw)
        else:
            hookenv.log('Not a secondary', DEBUG)
    return wrapper
