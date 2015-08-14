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

from charmhelpers import context
from charmhelpers.core import hookenv
from charmhelpers.core.hookenv import DEBUG

import helpers


def data_ready_action(func):
    '''Decorate func to be used as a data_ready item.'''
    @wraps(func)
    def wrapper(service_name=None):
        if hookenv.remote_unit():
            hookenv.log("** Action {}/{} ({})".format(hookenv.hook_name(),
                                                      func.__name__,
                                                      hookenv.remote_unit()))
        else:
            hookenv.log("** Action {}/{}".format(hookenv.hook_name(),
                                                 func.__name__))
        return func()
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
            if hookenv.status_get() != 'blocked':
                helpers.status_set('blocked',
                                   'Requirement {} failed'.format(name))
            return False


def relation_handler(*relnames):
    '''Invoke the decorated function once per matching relation.

    The decorated function should accept the Relation() instance
    as its single parameter.
    '''
    assert relnames, 'relation names required'

    def decorator(func):
        @wraps(func)
        def wrapper(service_name=None):
            rels = context.Relations()
            for relname in relnames:
                for rel in rels[relname].values():
                    if rel:
                        func(rel)
        return wrapper
    return decorator


def leader_only(func):
    '''Only run on the service leader.'''
    @wraps(func)
    def wrapper(*args, **kw):
        if hookenv.is_leader():
            return func(*args, **kw)
        else:
            hookenv.log('Not the leader', DEBUG)
    return wrapper


def not_leader(func):
    '''Only run on the service leader.'''
    @wraps(func)
    def wrapper(*args, **kw):
        if not hookenv.is_leader():
            return func(*args, **kw)
        else:
            hookenv.log("I'm the leader", DEBUG)
    return wrapper


def master_only(func):
    '''Only run on the appointed master.'''
    @wraps(func)
    def wrapper(*args, **kw):
        import postgresql
        if postgresql.is_master():
            return func(*args, **kw)
        else:
            hookenv.log('Not the master', DEBUG)
    return wrapper


def not_master(func):
    '''Don't run on the appointed master.'''
    @wraps(func)
    def wrapper(*args, **kw):
        import postgresql
        if postgresql.is_master():
            hookenv.log("I'm the master", DEBUG)
        else:
            return func(*args, **kw)
    return wrapper
