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

from charmhelpers.core import hookenv, unitdata
from charmhelpers.payload import execd

from reactive.workloadstatus import status_set
from reactive.postgresql import postgresql

from preflight import preflight

# def preflight():
#     block_on_bad_juju()
#     block_on_invalid_config()
#     preinstall()


# hookenv.atstart(hookenv.log, 'Running reactive.postgresql.preflight')
# hookenv.atstart(preflight)


@preflight
def block_on_bad_juju():
    if not hookenv.has_juju_version('1.24'):
        status_set('blocked', 'Requires Juju 1.24 or higher')
        # Error state, since we don't have 1.24 to give a nice blocked state.
        raise SystemExit(1)


@preflight
def block_on_invalid_config():
    """
    Sanity check charm configuration, blocking the unit if we have
    bogus bogus config values or config changes the charm does not
    yet (or cannot) support.
    """
    valid = True
    config = hookenv.config()

    enums = dict(version=set(['', '9.1', '9.2', '9.3', '9.4']),
                 package_status=set(['install', 'hold']))
    for key, vals in enums.items():
        config[key] = config[key].lower()  # Rewrite to lower case.
        if config[key] not in vals:
            valid = False
            status_set('blocked',
                       'Invalid value for {} ({!r})'.format(key, config[key]))

    unchangeable_config = ['locale', 'encoding', 'pgdg', 'manual_replication']
    if config._prev_dict is not None:
        for name in unchangeable_config:
            if config.changed(name):
                config[name] = config.previous(name)
                valid = False
                status_set('blocked',
                           'Cannot change {!r} after install '
                           '(from {!r} to {!r}).'
                           .format(name, config.previous(name),
                                   config.get('name')))
        if config.changed('version') and (config.previous('version') !=
                                          postgresql.version()):
            valid = False
            status_set('blocked',
                       'Cannot change version after install '
                       '(from {!r} to {!r}).'
                       .format(config.previous('version'), config['version']))
            config['version'] = config.previous('version')
            valid = False

    if not valid:
        raise SystemExit(0)


@preflight
def preinstall():
    '''Invoke charmhelpers.payload.execd.execd_run for site customization.

    This needs to happen before anything else (as much as practical),
    because anything else may fail if attempted before the site
    customization hooks have customized the site.
    '''
    # We can't use @once_only, because we need to guarantee this is
    # only run once (rather than only once, and maybe again after
    # upgrade-charm).
    store = unitdata.kv()
    if store.get('postgresql.preflight.preinstall.done'):
        return
    status_set('maintenance', 'Running preinstallation hooks')
    execd.execd_run('charm-pre-install', die_on_error=True)
    store.set('postgresql.preflight.preinstall.done', True)
