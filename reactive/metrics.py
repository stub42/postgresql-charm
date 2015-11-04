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

from charmhelpers.core import hookenv, templating
from charmhelpers.core.hookenv import DEBUG

from decorators import data_ready_action
import helpers


@data_ready_action
def write_metrics_cronjob():
    config = hookenv.config()
    path = os.path.join(helpers.cron_dir(), 'juju-postgresql-metrics')

    # need the following two configs to be valid
    metrics_target = config['metrics_target'].strip()
    metrics_sample_interval = config['metrics_sample_interval']
    if (not metrics_target or
            ':' not in metrics_target or not metrics_sample_interval):
        hookenv.log("Required config not found or invalid "
                    "(metrics_target, metrics_sample_interval), "
                    "disabling statsd metrics", DEBUG)
        if os.path.exists(path):
            os.unlink(path)
        return

    charm_dir = hookenv.charm_dir()
    statsd_host, statsd_port = metrics_target.split(':', 1)
    metrics_prefix = config['metrics_prefix'].strip()
    metrics_prefix = metrics_prefix.replace(
        "$UNIT", hookenv.local_unit().replace('.', '-').replace('/', '-'))

    # ensure script installed
    charm_script = os.path.join(charm_dir, 'files', 'metrics',
                                'postgres_to_statsd.py')
    script_path = os.path.join(helpers.scripts_dir(), 'postgres_to_statsd.py')
    with open(charm_script, 'r') as f:
        helpers.write(script_path, f.read(), mode=0o755)

    # write the crontab
    data = dict(interval=config['metrics_sample_interval'],
                script_path=script_path,
                metrics_prefix=metrics_prefix,
                metrics_sample_interval=metrics_sample_interval,
                statsd_host=statsd_host,
                statsd_port=statsd_port)
    templating.render('metrics_cronjob.template', charm_script, data,
                      perms=0o644)
