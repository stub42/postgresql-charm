#!/usr/bin/python3
#
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

import subprocess
import sys

args = list(sys.argv[1:])
# # Strip the -W option, as its noise messes with test output.
# if '-W' in args:
#     args.remove('-W')
cmd = ["juju-deployer"] + args
try:
    subprocess.check_output(cmd, stderr=subprocess.STDOUT, universal_newlines=True)
except subprocess.CalledProcessError as x:
    sys.stderr.write(x.output)
    sys.exit(x.returncode)
