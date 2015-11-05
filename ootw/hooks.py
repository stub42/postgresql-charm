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

# from charmhelpers.core import hookenv
# from charms import reactive
#
# import replication
# import service


# @reactive.hook()
# def main():
#     service.ensure_locale()
#     service.update_kernel_settings()
#     service.configure_sources()
#     service.install_packages()
#     service.install_administrative_scripts()
#
#     replication.set_replication_state()
#     service.set_service_state()
#
#     # emit_deprecated_option_warnings is called at the end of the hook
#     # so that the warnings to appear clearly at the end of the logs.
#     hookenv.atexit(service.emit_deprecated_option_warnings)
