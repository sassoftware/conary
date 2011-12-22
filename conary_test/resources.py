#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


import os


def get_path(*subpath):
    return os.path.join(_test_root, *subpath)


def get_archive(*subpath):
    return get_path('conary_test', 'archive', *subpath)


def _get_test_root():
    modname = __name__.split('.')
    modroot = os.path.abspath(__file__)
    while modname:
        modroot = os.path.dirname(modroot)
        modname.pop()
    return modroot
_test_root = _get_test_root()
