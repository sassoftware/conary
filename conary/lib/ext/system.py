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


from conary.lib.ext import ctypes_utils
from ctypes import c_int


def res_init():
    libc = ctypes_utils.get_libc()
    libc.__res_init.argtypes = ()
    libc.__res_init.restype = c_int
    rc = libc.__res_init()
    if rc:
        ctypes_utils.throw_errno(libc)
