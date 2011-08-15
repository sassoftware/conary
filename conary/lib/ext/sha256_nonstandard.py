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


from ctypes import c_int, c_void_p, create_string_buffer
from conary.lib.ext import ctypes_utils


def digest(data):
    buf = create_string_buffer(32)
    helper = ctypes_utils.get_helper('sha256_nonstandard')
    func = helper.sha256ns_digest
    func.argtypes = (c_void_p, c_int, c_void_p)
    func.restype = c_int
    func(data, len(data), buf)
    return buf.raw
