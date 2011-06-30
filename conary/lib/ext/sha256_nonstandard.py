#
# Copyright (c) 2011 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
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
