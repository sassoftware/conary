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

import ctypes


_libc = None

def get_libc():
    global _libc
    if _libc is None:
        for sover in ('6', '5'):
            try:
                _libc = ctypes.CDLL('libc.so.' + sover, use_errno=True)
                break
            except:
                pass
        else:
            raise OSError("Could not find a suitable libc")
    return _libc


def throw_errno(libc, cls=OSError):
    err = ctypes.get_errno()
    libc.strerror.restype = ctypes.c_char_p
    msg = libc.strerror(err)
    raise cls(err, msg)
