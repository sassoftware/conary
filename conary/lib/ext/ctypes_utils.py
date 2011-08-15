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


import ctypes
import os


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


def get_helper(name):
    helperDir = os.path.dirname(os.path.abspath(__file__))
    helperPath = os.path.join(helperDir, 'helper_%s.so' % name)
    return ctypes.CDLL(helperPath, use_errno=True)
