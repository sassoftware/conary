#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
