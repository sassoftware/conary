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

from testrunner import testhelp

class ChrootError(Exception):
    pass

# singleton
chrootCapability = None
def requireChroot():
    global chrootCapability
    if chrootCapability is None:
        libcap = ctypes.cdll.LoadLibrary('libcap.so.2')
        libcap.cap_to_text.restype = ctypes.c_char_p
        cap = libcap.cap_to_text(libcap.cap_get_pid(os.getpid()), None)
        chrootCapability = 'cap_sys_chroot' in cap
        libcap.cap_free(cap)

    if chrootCapability is False:
        raise ChrootError

    return


def rpm(func):
    # mark the context as rpm
    testhelp.context('rpm')(func)

    def run(*args, **kwargs):
        try:
            __import__('rpm')
            requireChroot()
        except ImportError:
            raise testhelp.SkipTestException('RPM module not present')
        except ChrootError:
            raise testhelp.SkipTestException('"sudo setcap cap_sys_chroot=ep /usr/bin/python" to run RPM module tests')
        else:
            return func(*args, **kwargs)

    run.func_name = func.func_name
    run._contexts = func._contexts

    return run
