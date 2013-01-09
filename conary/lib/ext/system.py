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


from conary.lib.ext import ctypes_utils
from ctypes import c_int


def res_init():
    libc = ctypes_utils.get_libc()
    libc.__res_init.argtypes = ()
    libc.__res_init.restype = c_int
    rc = libc.__res_init()
    if rc:
        ctypes_utils.throw_errno(libc)
