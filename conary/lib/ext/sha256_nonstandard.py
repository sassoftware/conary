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
