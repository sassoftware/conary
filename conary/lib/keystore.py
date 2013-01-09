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


try:
    import keyutils as _keyutils
    _keyring = _keyutils.KEY_SPEC_SESSION_KEYRING
except ImportError:
    _keyutils = _keyring = None


def getPassword(keyDesc):
    if _keyutils:
        keyId = _keyutils.request_key(keyDesc, _keyring)
        if keyId is not None:
            return _keyutils.read_key(keyId)
    return None


def setPassword(keyDesc, passwd):
    if _keyutils:
        _keyutils.add_key(keyDesc, passwd, _keyring)
    return passwd
