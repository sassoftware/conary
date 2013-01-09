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


import getpass
from conary.lib import keystore


def getPassword(server, userName=None, useCached=True):
    if userName is None:
        return None, None
    keyDesc = 'conary:%s:%s' % (server, userName)
    if useCached:
        passwd = keystore.getPassword(keyDesc)
        if passwd:
            return userName, passwd
    s = "Enter the password for %s on %s:" % (userName, server)
    passwd = getpass.getpass(s)
    if passwd:
        keystore.setPassword(keyDesc, passwd)
    return userName, passwd
