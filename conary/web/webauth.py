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


import base64

from conary import errors
from conary.lib import util

class PermissionDenied(errors.WebError):
    def __str__(self):
        return "permission denied"

def getAuth(req):
    if hasattr(req, 'headers_in'):
        # mod_python
        headers_in = req.headers_in
        remote_ip = req.connection.remote_ip
    else:
        # webob
        headers_in = req.headers
        remote_ip = req.remote_addr

    if not 'Authorization' in headers_in:
        authToken = ['anonymous', 'anonymous']
    else:
        info = headers_in['Authorization'].split(' ', 1)
        if len(info) != 2 or info[0] != "Basic":
            return None

        try:
            authString = base64.decodestring(info[1])
        except:
            return None

        authToken = authString.split(":", 1)
        if len(authToken) != 2:
            # No password
            authToken.append(util.ProtectedString(''))
        else:
            authToken[1] = util.ProtectedString(authToken[1])

    try:
        entitlementList = parseEntitlement(
                        headers_in.get('X-Conary-Entitlement', ''))
    except:
        return None

    authToken.append(entitlementList)
    authToken.append(remote_ip)
    return authToken

class Authorization:
    def __init__(self, passwordOK=False, isInternal=False, userId=-1):
        self.passwordOK = passwordOK
        self.isInternal = isInternal
        self.userId = userId

# various decorators for authenticated methods
# XXX granularize the errors raised
def requiresAuth(func):
    def wrapper(self, **kwargs):
        if not kwargs['auth'].passwordOK:
            raise PermissionDenied
        else:
            return func(self, **kwargs)
    return wrapper

def internalOnly(func):
    def wrapper(self, **kwargs):
        if not kwargs['auth'].isInternal:
            raise PermissionDenied
        else:
            return func(self, **kwargs)
    return wrapper

def externalOnly(func):
    def wrapper(self, **kwargs):
        if self.cfg.externalAccess:
            return func(self, **kwargs)
        else:
            raise PermissionDenied
    return wrapper

def parseEntitlement(entHeader):
    entitlementList = []

    allEntitlements = entHeader.split()
    for i in range(0, len(allEntitlements), 2):
        ent = [ allEntitlements[i] ]
        ent.append(base64.decodestring(allEntitlements[i + 1]))
        if ent[0] == '*':
            ent[0] = None
        entitlementList.append(tuple(ent))

    return entitlementList
