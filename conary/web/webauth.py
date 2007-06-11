#
# Copyright (c) 2004-2007 rPath, Inc.
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
import base64

from conary import errors

class PermissionDenied(errors.WebError):
    def __str__(self):
        return "permission denied"

def getAuth(req):
    if not 'Authorization' in req.headers_in:
        authToken = ['anonymous', 'anonymous']
    else:
        info = req.headers_in['Authorization'].split()
        if len(info) != 2 or info[0] != "Basic":
            return None

        try:
            authString = base64.decodestring(info[1])
        except:
            return None

        if authString.count(":") != 1:
            return None

        authToken = authString.split(":")

    try:
        entitlementList = parseEntitlement(
                        req.headers_in.get('X-Conary-Entitlement', ''))
    except:
        return None

    authToken.append(entitlementList)
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
