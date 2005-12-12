#
# Copyright (c) 2004-2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
from mod_python import apache
import base64

class PermissionDenied(Exception):
    def __str__(self):
        return "permission denied"

def getAuth(req):
    if not 'Authorization' in req.headers_in:
        return ('anonymous', 'anonymous', None, None)

    info = req.headers_in['Authorization'].split()
    if len(info) != 2 or info[0] != "Basic":
        return apache.HTTP_BAD_REQUEST

    try:
        authString = base64.decodestring(info[1])
    except:
        return apache.HTTP_BAD_REQUEST

    if authString.count(":") != 1:
        return apache.HTTP_BAD_REQUEST

    authToken = authString.split(":")

    entitlement = req.headers_in.get('X-Conary-Entitlement', None)
    if entitlement is not None:
        try:
            entitlement = entitlement.split()
            entitlement[1] = base64.decodestring(entitlement[1])
        except:
            self.send_error(400)
            return None
    else:
        entitlement = [ None, None ]

    return authToken + entitlement

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
