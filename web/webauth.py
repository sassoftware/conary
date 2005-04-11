#
# Copyright (c) 2004-2005 Specifix, Inc.
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

class PermissionDenied(Exception):
    def __str__(self):
        return "permission denied"
            
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
        if not kwards['auth'].isInternal:
            raise PermissionDenied
        else:
            return func(self, **kwargs)
    return wrapper

def externalOnly(func):
    def wrapper(self, **kwargs):
        if self.cfg.externalAccess:
            raise PermissionDenied
        else:
            return func(self, **kwargs)
    return wrapper
