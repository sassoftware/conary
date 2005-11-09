#
# Copyright (c) 2005 rPath, Inc.
# All rights reserved.
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

class MissingParameterError(Exception):
    def __init__(self, param):
        self.param = param
        
    def __str__(self):
        return "Missing Parameter: %s" % self.param

def strFields(**params):
    """Decorator for cgi fields.  Use like @strFields(foo=None, bar='foo') 
    where foo is a required parameter, and bar defaults to 'foo'.
    Converts parameters to the given type, leaves other paramters untouched.  
    """
    def deco(func):
        def wrapper(self, **kw):
            for name, default in params.iteritems():
                if name in kw:
                    value = str(kw[name])
                elif default is None:
                    raise MissingParameterError(str(name)) 
                else:
                    value = default
                kw[name] = value
            return func(self, **kw)
        return wrapper
    return deco

def intFields(**params):
    """Decorator for cgi fields.  Use like @intFields(foo=None, bar=2) 
    where foo is a required parameter, and bar defaults to 2.
    Converts parameters to the given type, leaves other paramters untouched.  
    """

    def deco(func):
        def wrapper(self, **kw):
            for name, default in params.iteritems():
                if name in kw:
                    value = int(kw[name])
                elif default is None:
                    raise MissingParameterError(str(name))
                else:
                    value = default
                kw[name] = value
            return func(self, **kw)
        return wrapper
    return deco

def listFields(memberType, **params):
    def deco(func):
        def wrapper(self, **kw):
            for name, default in params.iteritems():
                if name in kw:
                    if not isinstance(kw[name], list):
                        value = [memberType(kw[name])]
                    else:
                        value = [ memberType(x) for x in kw[name] ]
                elif default is None:
                    raise MissingParameterError(name)
                else:
                    value = default
                kw[name] = value
            return func(self, **kw)
        return wrapper
    return deco

def boolFields(**params):
    def deco(func):
        def wrapper(self, **kw):
            for name, default in params.iteritems():
                if name in kw:
                    value = bool(int(kw[name]))
                elif default is None:
                    raise MissingParameterError(name)
                else:
                    value = default
                kw[name] = value
            return func(self, **kw)
        return wrapper
    return deco

def dictFields(**params):
    def deco(func):
        def wrapper(self, **kw):
            for key in kw.keys():
                parts = key.split('.')
                if len(parts) > 1 and parts[0] in params:
                    d = kw
                    d.setdefault(parts[0], {}) 
                    while len(parts) > 1:
                        d.setdefault(parts[0], {}) 
                        d = d[parts[0]]
                        parts = parts[1:]
                    value = kw[key]
                    d[parts[0]] = str(value)
                    del kw[key]
            return func(self, **kw)
        return wrapper
    return deco

