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


from conary import errors

class MissingParameterError(errors.WebError):
    def __init__(self, param):
        self.param = param

    def __str__(self):
        return "Missing Parameter: %s" % self.param

class BadParameterError(errors.WebError):
    def __init__(self, param, badvalue):
        self.param = param
        self.badvalue = badvalue

    def __str__(self):
        return "Bad parameter %s received for parameter %s" % \
                (self.badvalue, self.param)

def strFields(**params):
    """Decorator for cgi fields.  Use like @strFields(foo=None, bar='foo')
    where foo is a required parameter, and bar defaults to 'foo'.
    Converts parameters to the given type, leaves other parameters untouched.
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
    Converts parameters to the given type, leaves other parameters untouched.
    """

    def deco(func):
        def wrapper(self, **kw):
            for name, default in params.iteritems():
                if name in kw:
                    try:
                        value = int(kw[name])
                    except ValueError, ve:
                        raise BadParameterError(param=name, badvalue=kw[name])
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
                    try:
                        value = bool(int(kw[name]))
                    except ValueError, ve:
                        raise BadParameterError(param=name, badvalue=kw[name])
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
