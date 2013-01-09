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


"""
Module implementing the "macro" dictionary class
"""

import re

from conary.build.errors import MacroKeyError

class Macros(dict):
    def __init__(self, macros={}, shadow=False, ignoreUnknown=False):
        self.__tracked = {}
        self.__track = False
        self.__overrides = {}
        self.__callbacks = {}
        self.__ignoreUnknown = ignoreUnknown
        if shadow:
            self.__macros = macros
        else:
            self.__macros = {}
            self.update(macros)

    def _get(self, key):
        return dict.__getitem__(self, key)

    def update(self, other):
        if isinstance(other, dict):
            for key, item in other.iteritems():
                self[key] = item
        else:
            for key, item in other:
                self[key] = item

    def setCallback(self, name, callback):
        """ Add a callback to a particular macros.  When that macro is
            accessed, the callback function will be called with that macro's
            name as an argument
        """
        self.__callbacks[name] = callback

    def unsetCallback(self, name):
        del self.__callbacks[name]

    def __setitem__(self, name, value):
        if name.startswith('_Macros'):
            dict.__setitem__(self, name, value)
            return
        # '.' in name reserved for getting alternative representations
        if '.' in name:
            raise MacroError, 'name "%s" contains illegal character: "."' % name
        if self.__track:
            self.__tracked[name] = 1
        # only expand references to ourself
        d = {name: self.get(name)}
        # escape any macros in the new value
        value = value.replace('%', '%%')
        # unescape references to ourself
        value = value.replace('%%%%(%s)s' %name, '%%(%s)s'%name)
        # expand our old value when defining the new value
        dict.__setitem__(self, name, value % d)

    # overrides allow you to set a macro value at the command line
    # or in a config file and use it despite the value being
    # set subsequently within the recipe

    def _override(self, key, value):
        self.__overrides[key] = value
        self[key] = value

    def __setattr__(self, name, value):
        self.__setitem__(name, value)

    def __getitem__(self, name):
        if name.startswith('_Macros'):
            return dict.__getitem__(self, name)
        repmethod = None
        parts = name.split('.', 1)
        if len(parts) > 1:
            repmethod = parts[1]
            name = parts[0]
        if name in self.__callbacks:
            self.__callbacks[name](name)
        if name in self.__overrides:
            return self.__repmethod(self.__overrides[name], repmethod)
        if not name in self:
            # update on access
            try:
                value = self.__macros[name]
            except KeyError:
                # let's make this error message more helpful
                # so our users will have a chance of debugging.
                if self.__ignoreUnknown:
                    return ''
                raise MacroKeyError(name)

            value = self.__macros[name]
            self[name] = value
            return self.__repmethod(value, repmethod)
        else:
            return self.__repmethod(dict.__getitem__(self, name) % self, repmethod)

    def __repmethod(self, name, repmethod):
        if repmethod is None:
            return name
        if repmethod == 'literalRegex':
            return re.escape(name)
        # should not be reached
        raise MacroError, 'unknown representation method %s for %s' %(repmethod, name)

    def __getattr__(self, name):
        return self.__getitem__(name)

    def trackChanges(self, flag=True):
        self.__track = flag

    def getTrackedChanges(self):
        return self.__tracked.keys()

    def copy(self, shadow=True):
        # shadow saves initial copying cost for a higher search cost
        if not shadow:
            return Macros([(x, self._get(x)) for x in dict.__iter__(self)])
        return Macros(self, shadow)

    def __deepcopy__(self, memo):
        return self.copy(self, shadow=False)

    # occasionally it may be desirable to switch from shadowing
    # to a flattened representation
    def _flatten(self):
        if self.__macros:
            # just accessing the element will copy it to this
            # macro
            for key in self.__macros.keys():
                dummy = self[key]
            self.__macros = {}

    def __iter__(self):
        # since we are accessing every element in the parent anyway
        # just flatten hierarchy first, which greatly simplifies iterating
        self._flatten()
        # iter over self and parents
        for key in dict.__iter__(self):
            if not key.startswith('_Macros'):
                yield key

    def iterkeys(self):
        for key in self.__iter__():
            yield key

    def iteritems(self):
        for key in self.__iter__():
            yield (key, self[key])

    def itermacros(self):
        for key in self.__iter__():
            yield (key, self._get(key))

    def keys(self):
        return [ x for x in self.__iter__() ]


class MacroError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
        return self.msg

    def __str__(self):
        return repr(self)
