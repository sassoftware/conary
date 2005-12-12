#
# Copyright (c) 2005 rPath, Inc.
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

# Various stuff used by the dbstore drivers

# a case-insensitive key dict
class CaselessDict:
    def __l(self, s):
        if type(s) == type(""):
            return s.lower()
        return s
    def __init__(self, d = None):
        self.dict = {}
        if d is not None:
            for key, val in d.iteritems():
                self.dict[self.__l(key)] = (key, val)

    def __getitem__(self, key):
        return self.dict[self.__l(key)][1]
    def __setitem__(self, key, value):
        self.dict[self.__l(key)] = (key, value)

    def has_key(self, key):
        return self.dict.has_key(self.__l(key))

    def __len__(self):
        return len(self.dict)

    def keys(self):
        return [v[0] for v in self.dict.values()]
    def values(self):
        return [v[1] for v in self.dict.values()]
    def items(self):
        return self.dict.values()

    def setdefault(self, key, val):
        return self.dict.setdefault(self.__l(key), (key, val))[1]

    def __contains__(self, key):
        return self.__l(key) in self.dict

    def __repr__(self):
        items = ", ".join([("%r: %r" % (k,v)) for k,v in self.iteritems()])
        return "{%s}" % items
    def __str__(self):
        return repr(self)
