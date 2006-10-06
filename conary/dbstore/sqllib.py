#
# Copyright (c) 2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

# Various stuff used by the dbstore drivers

# a case-insensitive key dict
class CaselessDict(dict):
    def __init__(self, d=None):
        if isinstance(d, list):
            dict.__init__(self, d)
        elif isinstance(d, dict):
            dict.update(self, d)
    # lowercase the key
    def __l(self, s):
        if isinstance(s, str):
            return s.lower()
        return s
    def __getitem__(self, key):
        return dict.__getitem__(self, self.__l(key))[1]
    def __setitem__(self, key, value):
        dict.__setitem__(self, self.__l(key), (key, value))
    def has_key(self, key):
        return dict.has_key(self, self.__l(key))

    def keys(self):
        return [v[0] for v in dict.values(self)]
    def values(self):
        return [v[1] for v in dict.values(self)]
    def items(self):
        return dict.values(self)

    def setdefault(self, key, val):
        return dict.setdefault(self, self.__l(key), (key, val))[1]
    def update(self, other):
        for item in other.iteritems():
            self.__setitem__(*item)
    def __contains__(self, key):
        return dict.__contains__(self, self.__l(key))
    def __repr__(self):
        return repr(dict(dict.values(self)))
    def __str__(self):
        return str(dict(dict.values(self)))
    def __iter__(self):
        for k in dict.itervalues(self):
            yield k[0]
    def __eq__(self, other):
        if dict.__len__(self) != len(other):
            return False
        for k, v in other.iteritems():
            lk = self.__l(k)
            if not dict.has_key(self, lk):
                return False
            if dict.__getitem__(self, lk)[1] != v:
                return False
        return True
    def iteritems(self):
        return (v for v in dict.itervalues(self))
    def iterkeys(self):
        return (v[0] for v in dict.itervalues(self))
    def itervalues(self):
        return (v[1] for v in dict.itervalues(self))
