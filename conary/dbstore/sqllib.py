#
# Copyright (c) 2005-2008 rPath, Inc.
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

# Various stuff used by the dbstore drivers
import time

# class to aid in comparing database versions
class DBversion:
    def __init__(self, major, minor=0):
        self.major = major
        self.minor = minor

    def __nonzero__(self):
        return self != 0

    def __cmp__(self, other):
        if isinstance(other, int):
            if self.major == other:
                return cmp(self.minor, 0)
            return cmp(self.major, other)
        elif isinstance(other, tuple):
            assert(len(other) == 2)
            return cmp(self.major, other[0]) or cmp(self.minor, other[1])
        elif isinstance(other, self.__class__):
            return cmp(self.major, other.major) or cmp(self.minor, other.minor)
        raise RuntimeError("incompatible type compare for DBversion",
                           [(self.major, self.minor), other])
    def __repr__(self):
        return "DBversion(%d,%d)" % (self.major, self.minor)

    def __str__(self):
        if self.minor:
            return '%d.%d' % (self.major, self.minor)
        else:
            return str(self.major)

# a case-insensitive key dict
class CaselessDict(dict):
    def __init__(self, d=None):
        dict.__init__(self)
        if isinstance(d, list):
            d = dict(d)
        if isinstance(d, dict):
            self.update(d)
    # lowercase the key
    def __l(self, s):
        if isinstance(s, str):
            return s.lower()
        return s
    def __getitem__(self, key):
        return dict.__getitem__(self, self.__l(key))[1]
    def __setitem__(self, key, value):
        dict.__setitem__(self, self.__l(key), (key, value))
    def __delitem__(self, key):
        dict.__delitem__(self, self.__l(key))
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
        if not isinstance(other, dict):
            return False
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

# PostgreSQL lowercase everything automatically, so we need a special
# "lowercase match" list type for matches like
# idxname in db.tables[x]
class Llist(list):
    def __contains__(self, item):
        return item.lower() in [x.lower() for x in list.__iter__(self)]
    def remove(self, item):
        return list.pop(self, self.index(item))
    def index(self, item):
        return [x.lower() for x in list.__iter__(self)].index(item.lower())

# convert time.time() to timestamp with optional offset
def toDatabaseTimestamp(secsSinceEpoch=None, offset=0):
    """
    Given the number of seconds since the epoch, return a datestamp
    in the following format: YYYYMMDDhhmmss.

    Default behavior is to return a timestamp based on the current time.

    The optional offset parameter lets you retrive a timestamp whose time
    is offset seconds in the past or in the future.

    This function assumes UTC.
    """

    if secsSinceEpoch == None:
        secsSinceEpoch = time.time()

    timeToGet = time.gmtime(secsSinceEpoch + float(offset))
    return long(time.strftime('%Y%m%d%H%M%S', timeToGet))
