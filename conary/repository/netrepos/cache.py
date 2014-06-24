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


import time as pytime

class EmptyCache(dict):

    def __init__(self, limit = 10000):
        dict.__init__(self)

    def get(self, key, key_prefix = None):
        return None

    def get_multi(self, keys, key_prefix = None):
        return {}

    def set(self, key, val, time = 0, key_prefix = None):
        return

    def set_multi(self, items, time = 0, key_prefix = None):
        return

    def incr(self, key, delta=1):
        return None


class DumbCache(dict):

    def __init__(self, limit = 2000):
        dict.__init__(self)
        self.limit = limit

    def get(self, key, key_prefix = None):
        key = (key_prefix, key)
        val = dict.get(self, key, None)
        if val is None:
            return None

        (val, expires) = val
        if expires is None:
            return val

        if pytime.time() > expires:
            del self[key]
            return None

        return val

    def get_multi(self, keys, key_prefix = None):
        r = {}
        for key in keys:
            val = self.get(key, key_prefix = key_prefix)
            if val is not None:
                r[key] = val

        return r

    def set(self, key, value, time = 0, key_prefix = None):
        key = (key_prefix, key)
        if time:
            self[key] = (value, pytime.time() + time)
        else:
            self[key] = (value, None)

        if len(self) > self.limit:
            self._shrink()

    def set_multi(self, items, time = 0, key_prefix = None):
        for key, val in items.iteritems():
            self.set(key, val, time = time, key_prefix = key_prefix)

    def _shrink(self):
        order = sorted((x for x in self.items() if x[1][1] is not None),
                       lambda a, b: cmp(b[1][1], a[1][1]))
        toRemove = order[0:self.limit / 10]
        if (len(self) - len(toRemove)) > (self.limit * 0.95):
            toRemove += [ x for x in self.items() ][0:self.limit / 10]

        for x in toRemove:
            del self[x[0]]

    def incr(self, key, delta=1):
        val = self.get(key)
        if val is None:
            return None
        try:
            val = long(val)
        except ValueError:
            return None
        val = str(val + delta)
        self.set(key, val)
        return val


def getCache(url):
    if url is None:
        return DumbCache()

    import memcache
    return memcache.Client([ url ])
