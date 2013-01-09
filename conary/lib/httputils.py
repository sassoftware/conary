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


import socket

from conary.lib import util

LocalHosts = set([
    'localhost', 'localhost.localdomain',
    'localhost4', 'localhost4.localdomain4',
    'localhost6', 'localhost6.localdomain6',
    '127.0.0.1', '::1',
    '::ffff:127.0.0.1',
    ])


class IPCache(object):
    """
    A global IP cache
    """

    # Maps hostname to a list of IP addresses, as returned by getaddrinfo.
    _cache = util.TimestampedMap(delta=600)

    @staticmethod
    def _resolve(host):
        # This is split out to make mocking easier
        return socket.getaddrinfo(host, None, 0, socket.SOCK_STREAM)

    @classmethod
    def getMany(cls, host, resetResolver=False, stale=False):
        # Fetch fresh results only first
        ret = cls._cache.get(host, None, stale=False)
        if ret is not None:
            return ret
        try:
            results = cls._resolve(host)
        except (IOError, socket.error):
            if not resetResolver and not stale:
                raise
            if stale:
                ret = cls._cache.get(host, None, stale=True)
                if ret is not None:
                    return ret
            # Recursively call ourselves
            util.res_init()
            return cls.get(host, resetResolver=False, stale=False)
        else:
            # [(family, type, proto, canonname, (host, port, ...))]
            results = [x[4][0] for x in results]
            cls._cache.set(host, results)
            return results

    @classmethod
    def get(cls, host, resetResolver=False, stale=False):
        return cls.getMany(host, resetResolver, stale)[0]

    @classmethod
    def clear(cls):
        cls._cache.clear()
