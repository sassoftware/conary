#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


import socket

from conary.lib import util

LocalHosts = set(['localhost', 'localhost.localdomain', '127.0.0.1'])


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
