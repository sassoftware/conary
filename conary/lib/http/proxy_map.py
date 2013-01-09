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


import random
import urllib

from conary.lib import networking
from conary.lib import util
from conary.lib.http import request as req_mod


class ProxyMap(object):
    BLACKLIST_TTL = 60 * 60  # The TTL for server blacklist entries (seconds)

    _MISSING = object()

    def __init__(self):
        self.filterList = []
        self._blacklist = util.TimestampedMap(self.BLACKLIST_TTL)

    def __nonzero__(self):
        return bool(self.filterList)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.filterList == other.filterList

    def items(self):
        return self.filterList[:]

    def clear(self):
        self.filterList = []

    def addStrategy(self, matchHost, targets, replaceScheme=None):
        filterSpec = FilterSpec(matchHost)
        targets2 = []
        for target in targets:
            if isinstance(target, basestring):
                if target.lower() == 'direct':
                    target = DirectConnection
                else:
                    target = req_mod.URL.parse(target)
            if (replaceScheme and target != DirectConnection and
                    target.scheme.startswith('http')):
                # https -> conarys, etc.
                target = target._replace(
                        scheme=replaceScheme + target.scheme[4:])
            targets2.append(target)
        self.filterList.append((filterSpec, targets2))

    @classmethod
    def fromDict(cls, values):
        val = cls()
        if isinstance(values, dict):
            values = values.iteritems()
        elif values is None:
            return val
        for scheme, url in sorted(values):
            if scheme in ('http', 'https'):
                val.addStrategy(scheme + ':*', [url])
        return val

    @classmethod
    def fromEnvironment(cls):
        return cls.fromDict(urllib.getproxies())

    def blacklistUrl(self, url, error=None):
        assert isinstance(url, req_mod.URL)
        self._blacklist.set(url, error)

    def isUrlBlacklisted(self, url):
        error = self._blacklist.get(url, self._MISSING)
        return error is not self._MISSING

    def clearBlacklist(self):
        self._blacklist.clear()

    def getProxyIter(self, url, protocolFilter=('http', 'https')):
        """Returns an iterator which yields successive connection strategies to
        the given URL.

        @param url: Destination URL
        @param protocolFilter: Use only proxies with these protocols.
        """
        if isinstance(url, basestring):
            url = req_mod.URL.parse(url)

        hasMatches = False
        for filterSpec, targets in self.filterList[:]:
            if not filterSpec.match(url):
                # Filter doesn't match the current request.
                continue
            targets = targets[:]
            random.shuffle(targets)
            for target in targets:
                if target != DirectConnection:
                    if target.scheme not in protocolFilter:
                        # Target isn't usable for whatever request is being
                        # made.
                        continue
                    hasMatches = True
                    if self.isUrlBlacklisted(target):
                        # Target is blacklisted
                        continue
                else:
                    hasMatches = True
                yield target

        if not hasMatches:
            # Assume a direct connection if no strategies matched.
            yield DirectConnection


class FilterSpec(networking.namedtuple('FilterSpec', 'protocol address')):
    __slots__ = ()

    def __new__(cls, value, address=None):
        if isinstance(value, FilterSpec):
            protocol = value.protocol
            address = value.address
        else:
            if value is None:
                protocol = None
            elif value.startswith('http:'):
                protocol = 'http'
                address = value[5:]
            elif value.startswith('https:'):
                protocol = 'https'
                address = value[6:]
            elif address is not None:
                protocol = value
            else:
                protocol = None
                address = value
            if not isinstance(address, networking.HostPort):
                address = networking.HostPort(address)
        return tuple.__new__(cls, (protocol, address))

    def __str__(self):
        value = str(self.address)
        if self.protocol:
            return ':'.join((self.protocol, value))
        else:
            return value

    def match(self, url):
        if self.protocol and self.protocol != url.scheme:
            return False
        return self.address.match(url.hostport)


class DirectConnectionType(object):
    """
    This singleton is used instead of a URL for strategies that connect
    directly to the target, instead of through a proxy.
    """
    __slots__ = ()

    def __new__(cls):
        # This is necessary to prevent various things from creating more than
        # one instance. It's still possible that some things might
        # double-instantiate though, so the recommended way to test against
        # this singleton is to check for equality.
        if not hasattr(cls, '_instance'):
            cls._instance = object.__new__(cls)
        return cls._instance

    def __str__(self):
        return 'DIRECT'

    def __repr__(self):
        return 'DirectConnection'

    def __eq__(self, other):
        return isinstance(other, type(self))

    def __ne__(self, other):
        return not isinstance(other, type(self))

    def __reduce__(self):
        # Pickle v1 will bypass __new__ for some reason unless this is here.
        return (type(self), ())


DirectConnection = DirectConnectionType()
