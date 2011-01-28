#
# Copyright (c) 2011 rPath, Inc.
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

import random

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

    def addStrategy(self, matchHost, targets):
        filterSpec = FilterSpec(matchHost)
        targets2 = []
        for target in targets:
            if isinstance(target, basestring):
                if target == 'DIRECT':
                    target = DirectConnection
                else:
                    target = req_mod.URL.parse(target)
            targets2.append(target)
        self.filterList.append((filterSpec, targets2))

    @classmethod
    def fromDict(cls, values):
        val = cls()
        for scheme, url in sorted(values.items()):
            if scheme in ('http', 'https'):
                val.addStrategy(scheme + ':*', [url])
        return val

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
            if not filterSpec.match(url.hostport):
                # Filter doesn't match the current request.
                continue
            targets = targets[:]
            random.shuffle(targets)
            for target in targets:
                if target is not DirectConnection:
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


class FilterSpec(object):

    def __init__(self, value):
        if value.startswith('http:'):
            self.protocol = 'http'
            address = value[5:]
        elif value.startswith('https:'):
            self.protocol = 'https'
            address = value[6:]
        else:
            self.protocol = None
            address = value
        self.address = networking.HostPort(address)

    def __str__(self):
        value = str(self.address)
        if self.protocol:
            return ':'.join((self.protocol, value))
        else:
            return value


class DirectConnection(object):

    def __str__(self):
        return 'DIRECT'

    def __repr__(self):
        return 'DirectConnection'

DirectConnection = DirectConnection()
