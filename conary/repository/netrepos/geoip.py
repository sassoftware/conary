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

import re
from conary.deps import deps
from conary.lib import networking

try:
    import pygeoip
    GeoIPError = pygeoip.GeoIPError
except ImportError:
    pygeoip = None  # pyflakes=ignore
    GeoIPError = RuntimeError  # for testing


FLAG_RE = re.compile('^[a-zA-Z0-9]+$')


class GeoIPLookup(object):

    RESERVED = {
        'site-local': [
            # RFC 1918
            networking.IPv4Address('10.0.0.0/8'),
            networking.IPv4Address('172.16.0.0/12'),
            networking.IPv4Address('192.168.0.0/16'),
            # RFC 4193
            networking.IPv6Address('fc00::/7'),
            # RFC 3879
            networking.IPv6Address('fec0::/10'),
            ],
        'carrier-nat': [
            # RFC 6598
            networking.IPv4Address('100.64.0.0/10'),
            ],
        'link-local': [
            # RFC 3927
            networking.IPv4Address('169.254.0.0/16'),
            # RFC 4291
            networking.IPv6Address('fe80::/10'),
            ],
        'loopback': [
            # RFC 5735
            networking.IPv4Address('127.0.0.0/8'),
            # RFC 4291
            networking.IPv6Address('::1/128'),
            ],
        '6to4': [
            # RFC 3068
            networking.IPv4Address('192.88.99.0/24'),
            ],
        }

    def __init__(self, paths):
        if paths and pygeoip is None:
            raise TypeError("pygeoip module is not installed")
        self.dbs = [pygeoip.GeoIP(x, pygeoip.MMAP_CACHE) for x in paths]

    def getFlags(self, remote_ip):
        if not isinstance(remote_ip, networking.BaseIPAddress):
            remote_ip = networking.BaseIPAddress.parse(remote_ip)
        for space, networks in self.RESERVED.items():
            for network in networks:
                if network.match(remote_ip):
                    return deps.parseFlavor('reserved.' + space)
        for db in self.dbs:
            try:
                cc = db.country_code_by_addr(str(remote_ip))
            except GeoIPError:
                continue
            if not FLAG_RE.match(cc):
                continue
            return deps.parseFlavor('country.' + cc)
        return deps.Flavor()
