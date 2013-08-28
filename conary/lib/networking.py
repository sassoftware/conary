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


import fnmatch
import socket
import struct

from conary.lib.compat import namedtuple


class Endpoint(object):
    """Base class for all host, IP, and HostPort types."""
    __slots__ = ()

    def isPrecise(self):
        """Returns C{True} if this endpoint identifies one or more individual
        hosts, or C{False} if it is a network or a pattern.
        """
        return False

    def __deepcopy__(self, memo=None):
        # All subclasses are immutable, so copy is a no-op
        return self
    __copy__ = __deepcopy__

    def __reduce__(self):
        # All subclasses have a complete string representation, and will accept
        # that string as an argument to the constructor, so pickle everything
        # that way.
        return (type(self), (str(self),))


class HostPort(namedtuple('HostPort', 'host port'), Endpoint):
    """Pair (host, port) where host is an address and port is None or a
    port to match against."""
    __slots__ = ()

    def __new__(cls, host, port=None):
        if isinstance(host, basestring) and port is None:
            host, port = splitHostPort(host, rawPort=True)

        if port == '*':
            port = None
        elif port:
            port = int(port)

        if not isinstance(host, BaseAddress):
            if '*' in host:
                host = HostGlob(host)
            else:
                host = BaseAddress.parse(host)

        return tuple.__new__(cls, (host, port))

    def __str__(self):
        out = str(self.host)
        if ':' in out:
            out = '[%s]' % (out,)
        if self.port is not None:
            out += ':%s' % self.port
        return out

    def match(self, other):
        if self.port is not None:
            if isinstance(other, HostPort):
                if self.port != other.port:
                    # Port mismatch
                    return False
            else:
                # Not a HostPort, so port can't match
                return False
            # Test the host part next
            other = other.host

        return self.host.match(other)

    def isPrecise(self):
        return self.host.isPrecise()


class BaseAddress(Endpoint):
    """Base class for all host or IP types, but not HostPort."""
    __slots__ = ()

    _address_cache = {}

    @classmethod
    def parse(cls, val):
        cached = cls._address_cache.get((cls, val))
        if cached is not None:
            return cached

        ret = cls._parse_direct(val)
        cls._address_cache[(cls, val)] = ret
        return ret

    @classmethod
    def _parse_direct(cls, val):
        if '/' in val:
            # It has a CIDR mask, so it must be an IP
            return BaseIPAddress._parse_direct(val)
        elif ':' in val:
            # Definitely an IPv6 address.
            return IPv6Address._parse_direct(val)
        elif val.replace('.', '').isdigit():
            # Probably an IPv4 address, but definitely not a (valid) hostname.
            return IPv4Address._parse_direct(val)
        else:
            # Must be a hostname.
            return Hostname(val)


class Hostname(namedtuple('Hostname', 'name'), BaseAddress):
    """A single hostname which is (probably) resolvable to an IP address."""
    __slots__ = ()

    def __str__(self):
        return self.name

    def resolve(self):
        from conary.lib import httputils
        results = httputils.IPCache.getMany(self.name)
        return [BaseIPAddress.parse(x) for x in results]

    def match(self, other):
        if isinstance(other, Hostname):
            return self.name.lower() == other.name.lower()
        elif isinstance(other, HostPort):
            return self.match(other.host)
        elif isinstance(other, BaseIPAddress):
            # Check the given IP against each possible resolve result for this
            # hostname.
            for ip in self.resolve():
                if ip.match(other):
                    return True
        return False

    def isPrecise(self):
        return True


class HostGlob(namedtuple('HostGlob', 'pattern'), BaseAddress):
    """A pattern (shell-style) that matches against hostnames."""
    __slots__ = ()

    def __str__(self):
        return self.pattern

    def match(self, other):
        if isinstance(other, Hostname):
            # Match against the literal hostname.
            return fnmatch.fnmatch(other.name.lower(), self.pattern.lower())
        elif isinstance(other, BaseIPAddress):
            # Match against the formatted IP address.
            if other.mask != other.bits:
                return False
            return fnmatch.fnmatch(other.format(False), self.pattern.lower())
        elif isinstance(other, HostPort):
            # Match against the host part.
            return self.match(other.host)
        return False


class BaseIPAddress(namedtuple('BaseIPAddress', 'address mask'), BaseAddress):
    """Base class for IPv4 and IPv6 addresses."""
    __slots__ = ()

    bits = None
    family = None

    def __new__(cls, address, mask=None):
        if isinstance(address, basestring):
            ret = cls.parse(address)
            if mask is None:
                return ret
            elif 0 <= mask <= cls.bits:
                return ret._replace(mask=mask)
            else:
                raise ValueError("Invalid address mask %d" % mask)
        else:
            if mask is None:
                mask = 128
            elif not (0 <= mask <= cls.bits):
                raise ValueError("Invalid address mask %d" % mask)
            return tuple.__new__(cls, (address, mask))

    def __repr__(self):
        return '%s(%r)' % (type(self).__name__, self.format())

    def __str__(self):
        return self.format()

    @classmethod
    def _parse_direct(cls, val):
        if cls is BaseIPAddress:
            # Called as BaseIPAddress, so try to figure out what it is
            if val.startswith('::ffff:') and '.' in val:
                val = val[7:]
            if ':' in val:
                return IPv6Address._parse_direct(val)
            else:
                return IPv4Address._parse_direct(val)

        if '/' in val:
            address, mask = val.split('/')
            mask = int(mask)
            if not (0 <= mask <= cls.bits):
                raise ValueError("Invalid address mask %d" % mask)
        else:
            address, mask = val, cls.bits
        return cls._parse_value(address, mask)

    @classmethod
    def _parse_value(cls, address, mask):
        try:
            addr_bin = socket.inet_pton(cls.family, address)
        except socket.error, err:
            raise ValueError("Invalid IP address %r: %s" % (address,
                err.args[0]))

        val = 0
        for i in range(0, cls.bits, 32):
            val <<= 32
            val |= struct.unpack('>I', addr_bin[:4])[0]
            addr_bin = addr_bin[4:]

        return cls(val, mask)

    def format(self, useMask=None):
        """
        Format the address as a string, possibly with a CIDR mask.

        If useMask is False, never append the CIDR mask.
        If useMask is True, always append the CIDR mask.
        If useMask is None, append the CIDR mask only if it is not equal to the
                maximum (e.g. a single host). This is the default.
        """
        addr_long = self.address
        bin_parts = []
        for i in range(0, self.bits, 32):
            bin_parts.append(struct.pack('>I', addr_long & 0xFFFFFFFF))
            addr_long >>= 32
        bin_parts.reverse()
        addr_bin = ''.join(bin_parts)

        out = socket.inet_ntop(self.family, addr_bin)
        if useMask or (useMask is None and self.mask != self.bits):
            out += '/%d' % self.mask
        return out

    def match(self, other):
        """Return C{True} if C{other} is on the network C{self)."""
        if type(self) == type(other):
            if self.mask > other.mask:
                # A more specific pattern cannot match a more general target.
                return False
            netmask = ((1 << self.mask) - 1) << (self.bits - self.mask)
            return (self.address & netmask) == (other.address & netmask)
        elif isinstance(other, HostPort):
            return self.match(other.host)
        elif isinstance(other, Hostname):
            for ip in other.resolve():
                if self.match(ip):
                    return True
        return False

    def isPrecise(self):
        return self.mask == self.bits


class IPv4Address(BaseIPAddress):
    __slots__ = ()

    bits = 32
    family = socket.AF_INET


class IPv6Address(BaseIPAddress):
    __slots__ = ()

    bits = 128
    family = socket.AF_INET6


def splitHostPort(hostport, rawPort=False):
    """Split hostnames like [dead::beef]:8080"""
    i = hostport.rfind(':')
    j = hostport.rfind(']')
    if i > j:
        host, port = hostport[:i], hostport[i+1:]
        if not rawPort:
            port = int(port)
    else:
        host = hostport
        port = None
    if host and host[0] == '[' and host[-1] == ']':
        host = host[1:-1]
    return host, port
