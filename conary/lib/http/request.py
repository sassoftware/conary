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

import urllib
import urlparse

from conary.lib import networking
from conary.lib import util
from conary.lib.compat import namedtuple


class URL(namedtuple('URL', 'scheme userpass hostport path')):

    @classmethod
    def parse(cls, url, defaultPort=None):
        scheme, netloc, path, query, fragment = urlparse.urlsplit(url)
        userpass, hostport = urllib.splituser(netloc)
        if userpass:
            username, password = userpass.split(':', 1)
            password = util.ProtectedString(password)
            userpass = username, password
        else:
            userpass = None, None
        hostport = networking.HostPort(hostport)
        if hostport.port is None:
            if scheme == 'https':
                hostport = hostport._replace(port=443)
            else:
                hostport = hostport._replace(port=80)
        path = urlparse.urlunsplit(('', '', path, query, fragment))
        return cls(scheme, userpass, hostport, path)


class HTTPHeaders(object):
    __slots__ = ('_headers',)

    def __init__(self, headers=()):
        self._headers = {}
        if isinstance(headers, dict):
            headers = headers.iteritems()
        for key, value in headers:
            self[key] = value

    @staticmethod
    def canonical(key):
        return '-'.join(x.capitalize() for x in key.split('-'))

    def __getitem__(self, key):
        key = self.canonical(key)
        return self._headers[key]

    def __setitem__(self, key, value):
        key = self.canonical(key)
        self._headers[key] = value

    def __delitem__(self, key):
        key = self.canonical(key)
        del self._headers[key]

    def get(self, key, default=None):
        key = self.canonical(key)
        return self._headers.get(key)

    def iteritems(self):
        return self._headers.iteritems()


class Request(object):

    def __init__(self, url, body=None, method='GET', headers=()):
        if isinstance(url, basestring):
            url = URL.parse(url)
        self.url = url
        self.body = body
        self.method = method
        self.headers = HTTPHeaders(headers)

    def sendRequest(self, conn):
        conn.putrequest(self.method, self.url.path, skip_host=1)
        sentHost = False
        for key, value in self.headers.iteritems():
            conn.putheader(key, value)
            if key.lower() == 'host':
                sentHost = True
        if not sentHost:
            host = str(self.url.hostport)
            if isinstance(host, unicode):
                host = host.encode('idna')
            conn.putheader("Host", host)
        conn.endheaders()
        if self.body is not None:
            conn.send(self.body)
