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

import base64
import urlparse
import zlib

from conary.lib import networking
from conary.lib import util
from conary.lib.compat import namedtuple


class URL(namedtuple('URL', 'scheme userpass hostport path')):

    def __new__(cls, scheme, userpass=None, hostport=None, path=None):
        if userpass is None and hostport is None and path is None:
            return cls.parse(scheme)
        else:
            return tuple.__new__(cls, (scheme, userpass, hostport, path))

    @classmethod
    def parse(cls, url, defaultScheme='http'):
        if '://' not in url and defaultScheme:
            url = '%s://%s' % (defaultScheme, url)
        (scheme, username, password, host, port, path, query, fragment,
                ) = util.urlSplit(url)
        if not port and port != 0:
            if scheme[-1] == 's':
                port = 443
            else:
                port = 80
        hostport = networking.HostPort(host, port)
        path = urlparse.urlunsplit(('', '', path, query, fragment))
        return cls(scheme, (username, password), hostport, path)

    def __str__(self):
        username, password = self.userpass
        host, port = self.hostport
        if (self.scheme == 'http' and port == 80) or (
                self.scheme == 'https' and port == 443):
            port = None
        return util.urlUnsplit((self.scheme, username, password, str(host),
            port, self.path, None, None))


class HTTPHeaders(object):
    __slots__ = ('_headers',)

    def __init__(self, headers=None):
        self._headers = {}
        if headers:
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

    def __contains__(self, key):
        key = self.canonical(key)
        return key in self._headers

    def get(self, key, default=None):
        key = self.canonical(key)
        return self._headers.get(key)

    def iteritems(self):
        return self._headers.iteritems()

    def setdefault(self, key, default):
        key = self.canonical(key)
        return self._headers.setdefault(key, default)


class Request(object):

    def __init__(self, url, method='GET', headers=()):
        if isinstance(url, basestring):
            url = URL.parse(url)
        self.url = url
        self.method = method
        self.headers = HTTPHeaders(headers)

        # Params for sending request entity
        self.abortCheck = lambda: False
        self.data = None
        self.size = None
        self.chunked = False
        self.callback = None
        self.rateLimit = None

    def setData(self, data, size=None, compress=False, callback=None,
            chunked=False, rateLimit=None):
        if compress:
            data = zlib.compress(data, 9)
            size = len(data)
            self.headers['Accept-Encoding'] = 'deflate'
            self.headers['Content-Encoding'] = 'deflate'
        self.data = data
        self.callback = callback
        self.rateLimit = rateLimit
        if size is None:
            try:
                size = len(data)
            except TypeError:
                pass
        self.size = size
        self.headers['Content-Length'] = str(size)
        if chunked or size is None:
            self.chunked = True
            self.headers['Transfer-Encoding'] = 'chunked'
        else:
            self.chunked = False

    def setAbortCheck(self, abortCheck):
        if not abortCheck:
            abortCheck = lambda: False
        self.abortCheck = abortCheck

    def sendRequest(self, conn, isProxied=False):
        if isProxied:
            cleanUrl = self.url._replace(userpass=(None,None))
            path = str(cleanUrl)
        else:
            path = self.url.path
        conn.putrequest(self.method, path, skip_host=1, skip_accept_encoding=1)
        self.headers.setdefault('Accept-Encoding', 'identity')
        for key, value in self.headers.iteritems():
            conn.putheader(key, value)
        if 'Host' not in self.headers:
            if self.url.hostport.port in (80, 443):
                host = str(self.url.hostport.host)
            else:
                host = str(self.url.hostport)
            if isinstance(host, unicode):
                host = host.encode('idna')
            conn.putheader("Host", host)
        if 'Authorization' not in self.headers and self.url.userpass[0]:
            conn.putheader("Authorization",
                    "Basic " + base64.b64encode(":".join(self.url.userpass)))
        conn.endheaders()
        self._sendData(conn)

    def _sendData(self, conn):
        if self.data is None:
            return
        if not hasattr(self.data, 'read'):
            conn.send(self.data)
            return

        if self.chunked:
            # Use chunked coding
            output = wrapper = ChunkedSender(conn)
        elif self.size is not None:
            # Use identity coding
            output = conn
            wrapper = None
        else:
            raise RuntimeError("Request must use chunked transfer coding "
                    "if size is not known.")
        util.copyfileobj(self.data, output, callback=self.callback,
                rateLimit=self.rateLimit, abortCheck=self.abortCheck,
                sizeLimit=self.size)
        if wrapper:
            wrapper.close()


class ChunkedSender(object):
    """
    Do HTTP chunked transfer coding by wrapping a socket-like object,
    intercepting send() calls and sending the correct leading and trailing
    metadata.
    """

    def __init__(self, target):
        self.target = target

    def send(self, data):
        self.target.send("%x\r\n%s\r\n" % (len(data), data))

    def close(self, trailer=''):
        self.target.send("0\r\n%s\r\n" % (trailer,))