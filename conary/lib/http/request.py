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

import StringIO
import urlparse
import zlib

from conary.lib import networking
from conary.lib import util
from conary.lib.compat import namedtuple
from conary.lib.http import http_error


class URL(namedtuple('URL', 'scheme userpass hostport path')):

    @classmethod
    def parse(cls, url, defaultScheme='http'):
        (scheme, username, password, host, port, path, query, fragment,
                ) = util.urlSplit(url)
        if not scheme and defaultScheme is not None:
            scheme = defaultScheme
        if not port:
            if scheme == 'https':
                port = 443
            else:
                port = 80
        hostport = networking.HostPort(host, port)
        path = urlparse.urlunsplit(('', '', path, query, fragment))
        return cls(scheme, (username, password), hostport, path)

    def __str__(self):
        username, password = self.userpass
        host, port = self.hostport
        return util.urlUnsplit((self.scheme, username, password, host, port,
            self.path, '', ''))


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
        self.abortCheck = lambda: False
        self.data = None
        self.chunked = False
        self.callback = None

    def setData(self, data, size=None, compress=False, callback=None):
        if compress:
            data = zlib.compress(data, 9)
            size = len(data)
            self.headers['Accept-Encoding'] = 'deflate'
            self.headers['Content-Encoding'] = 'deflate'
        self.data = data
        self.callback = callback
        if size is None:
            try:
                size = len(data)
            except TypeError:
                pass
        self.size = size
        self.headers['Content-Length'] = str(size)
        if size is None:
            self.chunked = True
            self.headers['Transfer-Encoding'] = 'chunked'
        else:
            self.chunked = False

    def setAbortCheck(self, abortCheck):
        if not abortCheck:
            abortCheck = lambda: False
        self.abortCheck = abortCheck

    def sendRequest(self, conn):
        conn.putrequest(self.method, self.url.path, skip_host=1,
                skip_accept_encoding=1)
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
        conn.endheaders()
        self._sendData(conn)

    def _sendData(self, conn):
        data = self.data
        size = self.size
        if data is None:
            return
        if not self.chunked:
            assert size is not None
            if not hasattr(data, 'read'):
                conn.send(data)
                return
            util.copyfileobj(data, conn, callback=self.callback,
                    sizeLimit=size, abortCheck=self.abortCheck)
            return
        if not hasattr(data, 'read'):
            data = StringIO.StringIO(data)
        while size or size is None:
            if self.abortCheck():
                raise http_error.AbortError()
            # send in 256k chunks
            chunk = 262144
            if size is not None and chunk > size:
                chunk = size
            chunk_data = data.read(chunk)
            conn.send(''.join((
                '%x\r\n' % len(chunk_data),
                chunk_data,
                '\r\n')))
            if size is not None:
                size -= chunk
        # terminate the chunked encoding
        conn.send('0\r\n\r\n')
