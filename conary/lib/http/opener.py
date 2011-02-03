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

import errno
import httplib
import logging
import socket

from conary.lib import httputils
from conary.lib import util
from conary.lib import timeutil
from conary.lib.http import connection as conn_mod
from conary.lib.http import http_error
from conary.lib.http import proxy_map
from conary.lib.http import request as req_mod

log = logging.getLogger(__name__)


class URLOpener(object):
    contentType = 'application/x-www-form-urlencoded'
    userAgent = 'conary-http-client/0.1'

    connectionFactory = conn_mod.Connection
    requestFactory = req_mod.Request

    # Only try proxies with these schemes.
    proxyFilter = ('http', 'https')
    maxRetries = 3

    def __init__(self, proxyMap=None, caCerts=None):
        if proxyMap is None:
            proxyMap = proxy_map.ProxyMap()
        self.proxyMap = proxyMap
        self.caCerts = caCerts

        self.connectionCache = {}
        self.lastProxy = None

    def newRequest(self, url, data=None, method=None, headers=()):
        req = self.requestFactory(url, method, headers)
        if data is not None:
            req.setData(data)
        return req

    def open(self, url, data=None, method=None, headers=(), forceProxy=False):
        if isinstance(url, req_mod.Request):
            req = url
        else:
            if isinstance(url, basestring):
                url = req_mod.URL.parse(url)
            elif isinstance(url, req_mod.URL):
                pass
            else:
                raise TypeError("Expected a URL or Request object")

            if method is None:
                if data is None:
                    method = 'GET'
                else:
                    method = 'POST'
            req = self.newRequest(url, data, method, headers)

        req.headers.setdefault('Content-Type', self.contentType)
        req.headers.setdefault('User-Agent', self.userAgent)

        if req.url.scheme == 'file':
            return self._handleFileRequest(req)
        elif req.url.scheme not in ('http', 'https'):
            raise TypeError("Unknown URL scheme %r" % (req.url.scheme,))

        response = self._doRequest(req, forceProxy=forceProxy)
        if response.status == 200:
            return self._handleResponse(req, response)
        else:
            return self._handleError(req, response)

    def _handleResponse(self, req, response):
        fp = response
        encoding = response.getheader('content-encoding', None)
        if encoding == 'deflate':
            # disable until performace is better
            #fp = DecompressFileObj(fp)
            fp = util.decompressStream(fp)
            fp.seek(0)
        return ResponseWrapper(fp, response)

    def _handleError(self, req, response):
        self._handleProxyErrors(response.status)
        raise http_error.ResponseError(req.url, self.lastProxy,
                response.status, response.reason)

    def _handleFileRequest(self, req):
        return open(req.url.path, 'rb')

    def _doRequest(self, req, forceProxy):
        connIterator = self.proxyMap.getProxyIter(req.url,
                protocolFilter=self.proxyFilter)
        resetResolv = False
        lastError = response = None
        for proxySpec in connIterator:
            if proxySpec is proxy_map.DirectConnection:
                proxySpec = None
            elif not forceProxy and self._shouldBypass(req.url, proxySpec):
                proxySpec = None
            # If a proxy was used, save it here
            self.lastProxy = proxySpec
            try:
                response = self._requestOnce(req, proxySpec)
                break

            except socket.error, err:
                lastError = util.SavedException()
                if err.args[0] == 'socket error':
                    err = err.args[1]
                self._processSocketError(err)
                lastError.replace(err)
                if isinstance(err, socket.gaierror):
                    if err.args[0] == socket.EAI_AGAIN:
                        pass
                    else:
                        break
                elif isinstance(err, socket.sslerror):
                    pass
                else:
                    break
            except httplib.BadStatusLine:
                # closed connection without sending a response.
                lastError = util.SavedException()
            except socket.error, e:
                self._processSocketError(e)
                util.rethrow(e)
            # try resetting the resolver - /etc/resolv.conf
            # might have changed since this process started.
            if not resetResolv:
                util.res_init()
                resetResolv = True

        if not response:
            if lastError:
                lastError.throw()
            else:
                # There wasn't anything to connect to, for some reason.
                raise http_error.TransportError("Unable to connect to host %s"
                        % (req.url.hostport,))

        return response

    def _shouldBypass(self, url, proxy):
        dest = str(url.hostport.host)
        pdest = str(proxy.hostport.host)

        # Don't proxy localhost unless the proxy is also localhost.
        if dest in httputils.LocalHosts and pdest not in httputils.LocalHosts:
            return True

        # Ignore no_proxy for Conary proxies.
        if proxy.scheme in ('conary', 'conarys'):
            return False

        # Check no_proxy
        npFilt = util.noproxyFilter()
        return npFilt.bypassProxy(dest)

    def _requestOnce(self, req, proxy):
        """Issue a request to a a single destination, retrying if the
        conditions allow it.
        """
        key = (req.url.scheme, req.url.hostport, proxy)
        conn = self.connectionCache.get(key)
        if conn is None:
            conn = self.connectionFactory(req.url, proxy, self.caCerts)
            self.connectionCache[key] = conn

        timer = timeutil.BackoffTimer()
        for attempt in range(self.maxRetries + 1):
            if attempt:
                timer.sleep()
            try:
                result = conn.request(req)
            except socket.error, err:
                if err.args[0] in (errno.ECONNREFUSED, socket.EAI_AGAIN):
                    # Server is down or the nameserver was unreachable, these
                    # are harmless enough to retry.
                    continue
                raise
            if result.status in (502, 503):
                # The remote server is down or the proxy is misconfigured, try
                # again.
                continue
            if attempt:
                log.info("Successfully reached %s after %d attempts.",
                        req.url.hostport, attempt + 1)
            return result
        raise

    def _handleProxyErrors(self, errcode):
        """Translate proxy error codes into exceptions."""
        if errcode == 503:
            # Service unavailable, make it a socket error
            e = socket.error(111, "Service unavailable")
        elif errcode == 502:
            # Bad gateway (server responded with some broken answer)
            e = socket.error(111, "Bad Gateway (error reported by proxy)")
        else:
            return
        self._processSocketError(e)
        raise e

    def _processSocketError(self, error):
        """Append proxy information to an exception."""
        if not self.lastProxy:
            return
        if self.lastProxy.scheme in ('conary', 'conarys'):
            kind = 'Conary'
        else:
            kind = 'HTTP'
        msgError = "%s (via %s proxy %s)" % (error[1], kind,
                self.lastProxy)
        error.args = (error[0], msgError)
        if hasattr(error, 'strerror'):
            error.strerror = msgError

    def close(self):
        for conn in self.connectionCache.values():
            conn.close()
        self.connectionCache.clear()


class ResponseWrapper(object):

    def __init__(self, fp, response):
        self.fp = fp
        self.response = response

        self.status = response.status
        self.reason = response.reason
        self.headers = response.msg
        self.protocolVersion = "HTTP/%.1f" % (response.version / 10.0)

        self.read = fp.read

    def close(self):
        self.fp.close()
        self.response.close()

    def _readlineify(self):
        if hasattr(self.fp, 'readline'):
            return
        fp = util.BoundedStringIO()
        util.copyfileobj(self.fp, fp)
        self.fp = fp

    def readline(self):
        self._readlineify()
        return self.fp.readline()

    # Backwards compatibility with urllib.addinfourl
    code = property(lambda self: self.status)
    msg = property(lambda self: self.headers)
