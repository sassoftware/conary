#
# Copyright (c) 2004-2010 rPath, Inc.
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
import errno
import glob
import httplib
import itertools
import logging
import os
import random
import select
import socket
import sys
import time
import urllib
import warnings
import zlib

from conary import errors
from conary.lib import util

log = logging.getLogger(__name__)

try:
    # Use m2crypto for checking server certificates
    from M2Crypto import SSL
    SSLVerificationError = SSL.Checker.SSLVerificationError
except ImportError:
    SSL = None
    class SSLVerificationError(Exception):
        # If M2Crypto is not installed, no verification is performed, so this
        # is just a placeholder to simplify exception handling
        pass

LocalHosts = set(['localhost', 'localhost.localdomain', '127.0.0.1',
                  socket.gethostname()])

class HTTPSConnection(httplib.HTTPConnection):
    """
    HTTPS connection that supports m2crypto contexts plus some other features.

    m2crypto's httpslib isn't used here because it is too simple to bother
    inheriting.

    Currently supported "extra" features:
     * Can pass in a list of peer certificate authorities.
     * Can set the hostname used to check the peer's certificate.
    """
    default_port = httplib.HTTPS_PORT

    def __init__(self, host, port=None, strict=None, caCerts=None,
            commonName=None):
        httplib.HTTPConnection.__init__(self, host, port, strict)
        self.caCerts = caCerts
        self.commonName = commonName

        self.ssl_ctx = SSL.Context('sslv23')
        if caCerts:
            self.ssl_ctx.set_verify(SSL.verify_peer, depth=9)
            paths = []
            for path in caCerts:
                paths.extend(sorted(list(glob.glob(path))))
            for path in paths:
                if os.path.isdir(path):
                    self.ssl_ctx.load_verify_locations(capath=path)
                elif os.path.exists(path):
                    self.ssl_ctx.load_verify_locations(cafile=path)

    def connect(self):
        self.sock = SSL.Connection(self.ssl_ctx)
        self.sock.clientPostConnectionCheck = self.checkSSL
        self.sock.connect((self.host, self.port))

    def adopt(self, sock):
        """
        Set this connection's underlying socket to C{sock} and wrap it with the
        SSL connection object. Assume the socket is already open but has not
        exchanged any SSL traffic.
        """
        self.sock = SSL.Connection(self.ssl_ctx, sock)
        self.sock.setup_ssl()
        self.sock.set_connect_state()
        self.sock.connect_ssl()
        if not self.checkSSL(self.sock.get_peer_cert(), self.host):
            raise SSLVerificationError('post connection check failed')

    def close(self):
        # See M2Crypto/httpslib.py:67
        pass

    def checkSSL(self, cert, host):
        """
        Peer cert checker that will use an alternate hostname for the
        comparison, e.g. if the actual connect host is an IP this can be used
        to specify the original hostname.
        """
        if self.commonName:
            host = self.commonName
        checker = SSL.Checker.Checker()
        return checker(cert, host)

class ConnectionManager(object):
    CONN_PLAIN = 0
    CONN_PROXY = 2
    CONN_SSL = 4
    CONN_TUNNEL = 8
    CONN_SSL_TUNNEL = CONN_SSL | CONN_TUNNEL

    URL = util.URL
    ProxyURL = util.ProxyURL

    ProtocolMaps = dict(http = [ 'http:http' ], https = [ 'http:https' ])

    class _ConnectionIterator(object):
        __slots__ = [ 'connSpec', 'proxyMap', 'protocols',
            'retries', 'proxyRetries',
            'proxyLists', '_listIndex', '_retryCount', '_timer', '_iter', ]
        ProxyTypeName = dict(http = 'HTTP', https = 'HTTP')

        def __init__(self, connSpec, proxyMap, protocols, retries, proxyRetries):
            self.connSpec = connSpec
            self.proxyMap = proxyMap
            self.retries = retries
            self.proxyRetries = proxyRetries
            self.protocols = protocols
            self._retryCount = 0
            self._timer = BackoffTimer()
            self._iter = self.proxyMap.getProxyIter(self.connSpec, self.protocols)

        def __iter__(self):
            return self

        @property
        def retryCount(self):
            return self._retryCount

        def next(self):
            try:
                proxy = self._iter.next()
            except StopIteration:
                if self._retryCount == self.retries:
                    raise StopIteration
                if self._retryCount != 0:
                    # Sleep and try again, per RFC 2616 s. 8.2.4
                    self._timer.sleep()
                self._retryCount += 1
                return (self.connSpec, None)
            except errors.ProxyListExhausted, e:
                ei = sys.exc_info()
                proxyErrors = [ x[1]
                    for x in self.proxyMap.iterBlacklist(stale=True)
                        if x[1].host in e.failedProxies ]
                proxyError = proxyErrors[-1]
                if len(proxyErrors) > 1:
                    errMsg = " %d proxies failed. Last error:" % (
                        len(proxyErrors), )
                else:
                    errMsg = ""
                errMsg = "Proxy error:%s %s" % (errMsg,
                    self._formatProxyError(proxyError))
                raise TransportError, errMsg, ei[2]
            return (self.connSpec, proxy)

        def markFailedProxy(self, proxy, error=None):
            if error:
                errorMsg = " (%s)" % str(error)
            else:
                errorMsg = ""
            log.info("Marking proxy %r as failed%s" % (
                proxy.asString(withAuth=True), errorMsg))
            self.proxyMap.blacklistUrl(proxy, error=error)

        def _formatProxyError(self, proxyError):
            tmpl = "%s (via %s proxy %r)"
            if proxyError.exception:
                errMsg = str(proxyError.exception)
            else:
                errMsg = "(unknown)"
            proxyType =  proxyError.host.requestProtocol
            ppType = self.ProxyTypeName.get(proxyType, proxyType)
            return tmpl % (errMsg, ppType,
                proxyError.host.asString(withAuth=True))


    class Connection(object):
        __slots__ = [ 'url', 'selector', 'connection', 'headers' ]
        def __init__(self, url, selector, connection, headers):
            self.connection = connection
            self.headers = headers
            self.url = url
            self.selector = selector

    def __init__(self, proxyMap, caCerts=None, retries=None,
            proxyRetries=None, forceProxy=False, localhosts=None,
            userAgent=None):
        self.proxyMap = proxyMap
        self.caCerts = caCerts
        self.retries = retries or 3
        self.proxyRetries = proxyRetries or max(self.retries, 7)
        self.forceProxy = forceProxy
        self.localhosts = localhosts or set()
        self.proxyUsed = None
        self.userAgent = userAgent

    def getConnectionIterator(self, url, ssl = None):
        connSpec = self.newUrl(url, ssl=ssl)
        protocols = self.ProtocolMaps[connSpec.protocol]
        connIter = self._ConnectionIterator(connSpec, self.proxyMap,
            protocols, self.retries, self.proxyRetries)
        return connIter

    @classmethod
    def newUrl(cls, url, defaultPort = None, ssl = None):
        if isinstance(url, cls.URL):
            return url
        protocol = (ssl and 'https') or 'http'
        cspec = cls.URL(url, defaultPort=defaultPort)
        cspec.protocol = protocol
        return cspec

    @classmethod
    def _splitport(self, hostport, defaultPort, getIP=True):
        if isinstance(hostport, tuple):
            host, port = hostport
        else:
            host, port = urllib.splitport(hostport)
        if port is None:
            port = defaultPort
        if getIP:
            return (IPCache.get(host), int(port))
        return (host, int(port))

    def proxy_ssl(self, endpoint, proxy):
        proxyHost, proxyPort = self._splitport((proxy.host, proxy.port),
            defaultPort = 3128)
        endpointHost, endpointPort = self._splitport(
            (endpoint.host, endpoint.port),
            defaultPort = httplib.HTTPS_PORT, getIP=False)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((proxyHost, proxyPort))
        except socket.error, e:
            raise ProxyError(e, host=proxy)

        sock.sendall("CONNECT %s:%s HTTP/1.0\r\n" %
                                         (endpointHost, endpointPort))
        sock.sendall("User-Agent: %s\r\n" % self.userAgent)
        headers = []
        self._addAuthHeader(headers, 'Proxy-Authorization', proxy.userpass)
        for k, v in headers:
            sock.sendall("%s: %s\r\n" % (k, v))
        sock.sendall('\r\n')

        # Have HTTPResponse parse the status line for us
        resp = httplib.HTTPResponse(sock, strict=True)
        try:
            resp.begin()
        except httplib.BadStatusLine:
            raise ProxyError(socket.error(-42, "Bad Status Line"), host=proxy)

        if resp.status != 200:
            # Fake a socket error, use a code that make it obvious it hasn't
            # been generated by the socket library
            raise socket.error(-71,
                               "Error talking to HTTP proxy %s:%s: %s (%s)" %
                               (proxy.host, proxy.port,
                                resp.status, resp.reason))

        # We can safely close the response, it duped the original socket
        resp.close()

        # Wrap the socket in an SSL socket
        if SSL and self.caCerts:
            # Doing server cert checking; use m2crypto
            h = HTTPSConnection(endpointHost, endpointPort,
                    caCerts=self.caCerts, commonName=endpointHost)
            try:
                h.adopt(sock)
            except SSLVerificationError, e:
                raise ProxyError(e, host=proxy)
        else:
            # No cert checking or no m2crypto
            h = httplib.HTTPConnection(endpointHost, endpointPort)
            # This is a bit unclean
            try:
                h.sock = self._wrapSsl(sock)
            except socket.sslerror, e:
                raise ProxyError(e, host=proxy)

        # Force HTTP/1.0 (this is the default for the old-style HTTP;
        # new-style HTTPConnection defaults to 1.1)
        h._http_vsn = 10
        h._http_vsn_str = 'HTTP/1.0'
        return h

    def _wrapSsl(self, sock):
        # python 2.6 deprecates socket.ssl in favor of ssl.SSLSocket
        if sys.version_info[:2] == (2, 6):
            import ssl
            return ssl.SSLSocket(sock)
        # Old-style Python
        sslSock = socket.ssl(sock, None, None)
        return httplib.FakeSocket(sock, sslSock)

    def _getConnectionType(self, endpoint, proxy):
        if proxy is None:
            return self._getConnectionTypeEndpoint(endpoint)

        return self._getConnectionTypeEndpointProxy(endpoint, proxy)

    def _getConnectionTypeEndpoint(self, endpoint):
        if endpoint.protocol == 'https':
            return self.CONN_SSL, endpoint.selector
        return self.CONN_PLAIN, endpoint.selector

    def _getConnectionTypeEndpointProxy(self, endpoint, proxy):
        connType, selector = self._getConnectionTypeEndpoint(endpoint)
        if self.proxyBypass(endpoint, proxy):
            return connType, selector
        connType |= self.CONN_PROXY
        # If the endpoint is SSL, then we need to tunnel.
        selector = endpoint.url
        if connType & self.CONN_SSL:
            connType |= self.CONN_TUNNEL
            selector = endpoint.selector
        return connType, selector

    @classmethod
    def _addAuthHeader(cls, headers, headerName, userpass):
        if not userpass:
            return
        headers.append(
            (headerName, util.ProtectedTemplate('Basic ${auth}',
                auth = util.ProtectedString(base64.b64encode(str(userpass))))))

    def getIPAddress(self, connSpec):
        host = IPCache.get(connSpec.host)
        if connSpec.port is not None:
            return "%s:%s" % (host, connSpec.port)
        return host

    def openConnection(self, connSpec):
        endpoint, proxy = connSpec

        headers = [ ('Host', endpoint.hostport) ]
        self._addAuthHeader(headers, 'Authorization', endpoint.userpass)

        self.proxyUsed = None

        connType, selector = self._getConnectionType(endpoint, proxy)
        if connType & self.CONN_TUNNEL:
            hndl = self.proxy_ssl(endpoint, proxy)
            self.proxyUsed = proxy
        else:
            if connType & self.CONN_PROXY:
                nexthop = proxy
                self.proxyUsed = proxy
            else:
                nexthop = endpoint
            nexthopHP = self.getIPAddress(nexthop)

            if connType & self.CONN_SSL:
                if self.caCerts and not SSL:
                    # There are two places to do cert checking but we only want to
                    # warn once, so check for this early.
                    warnings.warn('m2crypto not installed; server certificates '
                            'will not be validated')

                if self.caCerts and SSL:
                    # If cert checking is requested use our HTTPSConnection (which
                    # uses m2crypto)
                    commonName = nexthop.host
                    hndl = HTTPSConnection(nexthopHP, caCerts=self.caCerts,
                                           commonName=commonName)
                else:
                    # Either no cert checking was requested, or we don't have the
                    # module to support it, so use vanilla httplib.
                    hndl = httplib.HTTPSConnection(nexthopHP)
            else:
                # Target: proxy or origin server
                hndl = httplib.HTTPConnection(nexthopHP)
                if (connType & self.CONN_PROXY):
                    self._addAuthHeader(headers, 'Proxy-Authorization',
                        proxy.userpass)

        # Force HTTP/1.0 (this is the default for the old-style HTTP;
        # new-style HTTPConnection defaults to 1.1)
        hndl._http_vsn = 10
        hndl._http_vsn_str = 'HTTP/1.0'

        return self.Connection(endpoint, selector, hndl, headers)

    def proxyBypass(self, endpoint, proxy):
        if self.forceProxy:
            return False

        proxyHost = proxy.host
        destHost = endpoint.host

        # don't proxy localhost unless the proxy is running on
        # localhost as well
        if destHost in self.localhosts and proxyHost not in self.localhosts:
            return True
        return self.proxyBypassEnv(endpoint, proxy)

    def proxyBypassEnv(self, endpoint, proxy):
        # filter based on the no_proxy env var
        npFilt = util.noproxyFilter()
        return npFilt.bypassProxy(endpoint.host)

class HttpData(object):
    __slots__ = [ 'data', 'method', 'size', 'headers', 'compress',
                  'contentType', 'callback', 'chunked', 'bufferSize',
                  'rateLimit', ]
    BUFFER_SIZE = 8192
    def __init__(self, data=None, method=None, size=None, headers=None,
                 contentType=None, compress=False, callback=None, chunked=None,
                 bufferSize=None, rateLimit=None):
        if headers is None:
            headers = []
        if method is None:
            # Default to POST if data is present
            if data:
                method = 'POST'
                headers.append(('Accept-encoding', 'deflate'))
            else:
                method = 'GET'
        self.method = method
        if data is not None:
            if hasattr(data, 'read'):
                if chunked:
                    headers.append(('Transfer-Encoding', 'Chunked'))
            else:
                data = data.encode('ascii')
                if compress:
                    data = zlib.compress(data, 9)
                    headers.append(('Content-Encoding', 'deflate'))
                size = len(data)
        self.data = data
        self.headers = headers
        self.size = size
        self.compress = compress
        self.contentType = contentType
        self.callback = callback
        self.chunked = chunked
        self.bufferSize = bufferSize or self.BUFFER_SIZE
        self.rateLimit = rateLimit

    def iterheaders(self):
        for k, v in self.headers:
            yield k, str(v)
        # Don't send a Content-Length header if chunking
        if not self.chunked and self.size is not None:
            yield 'Content-Length', str(self.size)
        if self.contentType is not None:
            yield 'Content-Type', self.contentType

    def writeTo(self, connection):
        if self.data is None:
            return
        if not hasattr(self.data, 'read'):
            connection.send(self.data)
            return
        if not self.chunked:
            util.copyfileobj(self.data, connection, bufSize=self.bufferSize,
                callback=self.callback, rateLimit=self.rateLimit,
                sizeLimit=self.size)
            return
        assert self.size is not None
        # keep track of the total amount of data sent so that the
        # callback passed in to copyfileobj can report progress correctly
        total = 0
        size = self.size
        c = connection
        while size:
            # send in 256k chunks
            chunk = 262144
            if chunk > size:
                chunk = size
            # first send the hex-encoded size
            c.send('%x\r\n' %chunk)
            # then the chunk of data
            util.copyfileobj(self.data, c, bufSize=chunk,
                             callback=self.callback,
                             rateLimit=self.rateLimit, sizeLimit=chunk,
                             total=total)
            # send \r\n after the chunked data
            c.send("\r\n")
            total =+ chunk
            size -= chunk
        # terminate the chunked encoding
        c.send('0\r\n\r\n')

class URLOpener(urllib.FancyURLopener):
    '''Replacement class for urllib.FancyURLopener'''
    contentType = 'application/x-www-form-urlencoded'

    localhosts = LocalHosts
    ConnectionManager = ConnectionManager
    ProxyMap = util.ProxyMap

    # For debugging purposes only
    _sendConaryProxyHostHeader = True

    user_agent = 'conary-http-client/0.1'

    def __init__(self, *args, **kw):
        self.compress = False
        self.abortCheck = None
        self.proxyHost = None
        # FIXME: this should go away in a future release.
        # forceProxy is used to ensure that if the proxy returns some
        # bogus address like "localhost" from a URL fetch, we can
        # be sure to use the proxy the next time we speak to the proxy
        # too.
        self.forceProxy = kw.pop('forceProxy', False)
        # proxies will be a more complicated object in the future, so don't
        # rely on FancyURLOpener's proxy handing
        proxyMap = kw.pop('proxyMap', None)
        proxies = kw.pop('proxies', None)
        if args:
            proxies = args[0]
            args = ()
        if proxyMap is None:
            proxyMap = self.newProxyMapFromDict(proxies)
        self.connmgr = self.ConnectionManager(proxyMap,
            retries=kw.pop('retries', None),
            proxyRetries=kw.pop('proxyRetries', None),
            caCerts=kw.pop('caCerts', None),
            localhosts = self.localhosts,
            userAgent = self.user_agent)
        # Make sure urllib won't try to interpret the proxies from the
        # environment
        kw['proxies'] = {}
        urllib.FancyURLopener.__init__(self, *args, **kw)

    @property
    def usedProxy(self):
        return self.proxyHost is not None

    @classmethod
    def newProxyMapFromDict(cls, proxies):
        proxyMap = cls.ProxyMap.fromDict(proxies, readEnvironment=True)
        return proxyMap

    def setCompress(self, compress):
        self.compress = compress

    def setAbortCheck(self, check):
        self.abortCheck = check

    def open_https(self, url, data=None):
        return self.open_http(url, data=data, ssl=True)

    def open_http(self, url, data=None, ssl=False):
        """override this WHOLE FUNCTION to change
           one magic string -- the content type --
           which is hardcoded in (this version also supports https)"""
        # Splitting some of the functionality so we can reuse this code with
        # PUT requests too

        # Retry the connection if the remote end closes the connection
        # without sending a response. This may happen after shutting
        # down the SSL stream (BadStatusLine), or without doing so
        # (socket.sslerror)
        resetResolv = False
        if isinstance(url, tuple):
            connection = url[1] + ' using ' + url[0] + ' as a proxy'
        else:
            connection = url
        connectFailed = False

        if not isinstance(data, HttpData):
            data = HttpData(data, contentType=self.contentType,
                compress=self.compress)
        elif data.contentType is None:
            data.contentType = self.contentType

        connIterator = self.connmgr.getConnectionIterator(url, ssl=ssl)
        lastProxyError = None
        for connSpec in connIterator:
            try:
                conn = self.connmgr.openConnection(connSpec)
                # If a proxy was used, save it here
                self.proxyHost = self.connmgr.proxyUsed

                h = conn.connection
                h.putrequest(data.method, conn.selector)
                for args in itertools.chain(data.iterheaders(),
                                            conn.headers, self.addheaders):
                    h.putheader(*args)
                try:
                    h.endheaders()
                except socket.error, e:
                    if e.args[0] == errno.ECONNREFUSED and connSpec[1]:
                        raise ProxyError(e, host=connSpec[1])
                    raise
                data.writeTo(h)
                # wait for a response
                self._wait(h)
                response = h.getresponse()
                errcode, errmsg = response.status, response.reason
                headers = response.msg
                fp = response.fp
                break
            except ProxyError, e:
                lastProxyError = e
                connIterator.markFailedProxy(connSpec[1], e)
                continue
            except (socket.sslerror, socket.gaierror), e:
                if e.args[0] == 'socket error':
                    e = e.args[1]
                self._processSocketError(e)
                if isinstance(e, socket.gaierror):
                    if e.args[0] == socket.EAI_AGAIN:
                        pass
                    else:
                        connectFailed = True
                        break
                elif isinstance(e, socket.sslerror):
                    pass
                else:
                    connectFailed = True
                    break
            except httplib.BadStatusLine, e:
                # closed connection without sending a response.
                pass
            except socket.error, e:
                self._processSocketError(e)
                raise e
            # try resetting the resolver - /etc/resolv.conf
            # might have changed since this process started.
            if not resetResolv:
                util.res_init()
                resetResolv = True
        else:
            # we've run out of tries and so we've failed
            connectFailed = True
        if connectFailed:
            if lastProxyError:
                return self.handleProxyErrors(lastProxyError)
            log.info("Failed to connect to %s. Aborting after "
                      "%i of %i tries." % (connection,
                      connIterator.retryCount, connIterator.retries))
            raise e
        elif connIterator.retryCount > 1:
            log.info("Successfully connected to %s after "
                      "%i of %i tries." % (connection,
                      connIterator.retryCount, connIterator.retries))

        if errcode == 200:
            encoding = headers.get('Content-encoding', None)
            if encoding == 'deflate':
                # disable until performace is better
                #fp = DecompressFileObj(fp)
                fp = util.decompressStream(fp)
                fp.seek(0)

            protocolVersion = "HTTP/%.1f" % (response.version / 10.0)
            return InfoURL(fp, headers, conn.url, protocolVersion,
                code=errcode, msg=errmsg)
        else:
            self.handleProxyErrors(errcode)
            urlString = conn.url.asString(withAuth=True)
            return self.http_error(urlString, fp, errcode, errmsg, headers, data)

    def handleProxyErrors(self, errcode):
        e = None
        if isinstance(errcode, ProxyError):
            self.proxyHost = errcode.host
            e = errcode.exception
        elif errcode == 503:
            # Service unavailable, make it a socket error
            e = socket.error(111, "Service unavailable")
        elif errcode == 502:
            # Bad gateway (server responded with some broken answer)
            e = socket.error(111, "Bad Gateway (error reported by proxy)")
        if e:
            self._processSocketError(e)
            raise e

    def _processSocketError(self, error):
        if not self.proxyHost:
            return
        msgTempl =  "%s (via %s proxy %s)"
        proxyType = self.proxyHost.requestProtocol
        ppType = self.connmgr._ConnectionIterator.ProxyTypeName.get(proxyType, proxyType)
        msgError = msgTempl % (error[1], ppType, self.proxyHost.hostport)
        error.args = (error[0], msgError)
        if hasattr(error, 'strerror'):
            error.strerror = msgError

    def _wait(self, h):
        # wait for data if abortCheck is set
        if self.abortCheck:
            check = self.abortCheck
        else:
            check = lambda: False

        pollObj = select.poll()
        pollObj.register(h.sock.fileno(), select.POLLIN)

        lastTimeout = time.time()
        while True:
            if check():
                raise AbortError
            # wait 5 seconds for a response
            try:
                l = pollObj.poll(5000)
            except select.error, err:
                if err.args[0] == errno.EINTR:
                    # Interrupted system call -- we caught a signal but
                    # it was handled safely.
                    continue
                raise

            if not l:
                # still no response from the server.  send a space to
                # keep the connection alive - in case the server is
                # behind a load balancer/firewall with short
                # connection timeouts.
                now = time.time()
                if now - lastTimeout > 14.9:
                    h.send(' ')
                    lastTimeout = now
            else:
                # ready to read response
                break

    def http_error_default(self, url, fp, errcode, errmsg, headers, data=None):
        raise TransportError("Unable to open %s: %s" % (url, errmsg))

class IPCache(object):
    """
    A global IP cache
    """
    _cache = util.TimestampedMap(delta = 600)

    @classmethod
    def get(cls, host, resetResolver = False, stale = False):
        if host in LocalHosts:
            return host
        # Fetch fresh results only first
        ret = cls._cache.get(host, None, stale = False)
        if ret is not None:
            return ret
        try:
            ret = socket.gethostbyname(host)
            cls._cache.set(host, ret)
            return ret
        except (IOError, socket.error):
            if not resetResolver and not stale:
                raise
            if stale:
                ret = cls._cache.get(host, None, stale = True)
                if ret is not None:
                    return ret
            # Recursively call ourselves
            util.res_init()
            return cls.get(host, resetResolver = False, stale = False)

    @classmethod
    def clear(cls):
        cls._cache.clear()

class BackoffTimer(object):
    """Helper for functions that need an exponential backoff."""

    factor = 2.7182818284590451
    jitter = 0.11962656472

    def __init__(self, delay=0.1):
        self.delay = delay

    def sleep(self):
        time.sleep(self.delay)
        self.delay *= self.factor
        self.delay = random.normalvariate(self.delay, self.delay * self.jitter)

class InfoURL(urllib.addinfourl):
    def __init__(self, fp, headers, url, protocolVersion, code=None, msg=None):
        urllib.addinfourl.__init__(self, fp, headers, url)
        self.protocolVersion = protocolVersion
        self.code = code
        self.msg = msg

class AbortError(Exception): pass

class TransportError(Exception): pass

class ProxyError(Exception):
    def __init__(self, exception, *args, **kwargs):
        self.exception = exception
        self.host = kwargs.pop('host', None)
        Exception.__init__(self, *args, **kwargs)
