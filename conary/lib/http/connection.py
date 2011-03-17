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
import errno
import glob
import httplib
import os
import select
import socket
import time
import warnings

from conary import constants
from conary.lib import util
from conary.lib.http import http_error


try:
    from M2Crypto import SSL
    SSLVerificationError = SSL.Checker.SSLVerificationError
except ImportError:
    SSL = None
    class SSLVerificationError(Exception):
        pass


class Connection(object):
    """Connection to a single endpoint, possibly encrypted and/or proxied
    and/or tunneled.

    May be kept alive betwen requests and reopened if a kept-alive connection
    fails on subsequent use. Will not attempt to retry on other network errors,
    nor will it interpret HTTP responses.
    """

    userAgent = "conary-http-client/%s" % constants.version
    connectTimeout = 15

    def __init__(self, endpoint, proxy=None, caCerts=None, commonName=None):
        """
        @param endpoint: Destination URL (host, port, optional SSL, optional
            authorization)
        @param proxy: Optional proxy URL (host, port, optional SSL, optional
            authorization)
        @param caCerts: Optional list of CA certificate paths to check servers
            against.
        @param commonName: Optional hostname to use for checking server
            certificates.
        """
        # endpoint and proxy must be URL objects, not names.
        self.endpoint = endpoint
        self.proxy = proxy
        self.caCerts = caCerts
        if proxy:
            self.local = proxy
        else:
            self.local = endpoint
        if commonName is None:
            commonName = str(self.endpoint.hostport.host)
        self.commonName = commonName
        self.doSSL = endpoint.scheme == 'https'
        self.doTunnel = bool(proxy) and self.doSSL
        # Cached HTTPConnection object
        self.cached = None

    def close(self):
        if self.cached:
            self.cached.close()
            self.cached = None

    def request(self, req):
        if self.cached:
            # Try once to use the cached connection; if it fails to send the
            # request then discard and try again.
            try:
                return self.requestOnce(self.cached, req)
            except http_error.RequestError, err:
                err.wrapped.clear()
                self.cached.close()
                self.cached = None
        # If a problem occurs before or during the sending of the request, then
        # throw a wrapper exception so that the caller knows it is safe to
        # retry. Once the request is sent retries must be done more carefully
        # as side effects may have occurred.
        try:
            conn = self.openConnection()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            wrapped = util.SavedException()
            raise http_error.RequestError(wrapped)
        # Note that requestOnce may also throw RequestError, see above.
        ret = self.requestOnce(conn, req)
        if not ret.will_close:
            self.cached = conn
        return ret

    def openConnection(self):
        sock = self.connectSocket()
        sock = self.startTunnel(sock)
        sock = self.startSSL(sock)

        host, port = self.endpoint.hostport
        conn = httplib.HTTPConnection(host, port, strict=True)
        conn.sock = sock
        conn.auto_open = False
        return conn

    def connectSocket(self):
        """Open a connection to the proxy, or endpoint if no proxy."""
        host, port = self.local.hostport
        if hasattr(host, 'resolve'):
            host = host.resolve()[0]
        sock = socket.socket(host.family, socket.SOCK_STREAM)
        oldTimeout = sock.gettimeout()
        sock.settimeout(self.connectTimeout)
        sock.connect((str(host), port))
        sock.settimeout(oldTimeout)
        return sock

    def startTunnel(self, sock):
        """If needed, start a HTTP CONNECT tunnel on the proxy connection."""
        if not self.doTunnel:
            return sock

        # Send request
        lines = [
                "CONNECT %s HTTP/1.0" % (self.endpoint.hostport,),
                "User-Agent: %s" % (self.userAgent,),
                ]
        if self.proxy.userpass[0]:
            lines.append("Proxy-Authorization: Basic " +
                    base64.b64encode(":".join(self.proxy.userpass)))
        lines.extend(['', ''])
        sock.sendall('\r\n'.join(lines))

        # Parse response to make sure the tunnel was opened successfully.
        resp = httplib.HTTPResponse(sock, strict=True)
        try:
            resp.begin()
        except httplib.BadStatusLine:
            raise socket.error(-42, "Bad Status Line from proxy %s" %
                    (self.proxy,))
        if resp.status != 200:
            raise socket.error(-71,
                    "HTTP response error from HTTP proxy %s: %s %s" %
                    (self.proxy, resp.status, resp.reason))

        # We can safely close the response, it duped the original socket
        resp.close()
        return sock

    def startSSL(self, sock):
        """If needed, start SSL on the proxy or endpoint connection."""
        if not self.doSSL:
            return sock
        if self.caCerts:
            # If cert checking is requested use m2crypto
            if SSL:
                return startSSLWithChecker(sock, self.caCerts, self.commonName)
            else:
                warnings.warn("m2crypto is not installed; server certificates "
                        "will not be validated!")
        try:
            # Python >= 2.6
            import ssl
            return ssl.SSLSocket(sock)
        except ImportError:
            # Python < 2.6
            sslSock = socket.ssl(sock, None, None)
            return httplib.FakeSocket(sock, sslSock)

    def requestOnce(self, conn, req):
        if self.proxy and self.proxy.userpass[0] and not self.doTunnel:
            req.headers['Proxy-Authorization'] = ('Basic ' +
                    base64.b64encode(":".join(self.proxy.userpass)))
        try:
            req.sendRequest(conn, isProxied=(self.proxy is not None))
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            wrapped = util.SavedException()
            raise http_error.RequestError(wrapped)

        # Wait for a response.
        poller = select.poll()
        poller.register(conn.sock.fileno(), select.POLLIN)
        lastTimeout = time.time()
        while True:
            if req.abortCheck():
                raise http_error.AbortError()

            # Wait 5 seconds for a response.
            try:
                active = poller.poll(5000)
            except select.error, err:
                if err.args[0] == errno.EINTR:
                    # Interrupted system call -- we caught a signal but it was
                    # handled safely.
                    continue
                raise
            if active:
                break

            # Still no response from the server. Send blank lines to keep the
            # connection alive, in case the server is behind a load balancer or
            # firewall with short connection timeouts.
            now = time.time()
            if now - lastTimeout >= 15:
                conn.send('\r\n')
                lastTimeout = now

        return conn.getresponse()


def startSSLWithChecker(sock, caCerts, commonName):
    """Start SSL on the given socket and do server certificate validation.

    Returns the new M2Crypto SSL Connection object.
    """
    ssl_ctx = SSL.Context('sslv23')
    ssl_ctx.set_verify(SSL.verify_peer, depth=9)
    paths = []
    for path in caCerts:
        paths.extend(sorted(list(glob.glob(path))))
    for path in paths:
        if os.path.isdir(path):
            ssl_ctx.load_verify_locations(capath=path)
        elif os.path.exists(path):
            ssl_ctx.load_verify_locations(cafile=path)
    sslSock = SSL.Connection(ssl_ctx, sock)
    sslSock.setup_ssl()
    sslSock.set_connect_state()
    sslSock.connect_ssl()
    checker = SSL.Checker.Checker()
    if not checker(sslSock.get_peer_cert(), commonName):
        raise SSLVerificationError("post connection check failed")
    return sslSock
