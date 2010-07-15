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

""" XMLRPC transport class that uses urllib to allow for proxies
    Unfortunately, urllib needs some touching up to allow
    XMLRPC commands to be sent, hence the XMLOpener class """

import base64
import xmlrpclib

from conary.lib import util, httputils

# For compatibility
AbortError = httputils.AbortError
URLOpener = httputils.URLOpener
TransportError = httputils.TransportError

class ConaryURLOpener(URLOpener):
    """An opener aware of the conary proxies"""

    class ConnectionManager(URLOpener.ConnectionManager):
        ConaryProxyProtocols = set([ 'conary', 'conarys' ])
        ProtocolMaps = dict(http = [ 'conary', 'http' ],
            https = [ 'conarys', 'https' ])

        # For debugging purposes only
        _sendConaryProxyHostHeader = True
        def proxyBypassEnv(self, endpoint, proxy):
            if proxy.requestProtocol in self.ConaryProxyProtocols:
                return False
            return URLOpener.ConnectionManager.proxyBypassEnv(self, endpoint, proxy)

        def openConnection(self, connSpec):
            conn = URLOpener.ConnectionManager.openConnection(self, connSpec)
            endpoint, proxy = connSpec
            if (proxy is not None and
                    proxy.requestProtocol in self.ConaryProxyProtocols and
                    self._sendConaryProxyHostHeader):
                # Add a custom header to tell the proxy which name
                # we contacted it on
                conn.headers.append(('X-Conary-Proxy-Host', proxy.hostport))
            return conn

        def _getConnectionTypeEndpointProxy(self, endpoint, proxy):
            m =  URLOpener.ConnectionManager._getConnectionTypeEndpointProxy
            connType, selector = m(self, endpoint, proxy)
            if proxy.requestProtocol in self.ConaryProxyProtocols:
                if connType & self.CONN_TUNNEL:
                    # Conary proxies don't implement tunneling, so clear the bit
                    connType = connType ^ self.CONN_TUNNEL
                if connType & self.CONN_SSL and proxy.requestProtocol == 'conary':
                    # Endpoint is SSL, but we're mapping to a plain Conary
                    # proxy
                    connType = connType ^ self.CONN_SSL
                if connType & self.CONN_PROXY:
                    selector = endpoint.url
            return connType, selector

        class _ConnectionIterator(URLOpener.ConnectionManager._ConnectionIterator):
            ProxyTypeName = URLOpener.ConnectionManager._ConnectionIterator.ProxyTypeName.copy()
            ProxyTypeName.update(dict(conary = 'Conary', conarys = 'Conary'))


class XMLOpener(ConaryURLOpener):
    contentType = 'text/xml'

    def open_http(self, *args, **kwargs):
        fp = ConaryURLOpener.open_http(self, *args, **kwargs)
        usedAnonymous = 'X-Conary-UsedAnonymous' in fp.headers
        return usedAnonymous, fp

    def http_error(self, url, fp, errcode, errmsg, headers, data=None):
        raise xmlrpclib.ProtocolError(url, errcode, errmsg, headers)

class Transport(xmlrpclib.Transport):

    # override?
    user_agent = "xmlrpclib.py/%s (www.pythonware.com modified by " \
        "rPath, Inc.)" % xmlrpclib.__version__
    # make this a class variable so that across all attempts to transport
    # we'll only
    # spew messages once per host.
    failedHosts = set()
    UrlOpenerFactory = XMLOpener

    def __init__(self, https=False, proxies=None, proxyMap=None,
                 serverName=None, extraHeaders=None, caCerts=None):
        self.https = https
        self.compress = False
        self.abortCheck = None
        if proxyMap is None:
            proxyMap = self.UrlOpenerFactory.newProxyMapFromDict(proxies)
        self.proxyMap = proxyMap
        self.serverName = serverName
        self.setExtraHeaders(extraHeaders)
        self.caCerts = caCerts
        self.responseHeaders = None
        self.responseProtocol = None
        self.usedProxy = False
        self.entitlement = None
        self._proxyHost = None # Can be a URL object
        self.proxyHost = None
        self.proxyProtocol = None

    def setEntitlements(self, entitlementList):
        self.entitlements = entitlementList
        if entitlementList is not None:
            l = []
            for entitlement in entitlementList:
                if entitlement[0] is None:
                    l.append("* %s" % (base64.b64encode(entitlement[1])))
                else:
                    l.append("%s %s" % (entitlement[0],
                                        base64.b64encode(entitlement[1])))
            self.entitlement = " ".join(l)
        else:
            self.entitlement = None

    def getEntitlements(self):
        return self.entitlements

    def setExtraHeaders(self, extraHeaders):
        self.extraHeaders = extraHeaders or {}

    def addExtraHeaders(self, extraHeaders):
        self.extraHeaders.update(extraHeaders)

    def setCompress(self, compress):
        self.compress = compress

    def setAbortCheck(self, abortCheck):
        self.abortCheck = abortCheck

    def _protocol(self):
        if self.https:
            return 'https'
        return 'http'

    def request(self, host, handler, body, verbose=0):
        self.verbose = verbose

        protocol = self._protocol()

        opener = self.UrlOpenerFactory(proxyMap=self.proxyMap,
            caCerts=self.caCerts)
        opener.setCompress(self.compress)
        opener.setAbortCheck(self.abortCheck)

        opener.addheaders = []
        host, extra_headers, x509 = self.get_host_info(host)
        if extra_headers:
            if isinstance(extra_headers, dict):
                extra_headers = extra_headers.items()
            for key, value in extra_headers:
                opener.addheader(key, value)

        if self.entitlement:
            opener.addheader('X-Conary-Entitlement', self.entitlement)

        if self.serverName:
            opener.addheader('X-Conary-Servername', self.serverName)

        opener.addheader('User-agent', self.user_agent)
        for k, v in self.extraHeaders.items():
            opener.addheader(k, v)

        url = ''.join([protocol, '://', host, handler])
        # Make sure we capture some useful information from the
        # opener, even if we failed
        try:
            usedAnonymous, response = opener.open(url, body)
        finally:
            self.usedProxy = getattr(opener, 'usedProxy', False)
            self._proxyHost = getattr(opener, 'proxyHost', None)
            if self._proxyHost:
                self.proxyHost = self._proxyHost.hostport
                self.proxyProtocol = self._proxyHost.requestProtocol
        if hasattr(response, 'headers'):
            self.responseHeaders = response.headers
            self.responseProtocol = response.protocolVersion
        resp = self.parse_response(response)
        rc = ([usedAnonymous] + resp[0], )
        return rc

    def getparser(self):
        return util.xmlrpcGetParser()
