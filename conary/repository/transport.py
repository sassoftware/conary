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

""" XMLRPC transport class that uses urllib to allow for proxies
    Unfortunately, urllib needs some touching up to allow
    XMLRPC commands to be sent, hence the XMLOpener class """

import base64
import xmlrpclib

from conary.lib import util
from conary.lib.http import connection
from conary.lib.http import http_error
from conary.lib.http import opener
from conary.lib.http import proxy_map

# For compatibility
AbortError = http_error.AbortError
TransportError = http_error.TransportError
URLOpener = opener.URLOpener


class ConaryConnector(connection.Connection):

    def __init__(self, endpoint, proxy=None, caCerts=None, commonName=None):
        connection.Connection.__init__(self, endpoint, proxy, caCerts,
                commonName)
        # Always talk to conary proxies using the protocol from the proxy URL.
        # In other words, a SSL connection through a non-SSL conary proxy
        # should be unencrypted.
        if proxy:
            if proxy.scheme == 'conarys':
                self.commonName = str(proxy.hostport.host)
                self.doTunnel = False
                self.doSSL = True
            elif proxy.scheme == 'conary':
                self.doTunnel = False
                self.doSSL = False


class ConaryURLOpener(opener.URLOpener):
    proxyFilter = ('http', 'https', 'conary', 'conarys')
    connectionFactory = ConaryConnector

    def __init__(self, proxyMap=None, caCerts=None, proxies=None):
        if not proxyMap:
            if proxies:
                proxyMap = proxy_map.ProxyMap.fromDict(proxies)
            else:
                proxyMap = proxy_map.ProxyMap.fromEnvironment()
        opener.URLOpener.__init__(self, proxyMap=proxyMap, caCerts=caCerts)

    def _requestOnce(self, req, proxy):
        if proxy and proxy.scheme in ('conary', 'conarys'):
            # Add a custom header to tell the proxy which name
            # we contacted it on
            req.headers['X-Conary-Proxy-Host'] = str(proxy.hostport)
        return opener.URLOpener._requestOnce(self, req, proxy)


class XMLOpener(ConaryURLOpener):
    contentType = 'text/xml'


class Transport(xmlrpclib.Transport):

    # override?
    user_agent = "xmlrpclib.py/%s (www.pythonware.com modified by " \
        "rPath, Inc.)" % xmlrpclib.__version__

    openerFactory = XMLOpener

    def __init__(self, https=False, proxies=None, proxyMap=None,
                 serverName=None, extraHeaders=None, caCerts=None):
        self.https = https
        self.compress = False
        self.abortCheck = None
        if not proxyMap:
            if proxies:
                proxyMap = proxy_map.ProxyMap.fromDict(proxies)
            else:
                proxyMap = proxy_map.ProxyMap.fromEnvironment()
        self.proxyMap = proxyMap
        self.serverName = serverName
        self.setExtraHeaders(extraHeaders)
        self.caCerts = caCerts
        self.responseHeaders = None
        self.responseProtocol = None
        self.usedProxy = False
        self.entitlement = None
        self._proxyHost = None  # Can be a URL object
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

        opener = self.openerFactory(proxyMap=self.proxyMap,
                caCerts=self.caCerts)
        url = ''.join([protocol, '://', host, handler])
        host, extra_headers, x509 = self.get_host_info(host)

        req = opener.newRequest(url, method='POST', headers=extra_headers)
        req.setAbortCheck(self.abortCheck)
        req.setData(body, compress=self.compress)
        if self.entitlement:
            req.headers['X-Conary-Entitlement'] = self.entitlement
        if self.serverName:
            req.headers['X-Conary-Servername'] = self.serverName
        req.headers['User-agent'] = self.user_agent

        # Make sure we capture some useful information from the
        # opener, even if we failed
        try:
            response = opener.open(req)
        finally:
            self.usedProxy = opener.lastProxy is not None
            self._proxyHost = opener.lastProxy
            if self._proxyHost:
                self.proxyHost = self._proxyHost.hostport
                self.proxyProtocol = self._proxyHost.scheme
        usedAnonymous = 'X-Conary-UsedAnonymous' in response.headers
        self.responseHeaders = response.headers
        self.responseProtocol = response.protocolVersion
        resp = self.parse_response(response)
        rc = ([usedAnonymous] + resp[0], )
        return rc

    def getparser(self):
        return util.xmlrpcGetParser()
