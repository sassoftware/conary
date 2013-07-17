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


""" XMLRPC transport class that uses urllib to allow for proxies
    Unfortunately, urllib needs some touching up to allow
    XMLRPC commands to be sent, hence the XMLOpener class """

import base64
import socket
import sys
import xmlrpclib

from conary import constants
from conary.lib import timeutil
from conary.lib import util
from conary.lib.http import connection
from conary.lib.http import http_error
from conary.lib.http import opener
from conary.lib.http import proxy_map
from conary.repository import errors

# For compatibility
AbortError = http_error.AbortError
BackoffTimer = timeutil.BackoffTimer
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

    def __init__(self, proxyMap=None, caCerts=None, proxies=None,
            persist=False, connectAttempts=None):
        if not proxyMap:
            if proxies:
                proxyMap = proxy_map.ProxyMap.fromDict(proxies)
            else:
                proxyMap = proxy_map.ProxyMap.fromEnvironment()
        opener.URLOpener.__init__(self, proxyMap=proxyMap, caCerts=caCerts,
                persist=persist, connectAttempts=connectAttempts)

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
    user_agent = "Conary/%s" % constants.version

    openerFactory = XMLOpener

    def __init__(self, proxyMap=None, serverName=None, caCerts=None,
            connectAttempts=None):
        self.compress = False
        self.abortCheck = None
        self.proxyMap = proxyMap
        self.extraHeaders = {}
        self.serverName = serverName
        self.caCerts = caCerts
        self.responseHeaders = None
        self.responseProtocol = None
        self.usedProxy = None
        self.entitlement = None
        self._proxyHost = None  # Can be a URL object
        self.proxyHost = None
        self.proxyProtocol = None
        # More investigation about how persistent connections affect Conary
        # operation is needed. For now, just close the cached connections.
        self.opener = self.openerFactory(proxyMap=proxyMap, caCerts=caCerts,
                persist=False, connectAttempts=connectAttempts)

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

    def request(self, url, body, verbose=0):
        self.verbose = verbose

        req = self.opener.newRequest(url, method='POST',
                headers=self.extraHeaders)

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
            try:
                response = self.opener.open(req)
            except AbortError:
                raise
            except http_error.ResponseError, err:
                if err.errcode == 403:
                    raise errors.InsufficientPermission(
                            repoName=self.serverName, url=url)
                elif err.errcode == 500:
                    raise errors.InternalServerError(err)
                else:
                    # Already has adequate URL information, so just rethrow it
                    # without modifying the message.
                    util.rethrow(errors.OpenError, False)
            except (socket.error, EnvironmentError, http_error.TransportError):
                e_type, e_value, e_tb = sys.exc_info()
                if isinstance(e_value, socket.error):
                    errmsg = http_error.splitSocketError(e_value)[1]
                elif isinstance(e_value, EnvironmentError):
                    errmsg = e_value.sterror
                    # sometimes there is a socket error hiding inside an
                    # IOError!
                    if isinstance(errmsg, socket.error):
                        errmsg = http_error.splitSocketError(errmsg)[1]
                else:
                    e_name = getattr(e_type, '__name__', 'Unknown Error')
                    errmsg = '%s: %s' % (e_name, e_value)
                raise errors.OpenError(
                        "Error occurred opening repository %s: %s" %
                        (url, errmsg)), None, e_tb

            else:
                self.responseHeaders = response.headers
                self.responseProtocol = response.protocolVersion
                return self.parse_response(response)
        finally:
            self.usedProxy = self.opener.lastProxy
            self._proxyHost = self.opener.lastProxy
            if self._proxyHost:
                self.proxyHost = self._proxyHost.hostport
                self.proxyProtocol = self._proxyHost.scheme

    def getparser(self):
        return util.xmlrpcGetParser()
