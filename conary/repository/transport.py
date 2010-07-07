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

class ConaryURLOpener(URLOpener):
    """An opener aware of the conary:// protocol"""
    open_conary = URLOpener.open_http
    open_conarys = URLOpener.open_https


class XMLOpener(URLOpener):
    contentType = 'text/xml'

    def open_http(self, *args, **kwargs):
        fp = URLOpener.open_http(self, *args, **kwargs)
        usedAnonymous = 'X-Conary-UsedAnonymous' in fp.headers
        return usedAnonymous, fp

    def http_error(self, url, fp, errcode, errmsg, headers, data=None):
        raise xmlrpclib.ProtocolError(url, errcode, errmsg, headers)

    open_conary = open_http
    open_conarys = URLOpener.open_https

class Transport(xmlrpclib.Transport):

    # override?
    user_agent = "xmlrpclib.py/%s (www.pythonware.com modified by " \
        "rPath, Inc.)" % xmlrpclib.__version__
    # make this a class variable so that across all attempts to transport
    # we'll only
    # spew messages once per host.
    failedHosts = set()
    UrlOpenerFactory = XMLOpener

    def __init__(self, https=False, proxies=None, serverName=None,
                 extraHeaders=None, caCerts=None):
        self.https = https
        self.compress = False
        self.abortCheck = None
        self.proxies = proxies
        self.serverName = serverName
        self.setExtraHeaders(extraHeaders)
        self.caCerts = caCerts
        self.responseHeaders = None
        self.responseProtocol = None
        self.usedProxy = False
        self.entitlement = None
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

        opener = self.UrlOpenerFactory(self.proxies, caCerts=self.caCerts)
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
            self.proxyHost = getattr(opener, 'proxyHost', None)
            self.proxyProtocol = getattr(opener, 'proxyProtocol', None)
        if hasattr(response, 'headers'):
            self.responseHeaders = response.headers
            self.responseProtocol = response.protocolVersion
        resp = self.parse_response(response)
        rc = ([usedAnonymous] + resp[0], )
        return rc

    def getparser(self):
        return util.xmlrpcGetParser()
