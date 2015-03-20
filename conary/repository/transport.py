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
import cgi
import socket
import StringIO
import sys
import xmlrpclib
import zlib

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
            # The full target URL is sent on the request line, but
            # intermediaries like nginx don't have a way to pass it on.
            req.headers['X-Conary-Proxy-Target-Scheme'] = req.url.scheme
        return opener.URLOpener._requestOnce(self, req, proxy)


class XMLOpener(ConaryURLOpener):
    contentType = 'text/xml'


class Transport(xmlrpclib.Transport):

    # override?
    user_agent = "Conary/%s" % constants.version

    openerFactory = XMLOpener
    contentTypes = ['text/xml', 'application/xml']
    mixedType = 'multipart/mixed'

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
        req.headers['Accept'] = ','.join(self.contentTypes + [self.mixedType])

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
                    errmsg = e_value.strerror
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

    @staticmethod
    def _parse_multipart_header(response, boundary):
        if response.readline() != "--%s\r\n" % (boundary,):
            raise xmlrpclib.ResponseError("Response body is corrupted")
        ctype = cenc = clen = None
        while True:
            line = response.readline().rstrip('\r\n')
            if not line:
                break
            key, value = line.split(': ')
            if key.lower() == 'content-type':
                ctype = cgi.parse_header(value)[0]
            elif key.lower() == 'content-encoding':
                cenc = value
            elif key.lower() == 'content-length':
                clen = int(value)
        return ctype, cenc, clen

    def parse_response(self, response):
        ctype = response.headers.get('content-type', '')
        ctype, pdict = cgi.parse_header(ctype)
        if ctype in self.contentTypes:
            return xmlrpclib.Transport.parse_response(self, response)
        elif ctype != self.mixedType:
            raise xmlrpclib.ResponseError(
                    "Response has invalid or missing Content-Type")
        decoder = MultipartDecoder(response, pdict['boundary'])

        # Read XMLRPC response
        rpcHeaders, rpcBody = decoder.get()
        if (cgi.parse_header(rpcHeaders.get('content-type'))[0]
                not in self.contentTypes):
            raise xmlrpclib.ResponseError(
                    "Response has invalid or missing Content-Type")
        rpcBody = StringIO.StringIO(rpcBody)
        result = xmlrpclib.Transport.parse_response(self, rpcBody)

        # Replace the URL in the XMLRPC response with a file-like object that
        # reads out the second part of the multipart response
        csResponse = decoder.getStream()
        if csResponse.headers.get('content-type') != (
                'application/x-conary-change-set'):
            raise xmlrpclib.ResponseError(
                    "Response body has wrong Content-Type")
        result[0][1][0] = csResponse
        return result


class MultipartDecodeError(RuntimeError):
    pass


class MultipartDecoder(object):

    def __init__(self, fobj, boundary):
        self.fobj = fobj
        self.boundary = boundary

    def _getHeader(self):
        line = self.fobj.readline()
        if '\n' not in line:
            raise errors.TruncatedResponseError()
        if line != "--%s\r\n" % (self.boundary,):
            raise MultipartDecodeError("Invalid multipart response")
        headers = {}
        while True:
            line = self.fobj.readline()
            if '\n' not in line:
                raise errors.TruncatedResponseError()
            line = line.rstrip('\r\n')
            if not line:
                break
            key, value = line.split(': ')
            headers[key.lower()] = value
        return headers

    @staticmethod
    def _decode(body, headers, hname):
        encoding = headers.get(hname.lower())
        if encoding == 'deflate':
            return zlib.decompress(body)
        elif encoding in (None, 'identity'):
            return body
        else:
            raise MultipartDecodeError(
                    "Unrecognized %s in multipart response: %s"
                    % (hname.title(), encoding))

    def get(self):
        headers = self._getHeader()
        clen = int(headers.get('content-length', -1))
        if clen < 0:
            raise MultipartDecodeError("Invalid multipart response")
        body = self.fobj.read(clen)
        if len(body) < clen:
            raise errors.TruncatedResponseError()
        body = self._decode(body, headers, 'content-transfer-encoding')
        body = self._decode(body, headers, 'content-encoding')
        line = self.fobj.readline()
        if '\n' not in line:
            raise errors.TruncatedResponseError()
        if line != '\r\n':
            raise MultipartDecodeError("Invalid multipart response")
        return headers, body

    def getStream(self, final=True):
        headers = self._getHeader()
        clen = int(headers.get('content-length', -1))
        if clen < 0:
            raise MultipartDecodeError("Invalid multipart response")
        if headers.get('content-transfer-encoding') not in (None, 'identity'):
            raise MultipartDecodeError("Invalid multipart response")
        if headers.get('content-encoding') not in (None, 'identity'):
            raise MultipartDecodeError("Invalid multipart response")
        return MultipartResponseFile(self.fobj, headers, clen, self.boundary,
                final)


class MultipartResponseFile(object):
    def __init__(self, fobj, headers, clen, boundary, final):
        self.fobj = fobj
        self.headers = headers
        self.boundary = boundary
        self.remaining = clen
        self.eof = False
        self.final = final

    def read(self, count=None):
        if self.eof:
            return ''
        if count is None:
            count = self.remaining
        count = min(count, self.remaining)
        data = self.fobj.read(count)
        self.remaining -= len(data)
        if self.remaining == 0:
            self.eof = True
            line = self.fobj.readline()
            if '\n' not in line:
                raise errors.TruncatedResponseError()
            if line != '\r\n':
                raise MultipartDecodeError("Invalid multipart response")
        return data

    def close(self):
        if self.final:
            if self.eof:
                line = self.fobj.readline()
                if '\n' not in line:
                    raise errors.TruncatedResponseError()
                if line != "--%s--\r\n" % self.boundary:
                    raise MultipartDecodeError("Invalid multipart response")
            self.fobj.close()
        self.fobj = None

    def __reduce__(self):
        # This object is passed back in the XMLRPC client layer as part of the
        # 'response', which the client then tries to pickle into the client
        # call log when enabled. Better not let it try to pickle us.
        return str, ('<multipart response>',)
