#
# Copyright (c) 2004-2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

""" XMLRPC transport class that uses urllib to allow for proxies
    Unfortunately, urllib needs some touching up to allow 
    XMLRPC commands to be sent, hence the XMLOpener class """

import base64
import xmlrpclib
import urllib
import zlib
from StringIO import StringIO
        
class XMLOpener(urllib.FancyURLopener):
    def __init__(self, *args, **kw):
        self.compress = False
        urllib.FancyURLopener.__init__(self, *args, **kw)

    def setCompress(self, compress):
        self.compress = compress

    def open_https(self, url, data=None):
        return self.open_http(url, data=data, ssl=True)
    
    def open_http(self, url, data=None, ssl=False):
        """override this WHOLE FUNCTION to change
	   one magic string -- the content type --
	   which is hardcoded in (this version also supports https)"""
        if ssl:
            protocol='https'
        else:
            protocol='http'
        import httplib
        user_passwd = None
        if isinstance(url, str):
            host, selector = urllib.splithost(url)
            if host:
                user_passwd, host = urllib.splituser(host)
                host = urllib.unquote(host)
            realhost = host
        else:
            host, selector = url
            urltype, rest = urllib.splittype(selector)
            url = rest
            user_passwd = None
            # XXX proxy broken with https
            if urltype.lower() != 'http':
                realhost = None
            else:
                realhost, rest = urllib.splithost(rest)
                if realhost:
                    user_passwd, realhost = urllib.splituser(realhost)
                if user_passwd:
                    selector = "%s://%s%s" % (urltype, realhost, rest)
                if urllib.proxy_bypass(realhost):
                    host = realhost

            #print "proxy via http:", host, selector
        if not host: raise IOError, ('http error', 'no host given')
        if user_passwd:
            import base64
            auth = base64.encodestring(user_passwd).strip()
        else:
            auth = None
        if ssl:
            h = httplib.HTTPS(host, None, None)
        else:
            h = httplib.HTTP(host)
	# SPX: use the full URL here, not just the selector or name
	# based virtual hosts don't work
        fullUrl = '%s:%s' %(protocol, url)
        if data is not None:
            h.putrequest('POST', fullUrl)
            if self.compress:
                h.putheader('Content-encoding', 'deflate')
                data = zlib.compress(data, 9)
            h.putheader('Content-type', 'text/xml')
            h.putheader('Content-length', '%d' % len(data))
            h.putheader('Accept-encoding', 'deflate')
        else:
            h.putrequest('GET', fullUrl)
        if auth:
            h.putheader('Authorization', 'Basic %s' % auth)
        if realhost:
            h.putheader('Host', realhost)
        for args in self.addheaders:
            h.putheader(*args)
        h.endheaders()
        if data is not None:
            h.send(data)
        errcode, errmsg, headers = h.getreply()
        if errcode == 200:
            fp = h.getfile()
            usedAnonymous = 'X-Conary-UsedAnonymous' in headers

            encoding = headers.get('Content-encoding', None)
            if encoding == 'deflate':
                fp = StringIO(zlib.decompress(fp.read()))

            return usedAnonymous, urllib.addinfourl(fp, headers, fullUrl)
        else:
	    raise xmlrpclib.ProtocolError(url, errcode, errmsg, headers)

def getrealhost(host):
    """ Slice off username/passwd and portnum """
    atpoint = host.find('@') + 1
    colpoint = host.rfind(':')
    if colpoint == -1:
	return host[atpoint:]
    else:
	return host[atpoint:colpoint]


class Transport(xmlrpclib.Transport):

    # override?
    user_agent =  "xmlrpclib.py/%s (www.pythonware.com modified by rPath, Inc.)" % xmlrpclib.__version__

    def __init__(self, https = False, entitlement = None):
        self.https = https
        self.compress = False
        if entitlement is not None:
            self.entitlement = "%s %s" % (entitlement[0],
                                  base64.encodestring(entitlement[1])[:-1])
        else:
            self.entitlement = None

    def setCompress(self, compress):
        self.compress = compress

    def _protocol(self):
        if self.https:
            return 'https'
        return 'http'

    def request(self, host, handler, request_body, verbose=0):
	self.verbose = verbose

	# turn off proxy for localhost
	realhost = getrealhost(host)
	if realhost == 'localhost':
	    opener = XMLOpener({})
	else:
	    opener = XMLOpener()
        opener.setCompress(self.compress)

	opener.addheaders = []
	host, extra_headers, x509 = self.get_host_info(host)
	opener.addheader('Host', host)
	if extra_headers:
	    if isinstance(extra_headers, dict):
		extra_headers = extra_headers.items()
	    for key, value in extra_headers:
		opener.addheader(key,value)

        if self.entitlement:
            opener.addheader('X-Conary-Entitlement', self.entitlement)

	opener.addheader('User-agent', self.user_agent)
	usedAnonymous, response = opener.open(''.join([self._protocol(), '://', host, handler]), request_body)
        
        resp = self.parse_response(response)
        rc = ( [ usedAnonymous ] + resp[0], )
	return rc

    
