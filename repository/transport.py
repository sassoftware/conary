#
# Copyright (c) 2004 Specifix, Inc.
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
    XMLRPM commands to be sent, hence the XMLOpener class """

import xmlrpclib
import urllib

class XMLOpener(urllib.FancyURLopener):
    
    def open_http(self, url, data=None):
        """override this WHOLE FUNCTION to change
	   one magic string -- the content type --
	   which is hardcoded in"""
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
        h = httplib.HTTP(host)
        if data is not None:
            h.putrequest('POST', selector)
            h.putheader('Content-type', 'text/xml')
            h.putheader('Content-length', '%d' % len(data))
        else:
            h.putrequest('GET', selector)
        if auth: h.putheader('Authorization', 'Basic %s' % auth)
        if realhost: h.putheader('Host', realhost)
        for args in self.addheaders: h.putheader(*args)
        h.endheaders()
        if data is not None:
            h.send(data)
        errcode, errmsg, headers = h.getreply()
        fp = h.getfile()
        if errcode == 200:
            return urllib.addinfourl(fp, headers, "http:" + url)
        else:
	    raise xmlrpclib.ProtocolError(url, errcode, errmsg, headers)


class Transport(xmlrpclib.Transport):

    # override?
    user_agent =  "xmlrpclib.py/%s (by www.pythonware.com)" % xmlrpclib.__version__

    def request(self, host, handler, request_body, verbose=0):
	self.verbose = verbose
	opener = XMLOpener()
	opener.addheaders = []
	host, extra_headers, x509 = self.get_host_info(host)
	opener.addheader('Host', host)
	if extra_headers:
	    if isinstance(extra_headers, dict):
		extra_headers = extra_headers.items()
	    for key, value in extra_headers:
		opener.addheader(key,value)
	opener.addheader('User-agent', self.user_agent)
	response = opener.open(''.join(['http://', host, handler]), request_body)
	return self.parse_response(response)

