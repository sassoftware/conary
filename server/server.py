#!/usr/bin/python2.3
# -*- mode: python -*-
#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import base64
import os
import posixpath
import select
import sys
import tempfile
import xmlrpclib
import urllib
from BaseHTTPServer import HTTPServer
from SimpleHTTPServer import SimpleHTTPRequestHandler

if len(sys.argv) != 4:
    print "needs path to srs, the repository, and the authorization database"
    sys.exit(1)

sys.path.append(sys.argv[1])

import netserver
from netserver import NetworkRepositoryServer

FILE_PATH="/tmp/conary-server"
BASE_URL="http://%s:8000/" % os.uname()[1]

#class SRSServer(SimpleXMLRPCServer):

    #allow_reuse_address = 1

class HttpRequests(SimpleHTTPRequestHandler):
    
    outFiles = {}
    inFiles = {}

    def translate_path(self, path):
        """Translate a /-separated PATH to the local filename syntax.

        Components that mean special things to the local file system
        (e.g. drive or directory names) are ignored.  (XXX They should
        probably be diagnosed.)

        """
        path = posixpath.normpath(urllib.unquote(path))
	path = path.split("?", 1)[1]
        words = path.split('/')
        words = filter(None, words)
        path = FILE_PATH
        for word in words:
            drive, word = os.path.splitdrive(word)
            head, word = os.path.split(word)
            if word in (os.curdir, os.pardir): continue
            path = os.path.join(path, word)

	path += "-out"

	self.cleanup = path
        return path

    def do_GET(self):
	self.cleanup = None
	SimpleHTTPRequestHandler.do_GET(self)
	if self.cleanup:
	    os.unlink(self.cleanup)

    def do_POST(self):
	if not self.headers.has_key('Authorization'):
	    user = None
	    pw = None
	else:
	    info = self.headers['Authorization'].split()
	    if len(info) != 2 or info[0] != "Basic":
		self.send_response(400)
		return
    
	    try:
		authString = base64.decodestring(info[1])
	    except:
		self.send_response(400)
		return

	    if authString.count(":") != 1:
		self.send_response(400)
		return
		
	    (user, pw) = authString.split(":")

	authToken = (user, pw)

	contentLength = int(self.headers['Content-Length'])
	(params, method) = xmlrpclib.loads(self.rfile.read(contentLength))

	try:
	    result = netRepos.__class__.__dict__[method](netRepos, authToken,
							 *params)
	except netserver.InsufficientPermission:
	    self.send_response(403)
	    return
	#except:
	#    self.send_response(500)
	#    return

	resp = xmlrpclib.dumps((result,), methodresponse=1)

	self.send_response(200)
	self.send_header("Content-type", "text/xml")
	self.send_header("Content-length", str(len(resp)))
	self.end_headers()
	self.wfile.write(resp)

	return resp

    def do_PUT(self):
	path = self.path.split("?")[-1]
	path = FILE_PATH + '/' + path + "-in"

	size = os.stat(path).st_size
	if size != 0:
	    self.send_response(410, "Gone")
	    return

	out = open(path, "w")

	contentLength = int(self.headers['Content-Length'])
	while contentLength:
	    s = self.rfile.read(contentLength)
	    contentLength -= len(s)
	    out.write(s)

	self.send_response(200, 'OK')

if __name__ == '__main__':
    profile = 0
    if profile:
        import hotshot
        prof = hotshot.Profile('server.prof')
        prof.start()

    netRepos = NetworkRepositoryServer(sys.argv[2], FILE_PATH, BASE_URL,
				       sys.argv[3])

    httpServer = HTTPServer(("", 8000), HttpRequests)

    fds = {}
    fds[httpServer.fileno()] = httpServer

    p = select.poll()
    for fd in fds.iterkeys():
        p.register(fd, select.POLLIN)

    while True:
        try:
            events = p.poll()
            for (fd, event) in events:
                fds[fd].handle_request()
        except select.error:
            pass
        except:
            if profile:
                prof.stop()
                print "exception happened, exiting"
                sys.exit(1)
            else:
                raise
            
