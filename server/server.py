#!/usr/bin/python2.3
# -*- mode: python -*-
#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import os
import posixpath
import select
import sys
import tempfile
import urllib
from BaseHTTPServer import HTTPServer
from SimpleHTTPServer import SimpleHTTPRequestHandler
from SimpleXMLRPCServer import SimpleXMLRPCServer

if len(sys.argv) != 3:
    print "needs path to srs and to the repository"
    sys.exit(1)

sys.path.append(sys.argv[1])

from netserver import NetworkRepositoryServer

FILE_PATH="/tmp/conary-server"
BASE_URL="http://localhost:8001/"

class SRSServer(SimpleXMLRPCServer):

    allow_reuse_address = 1

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

    def do_PUT(self):
	path = FILE_PATH + '/' + os.path.basename(self.path) + "-in"

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

def handler(req):
    req.content_type = "text/xml"
    req.send_http_header()
    data = req.read()
    try:
        params, method = xmlrpclib.loads(data)
    except xmlrpclib.ResponseError, e:
        req.write ( xmlrpclib.dumps(e))
    return apache.OK

if __name__ == '__main__':
    netRepos = NetworkRepositoryServer(sys.argv[2], "c", FILE_PATH, BASE_URL)
    xmlServer = SRSServer(("localhost", 8000))
    xmlServer.register_instance(netRepos)
    xmlServer.register_introspection_functions()

    httpServer = HTTPServer(("localhost", 8001), HttpRequests)

    fds = {}
    fds[xmlServer.fileno()] = xmlServer
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
