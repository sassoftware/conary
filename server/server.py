#!/usr/bin/python2.3
# -*- mode: python -*-
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

thisFile = sys.modules[__name__].__file__
thisPath = os.path.dirname(thisFile)
if thisPath:
    mainPath = thisPath + "/.."
else:
    mainPath = ".."
mainPath = os.path.realpath(mainPath)

sys.path.append(mainPath)

import netserver
from netserver import NetworkRepositoryServer
from conarycfg import ConfigFile
from conarycfg import STRINGDICT
from lib import options

FILE_PATH="/tmp/conary-server"

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
        print "GET"
	SimpleHTTPRequestHandler.do_GET(self)
        print "done"
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
	    result = netRepos.callWrapper(method, authToken, params)
	except netserver.InsufficientPermission:
	    self.send_response(403)
	    return

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

class ResetableNetworkRepositoryServer(NetworkRepositoryServer):

    def reset(self, authToken, clientVersion):
        import shutil
        from localrep import fsrepos
	del self.repos
	shutil.rmtree(self.repPath + '/contents')
        os.unlink(self.repPath + '/sqldb')
	self.repos = fsrepos.FilesystemRepository(self.name, self.repPath,
						  self.map)
        return 0

class ServerConfig(ConfigFile):

    defaults = {
	'port'			:   '8000',
	'repositoryMap'         : [ STRINGDICT, {} ],
    }

    def __init__(self):
	ConfigFile.__init__(self)
	self.read("serverrc")

def usage():
    print "usage message goes here"
    sys.exit(1)

if __name__ == '__main__':
    cfg = ServerConfig()

    argDef = {}
    cfgMap = {
	'port'	: 'port',
	'map'	: 'repositoryMap',
    }

    if not os.path.isdir(FILE_PATH):
	print FILE_PATH + " needs to be a directory"
	sys.exit(1)

    argSet, otherArgs = options.processArgs(argDef, cfgMap, cfg, usage)

    if len(otherArgs) != 4:
	print "needs path to the repository, and authorization database, and the name of this repository"
	sys.exit(1)

    profile = 0
    if profile:
        import hotshot
        prof = hotshot.Profile('server.prof')
        prof.start()

    baseUrl="http://%s:%s/" % (os.uname()[1], cfg.port)

    netRepos = ResetableNetworkRepositoryServer(otherArgs[1], FILE_PATH, 
			baseUrl, otherArgs[2], otherArgs[3], cfg.repositoryMap)

    port = int(cfg.port)
    httpServer = HTTPServer(("", port), HttpRequests)

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
            
