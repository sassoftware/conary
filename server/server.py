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
import cgi
import os
import posixpath
import select
import sys
import tempfile
import xmlrpclib
import urllib
import zlib
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

from repository.netrepos import netserver
from repository.netrepos.netserver import NetworkRepositoryServer
from conarycfg import ConfigFile
from conarycfg import STRINGDICT
from lib import options
from lib import util
from http import HttpHandler
from htmlengine import HtmlEngine

DEFAULT_FILE_PATH="/tmp/conary-server"

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
        if self.path.endswith('/'):
            self.path = self.path[:-1]
        base = os.path.basename(self.path)
        if "?" in base:
            base, queryString = base.split("?")
        else:
            queryString = ""
        
        if base != 'changeset':
            if httpHandler.requiresAuth(base):
                authToken = self.checkAuth()
                if not authToken:
                    return
            else:
                authToken = (None, None)

            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            fields = cgi.parse_qs(queryString)
            try:
                httpHandler.handleCmd(self.wfile.write, base, authToken, fields)
            except:
                self.traceback()
        else:
            urlPath = posixpath.normpath(urllib.unquote(self.path))
            localName = FILE_PATH + "/" + urlPath.split('?', 1)[1] + "-out"

            if localName.endswith(".cf-out"):
                try:
                    f = open(localName, "r")
                except IOError:
                    self.send_error(404, "File not found")
                    return None

                os.unlink(localName)

                items = []
                totalSize = 0
                for l in f.readlines():
                    (path, size) = l.split()
                    size = int(size)
                    totalSize += size
                    items.append((path, size))
                del f
            else:
                size = os.stat(localName).st_size;
                items = [ (localName, size) ]
                totalSize = size
    
            self.send_response(200)
            self.send_header("Content-type", "application/octet-stream")
            self.send_header("Content-Length", str(totalSize))
            self.end_headers()

            f = open(items[0][0], "r")
            util.copyfileobj(f, self.wfile)

            if not localName.endswith(".cf-out"):
                os.unlink(items[0][0])

    def do_POST(self):
        if self.headers.get('Content-Type', '') == 'text/xml':
            authToken = self.getAuth()
            if authToken is None:
                return
            
            return self.handleXml(authToken)

        cmd = os.path.basename(self.path)
        if httpHandler.requiresAuth(cmd):
            authToken = self.checkAuth()
            if not authToken:
                return
        else:
            authToken = (None, None)

        try: 
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            c = cgi.FieldStorage(fp = self.rfile, headers = self.headers, 
                                 environ = { 'REQUEST_METHOD' : 'POST' })
            httpHandler.handleCmd(self.wfile.write, cmd, authToken, c)
        except:
            self.traceback()

    def getAuth(self):
        info = self.headers.get('Authorization', None)
        if info is None:
            return (None, None)
        info = info.split()

        try:
            authString = base64.decodestring(info[1])
        except:
            self.send_response(400)
            return None

        if authString.count(":") != 1:
            self.send_response(400)
            return None
            
        authToken = authString.split(":")

        return authToken
    
    def checkAuth(self):
 	if not self.headers.has_key('Authorization'):
            self.requestAuth()
            return None
	else:
            authToken = self.getAuth()
            if authToken is None:
                return
            
            # verify that the user/password actually exists in the database
            if not netRepos.auth.checkUserPass(authToken):
               self.requestAuth()
               return None

	return authToken
      
    def requestAuth(self):
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="Conary Repository"')
        self.end_headers()
        return None
      
    def traceback(self):
        htmlengine = HtmlEngine()
        htmlengine.setWriter(self.wfile)
        htmlengine.stackTrace(self.wfile)
        
    def handleXml(self, authToken):
	contentLength = int(self.headers['Content-Length'])
	(params, method) = xmlrpclib.loads(self.rfile.read(contentLength))

	try:
	    result = netRepos.callWrapper(method, authToken, params)
	except netserver.InsufficientPermission:
	    self.send_response(403)
	    return

	resp = xmlrpclib.dumps((result,), methodresponse=1)

	self.send_response(200)
        encoding = self.headers.get('Accept-encoding', '')
        if len(resp) > 200 and 'zlib' in encoding:
            resp = zlib.compress(resp, 5)
            self.send_header('Content-encoding', 'zlib')
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
        from repository.netrepos import fsrepos
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
	'tmpFilePath'           : DEFAULT_FILE_PATH,
    }

    def __init__(self, path="serverrc"):
	ConfigFile.__init__(self)
	self.read(path)

def usage():
    print "usage: %s repospath authdb reposname [config file]" %sys.argv[0]
    sys.exit(1)

if __name__ == '__main__':
    argDef = {}
    cfgMap = {
	'port'	: 'port',
	'map'	: 'repositoryMap',
	'tmp-file-path' : 'tmpFilePath',
    }

    cfg = ServerConfig()

    argSet, otherArgs = options.processArgs(argDef, cfgMap, cfg, usage)

    FILE_PATH = cfg.tmpFilePath

    if not os.path.isdir(FILE_PATH):
	print FILE_PATH + " needs to be a directory"
	sys.exit(1)


    if len(otherArgs) == 5:
        cfg.read(otherArgs.pop())

    if len(otherArgs) != 4:
	usage()

    profile = 0
    if profile:
        import hotshot
        prof = hotshot.Profile('server.prof')
        prof.start()

    baseUrl="http://%s:%s/" % (os.uname()[1], cfg.port)

    netRepos = ResetableNetworkRepositoryServer(otherArgs[1], FILE_PATH, 
			baseUrl, otherArgs[2], otherArgs[3], cfg.repositoryMap)
    httpHandler = HttpHandler(netRepos)

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
