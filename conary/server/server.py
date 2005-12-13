#!/usr/bin/python2.4
# -*- mode: python -*-
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

import base64
import cgi
import errno
import os
import posixpath
import select
import sys
import tempfile
import traceback
import xmlrpclib
import urllib
import zlib
from BaseHTTPServer import HTTPServer
from SimpleHTTPServer import SimpleHTTPRequestHandler

thisFile = sys.modules[__name__].__file__
thisPath = os.path.dirname(thisFile)
if thisPath:
    mainPath = thisPath + "/../.."
else:
    mainPath = "../.."
mainPath = os.path.realpath(mainPath)
sys.path.insert(0, mainPath)

from conary.conarycfg import CfgRepoMap
from conary.lib import options
from conary.lib import util
from conary.lib.cfg import ConfigFile,CfgPath,CfgInt,CfgBool
from conary.lib.tracelog import initLog, logMe
from conary.repository import changeset
from conary.repository import errors
from conary.repository.filecontainer import FileContainer
from conary.repository.netrepos import netauth
from conary.repository.netrepos import netserver
from conary.repository.netrepos.netserver import NetworkRepositoryServer

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
        def _writeNestedFile(outF, name, tag, size, f, sizeCb):
            if changeset.ChangedFileTypes.refr[4:] == tag[2:]:
                path = f.read()
                size = os.stat(path).st_size
                f = open(path)
                tag = tag[0:2] + changeset.ChangedFileTypes.file[4:]

            sizeCb(size, tag)
            bytes = util.copyfileobj(f, outF)

        if self.path.endswith('/'):
            self.path = self.path[:-1]
        base = os.path.basename(self.path)
        if "?" in base:
            base, queryString = base.split("?")
        else:
            queryString = ""

        if base == 'changeset':
            urlPath = posixpath.normpath(urllib.unquote(self.path))
            localName = FILE_PATH + "/" + urlPath.split('?', 1)[1] + "-out"
            if os.path.realpath(localName) != localName:
                self.send_error(403, "File not found")
                return None

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

            for path, size in items:
                if path.endswith('.ccs-out'):
                    cs = FileContainer(open(path))
                    cs.dump(self.wfile.write,
                            lambda name, tag, size, f, sizeCb:
                                _writeNestedFile(self.wfile, name, tag, size, f,
                                                 sizeCb))

                    del cs
                else:
                    f = open(path)
                    util.copyfileobj(f, self.wfile)

                if path.startswith(FILE_PATH):
                    os.unlink(path)
        else:
            self.send_error(501, "Not Implemented")

    def do_POST(self):
        if self.headers.get('Content-Type', '') == 'text/xml':
            authToken = self.getAuth()
            if authToken is None:
                return

            return self.handleXml(authToken)
        else:
            self.send_error(501, "Not Implemented")

    def getAuth(self):
        info = self.headers.get('Authorization', None)
        if info is None:
            httpAuthToken = [ 'anonymous', 'anonymous' ]
        else:
            info = info.split()

            try:
                authString = base64.decodestring(info[1])
            except:
                self.send_error(400)
                return None

            if authString.count(":") != 1:
                self.send_error(400)
                return None
                
            httpAuthToken = authString.split(":")

        entitlement = self.headers.get('X-Conary-Entitlement', None)
        if entitlement is not None:
            try:
                entitlement = entitlement.split()
                entitlement[1] = base64.decodestring(entitlement[1])
            except:
                self.send_error(400)
                return None
        else:
            entitlement = [ None, None ]

        return httpAuthToken + entitlement

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
                self.send_error(403)
                return None

	return authToken

    def requestAuth(self):
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="Conary Repository"')
        self.end_headers()
        return None

    def handleXml(self, authToken):
	contentLength = int(self.headers['Content-Length'])
        data = self.rfile.read(contentLength)

        encoding = self.headers.get('Content-Encoding', None)
        if encoding == 'deflate':
            data = zlib.decompress(data)

        (params, method) = xmlrpclib.loads(data)
        logMe(3, "decoded xml-rpc call %s from %d bytes request" %(method, contentLength))

	try:
	    result = netRepos.callWrapper(None, None, method, authToken, params)
	except errors.InsufficientPermission:
	    self.send_error(403)
	    return None
        logMe(3, "returned from", method)

	resp = xmlrpclib.dumps((result,), methodresponse=1)
        logMe(3, "encoded xml-rpc response to %d bytes" % (len(resp),))

	self.send_response(200)
        encoding = self.headers.get('Accept-encoding', '')
        if len(resp) > 200 and 'deflate' in encoding:
            resp = zlib.compress(resp, 5)
            self.send_header('Content-encoding', 'deflate')
	self.send_header("Content-type", "text/xml")
	self.send_header("Content-length", str(len(resp)))
	self.end_headers()
	self.wfile.write(resp)
        logMe(3, "sent response to client", len(resp), "bytes")
	return resp

    def do_PUT(self):
	path = self.path.split("?")[-1]

        if '/' in path:
	    self.send_error(403, "Forbidden")

	path = FILE_PATH + '/' + path + "-in"

	size = os.stat(path).st_size
	if size != 0:
	    self.send_error(410, "Gone")
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
        try:
            shutil.rmtree(self.contentsDir)
        except OSError, e:
            if e.errno != errno.ENOENT:
                raise
        os.mkdir(self.contentsDir)

        # cheap trick. sqlite3 doesn't mind zero byte files; just replace
        # the file with a zero byte one (to change the inode) and reopen
        open(self.repDB[1] + '.new', "w")
        os.rename(self.repDB[1] + '.new', self.repDB[1])
        self.reopen()

        return 0

class ServerConfig(netserver.ServerConfig):

    port		= (CfgInt,  8000)

    def __init__(self, path="serverrc"):
	netserver.ServerConfig.__init__(self)
	self.read(path)

    def check(self):
        assert(not self.cacheChangeSets)
        assert(not self.closed)
        assert(not self.commitAction)
        assert(not self.forceSSL)
        assert(not self.repositoryDir)
        assert(not self.serverName)

def usage():
    print "usage: %s repospath reposname" %sys.argv[0]
    print "       %s --add-user <username> repospath" %sys.argv[0]
    print ""
    print "server flags: --config-file <path>"
    print '              --log-file <path>'
    print '              --map "<from> <to>"'
    print "              --tmp-dir <path>"
    sys.exit(1)

def addUser(cfg, userName, otherArgs):
    if len(otherArgs) != 2:
        usage()

    if os.isatty(0):
        from getpass import getpass

        pw1 = getpass('Password:')
        pw2 = getpass('Reenter password:')

        if pw1 != pw2:
            print "Passwords do not match."
            return 1
    else:
        # chop off the trailing newline
        pw1 = sys.stdin.readline()[:-1]

    cfg.repositoryDB = ("sqlite", otherArgs[1] + '/sqldb')
    cfg.contentsDir = otherArgs[1] + '/contents'

    netRepos = ResetableNetworkRepositoryServer(cfg, '')
    netRepos.auth.addUser(userName, pw1)
    netRepos.auth.addAcl(userName, None, None, True, False, True)

if __name__ == '__main__':
    argDef = {}
    cfgMap = {
	'log-file'	: 'logFile',
	'map'	        : 'repositoryMap',
	'port'	        : 'port',
	'tmp-dir'       : 'tmpDir',
        'require-sigs'  : 'requireSigs'
    }

    cfg = ServerConfig()

    argDef["config"] = options.MULT_PARAM
    # magically handled by processArgs
    argDef["config-file"] = options.ONE_PARAM
    argDef['add-user'] = options.ONE_PARAM
    argDef['help'] = options.ONE_PARAM

    try:
        argSet, otherArgs = options.processArgs(argDef, cfgMap, cfg, usage)
    except options.OptionError, msg:
        print >> sys.stderr, msg
        sys.exit(1)

    cfg.check()

    FILE_PATH = cfg.tmpDir

    if argSet.has_key('help'):
        usage()

    if argSet.has_key('add-user'):
        sys.exit(addUser(cfg, argSet['add-user'], otherArgs))

    if not os.path.isdir(FILE_PATH):
	print FILE_PATH + " needs to be a directory"
	sys.exit(1)
    if not os.access(FILE_PATH, os.R_OK | os.W_OK | os.X_OK):
        print FILE_PATH + " needs to allow full read/write access"
        sys.exit(1)

    if len(otherArgs) != 3 or argSet:
	usage()

    profile = 0
    if profile:
        import hotshot
        prof = hotshot.Profile('server.prof')
        prof.start()

    baseUrl="http://%s:%s/" % (os.uname()[1], cfg.port)

    # start the logging
    initLog(level=3, trace=1)

    util.mkdirChain(otherArgs[1])

    cfg.repositoryDB = ("sqlite", otherArgs[1] + '/sqldb')
    cfg.contentsDir = otherArgs[1] + '/contents'
    cfg.serverName = otherArgs[2]

    netRepos = ResetableNetworkRepositoryServer(cfg, baseUrl)

    httpServer = HTTPServer(("", cfg.port), HttpRequests)

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
