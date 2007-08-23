#!/usr/bin/python2.4
# -*- mode: python -*-
#
# Copyright (c) 2004-2007 rPath, Inc.
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

import base64
import errno
import os
import posixpath
import select
import socket
import sys
import xmlrpclib
import urllib
import zlib
import BaseHTTPServer
from SimpleHTTPServer import SimpleHTTPRequestHandler

# Secure server support
try:
    from M2Crypto import SSL
except ImportError:
    SSL = None

thisFile = sys.modules[__name__].__file__
thisPath = os.path.dirname(thisFile)
if thisPath:
    mainPath = thisPath + "/../.."
else:
    mainPath = "../.."
mainPath = os.path.realpath(mainPath)
sys.path.insert(0, mainPath)
from conary.lib import coveragehook

from conary import dbstore
from conary.lib import options
from conary.lib import util
from conary.lib.cfg import CfgBool, CfgInt, CfgPath
from conary.lib.tracelog import initLog, logMe
from conary.repository import changeset, errors, netclient
from conary.repository.filecontainer import FileContainer
from conary.repository.netrepos import netserver, proxy
from conary.repository.netrepos.proxy import ProxyRepositoryServer
from conary.repository.netrepos.netserver import NetworkRepositoryServer
from conary.server import schema
from conary.web import webauth

sys.excepthook = util.genExcepthook(debug=True)

class HttpRequests(SimpleHTTPRequestHandler):

    outFiles = {}
    inFiles = {}

    tmpDir = None

    netRepos = None
    netProxy = None

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
        path = self.tmpDir
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
            if not queryString:
                # handle CNY-1142
                self.send_error(400)
                return None
            urlPath = posixpath.normpath(urllib.unquote(self.path))
            localName = self.tmpDir + "/" + queryString + "-out"
            if os.path.realpath(localName) != localName:
                self.send_error(404)
                return None

            if localName.endswith(".cf-out"):
                try:
                    f = open(localName, "r")
                except IOError:
                    self.send_error(404)
                    return None

                os.unlink(localName)

                items = []
                totalSize = 0
                for l in f.readlines():
                    (path, size, isChangeset, preserveFile) = l.split()
                    size = int(size)
                    isChangeset = int(isChangeset)
                    preserveFile = int(preserveFile)
                    totalSize += size
                    items.append((path, size, isChangeset, preserveFile))
                f.close()
                del f
            else:
                try:
                    size = os.stat(localName).st_size;
                except OSError:
                    self.send_error(404)
                    return None
                items = [ (localName, size, 0, 0) ]
                totalSize = size

            self.send_response(200)
            self.send_header("Content-type", "application/octet-stream")
            self.send_header("Content-Length", str(totalSize))
            self.end_headers()

            for path, size, isChangeset, preserveFile in items:
                if isChangeset:
                    cs = FileContainer(util.ExtendedFile(path,
                                                         buffering = False))
                    cs.dump(self.wfile.write,
                            lambda name, tag, size, f, sizeCb:
                                _writeNestedFile(self.wfile, name, tag, size, f,
                                                 sizeCb))

                    del cs
                else:
                    f = open(path)
                    util.copyfileobj(f, self.wfile)

                if not preserveFile:
                    os.unlink(path)
        else:
            self.send_error(501)

    def do_POST(self):
        if self.headers.get('Content-Type', '') == 'text/xml':
            authToken = self.getAuth()
            if authToken is None:
                return

            return self.handleXml(authToken)
        else:
            self.send_error(501)

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

        try:
            entitlementList = webauth.parseEntitlement(
                        self.headers.get('X-Conary-Entitlement', '') )
        except:
            self.send_error(400)
            return None

        httpAuthToken.append(entitlementList)
        return httpAuthToken

    def checkAuth(self):
 	if not self.headers.has_key('Authorization'):
            self.requestAuth()
            return None
	else:
            authToken = self.getAuth()
            if authToken is None:
                return

	return authToken

    def requestAuth(self):
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="Conary Repository"')
        self.end_headers()
        return None

    def handleXml(self, authToken):
	contentLength = int(self.headers['Content-Length'])
        data = self.rfile.read(contentLength)

        targetServerName = self.headers.get('X-Conary-Servername', None)

        encoding = self.headers.get('Content-Encoding', None)
        if encoding == 'deflate':
            data = zlib.decompress(data)

        (params, method) = xmlrpclib.loads(data)
        logMe(3, "decoded xml-rpc call %s from %d bytes request" %(method, contentLength))

        if self.netProxy:
            repos = self.netProxy
        else:
            repos = self.netRepos

        localAddr = "%s:%s" % self.request.getsockname()

        if repos is not None:
            try:
                result = repos.callWrapper('http', None, method, authToken,
                            params, remoteIp = self.connection.getpeername()[0],
                            rawUrl = self.path, localAddr = localAddr,
                            protocolString = self.request_version,
                            headers = self.headers)
            except errors.InsufficientPermission:
                self.send_error(403)
                return None
            logMe(3, "returned from", method)

        usedAnonymous = result[0]
        # Get the extra information from the end of result
        extraInfo = result[-1]
        result = result[1:-1]

	resp = xmlrpclib.dumps((result,), methodresponse=1)
        logMe(3, "encoded xml-rpc response to %d bytes" % (len(resp),))

	self.send_response(200)
        encoding = self.headers.get('Accept-encoding', '')
        if len(resp) > 200 and 'deflate' in encoding:
            resp = zlib.compress(resp, 5)
            self.send_header('Content-encoding', 'deflate')
	self.send_header("Content-type", "text/xml")
	self.send_header("Content-length", str(len(resp)))
        if usedAnonymous:
            self.send_header("X-Conary-UsedAnonymous", '1')
        if extraInfo:
            # If available, send to the client the via headers all the way up
            # to us
            via = extraInfo.getVia()
            if via:
                self.send_header('Via', via)
            # And add our own via header
            # Note that we don't do this if we are the origin server
            # (talking to a repository; extraInfo is None in that case)
            # We are HTTP/1.0 compliant
            via = proxy.formatViaHeader(localAddr, 'HTTP/1.0')
            self.send_header('Via', via)

	self.end_headers()
	self.wfile.write(resp)
        logMe(3, "sent response to client", len(resp), "bytes")
	return resp

    def do_PUT(self):
        chunked = False
        if 'Transfer-encoding' in self.headers:
            contentLength = 0
            chunked = True
        elif 'Content-Length' in self.headers:
            chunked = False
            contentLength = int(self.headers['Content-Length'])
        else:
            # send 411: Length Required
            self.send_error(411)

        authToken = self.getAuth()

        if self.cfg.proxyContentsDir:
            status, reason = netclient.httpPutFile(self.path, self.rfile, contentLength)
            self.send_response(status)
            return

        path = self.path.split("?")[-1]

        if '/' in path:
            self.send_error(403)

        path = self.tmpDir + '/' + path + "-in"

        size = os.stat(path).st_size
        if size != 0:
            self.send_error(410)
            return

        out = open(path, "w")
        if chunked:
            while 1:
                chunk = self.rfile.readline()
                chunkSize = int(chunk, 16)
                # chunksize of 0 means we're done
                if chunkSize == 0:
                    break
                util.copyfileobj(self.rfile, out, sizeLimit=chunkSize)
                # read the \r\n after the chunk we just copied
                self.rfile.readline()
        else:
            util.copyfileobj(self.rfile, out, sizeLimit=contentLength)
        self.send_response(200)

class ResetableNetworkRepositoryServer(NetworkRepositoryServer):
    publicCalls = set(tuple(NetworkRepositoryServer.publicCalls) + ('reset',))
    def reset(self, authToken, clientVersion):
        import shutil
        logMe(1, "resetting NetworkRepositoryServer", self.repDB)
        try:
            shutil.rmtree(self.contentsDir[0])
        except OSError, e:
            if e.errno != errno.ENOENT:
                raise
        os.mkdir(self.contentsDir[0])

        # cheap trick. sqlite3 doesn't mind zero byte files; just replace
        # the file with a zero byte one (to change the inode) and reopen
        open(self.repDB[1] + '.new', "w")
        os.rename(self.repDB[1] + '.new', self.repDB[1])
        db = dbstore.connect(self.repDB[1], 'sqlite')
        schema.loadSchema(db)
        db.commit()
        self.reopen()
        self.createUsers()
        return 0

    def createUser(self, name, password, write = False, admin = False,
                   remove = False):
        self.auth.addUser(name, password)
        self.auth.addAcl(name, None, None, write, False, admin, remove = remove)

    def createUsers(self):
        self.createUser('test', 'foo', admin = True, write = True,
                        remove = True)
        self.createUser('anonymous', 'anonymous', admin = False, write = False)

class HTTPServer(BaseHTTPServer.HTTPServer):
    def close_request(self, request):
        while select.select([request], [], [], 0)[0]:
            # drain any remaining data on this request
            # This avoids the problem seen with the keepalive code sending
            # extra bytes after all the request has been sent.
            if not request.recv(8096):
                break
        BaseHTTPServer.HTTPServer.close_request(self, request)

if SSL:
    class SecureHTTPServer(HTTPServer):
        def __init__(self, server_address, RequestHandlerClass, sslContext):
            self.sslContext = sslContext
            HTTPServer.__init__(self, server_address, RequestHandlerClass)

        def server_bind(self):
            HTTPServer.server_bind(self)
            conn = SSL.Connection(self.sslContext, self.socket)
            self.socket = conn

        def handle_request(self):
            try:
                return HTTPServer.handle_request(self)
            except SSL.SSLError:
                return

        def close_request(self, request):
            while select.select([request], [], [], 0)[0]:
                # drain any remaining data on this request
                # This avoids the problem seen with the keepalive code sending
                # extra bytes after all the request has been sent.
                if not request.recv(8096):
                    break
            request.set_shutdown(SSL.SSL_RECEIVED_SHUTDOWN |
                                 SSL.SSL_SENT_SHUTDOWN)
            HTTPServer.close_request(self, request)

    def createSSLContext(cfg):
        ctx = SSL.Context("sslv23")
        sslCert, sslKey = cfg.sslCert, cfg.sslKey
        ctx.load_cert_chain(sslCert, sslKey)
        return ctx

class ServerConfig(netserver.ServerConfig):

    port                    = (CfgInt,  8000)
    sslCert                 = CfgPath
    sslKey                  = CfgPath
    useSSL                  = CfgBool

    def __init__(self, path="serverrc"):
	netserver.ServerConfig.__init__(self)
	self.read(path, exception=False)

    def check(self):
        if self.closed:
            print >> sys.stderr, ("warning: closed config option is ignored "
                                  "by the standalone server")

        if self.forceSSL:
            print >> sys.stderr, ("warning: commitAction config option is "
                                  "ignored by the standalone server")

def usage():
    print "usage: %s" % sys.argv[0]
    print "       %s --add-user <username> [--admin] [--mirror]" % sys.argv[0]
    print "       %s --analyze" % sys.argv[0]
    print ""
    print "server flags: --config-file <path>"
    print '              --db "driver <path>"'
    print '              --log-file <path>'
    print '              --map "<from> <to>"'
    print "              --server-name <host>"
    print "              --tmp-dir <path>"
    sys.exit(1)

def addUser(netRepos, userName, admin = False, mirror = False):
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

    # never give anonymous write access by default
    write = userName != 'anonymous'
    netRepos.auth.addUser(userName, pw1)
    # user/group, trovePattern, label, write, capped, admin
    netRepos.auth.addAcl(userName, None, None, write = write)
    netRepos.auth.setMirror(userName, mirror)
    netRepos.auth.setAdmin(userName, admin)

def getServer():
    argDef = {}
    cfgMap = {
        'contents-dir'  : 'contentsDir',
	'db'	        : 'repositoryDB',
	'log-file'	: 'logFile',
	'map'	        : 'repositoryMap',
	'port'	        : 'port',
	'tmp-dir'       : 'tmpDir',
        'require-sigs'  : 'requireSigs',
        'server-name'   : 'serverName'
    }

    cfg = ServerConfig()

    argDef["config"] = options.MULT_PARAM
    # magically handled by processArgs
    argDef["config-file"] = options.ONE_PARAM

    argDef['add-user'] = options.ONE_PARAM
    argDef['admin'] = options.NO_PARAM
    argDef['analyze'] = options.NO_PARAM
    argDef['help'] = options.NO_PARAM
    argDef['migrate'] = options.NO_PARAM
    argDef['mirror'] = options.NO_PARAM

    try:
        argSet, otherArgs = options.processArgs(argDef, cfgMap, cfg, usage)
    except options.OptionError, msg:
        print >> sys.stderr, msg
        sys.exit(1)

    if 'migrate' not in argSet:
        cfg.check()

    if argSet.has_key('help'):
        usage()

    if not os.path.isdir(cfg.tmpDir):
	print cfg.tmpDir + " needs to be a directory"
	sys.exit(1)
    if not os.access(cfg.tmpDir, os.R_OK | os.W_OK | os.X_OK):
        print cfg.tmpDir + " needs to allow full read/write access"
        sys.exit(1)
    HttpRequests.tmpDir = cfg.tmpDir
    HttpRequests.cfg = cfg

    profile = 0
    if profile:
        import hotshot
        prof = hotshot.Profile('server.prof')
        prof.start()

    if cfg.useSSL:
        protocol = 'https'
    else:
        protocol = 'http'
    baseUrl="%s://%s:%s/conary/" % (protocol, os.uname()[1], cfg.port)

    # start the logging
    if 'migrate' in argSet:
        # make sure the migration progress is visible
        cfg.traceLog = (3, "stderr")
    if 'add-user' not in argSet and 'analyze' not in argSet:
        (l, f) = (3, "stderr")
        if cfg.traceLog:
            (l, f) = cfg.traceLog
        initLog(filename = f, level = l, trace=1)

    if os.path.realpath(cfg.tmpDir) != cfg.tmpDir:
        print "tmpDir cannot include symbolic links"
        sys.exit(1)

    if cfg.useSSL:
        errmsg = "Unable to start server with SSL support."
        if not SSL:
            print errmsg + " Please install m2crypto."
            sys.exit(1)
        if not (cfg.sslCert and cfg.sslKey):
            print errmsg + (" Please set the sslCert and sslKey "
                            "configuration options.")
            sys.exit(1)
        for f in [cfg.sslCert, cfg.sslKey]:
            if not os.path.exists(f):
                print errmsg + " %s does not exist" % f
                sys.exit(1)

    if cfg.proxyContentsDir:
        if len(otherArgs) > 1:
            usage()

        HttpRequests.netProxy = ProxyRepositoryServer(cfg, baseUrl)
    elif cfg.repositoryDB:
        if len(otherArgs) > 1:
            usage()

        if not cfg.contentsDir:
            assert(cfg.repositoryDB[0] == "sqlite")
            cfg.contentsDir = os.path.dirname(cfg.repositoryDB[1]) + '/contents'

        if cfg.repositoryDB[0] == 'sqlite':
            util.mkdirChain(os.path.dirname(cfg.repositoryDB[1]))

        (driver, database) = cfg.repositoryDB
        db = dbstore.connect(database, driver)
        logMe(1, "checking schema version")
        # if there is no schema or we're asked to migrate, loadSchema
        dbVersion = db.getVersion()
        # a more recent major is not compatible
        if dbVersion.major > schema.VERSION.major:
            print "ERROR: code base too old for this repository database"
            print "ERROR: repo=", dbVersion, "code=", self.VERSION
            sys.exit(-1)
        # determine is we need to call the schema migration
        loadSchema = False
        if dbVersion == 0: # no schema initialized
            loadSchema = True
        elif dbVersion.major < schema.VERSION.major: # schema too old
            loadSchema = True
        elif 'migrate' in argSet: # a migration was asked for
            loadSchema = True
        if loadSchema:
            dbVersion = schema.loadSchema(db, 'migrate' in argSet)
        if dbVersion < schema.VERSION: # migration failed...
            print "ERROR: schema migration has failed from %s to %s" %(
                dbVersion, schema.VERSION)
        if 'migrate' in argSet:
            logMe(1, "Schema migration complete", dbVersion)
            sys.exit(0)

        #netRepos = NetworkRepositoryServer(cfg, baseUrl)
        netRepos = ResetableNetworkRepositoryServer(cfg, baseUrl)
        HttpRequests.netRepos = proxy.SimpleRepositoryFilter(cfg, baseUrl, netRepos)

        if 'add-user' in argSet:
            admin = argSet.pop('admin', False)
            mirror = argSet.pop('mirror', False)
            userName = argSet.pop('add-user')
            if argSet:
                usage()
            sys.exit(addUser(netRepos, userName, admin = admin,
                             mirror = mirror))
        elif argSet.pop('analyze', False):
            if argSet:
                usage()
            netRepos.db.analyze()
            sys.exit(0)

    if argSet:
        usage()

    if cfg.useSSL:
        ctx = createSSLContext(cfg)
        httpServer = SecureHTTPServer(("", cfg.port), HttpRequests, ctx)
    else:
        httpServer = HTTPServer(("", cfg.port), HttpRequests)
    return httpServer, profile

def serve(httpServer, profile=False):
    fds = {}
    fds[httpServer.fileno()] = httpServer

    p = select.poll()
    for fd in fds.iterkeys():
        p.register(fd, select.POLLIN)

    logMe(1, "Server ready for requests")

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

def main():
    server, profile = getServer()
    serve(server)

if __name__ == '__main__':
    main()

