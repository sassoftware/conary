#
# Copyright (c) 2004-2005 Specifix, Inc.
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

from mod_python import apache
from mod_python import util
import os
import traceback
import xmlrpclib
import zlib

from repository.netrepos import netserver
import conarycfg

from web.webauth import getAuth

BUFFER=1024 * 256

class ServerConfig(conarycfg.ConfigFile):

    defaults = {
        'commitAction'      :  None,
        'forceSSL'          :  [ conarycfg.BOOLEAN, False ],
        'logFile'           :  None,
        'repositoryMap'     :  [ conarycfg.STRINGDICT, {} ],
        'repositoryDir'     :  None,
        'serverName'        :  None,
        'tmpDir'            :  "/var/tmp",
        'cacheChangeSets'   :  [ conarycfg.BOOLEAN, False ],
        'staticPath'        :  "/conary-static",
    }

def checkAuth(req, repos):
    if not req.headers_in.has_key('Authorization'):
        return None
    else:
        authToken = getAuth(req)
        if type(authToken) != tuple:
            return authToken

        if not repos.auth.checkUserPass(authToken):
            return None
            
    return authToken

def post(port, isSecure, repos, req):
    authToken = getAuth(req)
    if type(authToken) is int:
        return authToken

    if authToken[0] != "anonymous" and not isSecure and repos.forceSecure:
        return apache.HTTP_FORBIDDEN
    
    if isSecure:
        protocol = "https"
    else:
        protocol = "http"

    if req.headers_in['Content-Type'] == "text/xml":
        (params, method) = xmlrpclib.loads(req.read())

        try:
            result = repos.callWrapper(protocol, port, method, authToken, 
                                       params)
        except netserver.InsufficientPermission:
            return apache.HTTP_FORBIDDEN

        resp = xmlrpclib.dumps((result,), methodresponse=1)
        req.content_type = "text/xml"
        encoding = req.headers_in.get('Accept-encoding', '')
        if len(resp) > 200 and 'zlib' in encoding:
            req.headers_out['Content-encoding'] = 'zlib'
            resp = zlib.compress(resp, 5)
        req.write(resp)
        return apache.OK
    else:
        from http import HttpHandler
        httpHandler = HttpHandler(req, repos.cfg, repos, protocol, port) 
        return httpHandler._methodHandler()

def get(port, isSecure, repos, req):
    uri = req.uri
    if uri.endswith('/'):
        uri = uri[:-1]
    cmd = os.path.basename(uri)
    fields = util.FieldStorage(req)

    authToken = getAuth(req)
    if authToken[0] != "anonymous" and not isSecure and repos.forceSecure:
        return apache.HTTP_FORBIDDEN
   
    if cmd == "changeset":
        localName = repos.tmpPath + "/" + req.args + "-out"
        size = os.stat(localName).st_size

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

        req.content_type = "application/x-conary-change-set"
        for (path, size) in items:
            req.sendfile(path)

            if path.startswith(repos.tmpPath) and \
                    not(os.path.basename(path)[0:6].startswith('cache-')):
                os.unlink(path)

        return apache.OK
    else:
        from http import HttpHandler

        if isSecure:
            protocol = "https"
        else:
            protocol = "http"

        httpHandler = HttpHandler(req, repos.cfg, repos, protocol, port)
        return httpHandler._methodHandler()

def putFile(port, isSecure, repos, req):
    if not isSecure and repos.forceSecure:
        return apache.HTTP_FORBIDDEN

    path = repos.tmpPath + "/" + req.args + "-in"
    size = os.stat(path).st_size
    if size != 0:
	return apache.HTTP_UNAUTHORIZED

    f = open(path, "w+")
    s = req.read(BUFFER)
    while s:
	f.write(s)
	s = req.read(BUFFER)

    f.close()

    return apache.OK

def writeTraceback(wfile, cfg):
    kid_error.write(wfile, cfg = cfg, pageTitle = "Error",
                           error = traceback.format_exc())

def handler(req):
    repName = req.filename
    if not repositories.has_key(repName):
        cfg = ServerConfig()
        cfg.read(req.filename)

	if os.path.basename(req.uri) == "changeset":
	   rest = os.path.dirname(req.uri) + "/"
	else:
	   rest = req.uri

	rest = req.uri
	# pull out any queryargs
	if '?' in rest:
	    rest = req.uri.split("?")[0]

	# and throw away any subdir portion
	rest = req.uri[:-len(req.path_info)] + '/'
        
	urlBase = "%%(protocol)s://%s:%%(port)d" % \
                        (req.server.server_hostname) + rest

        if not cfg.repositoryDir:
            print "error: repositoryDir is required in %s" % req.filename
            return
        elif not cfg.serverName:
            print "error: serverName is required in %s" % req.filename
            return

	repositories[repName] = netserver.NetworkRepositoryServer(
                                cfg.repositoryDir,
                                cfg.tmpDir,
				urlBase, 
                                cfg.serverName,
                                cfg.repositoryMap,
				commitAction = cfg.commitAction,
                                cacheChangeSets = cfg.cacheChangeSets,
                                logFile = cfg.logFile)

	repositories[repName].forceSecure = cfg.forceSSL
        repositories[repName].cfg = cfg

    port = req.connection.local_addr[1]
    secure =  (port == 443)
    
    repos = repositories[repName]
    method = req.method.upper()

    if method == "POST":
	return post(port, secure, repos, req)
    elif method == "GET":
	return get(port, secure, repos, req)
    elif method == "PUT":
	return putFile(port, secure, repos, req)
    else:
	return apache.HTTP_METHOD_NOT_ALLOWED

repositories = {}
