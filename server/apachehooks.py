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

from mod_python import apache
from mod_python import util
import base64
import os
import xmlrpclib
import zlib

from repository.netrepos import netserver
from http import HttpHandler
from htmlengine import HtmlEngine
import conarycfg

BUFFER=1024 * 256

class ServerConfig(conarycfg.ConfigFile):

    defaults = {
        'authDatabase'      :  None,
        'commitAction'      :  None,
        'repositoryMap'     :  [ conarycfg.STRINGDICT, {} ],
        'repositoryDir'     :  None,
        'serverName'        :  None,
        'tmpDir'            :  "/var/tmp",
        'cacheChangeSets'   :  [ conarycfg.BOOLEAN, False ],
    }

def getAuth(req, repos):
    if not 'Authorization' in req.headers_in:
        return (None, None)

    info = req.headers_in['Authorization'].split()
    if len(info) != 2 or info[0] != "Basic":
        return apache.HTTP_BAD_REQUEST

    try:
        authString = base64.decodestring(info[1])
    except:
        return apache.HTTP_BAD_REQUEST

    if authString.count(":") != 1:
        return apache.HTTP_BAD_REQUEST
        
    authToken = authString.split(":")

    return authToken

def checkAuth(req, repos):
    if not req.headers_in.has_key('Authorization'):
        return None
    else:
        authToken = getAuth(req, repos)
        if type(authToken) != tuple:
            return authToken

        if not repos.auth.checkUserPass(authToken):
            return None
            
    return authToken

def post(repos, httpHandler, req):
    if req.headers_in['Content-Type'] == "text/xml":
        authToken = getAuth(req, repos)
        if type(authToken) is int:
            return authToken

        (params, method) = xmlrpclib.loads(req.read())

        try:
            result = repos.callWrapper(method, authToken, params)
        except netserver.InsufficientPermission:
            return apache.HTTP_FORBIDDEN

        resp = xmlrpclib.dumps((result,), methodresponse=1)
        req.content_type = "text/xml"
        encoding = req.headers_in.get('Accept-encoding', '')
        if len(resp) > 200 and 'zlib' in encoding:
            req.headers_out['Content-encoding'] = 'zlib'
            resp = zlib.compress(resp, 5)
        req.write(resp) 
    else:
        cmd = os.path.basename(req.uri)
        if httpHandler.requiresAuth(cmd):
            authToken = checkAuth(req, repos)
            if type(authToken) is int or authToken is None or authToken[0] is None:
                req.err_headers_out['WWW-Authenticate'] = \
                                    'Basic realm="Conary Repository"'
                return apache.HTTP_UNAUTHORIZED
        else:
            authToken = (None, None)
    
        req.content_type = "text/html"
        try:
            httpHandler.handleCmd(req.write, cmd, authToken,
                                  util.FieldStorage(req))
        except:
            traceback(req)

    return apache.OK

def get(repos, httpHandler, req):
    uri = req.uri
    if uri.endswith('/'):
        uri = uri[:-1]
    cmd = os.path.basename(uri)
    fields = util.FieldStorage(req)
   
    if cmd != "changeset":
	# we need to redo this with a trailing / for the root menu to work
	cmd = os.path.basename(req.uri)

        if httpHandler.requiresAuth(cmd):
            authToken = checkAuth(req, repos)
            if not authToken:
                req.err_headers_out['WWW-Authenticate'] = 'Basic realm="Conary Repository"'
                return apache.HTTP_UNAUTHORIZED
        else:
            authToken = (None, None)

        req.content_type = "text/html"
        try:
            httpHandler.handleCmd(req.write, cmd, authToken, fields)
        except:
            traceback(req)
        return apache.OK

    localName = repos.tmpPath + "/" + req.args + "-out"
    size = os.stat(localName).st_size

    if localName.endswith(".cf-out"):
        try:
            f = open(localName, "r")
        except IOError:
            self.send_error(404, "File not found")
            return None

        if req.args[0:6] != "cache-" or not repos.cacheChangeSets():
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
    req.sendfile(items[0][0])

    # erase single files
    if not localName.endswith(".cf-out") and \
           (req.args[0:6] != "cache-" or not repos.cacheChangeSets()):
        os.unlink(items[0][0])

    return apache.OK

def putFile(repos, req):
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

def traceback(wfile):
    htmlengine = HtmlEngine()
    htmlengine.setWriter(wfile.write)
    htmlengine.stackTrace(wfile)

def handler(req):
    repName = os.path.basename(req.filename)

    if not repositories.has_key(repName):
        cfg = ServerConfig()
        cfg.read(req.filename)

	if req.parsed_uri[apache.URI_PORT]:
	    port = req.parsed_uri[apache.URI_PORT]
	else:
	    port = 80

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

	urlBase = "http://%s:%d" % (req.server.server_hostname, port) + rest

        if not cfg.repositoryDir:
            print "error: repositoryDir is required in %s" % req.filename
            return
        elif not cfg.authDatabase:
            print "error: authDatabase is required in %s" % req.filename
            return
        elif not cfg.serverName:
            print "error: serverName is required in %s" % req.filename
            return

	repositories[repName] = netserver.NetworkRepositoryServer(
                                cfg.repositoryDir,
                                cfg.tmpDir,
				urlBase, 
                                cfg.authDatabase,
                                cfg.serverName,
                                cfg.repositoryMap,
				commitAction = cfg.commitAction,
                                cacheChangeSets = cfg.cacheChangeSets)
    
    repos = repositories[repName]
    httpHandler = HttpHandler(repos)
    
    method = req.method.upper()

    if method == "POST":
	return post(repos, httpHandler, req)
    elif method == "GET":
	return get(repos, httpHandler, req)
    elif method == "PUT":
	return putFile(repos, req)
    else:
	return apache.METHOD_NOT_ALLOWED

repositories = {}
