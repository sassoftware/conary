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

import netserver
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

def post(repos, req):
    if not req.headers_in.has_key('Authorization'):
	user = None
	pw = None
    else:
	info = req.headers_in['Authorization'].split()
	if len(info) != 2 or info[0] != "Basic":
	    return apache.HTTP_BAD_REQUEST

	try:
	    authString = base64.decodestring(info[1])
	except:
	    return apache.HTTP_BAD_REQUEST

	if authString.count(":") != 1:
	    return apache.HTTP_BAD_REQUEST
	    
	(user, pw) = authString.split(":")

    authToken = (user, pw)

    if req.headers_in['Content-Type'] == "text/xml":
        (params, method) = xmlrpclib.loads(req.read())

        try:
            result = repos.callWrapper(method, authToken, params)
        except netserver.InsufficientPermission:
            return apache.HTTP_FORBIDDEN

        resp = xmlrpclib.dumps((result,), methodresponse=1)
        req.content_type = "text/xml"
        req.write(resp) 
    else:
        req.content_type = "text/html"
        repos.handlePost(req.write, authToken, os.path.basename(req.uri), 
                         util.FieldStorage(req))

    return apache.OK

def getFile(repos, req):
    if os.path.basename(req.uri) != "changeset":
        req.content_type = "text/html"
        repos.handleGet(req.write, os.path.basename(req.uri))
        return apache.OK

    path = repos.tmpPath + "/" + req.args + "-out"
    size = os.stat(path).st_size
    req.content_type = "application/x-conary-change-set"
    req.sendfile(path)
    if req.args[0:6] != "cache-" or not repos.cacheChangeSets():
        os.unlink(path)
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

def handler(req):
    repName = os.path.basename(req.filename)

    if not repositories.has_key(repName):
        cfg = ServerConfig()
        cfg.read(req.filename)

	if req.parsed_uri[apache.URI_PORT]:
	    port = req.parsed_uri[apache.URI_PORT]
	else:
	    port = 80

	urlBase = "http://%s:%d" % (req.server.server_hostname, port) + req.uri

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

    method = req.method.upper()

    if method == "POST":
	return post(repos, req)
    elif method == "GET":
	return getFile(repos, req)
    elif method == "PUT":
	return putFile(repos, req)
    else:
	return apache.METHOD_NOT_ALLOWED

repositories = {}
