from mod_python import apache
import os
import xmlrpclib

import sys
sys.path.append("/home/ewt/srs")

import netserver

FILE_PATH="/tmp/conary-server"
BASE_URL="http://localhost/~ewt"
REP_PATH="/home/ewt/srs/srsrep"

BUFFER=1024 * 256

def xmlPost(req):
    (params, method) = xmlrpclib.loads(req.read())
    result = netRepos.__class__.__dict__[method](netRepos, *params)
    resp = xmlrpclib.dumps((result,), methodresponse=1)
    req.content_type = "text/xml"
    req.write(resp) 

    return apache.OK

def getFile(req):
    path = FILE_PATH + "/" + os.path.basename(req.filename) + "-out"
    size = os.stat(path).st_size
    req.content_type = "application/x-conary-change-set"
    req.sendfile(path)
    os.unlink(path)
    return apache.OK

def putFile(req):
    path = FILE_PATH + "/" + os.path.basename(req.filename) + "-in"
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
    if req.method == "POST" and req.headers_in['Content-Type'] == "text/xml":
	return xmlPost(req)
    elif req.method == "GET":
	return getFile(req)
    elif req.method == "PUT":
	return putFile(req)
    else:
	return apache.METHOD_NOT_ALLOWED

netRepos = netserver.NetworkRepositoryServer(REP_PATH, FILE_PATH, BASE_URL)
