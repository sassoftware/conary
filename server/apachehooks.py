from mod_python import apache
import base64
import os
import xmlrpclib

import netserver

BUFFER=1024 * 256

def xmlPost(repos, req):
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

    (params, method) = xmlrpclib.loads(req.read())

    try:
	result = repos.__class__.__dict__[method](repos, authToken, *params)
    except netserver.InsufficientPermission:
	return apache.HTTP_FORBIDDEN

    resp = xmlrpclib.dumps((result,), methodresponse=1)
    req.content_type = "text/xml"
    req.write(resp) 

    return apache.OK

def getFile(repos, req):
    path = repos.tmpPath + "/" + req.args + "-out"
    size = os.stat(path).st_size
    req.content_type = "application/x-conary-change-set"
    req.sendfile(path)
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
    repName = os.path.dirname(req.filename)

    if not repositories.has_key(repName):
	codeStr = open(req.filename, "r").read()
	d = {}
	exec codeStr in d

	if req.parsed_uri[apache.URI_PORT]:
	    port = req.parsed_uri[apache.URI_PORT]
	else:
	    port = 80

	urlBase = "http://%s:%d" % (req.hostname, port)
	
	urlBase += req.uri

	if d.has_key('commitaction'):
	    commitAction = d['commitaction']
	else:
	    commitAction = None

	repositories[repName] = netserver.NetworkRepositoryServer(
				d['reppath'], d['tmppath'], 
				urlBase, d['authpath'],
				d['servername'],
				commitAction = commitAction)

    repos = repositories[repName]

    if req.method == "POST" and req.headers_in['Content-Type'] == "text/xml":
	return xmlPost(repos, req)
    elif req.method == "GET":
	return getFile(repos, req)
    elif req.method == "PUT":
	return putFile(repos, req)
    else:
	return apache.METHOD_NOT_ALLOWED

repositories = {}
