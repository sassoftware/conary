#
# Copyright (c) 2004-2006 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

from mod_python import apache
from mod_python.util import FieldStorage
import os
import sys
import time
import xmlrpclib
import zlib

from conary.lib import log
from conary.repository import changeset
from conary.repository import errors
from conary.repository.filecontainer import FileContainer
from conary.web.webauth import getAuth

BUFFER=1024 * 256

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
        # handle XML-RPC requests
        encoding = req.headers_in.get('Content-Encoding', None)
        try:
            data = req.read()
        except IOError, e:
            # if we got a read timeout, marshal an exception back
            # to the client
            print >> sys.stderr, 'error reading from client: %s' %e
            result = (False, True, ('ClientTimeout',
                                    'The server was not able to read the '
                                    'XML-RPC request sent by this client. '
                                    'This is sometimes caused by MTU problems '
                                    'on your network connection.  Using a '
                                    'smaller MTU may work around this '
                                    'problem.'))
            startTime = time.time()
        else:
            # otherwise, we've read the data, let's process it
            if encoding == 'deflate':
                data = zlib.decompress(data)

            startTime = time.time()
            (params, method) = xmlrpclib.loads(data)
            repos.log(3, "decoding=%s" % method, authToken[0],
                      "%.3f" % (time.time()-startTime))
            try:
                result = repos.callWrapper(protocol, port, method, authToken,
                                           params,
                                           remoteIp = req.connection.remote_ip)
            except errors.InsufficientPermission:
                return apache.HTTP_FORBIDDEN


        usedAnonymous = result[0]
        result = result[1:]

        resp = xmlrpclib.dumps((result,), methodresponse=1)
        repos.log(1, method, "time=%.3f size=%d" % (time.time()-startTime,
                                                    len(resp)))

        req.content_type = "text/xml"
        # check to see if the client will accept a compressed response
        encoding = req.headers_in.get('Accept-encoding', '')
        if len(resp) > 200 and 'deflate' in encoding:
            req.headers_out['Content-encoding'] = 'deflate'
            resp = zlib.compress(resp, 5)
        req.headers_out['Content-length'] = '%d' % len(resp)
        if usedAnonymous:
            req.headers_out["X-Conary-UsedAnonymous"] = "1"
        req.write(resp)
        return apache.OK
    else:
        # Handle HTTP (web browser) requests
        from conary.server.http import HttpHandler
        httpHandler = HttpHandler(req, repos.cfg, repos, protocol, port)
        return httpHandler._methodHandler()

def get(port, isSecure, repos, req):
    def _writeNestedFile(req, name, tag, size, f, sizeCb):
        if changeset.ChangedFileTypes.refr[4:] == tag[2:]:
            path = f.read()
            size = os.stat(path).st_size
            tag = tag[0:2] + changeset.ChangedFileTypes.file[4:]
            sizeCb(size, tag)
            req.sendfile(path)
        else:
            sizeCb(size, tag)
            req.write(f.read())

    uri = req.uri
    if uri.endswith('/'):
        uri = uri[:-1]
    cmd = os.path.basename(uri)
    fields = FieldStorage(req)

    authToken = getAuth(req)

    if type(authToken) is int:
        return authToken

    if authToken[0] != "anonymous" and not isSecure and repos.forceSecure:
        return apache.HTTP_FORBIDDEN

    if cmd == "changeset":
        if '/' in req.args:
            return apache.HTTP_FORBIDDEN

        localName = repos.tmpPath + "/" + req.args + "-out"

        if localName.endswith(".cf-out"):
            try:
                f = open(localName, "r")
            except IOError:
                return apache.HTTP_NOT_FOUND

            os.unlink(localName)

            items = []
            totalSize = 0
            for l in f.readlines():
                (path, size) = l.split()
                size = int(size)
                totalSize += size
                items.append((path, size))
            f.close()
            del f
        else:
            try:
                size = os.stat(localName).st_size;
            except OSError:
                return apache.HTTP_NOT_FOUND
            items = [ (localName, size) ]
            totalSize = size

        req.content_type = "application/x-conary-change-set"
        for (path, size) in items:
            if path.endswith('.ccs-out'):
                cs = FileContainer(open(path))
                try:
                    cs.dump(req.write,
                            lambda name, tag, size, f, sizeCb:
                                _writeNestedFile(req, name, tag, size, f,
                                                 sizeCb))
                except IOError, e:
                    log.error('IOError dumping changeset: %s' % e)

                del cs
            else:
                req.sendfile(path)

            if path.startswith(repos.tmpPath) and \
                    not(os.path.basename(path)[0:6].startswith('cache-')):
                os.unlink(path)

        return apache.OK
    else:
        from conary.server.http import HttpHandler

        if isSecure:
            protocol = "https"
        else:
            protocol = "http"

        httpHandler = HttpHandler(req, repos.cfg, repos, protocol, port)
        return httpHandler._methodHandler()

def putFile(port, isSecure, repos, req):
    if not isSecure and repos.forceSecure or '/' in req.args:
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
