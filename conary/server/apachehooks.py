#
# Copyright (c) 2004-2006 rPath, Inc.
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
from mod_python.util import FieldStorage
from email import MIMEText
import os
import sys
import time
import smtplib
import traceback
import xmlrpclib
import zlib

from conary.lib import log
from conary.repository import changeset
from conary.repository import errors
from conary.repository.filecontainer import FileContainer
from conary.repository.netrepos import netserver
from conary.web.webauth import getAuth

BUFFER=1024 * 256

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
        encoding = req.headers_in.get('Content-Encoding', None)
        data = req.read()
        if encoding == 'deflate':
            data = zlib.decompress(data)

        (params, method) = xmlrpclib.loads(data)

        try:
            result = repos.callWrapper(protocol, port, method, authToken,
                                       params)
        except errors.InsufficientPermission:
            return apache.HTTP_FORBIDDEN

        usedAnonymous = result[0]
        result = result[1:]

        resp = xmlrpclib.dumps((result,), methodresponse=1)
        req.content_type = "text/xml"
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
    if authToken[0] != "anonymous" and not isSecure and repos.forceSecure:
        return apache.HTTP_FORBIDDEN

    if cmd == "changeset":
        if '/' in req.args:
            return apache.HTTP_FORBIDDEN

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
            f.close()
            del f
        else:
            size = os.stat(localName).st_size;
            items = [ (localName, size) ]
            totalSize = size

        req.content_type = "application/x-conary-change-set"
        for (path, size) in items:
            if path.endswith('.ccs-out'):
                cs = FileContainer(open(path))
                cs.dump(req.write,
                        lambda name, tag, size, f, sizeCb:
                            _writeNestedFile(req, name, tag, size, f,
                                             sizeCb))

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

def writeTraceback(wfile, cfg):
    kid_error.write(wfile, cfg = cfg, pageTitle = "Error",
                           error = traceback.format_exc())

def logErrorAndEmail(req, cfg, exception, e, bt):
    c = req.connection
    req.add_common_vars()
    info_dict = {
        'local_addr'     : c.local_ip + ':' + str(c.local_addr[1]),
        'remote_addr'    : c.remote_ip + ':' + str(c.remote_addr[1]),
        'remote_host'    : c.remote_host,
        'remote_logname' : c.remote_logname,
        'aborted'        : c.aborted,
        'keepalive'      : c.keepalive,
        'double_reverse' : c.double_reverse,
        'keepalives'     : c.keepalives,
        'local_host'     : c.local_host,
        'connection_id'  : c.id,
        'notes'          : c.notes,
        'the_request'    : req.the_request,
        'proxyreq'       : req.proxyreq,
        'header_only'    : req.header_only,
        'protocol'       : req.protocol,
        'proto_num'      : req.proto_num,
        'hostname'       : req.hostname,
        'request_time'   : time.ctime(req.request_time),
        'status_line'    : req.status_line,
        'status'         : req.status,
        'method'         : req.method,
        'allowed'        : req.allowed,
        'headers_in'     : req.headers_in,
        'headers_out'    : req.headers_out,
        'uri'            : req.uri,
        'unparsed_uri'   : req.unparsed_uri,
        'args'           : req.args,
        'parsed_uri'     : req.parsed_uri,
        'filename'       : req.filename,
        'subprocess_env' : req.subprocess_env,
        'referer'        : req.headers_in.get('referer', 'N/A')
    }
    info_dict_small = {
        'local_addr'     : c.local_ip + ':' + str(c.local_addr[1]),
        'uri'            : req.uri,
        'request_time'   : time.ctime(req.request_time),
    }

    timeStamp = time.ctime(time.time())

    # log error
    log.error('[%s] Unhandled exception from conary repository: %s: %s', 
              timeStamp, exception.__name__, e)
    log.error('sending mail to %s' % cfg.bugsToEmail)

    # send email
    body = 'Unhandled exception from conary repository:\n\n%s: %s\n\n' % (exception.__name__, e)
    body += 'Time of occurrence: %s\n\n' % timeStamp
    body += ''.join(traceback.format_tb(bt))
    body += '\nConnection Information:\n'
    for key, val in sorted(info_dict.items()):
        body += '\n' + key + ': ' + str(val)

    def sendMail(fromEmail, fromEmailName, toEmail, subject, body):
        msg = MIMEText.MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = '"%s" <%s>' % (fromEmailName, fromEmail)
        msg['To'] = toEmail

        s = smtplib.SMTP()
        s.connect()
        s.sendmail(fromEmail, [toEmail], msg.as_string())
        s.close()

    sendMail(cfg.bugsFromEmail, cfg.bugsEmailName, cfg.bugsToEmail, 
             cfg.bugsEmailSubject, body)

def handler(req):
    repName = req.filename
    if not repositories.has_key(repName):
        cfg = netserver.ServerConfig()
        cfg.read(req.filename)

        # Throw away any subdir portion.
        rest = req.uri[:-len(req.path_info)] + '/'

        urlBase = "%%(protocol)s://%s:%%(port)d" % \
                        (req.server.server_hostname) + rest

        if not cfg.repositoryDB:
            log.error("repositoryDB is required in %s" % req.filename)
            return apache.HTTP_INTERNAL_SERVER_ERROR
        elif not cfg.contentsDir:
            log.error("contentsDir is required in %s" % req.filename)
            return apache.HTTP_INTERNAL_SERVER_ERROR
        elif not cfg.serverName:
            log.error("serverName is required in %s" % req.filename)
            return apache.HTTP_INTERNAL_SERVER_ERROR

        if cfg.closed:
            repositories[repName] = netserver.ClosedRepositoryServer(cfg.closed)
        else:
            repositories[repName] = netserver.NetworkRepositoryServer(
                                                    cfg, urlBase)

            repositories[repName].forceSecure = cfg.forceSSL
            repositories[repName].cfg = cfg

    port = req.connection.local_addr[1]
    secure =  (port == 443)

    repos = repositories[repName]
    method = req.method.upper()

    try:
        if method == "POST":
            return post(port, secure, repos, req)
        elif method == "GET":
            return get(port, secure, repos, req)
        elif method == "PUT":
            return putFile(port, secure, repos, req)
        else:
            return apache.HTTP_METHOD_NOT_ALLOWED
    except:
        if cfg.bugsFromEmail and cfg.bugsToEmail:
            exception, e, bt = sys.exc_info()
            logErrorAndEmail(req, cfg, exception, e, bt)
        return apache.HTTP_INTERNAL_SERVER_ERROR
        

repositories = {}
