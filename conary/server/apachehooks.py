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
from email import MIMEText
import os
import sys
import time
import smtplib
import traceback

from conary.lib import coveragehook
from conary.lib import log
from conary.repository.netrepos import netserver
from conary.server.apachemethods import get, post, putFile

def writeTraceback(wfile, cfg):
    kid_error.write(wfile, cfg = cfg, pageTitle = "Error",
                           error = traceback.format_exc())

def formatRequestInfo(req):
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
    l = []
    for key, val in sorted(info_dict.items()):
        l.append('%s: %s' %(key, val))
    info = '\n'.join(l)
    return info

def logAndEmail(req, cfg, header, msg):
    timeStamp = time.ctime(time.time())

    log.error(header)
    if not cfg.bugsFromEmail or not cfg.bugsToEmail:
        return
    log.error('sending mail to %s' % cfg.bugsToEmail)

    # send email
    body = header + '\n'
    body += 'Time of occurrence: %s\n' % timeStamp
    body += 'Conary repository server: %s\n\n' % req.hostname
    body += msg + '\n'
    body += '\nConnection Information:\n'
    body += formatRequestInfo(req)

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

def logErrorAndEmail(req, cfg, exception, e, bt):
    timeStamp = time.ctime(time.time())

    header = 'Unhandled exception from conary repository:\n\n%s: %s\n\n' % (exception.__name__, e)
    msg = ''.join(traceback.format_tb(bt))
    logAndEmail(req, cfg, header, msg)
    # log error
    log.error(''.join(traceback.format_exception(*sys.exc_info())))

def handler(req):
    coveragehook.install()
    try:
        return _handler(req)
    finally:
        coveragehook.save()

def _handler(req):
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
            repositories[repName] = netserver.ClosedRepositoryServer(cfg)
            repositories[repName].forceSecure = False
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
    except apache.SERVER_RETURN:
        # if the exception was an apache server return code,
        # re-raise it and let mod_python handle it.
        raise
    except IOError, e:
        # ignore when the client hangs up on us
        if str(e).endswith('client closed connection.'):
            pass
        else:
            raise
    except:
        cfg = repos.cfg
        exception, e, bt = sys.exc_info()
        logErrorAndEmail(req, cfg, exception, e, bt)
        return apache.HTTP_INTERNAL_SERVER_ERROR


repositories = {}
