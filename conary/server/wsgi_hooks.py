#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import errno
import itertools
import logging
import os
import smtplib
import socket
import StringIO
import sys
import tempfile
import time
import webob
import zlib
from email import MIMEText
from webob import exc as web_exc

from conary.lib import log as cny_log
from conary.lib import util
from conary.lib.formattrace import formatTrace
from conary.lib.http.request import URL
from conary.repository import errors
from conary.repository import netclient
from conary.repository import shimclient
from conary.repository import xmlshims
from conary.repository.netrepos import netserver
from conary.repository.netrepos import proxy
from conary.repository.netrepos.auth_tokens import AuthToken
from conary.web import repos_web
from conary.web import webauth

log = logging.getLogger('wsgi_hooks')


def makeApp(settings):
    """Paster entry point"""
    envOverrides = {}
    if 'conary_config' in settings:
        envOverrides['conary.netrepos.config_file'] = settings['conary_config']
    elif 'CONARY_SERVER_CONFIG' in os.environ:
        envOverrides['conary.netrepos.config_file'
                ] = os.environ['CONARY_SERVER_CONFIG']
    if 'mount_point' in settings:
        envOverrides['conary.netrepos.mount_point'] = settings['mount_point']
    app = ConaryRouter(envOverrides)
    return app


def paster_main(global_config, **settings):
    """Wrapper to enable "paster serve" """
    return makeApp(settings)


def application(environ, start_response):
    """Trivial app entry point"""
    return makeApp({})(environ, start_response)


class ConaryRouter(object):

    requestFactory = webob.Request
    responseFactory = webob.Response

    def __init__(self, envOverrides=()):
        self.envOverrides = envOverrides
        self.configCache = {}

    def __call__(self, environ, start_response):
        if not logging.root.handlers:
            cny_log.setupLogging(consoleLevel=logging.INFO,
                    consoleFormat='apache',
                    consoleStream=environ['wsgi.errors'])
        # gunicorn likes to umask(0) when daemonizing, so put back something
        # reasonable if that's the case.
        oldUmask = os.umask(022)
        if oldUmask != 0:
            os.umask(oldUmask)
        environ.update(self.envOverrides)
        request = self.requestFactory(environ)
        try:
            response = self.handleRequest(request, start_response)
            if callable(response):
                # Looks like a webob response
                return response(environ, start_response)
            else:
                # Looks like a vanilla WSGI iterable
                return response
        except:
            exc_info = sys.exc_info()
            return self.handleError(request, exc_info, start_response)

    def handleRequest(self, request, start_response):
        if 'conary.netrepos.config_file' in request.environ:
            cfgPath = request.environ['conary.netrepos.config_file']
        else:
            raise ConfigurationError("conary.netrepos.config_file must be "
                    "present in the WSGI environment")
        cfg = self.configCache.get(cfgPath)
        if cfg is None:
            cfg = netserver.ServerConfig()
            cfg.read(cfgPath)
        handler = ConaryHandler(cfg)
        try:
            return handler.handleRequest(request)
        except:
            exc_info = sys.exc_info()
            return self.handleError(request, exc_info, start_response, cfg)

    def handleError(self, request, exc_info, start_response, cfg=None):
        trace, tracePath = self._formatErrorLarge(request, exc_info)
        short = self._formatErrorSmall(request, exc_info)
        short += 'Extended traceback at ' + tracePath
        log.error(short)

        if cfg and cfg.bugsFromEmail and cfg.bugsToEmail:
            try:
                self._sendMail(cfg, exc_info, trace, request)
            except:
                log.exception("Failed to send traceback mail:")

        response = self.responseFactory(
                "<h1>500 Internal Server Error</h1>\n"
                "<p>An unexpected error occurred on the server. Consult the "
                "server error logs for details.",
            status='500 Internal Server Error',
            content_type='text/html')
        # webob doesn't support exc_info, unfortunately
        start_response(response.status, response.headerlist, exc_info)
        return [response.body]

    def _formatErrorLarge(self, request, exc_info):
        e_class, e_value, e_tb = exc_info
        timestamp = time.ctime(time.time())

        # Format large traceback to file
        fd, tbPath = tempfile.mkstemp('.txt', 'repos-error-')
        tb = os.fdopen(fd, 'w+')
        print >> tb, "Unhandled exception from Conary repository", request.host
        print >> tb, "Time of occurence:", timestamp
        print >> tb, "System hostname:", socket.gethostname()
        print >> tb, "See also:", tbPath
        print >> tb
        formatTrace(e_class, e_value, e_tb, stream=tb, withLocals=False)
        print >> tb
        print >> tb, "WSGI Environment:"
        for key, value in sorted(request.environ.items()):
            print >> tb, " %s = %r" % (key, value)
        print >> tb
        print >> tb, "Full trace:"
        try:
            formatTrace(e_class, e_value, e_tb, stream=tb, withLocals=True)
        except:
            print >> tb, "*** Traceback formatter crashed! ***"
            print >> tb, "Formatter crash follows:"
            new_exc = sys.exc_info()
            formatTrace(new_exc[0], new_exc[1], new_exc[2], stream=tb,
                    withLocals=False)
            print >> tb, "*** End formatter crash log ***"
        print >> tb
        print >> tb, "End of traceback report"
        tb.seek(0)
        contents = tb.read()
        tb.close()
        return contents, tbPath

    def _formatErrorSmall(self, request, exc_info):
        e_class, e_value, e_tb = exc_info
        tb = StringIO.StringIO()
        print >> tb, "Unhandled exception from Conary repository", request.host
        formatTrace(e_class, e_value, e_tb, stream=tb, withLocals=False)
        return tb.getvalue()

    def _sendMail(self, cfg, exc_info, trace, request):
        firstLine = '%s: %s' % (exc_info[0].__name__, str(exc_info[1]))
        firstLine = firstLine.splitlines()[0]
        crashVars = dict(
                hostname=socket.gethostname(),
                firstLine=firstLine,
                )
        msg = MIMEText.MIMEText(trace)
        msg['Subject'] = cfg.bugsEmailSubject % crashVars
        msg['From'] = fromEmail = '"%s" <%s>' % (cfg.bugsEmailName,
                cfg.bugsFromEmail)
        msg['To'] = toEmail = '<%s>' % (cfg.bugsToEmail,)

        smtp = smtplib.SMTP()
        smtp.connect()
        smtp.sendmail(fromEmail, [toEmail], msg.as_string())
        smtp.close()


class ConaryHandler(object):

    requestFilter = xmlshims.RequestArgs
    responseFactory = webob.Response

    def __init__(self, cfg):
        self.cfg = cfg

        self.request = None
        self.auth = None
        self.isSecure = None
        self.repositoryServer = None
        self.proxyServer = None
        self.restHandler = None
        self.contentsStore = None

    def _getEnvBool(self, key, default=None):
        value = self.request.environ.get(key)
        if value is None:
            if default is None:
                raise KeyError("Environment variable %r must be set" % (key,))
            else:
                return default
        if value.lower() in ('yes', 'y', 'true', 't', '1', 'on'):
            return True
        elif value.lower() in ('no', 'n', 'false', 'f', '0', 'off'):
            return False
        else:
            raise ValueError(
                    "Environment variable %r must be a boolean, not %r" % (key,
                        value))

    def _loadCfg(self):
        """Load configuration and construct repository objects."""
        cfg = self.cfg
        req = self.request
        if cfg.repositoryDB:
            if cfg.proxyContentsDir:
                raise ConfigurationError("Exactly one of repositoryDB or "
                        "proxyContentsDir must be set.")
            for name in ('contentsDir', 'serverName'):
                if not cfg[name]:
                    raise ConfigurationError("%s must be set." % name)
        else:
            if not cfg.proxyContentsDir:
                raise ConfigurationError("Exactly one of repositoryDB or "
                        "proxyContentsDir must be set.")

        if os.path.realpath(cfg.tmpDir) != cfg.tmpDir:
            raise ConfigurationError("tmpDir must not contain symbolic links.")

        self._useForwardedHeaders = self._getEnvBool(
                'use_forwarded_headers', True)
        if self._useForwardedHeaders:
            for key in ('x-forwarded-scheme', 'x-forwarded-proto'):
                if req.headers.get(key):
                    req.scheme = req.headers[key]
                    break
            for key in ('x-forwarded-host', 'x-forwarded-server'):
                if req.headers.get(key):
                    req.host = req.headers[key]
                    break
        self.isSecure = req.scheme == 'https'

        if req.environ.get('PYTHONPATH'):
            # Allow SetEnv to propagate, so that commit hooks can have the
            # proper environment
            os.environ['PYTHONPATH'] = req.environ['PYTHONPATH']

        for mountPoint in [
                req.environ.get('conary.netrepos.mount_point'),
                cfg.baseUri, 'conary']:
            if mountPoint is not None:
                break
        for elem in mountPoint.split('/'):
            if not elem:
                continue
            if self.request.path_info_pop() != elem:
                raise web_exc.HTTPNotFound(
                        "Path %s is not handled by this application."
                        % self.request.script_name)

        urlBase = req.application_url
        if cfg.proxyContentsDir:
            # Caching proxy (no repository)
            self.repositoryServer = None
            self.shimServer = None
            self.proxyServer = proxy.ProxyRepositoryServer(cfg, urlBase)
        else:
            # Full repository with optional changeset cache
            self.repositoryServer = netserver.NetworkRepositoryServer(cfg,
                    urlBase)
            self.shimServer = shimclient.NetworkRepositoryServer(cfg, urlBase,
                    db=self.repositoryServer.db)

        if self.repositoryServer:
            self.proxyServer = proxy.SimpleRepositoryFilter(cfg, urlBase,
                    self.repositoryServer)
            self.contentsStore = self.repositoryServer.repos.contentsStore

    def _loadAuth(self):
        """Extract authentication info from the request."""
        self.auth = AuthToken()
        self._loadAuthPassword()
        self._loadAuthEntitlement()
        self.setRemoteIp(self.auth, self.request, self._useForwardedHeaders)

    @staticmethod
    def setRemoteIp(authToken, request, useForwarded=False):
        remote_ip = request.remote_addr
        forward = request.headers.get('X-Forwarded-For')
        if forward:
            forward = forward.split(',')
            if useForwarded:
                remote_ip = forward[-1].strip()
                forward = forward[:-1]
            authToken.forwarded_for = []
            for addr in forward:
                addr = addr.strip()
                if addr.startswith('::ffff:'):
                    addr = addr[7:]
                authToken.forwarded_for.append(addr)
        if remote_ip.startswith('::ffff:'):
            remote_ip = remote_ip[7:]
        authToken.remote_ip = remote_ip

    def _loadAuthPassword(self):
        """Extract HTTP Basic Authorization from the request."""
        info = self.request.authorization
        if not info or len(info) != 2 or info[0] != 'Basic':
            return
        try:
            info = info[1].decode('base64')
        except:
            return
        if ':' in info:
            self.auth.user, self.auth.password = info.split(':', 1)

    def _loadAuthEntitlement(self):
        """Extract conary entitlements from the request headers."""
        info = self.request.headers.get('X-Conary-Entitlement')
        if not info:
            return
        self.auth.entitlements = webauth.parseEntitlement(info)

    def _makeError(self, status, *lines):
        log.error("%s: %s: %s", self.auth.remote_ip, str(status), lines[0])
        body = "ERROR: " + "\r\n".join(lines) + "\r\n"
        return self.responseFactory(
                body=body,
                status=status,
                content_type='text/plain',
                )

    def getLocalPort(self):
        env = self.request.environ
        if 'gunicorn.socket' in env:
            return env['gunicorn.socket'].getsockname()[1]
        else:
            return 0

    def handleRequest(self, request):
        try:
            return self._handleRequest(request)
        except web_exc.HTTPException, err:
            return err
        finally:
            # This closes the repository server immediately after the initial
            # request handling phase, meaning that 'generator' responses will
            # not have access to it. Currently the only generator is
            # _produceChangeset() which does not need a repository server.
            self.close()

    def _handleRequest(self, request):
        self.request = request
        self._loadCfg()
        self._loadAuth()

        self.proxyServer.log.reset()

        if (self.auth.user != 'anonymous'
                and not self.isSecure
                and self.cfg.forceSSL):
            return self._makeError('403 Secure Connection Required',
                    "Password authentication is not allowed over unsecured "
                    "connections")

        if self.repositoryServer:
            self.repositoryServer.reopen()

        if self.request.method == 'GET':
            # cmd is the last part of the path, ignoring all intermediate
            # elements. When proxying, the intermediate part could be anything
            # depending on where the real repository is mounted.
            # e.g. /conary or /repos/foo
            cmd = os.path.basename(self.request.path_info.rstrip('/'))
            path = self.request.path_info_peek()
            if cmd == 'changeset':
                return self.getChangeset()
            elif path == 'api':
                self.request.path_info_pop()
                return self.getApi()
            # Fall through to web handler
        elif self.request.method == 'POST':
            # Only check content-type because of proxying considerations; as
            # above, the full URL will vary.
            if self.request.content_type == 'text/xml':
                return self.postRpc()
            # Fall through to web handler
        elif self.request.method == 'PUT':
            return self.putChangeset()
        else:
            return self._makeError('501 Not Implemented',
                    "Unsupported method %s" % self.request.method,
                    "Supported methods: GET POST PUT")

        if not self.repositoryServer:
            return self._makeError('404 Not Found',
                    "This is a Conary proxy server, it has no web interface.")
        if not self.cfg.webEnabled:
            return self._makeError('404 Not Found',
                    "Web interface disabled by administrator.")
        web = repos_web.ReposWeb(self.cfg, self.shimServer, authToken=self.auth)
        return web._handleRequest(request)

    def postRpc(self):
        if self.request.content_type != 'text/xml':
            return self._makeError('400 Bad Request',
                    "Unrecognized Content-Type")
        stream = self.request.body_file
        encoding = self.request.headers.get('Content-Encoding', 'identity')
        if encoding == 'deflate':
            stream = util.decompressStream(stream)
            stream.seek(0)
        elif encoding != 'identity':
            return self._makeError('400 Bad Request',
                    "Unrecognized Content-Encoding")

        try:
            params, method = util.xmlrpcLoad(stream)
        except:
            return self._makeError('400 Bad Request',
                    "Malformed XMLRPC request")

        localAddr = '%s:%s' % (socket.gethostname(), self.getLocalPort())
        try:
            request = self.requestFilter.fromWire(params)
        except (TypeError, ValueError, IndexError):
            return self._makeError('400 Bad Request',
                    "Malformed XMLRPC arguments")

        rawUrl = self.request.url
        scheme = self.request.headers.get('X-Conary-Proxy-Target-Scheme')
        if scheme in ('http', 'https'):
            rawUrl = str(URL(rawUrl)._replace(scheme=scheme))

        # Execution phase -- locate and call the target method
        try:
            responseArgs, extraInfo = self.proxyServer.callWrapper(
                    protocol=None,
                    port=None,
                    methodname=method,
                    authToken=self.auth,
                    request=request,
                    remoteIp=self.auth.remote_ip,
                    rawUrl=rawUrl,
                    localAddr=localAddr,
                    protocolString=self.request.http_version,
                    headers=self.request.headers,
                    isSecure=self.isSecure)
        except errors.InsufficientPermission:
            return self._makeError('403 Forbidden', "Insufficient permission")

        rawResponse, headers = responseArgs.toWire(request.version)
        if extraInfo:
            headers['Via'] = proxy.formatViaHeader(localAddr,
                    self.request.http_version, prefix=extraInfo.getVia())
        response = self.responseFactory(headerlist=headers.items())
        response.content_type = 'text/xml'

        # Output phase -- serialize and write the response
        body = util.xmlrpcDump((rawResponse,), methodresponse=1)
        accept = self.request.accept_encoding
        if len(body) > 200 and 'deflate' in accept:
            response.content_encoding = 'deflate'
            response.body = zlib.compress(body, 5)
        else:
            response.body = body

        if (method == 'getChangeSet'
                and request.version >= 71
                and not responseArgs.isException
                and response.status_int == 200
                and responseArgs.result[0]
                and 'multipart/mixed' in list(self.request.accept)
                ):
            return self.inlineChangeset(response, responseArgs, headers)
        else:
            return response

    def getChangeset(self, filename=None):
        """GET a prepared changeset file."""
        path = self._changesetPath('-out', filename)
        if not path:
            return self._makeError('403 Forbidden',
                    "Illegal changeset request")
        try:
            producer = proxy.ChangesetProducer(path, self.contentsStore)
        except IOError as err:
            if err.args[0] == errno.ENOENT:
                return self._makeError('404 Not Found', "Changeset not found")
            raise
        return self.responseFactory(
                status='200 OK',
                app_iter=producer,
                content_type='application/x-conary-change-set',
                content_length=str(producer.getSize()),
                )

    def inlineChangeset(self, rpcResponse, responseArgs, headers):
        filename = responseArgs.result[0].split('?')[-1]
        csResponse = self.getChangeset(filename=filename)
        if csResponse.status_int != 200:
            return csResponse

        # Build a multipart MIME response from the two responses
        boundary = os.urandom(24).encode('hex')
        totalSize = 0
        iterables = []
        for response in [rpcResponse, csResponse]:
            leader = "--%s\r\n" % boundary
            for name in ['Content-Type', 'Content-Length', 'Content-Encoding']:
                if name in response.headers:
                    leader += "%s: %s\r\n" % (name, response.headers[name])
            leader += "\r\n"
            trailer = "\r\n"
            totalSize += len(leader) + response.content_length + len(trailer)
            iterables.extend([[leader], response.app_iter, [trailer]])
        final = "--%s--\r\n" % boundary
        totalSize += len(final)
        iterables.append([final])

        response = self.responseFactory(
                status='200 OK',
                headerlist=headers.items(),
                app_iter=itertools.chain.from_iterable(iterables),
                )
        response.content_type = 'multipart/mixed; boundary="%s"' % boundary
        response.content_length=str(totalSize)
        return response

    def putChangeset(self):
        """PUT method -- handle changeset uploads."""
        if not self.repositoryServer:
            # FIXME: this mechanism is unauthenticated and can probably be used
            # to PUT content to random things on the internet
            if 'content-length' in self.request.headers:
                size = int(self.request.headers['content-length'])
            else:
                size = None
            headers = [x for x in self.request.headers.items()
                    if x[0].lower() in (
                        'x-conary-servername',
                        'x-conary-entitlement',
                        )]
            result = netclient.httpPutFile(self.request.url,
                    self.request.body_file,
                    size,
                    headers=headers,
                    chunked=(size is None),
                    withResponse=True,
                    )
            return self.responseFactory(
                    status='%s %s' % (result.status, result.reason),
                    app_iter=self._produceProxy(result),
                    #headerlist=result.getheaders(),
                    )

        # Copy request body to the designated temporary file.
        stream = self.request.body_file
        out = self._openForPut()
        if out is None:
            # File already exists or is in an illegal location.
            return self._makeError('403 Forbidden', "Illegal changeset upload")

        util.copyfileobj(stream, out)
        out.close()

        return self.responseFactory(status='200 OK')

    @staticmethod
    def _produceProxy(response):
        while True:
            d = response.read(1024)
            if not d:
                break
            yield d
        response.close()

    def _changesetPath(self, suffix, filename=None):
        if not filename:
            filename = self.request.query_string
            if not filename or os.path.sep in filename:
                return None
        server = self.repositoryServer or self.proxyServer
        return os.path.join(server.tmpPath, filename + suffix)

    def _openForPut(self):
        path = self._changesetPath('-in')
        if path:
            try:
                st = os.stat(path)
            except OSError, err:
                if err.errno != errno.ENOENT:
                    return None
                raise
            if st.st_size == 0:
                return open(path, 'wb+')
        return None

    def getApi(self):
        if not self.repositoryServer:
            return self._makeError('404 Not Found',
                    "Standalone Conary proxies cannot forward API requests")
        try:
            from crest import webhooks
        except ImportError:
            return self._makeError('404 Not Found',
                    "Conary web API is not enabled on this repository")
        prefix = self.request.script_name
        restHandler = webhooks.WSGIHandler(prefix, self.repositoryServer,
                authToken=self.auth)
        return restHandler.handle(self.request, path=None)

    def close(self):
        # Make sure any pooler database connections are released.
        if self.repositoryServer:
            self.repositoryServer.close()
        self.request = None
        self.auth = None
        self.isSecure = None
        self.repositoryServer = None
        self.proxyServer = None
        self.restHandler = None
        # Leave the contentsStore around in case produceChangeset needs it


class ConfigurationError(RuntimeError):
    pass
