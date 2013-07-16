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
from conary.repository import errors
from conary.repository import filecontainer
from conary.repository import netclient
from conary.repository import shimclient
from conary.repository import xmlshims
from conary.repository.netrepos import netserver
from conary.repository.netrepos import proxy
from conary.web import repos_web
from conary.web import webauth

log = logging.getLogger('wsgi_hooks')


def makeApp(settings):
    """Paster entry point"""
    envOverrides = {}
    if 'conary_config' in settings:
        envOverrides['conary.netrepos.config_file'] = settings['conary_config']
    if 'mount_point' in settings:
        envOverrides['conary.netrepos.mount_point'] = settings['mount_point']
    app = ConaryRouter(envOverrides)
    return app


def paster_main(global_config, **settings):
    """Wrapper to enable "paster serve" """
    cny_log.setupLogging(consoleLevel=logging.INFO, consoleFormat='apache')
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
                'use_forwarded_headers', False)
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
        if cfg.closed:
            # Closed repository -- returns an exception for all requests
            self.repositoryServer = netserver.ClosedRepositoryServer(cfg)
            self.shimServer = self.repositoryServer
        elif cfg.proxyContentsDir:
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
        self.auth = netserver.AuthToken()
        self._loadAuthPassword()
        self._loadAuthEntitlement()
        self.auth.remote_ip = self.request.remote_addr
        if self._useForwardedHeaders:
            forward = self.request.headers.get('X-Forwarded-For')
            if forward:
                self.auth.remote_ip = forward.split(',')[-1].strip()

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
        web = repos_web.ReposWeb(self.cfg, self.shimServer)
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

        localAddr = socket.gethostname()
        try:
            request = self.requestFilter.fromWire(params)
        except (TypeError, ValueError, IndexError):
            return self._makeError('400 Bad Request',
                    "Malformed XMLRPC arguments")

        # Execution phase -- locate and call the target method
        try:
            response, extraInfo = self.proxyServer.callWrapper(
                    protocol=None,
                    port=None,
                    methodname=method,
                    authToken=self.auth,
                    request=request,
                    remoteIp=self.auth.remote_ip,
                    rawUrl=self.request.url,
                    localAddr=localAddr,
                    protocolString=self.request.http_version,
                    headers=self.request.headers,
                    isSecure=self.isSecure)
        except errors.InsufficientPermission:
            return self._makeError('403 Forbidden', "Insufficient permission")

        rawResponse, headers = response.toWire(request.version)
        response = self.responseFactory(
                headerlist=headers.items(),
                content_type='text/xml',
                )

        # Output phase -- serialize and write the response
        body = util.xmlrpcDump((rawResponse,), methodresponse=1)
        accept = self.request.accept_encoding
        if len(body) > 200 and 'deflate' in accept:
            response.content_encoding = 'deflate'
            response.body = zlib.compress(body, 5)
        else:
            response.body = body
        if extraInfo:
            headers['Via'] = proxy.formatViaHeader(localAddr,
                    self.request.http_version, prefix=extraInfo.getVia())

        return response

    def getChangeset(self):
        """GET a prepared changeset file."""
        # IMPORTANT: As used here, "expandedSize" means the size of the
        # changeset as it is sent over the wire. The size of the file we are
        # reading from may be different if it includes references to other
        # files in lieu of their actual contents.
        path = self._changesetPath('-out')
        if not path:
            return self._makeError('403 Forbidden',
                    "Illegal changeset request")

        items = []
        totalSize = 0

        # TODO: incorporate the improved logic here into
        # proxy.ChangesetFileReader and consume it here.

        if path.endswith('.cf-out'):
            # Manifest of files to send sequentially (file contents or cached
            # changesets). Some of these may live outside of the tmpDir and
            # thus should not be unlinked afterwards.
            try:
                manifest = open(path, 'rt')
            except IOError, err:
                if err.errno == errno.ENOENT:
                    return self._makeError('404 Not Found',
                            "Changeset not found")
                raise
            os.unlink(path)

            for line in manifest:
                path, expandedSize, isChangeset, preserveFile = line.split()
                expandedSize = int(expandedSize)
                isChangeset = bool(int(isChangeset))
                preserveFile = bool(int(preserveFile))

                items.append((path, isChangeset, preserveFile))
                totalSize += expandedSize

            manifest.close()

        else:
            # Single prepared file. Always in tmpDir, so always unlink
            # afterwards.
            try:
                fobj = open(path, 'rb')
            except IOError, err:
                if err.errno == errno.ENOENT:
                    return self._makeError('404 Not Found',
                            "Changeset not found")
                raise
            expandedSize = os.fstat(fobj.fileno()).st_size
            items.append((path, False, False))
            totalSize += expandedSize

        return self.responseFactory(
                status='200 OK',
                app_iter=self._produceChangeset(items),
                content_type='application/x-conary-change-set',
                content_length=str(totalSize),
                )

    def _produceChangeset(self, items):
        readNestedFile = proxy.ChangesetFileReader.readNestedFile
        for path, isChangeset, preserveFile in items:
            if isChangeset:
                csFile = util.ExtendedFile(path, 'rb', buffering=False)
                changeSet = filecontainer.FileContainer(csFile)
                for data in changeSet.dumpIter(readNestedFile,
                        args=(self.contentsStore,)):
                    yield data
                del changeSet
            else:
                fobj = open(path, 'rb')
                for data in util.iterFileChunks(fobj):
                    yield data
                fobj.close()

            if not preserveFile:
                os.unlink(path)

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

    def _changesetPath(self, suffix):
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
