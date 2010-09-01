#
# Copyright (c) 2010 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.

import logging
import os
import xmlrpclib
from email.Message import Message

from conary.lib import log as cny_log
from conary.lib import util
from conary.repository import errors
from conary.repository.netrepos import netserver
from conary.repository.netrepos import proxy
from conary.web import webauth

try:
    from crest import webhooks as cresthooks
except ImportError:
    cresthooks = None  # pyflakes=ignore

log = logging.getLogger('wsgi_hooks')

_repository_cache = {}


class application(object):

    def __init__(self, environ, start_response):
        self.environ = environ
        self.start_response = start_response

        self.auth = None
        self.secure = None

        self.cfg = None
        self.repositoryServer = None
        self.proxyServer = None
        self.restHandler = None

        # TODO: figure out how to emit logs that aren't forcibly prefixed by
        # mod_wsgi. Maybe just start logging to a different place instead of
        # relying on httpd's error_log.
        cny_log.setupLogging(consoleLevel=logging.INFO,
                consoleFormat='apache_short')

        log.info("pid=%s cache=0x%x threaded=%s", os.getpid(),
                id(_repository_cache), environ['wsgi.multithread'])

        self._loadCfg()
        self._loadAuth()

    def _loadCfg(self):
        """Load configuration and construct repository objects."""
        cfgPath = self.environ.get('conary.netrepos.config_file')
        if not cfgPath:
            raise ConfigurationError("The conary.netrepos.config_file "
                    "environment variable must be set.")

        ino = util.statFile(cfgPath)
        cached = _repository_cache.get(cfgPath)
        if cached:
            cachedIno, repServer, proxyServer, restHandler = cached
            if ino == cachedIno:
                self.cfg = proxyServer.cfg
                self.repositoryServer = repServer
                self.proxyServer = proxyServer
                self.restHandler = restHandler
                return

        cfg = netserver.ServerConfig()
        cfg.read(cfgPath)

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

        if self.environ.get('HTTPS') == 'on':
            # Reverse proxies might forward via plain HTTP but still set the
            # HTTPS var
            scheme = 'https'
        else:
            scheme = self.environ['wsgi.url_scheme']
        self.secure = scheme == 'https'
        urlBase = '%s://%s%s' % (scheme,
                self.environ['HTTP_HOST'], self.environ['SCRIPT_NAME'])

        if cfg.closed:
            # Closed repository
            self.repositoryServer = netserver.ClosedRepositoryServer(cfg)
            self.restHandler = None
        elif cfg.proxyContentsDir:
            # Caching proxy
            self.repositoryServer = None
            self.proxyServer = proxy.ProxyRepositoryServer(cfg, urlBase)
            self.restHandler = None
        else:
            # Full repository with changeset cache
            self.repositoryServer = netserver.NetworkRepositoryServer(cfg,
                    urlBase)
            if cresthooks and cfg.baseUri:
                restUri = cfg.baseUri + '/api'
                self.restHandler = cresthooks.ApacheHandler(restUri,
                        self.repositoryServer)

        if self.repositoryServer:
            self.proxyServer = proxy.SimpleRepositoryFilter(cfg, urlBase,
                    self.repositoryServer)

        self.cfg = cfg
        # TODO: figure out how or what to cache, caching the whole thing is not
        # threadsafe since DB connections are stashed in repositoryServer.
        #_repository_cache[cfgPath] = (ino, self.repositoryServer,
        #        self.proxyServer, self.restHandler)

    def _loadAuth(self):
        self.auth = netserver.AuthToken()
        self._loadAuthPassword()
        self._loadAuthEntitlement()
        # FIXME: it's sort of insecure to just take the client's word for it
        forward = self.environ.get('HTTP_X_FORWARDED_FOR')
        if forward:
            self.auth.remote_ip = forward.split(',')[-1].strip()
        else:
            self.auth.remote_ip = self.environ.get('REMOTE_ADDR')

    def _loadAuthPassword(self):
        info = self.environ.get('HTTP_AUTHORIZATION')
        if not info:
            return
        info = info.split(' ', 1)
        if len(info) != 2 or info[0] != 'Basic':
            return
        try:
            info = info[1].decode('base64')
        except:
            return
        if ':' in info:
            self.auth.user, self.auth.password = info.split(':', 1)

    def _loadAuthEntitlement(self):
        info = self.environ.get('HTTP_X_CONARY_ENTITLEMENT')
        if not info:
            return
        self.auth.entitlements = webauth.parseEntitlement(info)

    def _getHeaders(self):
        """HTTP headers aren't actually RFC 2822, but it provides a convenient
        case-insensitive dictionary implementation.
        """
        out = Message()
        for key, value in self.environ.iteritems():
            if key[:5] != 'HTTP_':
                continue
            key = key[5:].lower().replace('_', '-')
            out[key] = value
        # These are displaced for some inane reason.
        if 'CONTENT_LENGTH' in self.environ:
            out['Content-Length'] = self.environ['CONTENT_LENGTH']
        if 'CONTENT_TYPE' in self.environ:
            out['Content-Type'] = self.environ['CONTENT_TYPE']
        return out

    def _response(self, status, body, headers=(), content_type='text/plain'):
        headers = list(headers)
        if content_type is not None:
            headers.append(('Content-type', content_type))
        self.start_response(status, headers)
        return body

    def _resp_iter(self, *args, **kwargs):
        return iter([self._response(*args, **kwargs)])

    def __iter__(self):
        """Do the actual request handling. Yields chunks of the response."""

        self.proxyServer.log.reset()

        if (self.auth.user != 'anonymous' and not self.secure
                and self.cfg.forceSSL):
            return self._resp_iter('403 Forbidden', "ERROR: Password "
                    "authentication is not allowed over unsecured "
                    "connections.\r\n")

        method = self.environ['REQUEST_METHOD']
        if method == 'POST':
            return self._iter_post()
        elif method == 'GET':
            return self._iter_get()
        elif method == 'PUT':
            return self._iter_put()
        else:
            return self._resp_iter('501 Not Implemented',
                    "ERROR: Unsupported method %s\r\n"
                    "Supported methods: GET POST PUT\r\n" % method)

    def _iter_post(self):
        """POST method -- handle XMLRPC requests"""

        # Input phase -- read and parse the XMLRPC request
        contentType = self.environ.get('CONTENT_TYPE')
        if contentType != 'text/xml':
            log.error("Unexpected content-type %r from %s", contentType,
                    self.auth.remote_ip)
            yield self._response('400 Bad Request',
                    "ERROR: Unrecognized Content-Type\r\n")
            return

        # TODO: pipeline
        stream = self.environ['wsgi.input']
        encoding = self.environ.get('HTTP_CONTENT_ENCODING')
        if encoding == 'deflate':
            stream = util.decompressStream(stream)
        elif encoding != 'identity':
            log.error("Unrecognized content-encoding %r from %s", encoding,
                    self.auth.remote_ip)
            yield self._response('400 Bad Request',
                    "ERROR: Unrecognized Content-Encoding\r\n")
            return

        stream.seek(0)
        try:
            params, method = util.xmlrpcLoad(stream)
        except (xmlrpclib.ResponseError, UnicodeDecodeError):
            log.error("Malformed XMLRPC request from %s", self.auth.remote_ip)
            yield self._response('400 Bad Request',
                    "ERROR: Malformed XMLRPC request\r\n")
            return

        localAddr = ':'.join((self.environ['SERVER_NAME'],
            self.environ['SERVER_PORT']))

        # Execution phase -- locate and call the target method
        try:
            result = self.proxyServer.callWrapper(
                    protocol=None,
                    port=None,
                    methodname=method,
                    authToken=self.auth,
                    args=params,
                    remoteIp=self.auth.remote_ip,
                    rawUrl=self.environ['REQUEST_URI'],
                    localAddr=localAddr,
                    protocolString=self.environ['SERVER_PROTOCOL'],
                    headers=self._getHeaders(),
                    isSecure=self.secure)
        except errors.InsufficientPermission:
            yield self._response('403 Forbidden',
                    "ERROR: Insufficient permissions.\r\n")
            return

        usedAnonymous, result, extraInfo = result[0], result[1:-1], result[-1]

        # Output phase -- serialize and write the response
        sio = util.BoundedStringIO()
        util.xmlrpcDump((result,), stream=sio, methodresponse=1)
        respLen = sio.tell()

        headers = [('Content-type', 'text/xml')]
        accept = self.environ.get('HTTP_ACCEPT_ENCODING', '')
        if respLen > 200 and 'deflate' in accept:
            headers.append(('Content-encoding', 'deflate'))
            sio.seek(0)
            sio = util.compressStream(sio, 5)
            respLen = sio.tell()
        headers.append(('Content-length', str(respLen)))

        if usedAnonymous:
            headers.append(('X-Conary-UsedAnonymous', '1'))
        if extraInfo:
            via = extraInfo.getVia()
            if via:
                headers.append(('Via', via))
            via = proxy.formatViaHeader(localAddr, 'HTTP/1.0')
            headers.append(('Via', via))

        self.start_response('200 OK', headers)

        sio.seek(0)
        while True:
            buf = sio.read(16384)
            if not buf:
                break
            yield buf

    def _iter_get(self):
        """GET method -- handle changeset and file contents downloads."""
        yield self._response('200 OK', 'wargh.\r\n')

    def _iter_put(self):
        """PUT method -- handle changeset uploads."""
        raise NotImplementedError

    def close(self):
        log.info("... closing")
        # Make sure any pooler database connections are released.
        if self.repositoryServer:
            self.repositoryServer.reset()


class ConfigurationError(RuntimeError):
    pass
