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

import errno
import logging
import os
import xmlrpclib
from email.Message import Message

from conary.lib import log as cny_log
from conary.lib import util
from conary.repository import errors
from conary.repository import filecontainer
from conary.repository.netrepos import netserver
from conary.repository.netrepos import proxy
from conary.server.server import _readNestedFile, _iterFileChunks
from conary.web import webauth

log = logging.getLogger('wsgi_hooks')

_config_cache = {}


class application(object):

    def __init__(self, environ, start_response):
        self.environ = environ
        self.start_response = start_response

        self.auth = None
        self.isSecure = None
        self.urlBase = None

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
                id(_config_cache), environ['wsgi.multithread'])

        self._loadCfg()
        self._loadAuth()

    def _loadCfg(self):
        """Load configuration and construct repository objects."""
        cfgPath = self.environ.get('conary.netrepos.config_file')
        if not cfgPath:
            raise ConfigurationError("The conary.netrepos.config_file "
                    "environment variable must be set.")

        # Check for a cached configuration object. If the mtime has changed,
        # reload.
        ino = util.statFile(cfgPath)
        cached = _config_cache.get(cfgPath)
        cfg = None
        if cached:
            cachedIno, cachedCfg = cached
            if ino == cachedIno:
                cfg = cachedCfg

        if cfg is None:
            cfg = netserver.ServerConfig()
            cfg.read(cfgPath)
            _config_cache[cfgPath] = (ino, cfg)

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
        self.isSecure = scheme == 'https'

        # Build a base URL for returned Location headers, etc. Note that this
        # uses neither the configured baseUri nor the placeholder variables
        # used by apachehooks -- we can get all the information we need to
        # construct an absolute URL from the request alone.
        #   Why not use PATH_INFO here? Because that works for mod_wsgi, which
        # sets SCRIPT_NAME as the root and PATH_INFO as the subpath, but not
        # nginx where we have to set SCRIPT_NAME ourselves and PATH_INFO
        # contains the full path.
        hostUrl = '%s://%s' % (scheme, self.environ['HTTP_HOST'])
        relPath = self.environ['REQUEST_URI']
        scriptName = self.environ.get('SCRIPT_NAME')
        if scriptName is None:
            raise ConfigurationError("SCRIPT_NAME must be set to the relative "
                    "URL path where Conary is mounted.")
        if scriptName[-1] != '/':
            scriptName += '/'
        if len(relPath) < len(scriptName) and relPath[-1] != '/':
            # /conary is OK where SCRIPT_NAME=/conary/
            relPath += '/'
        if not relPath.startswith(scriptName):
            raise ConfigurationError("Request URI is %r but it is outside "
                    "the SCRIPT_NAME %r" % (relPath, scriptName))
        # http://somehost:8080/conary/
        self.urlBase = hostUrl + scriptName
        # http://somehost:8080/conary/changeset/?wxyz
        self.rawUrl = hostUrl + relPath
        # changeset/
        self.pathInfo = relPath[len(scriptName):].split('?')[0]

        if cfg.closed:
            # Closed repository -- returns an exception for all requests
            self.repositoryServer = netserver.ClosedRepositoryServer(cfg)
            self.restHandler = None
        elif cfg.proxyContentsDir:
            # Caching proxy (no repository)
            self.repositoryServer = None
            self.proxyServer = proxy.ProxyRepositoryServer(cfg, self.urlBase)
            self.restHandler = None
        else:
            # Full repository with optional changeset cache
            self.repositoryServer = netserver.NetworkRepositoryServer(cfg,
                    self.urlBase)
            # TODO: need restlib and crest work to support WSGI
            #if cresthooks and cfg.baseUri:
            #    restUri = cfg.baseUri + '/api'
            #    self.restHandler = cresthooks.ApacheHandler(restUri,
            #            self.repositoryServer)

        if self.repositoryServer:
            self.proxyServer = proxy.SimpleRepositoryFilter(cfg, self.urlBase,
                    self.repositoryServer)

        self.cfg = cfg
        # TODO: figure out how or what to cache, caching the whole thing is not
        # threadsafe since DB connections are stashed in repositoryServer. When
        # a connection pool is in use, the cost of instantiating a new
        # repository server is negligible. Maybe instead there can be an
        # internal connection pool, or stashing db connections in threadlocal
        # storage, etc.
        #_repository_cache[cfgPath] = (ino, self.repositoryServer,
        #        self.proxyServer, self.restHandler)

    def _loadAuth(self):
        """Extract authentication info from the request."""
        self.auth = netserver.AuthToken()
        self._loadAuthPassword()
        self._loadAuthEntitlement()
        # XXX: it's sort of insecure to just take the client's word for it.
        # Maybe have a configuration directive when behind a reverse proxy?
        forward = self.environ.get('HTTP_X_FORWARDED_FOR')
        if forward:
            self.auth.remote_ip = forward.split(',')[-1].strip()
        else:
            self.auth.remote_ip = self.environ.get('REMOTE_ADDR')

    def _loadAuthPassword(self):
        """Extract HTTP Basic Authorization from the request."""
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
        """Extract conary entitlements from the request headers."""
        info = self.environ.get('HTTP_X_CONARY_ENTITLEMENT')
        if not info:
            return
        self.auth.entitlements = webauth.parseEntitlement(info)

    def _getHeaders(self):
        """Build a case-insensitive dictionary of HTTP headers."""
        # HTTP headers aren't actually RFC 2822, but it provides a convenient
        # case-insensitive dictionary implementation.
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
        """Helper for sending response headers and body. Returns the body for
        convenient yielding.

        Ex.: yield self._response('200 Ok', 'document here')
        """
        headers = list(headers)
        if content_type is not None:
            headers.append(('Content-type', content_type))
        self.start_response(status, headers)
        return body

    def _resp_iter(self, *args, **kwargs):
        """Helper for sending response headers and body. Yields the response
        body so it can be returned from a non-generator WSGI handler.

        Ex.: return self._response('400 Bad Request', 'document here')
        """
        return iter([self._response(*args, **kwargs)])

    def __iter__(self):
        """Do the actual request handling. Yields chunks of the response."""

        self.proxyServer.log.reset()

        if (self.auth.user != 'anonymous'
                and not self.isSecure
                and self.cfg.forceSSL):
            return self._resp_iter('403 Secure Connection Required',
                    "ERROR: Password authentication is not allowed over "
                    "unsecured connections.\r\n")

        if self.repositoryServer:
            self.repositoryServer.reopen()

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
            stream.seek(0)
        elif encoding != 'identity':
            log.error("Unrecognized content-encoding %r from %s", encoding,
                    self.auth.remote_ip)
            yield self._response('400 Bad Request',
                    "ERROR: Unrecognized Content-Encoding\r\n")
            return

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
                    rawUrl=self.rawUrl,
                    localAddr=localAddr,
                    protocolString=self.environ['SERVER_PROTOCOL'],
                    headers=self._getHeaders(),
                    isSecure=self.isSecure)
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
        for data in _iterFileChunks(sio):
            yield data

    def _iter_get(self):
        """GET method -- handle changeset and file contents downloads."""
        # Request URI looks like /changeset/?wxyz.ccs
        path = self.pathInfo
        if path.endswith('/'):
            path = path[:-1]
        command = os.path.basename(path)

        if command == 'changeset':
            return self._iter_get_changeset()
        else:
            return self._resp_iter('404 Not Found',
                    "ERROR: Resource not found.\r\n")

    def _iter_get_changeset(self):
        """GET a prepared changeset file."""
        # IMPORTANT: As used here, "expandedSize" means the size of the
        # changeset as it is sent over the wire. The size of the file we are
        # reading from may be different if it includes references to other
        # files in lieu of their actual contents.
        path = self._changesetPath('-out')
        if not path:
            yield self._response('403 Forbidden',
                    "ERROR: Illegal changeset request.\r\n")
            return

        items = []
        totalSize = 0

        if path.endswith('.cf-out'):
            # Manifest of files to send sequentially (file contents or cached
            # changesets). Some of these may live outside of the tmpDir and
            # thus should not be unlinked afterwards.
            try:
                manifest = open(path, 'rt')
            except IOError, err:
                if err.errno == errno.ENOENT:
                    yield self._response('404 Not Found',
                            "ERROR: Resource not found.\r\n")
                    return
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
                    yield self._response('404 Not Found',
                            "ERROR: Resource not found.\r\n")
                    return
                raise
            expandedSize = os.fstat(fobj.fileno()).st_size
            items.append((path, False, False))
            totalSize += expandedSize

        self.start_response('200 Ok', [
            ('Content-type', 'application/x-conary-change-set'),
            ('Content-length', str(totalSize)),
            ])
        for path, isChangeset, preserveFile in items:
            if isChangeset:
                csFile = util.ExtendedFile(path, 'rb', buffering=False)
                changeSet = filecontainer.FileContainer(csFile)
                for data in changeSet.dumpIter(_readNestedFile):
                    yield data
                del changeSet
            else:
                fobj = open(path, 'rb')
                for data in _iterFileChunks(fobj):
                    yield data
                fobj.close()

            if not preserveFile:
                os.unlink(path)

    def _iter_put(self):
        """PUT method -- handle changeset uploads."""
        if not self.repositoryServer:
            # FIXME
            raise NotImplementedError("Changeset uploading through a proxy "
                    "is not implemented yet")

        # Copy request body to the designated temporary file.
        out = self._openForPut()
        if out is None:
            # File already exists or is in an illegal location.
            yield self._response('403 Forbidden',
                    "ERROR: Illegal changeset upload.\r\n")
            return

        util.copyfileobj(self.environ['wsgi.input'], out)
        out.close()

        yield self._response('200 Ok', '')

    def _changesetPath(self, suffix):
        filename = self.environ['QUERY_STRING']
        if not filename or os.path.sep in filename:
            return None
        return os.path.join(self.repositoryServer.tmpPath, filename + suffix)

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

    def close(self):
        log.info("... closing")
        # Make sure any pooler database connections are released.
        if self.repositoryServer:
            self.repositoryServer.reset()


class ConfigurationError(RuntimeError):
    pass
