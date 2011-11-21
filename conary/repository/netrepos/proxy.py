#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


import cPickle
import itertools
import os
import resource
import tempfile
import time
import urllib2

from conary import constants, conarycfg, rpmhelper, trove, versions
from conary.lib import digestlib, sha1helper, tracelog, urlparse, util
from conary.lib.http import http_error
from conary.lib.http import request as req_mod
from conary.repository import changeset, datastore, errors, netclient
from conary.repository import filecontainer, transport, xmlshims
from conary.repository import filecontents
from conary.repository.netrepos import cache, netserver, reposlog

# A list of changeset versions we support
# These are just shortcuts
_CSVER0 = filecontainer.FILE_CONTAINER_VERSION_NO_REMOVES
_CSVER1 = filecontainer.FILE_CONTAINER_VERSION_WITH_REMOVES
_CSVER2 = filecontainer.FILE_CONTAINER_VERSION_FILEID_IDX
# The first in the list is the one the current generation clients understand
CHANGESET_VERSIONS = [ _CSVER2, _CSVER1, _CSVER0 ]
# Precedence list of versions - the version specified as key can be generated
# from the version specified as value
CHANGESET_VERSIONS_PRECEDENCE = {
    _CSVER0 : _CSVER1,
    _CSVER1 : _CSVER2,
}

class RepositoryVersionCache:

    def get(self, caller):
        basicUrl = str(caller._getBasicUrl())
        uri = basicUrl.split(':', 1)[1]

        if uri not in self.d:
            # checkVersion protocol is stopped at 50; we don't support kwargs
            # for that call, ever
            parentVersions = caller.checkVersion(50)
            self.d[uri] = max(set(parentVersions) & set(netserver.SERVER_VERSIONS))

        return self.d[uri]

    def __init__(self):
        self.d = {}

class ProxyClient(util.ServerProxy):

    pass

class ExtraInfo(object):
    """This class is used for passing extra information back to the server
    running class (either standalone or apache)"""
    __slots__ = [ 'responseHeaders', 'responseProtocol' ]
    def __init__(self, responseHeaders, responseProtocol):
        self.responseHeaders = responseHeaders
        self.responseProtocol = responseProtocol

    def getVia(self):
        if not self.responseHeaders:
            return None
        return self.responseHeaders.get('Via', None)

class ProxyCaller:
    """
    This class facilitates access to a remote repository using the same
    interface as L{RepositoryCaller}.
    """

    def callByName(self, methodname, version, *args, **kwargs):
        """Call a remote server method using the netserver convention."""
        request = xmlshims.RequestArgs(version, args, kwargs)
        response = self.callWithRequest(methodname, request)
        if response.isException:
            # exception occured. this lets us tunnel the error through
            # without instantiating it (which would be demarshalling the
            # thing just to remarshall it again)
            raise ProxyRepositoryError(response.excName, response.excArgs,
                    response.excKwargs)
        return response.result

    def callWithRequest(self, methodname, request):
        """Call a remote server method using request/response objects."""
        rawRequest = request.toWire()
        try:
            rawResponse = self.proxy._request(methodname, rawRequest)
        except IOError, e:
            raise errors.ProxyError(e.strerror)
        except http_error.ResponseError, e:
            if e.errcode == 403:
                raise errors.InsufficientPermission

            raise
        self._lastProxy = self._transport.usedProxy

        # XMLRPC responses are a 1-tuple
        rawResponse, = rawResponse
        return xmlshims.ResponseArgs.fromWire(request.version, rawResponse,
                self._transport.responseHeaders)

    def getExtraInfo(self):
        """Return extra information if available"""
        return ExtraInfo(self._transport.responseHeaders,
                         self._transport.responseProtocol)

    def __getattr__(self, method):
        # Don't invoke methods that start with _
        if method.startswith('_'):
            raise AttributeError(method)
        return lambda *args, **kwargs: self.callByName(method, *args, **kwargs)

    def _getBasicUrl(self):
        return self.url._replace(userpass=(None, None))

    def __init__(self, url, proxy, transport):
        self.url = url
        self.proxy = proxy
        self._lastProxy = None
        self._transport = transport


class ProxyCallFactory:

    @staticmethod
    def createCaller(protocol, port, rawUrl, proxyMap, authToken, localAddr,
                     protocolString, headers, cfg, targetServerName,
                     remoteIp, isSecure, baseUrl):
        entitlementList = authToken[2][:]
        injEntList = cfg.entitlement.find(targetServerName)
        if injEntList:
            entitlementList += injEntList

        userOverride = cfg.user.find(targetServerName)
        if userOverride:
            authToken = authToken[:]
            authToken[0], authToken[1] = userOverride

        url = redirectUrl(authToken, rawUrl)
        url = req_mod.URL.parse(url)

        via = []
        # Via is a multi-valued header. Multiple occurences will be collapsed
        # as a single string, separated by ,
        if 'Via' in headers:
            via.append(headers['via'])
        if localAddr and protocolString:
            via.append(formatViaHeader(localAddr, protocolString))
        lheaders = {}
        if via:
            lheaders['Via'] = ', '.join(via)

        # If the proxy injected entitlements or user information, switch to
        # SSL -- IF they are using
        # default ports (if they specify ports, we have no way of
        # knowing what port to use)
        if (url.hostport.port == 80 and
                (bool(injEntList) or bool(userOverride))):
            hostport = url.hostport._replace(port=443)
            url = url._replace(scheme='https', hostport=hostport)
        transporter = transport.Transport(proxyMap=proxyMap,
                                          serverName = targetServerName)
        transporter.setExtraHeaders(lheaders)
        transporter.setEntitlements(entitlementList)

        transporter.setCompress(True)
        proxy = ProxyClient(url, transporter)

        return ProxyCaller(url, proxy, transporter)

class RepositoryCaller(xmlshims.NetworkConvertors):
    """
    This class facilitates access to a local repository object using the same
    interface as L{ProxyCaller}.
    """

    # Shim calls never use a proxy, of course.
    _lastProxy = None

    def callByName(self, methodname, version, *args, **kwargs):
        """Call a repository method using the netserver convention."""
        args = (version,) + args
        return self.repos.callWrapper(
                protocol=self.protocol,
                port=self.port,
                methodname=methodname,
                authToken=self.authToken,
                orderedArgs=args,
                kwArgs=kwargs,
                remoteIp=self.remoteIp,
                rawUrl=self.rawUrl,
                isSecure=self.isSecure,
                )

    def callByRequest(self, methodname, request):
        """Call a repository method using request/response objects."""
        try:
            result = self.callByName(methodname, request.version,
                    *request.args, **request.kwargs)
            return xmlshims.ResponseArgs.newResult(result)
        except Exception, err:
            if hasattr(err, 'marshall'):
                args, kwArgs = err.marshall(self)
                return self.responseFilter.newException(
                        err.__class__.__name__, args, kwArgs)
            else:
                for cls, marshall in errors.simpleExceptions:
                    if isinstance(err, cls):
                        return self.responseFilter.newException(marshall,
                                (str(err),))
                raise

    def getExtraInfo(self):
        """No extra information available for a RepositoryCaller"""
        return None

    def __getattr__(self, method):
        # Don't invoke methods that start with _
        if method.startswith('_'):
            raise AttributeError(method)
        return lambda *args, **kwargs: self.callByName(method, *args, **kwargs)

    def __init__(self, protocol, port, authToken, repos, remoteIp, rawUrl,
                 isSecure):
        self.repos = repos
        self.protocol = protocol
        self.port = port
        self.authToken = authToken
        self.url = None
        self.remoteIp = remoteIp
        self.rawUrl = rawUrl
        self.isSecure = isSecure
        self.lastProxy = None


class RepositoryCallFactory:

    def __init__(self, repos, logger):
        self.repos = repos
        self.log = logger

    def createCaller(self, protocol, port, rawUrl, proxyMap, authToken,
                     localAddr, protocolString, headers, cfg,
                     targetServerName, remoteIp, isSecure, baseUrl):
        if 'via' in headers:
            self.log(2, "HTTP Via: %s" % headers['via'])
        return RepositoryCaller(protocol, port, authToken, self.repos,
                                remoteIp, baseUrl, isSecure)

class BaseProxy(xmlshims.NetworkConvertors):

    # a list of the protocol versions we understand. Make sure the first
    # one in the list is the lowest protocol version we support and the
    # last one is the current server protocol version.
    #
    # for thoughts on this process, see the IM log at the end of this file
    SERVER_VERSIONS = netserver.SERVER_VERSIONS
    publicCalls = netserver.NetworkRepositoryServer.publicCalls
    responseFilter = xmlshims.ResponseArgs

    def __init__(self, cfg, basicUrl):
        self.cfg = cfg
        self.basicUrl = basicUrl
        self.logFile = cfg.logFile
        self.tmpPath = cfg.tmpDir
        util.mkdirChain(self.tmpPath)
        self.proxyMap = conarycfg.getProxyMap(cfg)
        self.repositoryVersionCache = RepositoryVersionCache()

        self.log = tracelog.getLog(None)
        if cfg.traceLog:
            (l, f) = cfg.traceLog
            self.log = tracelog.getLog(filename=f, level=l, trace=l>2)

        if self.logFile:
            self.callLog = reposlog.RepositoryCallLogger(self.logFile, [])
        else:
            self.callLog = None

        self.log(1, "proxy url=%s" % basicUrl)

    def callWrapper(self, protocol, port, methodname, authToken, request,
                    remoteIp = None, rawUrl = None, localAddr = None,
                    protocolString = None, headers = None, isSecure = False):
        """
        @param localAddr: if set, a string host:port identifying the address
        the client used to talk to us.
        @param protocolString: if set, the protocol version the client used
        (i.e. HTTP/1.0)
        """
        extraInfo = None
        if methodname not in self.publicCalls:
            return (self.responseFilter.newException(
                "MethodNotSupported", (methodname,)), extraInfo)

        self._port = port
        self._protocol = protocol

        self.setBaseUrlOverride(rawUrl, headers, isSecure)

        targetServerName = headers.get('X-Conary-Servername', None)

        # simple proxy. FIXME: caching these might help; building all
        # of this framework for every request seems dumb. it seems like
        # we could get away with one total since we're just changing
        # hostname/username/entitlement
        caller = self.callFactory.createCaller(protocol, port, rawUrl,
                                               self.proxyMap, authToken,
                                               localAddr, protocolString,
                                               headers, self.cfg,
                                               targetServerName,
                                               remoteIp, isSecure,
                                               self.urlBase())

        response = None
        try:
            args = (request.version,) + request.args
            kwargs = request.kwargs
            if hasattr(self, methodname):
                # Special handling at the proxy level. The logged method name
                # is prefixed with a '+' to differentiate it from a vanilla
                # call.
                method = self.__getattribute__(methodname)

                if self.callLog:
                    self.callLog.log(remoteIp, authToken, '+' + methodname,
                            args, kwargs)

                r = method(caller, authToken, *args, **kwargs)
            else:
                # Forward directly to the next server.
                if self.callLog:
                    self.callLog.log(remoteIp, authToken, methodname, args,
                            kwargs)
                # This is incredibly silly.
                r = caller.callByName(methodname, *args, **kwargs)

            response = self.responseFilter.newResult(r)
            extraInfo = caller.getExtraInfo()
        except ProxyRepositoryError, e:
            response = self.responseFilter.newException(e.name, e.args,
                    e.kwArgs)
        except Exception, e:
            if hasattr(e, 'marshall'):
                args, kwArgs = e.marshall(self)
                response = self.responseFilter.newException(
                        e.__class__.__name__, args, kwArgs)
            else:
                for klass, marshall in errors.simpleExceptions:
                    if isinstance(e, klass):
                        response = self.responseFilter.newException(
                                marshall, (str(e),))
                        break

                if not response:
                    # this exception is not marshalled back to the client.
                    # re-raise it now.  comment the next line out to fall into
                    # the debugger
                    raise

                    # uncomment the next line to translate exceptions into
                    # nicer errors for the client.
                    #return (True, ("Unknown Exception", str(e)))

                    # fall-through to debug this exception - this code should
                    # not run on production servers
                    import traceback, sys
                    from conary.lib import debugger
                    excInfo = sys.exc_info()
                    lines = traceback.format_exception(*excInfo)
                    print "".join(lines)
                    if 1 or sys.stdout.isatty() and sys.stdin.isatty():
                        debugger.post_mortem(excInfo[2])
                    raise

        return response, extraInfo

    def setBaseUrlOverride(self, rawUrl, headers, isSecure):
        if not rawUrl:
            return
        if not rawUrl.startswith("/"):
            self._baseUrlOverride = rawUrl
        elif headers and "Host" in headers:
            proto = (isSecure and "https") or "http"
            self._baseUrlOverride = "%s://%s%s" % (proto,
                                                   headers['Host'],
                                                   rawUrl)
    def urlBase(self):
        if self._baseUrlOverride is not None:
            return self._baseUrlOverride

        return self._getUrlBase()

    def _getUrlBase(self):
        return self.basicUrl % { 'port' : self._port,
                                 'protocol' : self._protocol }

    def checkVersion(self, caller, authToken, clientVersion):
        self.log(2, authToken[0], "clientVersion=%s" % clientVersion)

        # cut off older clients entirely, no negotiation
        if clientVersion < self.SERVER_VERSIONS[0]:
            raise errors.InvalidClientVersion(
               'Invalid client version %s.  Server accepts client versions %s '
               '- read http://wiki.rpath.com/wiki/Conary:Conversion' %
               (clientVersion, ', '.join(str(x) for x in self.SERVER_VERSIONS)))

        parentVersions = caller.checkVersion(clientVersion)

        if self.SERVER_VERSIONS is not None:
            commonVersions = sorted(list(set(self.SERVER_VERSIONS) &
                                         set(parentVersions)))
        else:
            commonVersions = parentVersions

        return commonVersions

    def getContentsStore(self):
        return None


class ChangeSetInfo(object):

    __slots__ = [ 'size', 'trovesNeeded', 'removedTroves', 'filesNeeded',
                  'path', 'cached', 'version', 'fingerprint',
                  'rawSize' ]

    def pickle(self):
        return cPickle.dumps(((self.trovesNeeded, self.filesNeeded,
                               self.removedTroves), self.size))

    def __init__(self, pickled = None):
        if pickled is not None:
            ((self.trovesNeeded, self.filesNeeded, self.removedTroves),
                    self.size) = cPickle.loads(pickled)

class ChangesetFilter(BaseProxy):

    # Implements changeset caching, content injection for capsules, and
    # format conversion between changeset versions. The changeset cache
    # is passed in as an object rather than created here to allow different
    # types of changeset caches to be used in the future.

    forceGetCsVersion = None
    forceSingleCsJob = False
    withCapsuleInjection = False

    def __init__(self, cfg, basicUrl, cache):
        BaseProxy.__init__(self, cfg, basicUrl)
        self.csCache = cache

    @staticmethod
    def _getChangeSetVersion(clientVersion):
        # Determine the changeset version based on the client version
        return changeset.getNativeChangesetVersion(clientVersion)

    def _convertChangeSet(self, csPath, size, destCsVersion, csVersion):
        # Changeset is in the file csPath
        # Changeset was fetched from the cache using key
        # Convert it to destCsVersion
        if (csVersion, destCsVersion) == (_CSVER1, _CSVER0):
            return self._convertChangeSetV1V0(csPath, size, destCsVersion)
        elif (csVersion, destCsVersion) == (_CSVER2, _CSVER1):
            return self._convertChangeSetV2V1(csPath, size, destCsVersion)
        assert False, "Unknown versions"

    def _convertChangeSetV3V2(self, cspath, size, destCsVersion):
        (fd, newCsPath) = tempfile.mkstemp(dir = self.cfg.tmpDir,
                                           suffix = '.tmp')
        os.close(fd)
        size = changeset._convertChangeSetV3V2(cspath, newCsPath)

        return newCsPath, size

    def _convertChangeSetV2V1(self, cspath, size, destCsVersion):
        (fd, newCsPath) = tempfile.mkstemp(dir = self.cfg.tmpDir,
                                        suffix = '.tmp')
        os.close(fd)
        delta = changeset._convertChangeSetV2V1(cspath, newCsPath)

        return newCsPath, size + delta

    def _convertChangeSetV1V0(self, cspath, size, destCsVersion):
        cs = changeset.ChangeSetFromFile(cspath)
        newCs = changeset.ChangeSet()

        for tcs in cs.iterNewTroveList():
            if tcs.troveType() != trove.TROVE_TYPE_REMOVED:
                continue

            # Even though it's possible for (old) clients to request
            # removed relative changesets recursively, the update
            # code never does that. Raising an exception to make
            # sure we know how the code behaves.
            if not tcs.isAbsolute():
                raise errors.InternalServerError(
                    "Relative recursive changesets not supported "
                    "for removed troves")
            ti = trove.TroveInfo(tcs.troveInfoDiff.freeze())
            trvName = tcs.getName()
            trvNewVersion = tcs.getNewVersion()
            trvNewFlavor = tcs.getNewFlavor()
            if ti.flags.isMissing():
                # this was a missing trove for which we
                # synthesized a removed trove object.
                # The client would have a much easier time
                # updating if we just made it a regular trove.
                missingOldVersion = tcs.getOldVersion()
                missingOldFlavor = tcs.getOldFlavor()
                if missingOldVersion is None:
                    oldTrove = None
                else:
                    oldTrove = trove.Trove(trvName,
                                           missingOldVersion,
                                           missingOldFlavor)

                newTrove = trove.Trove(trvName,
                                       trvNewVersion,
                                       trvNewFlavor)
                diff = newTrove.diff(oldTrove, absolute = tcs.isAbsolute())[0]
                newCs.newTrove(diff)
            else:
                # this really was marked as a removed trove.
                # raise a TroveMissing exception
                raise errors.TroveMissing(trvName, trvNewVersion)

        # we need to re-write the munged changeset for an
        # old client
        cs.merge(newCs)
        # create a new temporary file for the munged changeset
        (fd, cspath) = tempfile.mkstemp(dir = self.cfg.tmpDir,
                                        suffix = '.tmp')
        os.close(fd)
        # now write out the munged changeset
        size = cs.writeToFile(cspath,
            versionOverride = filecontainer.FILE_CONTAINER_VERSION_NO_REMOVES)
        return cspath, size

    def getChangeSet(self, caller, authToken, clientVersion, chgSetList,
                     recurse, withFiles, withFileContents, excludeAutoSource,
                     changesetVersion = None, mirrorMode = False,
                     infoOnly = False):

        # This is how the caching algorithm works:
        # - Produce verPath, a path in the digraph of possible version
        # transformations. It starts with the version we need and ends with
        # the version the upstream server knows how to produce.
        # - For each changeset:
        #   - walk verPath. If version is found, add it to changeSetList and
        #     break (csInfo will contain the version we found, it may be newer
        #     than what the client needs), otherwise try the next version
        # - Fetch the changesets that are missing from changeSetList, and add
        # them to changeSetList. Their version is wireCsVersion. Cache them as
        # such in the process.
        # - Walk changeSetList; if version is newer than what the client
        # expects, start doing the conversions backwards.

        # Changeset version we need to produce
        neededCsVersion = changesetVersion or self._getChangeSetVersion(clientVersion)
        # Changeset version we expect the server to produce for us
        # If we're a proxy, we can look in the cache to find the server's
        # version, otherwise use the repository version
        if caller.url is None:
            serverVersion = ChangesetFilter.SERVER_VERSIONS[-1]
        else:
            serverVersion = self.repositoryVersionCache.get(caller)

        wireCsVersion = self._getChangeSetVersion(serverVersion)

        # forceGetCsVersion is set when this proxy object is sitting
        # in front of a repository object in the same server instance
        if self.forceGetCsVersion is not None:
            # Talking to a repository
            getCsVersion = self.forceGetCsVersion
        else:
            # This is a standalone proxy talking to a repository.  Talk
            # the latest common protocol version
            getCsVersion = serverVersion

        # Make sure we have a way to get from here to there
        iterV = neededCsVersion
        verPath = [iterV]
        while iterV != wireCsVersion:
            if iterV not in CHANGESET_VERSIONS_PRECEDENCE:
                # No way to move forward
                break
            # Move one edge in the DAG, try again
            iterV = CHANGESET_VERSIONS_PRECEDENCE[iterV]
            verPath.append(iterV)

        # This is important; if it doesn't work out the cache is likely
        # not working.
        if verPath[-1] != wireCsVersion:
            raise errors.InvalidClientVersion(
                "Unable to produce changeset version %s "
                "with upstream server %s" % (neededCsVersion, wireCsVersion))

        try:
            changeSetList = self._getNeededChangeSets(caller,
                authToken, verPath, chgSetList, serverVersion,
                getCsVersion, wireCsVersion, neededCsVersion,
                recurse, withFiles, withFileContents, excludeAutoSource,
                mirrorMode, infoOnly)
        finally:
            if self.csCache:
                # In case we missed releasing some of the locks
                self.csCache.resetLocks()

        if not infoOnly:
            (fd, path) = tempfile.mkstemp(dir = self.cfg.tmpDir,
                                          suffix = '.cf-out')
            url = os.path.join(self.urlBase(),
                               "changeset?%s" % os.path.basename(path[:-4]))
            f = os.fdopen(fd, 'w')

            for csInfo in changeSetList:
                # the hard-coded 1 means it's a changeset and needs to be walked
                # looking for files to include by reference
                f.write("%s %d 1 %d\n" % (csInfo.path, csInfo.size,
                csInfo.cached))

            f.close()
        else:
            url = ''

        if clientVersion < 50:
            allSizes = [ x.size for x in changeSetList ]
            allTrovesNeeded = [ x for x in itertools.chain(
                                 *[ x.trovesNeeded for x in changeSetList ] ) ]
            allFilesNeeded = [ x for x in itertools.chain(
                                 *[ x.filesNeeded for x in changeSetList ] ) ]
            allTrovesRemoved = [ x for x in itertools.chain(
                                 *[ x.removedTroves for x in changeSetList ] ) ]

            # client versions >= 44 use strings instead of ints for size
            # because xmlrpclib can't marshal ints > 2GiB
            if clientVersion >= 44:
                allSizes = [ str(x) for x in allSizes ]
            else:
                for size in allSizes:
                    if size >= 0x80000000:
                        raise errors.InvalidClientVersion(
                         'This version of Conary does not support downloading '
                         'changesets larger than 2 GiB.  Please install a new '
                         'Conary client.')

            if clientVersion < 38:
                return (url, allSizes, allTrovesNeeded, allFilesNeeded)

            return (url, allSizes, allTrovesNeeded, allFilesNeeded,
                    allTrovesRemoved)

        # clientVersion >= 50
        return (url, (
                [ (str(x.size), x.trovesNeeded, x.filesNeeded, x.removedTroves)
                    for x in changeSetList ] ) )

    def _callGetChangeSetFingerprints(self, caller, chgSetList,
            recurse, withFiles, withFileContents, excludeAutoSource,
            mirrorMode):
        fingerprints = [ '' ] * len(chgSetList)
        if self.csCache:
            try:
                if mirrorMode:
                    fingerprints = caller.getChangeSetFingerprints(49,
                            chgSetList, recurse, withFiles, withFileContents,
                            excludeAutoSource, mirrorMode)
                else:
                    fingerprints = caller.getChangeSetFingerprints(43,
                            chgSetList, recurse, withFiles, withFileContents,
                            excludeAutoSource)

            except errors.MethodNotSupported:
                # old server; act like no fingerprints were returned
                pass
        return fingerprints

    # mixins can override this (to provide fingerprint caching, perhaps)
    def lookupFingerprints(self, caller, authToken, chgSetList, recurse,
                           withFiles, withFileContents, excludeAutoSource,
                           mirrorMode):
        return self._callGetChangeSetFingerprints(
                            caller, chgSetList, recurse, withFiles,
                            withFileContents, excludeAutoSource, mirrorMode)

    def _callGetChangeSet(self, caller, changeSetList, getCsVersion,
                wireCsVersion, neededCsVersion, neededFiles, recurse,
                withFiles, withFileContents, excludeAutoSource, mirrorMode,
                infoOnly):
        if getCsVersion >= 51 and wireCsVersion == neededCsVersion:
            # We may be able to get proper size information for this from
            # underlying server without fetcing the changeset (this isn't
            # true for internal servers or old protocols)
            rc = caller.getChangeSet(getCsVersion,
                                 [ x[1][0] for x in neededFiles ],
                                 recurse, withFiles, withFileContents,
                                 excludeAutoSource,
                                 neededCsVersion, mirrorMode,
                                 infoOnly)
        elif getCsVersion >= 49:
            rc = caller.getChangeSet(getCsVersion,
                                 [ x[1][0] for x in neededFiles ],
                                 recurse, withFiles, withFileContents,
                                 excludeAutoSource,
                                 wireCsVersion, mirrorMode)
        else:
            # We don't support requesting specific changeset versions
            rc = caller.getChangeSet(getCsVersion,
                                 [ x[1][0] for x in neededFiles ],
                                 recurse, withFiles, withFileContents,
                                 excludeAutoSource)
        csInfoList = []
        url = rc[0]
        if getCsVersion < 50:
            # convert pre-protocol 50 returns into a protocol 50 return
            # turn list of sizes back into a single size
            assert(len(rc[1]) == 1)
            rc[1] = rc[1][0]
            rc = rc[1:]
            if getCsVersion < 38:
                # protocol version 38 does not return removedTroves.
                # tack an empty list on it
                rc.append([])
            allInfo = [ rc ]
        else:
            allInfo = rc[1]
        for info in allInfo:
            csInfo = ChangeSetInfo()
            (size, trovesNeeded, filesNeeded, removedTroves) = info[0:4]
            if len(info) > 4:
                rawSize = int(info[4])
            else:
                rawSize = int(size)

            csInfo.size = int(size)
            csInfo.rawSize = rawSize
            csInfo.trovesNeeded = trovesNeeded
            csInfo.filesNeeded = filesNeeded
            csInfo.removedTroves = removedTroves
            csInfo.version = wireCsVersion
            csInfoList.append(csInfo)

        del trovesNeeded
        del filesNeeded
        del removedTroves

        if (getCsVersion >= 51 and wireCsVersion == neededCsVersion
                and infoOnly and not url):
            # We only got size information from the repository; there
            # is no changeset to fetch/cache.  We can bail out early.
            for jobIdx, csInfo in enumerate(csInfoList):
                csInfo.path = None
                changeSetList[jobIdx] = csInfo
            return None, csInfoList

        return url, csInfoList

    def _getCachedChangeSetList(self, chgSetList, fingerprints, verPath):
        """
        Return a parallel list to chgSetList and fingerprints, with items
        set on the corresponding position if the changeset was retrieved from
        the cache
        """
        changeSetList = [ None ] * len(chgSetList)
        if not self.csCache:
            # We have no cache, so don't even bother
            return changeSetList

        # We need to order by fingerprint first
        # This prevents deadlocks from occurring - as long as different
        # processes acquire locks in the same order, we should be fine
        orderedData = sorted(
            enumerate(itertools.izip(chgSetList, fingerprints)),
            key = lambda x: x[1][1])

        for jobIdx, (rawJob, fingerprint) in orderedData:
            # if we have both a cs fingerprint and a cache, then we will
            # cache the cs for this job
            cachable = bool(fingerprint)
            if not cachable:
                continue

            # look up the changeset in the cache, oldest to newest
            for iterV in verPath:
                # We will only lock the last version (wireCsVersion)
                # Everything else gets derived from it, and is fast to convert
                shouldLock = (iterV == verPath[-1])
                csInfo = self.csCache.get((fingerprint, iterV),
                    shouldLock = shouldLock)
                if csInfo:
                    # Found in the cache (possibly with an older version)
                    csInfo.fingerprint = fingerprint
                    changeSetList[jobIdx] = csInfo
                    break
        return changeSetList

    def _getNeededChangeSets(self, caller, authToken, verPath, chgSetList,
            serverVersion,
            getCsVersion, wireCsVersion, neededCsVersion,
            recurse, withFiles, withFileContents, excludeAutoSource,
            mirrorMode, infoOnly, _recursed = False):

        fingerprints = self.lookupFingerprints(caller, authToken, chgSetList,
            recurse, withFiles, withFileContents, excludeAutoSource,
            mirrorMode)

        changeSetList = self._getCachedChangeSetList(chgSetList, fingerprints,
            verPath)

        changeSetsNeeded = \
            [ x for x in
                    enumerate(itertools.izip(chgSetList, fingerprints))
                    if changeSetList[x[0]] is None ]

        # If proxying for a repository, self.csCache is None, so
        # changeSetsNeeded is equivalent to chgSetList
        if withFileContents and not infoOnly and not _recursed:
            if (self.withCapsuleInjection and changeSetsNeeded and
                    self.cfg.capsuleServerUrl):
                assert self.csCache
                # We only have to retrieve the items that were not cached
                csWithInjection, csNoInjection = self._splitChangeSetInjection(
                    changeSetsNeeded)
                indexesNeeded = [ i for (i, _) in csWithInjection ]
                partChgSetList = [ chgSetList[i] for i in indexesNeeded ]
                fpNeeded = [ x[1] for (_, x) in csWithInjection ]
                partChangeSetList = self._getNeededChangeSets(caller, authToken,
                    verPath, partChgSetList, serverVersion,
                    getCsVersion, wireCsVersion, neededCsVersion,
                    recurse, withFiles = True, withFileContents = False,
                    excludeAutoSource = excludeAutoSource,
                    mirrorMode = mirrorMode, infoOnly = infoOnly)
                # Split the capsule troves from non-capsule ones
                capsuleJobsMap = dict()
                nonCapsuleJobsMap = dict()
                # This is a map from a job to its XMLRPC-safe representation
                sanitizedJobsMap = dict()

                absOldChgSetList = []
                for csInfo in partChangeSetList:
                    cs = changeset.ChangeSetFromFile(csInfo.path)
                    for tcs in cs.iterNewTroveList():
                        job = tcs.getJob()
                        sanitizedJobsMap[job] = self.toJob(job)
                        if tcs.getTroveInfo().capsule.type():
                            capsuleJobsMap[job] = tcs
                            if job[1][0]:
                                absOldChgSetList.append(
                                    (job[0], (None, None), job[1], True))
                        else:
                            nonCapsuleJobsMap[job] = tcs
                if capsuleJobsMap:
                    # We want to skip the changeset re-combination if there
                    # were no capsule jobs

                    # We also need to fetch the absolute changesets for the old
                    # troves
                    absOldCapsuleJobs = [ self.toJob(j)
                        for j in absOldChgSetList ]
                    # XXX some of the things in extraChgSetList may already
                    # be in nonCapsuleJobs
                    absOldChangeSetListCntnr = self._getNeededChangeSets(caller,
                        authToken, verPath, absOldCapsuleJobs, serverVersion,
                        getCsVersion, wireCsVersion, neededCsVersion,
                        recurse = False, withFiles = withFiles,
                        withFileContents = False,
                        excludeAutoSource = excludeAutoSource,
                        mirrorMode = mirrorMode, infoOnly = infoOnly,
                        _recursed = True)
                    absOldChangeSetMap = dict(zip(
                        ((x[0], x[2][0], x[2][1]) for x in absOldChgSetList),
                        (changeset.ChangeSetFromFile(x.path)
                            for x in absOldChangeSetListCntnr)))

                    nonCapsuleJobs = sorted(sanitizedJobsMap[j]
                        for j in nonCapsuleJobsMap)

                    # Call again, no recursion, since we gathered individual
                    # troves already
                    partChangeSetListNonCntnr = self._getNeededChangeSets(caller,
                        authToken, verPath, nonCapsuleJobs, serverVersion,
                        getCsVersion, wireCsVersion, neededCsVersion,
                        recurse = False, withFiles = withFiles,
                        withFileContents = True,
                        excludeAutoSource = excludeAutoSource,
                        mirrorMode = mirrorMode, infoOnly = infoOnly,
                        _recursed = True)
                    ncCsInfoMap = dict(zip(nonCapsuleJobs,
                        partChangeSetListNonCntnr))

                    # Reassemble changesets
                    for idx, csInfo, fprint in zip(
                            indexesNeeded, partChangeSetList, fpNeeded):
                        cs, extraFiles = self._reassembleChangeSet(
                            csInfo, ncCsInfoMap, absOldChangeSetMap)
                        self._fetchDerivedCapsuleFiles(caller, serverVersion,
                            extraFiles, cs)
                        (fd, tmppath) = tempfile.mkstemp(dir = self.cfg.tmpDir,
                                                         suffix = ".ccs-temp")
                        os.unlink(tmppath)
                        fobj = os.fdopen(fd, "w+")
                        csInfo.size = cs.appendToFile(fobj)
                        csInfo.rawSize = fobj.tell()
                        csInfo.fingerprint = fprint
                        fobj.seek(0)

                        # Cache the changeset
                        csPath = self.csCache.set(
                            (csInfo.fingerprint, csInfo.version),
                            (csInfo, fobj, csInfo.rawSize))
                        fobj.close()
                        csInfo.path = csPath
                        # save csInfo
                        changeSetList[idx] = csInfo
                    # At the end of this step, we have all our changesets
                    # that require injection, leave behind only the ones with
                    # no injection
                    changeSetsNeeded = csNoInjection

        if self.callLog and changeSetsNeeded:
            self.callLog.log(None, authToken, '__createChangeSets',
                             changeSetsNeeded)

        if serverVersion < 50 or self.forceSingleCsJob:
            # calling internal changeset generation, which only supports
            # a single job or calling an upstream repository that does not
            # support protocol version 50 (needed to send all jobs at once)
            neededList = [ [ x ] for x in changeSetsNeeded ]
        else:
            # calling a server which supports both neededCsVersion and
            # returns per-job supplmental information
            if changeSetsNeeded:
                neededList = [ changeSetsNeeded ]
            else:
                neededList = []

        # List of (url, csInfoList)

        # This is a loop to make supporting single-request changeset generation
        # easy; we need that not only for old servers we proxy, but for an
        # internal server as well (since internal servers only support
        # single jobs!)
        urlInfoList = [ self._callGetChangeSet(caller, changeSetList,
                getCsVersion, wireCsVersion, neededCsVersion, neededHere,
                recurse, withFiles, withFileContents, excludeAutoSource,
                mirrorMode, infoOnly)
            for neededHere in neededList ]
        forceProxy = caller._lastProxy

        for (url, csInfoList), neededHere in zip(urlInfoList, neededList):
            if url is None:
                # Only size information was received; nothing further needed
                continue
            self._cacheChangeSet(url, neededHere, csInfoList, changeSetList,
                    forceProxy)

        # hash versions to quickly find the index in verPath
        verHash = dict((csVer, idx) for (idx, csVer) in enumerate(verPath))

        # Handle format conversions
        for csInfo in changeSetList:
            if infoOnly and csInfo.path is None:
                assert(neededCsVersion == wireCsVersion)
                # the changeset isn't present
                continue

            fc = filecontainer.FileContainer(
                util.ExtendedFile(csInfo.path, 'r', buffering = False))
            csVersion = fc.version
            fc.close()
            if csInfo.version == neededCsVersion:
                # We already have the right version
                continue

            # Now walk the precedence list backwards for conversion
            oldV = csInfo.version
            csPath = csInfo.path

            # Find the position of this version into the precedence list
            idx = verHash[oldV]

            for iterV in reversed(verPath[:idx]):
                # Convert the changeset
                path, newSize = self._convertChangeSet(csPath, csInfo.size,
                                                       iterV, oldV)
                csInfo.size = newSize
                csInfo.version = iterV

                cachable = (csInfo.fingerprint and self.csCache)

                if not cachable:
                    # we're not caching; erase the old version
                    os.unlink(csPath)
                    csPath = path
                else:
                    csPath = self.csCache.set((csInfo.fingerprint, iterV),
                        (csInfo, open(path), None))

                oldV = iterV

            csInfo.version = neededCsVersion
            csInfo.path = csPath

        return changeSetList

    def _fetchDerivedCapsuleFiles(self, caller, clientVersion,
            extraFiles, ccs):
        if not extraFiles:
            return
        fileList = [ (self.fromFileId(x[2]), self.fromVersion(x[3]))
            for x in extraFiles ]
        url, sizes = caller.getFileContents(clientVersion, fileList)
        url = self._localUrl(url)
        self._saveFileContents(fileList, url, sizes,
                forceProxy=caller._lastProxy)
        newCs = changeset.ChangeSet()
        for pathId, path, fileId, fileVersion, fileObj in extraFiles:
            cachedPath = self.contents.hashToPath(
                sha1helper.sha1ToString(fileId) + '-c')
            contents = filecontents.FromFilesystem(cachedPath)
            newCs.addFileContents(pathId, fileId,
                    changeset.ChangedFileTypes.file, contents,
                    cfgFile=fileObj.flags.isConfig(),
                    compressed=True)
        ccs.merge(newCs)

    def _splitChangeSetInjection(self, changeSetsNeeded):
        # As of Conary 2.2, the client will separate changesets based
        # on the hostname portion of the label, and will not collapse
        # multiple jobs if they go to the same host (via a repository
        # map).

        # We only do content injection for troves on these servers
        injectCapsuleContentServers = set(self.cfg.injectCapsuleContentServers)

        csWithInjection = []
        csNoInjection = []
        for i, trvJobTup in changeSetsNeeded:
            newVerStr = trvJobTup[0][2][0]
            if newVerStr:
                newVer = versions.VersionFromString(newVerStr)
                if newVer.trailingLabel().getHost() in injectCapsuleContentServers:
                    csWithInjection.append((i, trvJobTup))
                    continue
            csNoInjection.append((i, trvJobTup))
        return csWithInjection, csNoInjection

    def _cacheChangeSet(self, url, neededHere, csInfoList, changeSetList,
            forceProxy):
        try:
            inF = transport.ConaryURLOpener(proxyMap=self.proxyMap).open(url,
                    forceProxy=forceProxy)
        except transport.TransportError, e:
            raise errors.RepositoryError(str(e))

        for (jobIdx, (rawJob, fingerprint)), csInfo in \
                        itertools.izip(neededHere, csInfoList):
            cachable = bool(fingerprint and self.csCache)

            if cachable:
                # Add it to the cache
                path = self.csCache.set((fingerprint, csInfo.version),
                    (csInfo, inF, csInfo.rawSize))
            else:
                # If only one file was requested, and it's already
                # a file://, this is unnecessary :-(
                (fd, tmpPath) = tempfile.mkstemp(dir = self.cfg.tmpDir,
                                              suffix = '.ccs-out')
                outF = os.fdopen(fd, "w")
                util.copyfileobj(inF, outF, sizeLimit = csInfo.rawSize)
                outF.close()
                path = tmpPath

            csInfo.fingerprint = fingerprint
            # path points to a wire version of the changeset (possibly
            # in the cache)
            csInfo.path = path
            # make a note if this path has been stored in the cache or not
            csInfo.cached = cachable
            changeSetList[jobIdx] = csInfo

        if url.startswith('file://localhost/'):
            os.unlink(url[16:])

        inF.close()

    def toJob(self, job):
        """
        Safe representation of job, that can be sent over XML-RPC
        """
        return (job[0],
            (job[1][0] and self.fromVersion(job[1][0]) or 0,
             self.fromFlavor(job[1][1])),
            (job[2][0] and self.fromVersion(job[2][0]) or 0,
             self.fromFlavor(job[2][1])),
            bool(job[3]))

    def _reassembleChangeSet(self, csInfo, nonCapsuleCsInfoMap,
            absOldChangeSetMap):
        newCs = changeset.ChangeSetFromFile(csInfo.path)
        # We need to restore primaryTroveList in the end
        primaryTroveList = newCs.primaryTroveList.copy()
        csfiles = changeset.files

        # Derived capsules may have content that has to be fetched
        # separately
        nonCapsuleFiles = []

        for trvCs in newCs.iterNewTroveList():
            job = trvCs.getJob()
            sjob = self.toJob(job)
            fileList = trvCs.getNewFileList()
            if not trvCs.getTroveInfo().capsule.type():
                # This is a non-capsule trove changeset. Merge the
                # corresponding changeset we previously retrieved withContents
                csff = changeset.ChangeSetFromFile(
                    nonCapsuleCsInfoMap[sjob].path)
                newCs.merge(csff)
                continue

            # This trove changeset is a capsule
            contType = changeset.ChangedFileTypes.file
            if not trvCs.getNewVersion():
                # we don't care about deletes
                continue

            # We need to add the file contents to the existing changeset

            # Even if it's a relative changeset, we need to add the config
            # files
            if trvCs.getOldVersion():
                oldTrv = trvCs.getOldNameVersionFlavor()
                oldChangeset = absOldChangeSetMap[oldTrv]
                oldTrvCs = oldChangeset.getNewTroveVersion(*oldTrv)
                oldTrove = trove.Trove(oldTrvCs)
                newTrove = oldTrove.copy()
                newTrove.applyChangeSet(trvCs)
            else:
                oldChangeset = None
                oldTrvCs = None
                oldTrove = None

            ccs = changeset.ChangeSet()
            capsuleSha1 = None
            # Sorting the files by path id will ensure we always get the
            # capsule first.
            for pathId, path, fileId, fileVersion in sorted(itertools.chain(
                                        trvCs.getChangedFileList(),
                                        trvCs.getNewFileList())):
                if not fileId:
                    # This can only happen on renames, which we don't
                    # quite support anyway
                    continue

                if pathId == trove.CAPSULE_PATHID:
                    capsuleSha1 = self._addCapsuleFileToChangeset(ccs,
                        oldChangeset, oldTrove, newCs,
                        trvCs.getTroveInfo().capsule.rpm,
                        (pathId, fileId))
                    continue

                configFileSha1 = False
                if not oldChangeset:
                    fileStream = newCs.getFileChange(None, fileId)
                    fileObj = csfiles.ThawFile(fileStream, pathId)
                else:
                    # Relative changeset. We need to figure out the sha1 of
                    # the config file, but if it came in as a diff, we need
                    # the old version too.
                    if path is None:
                        # relative changeset, so grab path from the new trove
                        path = newTrove.getFile(pathId)[0]

                    fileObj = self._getFileObject(pathId, fileId, oldTrove,
                        oldChangeset, newCs)


                if (isinstance(fileObj, csfiles.RegularFile)
                        and not fileObj.flags.isEncapsulatedContent()
                        and fileObj.contents.size() == 0):

                    # Special case: some ghost files were not properly marked
                    # as payload files, but they are always empty so we can
                    # fabricate the contents.

                    emptyContents = filecontents.FromString('')
                    ccs.addFileContents(pathId, fileId,
                            changeset.ChangedFileTypes.file, emptyContents,
                            cfgFile=False)
                    continue

                # derivedcapsulepolicy will set
                # isCapsuleOverride when isCapsuleAddition
                # is set, so only check for the more general one
                # See also CNY-3463 - we need to skip device files
                if fileObj.flags.isCapsuleOverride() and fileObj.hasContents:
                    nonCapsuleFiles.append((pathId, path, fileId, fileVersion, fileObj))
                    continue
                if not fileObj.flags.isConfig() or isinstance(fileObj,
                        (csfiles.SymbolicLink, csfiles.Directory)):
                    continue
                configFileSha1 = fileObj.contents.sha1()
                assert not fileObj.flags.isEncapsulatedContent()

                # Normally we should have fetched the capsule prior to the
                # config file
                assert capsuleSha1 is not None

                dl = self.CapsuleDownloader(self.cfg.capsuleServerUrl)
                rpmKey = self._getCapsuleKey(trvCs.getTroveInfo().capsule.rpm)
                rpmContents = dl.downloadCapsuleFile(rpmKey, capsuleSha1,
                    path, configFileSha1)

                contType = changeset.ChangedFileTypes.file
                ccs.addFileContents(pathId, fileId, contType,
                                    rpmContents, cfgFile = True)

            newCs.merge(ccs)
        # All this merging messed up the primary trove list
        newCs.primaryTroveList.thaw("")
        newCs.primaryTroveList.extend(primaryTroveList)
        return newCs, nonCapsuleFiles

    @classmethod
    def _getFileObject(cls, pathId, fileId, oldTrove, oldChangeset, newChangeset):
        csfiles = changeset.files
        if not oldChangeset or not oldTrove.hasFile(pathId):
            # This should catch two cases:
            # * an absolute changeset
            # * a relative changeset with a file addition (old changeset
            #   present but pathId not part of the old trove)
            fileStream = newChangeset.getFileChange(None, fileId)
            return csfiles.ThawFile(fileStream, pathId)
        oldFileId = oldTrove.getFile(pathId)[1]
        if oldFileId == fileId:
            # File is completely unchanged between versions.
            fileStream = oldChangeset.getFileChange(None, fileId)
            return csfiles.ThawFile(fileStream, pathId)
        fileDiff = newChangeset.getFileChange(oldFileId, fileId)
        if not csfiles.fileStreamIsDiff(fileDiff):
            # Absolute stream in a relative changeset.
            return csfiles.ThawFile(fileDiff, pathId)

        oldFileStream = oldChangeset.getFileChange(None, oldFileId)
        fileObj = csfiles.ThawFile(oldFileStream, pathId)
        fileObj.twm(fileDiff, fileObj)
        return fileObj

    def _addCapsuleFileToChangeset(self, destChangeset,
            oldChangeset, oldTrove, newChangeset, rpmData, (pathId, fileId)):
        csfiles = changeset.files
        csfilecontents = changeset.filecontents
        contType = changeset.ChangedFileTypes.file
        fileObj = self._getFileObject(pathId, fileId, oldTrove, oldChangeset,
            newChangeset)
        rpmSha1 = fileObj.contents.sha1()

        dl = self.CapsuleDownloader(self.cfg.capsuleServerUrl)
        rpmKey = self._getCapsuleKey(rpmData)
        rpmContents = dl.downloadCapsule(rpmKey, rpmSha1)
        destChangeset.addFileContents(pathId, fileId, contType,
                            rpmContents, cfgFile = False)
        return rpmSha1

    @classmethod
    def _getCapsuleKey(cls, rpmData):
        return (rpmData.name(), rpmData.epoch(), rpmData.version(),
            rpmData.release(), rpmData.arch())

    class CapsuleDownloader(object):
        def __init__(self, url):
            self.url = url

        def downloadCapsule(self, capsuleKey, sha1sum):
            url = self.getCapsuleDownloadUrl(capsuleKey, sha1sum)
            return self.download(url, sha1sum)

        def downloadCapsuleFile(self, capsuleKey, capsuleSha1sum,
                fileName, fileSha1sum):
            if capsuleSha1sum == fileSha1sum or fileName == '':
                # Troves do contain the capsule too, it's legitimate to
                # request it; however, we can fall back to downloadCapsule for
                # it.
                # The SHA-1 check doesn't normally work because the
                # FileStreams.sha1 column is often null for capsule files, but
                # the repository will also return fileName='' if the capsule
                # fileId is the same as the contents fileId. Either of these
                # mean that the capsule itself is being requested.
                return self.downloadCapsule(capsuleKey, capsuleSha1sum)
            url = self.getCapsuleDownloadUrl(capsuleKey, capsuleSha1sum)
            url = "%s/%s/%s" % (url, self.quote(fileName), self._sha1(fileSha1sum))
            return self.download(url, fileSha1sum)

        def downloadCapsuleFiles(self, capsuleKey, capsuleSha1sum, fileList):
            return [ self.downloadCapsuleFile(capsuleKey, capsuleSha1sum,
                fileName, fileSha1sum)
                    for (fileName, fileSha1sum) in fileList ]

        def getCapsuleDownloadUrl(self, capsuleKey, sha1sum):
            filename = self.quote(rpmhelper.NEVRA.filename(*capsuleKey[:5]))
            url = "%s/%s/%s" % (self.url, filename, self._sha1(sha1sum))
            return url

        @classmethod
        def download(cls, url, sha1sum):
            digest = digestlib.sha1()
            try:
                req = urllib2.urlopen(url)
            except urllib2.HTTPError, e:
                raise Exception("XXX FIXME")
            out = util.BoundedStringIO()
            util.copyfileobj(req, out, digest = digest)
            if digest.hexdigest() != cls._sha1(sha1sum):
                raise Exception("XXX FIXME")
            out.seek(0)
            return cls.fromFile(out)

        @classmethod
        def fromFile(cls, stream):
            return changeset.filecontents.FromFile(stream)

        @classmethod
        def quote(cls, string):
            # We need to quote twice, restlib unquotes the URL the first time
            return netclient.quote(netclient.quote(string))

        @classmethod
        def _sha1(cls, sha1sum):
            if len(sha1sum) == 20:
                return sha1helper.sha1ToString(sha1sum)
            return sha1sum

    def _getFileContentsCapsuleInfo(self, caller, clientVersion, neededFiles):
        fcCapsuleInfoList = caller.getFileContentsCapsuleInfo(clientVersion,
            neededFiles)
        ret = []
        for fcCapsuleInfo in fcCapsuleInfoList:
            if fcCapsuleInfo == '':
                ret.append(None)
                continue
            capsuleType, capsuleKey, capsuleSha1, fileKey, fileSha1 = \
                                                        fcCapsuleInfo[:5]
            n, e, v, r, a = capsuleKey[:5]
            if e == '':
                e = None
            ret.append((capsuleType, (n, e, v, r, a), capsuleSha1,
                fileKey, fileSha1))
        return ret

    def _localUrl(self, url):
        # If the changeset can be downloaded locally, return it
        parts = util.urlSplit(url)
        fname = parts[6]
        csfr = ChangesetFileReader(self.cfg.tmpDir)
        items = csfr.getItems(fname)
        if items is None:
            return url
        (fd, tmpPath) = tempfile.mkstemp(dir = self.cfg.tmpDir,
                                         suffix = '.tmp')
        dest = util.ExtendedFile(tmpPath, "w+", buffering = False)
        os.close(fd)
        os.unlink(tmpPath)
        csfr.writeItems(items, dest)
        dest.seek(0)
        return dest

class BaseCachingChangesetFilter(ChangesetFilter):
    # Changeset filter which uses a directory to create a ChangesetCache
    # instance for the cache
    def __init__(self, cfg, basicUrl):
        if cfg.changesetCacheDir:
            util.mkdirChain(cfg.changesetCacheDir)
            csCache = ChangesetCache(
                    datastore.ShallowDataStore(cfg.changesetCacheDir),
                    cfg.changesetCacheLogFile)
        else:
            csCache = None
        ChangesetFilter.__init__(self, cfg, basicUrl, csCache)

class RepositoryFilterMixin(object):

    # Simple mixin which lets a BaseProxy derivative sit in front of a
    # in-process repository class (defined in netrepos.py) rather than
    # acting as a proxy for a network repository somewhere else. repos
    # is a netrepos.NetworkRepositoryServer instance

    forceGetCsVersion = ChangesetFilter.SERVER_VERSIONS[-1]
    forceSingleCsJob = False

    def __init__(self, repos):
        self.repos = repos
        self.callFactory = RepositoryCallFactory(repos, self.log)

class Memcache(object):

    # mixin for providing memcache based caching of fingerprint, troveinfo
    # and deplists

    def __init__(self, cfg):
        self.memCacheTimeout = cfg.memCacheTimeout
        self.memCacheLocation = cfg.memCache
        self.memCacheUserAuth = cfg.memCacheUserAuth
        self.memCachePrefix = cfg.memCachePrefix

        if self.memCacheTimeout >= 0:
            self.memCache = cache.getCache(self.memCacheLocation)
        else:
            self.memCache = cache.EmptyCache()

    def _getKeys(self, authToken, listArgs, extraArgs=(), extraKwargs=None):
        if extraKwargs is None:
            extraKwargs = ()
        else:
            extraKwargs = tuple(sorted(extraKwargs.items()))
        if self.memCacheUserAuth:
            authInfo = (authToken[0], authToken[1], tuple(authToken[2]))
        else:
            authInfo = ()
        if self.memCachePrefix:
            extraArgs = (self.memCachePrefix,) + extraArgs
        # Hash the common arguments separately to save a few cycles.
        # Microbenchmarks indicate that this adds effectively zero cost even
        # with only one item.
        common = digestlib.sha1(
                str(authInfo + extraArgs + extraKwargs)
                ).digest()
        return [digestlib.sha1(common + str(x)).hexdigest() for x in listArgs]

    def _coalesce(self, authToken, callable, listArg, *extraArgs, **kwargs):
        """Memoize a proxy repository call.

        @param authToken: Caller's credentials, used to partition the saved
            results and in the method call if necessary.
        @param callable: Callable to invoke to retrieve results. It should
            accept a list of queries as the first argument, and return a
            parallel list of results.
        @param listArg: List to pass as the first argument to C{callable}.
        @param extraArgs: Additional positional arguments.
        @param key_prefix: String to prepend to the cache key. (keyword only)
        @param kwargs: Additional keyword arguments.
        """
        key_prefix = kwargs.pop('key_prefix')

        keys = self._getKeys(authToken, listArg, extraArgs, kwargs)
        cachedDict = self.memCache.get_multi(keys, key_prefix = key_prefix)
        finalResults = [ cachedDict.get(x) for x in keys ]

        needed = [ (i, x) for i, x in enumerate(listArg)
                    if keys[i] not in cachedDict ]

        if needed:
            others = callable([x[1] for x in needed], *extraArgs, **kwargs)

            for (i, x), result in itertools.izip(needed, others):
                finalResults[i] = result

            updates = dict( (keys[i], result) for
                            (i, x), result in itertools.izip(needed, others) )
            self.memCache.set_multi(updates,
                            key_prefix = key_prefix,
                            time = self.memCacheTimeout)

        return finalResults

    def lookupFingerprints(self, caller, authToken, chgSetList, recurse,
                           withFiles, withFileContents, excludeAutoSource,
                           mirrorMode):
        return self._coalesce(authToken,
                lambda *args : self._callGetChangeSetFingerprints(
                                    caller, *args),
                chgSetList,
                recurse, withFiles, withFileContents, excludeAutoSource,
                mirrorMode, key_prefix = "FPRINT")

    def getDepsForTroveList(self, caller, authToken, clientVersion, troveList,
                            provides = True, requires = True):
        # this could merge provides/requires in the cache (perhaps always
        # requesting both?), but doesn't
        return self._coalesce(authToken,
                lambda *args, **kwargs :
                        caller.getDepsForTroveList(clientVersion, *args,
                                                   **kwargs),
                troveList, provides = provides, requires = requires,
                key_prefix = "DEPS")

    def getTroveInfo(self, caller, authToken, clientVersion, infoType,
                     troveList):
        return self._coalesce(authToken,
                lambda nTroveList, nInfoType:
                        caller.getTroveInfo(clientVersion, nInfoType,
                                            nTroveList),
                troveList, infoType,
                key_prefix = "TROVEINFO")

class SimpleRepositoryFilter(Memcache, BaseCachingChangesetFilter, RepositoryFilterMixin):
    withCapsuleInjection = False

    # Basic class used for creating repositories with Conary. It places
    # a changeset caching layer on top of an in-memory repository.

    def __init__(self, cfg, basicUrl, repos):
        Memcache.__init__(self, cfg)
        BaseCachingChangesetFilter.__init__(self, cfg, basicUrl)
        RepositoryFilterMixin.__init__(self, repos)

    def getContentsStore(self):
        return self.repos.getContentsStore()


class FileCachingChangesetFilter(BaseCachingChangesetFilter):

    # Adds caching for getFileContents() call to allow proxies to keep
    # those results around

    def __init__(self, cfg, basicUrl):
        BaseCachingChangesetFilter.__init__(self, cfg, basicUrl)
        util.mkdirChain(cfg.proxyContentsDir)
        self.contents = datastore.DataStore(cfg.proxyContentsDir)

    def getFileContents(self, caller, authToken, clientVersion, fileList,
                        authCheckOnly = False):
        if clientVersion < 42:
            # server doesn't support auth checks through getFileContents
            return caller.getFileContents(clientVersion, fileList, authCheckOnly)

        hasFiles = []
        neededFiles = []

        for encFileId, encVersion in fileList:
            fileId = sha1helper.sha1ToString(self.toFileId(encFileId))
            if self.contents.hasFile(fileId + '-c'):
                path = self.contents.hashToPath(fileId + '-c')
                pathfd = None
                try:
                    try:
                        # touch the file; we don't want it to be removed
                        # by a cleanup job when we need it
                        pathfd = os.open(path, os.O_RDONLY)
                        hasFiles.append((encFileId, encVersion))
                        continue
                    except OSError:
                        pass
                finally:
                    if pathfd: os.close(pathfd)

            neededFiles.append((encFileId, encVersion))

        # make sure this user has permissions for these file contents. an
        # exception will get raised if we don't have sufficient permissions
        if hasFiles:
            caller.getFileContents(clientVersion, hasFiles, True)

        if neededFiles:
            fcCapsuleInfoList = self._getFileContentsCapsuleInfo(caller,
                clientVersion, neededFiles)
            # Use the indexer to pull down contents
            newNeededFiles = []
            indexerContent = dict()
            for (encFileId, encVersion), fcCapsuleInfo in zip(
                    neededFiles, fcCapsuleInfoList):
                if fcCapsuleInfo is None:
                    newNeededFiles.append((encFileId, encVersion))
                    continue
                capsuleType, capsuleKey, capsuleSha1, filePath, fileSha1 = \
                    fcCapsuleInfo
                # We will group files by capsule, so we can efficiently
                # extract all files in a single pass
                indexerContent.setdefault(
                    (capsuleType, capsuleKey, capsuleSha1), []).append(
                    ((filePath, fileSha1), (encFileId, encVersion)))

            if indexerContent:
                dl = self.CapsuleDownloader(self.cfg.capsuleServerUrl)
                for (capsuleType, capsuleKey, capsuleSha1), valList in \
                        indexerContent.items():
                    fileInfoList = [ x[0] for x in valList ]
                    fileObjs = dl.downloadCapsuleFiles(capsuleKey, capsuleSha1,
                        fileInfoList)
                    for fileObj, ((filePath, fileSha1),
                            (encFileId, encVersion)) in zip(fileObjs, valList):
                        fobj = self._compressStreamContents(fileObj.f)
                        self._cacheFileContents(encFileId, fobj)
            neededFiles = newNeededFiles

        if neededFiles:
            # now get the contents we don't have cached
            (url, sizes) = caller.getFileContents(
                    clientVersion, neededFiles, False)
            url = self._localUrl(url)
            self._saveFileContents(neededFiles, url, sizes,
                    forceProxy=caller._lastProxy)

        url, sizes = self._saveFileContentsChangeset(clientVersion, fileList)
        return url, sizes

    def _saveFileContents(self, fileList, url, sizes, forceProxy):
        # insure that the size is an integer -- protocol version
        # 44 returns a string to avoid XML-RPC marshal limits
        sizes = [ int(x) for x in sizes ]

        if hasattr(url, "read"):
            dest = url
            dest.seek(0, 2)
            size = dest.tell()
            dest.seek(0)
        else:
            (fd, tmpPath) = tempfile.mkstemp(dir = self.cfg.tmpDir,
                                             suffix = '.tmp')
            dest = util.ExtendedFile(tmpPath, "w+", buffering = False)
            os.close(fd)
            os.unlink(tmpPath)
            inUrl = transport.ConaryURLOpener(proxyMap=self.proxyMap).open(url,
                    forceProxy=forceProxy)
            size = util.copyfileobj(inUrl, dest)
            inUrl.close()
            dest.seek(0)

        totalSize = sum(sizes)
        start = 0

        # We skip the integrity check here because (1) the hash we're using
        # has '-c' applied and (2) the hash is a fileId sha1, not a file
        # contents sha1
        for (encFileId, envVersion), size in itertools.izip(fileList,
                                                            sizes):
            nestedF = util.SeekableNestedFile(dest, size, start)
            self._cacheFileContents(encFileId, nestedF)
            totalSize -= size
            start += size

        assert(totalSize == 0)
        # this closes the underlying fd opened by mkstemp for us
        dest.close()

    def _saveFileContentsChangeset(self, clientVersion, fileList):
        (fd, path) = tempfile.mkstemp(dir = self.tmpPath,
                                      suffix = '.cf-out')
        sizeList = []

        try:
            for encFileId, encVersion in fileList:
                fileId = sha1helper.sha1ToString(self.toFileId(encFileId))
                filePath = self.contents.hashToPath(fileId + '-c')
                size = os.stat(filePath).st_size
                sizeList.append(size)

                # 0 means it's not a changeset
                # 1 means it is cached (don't erase it after sending)
                os.write(fd, "%s %d 0 1\n" % (filePath, size))

            url = os.path.join(self.urlBase(),
                               "changeset?%s" % os.path.basename(path)[:-4])

            # client versions >= 44 use strings instead of ints for size
            # because xmlrpclib can't marshal ints > 2GiB
            if clientVersion >= 44:
                sizeList = [ str(x) for x in sizeList ]
            else:
                for size in sizeList:
                    if size >= 0x80000000:
                        raise errors.InvalidClientVersion(
                             'This version of Conary does not support '
                             'downloading file contents larger than 2 '
                             'GiB.  Please install a new Conary client.')
            return (url, sizeList)
        finally:
            os.close(fd)


    def _getFileContentsCapsuleInfo(self, caller, clientVersion, neededFiles):
        # If proxy is not configured to talk to a capsule indexer, don't
        # bother checking for additional information
        if not self.cfg.capsuleServerUrl:
            return [ None ] * len(neededFiles)
        return ChangesetFilter._getFileContentsCapsuleInfo(self,
            caller, clientVersion, neededFiles)

    def _cacheFileContents(self, encFileId, fileObj):
        # We skip the integrity check here because (1) the hash we're using
        # has '-c' applied and (2) the hash is a fileId sha1, not a file
        # contents sha1
        fileId = sha1helper.sha1ToString(self.toFileId(encFileId))
        self.contents.addFile(fileObj, fileId + '-c',
                                      precompressed = True,
                                      integrityCheck = False)

    def _compressStreamContents(self, src):
        """
        Compress the contents of the source stream
        Return a stream
        """
        fobj = util.BoundedStringIO()
        gz = util.gzip.GzipFile(mode="w", fileobj=fobj)
        util.copyStream(src, gz)
        gz.close()
        fobj.seek(0)
        return fobj

class ProxyRepositoryServer(Memcache, FileCachingChangesetFilter):

    # class for proxy servers used by standalone and apache implementations
    # adds a proxy specific version of getFileContentsFromTrove()

    SERVER_VERSIONS = range(42, netserver.SERVER_VERSIONS[-1] + 1)
    forceSingleCsJob = False
    withCapsuleInjection = True

    def __init__(self, cfg, basicUrl):
        Memcache.__init__(self, cfg)
        FileCachingChangesetFilter.__init__(self, cfg, basicUrl)
        self.callFactory = ProxyCallFactory()

    def setBaseUrlOverride(self, rawUrl, headers, isSecure):
        # Setting it to None here will make urlBase() do the right thing
        proxyHost = headers.get('X-Conary-Proxy-Host', None)
        if not proxyHost:
            self._baseUrlOverride = None
            return
        # We really don't want to use rawUrl in the proxy, that points to the
        # server and it won't help rewriting URLs with that address
        self._baseUrlOverride = headers.get('X-Conary-Proxy-Host', None)

        proto = (isSecure and "https") or "http"

        if rawUrl.startswith('/'):
            self._baseUrlOverride = '%s://%s%s' % (proto, proxyHost, rawUrl)
        else:
            items = list(urlparse.urlparse(rawUrl))
            items[0] = proto
            items[1] = proxyHost
            self._baseUrlOverride = urlparse.urlunparse(items)


    def getFileContentsFromTrove(self, caller, authToken, clientVersion,
                                 troveName, version, flavor, pathList):
        (url, sizes) = caller.getFileContentsFromTrove(
                clientVersion, troveName, version, flavor, pathList)

        # XXX This look too similar to _saveFileContents* - at some point we
        # should refactor this code to call those.

        # insure that the size is an integer -- protocol version
        # 44 returns a string to avoid XML-RPC marshal limits
        sizes = [ int(x) for x in sizes ]

        (fd, tmpPath) = tempfile.mkstemp(dir = self.cfg.tmpDir,
                                         suffix = '.tmp')
        dest = util.ExtendedFile(tmpPath, "w+", buffering = False)
        os.close(fd)
        os.unlink(tmpPath)
        inUrl = transport.ConaryURLOpener(proxyMap=self.proxyMap).open(url,
                forceProxy=caller._lastProxy)
        size = util.copyfileobj(inUrl, dest)
        inUrl.close()
        dest.seek(0)

        totalSize = sum(sizes)
        start = 0

        # We skip the integrity check here because (1) the hash we're using
        # has '-c' applied and (2) the hash is a fileId sha1, not a file
        # contents sha1
        fileList = []
        for size in sizes:
            nestedF = util.SeekableNestedFile(dest, size, start)
            (fd, tmpPath) = tempfile.mkstemp(dir = self.cfg.tmpDir,
                                             suffix = '.tmp')
            size = util.copyfileobj(nestedF, os.fdopen(fd, 'w'))
            totalSize -= size
            start += size
            fileList.append(tmpPath)

        assert(totalSize == 0)
        # this closes the underlying fd opened by mkstemp for us
        dest.close()

        (fd, path) = tempfile.mkstemp(dir = self.tmpPath,
                                      suffix = '.cf-out')
        sizeList = []

        try:
            for filePath in fileList:
                size = os.stat(filePath).st_size
                sizeList.append(size)

                # 0 means it's not a changeset
                # 0 means it is not cached (erase it after sending)
                os.write(fd, "%s %d 0 0\n" % (filePath, size))

            url = os.path.join(self.urlBase(),
                               "changeset?%s" % os.path.basename(path)[:-4])

            # client versions >= 44 use strings instead of ints for size
            # because xmlrpclib can't marshal ints > 2GiB
            if clientVersion >= 44:
                sizeList = [ str(x) for x in sizeList ]
            else:
                for size in sizeList:
                    if size >= 0x80000000:
                        raise errors.InvalidClientVersion(
                             'This version of Conary does not support '
                             'downloading file contents larger than 2 '
                             'GiB.  Please install a new Conary client.')
            return (url, sizeList)
        finally:
            os.close(fd)

class CachingRepositoryServer(FileCachingChangesetFilter, RepositoryFilterMixin):
    withCapsuleInjection = False

    # Base class for creating a repository which has file content injection.
    # This is not used by Conary itself, but is used by rBuilder for creating
    # repositories which need injected capsule content. Adds a
    # getFileContents() call which can handle content injections.

    def __init__(self, cfg, basicUrl, repos):
        FileCachingChangesetFilter.__init__(self, cfg, basicUrl)
        RepositoryFilterMixin.__init__(self, repos)

    def getFileContents(self, caller, authToken, clientVersion, fileList,
                        authCheckOnly = False):
        # Are any of the requested files capsule related?
        fcInfoList = self._getFileContentsCapsuleInfo(caller,
            clientVersion, fileList)
        # Compute indices of capsule-based files, we will need them later so
        # we can reconstruct the results
        capsuleBasedIndices = [ i for (i, x) in enumerate(fcInfoList)
            if x is not None ]
        otherIndices = [ i for (i, x) in enumerate(fcInfoList)
            if x is None ]
        capsuleBasedFileList = [ fileList[i] for i in capsuleBasedIndices ]
        otherFileList = [ fileList[i] for i in otherIndices ]
        # The case where one requests both capsule-based and non-capsule-based
        # files is a bit more tedious, since we need to re-create the
        # changeset. So if exactly one type of file contents are requested, we
        # return immediately.
        if capsuleBasedFileList:
            result = FileCachingChangesetFilter.getFileContents(self,
                caller, authToken, clientVersion, capsuleBasedFileList,
                authCheckOnly=authCheckOnly)
            if not otherFileList:
                return result
            elif not authCheckOnly:
                url, sizes = result
                url = self._localUrl(url)
                self._saveFileContents(capsuleBasedFileList, url, sizes,
                        forceProxy=caller._lastProxy)
        if otherFileList:
            result = caller.getFileContents(clientVersion, otherFileList,
                authCheckOnly)
            if not capsuleBasedFileList:
                return result
            elif not authCheckOnly:
                url, sizes = result
                url = self._localUrl(url)
                self._saveFileContents(otherFileList, url, sizes,
                        forceProxy=caller._lastProxy)
        # Now reassemble the results
        if authCheckOnly:
            # The getFileContents calls above will raise an exception if
            # something failed the auth check.
            return True
        else:
            return self._saveFileContentsChangeset(clientVersion, fileList)

    def getContentsStore(self):
        return self.repos.getContentsStore()


class ChangesetCache(object):

    # Provides a place to cache changeset; uses a directory for them
    # all indexed by fingerprint

    def __init__(self, dataStore, logPath=None):
        self.dataStore = dataStore
        self.logPath = logPath
        self.locksMap = {}
        # Use only 1/4 our file descriptor limit for locks
        limit = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
        self.maxLocks = limit / 4

    def hashKey(self, key):
        (fingerPrint, csVersion) = key
        return self.dataStore.hashToPath(fingerPrint + '-%d' % csVersion)

    def set(self, key, value):
        (csInfo, inF, sizeLimit) = value

        csPath = self.hashKey(key)
        dataPath = csPath + '.data'
        csDir = os.path.dirname(csPath)
        util.mkdirChain(csDir)

        csObj = self.locksMap.get(csPath)
        if csObj is None:
            # We did not get a lock for it
            csObj = util.AtomicFile(csPath, tmpsuffix = '.ccs-new')

        written = util.copyfileobj(inF, csObj, sizeLimit=sizeLimit)
        if sizeLimit is not None and written != sizeLimit:
            raise errors.RepositoryError("Changeset was truncated in transit "
                    "(expected %d bytes, got %d bytes for subchangeset)" %
                    (sizeLimit, written))

        csInfoObj = util.AtomicFile(dataPath, tmpsuffix = '.data-new')
        csInfoObj.write(csInfo.pickle())

        csInfoObj.commit()
        csObj.commit()
        # If we locked the cache file, we need to no longer track it
        self.locksMap.pop(csPath, None)

        self._log('WRITE', key, size=sizeLimit)

        return csPath

    def get(self, key, shouldLock = True):
        csPath = self.hashKey(key)
        csVersion = key[1]
        dataPath = csPath + '.data'
        if len(self.locksMap) >= self.maxLocks:
            shouldLock = False

        lockfile = util.LockedFile(csPath)
        util.mkdirChain(os.path.dirname(csPath))
        fileObj = lockfile.open(shouldLock=shouldLock)

        dataFile = util.fopenIfExists(dataPath, "r")

        # Use XOR - if one is None and one is not, we need to regenerate
        if (fileObj is not None) ^ (dataFile is not None):
            # We have csPath but not dataPath, or the other way around
            if not shouldLock:
                return None
            # Get rid of csPath - no other process can produce it because
            # we're holding the lock
            util.removeIfExists(csPath)
            util.removeIfExists(dataPath)
            # Unlock
            lockfile.close()
            # Try again
            fileObj = lockfile.open()

        if fileObj is None:
            if shouldLock:
                # We got the lock on csPath
                self.locksMap[csPath] = lockfile
            self._log('MISS', key)
            return None

        # touch to refresh atime
        # This makes sure tmpwatch will not remove this file while we are
        # reading it (which would not hurt this process, but would invalidate
        # a perfectly good cache entry)
        for fobj in [ fileObj, dataFile ]:
            fobj.read(1)
            fobj.seek(0)

        try:
            csInfo = ChangeSetInfo(pickled = dataFile.read())
            dataFile.close()
        except IOError, err:
            self._log('MISS', key, errno=err.errno)
            return None

        csInfo.path = csPath
        csInfo.cached = True
        csInfo.version = csVersion

        self._log('HIT', key)

        return csInfo

    def resetLocks(self):
        self.locksMap.clear()

    def _log(self, status, key, **kwargs):
        """Log a HIT/MISS/WRITE to file."""
        if self.logPath is None:
            return
        now = time.time()
        msecs = (now - long(now)) * 1000
        extra = ''.join(' %s=%r' % (x, y) for (x, y) in kwargs.items())
        rec = '%s,%03d %s-%d %s%s\n' % (
                time.strftime('%F %T', time.localtime(now)), msecs,
                key[0], key[1], status, extra)
        open(self.logPath, 'a').write(rec)

class AuthenticationInformation(object):

    # summarizes authentication information to keep in a cache

    __slots__ = ( 'name', 'pw', 'entitlements' )

    def __init__(self, authToken, entitlements):
        self.name = authToken[0]
        # this will
        self.pw = sha1helper.sha1ToString(authToken[1])
        self.entitlements = sorted(entitlements)

def redirectUrl(authToken, url):
    # return the url to use for the final server
    s = url.split('/')
    s[2] = ('%s:%s@' % (netclient.quote(authToken[0]),
                        netclient.quote(authToken[1]))) + s[2]
    url = '/'.join(s)

    return url


def formatViaHeader(localAddr, protocolString, prefix=''):
    via = "%s %s (Conary/%s)" % (protocolString, localAddr,
                                  constants.version)
    if prefix:
        return prefix + ', ' + via
    else:
        return via


class ProxyRepositoryError(Exception):

    def __init__(self, name, args, kwArgs):
        self.name = name
        self.args = tuple(args)
        self.kwArgs = kwArgs


class ChangesetFileReader(object):
    def __init__(self, tmpDir):
        self.tmpDir = tmpDir

    @staticmethod
    def readNestedFile(name, tag, rawSize, subfile, contentsStore):
        """Use with ChangeSet.dumpIter to handle external file references."""
        if changeset.ChangedFileTypes.refr[4:] == tag[2:]:
            # this is a reference to a compressed file in the contents store
            entry = subfile.read()
            sha1, expandedSize = entry.split(' ')
            expandedSize = int(expandedSize)
            tag = tag[0:2] + changeset.ChangedFileTypes.file[4:]
            path = contentsStore.hashToPath(sha1)
            fobj = open(path, 'rb')
            return tag, expandedSize, util.iterFileChunks(fobj)
        else:
            # this is data from the changeset itself
            return tag, rawSize, util.iterFileChunks(subfile)

    def getItems(self, fileName):
        localName = self.tmpDir + "/" + fileName + "-out"
        if os.path.realpath(localName) != localName:
            return None

        if localName.endswith(".cf-out"):
            try:
                f = open(localName, "r")
            except IOError:
                return None

            os.unlink(localName)

            items = []
            for l in f.readlines():
                (path, size, isChangeset, preserveFile) = l.split()
                size = int(size)
                isChangeset = int(isChangeset)
                preserveFile = int(preserveFile)
                items.append((path, size, isChangeset, preserveFile))
            f.close()
            del f
        else:
            try:
                size = os.stat(localName).st_size;
            except OSError:
                return None
            items = [ (localName, size, 0, 0) ]
        return items

    def writeItems(self, items, wfile, contentsStore=None):
        for path, size, isChangeset, preserveFile in items:
            if isChangeset:
                cs = filecontainer.FileContainer(util.ExtendedFile(path,
                                                     buffering = False))
                for data in cs.dumpIter(self.readNestedFile,
                        args=(contentsStore,)):
                    wfile.write(data)

                del cs
            else:
                f = open(path)
                util.copyfileobj(f, wfile)

            if not preserveFile:
                os.unlink(path)

# ewtroan: for the internal proxy, we support client version 38 but need to talk to a server which is at least version 41
# ewtroan: for external proxy, we support client version 41 and need a server which is at least 41
# ewtroan: and when I get a call, I need to know what version the server is, which I can't keep anywhere as state
# ewtroan: but I can't very well tell a client version 38 to call back with server version 41
# Gafton: hmm - is there a way to differentiate your internal/external state in the code ?
# ewtroan: I'm going to split the classes
# ***ewtroan copies some code around
# Gafton: same basic class with different dressings?
# ewtroan: I set the fullproxy to be versions 41-43
# ewtroan: while the changeset caching advertises versions 38-43
# ewtroan: it works because the internal proxy only talks to the internal repository, and those don't really need to explicitly negotiate
# ewtroan: not a perfect model, but good enough
# Gafton: okay, that makes sense
# ewtroan: and I'll make the internal one override the protocol version to call into the bottom one with for getChangeSet() and for the external one use the protocol version the client asked for
# ewtroan: which will all work fine with the autoconverstion of formats in the proxy
