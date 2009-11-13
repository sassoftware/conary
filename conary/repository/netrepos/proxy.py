#
# Copyright (c) 2007-2009 rPath, Inc.
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
#

import base64, cPickle, itertools, os, tempfile, urllib, urllib2, urlparse

from conary import constants, conarycfg, rpmhelper, trove, versions
from conary.lib import digestlib, sha1helper, tracelog, util
from conary.repository import changeset, datastore, errors, netclient
from conary.repository import filecontainer, transport, xmlshims
from conary.repository.netrepos import netserver, reposlog

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
        basicUrl = util.stripUserPassFromUrl(caller.url)
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

    def callByName(self, methodname, *args, **kwargs):
        # args[0] is protocolVersion
        if args[0] < 51:
            # older protocol versions didn't allow keyword arguments
            assert(not kwargs)
        else:
            args = [ args[0], args[1:], kwargs ]

        try:
            rc = self.proxy.__getattr__(methodname)(*args)
        except IOError, e:
            raise errors.ProxyError(e.strerror[1])
        except util.xmlrpclib.ProtocolError, e:
            if e.errcode == 403:
                raise errors.InsufficientPermission

            raise

        if args[0] < 60:
            # strip off useAnonymous flag
            rc = rc[1:3]

        if rc[0]:
            # exception occured. this lets us tunnel the error through
            # without instantiating it (which would be demarshalling the
            # thing just to remarshall it again)
            if args[0] < 60:
                raise ProxyRepositoryError(rc[1][0], rc[1][1:], None)
            else:
                # keyword args to exceptions appear
                raise ProxyRepositoryError(rc[1][0], rc[1][1], rc[1][2])

        return rc[1]

    def getExtraInfo(self):
        """Return extra information if available"""
        return ExtraInfo(self._transport.responseHeaders,
                         self._transport.responseProtocol)

    def __getattr__(self, method):
        # Don't invoke methods that start with __
        if method.startswith('__'):
            raise AttributeError(method)
        return lambda *args, **kwargs: self.callByName(method, *args, **kwargs)

    def __init__(self, url, proxy, transport):
        self.url = util.stripUserPassFromUrl(url)
        self.proxy = proxy
        self._transport = transport

class ProxyCallFactory:

    @staticmethod
    def createCaller(protocol, port, rawUrl, proxies, authToken, localAddr,
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

        urlProtocol, urlRest = urllib.splittype(url)
        urlUserHost, urlRest = urllib.splithost(urlRest)
        _, urlHost = urllib.splituser(urlUserHost)
        # If the proxy injected entitlements or user information, switch to
        # SSL -- IF they are using
        # default ports (if they specify ports, we have no way of
        # knowing what port to use)
        addSSL = ':' not in urlHost and (bool(injEntList) or bool(userOverride))
        withSSL = urlProtocol == 'https' or addSSL
        transporter = transport.Transport(https = withSSL,
                                          proxies = proxies,
                                          serverName = targetServerName)
        transporter.setExtraHeaders(lheaders)
        transporter.setEntitlements(entitlementList)

        transporter.setCompress(True)
        proxy = ProxyClient(url, transporter)

        return ProxyCaller(url, proxy, transporter)

class RepositoryCaller(xmlshims.NetworkConvertors):

    def callByName(self, methodname, *args, **kwargs):
        rc = self.repos.callWrapper(self.protocol, self.port, methodname,
                                    self.authToken, args, kwargs,
                                    remoteIp = self.remoteIp,
                                    rawUrl = self.rawUrl,
                                    isSecure = self.isSecure)

        return rc

    def getExtraInfo(self):
        """No extra information available for a RepositoryCaller"""
        return None

    def __getattr__(self, method):
        # Don't invoke methods that start with __
        if method.startswith('__'):
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

class RepositoryCallFactory:

    def __init__(self, repos, logger):
        self.repos = repos
        self.log = logger

    def createCaller(self, protocol, port, rawUrl, proxies, authToken,
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

    def __init__(self, cfg, basicUrl):
        self.cfg = cfg
        self.basicUrl = basicUrl
        self.logFile = cfg.logFile
        self.tmpPath = cfg.tmpDir
        util.mkdirChain(self.tmpPath)
        self.proxies = conarycfg.getProxyFromConfig(cfg)
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

    def callWrapper(self, protocol, port, methodname, authToken, args,
                    remoteIp = None, rawUrl = None, localAddr = None,
                    protocolString = None, headers = None, isSecure = False):
        """
        @param localAddr: if set, a string host:port identifying the address
        the client used to talk to us.
        @param protocolString: if set, the protocol version the client used
        (i.e. HTTP/1.0)
        """
        if methodname not in self.publicCalls:
            if protocol < 60:
                return (False, True, ("MethodNotSupported", methodname, ""),
                        None)
            else:
                return (True, ("MethodNotSupported", methodname, ""), None)

        self._port = port
        self._protocol = protocol

        self.setBaseUrlOverride(rawUrl, headers, isSecure)

        targetServerName = headers.get('X-Conary-Servername', None)

        # simple proxy. FIXME: caching these might help; building all
        # of this framework for every request seems dumb. it seems like
        # we could get away with one total since we're just changing
        # hostname/username/entitlement
        caller = self.callFactory.createCaller(protocol, port, rawUrl,
                                               self.proxies, authToken,
                                               localAddr, protocolString,
                                               headers, self.cfg,
                                               targetServerName,
                                               remoteIp, isSecure,
                                               self.urlBase())

        # args[0] is the protocol version
        protocolVersion = args[0]
        if args[0] < 51:
            kwargs = {}
        else:
            assert(len(args) == 3)
            kwargs = args[2]
            args = [ args[0], ] + args[1]

        extraInfo = None

        try:
            if hasattr(self, methodname):
                # handled internally
                method = self.__getattribute__(methodname)

                if self.callLog:
                    self.callLog.log(remoteIp, authToken, methodname, args,
                                     kwargs)

                r = method(caller, authToken, *args, **kwargs)
            else:
                r = caller.callByName(methodname, *args, **kwargs)

            r = (False, r)
            extraInfo = caller.getExtraInfo()
        except ProxyRepositoryError, e:
            r = (True, (e.name, e.args, e.kwArgs))
        except Exception, e:
            if hasattr(e, 'marshall'):
                marshalled = e.marshall(self)
                args, kwArgs = marshalled

                r = (True,
                        (e.__class__.__name__, args, kwArgs) )
            else:
                r = None
                for klass, marshall in errors.simpleExceptions:
                    if isinstance(e, klass):
                        r = (True, (marshall, (str(e),), {}) )

                if r is None:
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

        if protocolVersion < 60:
            if r[0] is True:
                # return (useAnon, isException, (exceptName,) + ordArgs) )
                return (False, True, (r[1][0],) + r[1][1], extraInfo)
            else:
                return (False, False, r[1], extraInfo)

        return r + (extraInfo,)


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

    forceGetCsVersion = None
    forceSingleCsJob = False
    isRepositoryFilter = True

    def __init__(self, cfg, basicUrl, cache):
        BaseProxy.__init__(self, cfg, basicUrl)
        self.csCache = cache

    def _cvtJobEntry(self, authToken, jobEntry):
        (name, (old, oldFlavor), (new, newFlavor), mbsolute) = jobEntry

        newVer = self.toVersion(new)

        if old == 0:
            l = (name, (None, None),
                       (self.toVersion(new), self.toFlavor(newFlavor)),
                       absolute)
        else:
            l = (name, (self.toVersion(old), self.toFlavor(oldFlavor)),
                       (self.toVersion(new), self.toFlavor(newFlavor)),
                       absolute)
        return l

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

        changeSetList = self._getNeededChangeSets(caller,
            authToken, verPath, chgSetList, serverVersion,
            getCsVersion, wireCsVersion, neededCsVersion,
            recurse, withFiles, withFileContents, excludeAutoSource,
            mirrorMode, infoOnly)

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

        for jobIdx, (rawJob, fingerprint) in \
                    enumerate(itertools.izip(chgSetList, fingerprints)):
            # if we have both a cs fingerprint and a cache, then we will
            # cache the cs for this job
            cachable = bool(fingerprint)
            if not cachable:
                continue

            # look up the changeset in the cache, oldest to newest
            for iterV in verPath:
                csInfo = self.csCache.get((fingerprint, iterV))
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

        fingerprints = self._callGetChangeSetFingerprints(caller, chgSetList,
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
            if (not self.isRepositoryFilter and changeSetsNeeded and
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
                        cs = self._reassembleChangeSet(csInfo, ncCsInfoMap,
                            absOldChangeSetMap)
                        (fd, tmppath) = tempfile.mkstemp(dir = self.cfg.tmpDir,
                                                         suffix = ".ccs-temp")
                        f = os.fdopen(fd, "w")
                        size = cs.appendToFile(f)
                        f.close()
                        csInfo.size = size
                        csInfo.fingerprint = fprint

                        # Cache the changeset
                        csPath = self.csCache.set(
                            (csInfo.fingerprint, csInfo.version),
                            (csInfo, file(tmppath), size))
                        csInfo.path = csPath
                        os.unlink(tmppath)
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

        for (url, csInfoList), neededHere in zip(urlInfoList, neededList):
            if url is None:
                # Only size information was received; nothing further needed
                continue
            self._cacheChangeSet(url, neededHere, csInfoList, changeSetList)

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

    def _cacheChangeSet(self, url, neededHere, csInfoList, changeSetList):
        try:
            inF = transport.ConaryURLOpener(proxies = self.proxies).open(url)
        except transport.TransportError, e:
            raise errors.RepositoryError(e.args[0])

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
        getFrozenFileFlags = csfiles.frozenFileFlags
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
                        trv = oldTrove.copy()
                        trv.applyChangeSet(trvCs)
                        path = trv.getFile(pathId)[0]

                    fileObj = self._getFileObject(pathId, fileId, oldTrove,
                        oldChangeset, newCs)

                if not fileObj.flags.isConfig() or isinstance(fileObj,
                        csfiles.SymbolicLink):
                    continue
                configFileSha1 = fileObj.contents.sha1()
                assert not fileObj.flags.isPayload()

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
        return newCs

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
            if capsuleSha1sum == fileSha1sum:
                # Troves do contain the capsule too, it's legitimate to
                # request it; however, we can fall back to downloadCapsule for
                # it
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

class SimpleRepositoryFilter(ChangesetFilter):

    forceGetCsVersion = ChangesetFilter.SERVER_VERSIONS[-1]
    forceSingleCsJob = False
    isRepositoryFilter = True

    def __init__(self, cfg, basicUrl, repos):
        if cfg.changesetCacheDir:
            util.mkdirChain(cfg.changesetCacheDir)
            csCache = ChangesetCache(datastore.DataStore(cfg.changesetCacheDir))
        else:
            csCache = None

        ChangesetFilter.__init__(self, cfg, basicUrl, csCache)
        self.repos = repos
        self.callFactory = RepositoryCallFactory(repos, self.log)

    def getFileContents(self, caller, authToken, clientVersion, fileList,
                        authCheckOnly = False):
        if self.cfg.excludeCapsuleContents:
            # Are any of the requested files capsule related?
            fcInfoList = self._getFileContentsCapsuleInfo(caller,
                clientVersion, fileList)
            fcInfoList = [ x for x in fcInfoList if x is not None ]
            if fcInfoList:
                raise errors.CapsuleServingDenied(
                    "Request for contents for a file belonging to a capsule, "
                    "based on capsules, from repository denying such operation")

        return caller.getFileContents(clientVersion, fileList,
            authCheckOnly = authCheckOnly)


class ProxyRepositoryServer(ChangesetFilter):

    SERVER_VERSIONS = range(42, netserver.SERVER_VERSIONS[-1] + 1)
    forceSingleCsJob = False
    isRepositoryFilter = False

    def __init__(self, cfg, basicUrl):
        util.mkdirChain(cfg.changesetCacheDir)
        csCache = ChangesetCache(datastore.DataStore(cfg.changesetCacheDir))

        util.mkdirChain(cfg.proxyContentsDir)
        self.contents = datastore.DataStore(cfg.proxyContentsDir)

        ChangesetFilter.__init__(self, cfg, basicUrl, csCache)

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

        # insure that the size is an integer -- protocol version
        # 44 returns a string to avoid XML-RPC marshal limits
        sizes = [ int(x) for x in sizes ]

        (fd, tmpPath) = tempfile.mkstemp(dir = self.cfg.tmpDir,
                                         suffix = '.tmp')
        dest = util.ExtendedFile(tmpPath, "w+", buffering = False)
        os.close(fd)
        os.unlink(tmpPath)
        inUrl = transport.ConaryURLOpener(proxies = self.proxies).open(url)
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
                        self._cacheFileContents(encFileId, fileObj.f)
            neededFiles = newNeededFiles

        if neededFiles:
            # now get the contents we don't have cached
            (url, sizes) = caller.getFileContents(
                    clientVersion, neededFiles, False)
            # insure that the size is an integer -- protocol version
            # 44 returns a string to avoid XML-RPC marshal limits
            sizes = [ int(x) for x in sizes ]

            (fd, tmpPath) = tempfile.mkstemp(dir = self.cfg.tmpDir,
                                             suffix = '.tmp')
            dest = util.ExtendedFile(tmpPath, "w+", buffering = False)
            os.close(fd)
            os.unlink(tmpPath)
            inUrl = transport.ConaryURLOpener(proxies = self.proxies).open(url)
            size = util.copyfileobj(inUrl, dest)
            inUrl.close()
            dest.seek(0)

            totalSize = sum(sizes)
            start = 0

            # We skip the integrity check here because (1) the hash we're using
            # has '-c' applied and (2) the hash is a fileId sha1, not a file
            # contents sha1
            for (encFileId, envVersion), size in itertools.izip(neededFiles,
                                                                sizes):
                nestedF = util.SeekableNestedFile(dest, size, start)
                self._cacheFileContents(encFileId, nestedF)
                totalSize -= size
                start += size

            assert(totalSize == 0)
            # this closes the underlying fd opened by mkstemp for us
            dest.close()

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

class ChangesetCache(object):
    __slots__ = ['dataStore']

    def __init__(self, dataStore):
        self.dataStore = dataStore

    def hashKey(self, key):
        (fingerPrint, csVersion) = key
        return self.dataStore.hashToPath(fingerPrint + '-%d' % csVersion)

    def set(self, key, value):
        (csInfo, inF, sizeLimit) = value

        csPath = self.hashKey(key)
        csDir = os.path.dirname(csPath)
        util.mkdirChain(csDir)
        (fd, csTmpPath) = tempfile.mkstemp(dir = csDir,
                                           suffix = '.ccs-new')
        outF = os.fdopen(fd, "w")
        util.copyfileobj(inF, outF, sizeLimit = sizeLimit)
        # closes the underlying fd opened by mkstemp
        outF.close()

        (fd, dataTmpPath) = tempfile.mkstemp(dir = csDir,
                                             suffix = '.data-new')
        data = os.fdopen(fd, 'w')
        data.write(csInfo.pickle())
        # closes the underlying fd
        data.close()

        os.rename(csTmpPath, csPath)
        os.rename(dataTmpPath, csPath + '.data')

        return csPath

    def get(self, key):
        csPath = self.hashKey(key)
        csVersion = key[1]
        dataPath = csPath + '.data'
        if not (os.path.exists(csPath) and os.path.exists(dataPath)):
            # Not in the cache
            return None

        # touch to refresh atime; try/except protects against race
        # with someone removing the entry during the time it took
        # you to read this comment
        try:
            fd = os.open(csPath, os.O_RDONLY)
            os.close(fd)
        except OSError:
            pass

        try:
            data = open(dataPath)
            csInfo = ChangeSetInfo(pickled = data.read())
            data.close()
        except IOError:
            return None

        csInfo.path = csPath
        csInfo.cached = True
        csInfo.version = csVersion

        return csInfo

def redirectUrl(authToken, url):
    # return the url to use for the final server
    s = url.split('/')
    s[2] = ('%s:%s@' % (netclient.quote(authToken[0]),
                        netclient.quote(authToken[1]))) + s[2]
    url = '/'.join(s)

    return url

def formatViaHeader(localAddr, protocolString):
    return "%s %s (Conary/%s)" % (protocolString, localAddr,
                                  constants.version)

class ProxyRepositoryError(Exception):

    def __init__(self, name, args, kwArgs):
        self.name = name
        self.args = tuple(args)
        self.kwArgs = kwArgs

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
