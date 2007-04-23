#
# Copyright (c) 2007 rPath, Inc.
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

import base64, cPickle, itertools, os, tempfile, urllib, xmlrpclib

from conary import conarycfg, trove
from conary.lib import sha1helper, tracelog, util
from conary.repository import changeset, datastore, errors, netclient
from conary.repository import filecontainer, transport, xmlshims
from conary.repository.netrepos import netserver, calllog

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

class ProxyClient(xmlrpclib.ServerProxy):

    pass

class ProxyCaller:

    def callByName(self, methodname, *args):
        try:
            rc = self.proxy.__getattr__(methodname)(*args)
        except IOError, e:
            rc = [ False, True, [ 'ProxyError', e.strerror[1] ] ]
        except xmlrpclib.ProtocolError, e:
            if e.errcode == 403:
                raise errors.InsufficientPermission

            raise

        if rc[1]:
            # exception occured
            raise ProxyRepositoryError(rc[2])

        return (rc[0], rc[2])

    def __getattr__(self, method):
        return lambda *args: self.callByName(method, *args)

    def __init__(self, proxy):
        self.proxy = proxy

class ProxyCallFactory:

    @staticmethod
    def createCaller(protocol, port, rawUrl, proxies, authToken):
        url = redirectUrl(authToken, rawUrl)

        if authToken[2] is not None:
            entitlement = authToken[2:4]
        else:
            entitlement = None

        transporter = transport.Transport(https = url.startswith('https:'),
                                          entitlement = entitlement,
                                          proxies = proxies)

        transporter.setCompress(True)
        proxy = ProxyClient(url, transporter)

        return ProxyCaller(proxy)

class RepositoryCaller:

    def callByName(self, methodname, *args):
        rc = self.repos.callWrapper(self.protocol, self.port, methodname,
                                    self.authToken, args)

        if rc[1]:
            # exception occured
            raise ProxyRepositoryError(rc[2])

        return (rc[0], rc[2])

    def __getattr__(self, method):
        return lambda *args: self.callByName(method, *args)

    def __init__(self, protocol, port, authToken, repos):
        self.repos = repos
        self.protocol = protocol
        self.port = port
        self.authToken = authToken

class RepositoryCallFactory:

    def __init__(self, repos):
        self.repos = repos

    def createCaller(self, protocol, port, rawUrl, proxies, authToken):
        return RepositoryCaller(protocol, port, authToken, self.repos)

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
        if cfg.proxy:
            self.proxies = cfg.proxy
        else:
            self.proxies = None

        self.log = tracelog.getLog(None)
        if cfg.traceLog:
            (l, f) = cfg.traceLog
            self.log = tracelog.getLog(filename=f, level=l, trace=l>2)

        if self.logFile:
            self.callLog = calllog.CallLogger(self.logFile, [])
        else:
            self.callLog = None

        self.log(1, "proxy url=%s" % basicUrl)

    def callWrapper(self, protocol, port, methodname, authToken, args,
                    remoteIp = None, rawUrl = None):
        if methodname not in self.publicCalls:
            return (False, True, ("MethodNotSupported", methodname, ""))

        self._port = port
        self._protocol = protocol

        # simple proxy. FIXME: caching these might help; building all
        # of this framework for every request seems dumb. it seems like
        # we could get away with one total since we're just changing
        # hostname/username/entitlement
        caller = self.callFactory.createCaller(protocol, port, rawUrl,
                                               self.proxies, authToken)

        try:
            if hasattr(self, methodname):
                # handled internally
                method = self.__getattribute__(methodname)

                if self.callLog:
                    self.callLog.log(remoteIp, authToken, methodname, args)

                anon, r = method(caller, authToken, *args)
                return (anon, False, r)

            r = caller.callByName(methodname, *args)
        except ProxyRepositoryError, e:
            return (False, True, e.args)

        return (r[0], False, r[1])

    def urlBase(self):
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

        useAnon, parentVersions = caller.checkVersion(clientVersion)

        if self.SERVER_VERSIONS is not None:
            commonVersions = sorted(list(set(self.SERVER_VERSIONS) &
                                         set(parentVersions)))
        else:
            commonVersions = parentVersions

        return useAnon, commonVersions

class ChangesetFilter(BaseProxy):

    forceGetCsVersion = None

    def __init__(self, cfg, basicUrl, cache):
        BaseProxy.__init__(self, cfg, basicUrl)
        self.csCache = cache

    def _cvtJobEntry(self, authToken, jobEntry):
        (name, (old, oldFlavor), (new, newFlavor), absolute) = jobEntry

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
        # Add more params if necessary
        if clientVersion < 38:
            return filecontainer.FILE_CONTAINER_VERSION_NO_REMOVES
        elif clientVersion < 43:
            return filecontainer.FILE_CONTAINER_VERSION_WITH_REMOVES
        # Add more changeset versions here as the currently newest client is
        # replaced by a newer one
        return filecontainer.FILE_CONTAINER_VERSION_FILEID_IDX

    def _convertChangeSet(self, csPath, size, destCsVersion, csVersion):
        # Changeset is in the file csPath
        # Changeset was fetched from the cache using key
        # Convert it to destCsVersion
        if (csVersion, destCsVersion) == (_CSVER1, _CSVER0):
            return self._convertChangeSetV1V0(csPath, size, destCsVersion)
        elif (csVersion, destCsVersion) == (_CSVER2, _CSVER1):
            return self._convertChangeSetV2V1(csPath, size, destCsVersion)
        assert False, "Unknown versions"

    def _convertChangeSetV2V1(self, cspath, size, destCsVersion):
        (fd, newCsPath) = tempfile.mkstemp(dir = self.cfg.tmpDir,
                                        suffix = '.tmp')
        os.close(fd)
        delta = changeset._convertChangeSetV2V1(cspath, newCsPath)

        return newCsPath, size + delta

    def _convertChangeSetV1V0(self, cspath, size, destCsVersion):
        # check to make sure that this user has access to see all
        # the troves included in a recursive changeset.
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
                raise ProxyRepositoryError(("TroveMissing", trvName,
                    self.fromVersion(trvNewVersion)))

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
                     recurse, withFiles, withFileContents, excludeAutoSource):

        def _addToCache(fingerPrint, inF, csVersion, returnVal, size):
            csPath = self.csCache.hashToPath(fingerPrint + '-%d' % csVersion)
            csDir = os.path.dirname(csPath)
            util.mkdirChain(csDir)
            (fd, csTmpPath) = tempfile.mkstemp(dir = csDir,
                                               suffix = '.ccs-new')
            outF = os.fdopen(fd, "w")
            util.copyfileobj(inF, outF)
            inF.close()
            # closes the underlying fd opened by mkstemp
            outF.close()

            (fd, dataTmpPath) = tempfile.mkstemp(dir = csDir,
                                                 suffix = '.data-new')
            data = os.fdopen(fd, 'w')
            data.write(cPickle.dumps((returnVal, size)))
            # closes the underlying fd
            data.close()

            os.rename(csTmpPath, csPath)
            os.rename(dataTmpPath, csPath + '.data')

            return csPath

        pathList = []
        allTrovesNeeded = []
        allFilesNeeded = []
        allTrovesRemoved = []
        allSizes = []

        if self.forceGetCsVersion is not None:
            getCsVersion = self.forceGetCsVersion
        else:
            getCsVersion = clientVersion

        neededCsVersion = self._getChangeSetVersion(clientVersion)
        wireCsVersion = self._getChangeSetVersion(getCsVersion)

        # Get the desired changeset version for this client
        iterV = neededCsVersion
        verPath = [iterV]
        if neededCsVersion != wireCsVersion:
            while 1:
                if iterV not in CHANGESET_VERSIONS_PRECEDENCE:
                    # No way to move forward
                    break
                # Move one edge in the DAG, try again
                iterV = CHANGESET_VERSIONS_PRECEDENCE[iterV]
                verPath.append(iterV)

        assert(verPath[-1] == wireCsVersion)

        fingerprints = [ '' ] * len(chgSetList)
        if self.csCache:
            try:
                useAnon, fingerprints = caller.getChangeSetFingerprints(43,
                        chgSetList, recurse, withFiles, withFileContents,
                        excludeAutoSource)
            except ProxyRepositoryError, e:
                # old server; act like no fingerprints were returned
                if e.args[0] == 'MethodNotSupported':
                    pass
                else:
                    raise

        for rawJob, fingerprint in itertools.izip(chgSetList, fingerprints):
            path = None
            # if we have both a cs fingerprint and a cache, then we will
            # cache the cs for this job
            cachable = bool(fingerprint and self.csCache)
            if fingerprint:
                # look up the changeset in the cache
                # empty fingerprint means "do not cache"
                fullPrint = fingerprint + '-%d' % neededCsVersion
                csPath = self.csCache.hashToPath(fullPrint)
                dataPath = csPath + '.data'
                if os.path.exists(csPath) and os.path.exists(dataPath):
                    # touch to refresh atime; try/except protects against race
                    # with someone removing the entry during the time it took
                    # you to read this comment
                    try:
                        fd = os.open(csPath, os.O_RDONLY)
                        os.close(fd)
                    except:
                        pass

                    data = open(dataPath)
                    (trovesNeeded, filesNeeded, removedTroves), size = \
                        cPickle.loads(data.read())
                    sizes = [ size ]
                    data.close()

                    path = csPath

            if path is None:
                # the changeset isn't in the cache.  create it
                url, sizes, trovesNeeded, filesNeeded, removedTroves = \
                    caller.getChangeSet(
                              getCsVersion, [ rawJob ], recurse, withFiles,
                              withFileContents, excludeAutoSource)[1]
                assert(len(sizes) == 1)
                # ensure that the size is an integer -- protocol version
                # 44 returns a string to avoid XML-RPC marshal limits
                sizes = [ int(x) for x in sizes ]
                size = sizes[0]

                if cachable:
                    inF = urllib.urlopen(url, proxies = self.proxies)
                    csPath =_addToCache(fingerprint, inF, wireCsVersion,
                                (trovesNeeded, filesNeeded, removedTroves),
                                size)
                    if url.startswith('file://localhost/'):
                        os.unlink(url[17:])
                elif url.startswith('file://localhost/'):
                    csPath = url[17:]
                else:
                    inF = urllib.urlopen(url, proxies = self.proxies)
                    (fd, tmpPath) = tempfile.mkstemp(dir = self.cfg.tmpDir,
                                                  suffix = '.ccs-out')
                    outF = os.fdopen(fd, "w")
                    size = util.copyfileobj(inF, outF)
                    assert(size == sizes[0])
                    inF.close()
                    outF.close()

                    csPath = tmpPath

                # csPath points to a wire version of the changeset (possibly
                # in the cache)

                # Now walk the precedence list backwards for conversion
                oldV = wireCsVersion
                for iterV in reversed(verPath[:-1]):
                    # Convert the changeset - not the first time around
                    path, size = self._convertChangeSet(csPath, size,
                                                        iterV, oldV)
                    sizes = [ size ]

                    if not cachable:
                        # we're not caching; erase the old version
                        os.unlink(csPath)
                        csPath = path
                    else:
                        csPath = _addToCache(fingerprint, open(path), iterV,
                                (trovesNeeded, filesNeeded, removedTroves),
                                size)

                    oldV = iterV

                path = csPath

            # make a note if this path has been stored in the cache or not
            pathList.append((path, size, cachable))
            allTrovesNeeded += trovesNeeded
            allFilesNeeded += filesNeeded
            allTrovesRemoved += removedTroves
            allSizes += sizes

        (fd, path) = tempfile.mkstemp(dir = self.cfg.tmpDir, suffix = '.cf-out')
        url = os.path.join(self.urlBase(),
                           "changeset?%s" % os.path.basename(path[:-4]))
        f = os.fdopen(fd, 'w')

        for path, size, cached in pathList:
            # the hard-coded 1 means it's a changeset and needs to be walked 
            # looking for files to include by reference
            f.write("%s %d 1 %d\n" % (path, size, cached))

        f.close()

        # client versions >= 44 use strings instead of ints for size
        # because xmlrpclib can't marshal ints > 2GiB
        if clientVersion >= 44:
            allSizes = [ str(x) for x in allSizes ]
        else:
            for size in allSizes:
                if size >= 0x80000000:
                    raise ProxyRepositoryError(('InvalidClientVersion',
                        'This version of Conary does not support downloading '
                        'changesets larger than 2 GiB.  Please install a new '
                        'Conary client.'))

        if clientVersion < 38:
            return False, (url, allSizes, allTrovesNeeded, allFilesNeeded)

        return False, (url, allSizes, allTrovesNeeded, allFilesNeeded, 
                      allTrovesRemoved)

class SimpleRepositoryFilter(ChangesetFilter):

    forceGetCsVersion = ChangesetFilter.SERVER_VERSIONS[-1]

    def __init__(self, cfg, basicUrl, repos):
        if cfg.changesetCacheDir:
            util.mkdirChain(cfg.changesetCacheDir)
            csCache = datastore.DataStore(cfg.changesetCacheDir)
        else:
            csCache = None

        ChangesetFilter.__init__(self, cfg, basicUrl, csCache)
        self.callFactory = RepositoryCallFactory(repos)

class ProxyRepositoryServer(ChangesetFilter):

    SERVER_VERSIONS = [ 41, 42, 43, 44, 45, 46 ]

    def __init__(self, cfg, basicUrl):
        util.mkdirChain(cfg.changesetCacheDir)
        csCache = datastore.DataStore(cfg.changesetCacheDir)

        util.mkdirChain(cfg.proxyContentsDir)
        self.contents = datastore.DataStore(cfg.proxyContentsDir)

        ChangesetFilter.__init__(self, cfg, basicUrl, csCache)

        self.callFactory = ProxyCallFactory()

    def getFileContents(self, caller, authToken, clientVersion, fileList,
                        authCheckOnly = False):
        if clientVersion < 42:
            # server doesn't support auth checks through getFileContents
            return caller(getFileContents,
                                clientVersion, fileList, authCheckOnly)

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
            # now get the contents we don't have cached
            (url, sizes) = caller.getFileContents(
                    clientVersion, neededFiles, False)[1]
            # insure that the size is an integer -- protocol version
            # 44 returns a string to avoid XML-RPC marshal limits
            sizes = [ int(x) for x in sizes ]

            (fd, tmpPath) = tempfile.mkstemp(dir = self.cfg.tmpDir,
                                             suffix = '.tmp')
            dest = util.ExtendedFile(tmpPath, "w+", buffering = False)
            os.close(fd)
            os.unlink(tmpPath)
            inUrl = urllib.urlopen(url, proxies = self.proxies)
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
                fileId = sha1helper.sha1ToString(self.toFileId(encFileId))
                nestedF = util.SeekableNestedFile(dest, size, start)
                self.contents.addFile(nestedF, fileId + '-c',
                                      precompressed = True,
                                      integrityCheck = False)
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
                        raise ProxyRepositoryError(
                            ('InvalidClientVersion',
                             'This version of Conary does not support '
                             'downloading file contents larger than 2 '
                             'GiB.  Please install a new Conary client.'))
            return False, (url, sizeList)
        finally:
            os.close(fd)

def redirectUrl(authToken, url):
    # return the url to use for the final server
    s = url.split('/')
    s[2] = ('%s:%s@' % (netclient.quote(authToken[0]),
                        netclient.quote(authToken[1]))) + s[2]
    url = '/'.join(s)

    return url

class ProxyRepositoryError(Exception):

    def __init__(self, args):
        self.args = args

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
