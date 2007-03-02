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

import base64, itertools, os, tempfile, urllib, xmlrpclib

from conary import conarycfg, trove
from conary.lib import sha1helper, tracelog, util
from conary.repository import changeset, datastore, errors, netclient
from conary.repository import transport, xmlshims
from conary.repository.netrepos import cacheset, netserver, calllog

class ProxyClient(xmlrpclib.ServerProxy):

    pass

class ProxyCaller:

    def callByName(self, methodname, *args):
        try:
            rc = self.proxy.__getattr__(methodname)(*args)
        except IOError, e:
            return [ False, True, [ 'ProxyError', e.strerror[1] ] ]
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
    def createCaller(protocol, port, rawUrl, authToken):
        url = redirectUrl(authToken, rawUrl)

        if authToken[2] is not None:
            entitlement = authToken[2:4]
        else:
            entitlement = None

        transporter = transport.Transport(https = url.startswith('https:'),
                                          entitlement = entitlement)

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

    def createCaller(self, protocol, port, rawUrl, authToken):
        return RepositoryCaller(protocol, port, authToken, self.repos)

class BaseProxy(xmlshims.NetworkConvertors):

    # a list of the protocol versions we understand. Make sure the first
    # one in the list is the lowest protocol version we support and the
    # last one is the current server protocol version.
    SERVER_VERSIONS = netserver.SERVER_VERSIONS
    publicCalls = netserver.NetworkRepositoryServer.publicCalls

    def __init__(self, cfg, basicUrl):
        self.cfg = cfg
        self.basicUrl = basicUrl
        self.logFile = cfg.logFile
        self.tmpPath = cfg.tmpDir

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
                                               authToken)

        if hasattr(self, methodname):
            # handled internally
            method = self.__getattribute__(methodname)

            if self.callLog:
                self.callLog.log(remoteIp, authToken, methodname, args)

            try:
                anon, r = method(caller, authToken, *args)
            except ProxyRepositoryError, e:
                return (False, True, e.args)

            return (anon, False, r)

        try:
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

    SERVER_VERSIONS = [ 41, 42, 43 ]

    def __init__(self, cfg, basicUrl, cache):
        BaseProxy.__init__(self, cfg, basicUrl)
        self.cache = cache

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

    def getChangeSet(self, caller, authToken, clientVersion, chgSetList,
                     recurse, withFiles, withFileContents, excludeAutoSource):
        pathList = []
        allTrovesNeeded = []
        allFilesNeeded = []
        allTrovesRemoved = []
        allSizes = []

        csVersion = netserver.NetworkRepositoryServer._getChangeSetVersion(
                                                                clientVersion)

        for rawJob in chgSetList:
            job = self._cvtJobEntry(authToken, rawJob)

            cacheEntry = self.cache.getEntry(job, recurse, withFiles,
                                     withFileContents, excludeAutoSource,
                                     csVersion)
            path = None

            if cacheEntry is not None:
                path, (trovesNeeded, filesNeeded, removedTroves, sizes), \
                        size = cacheEntry
                invalidate = False

                # revalidate the cache entries for both permissions and
                # currency
                troveList = []
                cs = changeset.ChangeSetFromFile(path)
                for trvCs in cs.iterNewTroveList():
                    troveList.append(
                            ((trvCs.getName(), trvCs.getNewVersion(),
                              trvCs.getNewFlavor()),
                              trvCs.getNewSigs().freeze()))

                fetchList = [ (x[0][0], self.fromVersion(x[0][1]),
                               self.fromFlavor(x[0][2]) ) for x in troveList ]
                serverSigs = caller.getTroveInfo(
                              clientVersion, trove._TROVEINFO_TAG_SIGS,
                              fetchList)[1]
                for (troveInfo, cachedSigs), (present, reposSigs) in \
                                    itertools.izip(troveList, serverSigs):
                    if present < 1 or \
                                not cachedSigs or \
                                cachedSigs != base64.decodestring(reposSigs):
                        invalidate = True
                        break

                if invalidate:
                    self.cache.invalidateEntry(None, job[0], job[2][0],
                                               job[2][1])
                    path = None

            if path is None:
                url, sizes, trovesNeeded, filesNeeded, removedTroves = \
                    caller.getChangeSet(
                              clientVersion, [ rawJob ], recurse, withFiles,
                              withFileContents, excludeAutoSource)[1]
                assert(len(sizes) == 1)

                if url.startswith('file://localhost/'):
                    tmpPath = url[17:]
                    size = sizes[0]
                else:
                    (fd, tmpPath) = tempfile.mkstemp(dir = self.cache.tmpDir,
                                                     suffix = '.tmp')
                    dest = os.fdopen(fd, "w")
                    size = util.copyfileobj(urllib.urlopen(url), dest)
                    dest.close()

                (key, path) = self.cache.addEntry(job, recurse, withFiles,
                                    withFileContents, excludeAutoSource,
                                    (trovesNeeded, filesNeeded, removedTroves,
                                     sizes), size = size, csVersion = csVersion)

                os.rename(tmpPath, path)

            pathList.append((path, size))
            allTrovesNeeded += trovesNeeded
            allFilesNeeded += filesNeeded
            allTrovesRemoved += removedTroves
            allSizes += sizes

        (fd, path) = tempfile.mkstemp(dir = self.cfg.tmpDir, suffix = '.cf-out')
        url = os.path.join(self.urlBase(),
                           "changeset?%s" % os.path.basename(path[:-4]))
        f = os.fdopen(fd, 'w')

        for path, size in pathList:
            f.write("%s %d\n" % (path, size))

        f.close()

        return False, (url, allSizes, allTrovesNeeded, allFilesNeeded, 
                      allTrovesRemoved)

class SimpleRepositoryFilter(ChangesetFilter):

    def __init__(self, cfg, basicUrl, repos):
        if cfg.cacheDB:
            cache = cacheset.CacheSet(cfg.cacheDB, cfg.tmpDir,
                                      cfg.deadlockRetry)
        else:
            cache = cacheset.NullCacheSet(cfg.tmpDir)

        ChangesetFilter.__init__(self, cfg, basicUrl, cache)
        self.callFactory = RepositoryCallFactory(repos)

class ProxyRepositoryServer(ChangesetFilter):

    def __init__(self, cfg, basicUrl):
        cache = cacheset.CacheSet(cfg.proxyDB, cfg.tmpDir, cfg.deadlockRetry)

        ChangesetFilter.__init__(self, cfg, basicUrl, cache)

        util.mkdirChain(self.cfg.proxyContentsDir)
        self.contents = datastore.DataStore(self.cfg.proxyContentsDir)
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
                try:
                    # touch the file; we don't want it to be removed
                    # by a cleanup job when we need it
                    os.open(path, os.O_RDONLY)
                    hasFiles.append((encFileId, encVersion))
                    continue
                except OSError:
                    pass

            neededFiles.append((encFileId, encVersion))

        # make sure this user has permissions for these file contents. an
        # exception will get raised if we don't have sufficient permissions
        if hasFiles:
            caller.getFileContents(clientVersion, hasFiles, True)

        if neededFiles:
            # now get the contents we don't have cached
            (url, sizes) = caller.getFileContents(
                    clientVersion, neededFiles, False)[1]

            (fd, tmpPath) = tempfile.mkstemp(dir = self.cache.tmpDir,
                                             suffix = '.tmp')
            dest = os.fdopen(fd, "w+")
            size = util.copyfileobj(urllib.urlopen(url), dest)
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

        (fd, path) = tempfile.mkstemp(dir = self.tmpPath,
                                      suffix = '.cf-out')
        sizeList = []

        try:
            for encFileId, encVersion in fileList:
                fileId = sha1helper.sha1ToString(self.toFileId(encFileId))
                filePath = self.contents.hashToPath(fileId + '-c')
                size = os.stat(filePath).st_size
                sizeList.append(size)
                os.write(fd, "%s %d\n" % (filePath, size))

            url = os.path.join(self.urlBase(),
                               "changeset?%s" % os.path.basename(path)[:-4])
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
