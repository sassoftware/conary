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
#

from itertools import izip
import cPickle, os, tempfile

from conary import errors, trove, versions
from conary.deps import deps
from conary.lib import log, util
from conary.repository import changeset, filecontainer, filecontents
from conary.repository import netclient, trovesource

class TroveCache(trovesource.AbstractTroveSource):

    VERSION = (1, 0)                    # (major, minor)

    troveCacheVersionFileId = 'TROVE-CACHE-FILE-VERSION--------'
    troveCacheVersionPathId = '\0' * 40

    depCachePathId = 'SYSTEM-MODEL-DEPENDENCY-CACHE---'
    depCacheFileId = '\0' * 40

    depSolutionsPathId = 'SYSTEM-MODEL-DEPENDENCY-SOLUTION'
    depSolutionsFileId = '\0' * 40

    findCachePathId = 'SYSTEM-MODEL-FIND-CACHE---------'
    findCacheFileId = '\0' * 40

    timeStampsPathId = 'SYSTEM-MODEL-TIMESTAMP-CACHE----'
    timeStampsFileId = '\0' * 40

    includeFilePathId = 'SYSTEM-MODEL-INCLUDE-FILE-CACHE-'
    includeFileFileId = '\0' * 40

    def __init__(self, troveSource):
        self.troveInfoCache = {}
        self.depCache = {}
        self.depSolutionCache = {}
        self.timeStampCache = {}
        self.cache = {}
        self.troveSource = troveSource
        self.findCache = {}
        self.fileCache = {}
        self._startingSizes = self._getSizeTuple()

    def _addToCache(self, troveTupList, troves, _cached = None):
        for troveTup, trv in izip(troveTupList, troves):
            self.cache[troveTup] = trv

        if _cached:
            _cached(troveTupList, troves)
        else:
            self._cached(troveTupList, troves)

    def _caching(self, troveTupList):
        pass

    def _cached(self, troveTupList, troveList):
        pass

    def _getSizeTuple(self):
        return ( len(self.cache),
                 sum([ len([ x[0] for x in self.depCache.itervalues()
                                 if x[0] is not None ]),
                       len([ x[1] for x in self.depCache.itervalues()
                                 if x[1] is not None ]) ] ),
                 len(self.depSolutionCache), len(self.timeStampCache),
                 len(self.findCache), len(self.fileCache) )

    def cacheTroves(self, troveTupList, _cached = None):
        troveTupList = [x for x in troveTupList if x not in self.cache]
        if not troveTupList:
            return

        self._caching(troveTupList)

        troves = self.troveSource.getTroves(troveTupList, withFiles=False,
                                            callback = self.callback)

        self._addToCache(troveTupList, troves, _cached = _cached)

    def addFindResult(self, spec, result):
        self.findCache[(None, spec)] = result

    def getFindResult(self, spec):
        return self.findCache.get((None, spec))

    def addDepSolution(self, sig, depSet, result):
        self.depSolutionCache[(sig, depSet)] = list(result)

    def getDepSolution(self, sig, depSet):
        return self.depSolutionCache.get( (sig, depSet), None )

    def getDepCacheEntry(self, troveTup):
        result = self.depCache.get(troveTup)
        if result is None:
            return None

        origResult = result
        if type(result[0]) is str:
            result = (deps.ThawDependencySet(result[0]), result[1])

        if type(result[1]) is str:
            result = (result[0], deps.ThawDependencySet(result[1]))

        if result != origResult:
            self.depCache[troveTup] = result

        return result

    def getDepsForTroveList(self, troveTupList, provides = True,
                            requires = True):
        def missingNeeded(depTuple):
            if depTuple is None: return True
            if provides and depTuple[0] is None: return True
            if requires and depTuple[1] is None: return True

            return False

        def mergeCacheEntry(troveTup, depTuple):
            existing = self.depCache.get(depTuple)
            if existing is None:
                self.depCache[troveTup] = depInfo
            else:
                self.depCache[troveTup] = (depTuple[0] or existing[0],
                                           depTuple[1] or existing[1])

        # look in the dep cache and trove cache
        result = [ None ] * len(troveTupList)
        for i, tup in enumerate(troveTupList):
            result[i] = self.getDepCacheEntry(tup)

            if result[i] is None and self.troveIsCached(tup):
                trv = self.cache[tup]
                result[i] = (trv.getProvides(), trv.getRequires())
            elif result[i] is None and trove.troveIsPackage(tup[0]):
                # packages provide only themselves; querying the repository
                # to figure that out seems unnecessarily complicated
                result[i] = (deps.parseDep('trove: %s' % tup[0]),
                             deps.DependencySet())

        needed = [ (i, troveTup) for i, (troveTup, depSets) in
                            enumerate(izip(troveTupList, result))
                            if missingNeeded(depSets)  ]
        if not needed:
            return result

        # use the getDepsForTroveList call; it raises an error if it needs
        # to access some repositories which don't support it
        log.info("Getting deps for %d troves" % len(needed))
        try:
            depList = self.troveSource.getDepsForTroveList(
                                                [ x[1] for x in needed ],
                                                provides = provides,
                                                requires = requires)
        except netclient.PartialResultsError, e:
            # we can't use this call everywhere; handle what we can and we'll
            # deal with the None's later
            depList = e.partialResults

        for (i, troveTup), depInfo in izip(needed, depList):
            # depInfo can be None if we got partial results due to
            # old servers
            if depInfo is not None:
                mergeCacheEntry(troveTup, depInfo)
                result[i] = depInfo

        # see if anything else is None; if so, we need to cache the complete
        needed = [ (i, troveTup) for i, troveTup in
                            enumerate(troveTupList) if result[i] is None ]

        trvs = self.getTroves([ x[1] for x in needed])
        for (i, troveTup), trv in izip(needed, trvs):
            result[i] = (trv.getProvides(), trv.getRequires())

        return result

    def getSizes(self, troveTupList):
        tiList = self.getTroveInfo(trove._TROVEINFO_TAG_SIZE, troveTupList)
        rc = [ None ] * len(tiList)
        for i, x in enumerate(tiList):
            if x:
                tiList[i] = x()

        return tiList

    def getTimestamps(self, troveTupList):
        # look in the dep cache and trove cache
        result = [ None ] * len(troveTupList)
        for i, tup in enumerate(troveTupList):
            result[i] = self.timeStampCache.get(tup[0:2])
            if result[i] is None and self.troveIsCached(tup):
                trv = self.cache[tup]
                result[i] = trv.getVersion()

        needed = [ (i, troveTup) for i, (troveTup, depSet) in
                            enumerate(izip(troveTupList, result))
                            if depSet is None ]
        if not needed:
            return result

        # use the timeStamps call; it raises an error if it needs
        # to access some repositories which don't support it
        log.info("Getting timeStamps for %d troves" % len(needed))
        try:
            depList = self.troveSource.getTimestamps(
                                                [ x[1] for x in needed ])
        except netclient.PartialResultsError, e:
            # we can't use this call everywhere; handle what we can and we'll
            # deal with the None's later
            depList = e.partialResults

        for (i, troveTup), timeStampedVersion in izip(needed, depList):
            # timeStampedVersion can be None if we got partial results due to
            # old servers
            if timeStampedVersion is not None:
                self.timeStampCache[troveTup[0:2]] = timeStampedVersion
                result[i] = timeStampedVersion

        # see if anything else is None; if so, we need to cache the complete
        needed = [ (i, troveTup) for i, troveTup in
                            enumerate(troveTupList) if result[i] is None ]

        trvs = self.getTroves([ x[1] for x in needed])
        for (i, troveTup), trv in izip(needed, trvs):
            result[i] = trv.getVersion()

        return result


    def getTroveInfo(self, infoType, troveTupList):
        troveTupList = list(troveTupList)
        infoCache = self.troveInfoCache.setdefault(infoType, {})

        result = [ None ] * len(troveTupList)
        for i, tup in enumerate(troveTupList):
            result[i] = infoCache.get(tup)
            if result[i] is None and self.troveIsCached(tup):
                trv = self.cache[tup]
                result[i] = getattr(trv.troveInfo,
                                    trv.troveInfo.streamDict[infoType][2])

        needed = [ (i, troveTup) for i, (troveTup, depSet) in
                            enumerate(izip(troveTupList, result))
                            if depSet is None ]
        if not needed:
            return result

        troveInfoList = self.troveSource.getTroveInfo(infoType,
                                                [ x[1] for x in needed ])
        for (i, troveTup), troveInfo in izip(needed, troveInfoList):
            infoCache[troveTup] = troveInfo
            result[i] = troveInfo

        return result

    def getPackageComponents(self, troveTup):
        return [ x[0][0] for x in self.iterTroveListInfo(troveTup) ]

    def getPathHashesForTroveList(self, troveList):
        return self.getTroveInfo(trove._TROVEINFO_TAG_PATH_HASHES, troveList)

    def getTrove(self, name, version, flavor, withFiles = True):
        assert(not withFiles)
        return self.getTroves(
                        [ (name, version, flavor) ], withFiles = False)[0]

    def getTroves(self, tupList, withFiles = False, _cached = None):
        assert(not withFiles)
        self.cacheTroves(tupList, _cached = _cached)
        return [ self.cache[x] for x in tupList ]

    def iterTroveListInfo(self, troveTup):
        if trove.troveIsComponent(troveTup[0]): return []
        return(self.cache[troveTup].iterTroveListInfo())

    def load(self, path):
        assert(not self.cache and not self.depCache)
        try:
            cs = changeset.ChangeSetFromFile(path)
        except filecontainer.BadContainer:
            log.warning('trove cache %s was corrupt, ignoring' %path)
            return
        except (IOError, errors.ConaryError):
            return

        for trvCs in cs.iterNewTroveList():
            trv = trove.Trove(trvCs, skipIntegrityChecks = True)
            self.cache[trv.getNameVersionFlavor()] = trv

        self._cached(self.cache.keys(), self.cache.values())

        try:
            contType, depContents = cs.getFileContents(
                               self.troveCacheVersionPathId,
                               self.troveCacheVersionFileId)
        except KeyError:
            self.version = (0, 0)
            cs.reset()
        else:
            versionList = depContents.get().read().split(' ')
            self.version = (int(versionList[0]), int(versionList[1]))

        if self.version[0] > self.VERSION[0]:
            # major number is too big for us; we can't load this
            return

        contType, depContents = cs.getFileContents(
                           self.depCachePathId, self.depCacheFileId)
        pickled = depContents.get().read()
        depList = cPickle.loads(pickled)
        for (name, frzVersion, frzFlavor, prov, req) in depList:
            self.depCache[ (name, versions.VersionFromString(frzVersion),
                            deps.ThawFlavor(frzFlavor)) ] = (prov, req)

        contType, depSolutions = cs.getFileContents(
                           self.depSolutionsPathId, self.depSolutionsFileId)
        pickled = depSolutions.get().read()
        depSolutionsList = cPickle.loads(pickled)

        for (sig, depSet, aResult) in depSolutionsList:
            depSet = deps.ThawDependencySet(depSet)
            allResults = []
            for resultList in aResult:
                allResults.append( [ (x[0], versions.VersionFromString(x[1]),
                                     deps.ThawFlavor(x[2]) )
                                    for x in resultList ] )

            self.addDepSolution(sig, depSet, allResults)

        contType, fileCache = cs.getFileContents(
                           self.findCachePathId, self.findCacheFileId)
        self.findCache = cPickle.loads(fileCache.get().read())

        if self.version[0] >= 1:
            contType, includeFileContents = cs.getFileContents(
                               self.includeFilePathId, self.includeFileFileId)
            pickled = includeFileContents.get().read()
            self.fileCache = cPickle.loads(pickled)

        contType, versionTimeStamps = cs.getFileContents(
                           self.timeStampsPathId, self.timeStampsFileId)

        pickled = versionTimeStamps.get().read()
        timeStampList = cPickle.loads(pickled)

        for (name, frozenVersion) in timeStampList:
            thawed = versions.ThawVersion(frozenVersion)
            self.timeStampCache[(name, thawed)] = thawed

        self._startingSizes = self._getSizeTuple()

    def save(self, path):
        # return early if we aren't going to have permission to save
        try:
            fd, cacheName = tempfile.mkstemp(
                    prefix=os.path.basename(path) + '.',
                    dir=os.path.dirname(path))
            os.close(fd)
        except (IOError, OSError):
            # may not have permissions; say, not running as root
            return

        cs = changeset.ChangeSet()
        for trv in self.cache.values():
            cs.newTrove(trv.diff(None, absolute = True)[0])

        cs.addFileContents(self.troveCacheVersionPathId,
                           self.troveCacheVersionFileId,
                           changeset.ChangedFileTypes.file,
                           filecontents.FromString("%d %d" % self.VERSION),
                           False)

        depList = []
        for troveTup, (prov, req) in self.depCache.iteritems():
            if type(prov) is not str and prov is not None:
                prov = prov.freeze()
            if type(req) is not str and req is not None:
                req = req.freeze()

            depList.append((troveTup[0], troveTup[1].asString(),
                            troveTup[2].freeze(), prov, req))

        depStr = cPickle.dumps(depList)

        cs.addFileContents(self.depCachePathId, self.depCacheFileId,
                           changeset.ChangedFileTypes.file,
                           filecontents.FromString(depStr), False)

        depSolutions = []
        for (sig, depSet), aResult in self.depSolutionCache.iteritems():
            allResults = []

            for resultList in aResult:
                allResults.append([ (x[0], x[1].asString(), x[2].freeze()) for
                                     x in resultList ])

            depSolutions.append( (sig, depSet.freeze(), allResults) )
        depSolutionsStr = cPickle.dumps(depSolutions)

        cs.addFileContents(self.depSolutionsPathId, self.depSolutionsFileId,
                           changeset.ChangedFileTypes.file,
                           filecontents.FromString(depSolutionsStr), False)

        cs.addFileContents(self.findCachePathId, self.findCacheFileId,
                           changeset.ChangedFileTypes.file,
                           filecontents.FromString(
                                cPickle.dumps(self.findCache)),
                           False)

        timeStamps = []
        for (name, baseVersion), version in self.timeStampCache.items():
            timeStamps.append( (timeStamps, version.freeze()) )
        timeStampsStr = cPickle.dumps(timeStamps)

        cs.addFileContents(self.timeStampsPathId, self.timeStampsFileId,
                           changeset.ChangedFileTypes.file,
                           filecontents.FromString(timeStampsStr), False)

        cs.addFileContents(self.includeFilePathId, self.includeFileFileId,
                           changeset.ChangedFileTypes.file,
                           filecontents.FromString(
                                cPickle.dumps(self.fileCache)),
                           False)

        try:
            try:
                cs.writeToFile(cacheName)
                if util.exists(path):
                    os.chmod(cacheName, os.stat(path).st_mode)
                else:
                    os.chmod(cacheName, 0644)
                os.rename(cacheName, path)
            except (IOError, OSError):
                # may not have permissions; say, not running as root
                pass
        finally:
            try:
                if os.path.exists(cacheName):
                    os.remove(cacheName)
            except OSError:
                pass

    def troveIsCached(self, troveTup):
        return troveTup in self.cache

    def troveReferencesTrove(self, troveTup, troveRef):
        return self.cache[troveTup].hasTrove(*troveRef)

    def cacheFile(self, key, contents):
        self.fileCache[key] = contents

    def getCachedFile(self, key):
        return self.fileCache.get(key)
