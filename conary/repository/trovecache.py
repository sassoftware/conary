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


from itertools import izip
import cPickle, os, tempfile

from conary import errors, trove, versions
from conary.deps import deps
from conary.lib import log, util
from conary.repository import changeset, filecontainer, filecontents
from conary.repository import netclient, trovesource


class CacheDict(dict):

    def has(self, troveTup, withFiles = False):
        if not withFiles or trove.troveIsCollection(troveTup[0]):
            return troveTup in self

        return self.get(troveTup, (None, False))[1] is True

    def __setitem__(self, troveTup, trv):
        dict.__setitem__(self, troveTup, (False, trv))

    def __getitem__(self, troveTup):
        return dict.__getitem__(self, troveTup)[1]

    def add(self, troveTup, trv, withFiles=False):
        dict.__setitem__(self, troveTup, (withFiles, trv))

class TroveCache(trovesource.AbstractTroveSource):

    VERSION = (2, 0)                    # (major, minor)

    _fileId = '\0' * 40
    _troveCacheVersionPathId = 'TROVE-CACHE-FILE-VERSION--------'
    _depCachePathId = 'SYSTEM-MODEL-DEPENDENCY-CACHE---'
    _depSolutionsPathId = 'SYSTEM-MODEL-DEPENDENCY-SOLUTION'
    _findCachePathId = 'SYSTEM-MODEL-FIND-CACHE---------'
    _timeStampsPathId = 'SYSTEM-MODEL-TIMESTAMP-CACHE----'
    _includeFilePathId = 'SYSTEM-MODEL-INCLUDE-FILE-CACHE-'

    def __init__(self, troveSource):
        self.troveInfoCache = {}
        self.depCache = {}
        self.depSolutionCache = {}
        self.timeStampCache = {}
        self.cache = CacheDict()
        self.troveSource = troveSource
        self.findCache = {}
        self.fileCache = {}
        self.callback = None
        self._startingSizes = self._getSizeTuple()
        self._cs = None

    def _addToCache(self, troveTupList, troves, _cached = None,
                    withFiles = False):
        for troveTup, trv in izip(troveTupList, troves):
            self.cache.add(troveTup, trv, withFiles = withFiles)

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

    def cacheTroves(self, troveTupList, _cached = None, withFiles = False):
        troveTupList = [x for x in troveTupList
                            if not self.cache.has(x, withFiles = withFiles) ]
        if not troveTupList:
            return

        self._caching(troveTupList)

        troves = self.troveSource.getTroves(troveTupList, withFiles=withFiles,
                                            callback = self.callback)

        self._addToCache(troveTupList, troves, _cached = _cached,
                         withFiles=withFiles)

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
        self.cacheTroves(tupList, _cached = _cached, withFiles = withFiles)
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

        self._cached(self.cache.keys(), [ x[1] for x in self.cache.values() ])

        try:
            # NB: "fileid" and pathid got reversed here by mistake, try not to
            # think too hard about it.
            contType, depContents = cs.getFileContents(
                    self._fileId, self._troveCacheVersionPathId)
        except KeyError:
            self.version = (0, 0)
        else:
            versionList = depContents.get().read().split(' ')
            self.version = (int(versionList[0]), int(versionList[1]))

        if self.version[0] > self.VERSION[0]:
            # major number is too big for us; we can't load this
            return

        # Timestamps must come first because some other caches use it to
        # construct versions.
        self._cs = cs
        self._loadTimestamps()
        self._loadDeps()
        self._loadDepSolutions()
        self._loadFileCache()
        self._startingSizes = self._getSizeTuple()
        self._cs = None

    def _loadPickle(self, pathId):
        self._cs.reset()
        contType, contents = self._cs.getFileContents(pathId, self._fileId)
        pickled = contents.get().read()
        return cPickle.loads(pickled)

    def _savePickle(self, pathId, data):
        pickled = cPickle.dumps(data, 2)
        self._cs.addFileContents(pathId, self._fileId,
                changeset.ChangedFileTypes.file,
                filecontents.FromString(pickled), False)

    def _loadTimestamps(self):
        timeStampList = self._loadPickle(self._timeStampsPathId)
        for (name, frozenVersion) in timeStampList:
            thawed = versions.ThawVersion(frozenVersion)
            self.timeStampCache[(name, thawed)] = thawed

    def _saveTimestamps(self):
        timeStamps = []
        for (name, baseVersion), version in self.timeStampCache.items():
            timeStamps.append( (timeStamps, version.freeze()) )
        self._savePickle(self._timeStampsPathId, timeStamps)

    def _loadDeps(self):
        depList = self._loadPickle(self._depCachePathId)
        for (name, thawedVersion, frzFlavor, prov, req) in depList:
            version = versions.VersionFromString(thawedVersion)
            flavor = deps.ThawFlavor(frzFlavor)
            self.depCache[ (name, version, flavor) ] = (prov, req)

    def _saveDeps(self):
        depList = []
        for troveTup, (prov, req) in self.depCache.iteritems():
            if type(prov) is not str and prov is not None:
                prov = prov.freeze()
            if type(req) is not str and req is not None:
                req = req.freeze()

            depList.append((troveTup[0], troveTup[1].asString(),
                            troveTup[2].freeze(), prov, req))
        self._savePickle(self._depCachePathId, depList)

    def _loadDepSolutions(self):
        if self.version < (2, 0):
            # Earlier versions were missing timestamps, which interferes with
            # dep solver tie-breaking.
            return
        depSolutionsList = self._loadPickle(self._depSolutionsPathId)
        for (sig, depSet, aResult) in depSolutionsList:
            depSet = deps.ThawDependencySet(depSet)
            allResults = []
            for resultList in aResult:
                allResults.append([
                    (x[0], versions.ThawVersion(x[1]), deps.ThawFlavor(x[2]))
                    for x in resultList])
            self.addDepSolution(sig, depSet, allResults)

    def _saveDepSolutions(self):
        depSolutions = []
        for (sig, depSet), aResult in self.depSolutionCache.iteritems():
            allResults = []
            for resultList in aResult:
                allResults.append([ (x[0], x[1].freeze(), x[2].freeze()) for
                                     x in resultList ])

            depSolutions.append( (sig, depSet.freeze(), allResults) )
        self._savePickle(self._depSolutionsPathId, depSolutions)

    def _loadFindCache(self):
        self.findCache = self._loadPickle(self._findCachePathId)

    def _saveFindCache(self):
        self._savePickle(self._findCachePathId, self.findCache)

    def _loadFileCache(self):
        if self.version < (1, 0):
            return
        self.fileCache = self._loadPickle(self._includeFilePathId)

    def _saveFileCache(self):
        self._savePickle(self._includeFilePathId, self.fileCache)

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
        for withFiles, trv in self.cache.values():
            # we just assume everything in the cache is w/o files. it's
            # fine for system model, safe, and we don't need the cache
            # anywhere else
            cs.newTrove(trv.diff(None, absolute = True)[0])

        # NB: "fileid" and pathid got reversed here by mistake, try not to
        # think too hard about it.
        cs.addFileContents(
                           self._fileId,
                           self._troveCacheVersionPathId,
                           changeset.ChangedFileTypes.file,
                           filecontents.FromString("%d %d" % self.VERSION),
                           False)
        self._cs = cs
        self._saveTimestamps()
        self._saveDeps()
        self._saveDepSolutions()
        self._saveFileCache()
        self._cs = None

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
