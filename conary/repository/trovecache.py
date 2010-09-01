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

from conary import trove
from conary.deps import deps
from conary.lib import log
from conary.repository import netclient, trovesource

class TroveCache(trovesource.AbstractTroveSource):

    def __init__(self, troveSource):
        self.troveInfoCache = {}
        self.depCache = {}
        self.cache = {}
        self.troveSource = troveSource

    def _caching(self, troveTupList):
        pass

    def _cached(self, troveTupList, troveList):
        pass

    def cacheTroves(self, troveTupList, _cached = None):
        troveTupList = [x for x in troveTupList if x not in self.cache]
        if not troveTupList:
            return

        self._caching(troveTupList)

        troves = self.troveSource.getTroves(troveTupList, withFiles=False,
                                            callback = self.callback)

        for troveTup, trv in izip(troveTupList, troves):
            self.cache[troveTup] = trv

        if _cached:
            _cached(troveTupList, troves)
        else:
            self._cached(troveTupList, troves)

    def getDepsForTroveList(self, troveTupList):
        # look in the dep cache and trove cache
        result = [ None ] * len(troveTupList)
        for i, tup in enumerate(troveTupList):
            result[i] = self.depCache.get(tup)
            if result[i] is None and self.troveIsCached(tup):
                trv = self.cache[tup]
                result[i] = (trv.getProvides(), trv.getRequires())
            elif result[i] is None and trove.troveIsPackage(tup[0]):
                # packages provide only themselves; querying the repository
                # to figure that out seems unnecessarily complicated
                result[i] = (deps.parseDep('trove: %s' % tup[0]),
                             deps.DependencySet())

        needed = [ (i, troveTup) for i, (troveTup, depSet) in
                            enumerate(izip(troveTupList, result))
                            if depSet is None ]
        if not needed:
            return result

        # use the getDepsForTroveList call; it raises an error if it needs
        # to access some repositories which don't support it
        log.info("Getting deps for %d troves" % len(needed))
        try:
            depList = self.troveSource.getDepsForTroveList(
                                                [ x[1] for x in needed ])
        except netclient.PartialResultsError, e:
            # we can't use this call everywhere; handle what we can and we'll
            # deal with the None's later
            depList = e.partialResults

        for (i, troveTup), depInfo in izip(needed, depList):
            # depInfo can be None if we got partial results due to
            # old servers
            if depInfo is not None:
                self.depCache[troveTup] = depInfo
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

    def troveIsCached(self, troveTup):
        return troveTup in self.cache

    def troveReferencesTrove(self, troveTup, troveRef):
        return self.cache[troveTup].hasTrove(*troveRef)

