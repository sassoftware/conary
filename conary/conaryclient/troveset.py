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


import itertools

from conary import trove, versions
from conary.conaryclient import cml
from conary.deps import deps
from conary.errors import ConaryError, TroveSpecsNotFound
from conary.lib import graph, sha1helper
from conary.repository import searchsource, trovesource

class SimpleFilteredTroveSource(trovesource.SimpleTroveSource):

    """
    TroveSource based on a list of (n,v,f) tuples where contents are
    available via another TroveSource (a cache for us).
    """

    def __init__(self, troveCache, troveTupList):
        trovesource.SimpleTroveSource.__init__(self, troveTupList)
        self.troveCache = troveCache
        self.searchAsDatabase()

class TroveTupleSetTroveSource(SimpleFilteredTroveSource):

    """
    TroveSource based on the (n,v,f) in a TroveTupleSet. Newly added
    groups are searchable, though the versions are not specified and
    the flavor is the same as the flavor being built.
    """

    def __init__(self, troveCache, troveSet):
        SimpleFilteredTroveSource.__init__(self, troveCache,
               [ x[0] for x in troveSet._walk(troveCache, recurse = True) ] )

class ResolveTroveTupleSetTroveSource(SimpleFilteredTroveSource):

    """
    Similar to TroveTupleSetTroveSource, but newly created groups are
    not present; instead their members are included. This is designed
    for dependency solving.
    """

    def __init__(self, troveCache, troveSet, flavor,
                 filterFn = None, depDb = None):
        assert(depDb)
        self.depDb = depDb
        self.troveSet = troveSet
        if filterFn is None:
            self.filterFn = lambda *args: False
        else:
            self.filterFn = filterFn

        self.troveTupList = []
        for troveTup, inInstall, isExplicit in \
                    self.troveSet._walk(troveCache, newGroups = False,
                                        recurse = True):
            if not self.filterFn(*troveTup):
                self.troveTupList.append(troveTup)

        self.troveTupSig = self.troveSet._getSignature(troveCache)

        self.inDepDb = [ False ] * len(self.troveTupList)

        SimpleFilteredTroveSource.__init__(self, troveCache,
                                              self.troveTupList)

        self.setFlavorPreferencesByFlavor(flavor)
        self.searchWithFlavor()
        self.searchLeavesOnly()
        # maps troveId's from the dependency database into self.troveTupList
        self.depTroveIdMap = {}
        self.providesIndex = None

    def resolveDependencies(self, label, depList, leavesOnly=False):
        def _depClassAndName(oneDep):
            s = set()

            for depClass, depName, flags in oneDep.iterRawDeps():
                s.add( (depClass, depName) )

            return s

        reqNames = set()
        finalDepList = []
        cachedSuggMap = {}
        for dep in depList:
            cachedResult = self.troveCache.getDepSolution(self.troveTupSig,
                                                          dep)
            if cachedResult is None:
                # Cache miss
                reqNames.update(_depClassAndName(dep))
                finalDepList.append(dep)
            else:
                # Cache hit ...
                if cachedResult:
                    # ... and a trove matched
                    cachedSuggMap[dep] = cachedResult

        if not finalDepList:
            return cachedSuggMap

        # Retrieve provides for all troves in the set
        emptyDep = deps.DependencySet()
        troveDeps = self.troveCache.getDepsForTroveList(self.troveTupList,
                                                        provides = True,
                                                        requires = False)

        if self.providesIndex is None:
            index = {}
            self.providesIndex = index
            for i, (troveTup, (p, r)) in enumerate(itertools.izip(
                    self.troveTupList, troveDeps)):
                classAndNameSet = _depClassAndName(p)
                for classAndName in classAndNameSet:
                    val = index.get(classAndName)
                    if val is None:
                        index[classAndName] = [ i ]
                    else:
                        val.append(i)

        # For each requirement to be resolved, load any matching provides into
        # the resolver DB
        depLoader = self.depDb.bulkLoader()
        for classAndName in reqNames:
            val = self.providesIndex.get(classAndName)
            if val is None:
                continue

            for i in val:
                if self.inDepDb[i]:
                    continue

                depTroveId = depLoader.addRaw(troveDeps[i][0], emptyDep)
                self.depTroveIdMap[depTroveId] = i
                self.inDepDb[i] = True

        depLoader.done()
        self.depDb.commit()

        if not self.depTroveIdMap:
            # No requirements were matched by troves in this set, so add
            # negative cache entries for all of the requirements checked.
            for depSet in depList:
                self.troveCache.addDepSolution(self.troveTupSig, depSet, [])
            return cachedSuggMap

        suggMap = self.depDb.resolve(label, finalDepList, leavesOnly=leavesOnly,
                                     troveIdList = self.depTroveIdMap.keys())

        # Convert resolver results back to trove tuples and insert into the
        # suggestion map
        for depSet, solListList in suggMap.iteritems():
            newSolListList = []
            for solList in solListList:
                newSolListList.append([
                        self.troveTupList[self.depTroveIdMap[x]]
                        for x in solList ])

            if newSolListList:
                suggMap[depSet] = newSolListList
                self.troveCache.addDepSolution(self.troveTupSig, depSet,
                                               newSolListList)

        # Add negative cache entries for any remaining requirements that
        # weren't solved
        for depSet in finalDepList:
            if depSet not in suggMap:
                self.troveCache.addDepSolution(self.troveTupSig, depSet, [])

        self.depDb.db.rollback()

        suggMap.update(cachedSuggMap)

        return suggMap

class TroveTupleSetSearchSource(searchsource.SearchSource):
    """
        Search source using a list of troves.  Accepts either
        a list of trove tuples or a list of trove objects.
    """
    def __init__(self, troveSource, troveSet, flavor):
        assert(isinstance(troveSource, SimpleFilteredTroveSource))
        searchsource.SearchSource.__init__(self, troveSource, flavor)

    def getSearchPath(self):
        return self.troveSet._getInstallSet() | self.troveSet._getOptionalSet()

class TroveSet(object):

    def __init__(self, graph = None, index = None):
        assert(graph)
        self.realized = False
        self.g = graph
        self.index = index

    def __str__(self):
        return self.__class__.__name__ #+ '%' + str(id(self))

    def _action(self, *args, **kwargs):
        ActionClass = kwargs.pop('ActionClass')
        index = kwargs.pop('index', None)
        edgeList = kwargs.pop('edgeList', None)
        if isinstance(edgeList, (list, tuple, set)):
            edgeList = iter(edgeList)

        action = ActionClass(self, *args, **kwargs)
        troveSet = action.getResultTupleSet(graph = self.g, index = index)
        inputSets = action.getInputSets()

        self.g.addNode(troveSet)

        for inputSet in inputSets:
            if edgeList:
                edgeValue = edgeList.next()
            else:
                edgeValue = None

            self.g.addEdge(inputSet, troveSet, value = edgeValue)

        return troveSet

class TroveTupleSet(TroveSet):

    def _findTroves(self, troveTuple, allowMissing = False):
        return self._getSearchSource().findTroves(troveTuple,
                                                  allowMissing = allowMissing)

    def _getTroveSource(self):
        if self._troveSource is None:
            self._troveSource = TroveTupleSetTroveSource(
                        self.g.actionData.troveCache, self)

        return self._troveSource

    def _getResolveSource(self, filterFn = None, depDb = None):
        if self._resolveSource is None:
            resolveTroveSource = ResolveTroveTupleSetTroveSource(
                                        self.g.actionData.troveCache, self,
                                        self.g.actionData.flavor,
                                        filterFn = filterFn, depDb = depDb)
            self._resolveSource = TroveTupleSetSearchSource(
                                    resolveTroveSource, self,
                                    self.g.actionData.flavor)

        return self._resolveSource

    def _getSearchSource(self):
        if self._searchSource is None:
            self._searchSource = TroveTupleSetSearchSource(
                                    self._getTroveSource(), self,
                                    self.g.actionData.flavor)

        return self._searchSource

    def _setInstall(self, l):
        self.installSet.update(l)
        self.optionalSet.difference_update(set(l))

    def _setOptional(self, l):
        self.optionalSet.update(l)
        self.installSet.difference_update(set(l))

    def _getInstallSet(self):
        assert(self.realized)
        return self.installSet

    def _getOptionalSet(self):
        assert(self.realized)
        return self.optionalSet

    def _getSignature(self, troveCache):
        if self._sig is None:
            troveTupCollection = trove.TroveTupleList()

            for troveTup, inInstall, isExplicit in \
                        self._walk(troveCache, newGroups = False,
                                   recurse = True):
                if isExplicit:
                    troveTupCollection.add(*troveTup)

            s = troveTupCollection.freeze()
            self._sig = sha1helper.sha1String(s)

        return self._sig

    def __init__(self, *args, **kwargs):
        TroveSet.__init__(self, *args, **kwargs)
        self._troveSource = None
        self._resolveSource = None
        self._searchSource = None
        self.installSet = set()
        self.optionalSet = set()
        self._walkCache = None
        self._sig = None

    def _walk(self, troveCache, newGroups = True, recurse = False,
              installSetOverrides = {}):
        """
        Return ((name, version, flavor), inInstallSet, explicit) tuples
        for the troves referenced by this TroveSet. inInstallSet is True
        if this trove is included in the installSet (byDefault True) for
        any of the troves which include it. It is considered explicit
        iff it is included directly by this TroveSet.

        @param troveCache: TroveCache to use for iterating trove contents
        @type troveCache: TroveSource
        @param newGroups: Return newly created groups. Version will
        be NewVersion().
        @type newGroups: bool
        @param recurse: Return full recursive closure. When possible, implicit
        includes are used to generate this information.
        @type recurse: bool
        @rtype: ((str, versions.Version, deps.Flavor), isInstall, isExplicit)
        """

        if not recurse:
            result = []
            for (troveTup) in self._getInstallSet():
                inInstallSet = installSetOverrides.get(troveTup, True)
                if (newGroups
                        or not isinstance(troveTup[1], versions.NewVersion)):
                    result.append( (troveTup, inInstallSet, True) )

            for (troveTup) in self._getOptionalSet():
                inInstallSet = installSetOverrides.get(troveTup, False)
                if (newGroups
                        or not isinstance(troveTup[1], versions.NewVersion)):
                    result.append( (troveTup, inInstallSet, True) )

            return result

        if not installSetOverrides and self._walkCache is not None:
            return self._walkCache

        walkResult = []

        usedPackages = set()
        for troveTuple in itertools.chain(self.installSet, self.optionalSet):
            if trove.troveIsComponent(troveTuple[0]):
                usedPackages.add(troveTuple[0].split(":")[0])

        collections = list()
        newCollections = list()
        for troveTuple in itertools.chain(self.installSet, self.optionalSet):
            if (isinstance(troveTuple[1], versions.NewVersion)):
                newCollections.append(troveTuple)
            elif (trove.troveIsGroup(troveTuple[0]) or
                        troveTuple[0] in usedPackages):
                collections.append(troveTuple)

        troveCache.cacheTroves(collections)

        containedBy = dict ( (x, []) for x in
                           itertools.chain(self.installSet, self.optionalSet))
        containsItems = dict ( (x, False) for x in
                           itertools.chain(self.installSet, self.optionalSet))

        for troveTuple in itertools.chain(self.installSet, self.optionalSet):
            for collection in itertools.chain(collections, newCollections):
                if troveCache.troveReferencesTrove(collection, troveTuple):
                    containsItems[collection] = True
                    containedBy[troveTuple].append(collection)

        # for each pair of troves determine the longest path between them; we
        # do this through a simple tree walk
        maxPathLength = {}
        searchList = [ (x, x, 0) for x, y in containsItems.iteritems()
                            if not y ]
        while searchList:
            start, next, depth = searchList.pop(0)

            knownDepth = maxPathLength.get( (start, next), -1 )
            if depth > knownDepth:
                maxPathLength[(start, next)] = depth

            for container in containedBy[next]:
                searchList.append( (start, container, depth + 2) )

        searchList = sorted([ (x, x, 0) for x, y in containsItems.iteritems()
                              if not y ])

        def handle(tt, dp, ii):
            val = results.get(tt)

            if val is None:
                results[tt] = (dp, ii)
            elif val[0] == dp:
                results[tt] = (dp, ii or val[1])
            elif val[0] > dp:
                results[tt] = (dp, ii)

        results = {}
        seenDepths = {}
        while searchList:
            start, troveTup, depth = searchList.pop(0)

            if depth < maxPathLength[(start, troveTup)]:
                continue
            assert(maxPathLength[(start, troveTup)] == depth)

            seenAtDepth = seenDepths.get(troveTup)
            if seenAtDepth is not None and seenAtDepth <= depth:
                # we've walked this at a lower depth; there is no reason
                # to do so again
                continue
            seenDepths[troveTup] = depth

            inInstallSet = installSetOverrides.get(troveTup,
                                                   troveTup in self.installSet)

            handle(troveTup, depth, inInstallSet)

            for child in containedBy[troveTup]:
                searchList.append( (start, child, depth + 2) )

            if not recurse:
                continue

            if inInstallSet or not trove.troveIsPackage(troveTup[0]):
                for subTroveTup, subIsInstall, subIsExplicit in \
                                troveCache.iterTroveListInfo(troveTup):
                    overridenSubIsInstall = installSetOverrides.get(
                            subTroveTup, subIsInstall)
                    handle(subTroveTup, depth + 1,
                           inInstallSet and overridenSubIsInstall)
            else:
                for componentName in troveCache.getPackageComponents(troveTup):
                    handle((componentName, troveTup[1], troveTup[2]),
                           depth + 1, False)

        for (troveTup), (depth, isInstall) in results.iteritems():
            if (newGroups
                    or not isinstance(troveTup[1], versions.NewVersion)):
                walkResult.append(
                        (troveTup, isInstall,
                            (troveTup in self.installSet or
                             troveTup in self.optionalSet) ) )

        if not installSetOverrides:
            self._walkCache = walkResult

        return walkResult

class DelayedTupleSet(TroveTupleSet):

    def __init__(self, graph = None, action = None, index = None):
        assert(graph)
        assert(action)
        TroveTupleSet.__init__(self, graph = graph, index = index)
        self.action = action

    def __str__(self):
        if self.index is not None:
            return str(self.action) + ':' + str(self.index)

        return str(self.action)

    def beenRealized(self, data):
        self.realized = True

    def realize(self, data):
        result = self.action(data)
        assert(result is not None)  # don't let actions forget to return a bool
        if result:
            self.beenRealized(data)
            return True

        return False

class StaticTroveTupleSet(TroveTupleSet):

    def __str__(self):
        if self._getInstallSet():
            return list(self._getInstallSet())[0][0]

        return TroveTupleSet.__str__(self)

    def __init__(self, *args, **kwargs):
        troveTuple = kwargs.pop('troveTuple', None)
        TroveTupleSet.__init__(self, *args, **kwargs)
        if troveTuple is not None:
            self._setInstall(set(troveTuple))
        self.realized = True

class SearchSourceTroveSet(TroveSet):

    def _findTroves(self, troveTuple, allowMissing = True):
        return self.searchSource.findTroves(troveTuple, requireLatest = True,
                                            allowMissing = allowMissing)

    def _getResolveSource(self, depDb = None, filterFn = None):
        return self.searchSource

    def _getSearchSource(self):
        return self.searchSource

    def __init__(self, searchSource, graph = None, index = None):
        TroveSet.__init__(self, graph = graph, index = index)
        self.realized = (searchSource is not None)
        self.searchSource = searchSource

class SearchPathTroveSet(SearchSourceTroveSet):

    def __init__(self, troveSetList = None, graph = None, index = None):
        SearchSourceTroveSet.__init__(self, None, graph = graph,
                                      index = index)

        self.troveSetList = None

        if troveSetList is not None:
            self.setTroveSetList(self.fetch(troveSetList))

    def fetch(self, troveSetList):
        # fetch all of the trovesets in troveSetList. it is acceptable
        # for the caller to do this manually, in which case setTroveList()
        # can be used directly to avoid this step
        resultList = []
        for i, troveSet in enumerate(troveSetList):
            if isinstance(troveSet, TroveTupleSet):
                fetched = troveSet._action(ActionClass = FetchAction)
                resultList.append(fetched)
                self.g.addEdge(fetched, self, value = str(i + 1))
            else:
                resultList.append(troveSet)
                self.g.addEdge(troveSet, self, value = str(i + 1))

        return resultList

    def setTroveSetList(self, troveSetList, fetch = True):
        # troveSetList must be fetched before calling this
        assert(self.troveSetList is None)
        self.troveSetList = troveSetList

    def _getResolveSource(self, depDb = None, filterFn = None):
        # we search differently then we resolve; resolving is recursive
        # while searching isn't
        sourceList = [ ts._getResolveSource(depDb = depDb, filterFn = filterFn)
                            for ts in self.troveSetList ]
        return searchsource.SearchSourceStack(*sourceList)

    def realize(self, data):
        sourceList = [ ts._getSearchSource() for ts in self.troveSetList ]
        self.searchSource = searchsource.SearchSourceStack(*sourceList)
        self.realized = True

class ActionData(object):

    def __init__(self, repos, flavor):
        self.troveCache = repos
        self.flavor = flavor

class Action(object):

    def __str__(self):
        return self.__class__.__name__[:-6]

class DelayedTupleSetAction(Action):

    prefilter = None
    resultClass = DelayedTupleSet

    def __init__(self, primaryTroveSet, *args):
        inputSets = [ primaryTroveSet ]
        inputSets += [ x for x in args if isinstance(x, TroveSet) ]
        self._inputSets = self._applyFilters(inputSets)
        self.primaryTroveSet = self._inputSets[0]

    def _applyFilters(self, l):
        if not self.prefilter:
            return l

        r = []
        for ts in l:
            newTs = ts._action(ActionClass = self.prefilter)
            r.append(newTs)

        return r

    def getInputSets(self):
        return self._inputSets

    def getResultTupleSet(self, graph = None, index = None):
        self.outSet = self.resultClass(action = self, graph = graph,
                                       index = index)
        return self.outSet

class ParallelAction(DelayedTupleSetAction):

    pass

class DifferenceAction(DelayedTupleSetAction):

    def differenceAction(self, data):
        left = self.primaryTroveSet
        right = self.right
        all = right._getInstallSet().union(right._getInstallSet())

        self.outSet._setInstall(left._getInstallSet().difference(all))
        self.outSet._setOptional(left._getOptionalSet().difference(all))

        return True

    __call__ = differenceAction

    def __init__(self, primaryTroveSet, other):
        DelayedTupleSetAction.__init__(self, primaryTroveSet, other)
        self.right = other

class FetchAction(ParallelAction):

    # this is somewhat recursive because troveCache.getTroves() is
    # somewhat recursive; we need to mimic that for created subgroups
    #
    # it would be awfully nice if this used iterTroveListInfo(), but
    # the whole point is to cache troves so iterTroveListInfo() can assume
    # they're already there

    def __init__(self, primaryTroveSet, all = False):
        ParallelAction.__init__(self, primaryTroveSet)
        self.fetchAll = all

    def fetchAction(self, actionList, data):
        for action in actionList:
            action.outSet._setOptional(action.primaryTroveSet._getOptionalSet())
            action.outSet._setInstall(action.primaryTroveSet._getInstallSet())

        self._fetch(actionList, data);

        return True

    __call__ = fetchAction

    @staticmethod
    def _fetch(actionList, data):
        troveTuples = set()

        for action in actionList:
            newTuples = [ x[0] for x in
                                 action.primaryTroveSet._walk(data.troveCache,
                                                 newGroups = False) ]

            if not action.fetchAll:
                newTuples = [ x for x in newTuples
                                if not trove.troveIsComponent(x[0]) ]

            troveTuples.update(newTuples)

        data.troveCache.getTroves(troveTuples, withFiles = False)

class FindAction(ParallelAction):

    def __init__(self, primaryTroveSet, *troveSpecs):
        ParallelAction.__init__(self, primaryTroveSet)
        self.troveSpecs = troveSpecs

    def findAction(self, actionList, data):
        troveSpecsByInSet = {}
        for action in actionList:
            l = troveSpecsByInSet.setdefault(action.primaryTroveSet, [])
            from conary.conaryclient.cmdline import parseTroveSpec
            for troveSpec in action.troveSpecs:
                # handle str's that need parsing as well as tuples which
                # have already been parsed
                if isinstance(troveSpec, str):
                    l.append((action.outSet, parseTroveSpec(troveSpec)))
                else:
                    l.append((action.outSet, troveSpec))

        notFound = set()
        for inSet, searchList in troveSpecsByInSet.iteritems():
            cacheable = set()
            cached = set()
            for i, (outSet, troveSpec) in enumerate(searchList):
                if troveSpec.version and '/' in troveSpec.version:
                    match = data.troveCache.getFindResult(troveSpec)
                    if match is None:
                        cacheable.add(i)
                    else:
                        cached.add(i)
                        outSet._setInstall(match)

            d = inSet._findTroves([ x[1] for i, x in enumerate(searchList)
                                            if i not in cached ])
            for i, (outSet, troveSpec) in enumerate(searchList):
                if i in cached:
                    continue

                if troveSpec in d:
                    outSet._setInstall(d[troveSpec])
                    if i in cacheable:
                        data.troveCache.addFindResult(troveSpec,
                                                      d[troveSpec])
                else:
                    notFound.add(troveSpec)

        if notFound:
            raise TroveSpecsNotFound(sorted(notFound))

        return True

    __call__ = findAction

    def __str__(self):
        if isinstance(self.troveSpecs[0], str):
            n1 = self.troveSpecs[0].split('=')[0]
        else:
            n1 = self.troveSpecs[0][0]

        if isinstance(self.troveSpecs[-1], str):
            n2 = self.troveSpecs[-1].split('=')[0]
        else:
            n2 = self.troveSpecs[-1][0]

        if len(self.troveSpecs) == 1:
            s =  n1
        elif len(self.troveSpecs) == 2:
            s =  n1 + r' ,\n' + n2
        else:
            s =  n1 + r' ...\n' + n2

        return r'Find\n' + s

class UnionAction(DelayedTupleSetAction):

    def __init__(self, primaryTroveSet, *args):
        DelayedTupleSetAction.__init__(self, primaryTroveSet, *args)

    def unionAction(self, data):
        # this ordering means that if it's in the install set anywhere, it
        # will be in the install set in the union
        tsList = self._inputSets
        for troveSet in tsList:
            self.outSet._setOptional(troveSet._getOptionalSet())

        for troveSet in tsList:
            self.outSet._setInstall(troveSet._getInstallSet())

        return True

    __call__ = unionAction

class OptionalAction(DelayedTupleSetAction):

    def __init__(self, primaryTroveSet, *args):
        DelayedTupleSetAction.__init__(self, primaryTroveSet, *args)

    def optionalAction(self, data):
        for troveSet in self._inputSets:
            self.outSet._setOptional(troveSet._getOptionalSet())
            self.outSet._setOptional(troveSet._getInstallSet())

        return True

    __call__ = optionalAction

class RemoveAction(DelayedTupleSetAction):

    prefilter = FetchAction

    def __init__(self, primaryTroveSet, removeTroveSet = None):
        DelayedTupleSetAction.__init__(self, primaryTroveSet, removeTroveSet)
        self.removeTroveSet = removeTroveSet

    def removeAction(self, data):
        explicitRemoveSet = (self.removeTroveSet._getOptionalSet() |
                     self.removeTroveSet._getInstallSet())

        implicitRemoveSet = (set(
            [ x[0] for x in self.removeTroveSet._walk(data.troveCache,
                                                      recurse = True) ] )
            - explicitRemoveSet)

        self.outSet._setInstall(self.primaryTroveSet._getInstallSet()
                                    - explicitRemoveSet - implicitRemoveSet)
        self.outSet._setOptional(self.primaryTroveSet._getOptionalSet()
                                    | explicitRemoveSet)

        return True

    __call__ = removeAction

class AbstractModifyAction(DelayedTupleSetAction):

    def buildAfter(self, troveCache):
        after = trove.Trove("@tsupdate", versions.NewVersion(),
                             deps.Flavor())

        # store the mapping of what changed for explicit troves; we peek
        # at this for CM simplification
        explicitTups = set()

        afterInfo = {}
        updateNames = set()
        for troveTup, inInstallSet, explicit in \
                  self.updateTroveSet._walk(troveCache, recurse = True):
            after.addTrove(troveTup[0], troveTup[1], troveTup[2])
            afterInfo[troveTup] = (inInstallSet, explicit)
            updateNames.add(troveTup[0])
            if explicit:
                explicitTups.add(troveTup)

        return after, afterInfo, updateNames, explicitTups

    def buildBefore(self, troveCache, updateNames, installOverrides = {}):
        before = trove.Trove("@tsupdate", versions.NewVersion(),
                             deps.Flavor())

        beforeInfo = {}
        installSet = set()
        optionalSet = set()
        for troveTup, inInstallSet, explicit in \
                  self.primaryTroveSet._walk(troveCache, recurse = True,
                                             installSetOverrides =
                                                installOverrides):
            if troveTup[0] in updateNames:
                before.addTrove(troveTup[0], troveTup[1], troveTup[2])
                beforeInfo[troveTup] = (inInstallSet, explicit)
            elif explicit:
                if inInstallSet:
                    installSet.add(troveTup)
                else:
                    optionalSet.add(troveTup)

        return before, beforeInfo, installSet, optionalSet

class PatchAction(AbstractModifyAction):

    prefilter = FetchAction

    def __init__(self, primaryTroveSet, updateTroveSet):
        AbstractModifyAction.__init__(self, primaryTroveSet, updateTroveSet)
        self.updateTroveSet = updateTroveSet

    def patchAction(self, data):
        before = trove.Trove("@tsupdate", versions.NewVersion(),
                             deps.Flavor())

        after, afterInfo, updateNames, explicitTups = \
                    self.buildAfter(data.troveCache)
        before, beforeInfo, installSet, optionalSet = \
                    self.buildBefore(data.troveCache, updateNames)

        troveMapping = after.diff(before)[2]

        # populate the cache with timestamped versions as a bulk operation
        data.troveCache.getTimestamps( beforeInfo.keys() + afterInfo.keys() )

        for (trvName, (oldVersion, oldFlavor),
                      (newVersion, newFlavor), isAbsolute) in troveMapping:
            oldTuple = (trvName, oldVersion, oldFlavor)
            newTuple = (trvName, newVersion, newFlavor)
            self._handleTrove(data, beforeInfo, afterInfo, oldTuple, newTuple,
                              installSet, optionalSet)

        self.outSet._setInstall(installSet)
        self.outSet._setOptional(optionalSet)

        return True

    __call__ = patchAction

    def _handleTrove(self, data, beforeInfo, afterInfo, oldTuple, newTuple,
                     installSet, optionalSet):
        if oldTuple[1] and newTuple[1]:
            oldVersion = data.troveCache.getTimestamps([ oldTuple ])[0]
            newVersion = data.troveCache.getTimestamps([ newTuple ])[0]

            if (oldVersion > newVersion):
                # the old one is newer than the new one. leave it where we
                # found it
                wasInInstallSet, wasExplicit = beforeInfo[oldTuple]
                if wasExplicit:
                    if wasInInstallSet:
                        installSet.add(oldTuple)
                    else:
                        optionalSet.add(oldTuple)

                return
        else:
            oldVersion = oldTuple[1]
            newVersion = newTuple[1]

        if not oldVersion:
            # something in the update doesn't map to something we
            # had previously; make it optional
            optionalSet.add(newTuple)
        elif not newVersion:
            # something we used to have doesn't map to anything in
            # the update, keep it
            wasInInstallSet, wasExplicit = beforeInfo[oldTuple]
            if wasExplicit:
                if wasInInstallSet:
                    installSet.add(oldTuple)
                else:
                    optionalSet.add(oldTuple)
        else:
            # we've mapped an update; turn off the old version (by
            # marking it as optional) and include the new one with the
            # same defaultness as the old one had.
            wasInInstallSet, wasExplicit = beforeInfo[oldTuple]
            inInstallSet, isExplicit = afterInfo[newTuple]
            if wasInInstallSet:
                installSet.add(newTuple)
            else:
                optionalSet.add(newTuple)

            if newTuple != oldTuple:
                optionalSet.add(oldTuple)

class UpdateAction(AbstractModifyAction):

    prefilter = FetchAction

    def __init__(self, primaryTroveSet, updateTroveSet):
        AbstractModifyAction.__init__(self, primaryTroveSet, updateTroveSet)
        self.updateTroveSet = updateTroveSet

    def _completeMapping(self, troveMapping, before, after):
        # mappings completely miss anything where the only change is
        # byDefault status, or where there is no change at all. both of those
        # are important for update
        for troveTup in (set(after.iterTroveList(strongRefs = True)) &
                         set(before.iterTroveList(strongRefs = True))):
            troveMapping.append( (troveTup[0], troveTup[1:3],
                                  troveTup[1:3], False) )

    def updateAction(self, data):
        # figure out which updates are from explictly named troves in the
        # update set
        after = trove.Trove("@tsupdateouter", versions.NewVersion(),
                             deps.Flavor())
        before = trove.Trove("@tsupdateouter", versions.NewVersion(),
                             deps.Flavor())
        names = set()
        for troveTup, inInstallSet, explicit in \
                  self.updateTroveSet._walk(data.troveCache, recurse = False):
            assert(inInstallSet)
            if explicit:
                after.addTrove(*troveTup)
                names.add(troveTup[0])

        beforeIncluded = {}
        for troveTup, inInstallSet, explicit in \
                  self.primaryTroveSet._walk(data.troveCache, recurse = True):
            if troveTup[0] in names:
                before.addTrove(*troveTup)
                beforeIncluded[troveTup] = inInstallSet

        troveMapping = after.diff(before)[2]
        self._completeMapping(troveMapping, before, after)
        del before, after, names

        # this doesn't really belong here, but we need this information
        # for old troves only on update
        data.troveCache.cacheTroves( [ (name,) + oldInfo for
            (name, oldInfo, newInfo, _) in troveMapping
            if oldInfo[0] and newInfo[0] ] )

        installOverrides = {}
        for (name, oldInfo, newInfo, absolute) in troveMapping:
            if oldInfo[0] is None or newInfo[0] is None:
                continue

            oldTuple = (name,) + oldInfo
            if beforeIncluded[oldTuple]:
                continue

            installOverrides[oldTuple] = True

            for subTroveTup, subIsInstall, subIsExplicit in \
                            data.troveCache.iterTroveListInfo(oldTuple):
                installOverrides[subTroveTup] = (
                    installOverrides.get(subTroveTup, False) or subIsInstall)

        after, afterInfo, updateNames, explicitTups = \
                    self.buildAfter(data.troveCache)
        before, beforeInfo, installSet, optionalSet = \
                    self.buildBefore(data.troveCache, updateNames,
                                     installOverrides = installOverrides)

        troveMapping = after.diff(before)[2]
        self._completeMapping(troveMapping, before, after)

        self.outSet.updateMap = {}
        for (trvName, (oldVersion, oldFlavor),
                      (newVersion, newFlavor), isAbsolute) in troveMapping:
            oldTuple = (trvName, oldVersion, oldFlavor)
            newTuple = (trvName, newVersion, newFlavor)
            self._handleTrove(data, beforeInfo, afterInfo, oldTuple, newTuple,
                              installSet, optionalSet)

            if newTuple in explicitTups:
                if oldTuple[1] is not None and oldTuple != newTuple:
                    self.outSet.updateMap[newTuple] = oldTuple
                else:
                    self.outSet.updateMap[newTuple] = None

        self.outSet._setInstall(installSet)
        self.outSet._setOptional(optionalSet)

        return True

    __call__ = updateAction

    def _handleTrove(self, data, beforeInfo, afterInfo, oldTuple, newTuple,
                     installSet, optionalSet):
        oldVersion = oldTuple[1]
        newVersion = newTuple[1]

        if not oldVersion:
            # something in the update doesn't map to something we
            # had previously, include it if it is explicit in the
            # new trove, and use the new trove's install/optional value
            isInInstallSet, isExplicit = afterInfo[newTuple]
            if isExplicit:
                if isInInstallSet:
                    installSet.add(newTuple)
                else:
                    optionalSet.add(newTuple)
        elif not newVersion:
            # something we used to have doesn't map to anything in
            # the update, keep it if it was explicit
            wasInInstallSet, wasExplicit = beforeInfo[oldTuple]
            if wasExplicit:
                if wasInInstallSet:
                    installSet.add(oldTuple)
                else:
                    optionalSet.add(oldTuple)
        else:
            # it existed before and after; keep the install setting
            # we used before
            wasInInstallSet, wasExplicit = beforeInfo[oldTuple]
            isInInstallSet, isExplicit = afterInfo[newTuple]
            if wasInInstallSet or (isExplicit and isInInstallSet):
                installSet.add(newTuple)
            else:
                optionalSet.add(newTuple)

            if oldTuple != newTuple:
                optionalSet.add(oldTuple)

class IncludeException(ConaryError):

    pass

class IncludeAction(DelayedTupleSetAction):

    def __init__(self, primaryTroveSet, includeSet, searchSet,
                 compiler = None, SearchPathClass = None):
        DelayedTupleSetAction.__init__(self, primaryTroveSet,
                                       includeSet, searchSet)

        self.includeSet = includeSet
        self.searchSet = searchSet
        self.compiler = compiler
        self.resultSet = None
        self.SearchPathClass = SearchPathClass

    def getResultTupleSet(self, graph = None, index = None):
        result = DelayedTupleSetAction.getResultTupleSet(self,
                                                         graph = graph,
                                                         index = index)
        self.outSet.finalSearchSet = self.SearchPathClass(graph = graph,
                                                          index = index)
        self.outSet.g.addEdge(result, self.outSet.finalSearchSet)
        return result

    def getCML(self, troveCache, nvf):
        key = "%s=%s[%s]" % nvf
        lines = troveCache.getCachedFile(key)
        if lines is not None:
            return lines

        cs = troveCache.getRepos().createChangeSet(
            [ (nvf[0], (None, None), (nvf[1], nvf[2]), True) ],
            withFiles = True, withFileContents = True)

        trv = trove.Trove(cs.getNewTroveVersion(*nvf))

        files = list(trv.iterFileList())
        if nvf[0].endswith(':source'):
            files = [ x for x in files if x[1].endswith('.cml') ]
        if len(files) > 1:
            raise IncludeException('Too many cml files found in %s=%s[%s]: %s'
                        % (nvf + (" ".join(x[1] for x in sorted(files)),)))
        elif not files:
            raise IncludeException('No cml files found in %s=%s[%s]' % nvf)

        fileContents = cs.getFileContents(files[0][0], files[0][2])
        lines = fileContents[1].get().readlines()
        troveCache.cacheFile(key, lines)

        return lines

    def includeAction(self, data):
        if self.resultSet:
            self.outSet._setInstall(self.resultSet._getInstallSet())
            self.outSet._setOptional(self.resultSet._getOptionalSet())

            return True

        assert(not self.includeSet._getOptionalSet())
        assert(not self.includeSet._getInstallSet() == 1)

        nvf = list(self.includeSet._getInstallSet())[0]

        if not trove.troveIsComponent(nvf[0]):
            assert(trove.troveIsPackage(nvf[0]))
            # getTrove is sometimes disabled to prevent one at a time calls
            # can't be helped here
            trv = data.troveCache.getTroves([ nvf], withFiles=False)[0]
            found = None
            for subNVF in trv.iterTroveList(strongRefs = True):
                if subNVF[0].endswith(':cml'):
                    found = subNVF
                    break

            if not found:
                raise IncludeException('Package %s=%s[%s] does not contain a '
                                       'cml component for inclusion' % nvf)
            nvf = found
        elif nvf[0].split(':')[1] not in [ 'cml', 'source' ]:
            raise IncludeException('Include only supports source and cml '
                                   'components')

        if nvf in self.outSet.g.included:
            raise IncludeException('Include loop detected involving %s=%s[%s]'
                                   % nvf)

        self.outSet.g.included.add(nvf)

        cmlFileLines = self.getCML(data.troveCache, nvf)

        model = cml.CML(None, context = nvf[0])
        model.parse(fileData = cmlFileLines)
        self.resultSet = self.compiler.augment(model, self.searchSet,
                                               self.primaryTroveSet)
        self.outSet.g.addEdge(self.resultSet, self.outSet)

        self.outSet.finalSearchSet.setTroveSetList(
                            self.outSet.finalSearchSet.fetch(
                                            [ self.resultSet.searchPath ]) )

        return False

    def augment(self, model, totalSearchSet, finalTroveSet):

        return False

    __call__ = includeAction

class OperationGraph(graph.DirectedGraph):

    def __init__(self, *args, **kwargs):
        graph.DirectedGraph.__init__(self, *args, **kwargs)
        self.included = set()

    def realize(self, data):
        # this is a hack
        self.actionData = data

        reset = True
        while True:
            if reset:
                transpose = self.transpose()
                ordering = self.getTotalOrdering()
                reset = False

            # grab as many bits as we can whose parents have been realized
            layer = []
            needWork = False
            for node in ordering:
                if node.realized: continue

                needWork = True
                parents = transpose.getChildren(node)
                if len([ x for x in parents if x.realized ]) == len(parents):
                    layer.append(node)

            if not needWork:
                assert(not layer)
                break

            assert(layer)
            byAction = {}

            for node in layer:
                if not node.realized:
                    if isinstance(node, DelayedTupleSet):
                        byAction.setdefault(
                            node.action.__class__, []).append(node)
                    else:
                        node.realize(data)

            for action, nodeList in byAction.iteritems():
                if issubclass(action, ParallelAction):
                    nodeList[0].action([ node.action for node in nodeList ],
                                       data)
                    for node in nodeList:
                        node.beenRealized(data)
                else:
                    for node in nodeList:
                        if not node.realize(data):
                            reset = True

    def trace(self, troveSpecList):
        ordering = self.getTotalOrdering()

        for node in ordering:
            if isinstance(node, TroveTupleSet):
                if node.index is None:
                    continue

                matches = node._findTroves(troveSpecList, allowMissing = True)
                for troveSpec, matchList in matches.iteritems():
                    installMatches = set(matchList) & node._getInstallSet()
                    optionalMatches = set(matchList) & node._getOptionalSet()
                    if installMatches:
                        print 'trace line %s matched install set "%s": %s' % (
                             str(node), str(troveSpec),
                             " ".join([ "%s=%s[%s]" % x
                             for x in installMatches ]) )
                    if optionalMatches:
                        print 'trace line %s matched optional set "%s": %s' % (
                             str(node), str(troveSpec),
                             " ".join([ "%s=%s[%s]" % x
                             for x in optionalMatches ]) )
