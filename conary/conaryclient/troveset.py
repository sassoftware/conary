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

import itertools

from conary import trove, versions
from conary.deps import deps
from conary.lib import graph, sha1helper
from conary.local import deptable
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
                   itertools.chain(troveSet._getInstallSet(),
                                   troveSet._getOptionalSet()))

class ResolveTroveTupleSetTroveSource(SimpleFilteredTroveSource):

    """
    Similar to TroveTupleSetTroveSource, but newly created groups are
    not present; instead their members are included. This is designed
    for dependency solving.
    """

    def __init__(self, troveCache, troveSet, flavor,
                 filterFn = lambda *args: False):
        self.depDb = None

        self.troveTupList = []
        troveTupCollection = trove.TroveTupleList()
        for troveTup, inInstall, isExplicit in troveSet._walk(troveCache,
                                                    newGroups = False,
                                                    recurse = True):
            if not filterFn(*troveTup):
                self.troveTupList.append(troveTup)
                troveTupCollection.add(*troveTup)

        self.troveTupSig = sha1helper.sha1String(troveTupCollection.freeze())

        self.inDepDb = [ False ] * len(self.troveTupList)

        SimpleFilteredTroveSource.__init__(self, troveCache,
                                              self.troveTupList)

        self.setFlavorPreferencesByFlavor(flavor)
        self.searchWithFlavor()
        self.searchLeavesOnly()
        self.depDb = deptable.DependencyDatabase()
        self.providesIndex = None

    def resolveDependencies(self, label, depList, leavesOnly=False):
        def _depClassAndName(oneDep):
            s = set()

            for depClass, depName, flags in oneDep.iterRawDeps():
                s.add( (depClass, depName) )

            return s

        reqNames = set()
        for dep in depList:
            reqNames.update(_depClassAndName(dep))

        emptyDep = deps.DependencySet()
        troveDeps = self.troveCache.getDepsForTroveList(self.troveTupList)

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


        for classAndName in reqNames:
            val = self.providesIndex.get(classAndName)
            if val is None:
                continue

            for i in val:
                if self.inDepDb[i]:
                    continue

                self.depDb.add(i, troveDeps[i][0], emptyDep)
                self.inDepDb[i] = True

        self.depDb.commit()

        suggMap = self.depDb.resolve(label, depList, leavesOnly=leavesOnly)
        for depSet, solListList in suggMap.iteritems():
            newSolListList = []
            for solList in solListList:
                newSolListList.append([ self.troveTupList[x] for x in solList ])

            suggMap[depSet] = newSolListList

        self.depDb.db.rollback()

        if suggMap:
            import epdb;epdb.st()

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

    def __init__(self, graph = None):
        assert(graph)
        self.realized = False
        self.g = graph

    def __str__(self):
        return self.__class__.__name__

    def _action(self, *args, **kwargs):
        ActionClass = kwargs.pop('ActionClass')
        edgeList = kwargs.pop('edgeList', None)
        if isinstance(edgeList, (list, tuple, set)):
            edgeList = iter(edgeList)

        action = ActionClass(self, *args, **kwargs)
        troveSet = action.getResultTupleSet(graph = self.g)
        inputSets = action.getInputSets(graph = self.g)

        self.g.addNode(troveSet)

        for inputSet in inputSets:
            if edgeList:
                edgeValue = edgeList.next()
            else:
                edgeValue = None

            self.g.addEdge(inputSet, troveSet, value = edgeValue)

        return troveSet

class TroveTupleSet(TroveSet):

    def _findTroves(self, troveTuple):
        return self._getSearchSource().findTroves(troveTuple)

    def _getTroveSource(self):
        if self._troveSource is None:
            self._troveSource = TroveTupleSetTroveSource(
                        self.g.actionData.troveCache, self)

        return self._troveSource

    def _getResolveSource(self, filterFn = lambda *args: False):
        if self._resolveSource is None:
            resolveTroveSource = ResolveTroveTupleSetTroveSource(
                                        self.g.actionData.troveCache, self,
                                        self.g.actionData.flavor,
                                        filterFn = filterFn)
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

    def __init__(self, *args, **kwargs):
        TroveSet.__init__(self, *args, **kwargs)
        self._troveSource = None
        self._resolveSource = None
        self._searchSource = None
        self.installSet = set()
        self.optionalSet = set()

    def _walk(self, troveCache, newGroups = True, recurse = False):
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
            for (troveTup) in self._getInstallSet():
                if (newGroups
                        or not isinstance(troveTup[1], versions.NewVersion)):
                    yield (troveTup, True, True)

            for (troveTup) in self._getOptionalSet():
                if (newGroups
                        or not isinstance(troveTup[1], versions.NewVersion)):
                    yield (troveTup, False, True)

            return

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

            inInstallSet = (troveTup in self.installSet)

            handle(troveTup, depth, inInstallSet)

            for child in containedBy[troveTup]:
                searchList.append( (start, child, depth + 2) )

            if not recurse:
                continue

            for subTroveTup, subIsInstall, subIsExplicit in \
                            troveCache.iterTroveListInfo(troveTup):
                handle(subTroveTup, depth + 1, inInstallSet and subIsInstall)

        for (troveTup), (depth, isInstall) in results.iteritems():
            if (newGroups
                    or not isinstance(troveTup[1], versions.NewVersion)):
                yield (troveTup, isInstall,
                       (troveTup in self.installSet or
                        troveTup in self.optionalSet) )

        return

class DelayedTupleSet(TroveTupleSet):

    def __init__(self, graph = None, action = None):
        assert(graph)
        assert(action)
        TroveTupleSet.__init__(self, graph = graph)
        self.action = action

    def __str__(self):
        return str(self.action)

    def beenRealized(self, data):
        self.realized = True

    def realize(self, data):
        self.action(data)
        self.beenRealized(data)

class StaticTroveTupleSet(TroveTupleSet):

    def __init__(self, *args, **kwargs):
        troveTuple = kwargs.pop('troveTuple', None)
        TroveTupleSet.__init__(self, *args, **kwargs)
        if troveTuple is not None:
            self._setInstall(set(troveTuple))
        self.realized = True

class SearchSourceTroveSet(TroveSet):

    def _findTroves(self, troveTuple):
        return self.searchSource.findTroves(troveTuple, requireLatest = True,
                                            allowMissing = True)

    def _getResolveSource(self):
        return self.searchSource

    def _getSearchSource(self):
        return self.searchSource

    def __init__(self, searchSource, graph = graph):
        TroveSet.__init__(self, graph = graph)
        self.realized = (searchSource is not None)
        self.searchSource = searchSource

class SearchPathTroveSet(SearchSourceTroveSet):

    def __init__(self, troveSetList, graph = None):
        self.troveSetList = troveSetList
        SearchSourceTroveSet.__init__(self, None, graph = graph)

        for i, troveSet in enumerate(troveSetList):
            graph.addEdge(troveSet, self, value = str(i + 1))

    def _getResolveSource(self):
        # we search differently then we resolve; resolving is recursive
        # while searching isn't
        sourceList = [ ts._getResolveSource() for ts in self.troveSetList ]
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
        self.primaryTroveSet = primaryTroveSet
        self._inputSets = [ self.primaryTroveSet ]
        self._inputSets += [ x for x in args if isinstance(x, TroveTupleSet) ]

    def _applyFilters(self, l, graph = None):
        r = []
        for (ts, filterAction) in l:
            newTs = ts._action(ActionClass = filterAction)
            r.append(newTs)

        return r

    def getInputSets(self, graph = None):
        if self.prefilter is None:
            return self._inputSets

        return self._applyFilters(
                [ (ts, self.prefilter) for ts in self._inputSets ] )

    def getResultTupleSet(self, graph = None):
        self.outSet = self.resultClass(action = self, graph = graph)
        return self.outSet

class ParallelAction(DelayedTupleSetAction):

    pass

class DifferenceAction(DelayedTupleSetAction):

    def __call__(self, data):
        left = self.primaryTroveSet
        right = self.right
        all = right._getInstallSet().union(right._getInstallSet())

        self.outSet._setInstall(left._getInstallSet().difference(all))
        self.outSet._setOptional(left._getOptionalSet().difference(all))

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

    def __call__(self, actionList, data):
        for action in actionList:
            action.outSet._setOptional(action.primaryTroveSet._getOptionalSet())
            action.outSet._setInstall(action.primaryTroveSet._getInstallSet())

        self._fetch(actionList, data);

    def _fetch(self, actionList, data):
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

    def __call__(self, actionList, data):
        troveSpecsByInSet = {}
        for action in actionList:
            l = troveSpecsByInSet.setdefault(action.primaryTroveSet, [])
            from conary.conaryclient.cmdline import parseTroveSpec
            for troveSpec in action.troveSpecs:
                # handle str's that need parsing as well as tuples which
                # have already been parsed
                if isinstance(troveSpec, str):
                    l.extend([ (action.outSet, parseTroveSpec(troveSpec))
                                    for troveSpec in action.troveSpecs ] )
                else:
                    l.extend([ (action.outSet, troveSpec)
                                    for troveSpec in action.troveSpecs ] )

        notFound = []
        for inSet, searchList in troveSpecsByInSet.iteritems():
            d = inSet._findTroves([ x[1] for x in searchList ])
            for outSet, troveSpec in searchList:
                if troveSpec in d:
                    outSet._setInstall(d[troveSpec])
                else:
                    notFound.append(troveSpec)

        if notFound:
            raise MissingTroves(notFound)

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
        self.troveSets = [ primaryTroveSet ] + list(args)

    def __call__(self, data):
        # this ordering means that if it's in the install set anywhere, it
        # will be in the install set in the union
        tsList = self._inputSets
        for troveSet in tsList:
            self.outSet._setOptional(troveSet._getOptionalSet())

        for troveSet in tsList:
            self.outSet._setInstall(troveSet._getInstallSet())

class ReplaceAction(DelayedTupleSetAction):

    prefilter = FetchAction

    def __init__(self, primaryTroveSet, updateTroveSet):
        DelayedTupleSetAction.__init__(self, primaryTroveSet, updateTroveSet)
        self.updateTroveSet = updateTroveSet

    def replaceAction(self, data):
        before = trove.Trove("@tsupdate", versions.NewVersion(),
                             deps.Flavor())
        after = trove.Trove("@tsupdate", versions.NewVersion(),
                             deps.Flavor())
        afterInfo = {}
        updateNames = set()
        for troveTup, inInstallSet, explicit in \
                  self.updateTroveSet._walk(data.troveCache, recurse = True):
            after.addTrove(troveTup[0], troveTup[1], troveTup[2])
            afterInfo[troveTup] = (inInstallSet, explicit)
            updateNames.add(troveTup[0])

        beforeInfo = {}
        installSet = set()
        optionalSet = set()
        for troveTup, inInstallSet, explicit in \
                  self.primaryTroveSet._walk(data.troveCache, recurse = True):
            if troveTup[0] in updateNames:
                before.addTrove(troveTup[0], troveTup[1], troveTup[2])
                beforeInfo[troveTup] = (inInstallSet, explicit)
            elif explicit:
                if inInstallSet:
                    installSet.add(troveTup)
                else:
                    optionalSet.add(troveTup)

        troveMapping = after.diff(before)[2]
        # this completely misses anything where the only change is
        # byDefault status
        for troveTup in (set(after.iterTroveList(strongRefs = True)) &
                         set(before.iterTroveList(strongRefs = True))):
            if beforeInfo[troveTup] != afterInfo[troveTup]:
                troveMapping.append( (troveTup[0], troveTup[1:3],
                                      troveTup[1:3], False) )

        for (trvName, (oldVersion, oldFlavor),
                      (newVersion, newFlavor), isAbsolute) in troveMapping:
            oldTuple = (trvName, oldVersion, oldFlavor)
            newTuple = (trvName, newVersion, newFlavor)
            self._handleTrove(beforeInfo, afterInfo, oldTuple, newTuple,
                              installSet, optionalSet)

        self.outSet._setInstall(installSet)
        self.outSet._setOptional(optionalSet)

    __call__ = replaceAction

    def _handleTrove(self, beforeInfo, afterInfo, oldTuple, newTuple,
                     installSet, optionalSet):
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
            # same defaultness as the old one had. if it is explicit
            # in the new one, we always include it
            wasInInstallSet, wasExplicit = beforeInfo[oldTuple]
            inInstallSet, isExplicit = afterInfo[newTuple]
            if (isExplicit and inInstallSet) or wasInInstallSet:
                installSet.add(newTuple)
            else:
                optionalSet.add(newTuple)

            if newTuple != oldTuple:
                optionalSet.add(oldTuple)

class UpdateAction(ReplaceAction):

    def _handleTrove(self, beforeInfo, afterInfo, oldTuple, newTuple,
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


class OperationGraph(graph.DirectedGraph):

    def realize(self, data):
        # this is a hack
        self.actionData = data

        transpose = self.transpose()
        ordering = self.getTotalOrdering()

        while True:
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
                        node.realize(data)

class MissingTroves(Exception):

    def __init__(self, specList):
        self.specList = specList

    def __str__(self):
        l = [ "Cannot find matches for " ]
        for spec in self.specList:
            item = []
            item.append(spec[0])
            if spec[1]:
                item.append("=")
                item.append(spec[1])

            if spec[2]:
                item.append("[")
                item.append(spec[2])
                item.append("]")

            l.append("".join(item))

        return " ".join(l)

