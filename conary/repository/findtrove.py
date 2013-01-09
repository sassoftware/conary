#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import itertools

from conary.deps import deps
from conary import errors
from conary import versions

class QuerySet(object):
    """
        A representation of the translation from (name, versionStr, flavor)
        to a set of queries that can be made to the repository.

        The set of queries may be ordered, as when an installLabelPath
        is checked, or they may be unordered as when using label affinity
        to search for updates to two packages at once.

        The query has several components.

        troveSpec: The original (n,v,f) query
        success: True if this query is successful
        results: List of results returned by this query (unordered)
    """

    __slots__ = ['troveSpec', 'success', 'results',
                 '_alternatesByQueue', '_activeByQueue',
                 '_searchIdx', '_searchQueues',
                 'searchKey']

    useFilter = False

    def __init__(self, troveSpec, searchKey):
        self.troveSpec = troveSpec
        self.searchKey = searchKey
        self.success = False

        self.results = []

        # Note: the reason why we have multiple searchQueues associated
        # with one troveSpec is affinity.  The reason why its easier
        # to have these multiple queues under one object is that there is
        # always one error message per troveSpec, even if there are
        # multiple searches due to affinity.

        self._searchQueues = [] # list of lists of (n,v,f) queries to be
                                # sent to the repository.
        self._searchIdx = 0

        # if a query is unsuccessful but the query
        # returned alternate flavor queries that would have matched,
        # this is that list of alternate flavor queries.
        self._alternatesByQueue = []

        # whether to continue searching for this particular
        # query (queries stop once a result has been found)
        self._activeByQueue = []

    def getFilter(self):
        return None

    def __repr__(self):
        return '%s(%r, %r, %r)' % (self.__class__.__name__,
                                   self.troveSpec[0],
                                   self.troveSpec[1], self.troveSpec[2])


    def markAsSuccess(self):
        self.success = True


    def addSearchQueue(self, queue):
        """
            Add a set of searches to do, in order, for this QuerySet.

            The queue should be a list of lists to search, e.g.:
            [[(n,v,f), (n,v,f')], [(n,v',f), (n,v',f'')]]

            Items in the first list will be searched first and a result
            for any of those queries will stop the queue.  If no results
            from the first list return the second list will be searched,
            and so on.
        """
        self._searchQueues.append(queue)
        self._alternatesByQueue.append([])
        self._activeByQueue.append(True)

    def nextSearchList(self):
        """
            Return the next set of queries to pass to the repository
            (max one per queue).

            The returned result is of the form:
            [(queueIdx, list of queries for this idx), (queueIdx', ...)]
        """
        active = False
        searches = []
        for queueIdx, searchQueue in enumerate(self._searchQueues):
            if not self._activeByQueue[queueIdx]:
                continue
            if len(searchQueue) <= self._searchIdx:
                # this queue is now empty
                self._activeByQueue[queueIdx] = False
                continue
            searches.append((queueIdx, searchQueue[self._searchIdx]))
        if not searches:
            # we have done all the querying that is possible for this
            # QuerySet.
            return False
        self._searchIdx += 1
        return searches

    def foundResults(self, queueIdx, resultList):
        self.success = True
        self.results.extend(resultList)
        self._alternatesByQueue[queueIdx] = []
        self._activeByQueue[queueIdx] = False

    def noResultsFound(self, queueIdx, alternateList):
        self._alternatesByQueue[queueIdx].extend(alternateList)

    def iterAlternatesByQueue(self):
        """
            Return alternates and the searches that resulted in those
            alternates being suggested.
        """
        return itertools.izip(self._searchQueues, self._alternatesByQueue)


class QuerySetFactory(object):
    """
        Generates a QuerySet with the appropriate searchQueues
        given a troveSpec with a particular searchKey and flavorList.

        The subclass of QuerySet created is determined by the QuerySetFactory
        subclass.
    """

    def create(self, troveSpec, searchKey, affinityTroves, queryOptions):
        """
            Returns a query for this searchKey
        """
        query = self.queryClass(troveSpec, searchKey)
        if affinityTroves:
            affinityTroves =  self.filterAffinityTroves(affinityTroves,
                                                        searchKey)
        if not affinityTroves:
            self.createQueryNoAffinity(query, searchKey, queryOptions)
        else:
            self.createQueryWithAffinity(query, searchKey, affinityTroves,
                                         queryOptions)
        return query

    def filterAffinityTroves(self, affinityTroves, searchKey):
        return affinityTroves

    def createQueryNoAffinity(self, query, searchKey, queryOptions):
        acrossLabels = queryOptions.searchAcrossLabels
        acrossFlavors = queryOptions.searchAcrossFlavors
        if isinstance(searchKey, set):
            acrossLabels = True
        flavorList = self.mergeFlavors(queryOptions, query.troveSpec[2])
        name = query.troveSpec[0]
        queue = self.createSearchQueue(name, searchKey, flavorList,
                                       acrossFlavors=acrossFlavors,
                                       acrossItems=acrossLabels)
        query.addSearchQueue(queue)
        return query

    def overrideFlavors(self, queryOptions, flavor, defaultFlavorPath=None):
        """
            override the flavors in the defaultFlavorPath with flavor,
            replacing instruction set entirely if given.
        """
        if defaultFlavorPath is None:
            defaultFlavorPath = queryOptions.defaultFlavorPath
        if flavor is None:
            return defaultFlavorPath
        if not defaultFlavorPath:
            return [flavor]
        flavors = []
        for defaultFlavor in queryOptions.defaultFlavorPath:
            flavors.append(deps.overrideFlavor(defaultFlavor, flavor,
                                        mergeType = deps.DEP_MERGE_TYPE_PREFS))
        return flavors

    def mergeFlavors(self, queryOptions, flavor, defaultFlavorPath=None):
        """
            Merges the given flavor with the flavorPath - if flavor
            doesn't contain use flags, then include the defaultFlavor's
            use flags.  If flavor doesn't contain an instruction set, then
            include the flavorpath's instruction set(s)
        """
        if defaultFlavorPath is None:
            defaultFlavorPath = queryOptions.defaultFlavorPath
        if not defaultFlavorPath:
            return [flavor]
        if flavor is None:
            return defaultFlavorPath
        return [ deps.overrideFlavor(x, flavor) for x in defaultFlavorPath ]

    def createSearchQueue(self, name, searchKey, flavorList,
                          acrossFlavors=False, acrossItems=False):
        """
            Adds a search for a package along a list of query items
            (like labels, branch, etc) that should be queried successively
            and a list of flavors that should be checked for those items.

            At the end of this process a search queue is added to the
            query object.
        """
        if not isinstance(searchKey, (tuple,set,list)):
            searchKey = [ searchKey ]
        if not isinstance(searchKey, list):
            searchKey = list(searchKey)

        if not isinstance(flavorList, (tuple,set,list)):
            flavorList = [ flavorList ]

        if acrossItems and acrossFlavors:
            # do one big flat search across everything
            oneSearch = []
            for flavor in flavorList:
                oneSearch.extend((name, x, flavor) for x in searchKey)
            return [oneSearch]
        elif acrossItems and not acrossFlavors:
            # flat search across items, but search through the flavorList
            # in order
            queue = []
            for flavor in flavorList:
                queue.append([(name, x, flavor) for x in searchKey])
            return queue
        else:
            assert(not acrossItems)
            if acrossFlavors:
                # search through the searchKey in order, but flatten flavorList
                queue = []
                for item in searchKey:
                    queue.append([ (name, item, x) for x in flavorList ])
                return queue
            else:
                # search (item a, flavor 1), (item a, flavor 2),
                #        (item b, flavor 1), (item b, flavor 2))
                queue = []
                for flavor in flavorList:
                    for item in searchKey:
                        queue.append([(name, item, flavor)])
                return queue


class QueryOptions(object):
    def __init__(self, troveSource, labelPath, defaultFlavorPath, acrossLabels,
                 acrossFlavors, affinityDatabase, getLeaves=True,
                 bestFlavor=True, allowNoLabel=False, troveTypes=None,
                 exactFlavors=False, requireLatest=False, allowMissing=False):
        self.troveSource = troveSource
        if isinstance(labelPath, versions.Label):
            labelPath = [labelPath]
        self.labelPath = labelPath
        if defaultFlavorPath is not None and not isinstance(defaultFlavorPath,
                                                            list):
            defaultFlavorPath = [defaultFlavorPath]
        self.defaultFlavorPath = defaultFlavorPath
        self.searchAcrossLabels = acrossLabels
        if not labelPath:
            # if you don't pass in a label, we may have to generate a label
            # list.  In that case, we should always return all results.
            acrossLabels = True
        if exactFlavors:
            bestFlavor = False
            defaultFlavorPath = None
        self.labelPath = labelPath
        self.getLeaves = getLeaves
        self.bestFlavor = bestFlavor
        self.allowMissing = allowMissing
        self.searchAcrossFlavors = acrossFlavors
        self.allowNoLabel = allowNoLabel
        self.exactFlavors = exactFlavors
        if troveTypes is None:
            from conary.repository import netclient
            troveTypes = netclient.TROVE_QUERY_PRESENT
        self.troveTypes = troveTypes
        self.affinityDatabase = affinityDatabase
        self.requireLatest = requireLatest

class TroveFinder(object):
    def __init__(self, *args, **kw):
        self.setQueryOptions(*args, **kw)
        self.queries = []

    def setQueryOptions(self, troveSource, labelPath, defaultFlavorPath,
                        acrossLabels, acrossFlavors, affinityDatabase,
                        getLeaves=True, bestFlavor=True, allowNoLabel=False,
                        troveTypes=None, exactFlavors=False,
                        requireLatest=False):
        self.queryOptions = QueryOptions(troveSource=troveSource,
                                         labelPath=labelPath,
                                         defaultFlavorPath=defaultFlavorPath,
                                         acrossLabels=acrossLabels,
                                         acrossFlavors=acrossFlavors,
                                         affinityDatabase=affinityDatabase,
                                         getLeaves=getLeaves,
                                         bestFlavor=bestFlavor,
                                         allowNoLabel=allowNoLabel,
                                         troveTypes=troveTypes,
                                         exactFlavors=exactFlavors,
                                         requireLatest=requireLatest)

    def findTroves(self, troveSpecs, allowMissing=False):
        """
            Creates the required query classes and then executes
            the queries, either raising errors.TroveNotFound
            or returning a dict of results.
        """
        queryOptions = self.queryOptions
        queryOptions.allowMissing = allowMissing
        if queryOptions.affinityDatabase:
            affinityTroveDict = queryOptions.affinityDatabase.findTroves(
                                      None,
                                      [(x[0], None, None) for x in troveSpecs],
                                      allowMissing=True)
        else:
            affinityTroveDict = {}

        for troveSpec in troveSpecs:
            affinityTroves = affinityTroveDict.get(
                                            (troveSpec[0], None, None), [])
            queryFactory, item = self.getFactory(troveSpec, affinityTroves)
            query = queryFactory().create(troveSpec, item, affinityTroves,
                                          queryOptions)
            self.queries.append(query)
        queriesByClass = {}
        for query in self.queries:
            methodName = query.getQueryFunction(queryOptions)
            key = query.__class__, methodName
            if key not in queriesByClass:
                queriesByClass[key] = []
            queriesByClass[key].append(query)
        for (queryClass, methodName), queryList in queriesByClass.iteritems():
            self._findAllForQueryClass(queryClass,
                                       queryList, methodName, queryOptions)
        results = {}
        missingMsgs = []
        for query in self.queries:
            if not query.success:
                missingMsgs.append(query.missingMsg())
            else:
                results[query.troveSpec] = list(set(query.results))

        if missingMsgs and not allowMissing:
            if len(missingMsgs) > 1:
                raise errors.TroveNotFound, '%d troves not found:\n%s\n' \
                        % (len(missingMsgs), '\n'.join(x for x in missingMsgs))
            else:
                raise errors.TroveNotFound, missingMsgs[0]

        return results

    def _findAllForQueryClass(self, queryClass, queryList, methodName,
                              queryOptions):
        """
            Performs the actual searches for a list of queries that
            all are of the same query class.  The results are stored
            in each individual query.
        """
        method = getattr(queryOptions.troveSource, methodName)
        while True:
            queueIds = []
            searchSpecs = []
            for query in queryList:
                searchList = query.nextSearchList()
                if not searchList:
                    continue
                for queueIdx, querySearchSpecs in searchList:
                    queueIds.extend((query, queueIdx) for x in querySearchSpecs)
                    searchSpecs.extend(querySearchSpecs)
            if not searchSpecs:
                return
            elif queryClass.useFilter or queryOptions.exactFlavors or \
                    queryOptions.requireLatest:
                newSearchSpecs = [ (x[0], x[1], None) for x in searchSpecs ]
                kw = dict(bestFlavor=False, troveTypes=queryOptions.troveTypes)
                results, errorList = method(newSearchSpecs, **kw)
                results, errorList = \
                        self._filterQueryResults( \
                        queueIds, searchSpecs, results, queryOptions)
            else:
                kw = dict(bestFlavor=queryOptions.bestFlavor,
                          troveTypes=queryOptions.troveTypes)
                results, errorList = method(searchSpecs, **kw)

            allInfo = itertools.izip(results, errorList, queueIds)

            for (troveList, errorList, (query, queueIdx)) in allInfo:
                if troveList:
                    query.foundResults(queueIdx, troveList)
                else:
                    query.noResultsFound(queueIdx, errorList)


    def _filterQueryResults(self, queryList, searchSpecs, troveLists,
                            queryOptions):
        """
            Filters results by version string or other filtering
            if necessary before passing the troves back to be filtered
            by flavor.
        """
        newTroveLists = []
        for (query, queueId), troveList in itertools.izip(queryList,
                                                          troveLists):
            filterFn = query.getFilter()
            if filterFn is not None:
                newTroveList = filterFn(troveList)
            else:
                newTroveList = troveList
            if queryOptions.exactFlavors:
                flavorQuery = query.troveSpec[2]
                if flavorQuery is None:
                    flavorQuery = deps.parseFlavor('')
                newTroveList2 = [ x for x in newTroveList
                                  if flavorQuery == x[2] ]
                errorList = [ x[2] for x in newTroveList
                              if x not in newTroveList2 ]
                newTroveList = newTroveList2
            newTroveLists.append(newTroveList)
        if queryOptions.requireLatest:
            savedTroveLists = newTroveLists[:]
        flavorQueries = [ x[2] for x in searchSpecs ]
        bestFlavor = queryOptions.bestFlavor
        getLeaves = queryOptions.getLeaves
        troveTypes = queryOptions.troveTypes
        results, errorList = queryOptions.troveSource.filterByFlavors(
                                           flavorQueries,
                                           newTroveLists,
                                           getLeaves=getLeaves,
                                           bestFlavor=bestFlavor,
                                           troveTypes=troveTypes)
        if queryOptions.requireLatest:
            latestRequired = []
            for origRes, newRes in itertools.izip(savedTroveLists, results):
                # empty newRes is a TroveNotFound. we can ignore it for
                # requireLatest tests
                if newRes and (origRes != newRes):
                    maxVersion = max(x[1] for x in origRes)
                    if maxVersion != max(x[1] for x in newRes):
                        trvName = newRes[0][0]
                        flavors = sorted([x[2] for x in origRes \
                                if x[1] == maxVersion])
                        latestRequired.append((newRes[0], flavors, maxVersion))
            if latestRequired:
                raise errors.LatestRequired(latestRequired)
        return results, errorList

    def getFactory(self, troveSpec, affinityTroves):
        """
        Return a string that describes this troveSpec's versionStr
        The string returned corresponds to a function name for sorting on
        that versionStr type.
        """
        name = troveSpec[0]
        versionStr = troveSpec[1]
        if not versionStr:
            labelPath = self._getLabelPath(troveSpec, self.queryOptions,
                                           affinityTroves, versionStr)
            if (not labelPath and not self.queryOptions.allowNoLabel
                and not self.queryOptions.allowMissing):
                message = ("No search label path given and no label specified"
                           " for trove %s - set the installLabelPath" % name)
                raise errors.LabelPathNeeded(message)

            return QueryByLabelPathSetFactory, labelPath
        if isinstance(versionStr, versions.Version):
            return QueryByVersionSetFactory, versionStr
        elif isinstance(versionStr, versions.Branch):
            return QueryByBranchSetFactory, versionStr

        firstChar = versionStr[0]
        if firstChar == '/':
            try:
                version = versions.VersionFromString(versionStr)
            except errors.ParseError, e:
                raise errors.TroveNotFound, str(e)
            if isinstance(version, versions.Branch):
                return QueryByBranchSetFactory, version
            else:
                return QueryByVersionSetFactory, version

        slashCount = versionStr.count('/')

        if slashCount > 1:
            # if we've got a version string, and it doesn't start with a
            # /, only one / is allowed
            raise errors.TroveNotFound, \
                    "incomplete version string %s not allowed" % versionStr
        labelPath = self._getLabelPath(troveSpec, self.queryOptions,
                                       affinityTroves, versionStr)
        newLabelPath, remainder = self._convertLabelPath(name, labelPath,
                                                         versionStr)
        if remainder:
            return QueryByRevisionByLabelPathSetFactory, newLabelPath
        return QueryByLabelPathSetFactory, newLabelPath

    def _getLabelPath(self, troveSpec, queryOptions, affinityTroves,
                      versionStr):
        """
            Returns the labelPath to use when searching for this troveSpec,
            as determined by the following algorithm:
                - if there are affinityTroves and the troveSpec doesn't specify
                  part of the label to search, return the affinityTrove labels
                - if there's a labelPath specified to findTroves, use that
                - If we cannot allow no label path to be passed in, error.
                - Otherwise, return every label in the source that exists.
        """
        if affinityTroves and (not versionStr or (':' not in versionStr
                                                  and '@' not in versionStr)):
            return [ x[1].trailingLabel() for x in affinityTroves ]
        if queryOptions.labelPath:
            return queryOptions.labelPath
        if not queryOptions.allowNoLabel:
            return []
        return set([ x.trailingLabel() \
                    for x in queryOptions.troveSource.getTroveVersionList(
                                                troveSpec[0],
                                    troveTypes=queryOptions.troveTypes)])


    def _convertLabelPath(self, name, labelPath, versionStr):
        """
            Given a label path and a versionString that modifies it,
            return the modified labelPath.
        """
        newLabelPath = []
        if '/' in versionStr:
            labelPart, remainder = versionStr.split('/', 1)
        else:
            labelPart, remainder = versionStr, ''
        firstChar = labelPart[0]
        repoInfo = [(x.getHost(), x.getNamespace(), x.getLabel())
                     for x in labelPath ]
        if firstChar == ':':
            for serverName, namespace, tag in repoInfo:
                newLabelPath.append(versions.Label("%s@%s%s" %
                                   (serverName, namespace, labelPart)))
        elif firstChar == '@':
            for serverName, namespace, tag in repoInfo:
                newLabelPath.append(versions.Label("%s%s" %
                                                   (serverName, labelPart)))
        elif labelPart[-1]  == '@':
            for serverName, namespace, tag in repoInfo:
                newLabelPath.append(versions.Label("%s%s:%s" %
                                               (labelPart, namespace, tag)))
        elif '@' in labelPart:
            try:
                label = versions.Label(labelPart)
                newLabelPath = [ label ]
            except errors.ParseError:
                raise errors.TroveNotFound, \
                                    "invalid version %s" % versionStr
        else:
            # no labelPath parts in versionStr, use the old one.
            newLabelPath = labelPath
            remainder = versionStr
            if not newLabelPath and not self.queryOptions.allowNoLabel and not self.queryOptions.allowMissing:
                raise errors.LabelPathNeeded("No search label path given and no label specified for trove %s=%s - set the installLabelPath" % (name, versionStr))
        if not newLabelPath and not self.queryOptions.allowNoLabel and not self.queryOptions.allowMissing:
            raise errors.LabelPathNeeded("No search label path given and partial label specified for trove %s=%s - set the installLabelPath" % (name, versionStr))
        if isinstance(labelPath, set):
            newLabelPath = set(newLabelPath)
        if '-' in remainder:
            # attempt to parse the versionStr
            try:
                versions.Revision(remainder)
            except errors.ParseError, err:
                raise errors.TroveNotFound(str(err))
        return newLabelPath, remainder



class QueryByLabelPathSet(QuerySet):
    """
        Set of queries by searchKey.
    """

    def getQueryFunction(self, queryOptions):
        if queryOptions.getLeaves:
            return 'getTroveLatestByLabel'
        else:
            return 'getTroveVersionsByLabel'

    def missingMsg(self):
        name = self.troveSpec[0]
        if self.searchKey:
            msg =  "%s was not found on path %s" \
                    % (name, ', '.join(x.asString() for x in self.searchKey))
        else:
            msg = "%s was not found" % name
        return msg + getAlternateFlavorMessage(self)

class QueryByRevisionByLabelPathSet(QueryByLabelPathSet):

    useFilter = True

    def getQueryFunction(self, queryOptions):
        return 'getTroveVersionsByLabel'

    def _filterByRevision(self, troveList):
        versionStr = self.troveSpec[1].rsplit('/')[-1]
        revision = versions.Revision(versionStr)
        return [ x for x in troveList
                 if x[1].trailingRevision() == revision ]

    def _filterByVersionString(self, troveList):
        versionStr = self.troveSpec[1].rsplit('/')[-1]
        return [ x for x in troveList if
                 x[1].trailingRevision().version == versionStr ]

    def getFilter(self):
        versionStr = self.troveSpec[1].rsplit('/')[-1]
        try:
            verRel = versions.Revision(versionStr)
        except errors.ParseError:
            verRel = None
        if verRel:
            return self._filterByRevision
        else:
            return self._filterByVersionString

    def missingMsg(self):
        revision = self.troveSpec[1].rsplit('/', 1)[-1]
        name = self.troveSpec[0]
        if self.searchKey:
            msg =  "revision %s of %s was not found on label(s) %s" \
                    % (revision, name, ', '.join(x.asString() for x in self.searchKey))
        else:
            msg =  "revision %s of %s was not found" % (revision, name)
        return msg + getAlternateFlavorMessage(self)

class QueryByVersionSet(QuerySet):

    def getQueryFunction(self, queryOptions):
        return 'getTroveVersionFlavors'

    def missingMsg(self):
        name = self.troveSpec[0]
        return "version %s of %s was not found%s" % (self.searchKey, name,
                                             getAlternateFlavorMessage(self))



class QueryByBranchSet(QuerySet):

    def getQueryFunction(self, queryOptions):
        if queryOptions.getLeaves:
            return 'getTroveLeavesByBranch'
        else:
            return 'getTroveVersionsByBranch'

    def missingMsg(self):
        name = self.troveSpec[0]
        return "%s was not found on branch %s%s" \
                % (name, self.searchKey, getAlternateFlavorMessage(self))


class QueryByLabelPathSetFactory(QuerySetFactory):

    queryClass = QueryByLabelPathSet

    def filterAffinityTroves(self, affinityTroves, labelPath):
        return [ x for x in affinityTroves
                 if x[1].trailingLabel() in labelPath ]

    def createQueryWithAffinity(self, query, searchKey,
                                affinityTroves, queryOptions):
        acrossFlavors = queryOptions.searchAcrossFlavors

        baseFlavor = query.troveSpec[2]
        name = query.troveSpec[0]
        for affTrove in affinityTroves:
            if affTrove[1].isOnLocalHost():
                query.markAsSuccess()
                continue
            label = affTrove[1].trailingLabel()
            flavorList = self.overrideFlavors(queryOptions, affTrove[2])
            flavorList = self.mergeFlavors(queryOptions,
                                           baseFlavor, flavorList)
            queue = self.createSearchQueue(name, label, flavorList,
                                           acrossFlavors=acrossFlavors)
            query.addSearchQueue(queue)

class QueryByRevisionByLabelPathSetFactory(QueryByLabelPathSetFactory):
    queryClass = QueryByRevisionByLabelPathSet


class QueryByItemSetFactory(QuerySetFactory):
    queryClass = None

    def createQueryWithAffinity(self, query, searchKey, affinityTroves,
                                queryOptions):
        # since the exact branch to search was specified we can't
        # use that information but we can use the flavors that are installed.
        # e.g. if there's an x86 + x86_64 version we should try to
        # update both.
        name = query.troveSpec[0]
        baseFlavor = query.troveSpec[2]
        allFlavors = set(x[2] for x in affinityTroves)
        for flavor in allFlavors:
            flavorList = self.overrideFlavors(queryOptions, flavor)
            flavorList = self.mergeFlavors(queryOptions,
                                           baseFlavor,
                                           flavorList)
            queue = self.createSearchQueue(name, searchKey, flavorList,
                                           queryOptions.searchAcrossFlavors)
            query.addSearchQueue(queue)

class QueryByVersionSetFactory(QueryByItemSetFactory):
    queryClass = QueryByVersionSet

class QueryByBranchSetFactory(QueryByItemSetFactory):
    queryClass = QueryByBranchSet

def _getFlavorLength(flavor):
    total = 0
    for depClass, dep in flavor.iterDeps():
        total += 1 + len(dep.getFlags()[0])
    return total


def getAlternateFlavorMessage(query):
    """
        Returns a message that describes any available alternate flavors
        that could be passed in to return a query result.
    """

    minimalMatches = []
    archMinimalMatches = []
    archPartialMatches = []
    for searchQueue, alternateList in query.iterAlternatesByQueue():
        if not alternateList:
            continue

        flavors = set([ x[2] for x in itertools.chain(*searchQueue) ])
        for flavor in flavors:
            if flavor is None:
                flavor = deps.Flavor()
            ISD = deps.InstructionSetDependency
            archNames = set(x.name for x in flavor.iterDepsByClass(ISD))
            for toMatchFlavor in alternateList:
                minimalMatch = deps.getMinimalCompatibleChanges(flavor,
                                                              toMatchFlavor,
                                                              keepArch=True)
                minimalMatches.append(minimalMatch)
                matchArchNames = set(x.name for x
                                   in toMatchFlavor.iterDepsByClass(ISD))
                if not matchArchNames - archNames:
                    # if we don't have to change architectures to match
                    # this flavor, that's much better than the alternative.
                    archMinimalMatches.append(minimalMatch)
                if matchArchNames & archNames:
                    archPartialMatches.append(minimalMatch)
    if archMinimalMatches:
        minimalMatches = archMinimalMatches
    elif archPartialMatches:
        minimalMatches = archPartialMatches
    if minimalMatches:
        minimalMatches = set(minimalMatches)
        matchesByLength = sorted(((_getFlavorLength(x), x)
                                   for x in minimalMatches),
                                  key = lambda x: x[0])
        shortestMatchLength = matchesByLength[0][0]
        # only return the flavors that require the least amount of change
        # to our current query.
        minimalMatches = [ x[1] for x in matchesByLength
                           if x[0] == shortestMatchLength ]
        flavorStrs = '], ['.join([str(x) for x in sorted(minimalMatches)])
        return ' (Closest alternate flavors found: [%s])' % flavorStrs
    return ''
