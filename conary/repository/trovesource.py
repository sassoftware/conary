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

from conary import errors as conaryerrors
from conary import files
from conary import trove
from conary import trovetup
from conary.deps import arch, deps
from conary.local import deptable
from conary.repository import changeset, errors, findtrove
from conary.lib import api

TROVE_QUERY_ALL = 0                 # normal, removed, redirect
TROVE_QUERY_PRESENT = 1             # normal, redirect (and repositories < 1.1)
TROVE_QUERY_NORMAL = 2              # hide branches which end in redirects

class AbstractTroveSource:
    """ Provides the interface necessary for performing
        findTrove operations on arbitrary sets of troves.
        As long as the subclass provides the following methods,
        findTrove will be able to search it.  You can set the
        type of searching findTrove will default to here as
        well.
    """


    def __init__(self, searchableByType=False):
        self._allowNoLabel = True
        self._bestFlavor = False
        self._getLeavesOnly = False
        self._searchableByType = searchableByType
        self._flavorPreferences = []
        self.TROVE_QUERY_ALL = TROVE_QUERY_ALL
        self.TROVE_QUERY_PRESENT = TROVE_QUERY_PRESENT
        self.TROVE_QUERY_NORMAL = TROVE_QUERY_NORMAL

    def setFlavorPreferenceList(self, preferenceList):
        if not preferenceList:
            preferenceList = []
        self._flavorPreferences = preferenceList

    def setFlavorPreferencesByFlavor(self, flavor):
        self.setFlavorPreferenceList(
                                arch.getFlavorPreferencesFromFlavor(flavor))

    def getFlavorPreferenceList(self):
        return self._flavorPreferences

    def requiresLabelPath(self):
        return not self._allowNoLabel

    def getFileVersion(self, pathId, fileId, version):
        return self.getFileVersions([(pathId, fileId, version)])[0]

    def getFileVersions(self, fileIds):
        raise NotImplementedError

    def searchableByType(self):
        return self._searchableByType

    def getTroveInfo(self, infoType, troveTupleList):
        raise NotImplementedError

    def getTroveLeavesByLabel(self, query, bestFlavor=True,
                              troveTypes=TROVE_QUERY_PRESENT):
        raise NotImplementedError

    def getTroveLatestByLabel(self, query, bestFlavor=True,
                              troveTypes=TROVE_QUERY_PRESENT):
        raise NotImplementedError

    def getTroveVersionsByLabel(self, query, bestFlavor=True,
                                troveTyes=TROVE_QUERY_PRESENT):
        raise NotImplementedError

    def getTroveLeavesByBranch(self, query, bestFlavor=True,
                               troveTypes=TROVE_QUERY_PRESENT):
        raise NotImplementedError

    def getTroveVersionsByBranch(self, query, bestFlavor=True,
                                 troveTypes=TROVE_QUERY_PRESENT):
        raise NotImplementedError

    def getTroveVersionFlavors(self, query, bestFlavor=True,
                                 troveTypes=TROVE_QUERY_PRESENT):
        raise NotImplementedError

    def getTroves(self, troveList, withFiles = True):
        raise NotImplementedError

    def resolveDependencies(self, label, depList, leavesOnly=False):
        results = {}
        for depSet in depList:
            results[depSet] = [ [] for x in depSet.iterDeps() ]
        return results

    def resolveDependenciesByGroups(self, troveList, depList):
        results = {}
        for depSet in depList:
            results[depSet] = [ [] for x in depSet.iterDeps() ]
        return results

    def hasTroves(self, troveList):
        raise NotImplementedError

    def hasTrove(self, name, version, flavor):
        return self.hasTroves(((name, version, flavor),))[0]

    def getTrove(self, name, version, flavor, withFiles = True):
        trv = self.getTroves([(name, version, flavor)], withFiles)[0]
        if trv is None:
            raise errors.TroveMissing(name, version)
        else:
            return trv

    def getTroveVersionList(self, name, withFlavors=False,
                            troveTypes=TROVE_QUERY_PRESENT):
        raise NotImplementedError

    def getLabelsForTroveName(self, name, troveTypes=TROVE_QUERY_PRESENT):
        """
            Gets a set of labels on which a particular package is available
        """
        versionList = self.getTroveVersionList(name, troveTypes=troveTypes)
        return set(x.trailingLabel() for x in versionList)

    def findTroves(self, labelPath, troves, defaultFlavor=None,
                   acrossLabels=False, acrossFlavors=False,
                   affinityDatabase=None, allowMissing=False,
                   bestFlavor=None, getLeaves=None,
                   troveTypes=TROVE_QUERY_PRESENT, exactFlavors=False,
                   **kw):
        """
        @raises conary.errors.TroveNotFound:
        @raises conary.errors.LabelPathNeeded: raised if installLabelPath is
        not set and no label is specified in a trove's version string
        """

        if bestFlavor is None:
            bestFlavor = self._bestFlavor
        if getLeaves is None:
            getLeaves = self._getLeavesOnly
        troveFinder = findtrove.TroveFinder(self, labelPath,
                                            defaultFlavor, acrossLabels,
                                            acrossFlavors, affinityDatabase,
                                            allowNoLabel=self._allowNoLabel,
                                            bestFlavor=bestFlavor,
                                            getLeaves=getLeaves,
                                            exactFlavors=exactFlavors,
                                            troveTypes=troveTypes,
                                            **kw)
        return troveFinder.findTroves(troves, allowMissing)

    def findTrove(self, labelPath, (name, versionStr, flavor),
                  defaultFlavor=None, acrossLabels = False,
                  acrossFlavors = False, affinityDatabase = None,
                  bestFlavor = None, getLeaves = None,
                  troveTypes=TROVE_QUERY_PRESENT, **kw):
        """
        See L{repository.trovesource.AbstractTroveSource.findTroves}
        """
        res = self.findTroves(labelPath, ((name, versionStr, flavor),),
                              defaultFlavor, acrossLabels, acrossFlavors,
                              affinityDatabase, bestFlavor=bestFlavor,
                              getLeaves=getLeaves, troveTypes=troveTypes,
                              **kw)
        return res[(name, versionStr, flavor)]

    def iterFilesInTrove(self, n, v, f, sortByPath=False, withFiles=False):
        raise NotImplementedError

    def walkTroveSet(self, topTrove, ignoreMissing = True,
                     withFiles=True, asTuple=True):
        """
        Generator returns all of the troves included by topTrove, including
        topTrove itself. It is a depth first search of strong refs. Punchouts
        are taken into account.

        @param asTuple: If True, (name, version, flavor) tuples are returned
        instead of Trove objects. This can be much faster.
        """
        def _collect(l, tup):
            if tup[1] is None:
                if trove.troveIsComponent(tup[0][0]):
                    # don't bother looking for children of components
                    tup[1] = []
                else:
                    l.append(tup)
            else:
                for t in tup[1]:
                    _collect(l, t)

        if asTuple and hasattr(self, 'getTroveTroves'):
            assert(not withFiles)
            seen = set()
            all = [ topTrove.getNameVersionFlavor(), None ]
            seen.add(topTrove.getNameVersionFlavor())
            while True:
                getList = []
                _collect(getList, all)
                if not getList:
                    break

                refs = self.getTroveTroves([ x[0] for x in getList],
                                               justPresent = True)

                for item, refList in itertools.izip(getList, refs):
                    item[1] = []
                    for x in refList:
                        if x not in seen:
                            seen.add(x)
                            item[1].append([x, None])

            stack = [ all ]
            while stack:
                next = stack.pop()
                yield next[0]
                stack += next[1]

            return

        def _format(trv):
            if asTuple:
                return trv.getNameVersionFlavor()
            else:
                return trv
        yield _format(topTrove)
        seen = { topTrove.getName() : [ (topTrove.getVersion(),
                                         topTrove.getFlavor()) ] }

        troveList = [x for x in sorted(topTrove.iterTroveList(strongRefs=True))]

        while troveList:
            (name, version, flavor) = troveList[0]
            del troveList[0]

            if seen.has_key(name):
                match = False
                for (ver, fla) in seen[name]:
                    if version == ver and fla == flavor:
                        match = True
                        break
                if match: continue

                seen[name].append((version, flavor))
            else:
                seen[name] = [ (version, flavor) ]

            try:
                trv = self.getTrove(name, version, flavor, withFiles=withFiles)

                yield _format(trv)

                troveList = ([ x for x in
                                sorted(trv.iterTroveList(strongRefs=True)) ]
                              + troveList)
            except errors.TroveMissing:
                if not ignoreMissing:
                    raise
            except KeyError:
                if not ignoreMissing:
                    raise


    def mergeDepSuggestions(self, allSuggs, newSugg):
        """
            Given two suggestion lists, merge them so that
            all the suggestions are together.
        """
        for depSet, trovesByDepList in newSugg.iteritems():
            if depSet not in allSuggs:
                lst = [ [] for x in trovesByDepList ]
                allSuggs[depSet] = lst
            else:
                lst = allSuggs[depSet]

            for i, troveList in enumerate(trovesByDepList):
                lst[i].extend(troveList)

    def getPathHashesForTroveList(self, troveList):
        raise NotImplementedError

    def getDepsForTroveList(self, troveList, provides = True, requires = True):
        # provides/requires are hints only; implementations may return
        # both if preferred
        raise NotImplementedError

# constants mostly stolen from netrepos/netserver
_GET_TROVE_ALL_VERSIONS = 1
_GET_TROVE_VERY_LATEST  = 2         # latest of any flavor

#_GET_TROVE_NO_FLAVOR          = 1     # no flavor info is returned
_GET_TROVE_ALL_FLAVORS        = 2     # all flavors (no scoring)
_GET_TROVE_BEST_FLAVOR        = 3     # the best flavor for flavorFilter
_GET_TROVE_ALLOWED_FLAVOR     = 4     # all flavors which are legal

_CHECK_TROVE_REG_FLAVOR    = 1          # use exact flavor and ensure trove
                                        # flavor is satisfied by query flavor
_CHECK_TROVE_STRONG_FLAVOR = 2          # use strong flavors and reverse sense

_GTL_VERSION_TYPE_NONE    = 0
_GTL_VERSION_TYPE_LABEL   = 1
_GTL_VERSION_TYPE_VERSION = 2
_GTL_VERSION_TYPE_BRANCH  = 3
_GTL_VERSION_TYPE_LABEL_LATEST = 4

class SearchableTroveSource(AbstractTroveSource):
    """ A simple implementation of most of the methods needed
        for findTrove - all of the methods are implemplemented
        in terms of trovesByName, which is left for subclasses to
        implement.
    """

    def __init__(self, *args, **kwargs):
        self.searchAsDatabase()
        AbstractTroveSource.__init__(self, *args, **kwargs)

    def trovesByName(self, name):
        raise NotImplementedError

    def iterAllTroveNames(self):
        raise NotImplementedError

    def getTroves(self, troveList, withFiles = True, callback = None):
        raise NotImplementedError

    def createChangeSet(self, jobList, withFiles = True, recurse = False,
                        withFileContents = False, callback = None):
        # return changeset, and unhandled jobs
        cs = changeset.ReadOnlyChangeSet()
        return cs, jobList

    def searchWithFlavor(self):
        self._bestFlavor = True
        self._flavorCheck = _CHECK_TROVE_REG_FLAVOR

    def searchLeavesOnly(self):
        self._getLeavesOnly = True

    def searchAsRepository(self):
        self._allowNoLabel = False
        self._bestFlavor = True
        self._getLeavesOnly = True
        self._flavorCheck = _CHECK_TROVE_REG_FLAVOR

    def searchAsDatabase(self):
        self._allowNoLabel = True
        self._bestFlavor = False
        self._getLeavesOnly = False
        self._flavorCheck = _CHECK_TROVE_STRONG_FLAVOR

    def isSearchAsDatabase(self):
        return self._allowNoLabel

    def getTroveVersionList(self, name, withFlavors=False,
                            troveTypes=TROVE_QUERY_PRESENT):
        if withFlavors:
            return [ x[1:] for x in self.trovesByName(name) ]
        else:
            return [ x[1] for x in self.trovesByName(name) ]

    def _toQueryDict(self, troveList):
        d = {}
        for (n,v,f) in troveList:
            d.setdefault(n, {}).setdefault(v, []).append(f)
        return d

    def _getFilterOptions(self, getLeaves, bestFlavor, troveTypes,
                         splitByBranch=False):
        if getLeaves:
            latestFilter = _GET_TROVE_VERY_LATEST
        else:
            latestFilter = _GET_TROVE_ALL_VERSIONS

        if bestFlavor:
            flavorFilter = _GET_TROVE_BEST_FLAVOR
        else:
            flavorFilter = _GET_TROVE_ALL_FLAVORS
        flavorCheck = self._flavorCheck
        return FilterOptions(latestFilter, flavorFilter, flavorCheck,
                             troveTypes, splitByBranch=splitByBranch)


    def _getTrovesByType(self, troveSpecs,
                         versionType=_GTL_VERSION_TYPE_NONE,
                         latestFilter=_GET_TROVE_ALL_VERSIONS,
                         bestFlavor=False, troveTypes=TROVE_QUERY_PRESENT):
        if isinstance(troveSpecs, dict):
            troveSpecList = []
            for name, versionDict in troveSpecs.iteritems():
                for version, flavorList in versionDict.iteritems():
                    if flavorList is None or flavorList is '':
                        troveSpecList.append((name, version, flavorList))
                    else:
                        troveSpecList.extend((name, version, x) for x in
                                             flavorList)
        else:
            troveSpecList = troveSpecs
        results, altFlavors = self._getTrovesByTypeTuples(
                                                 troveSpecList,
                                                 versionType=versionType,
                                                 latestFilter=latestFilter,
                                                 bestFlavor=bestFlavor,
                                                 troveTypes=troveTypes)
        if not isinstance(troveSpecs, dict):
            return results, altFlavors

        resultDict = {}
        for troveList in results:
            for (name, version, flavor) in troveList:
                if name not in resultDict:
                    vDict = resultDict[name] = {}
                else:
                    vDict = resultDict[name]
                if version not in vDict:
                    fList = vDict[version] = []
                else:
                    fList = vDict[version]
                fList.append(flavor)
        for name, versionDict in resultDict.iteritems():
            for version in versionDict:
                versionDict[version] = list(set(versionDict[version]))
        return resultDict

    def _getTrovesByTypeTuples(self, troveSpecs,
                         versionType=_GTL_VERSION_TYPE_NONE,
                         latestFilter=_GET_TROVE_ALL_VERSIONS,
                         bestFlavor=False, troveTypes=TROVE_QUERY_PRESENT):
        """ Implements the various getTrove methods by grabbing
            information from trovesByName.  Note it takes an
            extra parameter over the netrepos/netserver version -
            flavorCheck - which, if set to _GET_TROVE_STRONG_FLAVOR,
            does a strong comparison against the listed troves -
            the specified flavor _must_ exist in potentially matching
            troves, and sense reversal is not allowed - e.g.
            ~!foo is not an acceptable match for a flavor request of
            ~foo.  The mode is useful when the troveSpec is specified
            by the user and is matching against a limited set of troves.
        """
        # some cases explained:
        # if latestFilter ==  _GET_TROVE_ALL_VERSIONS and
        # flavorFilter == _GET_TROVE_BEST_FLAVOR,
        # for each version, return the best flavor for that version.

        # if latestFilter == _GET_TROVE_VERY_LATEST and flavorFilter
        # == _GET_TROVE_BEST_FLAVOR
        # get the latest version that has an allowed flavor, then get
        # only the best one of the flavors at that latest version

        # we don't handle queries for non-present troves at this moment.
        # doing so may be impossible and/or expensive - for example, we would
        # have to instantiate matching troves from changesets before we could
        # filter by flavor.

        # That said, we just ignore the troveType and assume that
        # the trove source only has the correct types in it.
        #assert(troveTypes == TROVE_QUERY_PRESENT)

        if bestFlavor:
            flavorFilter = _GET_TROVE_BEST_FLAVOR
        else:
            flavorFilter = _GET_TROVE_ALL_FLAVORS
        finalResults = [ [] for x in troveSpecs ]
        finalAltFlavors = [ [] for x in troveSpecs ]

        splitByBranch = (versionType == _GTL_VERSION_TYPE_LABEL)
        filterOptions = FilterOptions(latestFilter, flavorFilter,
                                      self._flavorCheck, troveTypes,
                                      splitByBranch=splitByBranch)

        scoreCache = {}

        troveSpecDict = {}
        for idx, (name, versionSpec, flavorSpec) in enumerate(troveSpecs):
            vDict = troveSpecDict.setdefault(name, {})
            fDict = vDict.setdefault(versionSpec, {})
            fDict.setdefault(flavorSpec, []).append(idx)

        for name, versionQuery in troveSpecDict.iteritems():
            troves = self._toQueryDict(self.trovesByName(name))
            if not troves:
                continue
            if not versionQuery:
                allTroves.update(troves)
                continue
            for resultName in troves:
                versionResults = self._filterByVersionQuery(versionType,
                                                            troves[resultName].keys(),
                                                            versionQuery)

                for queryKey, versionList in versionResults.iteritems():

                    flavorQueryList = versionQuery[queryKey]
                    versionFlavorDict = dict((x, troves[resultName][x])
                                              for x in versionList)
                    for flavorQuery, idxList in flavorQueryList.iteritems():
                        results, altFlavors = self._filterResultsByFlavor(
                                                    versionFlavorDict,
                                                    flavorQuery, filterOptions,
                                                    scoreCache)
                        for idx in idxList:
                            if results:
                                finalAltFlavors[idx] = []
                            else:
                                finalAltFlavors[idx] = altFlavors
                            for idx in troveSpecDict[name][queryKey][flavorQuery]:
                                for version, flavorList in results.iteritems():
                                    finalResults[idx].extend((resultName, version, x)
                                                             for x in flavorList)

        return finalResults, finalAltFlavors

    def filterByFlavors(self, flavorQueryList, troveLists,
                        getLeaves, bestFlavor,
                        troveTypes = TROVE_QUERY_PRESENT):
        filterOptions = self._getFilterOptions(getLeaves=getLeaves,
                                               bestFlavor=bestFlavor,
                                               troveTypes=troveTypes)
        scoreCache = {}
        newResults, newAltFlavors = [], []
        for flavorQuery, troveList in itertools.izip(flavorQueryList,
                                                     troveLists):
            results, altFlavors = self.filterByFlavor(flavorQuery, troveList,
                                                  filterOptions, scoreCache)
            newResults.append(results)
            newAltFlavors.append(altFlavors)
        return newResults, newAltFlavors

    def filterByFlavor(self, flavorQuery, troveList, filterOptions,
                       scoreCache=None):
        versionFlavorDict = {}
        for name, version, flavor in troveList:
            if version not in versionFlavorDict:
                versionFlavorDict[version] = {}
            vDict = versionFlavorDict[version]
            if flavor not in vDict:
                vDict[flavor] = []
            vDict[flavor].append(name)
        results, altFlavors = self._filterResultsByFlavor(versionFlavorDict,
                                                          flavorQuery,
                                                          filterOptions,
                                                          scoreCache)

        resultsList = []
        for version, flavorList in results.iteritems():
            for flavor in flavorList:
                for name in versionFlavorDict[version][flavor]:
                    resultsList.append(trovetup.TroveTuple(name, version, flavor))
        return resultsList, altFlavors

    def _filterResultsByFlavor(self, versionFlavorDict,
                               flavorQuery, filterOptions,
                               scoreCache):
        """
            Updated results with a subset of the contents of versionFlavorDict
            if any of them pass the flavorQueries.

            @param versionFlavorDict: dict of available version flavor pairs
            @type versionFlavorDict: {version -> [flavor]}
            @param flavorQuery: requested flavors that this query must
            match.
            @type flavorQuery: list of flavors
            @param filterOptions: how to limit matching results for return
            against the available troves.
            @type filterOptions: L{FilterOptions}
            @param scoreCache: dict that will hold cached flavor scoring
        """
        if not filterOptions.splitByBranch:
            return self._filterByFlavorQuery(versionFlavorDict,
                                             flavorQuery,
                                             filterOptions,
                                             scoreCache)

        results = {}
        allAltFlavors = []
        for versionFlavorDict in self._splitResultByBranch(versionFlavorDict):
            filteredDict, altFlavors = self._filterByFlavorQuery(
                                                        versionFlavorDict,
                                                        flavorQuery,
                                                        filterOptions,
                                                        scoreCache)
            allAltFlavors.extend(altFlavors)
            if not filteredDict:
                continue
            for version, flavorList in filteredDict.iteritems():
                if version not in results:
                    results[version] = []
                results[version].extend(flavorList)
        if results:
            allAltFlavors = []
        return results, allAltFlavors

    def _splitResultByBranch(self, versionFlavorDict):
        versionsByBranch = {}
        for version in versionFlavorDict:
            versionsByBranch.setdefault(version.branch(),
                                        []).append(version)
        versionFlavorDicts = []
        for versionList in versionsByBranch.values():
            versionFlavorDicts.append(
                    dict((x, versionFlavorDict[x]) for x in
                            versionList))
        return versionFlavorDicts

    def _filterByVersionQuery(self, versionType, versionList, versionQuery):
        versionResults = {}
        for version in versionList:
            if versionType in (_GTL_VERSION_TYPE_LABEL_LATEST,
                               _GTL_VERSION_TYPE_LABEL):
                theLabel = version.trailingLabel()
                if theLabel not in versionQuery:
                    continue
                versionResults.setdefault(theLabel, []).append(version)
            elif versionType == _GTL_VERSION_TYPE_BRANCH:
                theBranch = version.branch()
                if theBranch not in versionQuery:
                    continue
                versionResults.setdefault(theBranch, []).append(version)
            elif versionType == _GTL_VERSION_TYPE_VERSION:
                if version not in versionQuery:
                    continue
                versionResults.setdefault(version, []).append(version)
            else:
                assert(False)
        return versionResults

    def _filterByFlavorQuery(self, versionFlavorDict, flavorQuery,
                             filterOptions, scoreCache):
        """
            @param versionFlavorDict: dict of available version flavor pairs
            @type versionFlavorDict: {version -> [flavor]}
            @param flavorQuery: requested flavors that this query must
            match.
            @type flavorQuery: list of flavors
            @param filterOptions: how to limit matching results for return
            against the available troves.
            @type filterOptions: L{FilterOptions}
            @param scoreCache: dict that will hold cached flavor scoring
        """
        flavorFilter = filterOptions.flavorFilter
        latestFilter = filterOptions.latestFilter
        flavorCheck  = filterOptions.flavorCheck
        if latestFilter == _GET_TROVE_VERY_LATEST:
            versionFlavorList = sorted(versionFlavorDict.iteritems(),
                                       reverse=True)
        else:
            versionFlavorList = list(versionFlavorDict.items())
        if flavorFilter == _GET_TROVE_ALL_FLAVORS and flavorQuery is not None:
            flavorFilter = _GET_TROVE_ALLOWED_FLAVOR

        if flavorFilter == _GET_TROVE_ALL_FLAVORS or flavorQuery is None:
            results = {}
            if latestFilter == _GET_TROVE_VERY_LATEST:
                usedFlavors = set()
                for version, flavorList in versionFlavorList:
                    for flavor in flavorList:
                        if flavor in usedFlavors:
                            continue
                        usedFlavors.add(flavor)
                        results.setdefault(version, set()).add(flavor)
                del usedFlavors
            else:
                for version, flavorList in versionFlavorList:
                    fSet = results.setdefault(version, set())
                    fSet.update(flavorList)
            # no flavor alternatives, we're getting all flavors
            return results, []
        flavorPreferenceList = self._flavorPreferences

        newerEmptyMatches = []
        usedFlavors = set()
        queryResults = {}
        # lower preference score is better, means you've got a
        # flavor that matches a flavor preference earlier in the list.

        if isinstance(flavorQuery, (tuple, list)):
            flavorQuery, primaryFlavorQuery = flavorQuery
        else:
            primaryFlavorQuery = None

        if flavorQuery is not None:
            # if the user specifies something like [is:x86 x86_64]
            # in their query, that should trigger an override of their
            # default flavor preferences to match those in deps/arch.py.
            newPreferenceList = arch.getFlavorPreferencesFromFlavor(
                                                                flavorQuery)
            if newPreferenceList:
                # of course, if they just specified [ssl] then we don't
                # match a preference list and so we don't override
                # the current one.
                flavorPreferenceList = newPreferenceList
        if flavorFilter == _GET_TROVE_ALLOWED_FLAVOR:
            flavorPreferenceList = []

        currentPreferenceScore = len(flavorPreferenceList) + 1

        for version, flavorList in versionFlavorList:
            if primaryFlavorQuery is not None:
                self._calculateFlavorScores(_CHECK_TROVE_STRONG_FLAVOR,
                                            primaryFlavorQuery,
                                            flavorList, scoreCache)
                flavorList = [ x for x in flavorList
                       if scoreCache[primaryFlavorQuery, x] is not False ]
            if not flavorList:
                continue
            self._calculateFlavorScores(flavorCheck, flavorQuery,
                                        flavorList, scoreCache)
            flavorList = [ x for x in flavorList
                           if scoreCache[flavorQuery, x] is not False ]

            if not flavorList:
                continue

            preferenceScore, flavorList, emptyMatches = \
                                            self._filterByPreferences(
                                                    flavorList,
                                                    currentPreferenceScore,
                                                    flavorPreferenceList)
            if preferenceScore is None:
                # didn't find any results better/equal to the current
                # preference score
                continue
            elif (flavorFilter == _GET_TROVE_BEST_FLAVOR
                  and latestFilter == _GET_TROVE_VERY_LATEST
                  and preferenceScore == currentPreferenceScore
                  and queryResults):
                # we match all flavors associated w/ one version
                # at the same time.  So if we're trying to grab the
                # latest viable troves, and we've already got a
                # preference score of 2 for some later version,
                # there's no point finding earlier versions that
                # also have a preference score of 2, since they'll
                # be filtered out by the requirement to have the latest
                # version.
                continue
            elif preferenceScore < currentPreferenceScore:
                currentPreferenceScore = preferenceScore
                # We matched something that was preferred according
                # to the flavorPreference list, for example,
                # if our preferenceList is "is: x86", "ix: x86_64",
                # adn we just matched an older x86_64 trove when we'd
                # already matched a newer x86 trove.  In that case,
                # throw out the x86 trove - it's no longer a valid result.
                queryResults = {}
                usedFlavors = set()
                for emptyVersion, emptyFlavorList in newerEmptyMatches:
                    self._addFilteredFlavorsToResults(emptyVersion,
                                emptyFlavorList, flavorQuery, usedFlavors,
                                queryResults, latestFilter,
                                flavorFilter, scoreCache)
                if latestFilter != _GET_TROVE_VERY_LATEST or not newerEmptyMatches:
                    self._addFilteredFlavorsToResults(version, flavorList,
                                flavorQuery, usedFlavors, queryResults,
                                latestFilter, flavorFilter, scoreCache)
            else:
                self._addFilteredFlavorsToResults(version, flavorList,
                                                  flavorQuery,
                                                  usedFlavors, queryResults,
                                                  latestFilter,
                                                  flavorFilter, scoreCache)

            if emptyMatches:
                if latestFilter == _GET_TROVE_VERY_LATEST:
                    if not newerEmptyMatches:
                        newerEmptyMatches = [(version, emptyMatches)]
                else:
                    newerEmptyMatches.append((version, emptyMatches))


            if (currentPreferenceScore == 0
                and latestFilter == _GET_TROVE_VERY_LATEST
                and flavorFilter == _GET_TROVE_BEST_FLAVOR):
                break
        if not queryResults:
            altFlavors = list(
                            itertools.chain(*[x[1] for x in versionFlavorList]))
        else:
            altFlavors = []
        results = {}
        for version, flavorList in queryResults.iteritems():
            results.setdefault(version, set()).update(flavorList)
        return results, altFlavors

    def _filterByPreferences(self, flavorList, scoreToMatch,
                             preferenceList):
        if not preferenceList:
            return 0, flavorList, []
        strongList = [ (x.toStrongFlavor(), x) for x in flavorList ]
        indexedList = list(enumerate(preferenceList[:scoreToMatch + 1]))
        nomatches = []
        minScore = None
        matchingFlavors = []
        for strongFlavor, flavor in strongList:
            for currentScore, preferenceFlavor in indexedList:
                if strongFlavor.satisfies(preferenceFlavor):
                    if minScore is None or currentScore < minScore:
                        matchingFlavors = []
                    elif currentScore > minScore:
                        break
                    minScore = currentScore
                    matchingFlavors.append(flavor)
                    break
            else:
                # no matching flavors
                nomatches.append(flavor)
        if matchingFlavors:
            if scoreToMatch >= minScore:
                return minScore, matchingFlavors, nomatches
            elif nomatches:
                return scoreToMatch, nomatches, nomatches
            else:
                return None, [], []
        elif nomatches:
            return scoreToMatch, nomatches, nomatches
        else:
            return None, [], []

    def filterTrovesByPreferences(self, troveList):
        preferenceList = self._flavorPreferences
        if not preferenceList:
            return troveList
        bestPreference = []
        matchingNone = set(troveList)
        bestMatching = []
        for pref in preferenceList:
            matching = [ x for x in troveList
                         if x[2].stronglySatisfies(pref) ]
            matchingNone.difference_update(matching)
            if not bestMatching:
                bestMatching = matching
        return bestMatching + list(matchingNone)

    def _calculateFlavorScores(self, flavorCheck, flavorQuery, flavorList,
                               scoreCache):
        assert(flavorQuery is not None)
        toCalc = [ x for x in flavorList
                      if (flavorQuery, x) not in scoreCache ]
        if not toCalc:
            return
        if flavorCheck == _CHECK_TROVE_STRONG_FLAVOR:
            strongFlavors = [x.toStrongFlavor() for x in toCalc]
            toCalc = zip(strongFlavors, toCalc)
            scores = ((x[0].score(flavorQuery), x[1]) \
                                        for x in toCalc)
        else:
            scores = ((flavorQuery.score(x), x) for x in flavorList)

        scoreCache.update(((flavorQuery, x[1]),x[0]) for x in scores)

    def _addFilteredFlavorsToResults(self, version, flavorList, flavorQuery,
                                   usedFlavors, queryResults, latestFilter,
                                   flavorFilter, scoreCache):
        troveFlavors = set()
        if flavorFilter == _GET_TROVE_BEST_FLAVOR:
            bestFlavor = max([(scoreCache[flavorQuery, x], x)
                                for x in flavorList ])[1]
            troveFlavors.add(bestFlavor)
        elif flavorFilter == _GET_TROVE_ALLOWED_FLAVOR:
            troveFlavors.update(flavorList)
            if latestFilter == _GET_TROVE_VERY_LATEST:
                troveFlavors.difference_update(usedFlavors)
                usedFlavors.update(flavorList)
        else:
            assert(0)
        if troveFlavors:
            queryResults[version] = troveFlavors


    def getTroveLeavesByLabel(self, troveSpecs, bestFlavor=True,
                              troveTypes=TROVE_QUERY_PRESENT):
        return self._getTrovesByType(troveSpecs, _GTL_VERSION_TYPE_LABEL,
                                 _GET_TROVE_VERY_LATEST, bestFlavor, troveTypes=troveTypes)

    def getTroveLatestByLabel(self, troveSpecs, bestFlavor=True,
                             troveTypes=TROVE_QUERY_PRESENT):
        return self._getTrovesByType(troveSpecs, _GTL_VERSION_TYPE_LABEL_LATEST,
                                 _GET_TROVE_VERY_LATEST, bestFlavor,
                                 troveTypes=troveTypes)


    def getTroveVersionsByLabel(self, troveSpecs, bestFlavor=True,
                                troveTypes=TROVE_QUERY_PRESENT):
        return self._getTrovesByType(troveSpecs, _GTL_VERSION_TYPE_LABEL,
                                     _GET_TROVE_ALL_VERSIONS, bestFlavor,
                                     troveTypes=troveTypes)

    def getTroveLeavesByBranch(self, troveSpecs, bestFlavor=True,
                               troveTypes=TROVE_QUERY_PRESENT):
        """ Takes {n : { Version : [f,...]}} dict """
        return self._getTrovesByType(troveSpecs, _GTL_VERSION_TYPE_BRANCH,
                                     _GET_TROVE_VERY_LATEST, bestFlavor,
                                     troveTypes=troveTypes)

    def getTroveVersionsByBranch(self, troveSpecs, bestFlavor=True,
                                 troveTypes=TROVE_QUERY_PRESENT):
        return self._getTrovesByType(troveSpecs, _GTL_VERSION_TYPE_BRANCH,
                                     _GET_TROVE_ALL_VERSIONS, bestFlavor,
                                     troveTypes=troveTypes)

    def getTroveVersionFlavors(self, troveSpecs, bestFlavor=True,
                               troveTypes=TROVE_QUERY_PRESENT):
        """ Takes {n : { Version : [f,...]}} dict """
        return self._getTrovesByType(troveSpecs,
                                     _GTL_VERSION_TYPE_VERSION,
                                     _GET_TROVE_ALL_VERSIONS,
                                     bestFlavor, troveTypes=troveTypes)

class SimpleTroveSource(SearchableTroveSource):

    def __init__(self, troveTups=[]):
        SearchableTroveSource.__init__(self)
        troveTups = list(troveTups)
        _trovesByName = {}
        for (n,v,f) in troveTups:
            _trovesByName.setdefault(n,set()).add((n,v,f))
        self._trovesByName = _trovesByName

    def trovesByName(self, name):
        if name is None:
            return list(self)
        return self._trovesByName.get(name, [])

    def iterAllTroveNames(self):
        return iter(self._trovesByName)

    def hasTroves(self, troveTups):
        return [ x in self._trovesByName.get(x[0], []) for x in troveTups ]

    def __len__(self):
        return len([x for x in self ])

    def __iter__(self):
        return itertools.chain(*self._trovesByName.itervalues())

    def addTrove(self, n, v, f):
        self._trovesByName.setdefault(n,set()).add((n,v,f))


class TroveListTroveSource(SimpleTroveSource):
    def __init__(self, source, troveTups, recurse=True):
        SimpleTroveSource.__init__(self, troveTups)
        self.source = source
        self.sourceTups = troveTups[:]

        foundTups = set()

        # recurse into the given trove tups to include all child troves
        for (n,v,f) in troveTups:
            self._trovesByName.setdefault(n, set()).add((n,v,f))

        if recurse:
            newTroves = source.getTroves(troveTups, withFiles=False)
            for newTrove in newTroves:
                for tup in newTrove.iterTroveList(strongRefs=True,
                                                  weakRefs=True):
                    self._trovesByName.setdefault(tup[0], set()).add(tup)

    def getSourceTroves(self):
        return self.getTroves(self.sourceTups)

    def getTroves(self, troveTups, withFiles=False, callback=None):
        return self.source.getTroves(troveTups, withFiles, callback=callback)

    def hasTroves(self, troveTups):
        return self.source.hasTroves(troveTups)

    def resolveDependenciesByGroups(self, troveList, deps):
        return self.source.resolveDependenciesByGroups(troveList, deps)



class GroupRecipeSource(SearchableTroveSource):
    """ A TroveSource that contains all the troves in a cooking
        (but not yet committed) recipe.  Useful for modifying a recipe
        in progress using findTrove.
    """

    def __init__(self, source, groupRecipe):
        SearchableTroveSource.__init__(self)
        self.searchAsDatabase()
        self.deps = {}
        self._trovesByName = {}
        self.source = source
        self.sourceTups = groupRecipe.troves

        for (n,v,f) in self.sourceTups:
            self._trovesByName.setdefault(n, []).append((n,v,f))

    def getTroves(self, troveTups, withFiles=False, callback=None):
        return self.source.getTroves(troveTups, withFiles, callback=callback)

    def hasTroves(self, troveTups):
        return self.source.hasTroves(troveTups)

    def trovesByName(self, name):
        return self._trovesByName.get(name, [])

    def delTrove(self, name, version, flavor):
        self._trovesByName[name].remove((name, version, flavor))

    def addTrove(self, name, version, flavor):
        self._trovesByName.setdefault(name, []).append((name, version, flavor))

class ReferencedTrovesSource(SearchableTroveSource):
    """ A TroveSource that only (n,v,f) pairs for troves that are
        referenced by other, installed troves.
    """
    def __init__(self, source):
        SearchableTroveSource.__init__(self)
        self.searchAsDatabase()
        self.source = source

    def hasTroves(self, troveTups):
        return self.source.hasTroves(troveTups)

    def getTroves(self, troveTups, *args, **kw):
        return self.source.getTroves(troveTups, *args, **kw)

    def trovesByName(self, name):
        return self.source.findTroveReferences([name])[0]

class ChangesetFilesTroveSource(SearchableTroveSource):

    # Provide a trove source based on both absolute and relative change
    # set files. Changesets withFiles=False can be generated from this
    # source. Conflicting troves added to this cause an exception, and
    # if the old version for a relative trovechangeset is not available,
    # an exception is thrown.

    # it's likely this should all be indexed by troveName instead of
    # full tuples

    def __init__(self, db, storeDeps=False):
        SearchableTroveSource.__init__(self)
        self.db = db
        self.troveCsMap = {}
        self.jobMap = {}
        self.providesMap = {}
        self.csList = []
        self.invalidated = False
        self.erasuresMap = {}
        self.rooted = {}
        self.idMap = {}
        self.storeDeps = storeDeps

        # Parallel list to csList: file names for the changesets
        # Format is (filename, includesFileContents)
        self.csFileNameList = []

        if storeDeps:
            self.depDb = deptable.DependencyDatabase()

    def addChangeSets(self, csList, includesFileContents = False):
        for cs in csList:
            self.addChangeSet(cs, includesFileContents=includesFileContents)

    def addChangeSet(self, cs, includesFileContents = False):
        if cs.isEmpty():
            return
        relative = []

        if not self.idMap:
            startId = 0
        else:
            startId = max(self.idMap) + 1

        for idx, trvCs in enumerate(cs.iterNewTroveList()):
            troveId = idx + startId
            info = (trvCs.getName(), trvCs.getNewVersion(),
                    trvCs.getNewFlavor())
            self.providesMap.setdefault(trvCs.getProvides(), []).append(info)
            if self.storeDeps:
                self.depDb.add(troveId, trvCs.getProvides(),
                               trvCs.getRequires())
            self.idMap[troveId] = info

            if trvCs.getOldVersion() is None:
                jobMapKey = (info[0], (None, None), info[1:],
                             trvCs.isAbsolute())
                if info in self.troveCsMap:
                    oldInc = int(self.jobMap.get(jobMapKey, (None, False))[1])
                    if includesFileContents <= oldInc:
                        # Refer to the older one, it's more complete
                        continue
                self.troveCsMap[info] = cs
                self.jobMap[jobMapKey] = cs, includesFileContents
                continue


            relative.append((trvCs, info))

        for info in cs.getOldTroveList():
            self.erasuresMap[info] = cs

        if self.storeDeps:
            self.depDb.commit()

        if relative:
            for (trvCs, info) in relative:
                jobMapKey = (info[0],
                             (trvCs.getOldVersion(), trvCs.getOldFlavor()),
                             info[1:], trvCs.isAbsolute())

                if info in self.troveCsMap:
                    oldInc = int(self.jobMap.get(jobMapKey, (None, False))[1])
                    if includesFileContents <= oldInc:
                        # Refer to the older one, it's more complete
                        continue
                if not self.db.hasTrove(*trvCs.getOldNameVersionFlavor()):
                    # we don't has the old version of this trove, don't
                    # use this changeset when updating this trove.
                    continue
                self.troveCsMap[info] = cs
                self.jobMap[jobMapKey] = (cs, includesFileContents)

        self.csList.append(cs)
        # Save file name too
        fileName = None
        if hasattr(cs, 'fileName'):
            fileName = cs.fileName
        self.csFileNameList.append((fileName, includesFileContents))

    def reset(self):
        for cs in self.csList:
            cs.reset()

    def trovesByName(self, name):
        l = []
        for info in self.troveCsMap:
            if info[0] == name:
                l.append(info)

        return l

    def iterAllTroveNames(self):
        troveNames = set()
        for name, _, _ in self.troveCsMap:
            troveNames.add(name)
        return iter(troveNames)

    def iterFilesInTrove(self, n, v, f, sortByPath=False, withFiles=False,
                         capsules = False):
        try:
            cs = self.troveCsMap[n,v,f]
        except KeyError:
            raise errors.TroveMissing(n, v)

        trvCs = cs.getNewTroveVersion(n,v,f)
        fileList = trvCs.getNewFileList()
        if not fileList:
            return

        if capsules:
            fileList = [ x for x in fileList if x[0] == trove.CAPSULE_PATHID ]
        else:
            fileList = [ x for x in fileList if x[0] != trove.CAPSULE_PATHID ]

        if not withFiles:
            if sortByPath:
                for item in sorted(fileList):
                    yield item
            else:
                for item in fileList:
                    yield item
            return

        if sortByPath:
            # files stored in changesets are sorted by pathId, and must be
            # retrieved in that order.  But we want to display them by
            # path.  So, retrieve the info from the changeset by pathId
            # and stored it in a dict to be retrieved after sorting by
            # path
            changes = {}
            for pathId, path, fileId, version in fileList:
                changes[pathId] = cs.getFileChange(None, fileId)

            fileList = sorted(fileList, key=lambda x: x[1])

        for pathId, path, fileId, version in fileList:
            change = changes[pathId]
            if change is None:
                fileObj = None
            else:
                fileObj = files.ThawFile(change, pathId)
            yield pathId, path, fileId, version, fileObj

    def getFileVersion(self, pathId, fildId, version):
        # TODO: implement getFileVersion for changeset source
        raise KeyError

    def getTroveInfo(self, infoType, troveList):
        retList = []

        attrName = trove.TroveInfo.streamDict[infoType][2]

        for info in troveList:
            cs = self.troveCsMap.get(info, None)
            if cs is None:
                retList.append(-1)
                continue

            trvCs = cs.getNewTroveVersion(*info)
            ti = trvCs.getTroveInfo()
            retList.append(getattr(ti, attrName))

        return retList

    def getDepsForTroveList(self, troveList, provides = True, requires = True):
        # returns a list of (prov, req) pairs
        retList = []

        for info in troveList:
            cs = self.troveCsMap.get(info, None)
            if cs is None:
                retList.append(None)
            else:
                trvCs = cs.getNewTroveVersion(*info)
                retList.append((trvCs.getProvides(), trvCs.getRequires()))

        return retList

    def getPathHashesForTroveList(self, troveList):
        retList = []

        for info in troveList:
            cs = self.troveCsMap.get(info, None)
            if cs is None:
                return SearchableTroveSource.getPathHashesForTroveList(self,
                                                                   troveList)

            trvCs = cs.getNewTroveVersion(*info)
            hashes = trvCs.getNewPathHashes()
            if hashes is None:
                # this happens for relative trvCs if there is no absolute
                # trove info. that would be a really old server (server,
                # not trove, since absolute trove info is a changeset
                # artifact only).
                trv = self.getTrove(withFiles=False, *info)
                hashes = trv.getPathHashes()

            retList.append(hashes)

        return retList

    def getTroves(self, troveList, withFiles = True, callback = None):
        retList = []

        for info in troveList:
            if info not in self.troveCsMap:
                retList.append(None)
                continue

            trvCs = self.troveCsMap[info].getNewTroveVersion(*info)
            if trvCs.getOldVersion() is None:
                newTrove = trove.Trove(trvCs,
                                       skipIntegrityChecks = not withFiles)
                if withFiles:
                    for pathId, path, fileId, version in trvCs.getNewFileList():
                        newTrove.addFile(pathId, path, version, fileId)
            else:
                newTrove = self.db.getTrove(trvCs.getName(),
                                            trvCs.getOldVersion(),
                                            trvCs.getOldFlavor(),
                                            withFiles=withFiles)

                newTrove.applyChangeSet(trvCs,
                                        skipFiles = not withFiles,
                                        skipIntegrityChecks = not withFiles)
            retList.append(newTrove)

        return retList

    def getTroveChangeSets(self, jobList, withFiles=False):
        trvCsList = []
        for job in jobList:
            name, (oldVer, oldFla), (newVer, newFla) = job[:3]
            if newVer:
                info = (name, newVer, newFla)
                trvCs = self.troveCsMap[info].getNewTroveVersion(*info)
                assert((trvCs.getOldVersion(), trvCs.getOldFlavor()) ==
                       (oldVer, oldFla))
                trvCsList.append(trvCs)
            else:
                raise NotImplementedError, 'we don"t store erasures'
        return trvCsList

    def getTroveChangeSet(self, job, withFiles=False):
        return self.getTroveChangeSets([job], withFiles)[0]

    def hasTroves(self, troveList):
        return [ x in self.troveCsMap for x in troveList ]

    def getChangeSet(self, job):
        name, (oldVer, oldFla), (newVer, newFla) = job[:3]
        if newVer:
            info = (name, newVer, newFla)
            return self.troveCsMap[info]
        else:
            info = (name, oldVer, oldFla)
            return self.erasuresMap[info]

    def iterChangeSets(self):
        return iter(self.csList)

    def iterChangeSetsFlags(self):
        for cs, (fname, incFileConts) in zip(self.csList, self.csFileNameList):
            yield cs, fname, incFileConts

    def resolveDependencies(self, label, depList, leavesOnly=False):
        assert(self.storeDeps)
        suggMap = self.depDb.resolve(label, depList, leavesOnly=leavesOnly)
        for depSet, solListList in suggMap.iteritems():
            newSolListList = []
            for solList in solListList:
                newSolListList.append([ self.idMap[x] for x in solList ])

            suggMap[depSet] = newSolListList
        return suggMap

    def createChangeSet(self, jobList, withFiles = True, recurse = False,
                        withFileContents = False, useDatabase = True,
                        callback = None):
        # Returns the changeset plus a remainder list of the bits it
        # couldn't do
        def _findTroveObj(availSet, (name, version, flavor)):
            info = (name, version, flavor)
            (inDb, fromCs) = availSet[info]

            if fromCs:
                [ trv ] = self.getTroves([info], withFiles = False)
            elif inDb and self.db:
                # XXX this should be parallelized...
                trv = self.db.getTrove(withFiles = False, *info)
            else:
                trv = None

            return trv

        assert((withFiles and withFileContents) or
               (not withFiles and not withFileContents))

        if recurse:
            # FIXME: can't handle recursive change set creation,
            # just bail
            cs = changeset.ReadOnlyChangeSet()
            return cs, jobList

        troves = []
        for job in jobList:
            if job[1][0] is not None:
                troves.append((job[0], job[1][0], job[1][1]))
            if job[2][0] is not None:
                troves.append((job[0], job[2][0], job[2][1]))

        if useDatabase and self.db:
            inDatabase = self.db.hasTroves(troves)
        else:
            inDatabase = [ False ] * len(troves)

        asChangeset = [ info in self.troveCsMap for info in troves ]
        trovesAvailable = dict((x[0], (x[1], x[2])) for x in
                            itertools.izip(troves, inDatabase, asChangeset))

        remainder = []

        # Track jobs we need to go get directly from change sets later, and
        # jobs which need to be rooted relative to a change set.
        changeSetJobs = set()
        needsRooting = []

        newCs = changeset.ChangeSet()
        mergedCs = changeset.ReadOnlyChangeSet()

        jobFromCs = set()

        for job in jobList:
            oldInfo = (job[0], job[1][0], job[1][1])
            newInfo = (job[0], job[2][0], job[2][1])

            if newInfo[1] is None:
                newCs.oldTrove(*oldInfo)
                continue

            # if this job is available from a changeset already, just deliver
            # it w/o computing it
            if job in self.jobMap:
                subCs, filesAvailable = self.jobMap[job]
                if (not withFileContents or filesAvailable):
                    changeSetJobs.add(job)
                    continue

            if withFileContents:
                # is this available as an absolute change set with files?
                absJob = (job[0], (None, None), job[2], True)
                if absJob in self.jobMap and self.jobMap[absJob][1] is True:
                    needsRooting.append((job, absJob))
                    continue
                else:
                    # otherwise we can't deliver files
                    remainder.append(job)
                    continue

            newTrv = _findTroveObj(trovesAvailable, newInfo)
            if newTrv is None:
                remainder.append(job)
                continue

            if oldInfo[1] is None:
                oldTrv = None
            else:
                oldTrv = _findTroveObj(trovesAvailable, oldInfo)
                if oldTrv is None:
                    remainder.append(job)
                    continue

            newCs.newTrove(newTrv.diff(oldTrv)[0])

        rootMap = {}
        for (relJob, absJob) in needsRooting:
            assert(relJob[0] == absJob[0])
            assert(relJob[2] == absJob[2])
            rootMap[(absJob[0], absJob[2][0], absJob[2][1])] = relJob[1]
            # and let the normal changeset handling assemble the final
            # changesets for us. the relative job will exist after the
            # rooting.
            changeSetJobs.add(relJob)

        if rootMap:
            # we can't root troves changesets multiple times
            for info in rootMap:
                assert(info not in self.rooted)

            for subCs in self.csList:
                subCs.rootChangeSet(self.db, rootMap)

            self.rooted.update(rootMap)

        # assemble jobs directly from changesets and update those changesets
        # to not have jobs we don't need
        if changeSetJobs:
            # Build up a changeset that contains exactly the trvCs objects
            # we need. The file contents come from the changesets included
            # in this trove (we don't reset() the underlying changesets
            # because this isn't a good place to coordinate that reset when
            # multiple threads are in use)
            jobsToFind = list(changeSetJobs)
            for subCs in self.csList:
                keep = False
                for trvCs in subCs.iterNewTroveList():
                    if trvCs.getOldVersion() is None:
                        job = (trvCs.getName(),
                                  (None, None),
                                  (trvCs.getNewVersion(), trvCs.getNewFlavor()),
                                trvCs.isAbsolute())
                    else:
                        job = (trvCs.getName(),
                                  (trvCs.getOldVersion(), trvCs.getOldFlavor()),
                                  (trvCs.getNewVersion(), trvCs.getNewFlavor()),
                                trvCs.isAbsolute())

                    if job in jobsToFind:
                        newCs.newTrove(trvCs)
                        jobsToFind.remove(job)
                        keep = True
                if keep:
                    subCs.reset()
                    mergedCs.merge(subCs)

        # Remove all of the new and old job information from the merged
        # changeset and replace it with the job information we assembled
        # in newCs
        mergedCs.clearTroves()
        mergedCs.merge(newCs)

        return (mergedCs, remainder)

    def merge(self, source):
        assert(not self.storeDeps and not source.storeDeps)
        self.troveCsMap.update(source.troveCsMap)
        self.jobMap.update(source.jobMap)
        self.providesMap.update(source.providesMap)
        self.csList.extend(source.csList)
        self.invalidated = self.invalidated or source.invalidated
        self.erasuresMap.update(source.erasuresMap)
        self.rooted.update(source.rooted)
        self.idMap.update(source.idMap)
        self.storeDeps = self.storeDeps or source.storeDeps


class SourceStack(object):

    def __init__(self, *sources):
        self.sources = []
        for source in sources:
            self.addSource(source)

    def addSource(self, source):
        if source is None:
            return

        if isinstance(source, self.__class__):
            for subSource in source.iterSources():
                self.addSource(subSource)
            return

        if source not in self:
            self.sources.append(source)

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__,
                           ', '.join(repr(x) for x in self.sources))

    def hasSource(self, source):
        return source in self

    def hasTroves(self, troveList):
        results = [False] * len(troveList)
        troveList = list(enumerate(troveList))
        for source in self.sources:
            try:
                hasTroves = source.hasTroves([x[1] for x in troveList])
            except errors.OpenError:
                continue
            newTroveList = []
            if isinstance(hasTroves, list):
                for (index, troveTup), hasTrove in zip(troveList, hasTroves):
                    if hasTrove:
                        results[index] = True
                    else:
                        newTroveList.append((index, troveTup))
            else:
                for (index, troveTup) in troveList:
                    if hasTroves.get(troveTup):
                        results[index] = True
                    else:
                        newTroveList.append((index, troveTup))
            troveList = newTroveList
            if not troveList:
                break
        return results

    def iterSources(self):
        for source in self.sources:
            yield source

    def getFileContents(self, fileList, *args, **kw):
        results = [ None for x in fileList ]
        map = dict((x[0], x[0]) for x in enumerate(fileList))
        for source in self.sources:
            if not hasattr(source, 'getFileContents'):
                continue
            newMap = []
            results = source.getFileContents(fileList, *args, **kw)
            for curIdx, result in enumerate(results):
                if result is None:
                    newMap.append((len(newMap), map[curIdx]))
                else:
                    results[map[curIdx]] = result
            if not newMap:
                break
            map = dict(newMap)
        return results

    def copy(self):
        return self.__class__(*self.sources)

    def __contains__(self, newSource):
        # don't use == because some sources may define ==
        for source in self.sources:
            if source is newSource:
                return True
        return False

    def getTrove(self, name, version, flavor, withFiles = True):
        trv = self.getTroves([(name, version, flavor)], withFiles=withFiles)[0]
        if trv is None:
            raise errors.TroveMissing(name, version)
        return trv

    def getTroves(self, troveList, withFiles = True, allowMissing=True,
                    callback=None):
        troveList = list(enumerate(troveList)) # make a copy and add indexes
        numTroves = len(troveList)
        results = [None] * numTroves

        for source in self.sources:
            newTroveList = []
            newIndexes = []
            try:
                troves = source.getTroves([x[1] for x in troveList],
                                          withFiles=withFiles,
                                          callback=callback)
            except NotImplementedError:
                continue
            for ((index, troveTup), trove) in itertools.izip(troveList, troves):
                if trove is None:
                    newTroveList.append((index, troveTup))
                else:
                    results[index] = trove
            troveList = newTroveList
        if troveList and not allowMissing:
            raise errors.TroveMissingError(troveList[0][1][0],
                                           troveList[0][1][1])
        return results

    def getTroveInfo(self, infoType, troveTupList):
        # -1 means "unknown trove" None means "troveinfo not in the trove"
        results = [ -1 ] * len(troveTupList)
        for source in self.sources:
            need = [ (i, troveTup) for i, (troveTup, ti) in
                        enumerate(itertools.izip(troveTupList, results))
                        if ti == -1]
            if not need:
                break

            tiList = source.getTroveInfo(infoType, [ x[1] for x in need] )
            for (i, troveTup), troveInfo in itertools.izip(need, tiList):
                if troveInfo == 0:
                    results[0] = None
                else:
                    results[i] = troveInfo

        return results

    def getDepsForTroveList(self, troveInfoList, provides = True,
                            requires = True):
        results = [ None ] * len(troveInfoList)
        for source in self.sources:
            need = [ (i, troveInfo) for i, (troveInfo, depTuple) in
                        enumerate(itertools.izip(troveInfoList, results))
                        if depTuple is None ]
            if not need:
                break

            depList = source.getDepsForTroveList([ x[1] for x in need] )
            for (i, troveInfo), depTuple in itertools.izip(need, depList):
                if depTuple is not None:
                    results[i] = depTuple

        return results

    def createChangeSet(self, jobList, withFiles = True, recurse = False,
                        withFileContents = False, callback = None):

        cs = changeset.ReadOnlyChangeSet()

        for source in self.sources:
            if not jobList:
                break

            res = source.createChangeSet(jobList,
                                       withFiles = withFiles,
                                       withFileContents = withFileContents,
                                       recurse = recurse,
                                       callback = callback)
            if isinstance(res, (list, tuple)):
                newCs, jobList = res
            else:
                newCs, jobList = res, None
            cs.merge(newCs)

        return cs, jobList

    def getFileVersions(self, fileIds):
        results = [ None ] * len(fileIds)
        needed = list(enumerate(fileIds))
        for source in self.sources:
            try:
                newResults = source.getFileVersions([ x[1] for x in needed ])
                for result, (i, info) in itertools.izip(newResults, needed):
                    if info:
                        results[i] = result
                needed = [ tup for tup in needed if results[tup[0]] is None ]
                if not needed:
                    break

            # FIXME: there should be a better error for this
            except (KeyError, NotImplementedError), e:
                continue

        return results


    def getFileVersion(self, pathId, fileId, version):
        for source in self.sources:
            try:
                return source.getFileVersion(pathId, fileId, version)
            # FIXME: there should be a better error for this
            except (KeyError, NotImplementedError), e:
                continue
        return None

    def iterFilesInTrove(self, n, v, f, *args, **kw):
        for source in self.sources:
            try:
                for value in source.iterFilesInTrove(n, v, f, *args, **kw):
                    yield value
                return
            except NotImplementedError:
                pass
            except errors.TroveMissing:
                pass
        raise errors.TroveMissing(n,v)


class TroveSourceStack(SourceStack, SearchableTroveSource):
    def __init__(self, *args, **kw):
        SourceStack.__init__(self, *args, **kw)
        SearchableTroveSource.__init__(self)

    def requiresLabelPath(self):
        for source in self.iterSources():
            if source.requiresLabelPath():
                return True
        return False

    def insertSource(self, source, idx=0):
        if source is not None and source not in self:
            self.sources.insert(idx, source)

    def trovesByName(self, name):
        return list(itertools.chain(*(x.trovesByName(name) for x in self.sources)))

    def isSearchAsDatabase(self):
        for source in self.sources:
            if not source._allowNoLabel:
                return False
        return True

    def findTroves(self, labelPath, troveSpecs, defaultFlavor=None,
                   acrossLabels=False, acrossFlavors=False,
                   affinityDatabase=None, allowMissing=False,
                   bestFlavor=None, getLeaves=None,
                   troveTypes=TROVE_QUERY_PRESENT, exactFlavors=False,
                   **kw):
        sourceBestFlavor = bestFlavor
        sourceGetLeaves = getLeaves
        troveSpecs = list(troveSpecs)

        results = {}

        for source in self.sources[:-1]:
            # FIXME: it should be possible to reuse the trove finder
            # but the bestFlavr and getLeaves data changes per source
            # and is passed into several TroveFinder sub objects.
            # TroveFinder should be cleaned up
            if source._allowNoLabel:
                sourceLabelPath = None
            else:
                sourceLabelPath = labelPath
            if source._flavorCheck == _CHECK_TROVE_STRONG_FLAVOR:
                sourceDefaultFlavor = None
            else:
                sourceDefaultFlavor = defaultFlavor

            if source.searchableByType():
                sourceTroveTypes = troveTypes
            else:
                sourceTroveTypes = TROVE_QUERY_PRESENT

            if bestFlavor is None:
                sourceBestFlavor = source._bestFlavor
            if getLeaves is None:
                sourceGetLeaves = source._getLeavesOnly
            troveFinder = findtrove.TroveFinder(source, sourceLabelPath,
                                            sourceDefaultFlavor, acrossLabels,
                                            acrossFlavors, affinityDatabase,
                                            allowNoLabel=source._allowNoLabel,
                                            bestFlavor=sourceBestFlavor,
                                            getLeaves=sourceGetLeaves,
                                            troveTypes=sourceTroveTypes,
                                            exactFlavors=exactFlavors,
                                            **kw)

            foundTroves = troveFinder.findTroves(troveSpecs, allowMissing=True)

            newTroveSpecs = []
            for troveSpec in troveSpecs:
                if troveSpec in foundTroves:
                    results[troveSpec] = foundTroves[troveSpec]
                else:
                    newTroveSpecs.append(troveSpec)

            troveSpecs = newTroveSpecs

        source = self.sources[-1]

        if source._flavorCheck == _CHECK_TROVE_STRONG_FLAVOR:
            sourceDefaultFlavor = None
        else:
            sourceDefaultFlavor = defaultFlavor

        if source._allowNoLabel:
            sourceLabelPath = None
        else:
            sourceLabelPath = labelPath
        if source.searchableByType():
            sourceTroveTypes = troveTypes
        else:
            sourceTroveTypes = TROVE_QUERY_PRESENT
        if bestFlavor is None:
            sourceBestFlavor = source._bestFlavor
        if getLeaves is None:
            sourceGetLeaves = source._getLeavesOnly


        troveFinder = findtrove.TroveFinder(source, sourceLabelPath,
                                        sourceDefaultFlavor, acrossLabels,
                                        acrossFlavors, affinityDatabase,
                                        allowNoLabel=source._allowNoLabel,
                                        bestFlavor=sourceBestFlavor,
                                        getLeaves=sourceGetLeaves,
                                        troveTypes=sourceTroveTypes,
                                        exactFlavors=exactFlavors,
                                        **kw)

        results.update(troveFinder.findTroves(troveSpecs,
                                              allowMissing=allowMissing))
        return results

    def resolveDependencies(self, label, depList, leavesOnly=False):
        results = {}
        depList = set(depList)
        for depSet in depList:
            results[depSet] = [ [] for x in depSet.iterDeps() ]

        for source in self.sources:
            if not depList:
                break

            sugg = source.resolveDependencies(label, depList,
                                              leavesOnly=leavesOnly)
            for depSet, troves in sugg.iteritems():
                if [ x for x in troves if x ]:
                    # only consider this depSet 'solved' if at least
                    # on of the deps had a trove suggested for it.
                    # FIXME: We _could_ manipulate the depSet and send
                    # it back to get more responses from other trove sources.
                    depList.remove(depSet)
                    results[depSet] = troves
        return results

    def resolveDependenciesByGroups(self, troveList, depList):
        allSugg = {}
        for source in self.sources:
            sugg = source.resolveDependenciesByGroups(troveList, depList)
            # there's no ordering of suggestions when you're doing
            # resolveDependencies by groups
            self.mergeDepSuggestions(allSugg, sugg)
        return allSugg


def stack(*sources):
    """ create a trove source that will search first source1, then source2 """
    if len(sources) > 2:
        return stack(sources[0], stack(*sources[1:]))
    elif len(sources) == 1:
        return sources[0]
    elif not sources:
        return None
    else:
        source1, source2 = sources

    if source1 is source2:
        # trove source stacks may have different behavior than
        # individual trove sources, so always return a
        # stack even when there's nothing to stack
        if isinstance(source1, TroveSourceStack):
            return source1
        return TroveSourceStack(source1)

    if isinstance(source1, TroveSourceStack):
        if source1.hasSource(source2):
            return source1
        source1 = source1.copy()
        source1.addSource(source2)
        return source1
    elif isinstance(source2, TroveSourceStack):
        # addSource will do the proper thing to add source2's sources to
        # the stack
        s = TroveSourceStack(source1)
        s.addSource(source2)
        return s
    return TroveSourceStack(*sources)

class AbstractJobSource(AbstractTroveSource):

    # new, modified and old files.  Used to describe the files returned
    # by iterFilesInJob

    NEW_F = 0
    MOD_F = 1
    OLD_F = 2

    def iterAllJobs(self):
        raise NotImplementedError

    def findJob(self, name, oldVer, oldFla, newVer, newFla):
        raise NotImplementedError

    def findTroves(self, *args, **kw):
        raise NotImplementedError

    def getTroves(self, troves, withFiles=False):
        raise NotImplementedError

    def getTroveVersionList(self, name, withFlavors=False,
                            troveTypes=TROVE_QUERY_PRESENT):
        raise NotImplementedError

    def iterFilesInJob(self, job, sortByPath=False, withFiles=False,
                                                    withOldFiles=False):
        raise NotImplementedError


class JobSource(AbstractJobSource):

    def __init__(self, newTroveSource, oldTroveSource):
        AbstractJobSource.__init__(self)
        self.newTroveSource = newTroveSource
        self.oldTroveSource = oldTroveSource
        self.jobMap = {}
        self.jobsByNew = {}
        self.oldTroveList = SimpleTroveSource()
        self.newTroveList = SimpleTroveSource()
        self.allTroveList = SimpleTroveSource()
        self.eraseJobs = []

    def iterAllJobs(self):
        return iter(self.jobMap)

    def getTroveCsList(self, jobList, withFiles=False):
        raise NotImplementedError
        # basically needs to call createChangeSet, which, of course,
        # defeats the whole point of doing this with a job list.

    def addJob(self, job, value=None):
        if value is None:
            self.jobMap[job[:3]] = job[3]
        else:
            self.jobMap[job[:3]] = value

        name = job[0]

        if job[1][0] is not None:
            self.oldTroveList.addTrove(name, *job[1])
            self.allTroveList.addTrove(name, *job[1])

        if job[2][0] is not None:
            self.newTroveList.addTrove(name, *job[2])
            self.allTroveList.addTrove(name, *job[2])
            self.jobsByNew[name, job[2][0], job[2][1]] = job

    def findTroves(self, *args, **kw):
        return self.allTroveList.findTroves(*args, **kw)

    def findJobs(self, jobList):
        """ Finds a job given a changeSpec
            foo=--1.0 will match a fresh install to 1.0
            foo=1.0 will match a fresh install or an upgrade to 1.0
            foo will match an install, upgrade or erasure of foo
            foo=1.0-- will match an erasure of foo v. 1.0
            foo=1.0--2.0 will match an upgrade of foo from 1.0 to 2.0
        """
        allRes = {}

        newTroves = []
        oldTroves = []
        for (n, (oldVS, oldFS), (newVS, newFS), isAbs) in jobList:
            if isAbs:
                assert(not oldVS and not oldFS)

                newTroves.append((n, newVS, newFS))
                oldTroves.append((n, None, None))
                continue
            else:
                oldTroves.append((n, oldVS, oldFS))

                if newVS or newFS:
                    newTroves.append((n, newVS, newFS))

        newTroves = self.newTroveList.findTroves(None, newTroves,
                                                 allowMissing=True)
        oldTroves = self.oldTroveList.findTroves(None, oldTroves,
                                                 allowMissing=True)

        for (n, (oldVS, oldFS), (newVS, newFS), isAbs) in jobList:
            results = []
            if isAbs:
                newTups = newTroves.get((n, newVS, newFS), None)
                oldTups = oldTroves.get((n, None, None), None)
                if oldTups is None:
                    oldTups = []
                    oldTroves[(n, None, None)] = oldTups

                oldTups.append((n, None, deps.Flavor()))
            else:
                oldTups = oldTroves[n, oldVS, oldFS]

                if newVS or newFS:
                    newTups = newTroves[n, newVS, newFS]
                else:
                    newTups = None

            if newTups is None:
                for oldTup in oldTups:
                    job = (n, oldTup[1:], (None, None))
                    if job in self.jobMap:
                        results.append(job)
            else:
                for newTup in newTups:
                    job = self.jobsByNew[newTup]
                    for oldTup in oldTups:
                        if job[1] == oldTup[1:]:
                            results.append(job)

            allRes[(n, (oldVS, oldFS), (newVS, newFS), isAbs)] = results

        return allRes

    def iterFilesInJob(self, jobTup, sortByPath=False, withFiles=False,
                                                    withOldFiles=False):
        # basically needs to call createChangeSet, which defeats the
        # purpose of this class.
        raise NotImplementedError

class ChangeSetJobSource(JobSource):
    def __init__(self, newTroveSource, oldTroveSource):
        self.csSource = ChangesetFilesTroveSource(oldTroveSource)
        JobSource.__init__(self, stack(self.csSource, newTroveSource),
                                 stack(self.csSource, oldTroveSource))

    def addChangeSet(self, cs):
        self.csSource.addChangeSet(cs)

        for trvCs in cs.iterNewTroveList():
            name = trvCs.getName()
            newVer, newFla = trvCs.getNewVersion(), trvCs.getNewFlavor()
            oldVer, oldFla = trvCs.getOldVersion(), trvCs.getOldFlavor()
            isAbs = trvCs.isAbsolute()


            job = (name, (oldVer, oldFla), (newVer, newFla))
            self.addJob(job, (isAbs, cs))

        for (name, oldVer, oldFla) in cs.getOldTroveList():
            job = (name, (oldVer, oldFla), (None, None))
            self.addJob(job, (False, cs))
            self.eraseJobs.append(job)

    def getChangeSet(self, job):
        return self.csSource.getChangeSet(job)

    def getTroveChangeSet(self, job, withFiles=False):
        return self.getTroveChangeSets([job], withFiles=withFiles)[0]

    def getTroveChangeSets(self, jobList, withFiles=False):
        # NOTE: we can't store erasures as TroveChangeSets :(
        # they would fail to behave well with merges, so they're not allowed
        erasures = [ x for x in jobList if not x[2][0]]
        assert(not erasures)

        return self.csSource.getTroveChangeSets(jobList, withFiles=withFiles)

    def getTrove(self, n, v, f, withFiles=False):
        return self.csSource.getTrove(n, v, f, withFiles=withFiles)

    def iterFilesInJob(self, job, withFiles=False, withOldFiles=False,
                        sortByPath=False):
        # FIXME: do I need to get all files for a cs at once?
        iter = self._iterFilesInJob(job, withFiles, withOldFiles)
        if sortByPath:
            return sorted(iter, key=lambda x: x[1])
        else:
            return iter

    def _iterFilesInJob(self, job, withFiles=False, withOldFiles=False):
        def _getOldFile(pathId, fileId, version):
            return self.oldTroveSource.getFileVersion(pathId, fileId, version)

        if withOldFiles:
            assert(withFiles)

        n, (oldVer, oldFla), (newVer, newFla) = job[:3]

        isDel = not newVer
        newTrove = None

        if not isDel:
            trvCs = self.getTroveChangeSet(job, withFiles=withFiles)
            oldTrove = None
        else:
            oldTrove = self.oldTroveSource.getTrove(n, oldVer, oldFla,
                                                    withFiles=True)
            trvCs = None

        if not withFiles:
            if isDel:
                return
            for fileInfo in itertools.chain(trvCs.getNewFileList(),
                                            trvCs.getChangedFileList()):
                yield fileInfo
            return
        else:
            cs = self.getChangeSet(job)

            if isDel:
                fileList = [(x, self.OLD_F) for x in oldTrove.iterFileList()]

            else:
                newFiles = trvCs.getNewFileList()
                modFiles = trvCs.getChangedFileList()
                oldFiles = trvCs.getOldFileList()

                fileList = []
                fileList += [(x, self.NEW_F) for x in newFiles]
                fileList += [(x, self.MOD_F) for x in modFiles]
                fileList += [((x, None, None, None), self.OLD_F) for x in oldFiles]


                if oldVer and (newFiles or oldFiles or modFiles):
                    oldTrove = self.oldTroveSource.getTrove(n, oldVer, oldFla,
                                                            withFiles=True)

                if newFiles or modFiles:
                    if oldVer:
                        newTrove = oldTrove.copy()
                        newTrove.applyChangeSet(trvCs)
                    else:
                        newTrove = self.newTroveSource.getTrove(n, newVer,
                                                                newFla,
                                                                withFiles=True)

            # sort files by pathId
            # FIXME this is job-wide, it probably needs to be changeset wide
            fileList.sort()

            for (pathId, path, fileId, version), modType in fileList:
                oldFileObj = None
                fileObj = None

                if modType is self.NEW_F:
                    change = cs.getFileChange(None, fileId)
                    fileObj = files.ThawFile(change, pathId)
                    oldPath = oldFileId = oldVersion = oldFileObj = None
                elif modType is self.MOD_F:
                    (oldPath, oldFileId, oldVersion) = oldTrove.getFile(pathId)
                    change = cs.getFileChange(oldFileId, fileId)

                    if withOldFiles or files.fileStreamIsDiff(change):
                        oldFileObj = _getOldFile(pathId, oldFileId, oldVersion)

                    if files.fileStreamIsDiff(change):
                        fileObj = oldFileObj.copy()
                        fileObj.twm(change, fileObj)
                    else:
                        fileObj = files.ThawFile(change, pathId)

                    if path is None:
                        path = oldPath
                else:
                    assert(modType == self.OLD_F)
                    fileObj = None
                    if isDel:
                        oldPath, oldFileId, oldVersion = path, fileId, version
                        path = fileId = version = None
                    else:
                        (oldPath, oldFileId, oldVersion) = oldTrove.getFile(pathId)

                    oldFileObj = _getOldFile(pathId, oldFileId, oldVersion)

                if withOldFiles:
                    yield (pathId, path, fileId, version, fileObj, oldPath,
                           oldFileId, oldVersion, oldFileObj, modType)
                elif modType == self.OLD_F:
                    yield (pathId, oldPath, oldFileId,
                           oldVersion, oldFileObj, modType)
                else:
                    yield pathId, path, fileId, version, fileObj, modType


class FilterOptions(object):
    def __init__(self, latestFilter, flavorFilter, flavorCheck,
                 troveTypes, splitByBranch):
        self.latestFilter = latestFilter
        self.flavorFilter = flavorFilter
        self.flavorCheck = flavorCheck
        self.troveTypes = troveTypes
        self.splitByBranch = splitByBranch

    def updateOptions(self, getLeaves=None):
        if getLeaves is not None:
            if getLeaves:
                latestFilter = _GET_TROVE_VERY_LATEST
            else:
                latestFilter = _GET_TROVE_ALL_VERSIONS
            self.latestFilter = latestFilter
