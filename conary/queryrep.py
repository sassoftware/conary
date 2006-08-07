#
# Copyright (c) 2004-2006 rPath, Inc.
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
"""
Provides the output for the "conary repquery" command
"""
import itertools

from conary import conaryclient
from conary.conaryclient import cmdline
from conary import display
from conary.deps import deps
from conary.repository import trovesource, errors
from conary.lib import log

VERSION_FILTER_ALL    = 0
VERSION_FILTER_LATEST = 1
VERSION_FILTER_LEAVES = 2

FLAVOR_FILTER_ALL    = 0
FLAVOR_FILTER_AVAIL  = 1
FLAVOR_FILTER_BEST   = 2


def displayTroves(cfg, troveSpecs=[], pathList = [], whatProvidesList=[],
                  # query options
                  versionFilter=VERSION_FILTER_LATEST, 
                  flavorFilter=FLAVOR_FILTER_BEST, 
                  useAffinity = False,
                  # trove options
                  info = False, digSigs = False, showDeps = False,
                  showBuildReqs = False, 
                  # file options
                  ls = False, lsl = False, ids = False, sha1s = False, 
                  tags = False, fileDeps = False, fileVersions = False,
                  # collection options
                  showTroves = False, recurse = None, showAllTroves = False,
                  weakRefs = False, showTroveFlags = False, 
                  alwaysDisplayHeaders = False,
                  troveTypes=trovesource.TROVE_QUERY_PRESENT):
    """
       Displays information about troves found in repositories

       @param repos: a network repository client 
       @type repos: repository.netclient.NetworkRepositoryClient
       @param cfg: conary config
       @type cfg: conarycfg.ConaryConfiguration
       @param troveSpecs: troves to search for
       @type troveSpecs: list of troveSpecs (n[=v][[f]])
       @param versionFilter: add documentation here.  Check man page for 
       general description
       @type versionFilter: bool
       @param flavorFilter: add documentation here.  Check man page for 
       general description.
       @type flavorFilter: bool
       @param useAffinity: If False, disallow affinity database use.
       @type useAffinity: bool
       @param info: If true, display general information about the trove
       @type info: bool
       @param digSigs: If true, display digital signatures for a trove.
       @type digSigs: bool
       @param showBuildReqs: If true, display the versions and flavors of the
       build requirements that were used to build the given troves
       @type showBuildReqs: bool
       @param showDeps: If true, display provides and requires information 
       for the trove.
       @type showDeps: bool
       @param ls: If true, list files in the trove
       @type ls: bool
       @param lsl: If true, list files in the trove + ls -l information
       @type lsl: bool
       @param ids: If true, list pathIds for files in the troves
       @type ids: bool
       @param sha1s: If true, list sha1s for files in the troves
       @type sha1s: bool
       @param tags: If true, list tags for files in the troves
       @type tags: bool
       @param fileDeps: If true, print file-level dependencies
       @type fileDeps: bool
       @param fileVersions: If true, print fileversions
       @type fileVersions: bool
       @param showTroves: If true, display byDefault True child troves of this
       trove
       @type showTroves: bool
       @param recurse: display child troves of this trove, recursively
       @type recurse: bool
       @param showAllTroves: If true, display all byDefault False child troves 
       of this trove
       @type showAllTroves: bool
       @param weakRefs: display both weak and strong references of this trove.
       @type weakRefs: bool
       @param showTroveFlags: display [<flags>] list with information about
       the given troves.
       @type showTroveFlags: bool
       @param alwaysDisplayHeaders: If true, display headers even when listing  
       files.
       @type alwaysDisplayHeaders: bool
       @param showRemovedTroves: If True, display troves that have been removed from
       the repository (default False).
       @type showRemovedTroves: bool
       @param showRedirects: If True, display redirects (default False) 
       @type showRedirects: bool
       @rtype: None
    """

    client = conaryclient.ConaryClient(cfg)
    repos = client.getRepos()

    if useAffinity:
        affinityDb = client.db
    else:
        affinityDb = None

    whatProvidesList = [ deps.parseDep(x) for x in whatProvidesList ]

    troveTups = getTrovesToDisplay(repos, troveSpecs, pathList, 
                                   whatProvidesList,
                                   versionFilter, flavorFilter,
                                   cfg.installLabelPath, cfg.flavor, 
                                   affinityDb,
                                   troveTypes=troveTypes)

    dcfg = display.DisplayConfig(repos, affinityDb)

    dcfg.setTroveDisplay(deps=showDeps, info=info, 
                         showBuildReqs=showBuildReqs,
                         digSigs=digSigs, fullVersions=cfg.fullVersions,
                         showLabels=cfg.showLabels, fullFlavors=cfg.fullFlavors,
                         showComponents = cfg.showComponents,
                         baseFlavors = cfg.flavor)

    dcfg.setFileDisplay(ls=ls, lsl=lsl, ids=ids, sha1s=sha1s, tags=tags,
                        fileDeps=fileDeps, fileVersions=fileVersions)

    recurseOne = showTroves or showAllTroves or weakRefs
    if recurse is None and not recurseOne and troveSpecs:
        # if we didn't explicitly set recurse and we're not recursing one
        # level explicitly and we specified troves (so everything won't 
        # show up at the top level anyway), guess at whether to recurse
        recurse = True in (ls, lsl, ids, sha1s, tags, showDeps, fileDeps,
                           fileVersions)
    displayHeaders = alwaysDisplayHeaders or showTroveFlags 

    dcfg.setChildDisplay(recurseAll = recurse, recurseOne = recurseOne,
                         showNotByDefault = showAllTroves,
                         showWeakRefs = weakRefs,
                         showTroveFlags = showTroveFlags,
                         displayHeaders = displayHeaders,
                         checkExists = False)

    if troveSpecs:
        dcfg.setPrimaryTroves(set(troveTups))


    formatter = display.TroveFormatter(dcfg)

    display.displayTroves(dcfg, formatter, troveTups)


def getTrovesToDisplay(repos, troveSpecs, pathList, whatProvidesList, 
                       versionFilter, flavorFilter, labelPath, defaultFlavor, 
                       affinityDb, troveTypes=trovesource.TROVE_QUERY_PRESENT):
    """ Finds troves that match the given trove specifiers, using the
        current configuration, and parameters

        @param repos: a network repository client
        @type repos: repository.netclient.NetworkRepositoryClient
        @param troveSpecs: troves to search for
        @type troveSpecs: list of troveSpecs (n[=v][[f]])
        @param versionFilter: The VERSION_FILTER_* to use.  See man
        page for documentation for now.
        @type all: bool
        @param flavorFilter: The FLAVOR_FILTER_* to use.  See man
        page for documentation for now.
        @param labelPath: The labelPath to search
        @type labelPath: list
        @param defaultFlavor: The default flavor(s) to search with
        @type defaultFlavor: list
        @param affinityDb: The affinity database to search with.
        @type affinityDb: bool

        @rtype: troveTupleList (list of (name, version, flavor) tuples)
    """
    def _merge(resultD, response):
        for troveName, troveVersions in response.iteritems():
            d = resultD.setdefault(troveName, {})
            for version, flavors in troveVersions.iteritems():
                d.setdefault(version, []).extend(flavors)
        return resultD

    troveTups = []
    if troveSpecs or pathList or whatProvidesList:
        if whatProvidesList:
            tupList = []
            for label in labelPath:
                sols = repos.resolveDependencies(label, whatProvidesList)
                for solListList in sols.itervalues():
                    # list of list of solutions to the depSet
                    tupList.extend(itertools.chain(*solListList))

            source = trovesource.SimpleTroveSource(tupList)
            source.searchAsRepository()

            troveNames = set(x[0] for x in tupList)
            results = getTrovesToDisplay(source, troveNames, [], [],
                                         versionFilter, flavorFilter, labelPath,
                                         defaultFlavor, affinityDb=None,
                                         troveTypes=troveTypes)
            troveTups.extend(results)

        # Search for troves using findTroves.  The options we
        # specify to findTroves are determined by the version and 
        # flavor filter.
        if pathList:
            troveTups += getTrovesByPath(repos, pathList, versionFilter,
                                         flavorFilter, labelPath, defaultFlavor)


        if not troveSpecs:
            return sorted(troveTups)
        troveSpecs = [ cmdline.parseTroveSpec(x, allowEmptyName=False)
                       for x in troveSpecs ]
        searchFlavor = defaultFlavor

        acrossLabels = True
        if versionFilter == VERSION_FILTER_ALL:
            getLeaves = False
        elif versionFilter == VERSION_FILTER_LATEST:
            # we just want to limit all searches for the very latest
            # version node.  Find trove makes this difficult, we
            # do the leaves search and then filter.
            getLeaves = True
            acrossLabels = False
        elif versionFilter == VERSION_FILTER_LEAVES:
            # This will return all versions that are 'leaves', that is,
            # are the latest with a unique flavor string.
            getLeaves = True
            acrossLabels = False
        else:
            assert(0)

        if flavorFilter == FLAVOR_FILTER_ALL:
            searchFlavor = None 
            bestFlavor = False 
            acrossFlavors = True # there are no flavors to go 'across'
            newSpecs = []
            origSpecs = {}
            # We do extra processing here.  We want FLAVOR_FILTER_ALL to work 
            # when you specify a flavor to limit the all to. 
            # But findTrove won't let us do that, since it expects that
            # the flavors it gets passed are supersets of the trove flavors
            # So we search with no flavor and search by hand afterwards.
            for (n, vS, fS) in troveSpecs:
                origSpecs.setdefault((n, vS), []).append(fS)
                newSpecs.append((n, vS, None))
            troveSpecs = newSpecs
            affinityDb = None
        elif flavorFilter == FLAVOR_FILTER_AVAIL:
            # match install flavor + maybe affinity, could affect rq branch,
            # return all flavors that match.
            bestFlavor = False
            acrossFlavors = True
            if versionFilter != VERSION_FILTER_ALL:
                getLeaves = True
        elif flavorFilter == FLAVOR_FILTER_BEST:
            # match install flavor + affinity, could affect rq branch,
            # return best match.
            bestFlavor = True
            acrossFlavors = False

        if not affinityDb:
            acrossLabels = True

        results = repos.findTroves(labelPath,
                                   troveSpecs, searchFlavor,
                                   affinityDatabase = affinityDb,
                                   acrossLabels = acrossLabels,
                                   acrossFlavors = acrossFlavors,
                                   allowMissing = False,
                                   bestFlavor = bestFlavor,
                                   getLeaves = getLeaves,
                                   troveTypes = troveTypes)

        # do post processing on the result if necessary
        if (flavorFilter == FLAVOR_FILTER_ALL or versionFilter == VERSION_FILTER_LATEST):
            for (n,vS,fS), tups in results.iteritems():
                if not tups:
                    continue
                if versionFilter == VERSION_FILTER_LATEST:
                    # only look at latest leaves (1 per branch).
                    versionsByBranch = {}
                    for tup in tups:
                        versionsByBranch.setdefault(tup[1].branch(),
                                                    []).append(tup[1])
                    maxVersions = set(max(x) for x in versionsByBranch.values())
                    tups = [ x for x in tups if x[1] in maxVersions ]
                for (_, v, f) in tups:
                    if flavorFilter == FLAVOR_FILTER_ALL:
                        # only look at latest leaf.
                        foundMatch = False
                        for fS in origSpecs[n, vS]:
                            # FIXME: switch to stronglySatisfies
                            # in order to implement primary flavor support
                            # here at least?
                            if (fS is None) or f.satisfies(fS):
                                foundMatch = True
                                break
                        if not foundMatch:
                            continue
                    troveTups.append((n, v, f))
        else:
            troveTups.extend(itertools.chain(*results.itervalues()))
    else:
        # no troves specified, use generic fns with no names given.
        if versionFilter == VERSION_FILTER_ALL:
            queryFn = repos.getTroveVersionsByLabel
        elif versionFilter == VERSION_FILTER_LATEST:
            queryFn = repos.getTroveLeavesByLabel
        elif versionFilter == VERSION_FILTER_LEAVES:
            queryFn = repos.getTroveLeavesByLabel


        if flavorFilter == FLAVOR_FILTER_ALL:
            flavor = None
            bestFlavor = False
            affinityDb = None
        elif flavorFilter == FLAVOR_FILTER_AVAIL:
            # match affinity flavors
            # must be done client side...
            flavor = None
            bestFlavor = False
            affinityDb = None # for now turn off
        elif flavorFilter == FLAVOR_FILTER_BEST:
            # match affinity flavors
            # must be done client side...
            flavor = None
            bestFlavor = False
            affintyDb = None # XXX for now turn off.

        resultsDict = {}

        resultsDict = queryFn({'': {labelPath[0] : flavor}}, 
                               bestFlavor = bestFlavor, troveTypes=troveTypes)
        for label in labelPath[1:]:
            d = queryFn({'': {label : flavor}}, bestFlavor = bestFlavor,
                        troveTypes=troveTypes)
            _merge(resultsDict, d)

        # do post processing for VERSION_FILTER_LATEST, FLAVOR_FILTER_BEST,
        # and FLAVOR_FILTER_AVAIL
        leavesFilter = {}
        troveTups = []
        for name, versionDict in resultsDict.iteritems():
            if affinityDb:
                localFlavors = [x[2] for x in affinityDb.trovesByName(name)]
            else:
                localFlavors = []

            if versionFilter == VERSION_FILTER_LATEST:
                versionsByBranch = {}
                for version in versionDict:
                    versionsByBranch.setdefault(version.branch(),
                                                []).append(version)

                maxVersions = [ max(x) for x in versionsByBranch.itervalues() ]
                versionDict = dict((x, versionDict[x]) for x in maxVersions)

            for version, flavorList in versionDict.iteritems():
                if flavorFilter == FLAVOR_FILTER_BEST:
                    best = None
                    for systemFlavor in defaultFlavor:
                        mathing = []
                        matchScores = []
                        if localFlavors:
                            matchFlavors = [ deps.overrideFlavor(systemFlavor, x) for x in localFlavors]
                        else:
                            matchFlavors = [systemFlavor]

                        for f in flavorList:
                            scores = ( (x.score(f), f) for x in matchFlavors)
                            scores = [ x for x in scores if x[0] is not False]
                            if scores:
                                matchScores.append(max(scores))
                        if matchScores:
                            best = max(matchScores)[1]
                            break
                    if best is not None:
                        flavorList = [best]
                    else:
                        continue
                elif flavorFilter == FLAVOR_FILTER_AVAIL:
                    if localFlavors:
                        matchFlavors = []
                        for systemFlavor in defaultFlavor:
                            matchFlavors.extend(deps.overrideFlavor(systemFlavor, x) for x in localFlavors)
                    else:
                        matchFlavors = defaultFlavor
                for flavor in flavorList:
                    if flavorFilter == FLAVOR_FILTER_AVAIL:
                        found = False
                        for matchFlavor in matchFlavors:
                            if matchFlavor.satisfies(flavor):
                                found = True
                                break
                        if not found:
                            continue
                    troveTups.append((name, version, flavor))
    return sorted(troveTups)


def getTrovesByPath(repos, pathList, versionFilter, flavorFilter, labelPath,
                    defaultFlavor):
    if not pathList:
        return []

    if versionFilter == VERSION_FILTER_ALL:
        queryFn = repos.getTroveVersionsByPath
    elif versionFilter == VERSION_FILTER_LEAVES:
        queryFn = repos.getTroveLeavesByPath
    elif versionFilter == VERSION_FILTER_LATEST:
        queryFn = repos.getTroveLeavesByPath
    else:
        assert(0)

    allResults = {}
    for label in labelPath:
        try:
            results = queryFn(pathList, label)
        except errors.MethodNotSupported:
            log.debug('repository server for the %s label does not support '
                      'queries by path' %label)
            continue
        for path, tups in results.iteritems():
            allResults.setdefault(path, []).extend(tups)

    allResults = [ allResults[x] for x in pathList ]

    finalList = [ ]
    for tupList in allResults:
        if not tupList:
            continue
        source = trovesource.SimpleTroveSource(tupList)
        source.searchAsRepository()
        troveNames = set(x[0] for x in tupList)
        # no affinity when searching by path.
        results = getTrovesToDisplay(source, troveNames, [], [],
                                     versionFilter, flavorFilter, labelPath,
                                     defaultFlavor, None)
        finalList.extend(results)
    return finalList
