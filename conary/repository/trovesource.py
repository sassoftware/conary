#
# Copyright (c) 2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import itertools

from conary import trove
from conary.deps import deps
from conary.local import deptable
from conary.repository import changeset, findtrove

class AbstractTroveSource:
    """ Provides the interface necessary for performing
        findTrove operations on arbitrary sets of troves.
        As long as the subclass provides the following methods,
        findTrove will be able to search it.  You can set the 
        type of searching findTrove will default to here as 
        well.
    """

    def __init__(self):
        self._allowNoLabel = True
        self._bestFlavor = False
        self._getLeavesOnly = False

    def getTroveLeavesByLabel(self, query, bestFlavor=True):
        raise NotImplementedError

    def getTroveVersionsByLabel(self, query, bestFlavor=True):
        raise NotImplementedError

    def getTroveLeavesByBranch(self, query, bestFlavor=True):
        raise NotImplementedError

    def getTroveVersionsByBranch(self, query, bestFlavor=True):
        raise NotImplementedError

    def getTroveVersionFlavors(self, query, bestFlavor=True):
        raise NotImplementedError

    def getTroves(self, troveList, withFiles = True):
        raise NotImplementedError

    def getTrove(self, name, version, flavor, withFiles = True):
        return self.getTroves([(name, version, flavor)], withFiles)[0]

    def getTroveVersionList(self, name, withFlavors=False):
        raise NotImplementedError

    def findTroves(self, labelPath, troves, defaultFlavor=None, 
                   acrossLabels=True, acrossFlavors=True, 
                   affinityDatabase=None, allowMissing=False):
        troveFinder = findtrove.TroveFinder(self, labelPath, 
                                            defaultFlavor, acrossLabels,
                                            acrossFlavors, affinityDatabase,
                                            allowNoLabel=self._allowNoLabel,
                                            bestFlavor=self._bestFlavor,
                                            getLeaves=self._getLeavesOnly)
        return troveFinder.findTroves(troves, allowMissing)

    def findTrove(self, labelPath, (name, versionStr, flavor), 
                  defaultFlavor=None, acrossSources = True, 
                  acrossFlavors = True, affinityDatabase = None):
        res = self.findTroves(labelPath, ((name, versionStr, flavor),),
                              defaultFlavor, acrossSources, acrossFlavors,
                              affinityDatabase)
        return res[(name, versionStr, flavor)]

# constants mostly stolen from netrepos/netserver
_GET_TROVE_ALL_VERSIONS = 1
_GET_TROVE_VERY_LATEST  = 2         # latest of any flavor

_GET_TROVE_NO_FLAVOR          = 1     # no flavor info is returned
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

class SearchableTroveSource(AbstractTroveSource):
    """ A simple implementation of most of the methods needed 
        for findTrove - all of the methods are implemplemented
        in terms of trovesByName, which is left for subclasses to 
        implement.
    """

    def trovesByName(self, name):
        raise NotImplementedError

    def getTroves(self, troveList, withFiles = True):
        raise NotImplementedError

    def createChangeSet(self, jobList, withFiles = True, recurse = False,
                        withFileContents = False):
        # return changeset, and unhandled jobs
        cs = changeset.ReadOnlyChangeSet()
        return cs, jobList
    
    def __init__(self):
        self.searchAsDatabase()
        AbstractTroveSource.__init__(self)

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

    def getTroveVersionList(self, name, withFlavors=False):
        if withFlavors:
            return [ x[1:] for x in self.trovesByName(name) ]
        else:
            return [ x[1] for x in self.trovesByName(name) ]

    def _toQueryDict(self, troveList):
        d = {}
        for (n,v,f) in troveList:
            d.setdefault(n, {}).setdefault(v, []).append(f)
        return d

    def _getTrovesByType(self, troveSpecs, 
                         versionType=_GTL_VERSION_TYPE_NONE,
                         latestFilter=_GET_TROVE_ALL_VERSIONS, 
                         bestFlavor=False):
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

        if bestFlavor:
            flavorFilter = _GET_TROVE_BEST_FLAVOR
        else:
            flavorFilter = _GET_TROVE_ALL_FLAVORS
            # if any flavor query specified is not None, assume
            # that we want all _allowed_ flavors, not all 
            # flavors
            for name, versionQuery in troveSpecs.iteritems():
                for version, flavorQuery in versionQuery.iteritems():
                    if flavorQuery is not None:
                        flavorFilter = _GET_TROVE_ALLOWED_FLAVOR
                        break
                if flavorFilter == _GET_TROVE_ALLOWED_FLAVOR:
                    break


        flavorCheck = self._flavorCheck

        allTroves = {}
        for name, versionQuery in troveSpecs.iteritems():
            troves = self._toQueryDict(self.trovesByName(name))
            if not troves:
                continue
            if not versionQuery:
                allTroves[name] = troves[name]
                continue
            versionResults = {}
            for version in troves[name].iterkeys():
                if versionType == _GTL_VERSION_TYPE_LABEL:
                    theLabel = version.branch().label()
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
            for queryKey, versionList in versionResults.iteritems():
                if latestFilter == _GET_TROVE_VERY_LATEST:
                    versionList.sort()
                    versionList.reverse()
                flavorQuery = versionQuery[queryKey]
                if (flavorFilter == _GET_TROVE_ALL_FLAVORS or
                                                 flavorQuery is None):

                    if latestFilter == _GET_TROVE_VERY_LATEST:
                        versionList = versionList[:1]
                    for version in versionList:
                        vDict = allTroves.setdefault(name, {})
                        fSet = vDict.setdefault(version, set())
                        fSet.update(troves[name][version])
                else:
                    for qFlavor in flavorQuery:
                        for version in versionList:
                            flavorList = troves[name][version]
                            troveFlavors = set() 
                            if flavorCheck == _CHECK_TROVE_STRONG_FLAVOR:
                                strongFlavors = [x.toStrongFlavor() for x in flavorList]
                                flavorList = zip(strongFlavors, flavorList)
                                scores = ((x[0].score(qFlavor), x[1]) \
                                                            for x in flavorList)
                            else:
                                scores = ((qFlavor.score(x), x) for x in flavorList)
                            scores = [ x for x in scores if x[0] is not False]
                            if scores:
                                if flavorFilter == _GET_TROVE_BEST_FLAVOR:
                                    troveFlavors.add(max(scores)[1])
                                elif flavorFilter == _GET_TROVE_ALLOWED_FLAVOR:
                                    troveFlavors.update([x[1] for x in scores])
                                else:
                                    assert(false)

                        
                            if troveFlavors: 
                                vDict = allTroves.setdefault(name, {})
                                fSet = vDict.setdefault(version, set())
                                fSet.update(troveFlavors)
                                if latestFilter == _GET_TROVE_VERY_LATEST:
                                    break
        return allTroves

    def getTroveLeavesByLabel(self, troveSpecs, bestFlavor=True):
        return self._getTrovesByType(troveSpecs, _GTL_VERSION_TYPE_LABEL, 
                                 _GET_TROVE_VERY_LATEST, bestFlavor)

    def getTroveVersionsByLabel(self, troveSpecs, bestFlavor=True):
        return self._getTrovesByType(troveSpecs, _GTL_VERSION_TYPE_LABEL, 
                                     _GET_TROVE_ALL_VERSIONS, bestFlavor)

    def getTroveLeavesByBranch(self, troveSpecs, bestFlavor=True):
        """ Takes {n : { Version : [f,...]} dict """
        return self._getTrovesByType(troveSpecs, _GTL_VERSION_TYPE_BRANCH,
                                     _GET_TROVE_VERY_LATEST, bestFlavor)

    def getTroveVersionsByBranch(self, troveSpecs, bestFlavor=True):
        return self._getTrovesByType(troveSpecs, _GTL_VERSION_TYPE_BRANCH, 
                                     _GET_TROVE_ALL_VERSIONS, bestFlavor)

    def getTroveVersionFlavors(self, troveSpecs, bestFlavor=True):
        """ Takes {n : { Version : [f,...]} dict """
        return self._getTrovesByType(troveSpecs, 
                                     _GTL_VERSION_TYPE_VERSION, 
                                     _GET_TROVE_ALL_VERSIONS, 
                                     bestFlavor)

class SimpleTroveSource(SearchableTroveSource):

    def __init__(self, troveTups=[]):
        SearchableTroveSource.__init__(self)
        troveTups = list(troveTups)
        _trovesByName = {}
        for (n,v,f) in troveTups:
            _trovesByName.setdefault(n,set()).add((n,v,f))
        self._trovesByName = _trovesByName

    def trovesByName(self, name):
        return self._trovesByName.get(name, [])

    def __len__(self):
        return len(list(self))
        
    def __iter__(self):
        return itertools.chain(*self._trovesByName.itervalues())

    def addTrove(self, n, v, f):
        self._trovesByName.setdefault(n,set()).add((n,v,f))


class TroveListTroveSource(SimpleTroveSource):
    def __init__(self, source, troveTups, withDeps=False):
        SimpleTroveSource.__init__(self, troveTups)
        self.deps = {}
        self.source = source
        self.sourceTups = troveTups[:]

        foundTups = set()
        
        # recurse into the given trove tups to include all child troves
        while troveTups:
            for (n,v,f) in troveTups:
                self._trovesByName.setdefault(n, set()).add((n,v,f))
                newTroves = source.getTroves(troveTups, withFiles=False)
            foundTups.update(newTroves)
            troveTups = []
            for newTrove in newTroves:
                for tup in newTrove.iterTroveList():
                    self._trovesByName.setdefault(tup[0], set()).add(tup)
                    if tup not in foundTups:
                        troveTups.append(tup)

    def getSourceTroves(self):
        return self.getTroves(self.sourceTups)

    def getTroves(self, troveTups, withFiles=False):
        return self.source.getTroves(troveTups, withFiles)


class GroupRecipeSource(SearchableTroveSource):
    """ A TroveSource that contains all the troves in a cooking 
        (but not yet committed) recipe.  Useful for modifying a recipe
        in progress using findTrove.
    """

    def __init__(self, source, groupRecipe):
        self.searchAsDatabase()
        self.deps = {}
        self._trovesByName = {}
        self.source = source
        self.sourceTups = groupRecipe.troves

        for (n,v,f) in self.sourceTups:
            self._trovesByName.setdefault(n, []).append((n,v,f))

    def getTroves(self, troveTups, withFiles=False):
        return self.source.getTroves(troveTups, withFiles)

    def trovesByName(self, name):
        return self._trovesByName.get(name, []) 

class ReferencedTrovesSource(SearchableTroveSource):
    """ A TroveSource that only (n,v,f) pairs for troves that are
        referenced by other, installed troves.
    """
    def __init__(self, source):
        self.searchAsDatabase()
        self.source = source

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

    def __init__(self, db):
        SearchableTroveSource.__init__(self)
        self.db = db
        self.troveCsMap = {}
        self.jobMap = {}
        self.providesMap = {}
        self.csList= []
        self.invalidated = False

        self.depDb = deptable.DependencyDatabase()

    def addChangeSet(self, cs, includesFileContents = False):
        relative = []
        self.idMap = {}
        for idx, trvCs in enumerate(cs.iterNewTroveList()):
            info = (trvCs.getName(), trvCs.getNewVersion(), 
                    trvCs.getNewFlavor())
            self.providesMap.setdefault(trvCs.getProvides(), []).append(info)
            self.depDb.add(idx, trvCs.getProvides(), trvCs.getRequires())
            self.idMap[idx] = info

            if trvCs.getOldVersion() is None:
                if info in self.troveCsMap:
                    # FIXME: there is no such exception in this context
                    raise DuplicateTrove
                self.troveCsMap[info] = cs
                self.jobMap[(info[0], (None, None), info[1:], 
                             trvCs.isAbsolute())] = cs, includesFileContents
                continue


            relative.append((trvCs, info))

        self.depDb.commit()

        present = self.db.hasTroves([ (x[0].getName(), x[0].getOldVersion(),
                                       x[0].getOldFlavor()) for x in relative ])
        for (trvCs, info), isPresent in itertools.izip(relative, present):
            if not isPresent:
                # FIXME: there is no such exception in this context
                raise MissingTrove
            
            if info in self.troveCsMap:
                # FIXME: there is no such exception in this context
                raise DuplicateTrove
            self.troveCsMap[info] = cs
            self.jobMap[(info[0], (trvCs.getOldVersion(), 
                                   trvCs.getOldFlavor()), 
                         info[1:], trvCs.isAbsolute())] = \
                                            (cs, includesFileContents)

        self.csList.append(cs)

    def trovesByName(self, name):
        l = []
        for info in self.troveCsMap:
            if info[0] == name:
                l.append(info)

        return l

    def getTroves(self, troveList, withFiles = True):
        assert(not withFiles)
        assert(not self.invalidated)
        retList = []

        for info in troveList:
            trvCs = self.troveCsMap[info].getNewTroveVersion(*info)
            if trvCs.getOldVersion() is None:
		newTrove = trove.Trove(trvCs.getName(), trvCs.getNewVersion(),
                                       trvCs.getNewFlavor(), 
                                       trvCs.getChangeLog())
            else:
                newTrove = self.db.getTrove(trvCs.getName(), 
                                            trvCs.getOldVersion(),
                                            trvCs.getOldFlavor())

            newTrove.applyChangeSet(trvCs, skipIntegrityChecks = not withFiles)
            retList.append(newTrove)

        return retList

    def hasTroves(self, troveList):
        assert(not self.invalidated)
        return [ x in self.troveCsMap for x in troveList ]

    def resolveDependencies(self, label, depList):
        suggMap = self.depDb.resolve(label, depList)
        for depSet, solListList in suggMap.iteritems():
            newSolListList = []
            for solList in solListList:
                newSolListList.append([ self.idMap[x] for x in solList ])

            suggMap[depSet] = newSolListList
        return suggMap
            
    def createChangeSet(self, jobList, withFiles = True, recurse = False,
                        withFileContents = False, useDatabase = True):
        # Returns the changeset plus a remainder list of the bits it
        # couldn't do
        def _findTroveObj(availSet, (name, version, flavor)):
            info = (name, version, flavor)
            (inDb, fromCs) = availSet[info]

            if fromCs:
                [ trv ] = self.getTroves([info], withFiles = False)
            elif inDb:
                # XXX this should be parallelized...
                trv = self.db.getTrove(withFiles = False, *info)
            else:
                trv = None

            return trv

        assert(not self.invalidated)
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

        if useDatabase:
            inDatabase = self.db.hasTroves(troves)
        else:
            inDatabase = [ False ] * len(troves)
            
        asChangeset = [ info in self.troveCsMap for info in troves ]
        trovesAvailable = dict((x[0], (x[1], x[2])) for x in 
                            itertools.izip(troves, inDatabase, asChangeset))

        cs = changeset.ReadOnlyChangeSet()
        remainder = []

        # Track jobs we need to go get directly from change sets later, and
        # jobs which need to be rooted relative to a change set.
        changeSetJobs = set()
        needsRooting = []
        
        jobFromCs = set()

        for job in jobList:
            oldInfo = (job[0], job[1][0], job[1][1])
            newInfo = (job[0], job[2][0], job[2][1])

            if newInfo[1] is None:
                cs.oldTrove(*oldInfo)
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

            cs.newTrove(newTrv.diff(oldTrv)[0])

        # we can't combine these (yet; we should work on that)
        assert(not changeSetJobs or not needsRooting)

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
            # we can't root changesets multiple times
            self.invalidated = True
            for subCs in self.csList:
                if subCs.isAbsolute():
                    subCs.rootChangeSet(self.db, rootMap)

        # assemble jobs directly from changesets and update those changesets
        # to not have jobs we don't need
        if changeSetJobs:
            # this trick only works once
            self.invalidated = True
            for subCs in self.csList:
                toDel = []
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

                    if job not in changeSetJobs:
                        toDel.append((job[0], job[2][0], job[2][1]))

                for info in toDel:
                    subCs.delNewTrove(*info)

                # we generate our own deletions. we don't need to get them
                # from here
                for info in subCs.getOldTroveList():
                    subCs.delOldTrove(*info)

                cs.merge(subCs)

        return (cs, remainder)


class TroveSourceStack(SearchableTroveSource):

    def __init__(self, *sources):
        self.sources = []
        for source in sources:
            if isinstance(source, TroveSourceStack):
                for subSource in source.iterSources():
                    self.addSource(source)
            else:
                self.addSource(source)

    def addSource(self, source):
        if source is not None and source not in self.sources:
            self.sources.append(source)

    def hasSource(self, source):
        return source in self.sources

    def iterSources(self):
        for source in self.sources:
            yield source

    def copy(self):
        return TroveSourceStack(*self.sources)

    def __repr__(self):
        return 'TroveSourceStack(%s)' % (', '.join(repr(x) for x in self.sources))

    def trovesByName(self, name):
        return list(chain(*(x.trovesByName(name) for x in self.sources)))
        
    def getTroves(self, troveList, withFiles = True):
        # XXX I don't have a test case that tests this yet
        troveList = list(enumerate(troveList)) # make a copy and add indexes
        numTroves = len(troveList)
        results = [None] * numTroves

        for source in self.sources:
            newTroveList = []
            newIndexes = []
            troves = source.getTroves([x[0] for x in troveList], 
                                      withFiles=withFiles)
            for ((index, troveTup), trove) in intertools.izip(troveList, 
                                                              troves):
                if trove is None:
                    newTroveList.append((index, troveTup))
                else:
                    results[index] = trove
                    
    def findTroves(self, labelPath, troves, defaultFlavor=None, 
                   acrossLabels=True, acrossFlavors=True, 
                   affinityDatabase=None, allowMissing=False):

        troves = list(troveSpecs)

        results = {}
        troveFinder = findtrove.TroveFinder(self, labelPath, 
                                            defaultFlavor, acrossLabels,
                                            acrossFlavors, affinityDatabase,
                                            allowNoLabel=self._allowNoLabel,
                                            bestFlavor=self._bestFlavor,
                                            getLeaves=self._getLeavesOnly)

        for source in self.sources[:-1]:
            troveFinder.setTroveSource(source)

            foundTroves = troveFinder.findTroves(troveSpecs, allowMissing=True)

            newTroveSpecs = []
            for troveSpec in troveSpecs:
                if troveSpec in foundTroves:
                    results[troveSpec] = foundTroves[troveSpec]
                else:
                    newTroveSpecs.append(troveSpec)

            troveSpecs = newTroveSpecs

        troveFinder.setTroveSource(self.sources[-1])

        results.update(troveFinder.findTroves(troveSpecs, 
                                              allowMissing=allowMissing))
        return results

    def resolveDependencies(self, label, depList):
        results = {}

        depList = set(depList)

        for source in self.sources:
            if not depList:
                break

            sugg = source.resolveDependencies(label, depList)
            for depSet, troves in sugg.iteritems():
                depList.remove(depSet)
                results[depSet] = troves
        
        return results

    def createChangeSet(self, jobList, withFiles = True, recurse = False,
                        withFileContents = False):

        cs = changeset.ReadOnlyChangeSet()

        for source in self.sources:
            if not jobList:
                break

            res = source.createChangeSet(jobList, 
                                       withFiles = withFiles,
                                       withFileContents = withFileContents,
                                       recurse = recurse)
            if isinstance(res, (list, tuple)):
                newCs, jobList = res
            else: 
                newCs, jobList = res, None
            cs.merge(newCs)

        return cs, jobList

def stack(source1, source2):
    """ create a trove source that will search first source1, then source2 """

    if source1 is source2:
        return source1

    if isinstance(source1, TroveSourceStack):
        if source1.hasSource(source2):
            return source1
        source1.copy().addSource(source2)
        return source1
    
    return TroveSourceStack(source1, source2)
