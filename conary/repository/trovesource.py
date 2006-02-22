#
# Copyright (c) 2005-2006 rPath, Inc.
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

from conary import files
from conary import trove
from conary.deps import deps
from conary.local import deptable
from conary.repository import changeset, errors, findtrove

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

    def requiresLabelPath(self):
        return not self._allowNoLabel

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

    def resolveDependencies(self, label, depList):
        return {}

    def hasTroves(self, troveList):
        raise NotImplementedError

    def getTrove(self, name, version, flavor, withFiles = True):
        trv = self.getTroves([(name, version, flavor)], withFiles)[0]
        if trv is None:
            raise errors.TroveMissing(name, version)
        else:
            return trv
            
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

    def iterFilesInTrove(self, n, v, f, sortByPath=False, withFiles=False):
        raise NotImplementedError

    def walkTroveSet(self, trove, ignoreMissing = True,
                     withFiles=True):
	"""
	Generator returns all of the troves included by trove, including
	trove itself.
	"""
	yield trove
	seen = { trove.getName() : [ (trove.getVersion(),
				      trove.getFlavor()) ] }

	troveList = [x for x in trove.iterTroveList(strongRefs=True)]

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

                yield trv

                troveList += [ x for x in trv.iterTroveList(strongRefs=True) ]
	    except errors.TroveMissing:
		if not ignoreMissing:
		    raise
	    except KeyError:
		if not ignoreMissing:
		    raise



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

    def iterAllTroveNames(self):
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
                                    assert(0)

                        
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

    def iterAllTroveNames(self):
        return iter(self._trovesByName)

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
                for tup in newTrove.iterTroveList(strongRefs=True,
                                                  weakRefs=True):
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

    def __init__(self, db, storeDeps=False):
        SearchableTroveSource.__init__(self)
        self.db = db
        self.troveCsMap = {}
        self.jobMap = {}
        self.providesMap = {}
        self.csList= []
        self.invalidated = False
        self.erasuresMap = {}
        self.rooted = {}
        self.idMap = {}
        self.storeDeps = storeDeps

        if storeDeps:
            self.depDb = deptable.DependencyDatabase()

    def addChangeSet(self, cs, includesFileContents = False):
        relative = []

        if not self.idMap:
            startId = 0
        else:
            startId = max(self.idMap) + 1

        for idx, trvCs in enumerate(cs.iterNewTroveList()):
            id = idx + startId
            info = (trvCs.getName(), trvCs.getNewVersion(), 
                    trvCs.getNewFlavor())
            self.providesMap.setdefault(trvCs.getProvides(), []).append(info)
            if self.storeDeps:
                self.depDb.add(startId, trvCs.getProvides(), 
                               trvCs.getRequires())
            self.idMap[startId] = info

            if trvCs.getOldVersion() is None:
                if info in self.troveCsMap:
                    # FIXME: there is no such exception in this context
                    raise DuplicateTrove
                self.troveCsMap[info] = cs
                self.jobMap[(info[0], (None, None), info[1:], 
                             trvCs.isAbsolute())] = cs, includesFileContents
                continue


            relative.append((trvCs, info))

        for info in cs.getOldTroveList():
            self.erasuresMap[info] = cs
            
        if self.storeDeps:
            self.depDb.commit()

        if relative:
            for (trvCs, info) in relative:
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

    def iterAllTroveNames(self):
        troveNames = set()
        for name, _, _ in self.troveCsMap:
            troveNames.add(name)
        return iter(troveNames)

    def iterFilesInTrove(self, n, v, f, sortByPath=False, withFiles=False):
        try:
            cs = self.troveCsMap[n,v,f]
        except KeyError:
            raise errors.TroveMissing(n, v)

        trvCs = cs.getNewTroveVersion(n,v,f)
        fileList = trvCs.getNewFileList()
        if not fileList:    
            return

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
            fileObj = files.ThawFile(change, pathId)
            yield pathId, path, fileId, version, fileObj

    def getFileVersion(self, pathId, fildId, version):
        # TODO: implement getFileVersion for changeset source
        raise KeyError

    def getTroves(self, troveList, withFiles = True):
        assert(not self.invalidated)
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
        assert(not self.invalidated)
        return [ x in self.troveCsMap for x in troveList ]

    def getChangeSet(self, job):
        name, (oldVer, oldFla), (newVer, newFla) = job[:3]
        if newVer:
            info = (name, newVer, newFla)
            return self.troveCsMap[info]
        else:
            info = (name, oldVer, oldFla)
            return self.erasuresMap[info]

    def resolveDependencies(self, label, depList):
        assert(self.storeDeps)
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
            for info in rootMap:
                assert(info not in self.rooted)

            for subCs in self.csList:
                if subCs.isAbsolute():
                    subCs.rootChangeSet(self.db, rootMap)

            self.rooted.update(rootMap)

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

    def requiresLabelPath(self):
        for source in self.iterSources():
            if source.requiresLabelPath():
                return True
        return False

    def addSource(self, source):
        if source is not None and source not in self:
            self.sources.append(source)

    def insertSource(self, source, idx=0):
        if source is not None and source not in self:
            self.sources.insert(idx, source)
        
    def hasSource(self, source):
        return source in self

    def hasTroves(self, troveList):
        results = [False] * len(troveList)

        troveList = list(enumerate(troveList)) 

        for source in self.sources:
            newTroveList = []
            hasTroves = source.hasTroves([x[1] for x in troveList])
            if isinstance(hasTroves, list):
                hasTroves = dict(itertools.izip([x[1] for x in troveList], 
                                                hasTroves))

            for (index, troveTup) in troveList:
                if not hasTroves[troveTup]:
                    newTroveList.append((index, troveTup))
                else:
                    results[index] = True
        return results
        

    def iterSources(self):
        for source in self.sources:
            yield source

    def copy(self):
        return TroveSourceStack(*self.sources)

    def __repr__(self):
        return 'TroveSourceStack(%s)' % (', '.join(repr(x) for x in self.sources))

    def __contains__(self, newSource):
        # don't use == because some sources may define ==
        for source in self.sources:
            if source is newSource:
                return True
        return False

    def trovesByName(self, name):
        return list(chain(*(x.trovesByName(name) for x in self.sources)))
        
    def getTroves(self, troveList, withFiles = True):
        troveList = list(enumerate(troveList)) # make a copy and add indexes
        numTroves = len(troveList)
        results = [None] * numTroves

        for source in self.sources:
            newTroveList = []
            newIndexes = []
            troves = source.getTroves([x[1] for x in troveList], 
                                      withFiles=withFiles)
            for ((index, troveTup), trove) in itertools.izip(troveList, troves):
                if trove is None:
                    newTroveList.append((index, troveTup))
                else:
                    results[index] = trove
            troveList = newTroveList
        return results
                    
    def findTroves(self, labelPath, troveSpecs, defaultFlavor=None, 
                   acrossLabels=True, acrossFlavors=True, 
                   affinityDatabase=None, allowMissing=False):
        troveSpecs = list(troveSpecs)

        results = {}

        someRequireLabel = False
        for source in self.sources:
            if not source._allowNoLabel:
                assert(labelPath)
                someRequireLabel = True

                

        for source in self.sources[:-1]:
            # FIXME: it should be possible to reuse the trove finder
            # but the bestFlavr and getLeaves data changes per source
            # and is passed into several TroveFinder sub objects.  
            # TroveFinder should be cleaned up
            if someRequireLabel and source._allowNoLabel:
                sourceLabelPath = None
            else:
                sourceLabelPath = labelPath
                
            troveFinder = findtrove.TroveFinder(source, sourceLabelPath, 
                                            defaultFlavor, acrossLabels,
                                            acrossFlavors, affinityDatabase,
                                            allowNoLabel=source._allowNoLabel,
                                            bestFlavor=source._bestFlavor,
                                            getLeaves=source._getLeavesOnly)

            foundTroves = troveFinder.findTroves(troveSpecs, allowMissing=True)

            newTroveSpecs = []
            for troveSpec in troveSpecs:
                if troveSpec in foundTroves:
                    results[troveSpec] = foundTroves[troveSpec]
                else:
                    newTroveSpecs.append(troveSpec)

            troveSpecs = newTroveSpecs

        source = self.sources[-1]

        if someRequireLabel and source._allowNoLabel:
            sourceLabelPath = None
        else:
            sourceLabelPath = labelPath
         
        troveFinder = findtrove.TroveFinder(source, labelPath, 
                                        defaultFlavor, acrossLabels,
                                        acrossFlavors, affinityDatabase,
                                        allowNoLabel=source._allowNoLabel,
                                        bestFlavor=source._bestFlavor,
                                        getLeaves=source._getLeavesOnly)



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

            try:
                res = source.createChangeSet(jobList, 
                                           withFiles = withFiles,
                                           withFileContents = withFileContents,
                                           recurse = recurse)
            except errors.OpenError:
                res = changeset.ReadOnlyChangeSet(), jobList
            if isinstance(res, (list, tuple)):
                newCs, jobList = res
            else: 
                newCs, jobList = res, None
            cs.merge(newCs)

        return cs, jobList

    def getFileVersion(self, pathId, fileId, version):
        for source in self.sources:
            try:
                return source.getFileVersion(pathId, fileId, version)
            # FIXME: there should be a better error for this
            except KeyError:
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

def stack(*sources):
    """ create a trove source that will search first source1, then source2 """
    if len(sources) > 2:
        return stack(sources[0], stack(*sources[1:]))
    elif len(sources) == 1:
        return sources[1]
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
        if source2.hasSource(source1):
            return source2
        source2 = source2.copy()
        source2.insertSource(source1)
        return source2
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

    def getTroveVersionList(self, name, withFlavors=False):
        raise NotImplementedError

    def iterFilesInJob(self, job, sortByPath=False, withFiles=False,
                                                    withOldFiles=False):
        raise NotImplementedError


class JobSource(AbstractJobSource):

    def __init__(self, newTroveSource, oldTroveSource):
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
        return self.allTroves.findTroves(*args, **kw)

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
        oldTroves = self.oldTroveList.findTroves(None, oldTroves)

        for (n, (oldVS, oldFS), (newVS, newFS), isAbs) in jobList:
            results = []
            if isAbs:
                newTups = newTroves.get((n, newVS, newFS), None)
                oldTups = oldTroves[n, None, None]
                oldTups.append((n, None, None))
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


                if oldFiles or modFiles:
                    oldTrove = self.oldTroveSource.getTrove(n, oldVer, oldFla,
                                                            withFiles=True)

                if newFiles or modFiles:
                    newTrove = self.newTroveSource.getTrove(n, newVer, newFla,
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
