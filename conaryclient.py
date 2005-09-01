#
# Copyright (c) 2004-2005 rPath, Inc.
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
import os
import pickle

#conary imports
from callbacks import UpdateCallback
import conarycfg
import deps
import versions
import trove
import metadata
from deps import deps
from lib import util
from local import database
from repository import changeset
from repository import repository
from repository import changeset
from repository import trovesource
from repository.netclient import NetworkRepositoryClient

class ClientError(Exception):
    """Base class for client errors"""

class TroveNotFound(Exception):
    def __init__(self, troveName):
        self.troveName = troveName
        
    def __str__(self):
        return "trove not found: %s" % self.troveName

class UpdateError(ClientError):
    """Base class for update errors"""
    def display(self):
        return str(self)

class DependencyFailure(UpdateError):
    """ Base class for dependency failures """
    pass

class DepResolutionFailure(DependencyFailure):
    """ Unable to resolve dependencies """
    def __init__(self, failures):
        self.failures = failures

    def getFailures(self):
        return self.failures

    def __str__(self):
        res = ["The following dependencies could not be resolved:"]
        for (troveName, depSet) in self.failures:
            res.append("    %s:\n\t%s" %  \
                       (troveName, "\n\t".join(str(depSet).split("\n"))))
        return '\n'.join(res)

class EraseDepFailure(DepResolutionFailure):
    """ Unable to resolve dependencies due to erase """
    def getFailures(self):
        return self.failures

    def __str__(self):
        res = []
        res.append("Troves being removed create unresolved dependencies:")
        for (troveName, depSet) in self.failures:
            res.append("    %s:\n\t%s" %  \
                        (troveName, "\n\t".join(str(depSet).split("\n"))))
        return '\n'.join(res)

class NeededTrovesFailure(DependencyFailure):
    """ Dependencies needed and resolve wasn't used """
    def __init__(self, suggMap):
         self.suggMap = suggMap

    def getSuggestions(self):
        return self.suggMap

    def __str__(self):
        res = []
        res.append("Additional troves are needed:")
        for (req, suggList) in self.suggMap.iteritems():
            res.append("    %s -> %s" % \
              (req, " ".join(["%s(%s)" % 
              (x[0], x[1].trailingRevision().asString()) for x in suggList])))
        return '\n'.join(res)

class VersionSuppliedError(UpdateError):
    def __str__(self):
        return "version should not be specified when a Conary change set " \
               "is being installed"

class NoNewTrovesError(UpdateError):
    def __str__(self):
        return "no new troves were found"

class UpdateChangeSet(changeset.ReadOnlyChangeSet):

    _streamDict = changeset.ReadOnlyChangeSet._streamDict

    def merge(self, cs, src = None):
        changeset.ReadOnlyChangeSet.merge(self, cs)
        if isinstance(cs, UpdateChangeSet):
            self.contents += cs.contents
        else:
            self.contents.append(src)
        self.empty = False

    def __init__(self, *args):
        changeset.ReadOnlyChangeSet.__init__(self, *args)
        self.contents = []
        self.empty = True

class ConaryClient:
    """
    ConaryClient is a high-level class to some useful Conary operations,
    including trove updates and erases.
    """
    def __init__(self, cfg = None):
        """
        @param cfg: a custom L{conarycfg.ConaryConfiguration object}.
                    If None, the standard Conary configuration is loaded
                    from /etc/conaryrc, ~/.conaryrc, and ./conaryrc.
        @type cfg: L{conarycfg.ConaryConfiguration}
        """
        if cfg == None:
            cfg = conarycfg.ConaryConfiguration()
            cfg.initializeFlavors()
        
        cfg.installLabel = cfg.installLabelPath[0]
        self.cfg = cfg
        self.db = database.Database(cfg.root, cfg.dbPath)
        self.repos = NetworkRepositoryClient(cfg.repositoryMap,
                                             localRepository = self.db)

    def _rootChangeSet(self, cs, keepExisting = False):
	troveList = ((x.getName(), x.getNewVersion(), x.getNewFlavor()) 
                     for x in cs.iterNewTroveList())

	if keepExisting:
	    outdated = None
	else:
	    # this ignores eraseList, just like we do when trove names
	    # are specified
	    outdated, eraseList = self.db.outdatedTroves(troveList)

	    for key, tup in outdated.items():
		outdated[key] = tup[1:3]

	cs.rootChangeSet(self.db, outdated)

    def _resolveDependencies(self, cs, uJob, keepExisting = None, 
                             depsRecurse = True, split = False,
                             resolve = True):

        def _selectResolutionTrove(troveTups, installFlavor, affFlavorDict):
            """ determine which of the given set of troveTups is the 
                best choice for installing on this system.  Because the
                repository didn't try to determine which flavors are best for 
                our system, we have to filter the troves locally.  
            """
            # we filter the troves in the following ways:
            # 1. remove trove tups that don't match this installFlavor
            #    (after modifying the flavor by any affinity flavor found
            #     in an installed trove by the same name)
            # 2. filter so that only the latest version of a trove is left
            #    for each name,branch pair. (this ensures that a really old
            #    version of a trove doesn't get preferred over a new one 
            #    just because its got a better flavor)
            # 3. pick the best flavor out of the remaining

            flavoredList = []

            for troveTup in troveTups:
                f = installFlavor.copy()
                affFlavors = affFlavorDict[troveTup[0]]
                if affFlavors:
                    affFlavor = affFlavors[0][2]
                    flavorsMatch = True
                    for newF in [x[2] for x in affFlavors[1:]]:
                        if newF != affFlavor:
                            flavorsMatch = False
                            break
                    if flavorsMatch:
                        f.union(affFlavor,
                                mergeType=deps.DEP_MERGE_TYPE_PREFS)

                flavoredList.append((f, troveTup))

            trovesByNB = {}
            for installFlavor, (n,v,f) in flavoredList:
                b = v.branch()
                myTimeStamp = v.timeStamps()[-1]
                myScore = installFlavor.score(f)
                if myScore is False:
                    continue

                if (n,b) in trovesByNB:
                    curScore, curTimeStamp, curTup = trovesByNB[n,b]
                    if curTimeStamp > myTimeStamp:
                        continue
                    if curTimeStamp == myTimeStamp:
                        if myScore < curScore:
                            continue

                trovesByNB[n,b] = (myScore, myTimeStamp, (n,v,f))

            scoredList = sorted(trovesByNB.itervalues())
            if not scoredList:
                return None
            else:
                return scoredList[-1][-1]


        pathIdx = 0
        foundSuggestions = False
        (depList, cannotResolve, changeSetList) = \
                        self.db.depCheck(cs, findOrdering = split)
        suggMap = {}
        lastCheck = []

        if not resolve:
            depList = []
            cannotResolve = []

        while depList:
            nextCheck = [ x[1] for x in depList ]
            if nextCheck == lastCheck:
                # if we didn't resolve anything last time, so we're
                # checking the exact same set of dependencies  --
                # just give up
                sugg = {}
            else:
                sugg = self.repos.resolveDependencies(
                                self.cfg.installLabelPath[pathIdx], 
                                nextCheck)
                lastCheck = nextCheck

            troves = set()
            if sugg:
                for (troveName, depSet) in depList:
                    if sugg.has_key(depSet):
                        suggList = set()
                        for choiceList in sugg[depSet]:
                            troveNames = set(x[0] for x in choiceList)

                            if keepExisting:
                                affTroveDict = dict((x, []) for x in troveNames)
                            else:
                                affTroveDict = \
                                    dict((x, self.db.trovesByName(x))
                                                      for x in troveNames)

                            # iterate over flavorpath -- use suggestions 
                            # from first flavor on flavorpath that gets a match 
                            for installFlavor in self.cfg.flavor:
                                choice = _selectResolutionTrove(choiceList, 
                                                                installFlavor,
                                                                affTroveDict)
                                                                
                                if choice:
                                    suggList.add(choice)
                                    l = suggMap.setdefault(troveName, set())
                                    l.add(choice)
                                    break

			troves.update([ (x[0], (None, None), x[1:], True) 
                                        for x in suggList ])

                # if we've found good suggestions, merge in those troves
                if troves:
                    newCs = self._updateChangeSet(troves, uJob,
                                               keepExisting = keepExisting)[0]
                    cs.merge(newCs)

                    (depList, cannotResolve, changeSetList) = \
                                    self.db.depCheck(cs, findOrdering = split)

            if troves and depsRecurse:
                pathIdx = 0
                foundSuggestions = False
            else:
                pathIdx += 1
                lastCheck = []
                if troves:
                    foundSuggestions = True
                if pathIdx == len(self.cfg.installLabelPath):
                    if not foundSuggestions or not depsRecurse:
                        return (cs, depList, suggMap, cannotResolve,
                                changeSetList)
                    pathIdx = 0
                    foundSuggestions = False

        return (cs, depList, suggMap, cannotResolve, changeSetList)

    def _processRedirects(self, jobSet, troveSource, recurse):
        # this returns redirectHack, which maps targets of redirections
        # to the sources of those redirections

        # Looks for redirects in the change set, and returns a list of
        # troves which need to be included in the update. 
        troveSet = {}
        redirectHack = {}

        toDoList = list(jobSet)

        # We only have to look through primaries. Troves which aren't
        # primaries can't be redirects because collections cannot include
        # redirects. Nice, huh?
        while toDoList:
            job = toDoList.pop()
            
            (name, (oldVersion, oldFlavor), (newVersion, newFlavor),
                isAbsolute) = job

            if newVersion is None:
                # skip erasure
                continue

            trv = troveSource.getTrove(name, newVersion, newFlavor, 
                                       withFiles = False)

            if not trv.isRedirect():
                continue

            if not recurse:
                raise UpdateError,  "Redirect found with --no-recurse set"

            item = (name, newVersion, newFlavor)

            isPrimary = job in jobSet
            if isPrimary: 
                # The redirection is a primary. Remove it.
                jobSet.remove(job)

            targets = []
            for (subName, subVersion, subFlavor) in trv.iterTroveList():
                if (":" not in subName and ":" not in name) or \
                   (":"     in subName and ":"     in name):
                    l = redirectHack.setdefault((subName, subVersion,
                                                 subFlavor), [])
                    l.append(item)
                    targets.append((subName, subVersion, subFlavor))
                else:
                    toDoList.append((subName, (None, None), 
                                              (subVersion, subFlavor), True))

            if isPrimary:
                for target in targets:
                    jobSet.add((subName, (None, None), (subVersion, subFlavor),
                                True))

        for l in redirectHack.itervalues():
	    outdated, eraseList = self.db.outdatedTroves(l)
            del l[:]
            for (name, newVersion, newFlavor), \
                  (oldName, oldVersion, oldFlavor) in outdated.iteritems():
                if oldVersion is not None:
                    l.append((oldName, oldVersion, oldFlavor))

        return redirectHack

    def _mergeGroupChanges(self, primaryJobList, troveSource, uJob, 
                           redirectHack, keepExisting, recurse, ineligible):

        def _newBase(newTrv):
            """
            This creates three different troves. Read carefully or it's
            really confusing:

                1. The local idea of what version of this trove is
                   already installed.
                2. A pristine version of the old local version. The diff
                   between pristine and local modifies the changeset
                   to the new version. This lets us prevent some troves
                   from being installed.
                3. The old version of the local trove. The diff between
                   this and local is used to create the job. This allows
                   us to update other troves currently installed on the
                   system.
            """

            localTrv = newTrv.copy()
            pristineTrv = newTrv.copy()
            oldTrv = trove.Trove(localTrv.getName(), localTrv.getVersion(),
                                 localTrv.getFlavor(), None)
                                        
            delList = []
            for info in localTrv.iterTroveList():
                # off by default
                if not localTrv.includeTroveByDefault(*info):
                    delList.append(info)
                    continue

                # in excludeTroves
                for reStr, regExp in self.cfg.excludeTroves:
                    match = False
                    if regExp.match(info[0]):
                        delList.append(info)
                        match = True
                        break

                    if match: continue

                # if this is the target of a redirection, make sure we have
                # the source of that redirection installed
                if info in redirectHack:
                    l = redirectHack[info]
                    present = self.db.hasTroves(l)
                    if sum(present) == 0:
                        # sum of booleans -- tee-hee
                        delList.append(info)
                        break

            for info in delList:
                localTrv.delTrove(*(info + (False,)))

            return (oldTrv, pristineTrv, localTrv)

        def _alreadyInstalled(trv):
            troveInfo = [ x for x in newTrv.iterTroveList() ]
            present = self.db.hasTroves(troveInfo)
            r = [ info for info,present in itertools.izip(troveInfo, present)
                     if present ]

            return dict.fromkeys(r)

        def _lockedList(neededList):
            l = [ (x[0], x[1], x[3]) for x in neededList if x[1] is not None ]
            l.reverse()
            lockList = self.db.trovesAreLocked(l)
            r = []
            for item in neededList:
                if item[1] is not None:
                    r.append(lockList.pop())
                else:
                    r.append(False)

            return r

	def _findErasures(primaryErases, newJob, referencedTroves, 
                          recurse):
	    # each node is a ((name, version, flavor), state, edgeList
	    #		       fromUpdate)
	    # state is ERASE, KEEP, or UNKNOWN
	    # fromUpdate is True if erasing this node was suggested by
	    # a trvCs in the newJob (an update, not an erase). We need
	    # to track this to know what's being removed, but we don't
	    # need to cause them to be removed.
	    nodeList = []
	    nodeIdx = {}
	    ERASE = 1
	    KEEP = 2
	    UNKNOWN = 3

	    troveList = []

            # Make sure troves which the changeset thinks should be removed
            # get considered for removal. Ones which need to be removed
            # for a new trove to be installed are guaranteed to be removed.
            #
	    # Locks for updated troves are handled when the update trvCs
	    # is checked; no need to check it again here
            oldTroves = [ ((job[0], job[1][0], job[1][1]), True, ERASE) for
                                job in newJob
                                if job[1][0] is not None and 
                                   job[2][0] is not None ]
            eraseList = [ (job[0], job[1][0], job[1][1]) for
                                job in newJob
                                if job[1][0] is not None and 
                                   job[2][0] is None ]
            present = self.db.hasTroves(eraseList)
            oldTroves += [ (info, False, UNKNOWN) for info, isPresent in
                                itertools.izip(eraseList, present)
                                if isPresent ]

            for info, fromUpdate, state in oldTroves:
                if info in nodeIdx:
                    # the node is marked to erase multiple times in
                    # newJob! 
                    idx = nodeIdx[info]
                    otherState = nodeList[idx][1]
                    assert(state is UNKNOWN or otherState is UNKNOWN)
                    if otherState is UNKNOWN:
                        nodeList[idx] = [ (info, state, [], fromUpdate) ]
                    continue

                nodeIdx[info] = len(nodeList)
                nodeList.append([ info, state, [], fromUpdate ])

		if info[0].startswith('fileset-') or info[0].find(":") != -1:
		    trv = None
		else:
		    trv = self.db.getTrove(info[0], info[1], info[2], 
					   pristine = False)
		troveList.append((info, trv, None))

            del oldTroves, present, eraseList

            # primary troves need to be set to force erase (which may
            # not be done by the above logic!)
            for info in primaryErases:
                nodeList[nodeIdx[info]][1] = ERASE

	    while troveList:
		info, trv, fromTrove = troveList.pop()

		if info not in nodeIdx:
		    nodeId = len(nodeList)
		    nodeIdx[info] = nodeId
		    nodeList.append([info, UNKNOWN, [], False])
		else:
		    nodeId = nodeIdx[info]

		if not trv or not recurse:
		    continue

		refTroveInfo = [ x for x in trv.iterTroveList() ]
		present = self.db.hasTroves(refTroveInfo)
		locked = self.db.trovesAreLocked(refTroveInfo)
		areContainers = [ not(x[0].startswith('fileset-') or 
				    x[0].find(":") != -1)
				    for x in refTroveInfo ]

		contList = []
		for (subInfo, isPresent, isLocked, isContainer) in \
			itertools.izip(refTroveInfo, present, locked, 
				       areContainers):
		    if not isPresent or isLocked: continue
		    if not isContainer:
			troveList.append((subInfo, None, nodeId))
		    else:   
			contList.append(subInfo)

		trvs = self.db.getTroves(contList, pristine = False)
		troveList += [ (info, trv, nodeId) for info, trv in
				    itertools.izip(contList, trvs) ]

	    needParents = [ (nodeId, info) for nodeId, (info, state, edges,
                                                        alreadyHandled)
				in enumerate(nodeList) if state == UNKNOWN ]
	    while needParents:
		containers = self.db.getTroveContainers(x[1] for x in needParents)
                newNeedParents = []
		for (nodeId, nodeInfo), containerList in \
				itertools.izip(needParents, containers):
		    for containerInfo in containerList:
			if containerInfo in nodeIdx:
                            containerId = nodeIdx[containerInfo]
			    nodeList[containerId][2].append(nodeId)
			else:
			    containerId = len(nodeList)
			    nodeIdx[containerInfo] = containerId
			    nodeList.append([ containerInfo, KEEP, [ nodeId ],
                                              False])
			    newNeedParents.append((containerId, containerInfo))
                needParents = newNeedParents

            # don't erase nodes which are referenced by troves we're about
            # to install unless there is a really good reason
            for info in referencedTroves:
                if info in nodeIdx:
                    node = nodeList[nodeIdx[info]]
                    if node[1] == UNKNOWN:
                        node[1] = KEEP

	    seen = [ False ] * len(nodeList)
            # DFS to mark troves as KEEP
            keepNodes = [ nodeId for nodeId, node in enumerate(nodeList)
                                if node[1] == KEEP ]
            while keepNodes:
                nodeId = keepNodes.pop()
                if seen[nodeId]: continue
                seen[nodeId] = True
                nodeList[nodeId][1] = KEEP
                keepNodes += nodeList[nodeId][2] 

            # anything which isn't to KEEP is to erase, but skip those which
            # are already being removed by a trvCs
            eraseList = ((x[0][0], (x[0][1], x[0][2]), (None, None), False)
                         for x in nodeList if x[1] != KEEP and not x[3])

            return set(eraseList)

        def _removeDuplicateErasures(jobSet):
            outdated = {}
            for job in jobSet:
                if job[2][0] is not None:
                    l = outdated.setdefault((job[0], job[1][0], job[1][1]), [])
                    l.append(job)

            inelligible = []
            newItems = []
            toRemove = set()
            for old, l in outdated.iteritems():
                if len(l) == 1: 
                    inelligible.append(old)
                else:
                    newItems += [ (x[0], x[2][0], x[2][1]) for x in l ]
                    toRemove.update(set(l))

            if not newItems:
                return

            jobSet.difference_update(toRemove)

            # Everything left in outdated conflicts with itself. we'll
            # let outdated sort things out.
            outdated, eraseList = self.db.outdatedTroves(newItems, inelligible)
            needed = []
            for newInfo, oldInfo in outdated.iteritems():
                jobSet.add((newInfo[0], oldInfo[1:], newInfo[1:], False))

        # def _mergeGroupChanges -- main body begins here
            
        keepList = []
        erasePrimaryList = []
        toOutdate = set()
        newJob = set()

        for job in primaryJobList:
            if job[2][0] is not None:
                item = (job[0], job[2][0], job[2][1])
            
                if job[1][0] is None:
                    toOutdate.add(item)
                keepList.append(job)
            else:
                item = (job[0], job[1][0], job[1][1])

                erasePrimaryList.append(item)
                newJob.add(job)

        # Find out what the primaries outdate (we need to know that to
        # find the collection deltas). While we're at it, remove anything
        # which is the target of a redirect
        redirects = set(itertools.chain(*redirectHack.values()))

        outdated, eraseList = self.db.outdatedTroves(toOutdate | redirects, 
                                                     ineligible)
        for i, job in enumerate(keepList):
            item = (job[0], job[2][0], job[2][1])

            if item in outdated:
                job = (job[0], outdated[item][1:], job[2], False)
                keepList[i] = job

        for info in redirects:
            newJob.add((info[0], (info[1], info[2]), (None, None), False))

        del toOutdate

        referencedTroves = set()

        while keepList:
            job = keepList.pop()
            (trvName, (oldVersion, oldFlavor), (newVersion, newFlavor), abs) \
                                = job
            
            if oldVersion != newVersion or oldFlavor != newFlavor:
                newJob.add(job)

            if not recurse:
                continue

            # XXX it's crazy that we have to use the name of the trove to
            # figure out if it's a collection or not, but these changesets
            # are sans files (for performance), so that's what we're left
            # with
            if trvName.startswith('fileset-') or trvName.find(":") != -1:
                continue

            if oldVersion == newVersion and oldFlavor == newFlavor:
                # We need to install something which is already installed.
                # Needless to say, that's a bit silly. We don't need to
                # go through all of that, but we do need to recursively
                # update the referncedTroves set.
                trv = self.db.getTrove(trvName, oldVersion, oldFlavor,
                                          pristine = False)
                referencedTroves.update(x for x in trv.iterTroveList())
                for name, version, flavor in trv.iterTroveList():
                    keepList.append((name, (version, flavor),
                                           (version, flavor), False))

                del trv
                continue

            # collections should be in the changeset already. after all, it's
            # supposed to be recursive
            [ newPristine ] = troveSource.getTroves([(trvName, newVersion,
                                                  newFlavor)], 
                                                withFiles = False)

            if oldVersion is None:
                # Read the comments at the top of _newBase if you hope
                # to understand any of this.
                (oldTrv, pristineTrv, localTrv) = _newBase(newPristine)
                newTrv = pristineTrv.copy()
                newTrv.mergeCollections(localTrv, newPristine)
                finalTrvCs, fileList, neededTroveList = newTrv.diff(oldTrv)
            else:
                oldTrv = self.db.getTrove(trvName, oldVersion, oldFlavor,
                                       pristine = True)
                localTrv = self.db.getTrove(trvName, oldVersion, oldFlavor,
                                            pristine = False)
                newTrv = oldTrv.copy()
                newTrv.mergeCollections(localTrv, newPristine)
                finalTrvCs, fileList, neededTroveList = newTrv.diff(localTrv)

            assert(not oldTrv.hasFiles())
            assert(not localTrv.hasFiles())
            del oldTrv
            assert(not fileList)

            del finalTrvCs

            referencedTroves.update(x for x in newTrv.iterTroveList())

            alreadyInstalled = _alreadyInstalled(newTrv)
            locked = _lockedList(neededTroveList)
            for (name, oldVersion, newVersion, oldFlavor, newFlavor), \
                    oldIsLocked in itertools.izip(neededTroveList, locked):
                if (name, newVersion, newFlavor) not in alreadyInstalled:
                    if oldIsLocked:
                        if newVersion is not None:
                            uJob.addLockMapping(name, 
                                                (oldVersion, oldFlavor),
                                                (newVersion, newFlavor))
                    elif newVersion is None:
                        newJob.add((name, (oldVersion, oldFlavor),
                                          (None, None), False))
                    else:
                        keepList.append((name, (oldVersion, oldFlavor),
                                               (newVersion, newFlavor), False))
                else:
                    keepList.append((name, (newVersion, newFlavor),
                                           (newVersion, newFlavor), False))

        _removeDuplicateErasures(newJob)

        if not keepExisting:
            # try and match up everything absolute with something already
            # installed. respecting locks is important.
            removeSet = set(((x[0], x[1][0], x[1][1])
                             for x in newJob if x[1][0] is not None))

            absJob = [ x for x in newJob if x[1][0] is None ]
            outdated, eraseList = self.db.outdatedTroves(
                [ (x[0], x[2][0], x[2][1]) for x in absJob ],
                ineligible = removeSet | ineligible | referencedTroves | 
                             redirects)
            newJob = newJob - set(absJob)

            newTroves = (x[0] for x in outdated.iteritems() if x[1][1] is None)
            replacedTroves = [ (x[0], x[1]) for x in outdated.iteritems()
                               if x[1][1] is not None ]

            for info in newTroves:
                newJob.add((info[0], (None, None), (info[1], info[2]), 0))

            replacedAreLocked = self.db.trovesAreLocked((x[1] for x
                                                         in replacedTroves))

            for (newInfo, oldInfo), oldIsLocked in zip(replacedTroves,
                                                       replacedAreLocked):
                if not oldIsLocked:
                    newJob.add((newInfo[0], (oldInfo[1], oldInfo[2]),
                                (newInfo[1], newInfo[2]), 0))

        if keepExisting:
            # convert everything relative to new installs
            keepJobSet = set()
            for (name, (oldVersion, oldFlavor), (newVersion, newFlavor), 
                 absolute) in newJob:
                    keepJobSet.add((name, (None, None),
                                          (newVersion, newFlavor), absolute))

            newJob = keepJobSet

        # _findErasures picks what gets erased; nothing else gets to vote
	eraseSet = _findErasures(erasePrimaryList, newJob, referencedTroves, 
				 recurse)
        newJob -= set([ x for x in newJob if x[2][0] is None ])
        newJob.update(eraseSet)

        return newJob

    def _updateChangeSet(self, itemList, uJob, keepExisting = None, 
                         recurse = True, updateMode = True, sync = False):
        """
        Updates a trove on the local system to the latest version 
        in the respository that the trove was initially installed from.

        @param itemList: List specifying the changes to apply. Each item
        in the list must be a ChangeSetFromFile, or a standard job tuple.
        Versions in the job tuple may be strings, versions, branches, or 
        None. Flavors may be None.
        @type itemList: list
        """
        newJob = []
        changeSetJob = []
        finalCs = UpdateChangeSet()
        troveSource = trovesource.ChangesetFilesTroveSource(self.db)

        splittable = True

        toFind = {}
        toFindNoDb = {}
        for item in itemList:
            if isinstance(item, changeset.ChangeSetFromFile):
                splittable = False
                if item.isAbsolute():
		    self._rootChangeSet(item, keepExisting = keepExisting)

                finalCs.merge(item, (changeset.ChangeSetFromFile, item))

                continue

            (troveName, (oldVersionStr, oldFlavorStr),
                        (newVersionStr, newFlavorStr), isAbsolute) = item
            assert(oldVersionStr is None or not isAbsolute)

            if troveName[0] == '-':
                needsOld = True
                needsNew = newVersionStr or newFlavorStr
                troveName = troveName[1:]
            elif troveName[0] == '+':
                needsNew = True
                needsOld = oldVersionStr or oldFlavorStr
                troveName = troveName[1:]
            else:
                needsOld = not updateMode or oldVersionStr or oldFlavorStr
                needsNew = updateMode or newVersionStr or newFlavorStr


            if needsOld:
                oldTroves = self.db.findTrove(None, 
                                   (troveName, oldVersionStr, oldFlavorStr))
            else:
                oldTroves = []

            if not needsNew:
                assert(not newFlavorStr)
                assert(not isAbsolute)
                for troveInfo in oldTroves:
                    changeSetJob.append((troveInfo[0], 
                                         (troveInfo[1], troveInfo[2]),
                                         (None, None), False))
                # skip ahead to the next itemList
                continue                    

            if len(oldTroves) > 2:
                raise UpdateError, "Update of %s specifies multiple " \
                            "troves for removal" % troveName
            elif oldTroves:
                oldTrove = (oldTroves[0][1], oldTroves[0][2])
            else:
                oldTrove = (None, None)
            del oldTroves

            if isinstance(newVersionStr, versions.Version):
                assert(isinstance(newFlavorStr, deps.DependencySet))
                newJob.append((troveName, oldTrove,
                                    (newVersionStr, newFlavorStr), isAbsolute))
            elif isinstance(newVersionStr, versions.Branch):
                assert(isinstance(newFlavorStr, deps.DependencySet))
                toFind[(troveName, newVersionStr.asString(), 
                               newFlavorStr)] = oldTrove, isAbsolute
            elif (newVersionStr and newVersionStr[0] == '/'):
                # fully qualified versions don't need branch affinity
                # but they do use flavor affinity
                toFind[(troveName, newVersionStr, newFlavorStr)] = \
                                        oldTrove, isAbsolute
            else:
                if keepExisting and not sync:
                    # when using keepExisting, branch affinity doesn't make 
                    # sense - we are installing a new, generally unrelated 
                    # version of this trove
                    toFindNoDb[(troveName, newVersionStr, newFlavorStr)] \
                                    = oldTrove, isAbsolute
                else:
                    toFind[(troveName, newVersionStr, newFlavorStr)] \
                                    = oldTrove, isAbsolute

        results = {}
        if sync:
            source = trovesource.ReferencedTrovesSource(self.db)
            results.update(source.findTroves(None, toFind))
        else:
            if toFind:
                results.update(self.repos.findTroves(
                                        self.cfg.installLabelPath, toFind, 
                                        self.cfg.flavor,
                                        affinityDatabase=self.db))
            if toFindNoDb:
                results.update(self.repos.findTroves(
                                           self.cfg.installLabelPath, 
                                           toFindNoDb, self.cfg.flavor))

        for troveSpec, (oldTroveInfo, isAbsolute) in \
                itertools.chain(toFind.iteritems(), toFindNoDb.iteritems()):
            resultList = results[troveSpec]

            if len(resultList) > 1 and oldTroveInfo[0] is not None:
                raise UpdateError, "Relative update of %s specifies multiple " \
                            "troves for install" % troveName

            newJob += [ (x[0], oldTroveInfo, x[1:], isAbsolute) for x in 
                                    resultList ]

        # items which are already installed shouldn't be installed again
        present = self.db.hasTroves([ (x[0], x[2][0], x[2][1]) for x in 
                                                newJob ] )

        # we keep track of items that are considered for update but
        # are already installed so they don't get removed as a part
        # of some other update/install
        oldItems = [ (job[0], job[2][0], job[2][1]) for job, isPresent 
                            in itertools.izip(newJob, present) 
                            if isPresent ]

        newJob = [ job for job, isPresent 
                            in itertools.izip(newJob, present) 
                            if not isPresent ]
        changeSetJob += newJob
        del newJob

        # changeSetJob and oldItems should be unique 
        changeSetJob = set(changeSetJob)
        oldItems = set(oldItems)

        if finalCs.empty and not changeSetJob:
            raise NoNewTrovesError

        if changeSetJob:
            primaries = ([ (x[0], x[2][0], x[2][1]) for x in  changeSetJob
                                if x[2][0] is not None ] +
                         [ (x[0], x[1][0], x[1][1]) for x in  changeSetJob
                                if x[2][0] is     None ])
            cs = self.repos.createChangeSet(changeSetJob, withFiles = False,
                                            recurse = recurse,
                                            primaryTroveList = primaries)
            finalCs.merge(cs, (self.repos.createChangeSet, changeSetJob))

        troveSource.addChangeSet(finalCs)
        job = finalCs.getPrimaryJobSet()

        redirectHack = self._processRedirects(job, troveSource, recurse) 
        mergeItemList = self._mergeGroupChanges(job, troveSource, uJob, 
                                                redirectHack, keepExisting, 
                                                recurse, oldItems)

        # XXX this _resetTroveLists a hack, but building a whole new changeset
        # is a bit tricky due to changeset files
        cs1, remainder = troveSource.createChangeSet(mergeItemList, 
                                                 withFiles = False)
        finalCs._resetTroveLists()
        cs2 = self.repos.createChangeSet(remainder, withFiles = False,
                                        primaryTroveList = [], 
                                        recurse = False)
        finalCs.merge(cs1, (self.repos.createChangeSet, changeSetJob))
        finalCs.merge(cs2, (self.repos.createChangeSet, changeSetJob))

        return finalCs, splittable

    def fullUpdateItemList(self):
        items = self.db.findUnreferencedTroves()
        installed = self.db.findByNames(x[0] for x in items)

        installedDict = {}
        for (name, version, release) in installed:
            branchDict = installedDict.setdefault(name, {})
            l = branchDict.setdefault(version.branch(), [])
            l.append((version, release))

        updateItems = []

        for name, version, flavor in items:
            branch = version.branch()
            verInfo = installedDict[name][branch]

            if len(installedDict[name]) == 1 and len(verInfo) == 1:
                updateItems.append((name, None, None))
                continue
            elif len(verInfo) == 1:
                updateItems.append((name, branch, None))
                continue

            score = None
            for instFlavor in self.cfg.flavor:
                newScore = instFlavor.score(flavor) 
                if score is None or newScore > score:
                    score = newScore
                    finalFlavor = instFlavor

            flavor.union(finalFlavor, deps.DEP_MERGE_TYPE_OVERRIDE)

            if len(installedDict[name]) == 1:
                updateItems.append((name, None, flavor))
            else:
                updateItems.append((name, branch, flavor))

        return updateItems

    def updateChangeSet(self, itemList, keepExisting = False, recurse = True,
                        depsRecurse = True, resolveDeps = True, test = False,
                        updateByDefault = True, callback = UpdateCallback(),
                        split = False, sync = False):
        """
        Creates a changeset to update the system based on a set of trove update
        and erase operations.
	@param itemList: A list of 3-length tuples: (troveName, version,
	flavor).  If updateByDefault is True, trove names in itemList prefixed
	by a '-' will be erased. If updateByDefault is False, troves without a
	prefix will be erased, but troves prefixed by a '+' will be updated.
        @type itemList: [(troveName, version, flavor), ...]
	@param keepExisting: If True, troves updated not erase older versions
	of the same trove, as long as there are no conflicting files in either
	trove.
        @type keepExisting: bool
        @param recurse: Apply updates/erases to troves referenced by containers.
        @type recurse: bool
        @param depsRecurse: Resolve the dependencies the troves needed to
	resolove dependencies.
        @type depsRecurse: bool
        @param resolveDeps: Install troves needed to resolve dependencies.
        @type resolveDeps: bool
        @param test: If True, the operations will be attempted but the 
	filesystem and database will not be updated.
        @type test: bool
	@param updateByDefault: If True, troves passed to L{itemList} without a
	'-' or '+' prefix will be updated. If False, troves without a prefix 
	will be erased.
        @type updateByDefault: bool
        @param callback: L{callbacks.UpdateCallback} object.
        @type L{callbacks.UpdateCallback}
        @param split: Split large update operations into separate jobs.
        @type split: bool
        @param sync: Limit acceptabe trove updates only to versions 
        referenced in the local database.
        @type sync: bool
        @rtype: tuple
        """
        callback.preparingChangeSet()

        uJob = database.UpdateJob()

        finalCs, splittable = self._updateChangeSet(itemList, uJob,
                                        keepExisting = keepExisting,
                                        recurse = recurse,
                                        updateMode = updateByDefault,
                                        sync = sync)

        split = split and splittable
        updateThreshold = self.cfg.updateThreshold

        # When keep existing is provided none of the changesets should
        # be relative (since relative change sets, by definition, cause
        # something on the system to get replaced).
        if keepExisting:
            for troveCs in finalCs.iterNewTroveList():
                if troveCs.getOldVersion() is not None:
                    raise UpdateError, 'keepExisting specified for a ' \
                                       'relative change set'

        callback.resolvingDependencies()

        (cs, depList, suggMap, cannotResolve, splitJob) = \
            self._resolveDependencies(finalCs, uJob, 
                                      resolve = resolveDeps,
                                      keepExisting = keepExisting, 
                                      depsRecurse = depsRecurse,
                                      split = split)
        if depList:
            raise DepResolutionFailure(depList)
        elif suggMap and not self.cfg.autoResolve:
            raise NeededTrovesFailure(suggMap)
        elif cannotResolve:
            raise EraseDepFailure(cannotResolve)

        if split:
            startNew = True
            for job in splitJob:
                if startNew:
                    newCs = changeset.ChangeSet()
                    startNew = False
                    count = 0

                foundCollection = False

                count += len(job)
                for (name, (oldVersion, oldFlavor),
                           (newVersion, newFlavor), absolute) in job:
                    if newVersion is not None and ':' not in name:
                        foundCollection = True

                    if not newVersion:
                        newCs.oldTrove(name, oldVersion, oldFlavor)
                    else:
                        trvCs = cs.getNewTroveVersion(name, newVersion,
                                                      newFlavor)
                        assert(trvCs.getOldVersion() == oldVersion)
                        assert(trvCs.getOldFlavor() == oldFlavor)
                        newCs.newTrove(trvCs)

                if (foundCollection or 
                    (updateThreshold and (count >= updateThreshold))): 
                    uJob.addChangeSet(newCs)
                    startNew = True

            if not startNew:
                uJob.addChangeSet(newCs)
        else:
            uJob.addChangeSet(cs)

        callback.updateDone()

        return (uJob, suggMap)

    def applyUpdate(self, uJob, replaceFiles = False, tagScript = None, 
                    test = False, justDatabase = False, journal = None, 
                    localRollbacks = False, callback = UpdateCallback(),
                    autoLockList = conarycfg.RegularExpressionList()):

        def _createCs(repos, theCs, uJob, standalone = False):
            assert(not standalone or 
                   isinstance(theCs, changeset.ReadOnlyChangeSet))
            cs = changeset.ReadOnlyChangeSet()

            changedTroves = set()
            changedTroves.update((x.getName(),
                                  (x.getOldVersion(), x.getOldFlavor()),
                                  (x.getNewVersion(), x.getNewFlavor()), False)
                                 for x in theCs.iterNewTroveList())
            changedTroves.update((x[0], (x[1], x[2]), (None, None), False) 
                                 for x in theCs.getOldTroveList())

            if standalone:
                for (how, what) in theCs.contents:
                    if how == changeset.ChangeSetFromFile:
                        newCs = what

                        troves = [ (x.getName(), 
                                   (x.getOldVersion(), x.getOldFlavor()),
                                   (x.getNewVersion(), x.getNewFlavor()), False)
                                        for x in newCs.iterNewTroveList() ]
                        troves += [ (x[0], (x[1], x[2]), (None, None), False) 
                                            for x in newCs.getOldTroveList() ]

                        for item in troves:
                            if item in changedTroves:
                                changedTroves.remove(item)
                            elif item[2][0]:
                                newCs.delNewTrove(item[0], item[2][0], 
                                                  item[2][1])
                            else:
                                newCs.delOldTrove(item[0], item[1][0],
                                                  item[1][1])
                        cs.merge(newCs)

            if changedTroves:
                newCs = repos.createChangeSet(changedTroves,
                                              recurse = False,
                                              callback = callback)
                cs.merge(newCs)

            return cs

        def _applyCs(cs, uJob, removeHints = {}):
            try:
                rb = self.db.commitChangeSet(cs, uJob,
                                    replaceFiles = replaceFiles,
                                    tagScript = tagScript, test = test, 
                                    justDatabase = justDatabase,
                                    journal = journal, callback = callback,
                                    localRollbacks = localRollbacks,
                                    removeHints = removeHints,
                                    autoLockList = autoLockList)
            except database.CommitError, e:
                raise UpdateError, "changeset cannot be applied"

            return rb

        def _createAllCs(q, csSet, uJob, cfg):
	    # reopen the local database so we don't share a sqlite object
	    # with the main thread
	    db = database.Database(cfg.root, cfg.dbPath)
	    repos = NetworkRepositoryClient(cfg.repositoryMap,
					    localRepository = db)

            for i, theCs in enumerate(csSet):
                callback.setChangesetHunk(i + 1, len(csSet))
                newCs = _createCs(repos, theCs, uJob)
                q.put(newCs)

            q.put(None)
            thread.exit()

        csSet = uJob.getChangeSets()
        if isinstance(csSet[0], changeset.ReadOnlyChangeSet):
            # this handles change sets which include change set files
            assert(len(csSet) == 1)
            callback.setChangesetHunk(0, 0)
            newCs = _createCs(self.repos, csSet[0], uJob, standalone = True)
            callback.setUpdateHunk(0, 0)
            _applyCs(newCs, uJob)
        else:
            # build a set of everything which is being removed
            removeHints = set()
            for theCs in csSet:
                for trvCs in theCs.iterNewTroveList():
                    if trvCs.getOldVersion() is not None:
                        removeHints.add((trvCs.getName(), trvCs.getOldVersion(),
                                         trvCs.getOldFlavor()))

                for info in theCs.getOldTroveList():
                    removeHints.add(info)

            if not self.cfg.threaded:
                for i, theCs in enumerate(csSet):
                    callback.setChangesetHunk(i + 1, len(csSet))
                    newCs = _createCs(self.repos, theCs, uJob)
                    callback.setUpdateHunk(i + 1, len(csSet))
                    _applyCs(newCs, uJob, removeHints = removeHints)
            else:
                from Queue import Queue
                import thread

                csQueue = Queue(5)
                thread.start_new_thread(_createAllCs,
                                        (csQueue, csSet, uJob, self.cfg))

                newCs = csQueue.get()
                i = 1
                while newCs is not None:
                    callback.setUpdateHunk(i, len(csSet))
                    i += 1
                    _applyCs(newCs, uJob, removeHints = removeHints)
                    callback.updateDone()
                    newCs = csQueue.get()

    def getMetadata(self, troveList, label, cacheFile = None,
                    cacheOnly = False, saveOnly = False):
        md = {}
        if cacheFile and not saveOnly:
            try:
                cacheFp = open(cacheFile, "r")
                cache = pickle.load(cacheFp)
                cacheFp.close()
            except IOError, EOFError:
                if cacheOnly:
                    return {}
            else:
                lStr = label.asString()

                t = troveList[:]
                for troveName, branch in t:
                    bStr = branch.asString()

                    if lStr in cache and\
                       bStr in cache[lStr] and\
                       troveName in cache[lStr][bStr]:
                        md[troveName] = metadata.Metadata(cache[lStr][bStr][troveName])
                        troveList.remove((troveName, branch))

        # if the cache missed any, grab from the repos
        if not cacheOnly and troveList:
            md.update(self.repos.getMetadata(troveList, label))
            if md and cacheFile:
                try:
                    cacheFp = open(cacheFile, "rw")
                    cache = pickle.load(cacheFp)
                    cacheFp.close()
                except IOError, EOFError:
                    cache = {}

                cacheFp = open(cacheFile, "w")

                # filter down troveList to only contain items for which we found metadata
                cacheTroves = [x for x in troveList if x[0] in md]

                lStr = label.asString()
                for troveName, branch in cacheTroves:
                    bStr = branch.asString()

                    if lStr not in cache:
                        cache[lStr] = {}
                    if bStr not in cache[lStr]:
                        cache[lStr][bStr] = {}

                    cache[lStr][bStr][troveName] = md[troveName].freeze()

                pickle.dump(cache, cacheFp)
                cacheFp.close()

        return md

    def createBranch(self, newLabel, troveList = [], sourceTroves = True):
        return self._createBranchOrShadow(newLabel, troveList, shadow = False, 
                                     sourceTroves = sourceTroves)

    def createShadow(self, newLabel, troveList = [], sourceTroves = True):
        return self._createBranchOrShadow(newLabel, troveList, shadow = True, 
                                     sourceTroves = sourceTroves)

    def _createBranchOrShadow(self, newLabel, troveList, shadow,
                              sourceTroves):
        cs = changeset.ChangeSet()

        seen = set(troveList)
        dupList = []
        needsCommit = False

        newLabel = versions.Label(newLabel)

	while troveList:
            leavesByLabelOps = {}

            troves = self.repos.getTroves(troveList)
            troveList = set()
            branchedTroves = {}

	    for trove in troves:

                # add contained troves to the todo-list
                newTroves = [ x for x in trove.iterTroveList() if x not in seen ]
                troveList.update(newTroves)
                seen.update(newTroves)

                if sourceTroves and not trove.getName().endswith(':source'):
                    # XXX this can go away once we don't care about
                    # pre-troveInfo troves
                    if not trove.getSourceName():
                        log.warning('%s has no source information' % 
                                    trove.getName())
                    key  = (trove.getSourceName(),
                            trove.getVersion().getSourceVersion(),
                            deps.DependencySet())
                    if key not in seen:
                        troveList.add(key)
                        seen.add(key)
                    continue

                if shadow:
                    branchedVersion = trove.getVersion().createShadow(newLabel)
                else:
                    branchedVersion = trove.getVersion().createBranch(newLabel,
                                                                    withVerRel = 1)

                branchedTrove = trove.copy()
		branchedTrove.changeVersion(branchedVersion)
                #this clears the digital signatures from the shadow
                branchedTrove.troveInfo.sigs.reset()
                # FIXME we should add a new digital signature in cases
                # where we can (aka user is at kb and can provide secret key

		for (name, version, flavor) in trove.iterTroveList():
                    if shadow:
                        branchedVersion = version.createShadow(newLabel)
                    else:
                        branchedVersion = version.createBranch(newLabel,
                                                               withVerRel = 1)
                    byDefault = trove.includeTroveByDefault(name, 
                                                            version, flavor)
		    branchedTrove.delTrove(name, version, flavor,
                                           missingOkay = False)
		    branchedTrove.addTrove(name, branchedVersion, flavor,
                                            byDefault=byDefault)

                key = (trove.getName(), branchedTrove.getVersion(),
                       trove.getFlavor())
                branchedTroves[key] = branchedTrove.diff(None)[0]

            # check for duplicates - XXX this could be more efficient with
            # a better repository API
            queryDict = {}
            for (name, version, flavor) in branchedTroves.iterkeys():
                l = queryDict.setdefault(name, [])
                l.append(version)

            matches = self.repos.getAllTroveFlavors(queryDict)

            for (name, version, flavor), troveCs in branchedTroves.iteritems():
                if (matches.has_key(name) and matches[name].has_key(version) 
                    and flavor in matches[name][version]):
                    # this trove has already been branched
                    dupList.append((name, version.branch()))
                else:
                    cs.newTrove(troveCs)
                    cs.addPrimaryTrove(name, version, flavor)
                    needsCommit = True

        if needsCommit:
            self.repos.commitChangeSet(cs)

	return dupList

    def _createChangeSetList(self, csList, recurse = True, 
                             skipNotByDefault = False, 
                             excludeList = conarycfg.RegularExpressionList(),
                             callback = None):
        primaryList = []
        for (name, (oldVersion, oldFlavor),
                   (newVersion, newFlavor), abstract) in csList:
            if newVersion:
                primaryList.append((name, newVersion, newFlavor))
            else:
                primaryList.append((name, oldVersion, oldFlavor))

        cs = self.repos.createChangeSet(csList, recurse = recurse, 
                                        withFiles = False, callback = callback)

        # filter out non-defaults
        if skipNotByDefault:
            # Find out if troves were included w/ byDefault set (one
            # byDefault beats any number of not byDefault)
            inclusions = {}
            for troveCs in cs.iterNewTroveList():
                for (name, changeList) in troveCs.iterChangedTroves():
                    for (changeType, version, flavor, byDef) in changeList:
                        if changeType == '+':
                            if byDef:
                                inclusions[(name, version, flavor)] = True
                            else:
                                inclusions.setdefault((name, version, flavor), 
                                                      False)

            # use a list comprehension here because we're modifying the
            # underlying dict in the cs instance
            for troveCs in [ x for x in cs.iterNewTroveList() ]:
                if not troveCs.getNewVersion():
                    # erases get to stay since they don't have a byDefault flag
                    continue

                item = (troveCs.getName(), troveCs.getNewVersion(),
                        troveCs.getNewFlavor())
                if item in primaryList: 
                    # the item was explicitly asked for
                    continue
                elif inclusions[item]:
                    # the item was included w/ byDefault set (or we might
                    # have already erased it from the changeset)
                    continue

                # don't look at this trove again; we already decided to
                # erase it
                inclusions[item] = True
                cs.delNewTrove(*item)

        # now filter excludeList
        fullCsList = []
        for troveCs in cs.iterNewTroveList():
            name = troveCs.getName()
            newVersion = troveCs.getNewVersion()
            newFlavor = troveCs.getNewFlavor()

            skip = False

            # troves explicitly listed should never be excluded
            if (name, newVersion, newFlavor) not in primaryList:
                if excludeList.match(name):
                    skip = True

            if not skip:
                fullCsList.append((name, 
                           (troveCs.getOldVersion(), troveCs.getOldFlavor()),
                           (newVersion,              newFlavor),
                       not troveCs.getOldVersion()))

        # exclude packages that are being erased as well
        for (name, oldVersion, oldFlavor) in cs.getOldTroveList():
            skip = False
            if (name, oldVersion, oldFlavor) not in primaryList:
                for reStr, regExp in self.cfg.excludeTroves:
                    if regExp.match(name):
                        skip = True
            if not skip:
                fullCsList.append((name, (oldVersion, oldFlavor),
                                   (None, None), False))

        # recreate primaryList without erase-only troves for the primary trove 
        # list
        primaryList = [ (x[0], x[2][0], x[2][1]) for x in csList 
                        if x[2][0] is not None ]

        return (fullCsList, primaryList)

    def createChangeSet(self, csList, recurse = True, 
                        skipNotByDefault = True, 
                        excludeList = conarycfg.RegularExpressionList(),
                        callback = None, withFiles = False,
                        withFileContents = False):
        """
        Like self.createChangeSetFile(), but returns a change set object.
        withFiles and withFileContents are the same as for the underlying
        repository call.
        """
        (fullCsList, primaryList) = self._createChangeSetList(csList, 
                recurse = recurse, skipNotByDefault = skipNotByDefault, 
                excludeList = excludeList, callback = callback)

        return self.repos.createChangeSet(fullCsList, recurse = False,
                                       primaryTroveList = primaryList,
                                       callback = callback, 
                                       withFiles = withFiles,
                                       withFileContents = withFileContents)

    def createChangeSetFile(self, path, csList, recurse = True, 
                            skipNotByDefault = True, 
                            excludeList = conarycfg.RegularExpressionList(),
                            callback = None):
        """
        Creates <path> as a change set file.

        @param path: path to write the change set to
        @type path: string
        @param csList: list of (troveName, (oldVersion, oldFlavor),
                                (newVersion, newFlavor), isAbsolute)
        @param recurse: If true, conatiner troves are recursed through
        @type recurse: boolean
        @param skipNotByDefault: If True, troves which are included in
        a container with byDefault as False are not included (this flag
        doesn't do anything if recurse is False)
        @type recurse: boolean
        @param excludeList: List of regular expressions which are matched
        against recursively included trove names. Troves which match any 
        of the expressions are left out of the change set (this list
        is meaningless if recurse is False).
        @param callback: Callback object
        @type callback: callbacks.UpdateCallback
        """

        (fullCsList, primaryList) = self._createChangeSetList(csList, 
                recurse = recurse, skipNotByDefault = skipNotByDefault, 
                excludeList = excludeList, callback = callback)

        self.repos.createChangeSetFile(fullCsList, path, recurse = False,
                                       primaryTroveList = primaryList,
                                       callback = callback)

    def checkWriteableRoot(self):
        """
        Prepares the installation root for trove updates and change 
        set applications.
        """
        if not os.path.exists(self.cfg.root):
            util.mkdirChain(self.cfg.root)
        if not self.db.writeAccess():
            raise UpdateError, \
                "Write permission denied on conary database %s" % self.db.dbpath

    def lockTroves(self, troveList, lock = True):
        self.db.lockTroves(troveList, lock = lock)

