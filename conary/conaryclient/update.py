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

import itertools
import os
import time
import traceback
import sys

from callbacks import UpdateCallback
import conarycfg
from deps import deps
from lib import log
from local import database
from repository import changeset
from repository import trovesource
from repository.netclient import NetworkRepositoryClient
import trove
import versions

class UpdateChangeSet(changeset.ReadOnlyChangeSet):

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

class ClientUpdate:

    def _resolveDependencies(self, uJob, jobSet, split = False,
                             resolveDeps = True, useRepos = True):

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

        def _checkDeps(jobSet, trvSrc, findOrdering):

            while True:
                (depList, cannotResolve, changeSetList) = \
                                self.db.depCheck(jobSet, uJob.getTroveSource(),
                                                 findOrdering = findOrdering)

                if not cannotResolve:
                    return (depList, cannotResolve, changeSetList)

                oldIdx = {}
                for job in jobSet:
                    if job[1][0] is not None:
                        oldIdx[(job[0], job[1][0], job[1][1])] = job

                restoreSet = set()

                for (reqInfo, depSet, provInfoList) in cannotResolve:
                    for provInfo in provInfoList:
                        if provInfo not in oldIdx: continue

                        job = oldIdx[provInfo]
                        if job in restoreSet:
                            break

                        # if erasing this was a primary job, don't break
                        # it up
                        if (job[0], job[1], (None, None), False) in \
                                uJob.getPrimaryJobs():
                            continue

                        if job[2][0] is None:
                            # this was an erasure implied by package changes;
                            # leaving it in place won't hurt anything
                            restoreSet.add(job)
                            break

                        oldTrv = self.db.getTrove(withFiles = False,
                                                  *provInfo)
                        newTrv = trvSrc.getTrove(job[0], job[2][0], job[2][1],
                                                 withFiles = False)
                            
                        if oldTrv.compatibleWith(newTrv):
                            restoreSet.add(job)
                            break

                if not restoreSet:
                    return (depList, cannotResolve, changeSetList)

                for job in restoreSet:
                    jobSet.remove(job)
                    if job[2][0] is not None:
                        # if there was an install portion of the job,
                        # retain it
                        jobSet.add((job[0], (None, None), job[2], False))

        # def _resolveDependencies() begins here

        pathIdx = 0
        (depList, cannotResolve, changeSetList) = \
                    _checkDeps(jobSet, uJob.getTroveSource(),
                               findOrdering = split)
        suggMap = {}

        if not resolveDeps:
            # we're not supposed to resolve deps here; just skip the
            # rest of this
            depList = []
            cannotResolve = []

        resolveSource = uJob.getSearchSource()
        if useRepos:
            resolveSource = trovesource.stack(resolveSource, self.repos)
            

        while depList and pathIdx < len(self.cfg.installLabelPath):
            nextCheck = [ x[1] for x in depList ]
            sugg = resolveSource.resolveDependencies(
                            self.cfg.installLabelPath[pathIdx], 
                            nextCheck)

            troves = set()

            for (troveName, depSet) in depList:
                if depSet in sugg:
                    suggList = set()
                    for choiceList in sugg[depSet]:
                        troveNames = set(x[0] for x in choiceList)

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

            if troves:
                # we found good suggestions, merge in those troves
                newJob = self._updateChangeSet(troves, uJob,
                                          keepExisting = False,
                                          recurse = False,
                                          ignorePrimaryPins = False)[0]
                assert(not (newJob & jobSet))
                jobSet.update(newJob)

                lastCheck = depList
                (depList, cannotResolve, changeSetList) = \
                            _checkDeps(jobSet, uJob.getTroveSource(),
                                       findOrdering = split)
                if lastCheck != depList:
                    pathIdx = 0
            else:
                # we didnt find any suggestions; go on to the next label
                # in the search path
                pathIdx += 1

        return (depList, suggMap, cannotResolve, changeSetList)

    def _processRedirects(self, uJob, jobSet, recurse):
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
            item = (name, newVersion, newFlavor)

            if newVersion is None:
                # skip erasure
                continue

            trv = uJob.getTroveSource().getTrove(name, newVersion, newFlavor, 
                                                 withFiles = False)

            if not trv.isRedirect():
                continue

            if not recurse:
                raise UpdateError,  "Redirect found with --no-recurse set"

            isPrimary = job in jobSet
            if isPrimary: 
                # The redirection is a primary. Remove it.
                jobSet.remove(job)

            targets = []
            allTargets = trv.iterTroveList()
            # three possibilities:
            #   1. no targets -- make sure a simple erase occurs
            #   2. target is the primary target of this redirect (this is 
            #       tricky; we use the rule that components only redirect to a
            #       single component, and that collections redirect to a single
            #       collection (and other, secondary, collections). the
            #       primary redirects are added to the target list
            #   3. secondary targets are added to the toDoList for later
            #      handling
            if not allTargets:
                l = redirectHack.setdefault(None, [])
                l.append(item)
            else:
                for (subName, subVersion, subFlavor) in allTargets:
                    if (":" not in subName and ":" not in name) or \
                       (":"     in subName and ":"     in name):
                        # primary
                        l = redirectHack.setdefault((subName, subVersion,
                                                     subFlavor), [])
                        l.append(item)
                        targets.append((subName, subVersion, subFlavor))
                    else:
                        # secondary
                        toDoList.append((subName, (None, None), 
                                                  (subVersion, subFlavor), 
                                         True))

            if isPrimary:
                for subName, subVersion, subFlavor in targets:
                    jobSet.add((subName, (None, None), (subVersion, subFlavor),
                                True))

        for l in redirectHack.itervalues():
	    outdated = self.db.outdatedTroves(l)
            del l[:]
            for (name, newVersion, newFlavor), \
                  (oldName, oldVersion, oldFlavor) in outdated.iteritems():
                if oldVersion is not None:
                    l.append((oldName, oldVersion, oldFlavor))

        return redirectHack

    def _mergeGroupChanges(self, uJob, primaryJobList, redirectHack, 
                           recurse, ineligible, ignorePrimaryPins):

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
                if self.cfg.excludeTroves.match(info[0]):
                    delList.append(info)
                    continue

                # if this is the target of a redirection, make sure we have
                # the source of that redirection installed
                if info in redirectHack:
                    l = redirectHack[info]
                    present = self.db.hasTroves(l)
                    if sum(present) == 0:
                        # sum of booleans -- tee-hee
                        delList.append(info)

            for info in delList:
                localTrv.delTrove(*(info + (False,)))

            return (oldTrv, pristineTrv, localTrv)

        def _alreadyInstalled(trv):
            troveInfo = [ x for x in newTrv.iterTroveList() ]
            present = self.db.hasTroves(troveInfo)
            r = [ info for info,present in itertools.izip(troveInfo, present)
                     if present ]

            return dict.fromkeys(r)

        def _lockedList(neededList, ignorePin):
            if ignorePin:
                return ( False, ) * len(neededList)

            l = [ (x[0], x[1], x[3]) for x in neededList if x[1] is not None ]
            l.reverse()
            lockList = self.db.trovesArePinned(l)
            r = []
            for item in neededList:
                if item[1] is not None:
                    r.append(lockList.pop())
                else:
                    r.append(False)

            return r

	def _findErasures(primaryErases, origNewJob, referencedTroves, 
                          recurse):
	    # each node is a ((name, version, flavor), state, edgeList
	    #		       fromUpdate)
	    # state is ERASE, KEEP, or UNKNOWN
            #
            # fromUpdate is True if erasing this node reflects a trove being
            # replaced by a different one in the Job (an update, not an erase)
            # We need to track this to know what's being removed, but we don't
            # need to cause them to be removed.
	    nodeList = []
	    nodeIdx = {}
	    ERASE = 1
	    KEEP = 2
	    UNKNOWN = 3

            newJob = set(origNewJob)

            # Make sure troves which the changeset thinks should be removed
            # get considered for removal. Ones which need to be removed
            # for a new trove to be installed are guaranteed to be removed.
            oldTroves = [ ((job[0], job[1][0], job[1][1]), True, ERASE) for
                                job in newJob
                                if job[1][0] is not None and 
                                   job[2][0] is not None ]
            oldTroves += [ ((job[0], job[1][0], job[1][1]), False, UNKNOWN) for
                                job in newJob
                                if job[1][0] is not None and 
                                   job[2][0] is None ]

            # Create the nodes for the graph, one for each trove being
            # removed.
            for info, fromUpdate, state in oldTroves:
                if info in nodeIdx:
                    # the node is marked to erase multiple times in
                    # newJob! this can happen, for example, on 
                    # "update +foo=2.0 -foo=1.0" (not to mention it
                    # happening from complex, overlapping groups)
                    idx = nodeIdx[info]
                    otherState = nodeList[idx][1]
                    assert(state is UNKNOWN or otherState is UNKNOWN)
                    if otherState is UNKNOWN:
                        nodeList[idx] = [ (info, state, [], fromUpdate) ]
                    continue

                nodeIdx[info] = len(nodeList)
                nodeList.append([ info, state, [], fromUpdate ])

            del oldTroves

            # Primary troves are always erased.
            for info in primaryErases:
                nodeList[nodeIdx[info]][1] = ERASE

            # For nodes which we haven't decided to erase, we need to track
            # down all of the collections which include those troves.
	    needParents = [ (nodeId, info) for nodeId, (info, state, edges,
                                                        alreadyHandled)
				in enumerate(nodeList) if state == UNKNOWN ]
	    while needParents:
		containers = self.db.getTroveContainers(
                                        x[1] for x in needParents)
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
            # in case where you have 
            # (a, (oldV, oldF), (newV, newF)),
            # (a, (oldV, oldF), (newV2, newF2))
            # call outdated troves to fix up so that both troves
            # don't remove the same thing
            outdated = {}
            for job in jobSet:
                if job[2][0] is not None:
                    l = outdated.setdefault((job[0], job[1][0], job[1][1]), [])
                    l.append(job)

            inelligible = []
            newItems = set()
            toRemove = set()
            for old, l in outdated.iteritems():
                if len(l) == 1: 
                    inelligible.append(old)
                else:
                    newItems.update((x[0], x[2][0], x[2][1]) for x in l)
                    toRemove.update(l)

            if not newItems:
                return

            jobSet.difference_update(toRemove)

            # Everything left in outdated conflicts with itself. we'll
            # let outdated sort things out.
            outdated = self.db.outdatedTroves(newItems, inelligible)
            needed = []

            for newInfo, oldInfo in outdated.iteritems():
                jobSet.add((newInfo[0], oldInfo[1:], newInfo[1:], False))

                # it's possible we have hooked up to something that was 
                # scheduled for removal anyway - remove the duplicate
                # erasure
                jobSet.discard((oldInfo[0], oldInfo[1:], (None, None), False))

        def _removeDuplicateAdditions(jobSet):
            # the same trove could be added both as part of a cmd line update
            # and as part of a recursive update (or as part of two recursive
            # updates).  Prefer the update that already has a remove 
            # tied to it (if any).
            additions = {}

            for job in jobSet:
                if job[2][0] is not None:
                    l = additions.setdefault((job[0], job[2][0], job[2][1]), [])
                    l.append(job)
            
            toRemove = []
            for addition, l in additions.iteritems():
                if len(l) > 1:
                    empty = [ x for x in l if x[1][0] is None] 
                    # there should be only one of these, since we've already
                    # converted absolute updates to relative ones.
                    assert(len(empty) <= 1)

                    l = set(l)
                    l.difference_update(empty)

                    if len(l) > 1:
                        # it's not obvious what to do with this case, leave
                        # it as an error until we see whether it occurs irl
                        raise RuntimeError, ('Multiple relative updates '
                                             ' in same trove: %s' % l)
                    toRemove.extend(empty)

            jobSet.difference_update(toRemove)


        def _getPathHashes(trvSrc, db, trv, isCollection, inDb = False):
            if not isCollection: return trv.getPathHashes()

            ph = None
            for info in trv.iterTroveList():
                if inDb:
                    otherTrv = db.getTrove(withFiles = False, *info)
                else:
                    otherTrv = trvSrc.getTrove(withFiles = False, *info)

                if ph is None:
                    ph = otherTrv.getPathHashes()
                else:
                    ph.update(otherTrv.getPathHashes())

            if ph is None:
                # this gives us an empty set
                ph = trv.getPathHashes()

            return ph

        def _matchRelativeUpdates(newJob):
            changes = []
            noFlavor = deps.DependencySet()
            oldTrv = trove.Trove('@trv', versions.NewVersion(), noFlavor, None)
            newTrv = trove.Trove('@trv', versions.NewVersion(), noFlavor, None)

            # nonUpdate == either a new install or an erasure
            nonUpdate = [ x for x in newJob if not (x[1][0] and x[2][0])]
            newJob.difference_update(nonUpdate)

            for (name, oldInfo, newInfo, isAbs) in nonUpdate:
                if oldInfo[0]:
                    oldTrv.addTrove(name, oldInfo[0], oldInfo[1])
                else:
                    newTrv.addTrove(name, newInfo[0], newInfo[1])

            finalTrvCs, fileList, neededTroveList = newTrv.diff(oldTrv)
            for (name, oldVer, newVer, oldFla, newFla) in neededTroveList:
                newJob.add((name, (oldVer, oldFla), (newVer, newFla), False))

        # def _mergeGroupChanges -- main body begins here
            
        # jobQueue is (job, ignorePins)
        jobQueue = []
        erasePrimaryList = []
        toOutdate = set()
        newJob = set()

        log.debug("looking up troves to outdate for primary troves")
        for job in primaryJobList:
            if job[2][0] is not None:
                item = (job[0], job[2][0], job[2][1])
            
                if job[3]:
                    # try and outdate absolute change sets
                    assert(not job[1][0])
                    toOutdate.add(item)
                jobQueue.append(job)
            else:
                item = (job[0], job[1][0], job[1][1])

                erasePrimaryList.append(item)
                jobQueue.append(job)

        # Find out what the primaries outdate (we need to know that to
        # find the collection deltas). While we're at it, remove anything
        # which is the target of a redirect
        redirects = set(itertools.chain(*redirectHack.values()))

        outdated = self.db.outdatedTroves(toOutdate | redirects, ineligible)
        log.debug("assembling initial job list")
        for i, job in enumerate(jobQueue):
            item = (job[0], job[2][0], job[2][1])

            if item in outdated:
                job = (job[0], outdated[item][1:], job[2], False)
                jobQueue[i] = job

        for info in redirects:
            newJob.add((info[0], (info[1], info[2]), (None, None), False))

        del toOutdate

        # look up pin status for troves being removed
        removedList = [ (x[0], x[1][0], x[1][1]) for x in jobQueue if
                            x[1][0] is not None ]
        pins = self.db.trovesArePinned(removedList)
        pins.reverse()
        for i, job in enumerate(jobQueue):
            # was this job originally absolute? 
            absoluteAncestry = (job[0], (None, None), job[2], True) \
                                    in primaryJobList

            # add ignorePin, isPinned, absoluteAncestry flags
            if job[1][0] is None: 
                # it doesn't matter what we put here since we don't have it
                # already
                jobQueue[i] = (job, ignorePrimaryPins, False, absoluteAncestry)
            else:
                pin = pins.pop()
                jobQueue[i] = (job, ignorePrimaryPins, pin, absoluteAncestry)
        del removedList, pins

        referencedTroves = set()
        orphanedBranchJobs = set()

        while jobQueue:
            log.debug("examing job %s", jobQueue[-1])
            job, ignorePin, isPinned, absoluteAncestry = jobQueue.pop()

            (trvName, (oldVersion, oldFlavor), (newVersion, newFlavor), 
                    isAbsolute) = job
            
            # XXX it's crazy that we have to use the name of the trove to
            # figure out if it's a collection or not, but these changesets
            # are sans files (for performance), so that's what we're left
            # with
            if trvName.startswith('fileset-') or trvName.find(":") != -1:
                isCollection = False
            else:
                isCollection = True

            if oldVersion == newVersion and oldFlavor == newFlavor:
                # We need to install something which is already installed.
                # Needless to say, that's a bit silly. We don't need to
                # go through all of that, but we do need to recursively
                # update the referncedTroves set.
                log.debug("trove already installed -- recursing through "
                          "referenced troves")
                trv = self.db.getTrove(trvName, oldVersion, oldFlavor,
                                          pristine = False)
                referencedTroves.update(x for x in trv.iterTroveList())

                for name, version, flavor in trv.iterTroveList():
                    jobQueue.append(((name, (version, flavor),
                                           (version, flavor), False),
                                     ignorePin, False, absoluteAncestry))

                del trv
                continue

            # collections should be in the changeset already. after all, it's
            # supposed to be recursive
            if newVersion is not None:
                newPristine = uJob.getTroveSource().getTrove(trvName, 
                                                    newVersion, newFlavor,
                                                    withFiles = False)

            log.debug("merging trove into current diff")

            if newVersion is None:
                # handle erase recursion
                oldTrv = self.db.getTrove(trvName, oldVersion, oldFlavor,
                                          pristine = False)
                newTrv = trove.Trove(trvName, versions.NewVersion(), oldFlavor,
                                     None)
                finalTrvCs, fileList, neededTroveList = newTrv.diff(oldTrv)
                del finalTrvCs, fileList
            elif not isCollection:
                newTrv = newPristine
                neededTroveList = []
                oldTrv = None
                del newPristine
            elif oldVersion is None:
                # Read the comments at the top of _newBase if you hope
                # to understand any of this.
                (oldTrv, pristineTrv, localTrv) = _newBase(newPristine)
                newTrv = pristineTrv.copy()
                orphaned = newTrv.mergeCollections(localTrv, 
                                                   newPristine, 
                                                   self.cfg.excludeTroves)
                orphanedBranchJobs.update(orphaned)
                finalTrvCs, fileList, neededTroveList = newTrv.diff(oldTrv)
                del finalTrvCs, localTrv, fileList, newPristine
            else:
                oldTrv = self.db.getTrove(trvName, oldVersion, oldFlavor,
                                       pristine = True)
                localTrv = self.db.getTrove(trvName, oldVersion, oldFlavor,
                                            pristine = False)
                newTrv = oldTrv.copy()
                orphaned = newTrv.mergeCollections(localTrv, 
                                                   newPristine, 
                                                   self.cfg.excludeTroves)
                orphanedBranchJobs.update(orphaned)
                finalTrvCs, fileList, neededTroveList = newTrv.diff(localTrv)
                del finalTrvCs, localTrv, fileList, newPristine

            if isPinned and not newVersion:
                if not ignorePin:
                    # trying to erase something which is pinned
                    log.debug("skipping of pinned trove")
                    continue
                log.debug("erasing pinned trove")
                newJob.add(job)
            elif isPinned:
                log.debug("old version of trove is pinned")
                # trying to install something whose old version is pinned
                assert(oldVersion is not None)

                # see if we can simply install it next to the item which
                # is currently installed
                localTrv = self.db.getTrove(trvName, oldVersion, oldFlavor,
                                            pristine = False)

                oldPaths = _getPathHashes(uJob.getTroveSource(), self.db, 
                                          localTrv, isCollection, inDb = True)
                newPaths = _getPathHashes(uJob.getTroveSource(), self.db, 
                                          newTrv, isCollection)

                if oldPaths.compatibleWith(newPaths):
                    # make this job a fresh install since the paths
                    # are compatible
                    log.debug("install new trove as paths do not overlap")
                    newJob.add((trvName, (None, None), (newVersion, newFlavor),
                                False))
                    # regenerate this
                    finalTrvCs, fileList, neededTroveList = newTrv.diff(None)
                    del finalTrvCs, fileList
                elif not ignorePin:
                    # this is incompatible with the pinned trove being
                    # replaced; we need to leave the old one here
                    log.debug("skipping install job due to pin")
                    if newVersion is not None:
                        uJob.addPinMapping(trvName, 
                                            (oldVersion, oldFlavor),
                                            (newVersion, newFlavor))
                    continue
                else:
                    log.debug("ignoring pin")
                    newJob.add(job)
            else:
                log.debug("adding job")
                newJob.add(job)

            del oldTrv

            if not recurse:
                continue

            referencedTroves.update(x for x in newTrv.iterTroveList())

            alreadyInstalled = _alreadyInstalled(newTrv)

            if ignorePin and oldVersion:
                # recursively ignore the pin only if this item is pinned
                ignorePin = self.db.trovesArePinned(
                                [ (trvName, oldVersion, oldFlavor ) ] )[0]
            else:
                ignorePin = False

            pinned = _lockedList(neededTroveList, ignorePin)

            log.debug("iterating through referenced troves (if any)")
            for (name, oldVersion, newVersion, oldFlavor, newFlavor), \
                    oldIsPinned in itertools.izip(neededTroveList, pinned):
                if (name, newVersion, newFlavor) not in alreadyInstalled:
                    if newVersion is None:
                        # add this job to the keeplist to do erase recursion
                        log.debug("considering referenced trove for removal")
                        jobQueue.append(((name, (oldVersion, oldFlavor),
                                               (None, None), False),
                                          ignorePin and isPinned, oldIsPinned,
                                          absoluteAncestry))
                    else:
                        if oldVersion is None:
                            # this is absolute if the original job for the
                            # parents was absolute. primaries will have
                            # been rooted by now though, so look in the 
                            # original job as well
                            makeAbs = absoluteAncestry
                        else:
                            makeAbs = False
                                
                        log.debug("considering installation of referenced "
                                  "trove")
                        jobQueue.append(((name, (oldVersion, oldFlavor),
                                               (newVersion, newFlavor), 
                                         makeAbs), ignorePin and isPinned, 
                                         oldIsPinned, absoluteAncestry))
                else:
                    log.debug("referenced trove already installed")
                    # recurse through this trove's collection even though
                    # it doesn't need to be installed
                    if oldVersion is not None:
                        log.debug("removing old versoin of referenced trove")
                        jobQueue.append(((name, (oldVersion, oldFlavor),
                                                (None, None), False),
                                          ignorePin and isPinned, 
                                          oldIsPinned, absoluteAncestry))

                    jobQueue.append(((name, (newVersion, newFlavor),
                                           (newVersion, newFlavor), False),
                                     ignorePin, False, absoluteAncestry))

        _removeDuplicateErasures(newJob)

        absJob = [ x for x in newJob if x[3] is True ]
        absJob.extend((x[0], (None, None), x[1:], True) \
                                                for x in orphanedBranchJobs)

        if absJob:
            # try and match up everything absolute with something already
            # installed. respecting locks is important.
            
            # don't allow updating to old versions that are already in 
            # an upgrade 
            removeSet = set(((x[0], x[1][0], x[1][1])
                             for x in newJob if x[1][0] is not None
                                            and x[2][0] is not None))

            outdated = self.db.outdatedTroves(
                [ (x[0], x[2][0], x[2][1]) for x in absJob],
                ineligible = removeSet | ineligible | referencedTroves | 
                             redirects)

            # we'll add these back in as relative updates/installs
            newJob.difference_update(absJob)

            newTroves = (x[0] for x in outdated.iteritems() if x[1][1] is None)
            # an orphaned branch job is an update of a trove referenced in
            # a collection that traversed across branches and was dropped 
            # because it looked like the local system erased the trove It's
            # possible that the trove might have just switched branches instead 
            # of being deleted, so we check to see if it switched to the target
            # branch, in which case we use that as the old trove to update.

            # orphaned branch jobs are only valid if you find a 
            # local trove on the target branch that can be matched to it -
            # not as a fresh install
            newTroves = (x for x in newTroves if x not in orphanedBranchJobs) 

            # these are considered relative now; they're being newly
            # installed
            newJob.update((x[0], (None, None), (x[1], x[2]), 0) 
                          for x in newTroves)

            replacedTroves = ( (x[0], x[1]) for x in outdated.iteritems()
                               if x[1][1] is not None )

            # only allow orphaned branch jobs if they are updating
            # from a local trove that is on the same label as the orphaned
            # job
            replacedTroves = [x for x in replacedTroves
                              if x[0] not in orphanedBranchJobs
                                 or x[0][1].branch() == x[1][1].branch()]

            replacedArePinned = self.db.trovesArePinned((x[1] for x
                                                         in replacedTroves))

            for (newInfo, oldInfo), oldIsPinned in zip(replacedTroves,
                                                       replacedArePinned):
                if oldIsPinned:
                    oldTrv = self.db.getTrove(withFiles = False, 
                                              pristine = False, *oldInfo)
                    newTrv = uJob.getTroveSource().getTrove(withFiles = False,
                                                            *newInfo)

                    if newInfo[0].startswith('fileset-') or \
                                        newInfo[0].find(":") != -1:
                        isCollection = False
                    else:
                        isCollection = True

                    oldPaths = _getPathHashes(uJob.getTroveSource(), self.db, 
                                           oldTrv, isCollection, inDb = True)
                    newPaths = _getPathHashes(uJob.getTroveSource(), self.db, 
                                           newTrv, isCollection)
                    if oldPaths.compatibleWith(newPaths):
                        newJob.add((newInfo[0], (None, None),
                                    (newInfo[1], newInfo[2]), False))
                else:
                    # the old one isn't pinned
                    newJob.add((newInfo[0], (oldInfo[1], oldInfo[2]),
                                (newInfo[1], newInfo[2]), False))

                    # this job could be replacing an erase job.  
                    # just get rid of it.
                    newJob.discard((newInfo[0], (oldInfo[1], oldInfo[2]),
                                    (None, None), False))

        _removeDuplicateAdditions(newJob)
        _matchRelativeUpdates(newJob)

        # _findErasures picks what gets erased; nothing else gets to vote
	eraseSet = _findErasures(erasePrimaryList, newJob, referencedTroves, 
				 recurse)
        newJob.difference_update([x for x in newJob if x[2][0] is None])
        newJob.update(eraseSet)

        return newJob

    def _updateChangeSet(self, itemList, uJob, keepExisting = None, 
                         recurse = True, updateMode = True, sync = False,
                         useAffinity = True, ignorePrimaryPins = True):
        """
        Updates a trove on the local system to the latest version 
        in the respository that the trove was initially installed from.

        @param itemList: List specifying the changes to apply. Each item
        in the list must be a ChangeSetFromFile, or a standard job tuple.
        Versions in the job tuple may be strings, versions, branches, or 
        None. Flavors may be None.
        @type itemList: list
        """

        def _separateInstalledItems(jobSet):
            present = self.db.hasTroves([ (x[0], x[2][0], x[2][1]) for x in 
                                                    jobSet ] )
            oldItems = set([ (job[0], job[2][0], job[2][1]) for job, isPresent 
                                in itertools.izip(jobSet, present) 
                                if isPresent ])
            newItems = set([ job for job, isPresent 
                                in itertools.izip(jobSet, present) 
                                if not isPresent ])
            return newItems, oldItems

        # def _updateChangeSet -- body starts here

        # This job describes updates from a networked repository. Duplicates
        # (including installing things already installed) are skipped.
        newJob = set()
        # This job is from a changeset file.
        jobsFromChangeSetFiles = set()
        # These are items being removed.
        removeJob = set()

        splittable = True

        toFind = {}
        toFindNoDb = {}
        for item in itemList:
            if isinstance(item, changeset.ChangeSetFromFile):
                if keepExisting:
                    # We need to mark absolute troves in this changeset
                    # as relative to preserve proper keepExisting behavior.
                    newList = []
                    for troveCs in item.iterNewTroveList():
                        log.debug("found %s=%s[%s]" %
                                  (troveCs.getName(), 
                                   troveCs.getNewVersion().asString(),
                                   str(troveCs.getNewFlavor())))
                        if troveCs.isAbsolute():
                            # XXX we could just flip the absolute bit instead
                            # of going through all of this...
                            log.debug("(switching to relative install to "
                                      "force prevent rooting)")
                            newTrove = trove.Trove(troveCs.getName(), 
                                            troveCs.getNewVersion(), 
                                            troveCs.getNewFlavor(), 
                                            troveCs.getChangeLog())
                            newTrove.applyChangeSet(troveCs)
                            newCs = newTrove.diff(None, absolute = False)[0]
                            newList.append(newCs)

                    for troveCs in newList:
                        # new replaces old
                        item.newTrove(troveCs)


                splittable = False
                uJob.getTroveSource().addChangeSet(item, 
                                                   includesFileContents = True)
                jobsFromChangeSetFiles.update(item.getJobSet(primaries = True))
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
                needsOld = oldVersionStr or oldFlavorStr
                needsNew = newVersionStr or newFlavorStr
                if not (needsOld or needsNew):
                    if updateMode:
                        needsNew = True
                    else:
                        needsOld = True

            if needsOld:
                oldTroves = self.db.findTrove(None, 
                                   (troveName, oldVersionStr, oldFlavorStr))
            else:
                oldTroves = []

            if not needsNew:
                assert(not newFlavorStr)
                assert(not isAbsolute)
                for troveInfo in oldTroves:
                    log.debug("set up removal of %s", troveInfo)
                    removeJob.add((troveInfo[0], (troveInfo[1], troveInfo[2]),
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
                jobToAdd = (troveName, oldTrove,
                            (newVersionStr, newFlavorStr), isAbsolute)
                newJob.add(jobToAdd)
                log.debug("set up job %s", jobToAdd)
                del jobToAdd
            elif isinstance(newVersionStr, versions.Branch):
                toFind[(troveName, newVersionStr.asString(),
                        newFlavorStr)] = oldTrove, isAbsolute
            elif (newVersionStr and newVersionStr[0] == '/'):
                # fully qualified versions don't need branch affinity
                # but they do use flavor affinity
                toFind[(troveName, newVersionStr, newFlavorStr)] = \
                                        oldTrove, isAbsolute
            else:
                if not (isAbsolute or not useAffinity):
                    # not isAbsolute means keepExisting. when using
                    # keepExisting, branch affinity doesn't make sense - we are
                    # installing a new, generally unrelated version of this
                    # trove
                    toFindNoDb[(troveName, newVersionStr, newFlavorStr)] \
                                    = oldTrove, isAbsolute
                else:
                    toFind[(troveName, newVersionStr, newFlavorStr)] \
                                    = oldTrove, isAbsolute
        results = {}
        searchSource = uJob.getSearchSource()

        if not useAffinity:
            results.update(searchSource.findTroves(None, toFind))
        else:
            if toFind:
                log.debug("looking up troves w/ database affinity")
                results.update(searchSource.findTroves(
                                        self.cfg.installLabelPath, toFind, 
                                        self.cfg.flavor,
                                        affinityDatabase=self.db))
            if toFindNoDb:
                log.debug("looking up troves w/o database affinity")
                results.update(searchSource.findTroves(
                                           self.cfg.installLabelPath, 
                                           toFindNoDb, self.cfg.flavor))

        for troveSpec, (oldTroveInfo, isAbsolute) in \
                itertools.chain(toFind.iteritems(), toFindNoDb.iteritems()):
            resultList = results[troveSpec]

            if len(resultList) > 1 and oldTroveInfo[0] is not None:
                raise UpdateError, "Relative update of %s specifies multiple " \
                            "troves for install" % troveName

            newJobList = [ (x[0], oldTroveInfo, x[1:], isAbsolute) for x in 
                                    resultList ]
            newJob.update(newJobList)
            log.debug("adding jobs %s", newJobList)

        # Items which are already installed shouldn't be installed again. We
        # want to track them though to ensure they aren't removed by some
        # other action.
        changeSetList, oldItems = _separateInstalledItems(newJob)
        jobSet, oldItems2 = _separateInstalledItems(jobsFromChangeSetFiles)

        # Give nice warnings for things in oldItems2 (whose installs were
        # requested, but are already there). XXX It seems like this would
        # be a good idea for all jobs, not just ones which came from change
        # set files?
        for item in oldItems2:
            log.warning("trove %s %s is already installed -- skipping",
                        item[0], item[1].asString())

        oldItems.update(oldItems2)
        log.debug("items already installed: %s", oldItems)

        changeSetList.update(removeJob)
        del newJob, removeJob, oldItems2, jobsFromChangeSetFiles

        # we now have three things
        #   1. jobFromChangeSetFiles -- job which came from .ccs files
        #   2. oldItems -- items which we should not remove as a side effect
        #   3. changeSetList -- job we need to create a change set for

        if not changeSetList and not jobSet:
            raise NoNewTrovesError

        if changeSetList:
            jobSet.update(changeSetList)

            # FIXME changeSetSource: I should just be able to call 
            # csSource.createChangeSet but it can't handle recursive
            # createChangeSet calls, and you can only call createChangeSet
            # once on a csSource.  So, we avoid having to call it more than
            # once by checking to see if the changeSets are already in the
            # update job.
            hasTroves = uJob.getTroveSource().hasTroves(
                [ (x[0], x[2][0], x[2][1]) for x in changeSetList ] )

            changeSetList = [ x[1] for x in
                              itertools.izip(hasTroves, changeSetList)
                               if x[0] is not True ]

            csSource = trovesource.TroveSourceStack(
                                         uJob.getSearchSource(),
                                         self.repos)

            cs, notFound = csSource.createChangeSet(changeSetList, 
                                                    withFiles = False,
                                                    recurse = recurse)
            assert(not notFound)
            uJob.getTroveSource().addChangeSet(cs)
            del cs

        del changeSetList

        redirectHack = self._processRedirects(uJob, jobSet, recurse) 
        newJob = self._mergeGroupChanges(uJob, jobSet, redirectHack, 
                                         recurse, oldItems, ignorePrimaryPins)
        if not newJob:
            raise NoNewTrovesError

        uJob.setPrimaryJobs(jobSet)

        return newJob, splittable

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
                        resolveDeps = True, test = False,
                        updateByDefault = True, callback = UpdateCallback(),
                        split = False, sync = False, fromChangesets = [],
                        checkPathConflicts = True, ignorePrimaryPins = True,
                        resolveRepos = True):
        """
        Creates a changeset to update the system based on a set of trove update
        and erase operations. If self.cfg.autoResolve is set, dependencies
        within the job are automatically closed.

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
        @param resolveDeps: Should dependencies error be flagged or silently
        ignored?
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
        @param fromChangesets: When specified, these changesets are used
        as the source of troves instead of the repository.
        @type fromChangesets: list of changeset.ChangeSetFromFile
        @param ignorePrimaryPins: If True, pins on primary troves are
        ignored otherwise they are treated normally.
        @param resolveRepos: If True, search the repository for resolution
        troves.
        @rtype: tuple
        """
        callback.preparingChangeSet()

        uJob = database.UpdateJob(self.db)

        useAffinity = False

        if fromChangesets:
            csSource = trovesource.ChangesetFilesTroveSource(self.db)
            for cs in fromChangesets:
                csSource.addChangeSet(cs, includesFileContents = True)
                # FIXME ChangeSetSource: We shouldn't have to add this to 
                # uJob.troveSource() at this point, since the 
                # changeset is not part of the job yet.  But given the 
                # way changeSetSource.createChangeSet is written
                # (it can't handle recursive changeSet creation, e.g.)
                # we have no choice.  Search FIXME ChangeSetSource for 
                # a matching comment
                uJob.getTroveSource().addChangeSet(cs,
                                                   includesFileContents = True)

            uJob.setSearchSource(csSource)
        elif sync:
            uJob.setSearchSource(trovesource.ReferencedTrovesSource(self.db))
        else:
            uJob.setSearchSource(self.repos)
            useAffinity = True

        jobSet, splittable = self._updateChangeSet(itemList, uJob,
                                        keepExisting = keepExisting,
                                        recurse = recurse,
                                        updateMode = updateByDefault,
                                        useAffinity = useAffinity,
                                        ignorePrimaryPins = ignorePrimaryPins)
        split = split and splittable
        updateThreshold = self.cfg.updateThreshold

        # When keep existing is provided none of the changesets should
        # be relative (since relative change sets, by definition, cause
        # something on the system to get replaced).
        if keepExisting:
            for job in jobSet:
                if job[1][0] is not None:
                    raise UpdateError, 'keepExisting specified for a ' \
                                       'relative change set'

        callback.resolvingDependencies()

        # this updates jobSet w/ resolutions, and splitJob reflects the
        # jobs in the updated jobSet
        (depList, suggMap, cannotResolve, splitJob) = \
            self._resolveDependencies(uJob, jobSet, split = split,
                                      resolveDeps = resolveDeps,
                                      useRepos = resolveRepos)

        if depList:
            raise DepResolutionFailure(depList)
        elif suggMap and not self.cfg.autoResolve:
            raise NeededTrovesFailure(suggMap)
        elif cannotResolve:
            raise EraseDepFailure(cannotResolve)

        # look for troves which look like they'll conflict (same name/branch
        # and incompatible install paths)
        if not sync and checkPathConflicts:
            d = {}
            conflicts = {}
            for job in jobSet:
                if not job[2][0]: continue
                name, branch = job[0], job[2][0].branch()
                l = d.setdefault((name, branch), [])
                l.append(job)

            for jobList in d.values():
                if len(jobList) < 2: continue
                trvs = uJob.getTroveSource().getTroves(
                        [ (x[0], x[2][0], x[2][1]) for x in jobList ],
                        withFiles = False)
                paths = [ x.getPathHashes() for x in trvs ]
                
                for i, job in enumerate(jobList):
                    for j in range(i):
                        if not paths[i].compatibleWith(paths[j]):
                            l = conflicts.setdefault(job[0], [])
                            l.append((job[2], jobList[j][2]))

            if conflicts:
                raise InstallPathConflicts(conflicts)

        if split:
            startNew = True
            newJob = []
            for jobList in splitJob:
                if startNew:
                    newJob = []
                    startNew = False
                    count = 0

                foundCollection = False

                count += len(jobList)
                for job in jobList:
                    (name, (oldVersion, oldFlavor),
                           (newVersion, newFlavor), absolute) = job

                    if newVersion is not None and ':' not in name:
                        foundCollection = True

                    newJob.append(job)

                if (foundCollection or 
                    (updateThreshold and (count >= updateThreshold))): 
                    uJob.addJob(newJob)
                    startNew = True

            if not startNew:
                uJob.addJob(newJob)
        else:
            uJob.addJob(jobSet)

        return (uJob, suggMap)

    def applyUpdate(self, uJob, replaceFiles = False, tagScript = None, 
                    test = False, justDatabase = False, journal = None, 
                    localRollbacks = False, callback = UpdateCallback(),
                    autoPinList = conarycfg.RegularExpressionList(), threshold = 0):

        def _createCs(repos, job, uJob, standalone = False):
            baseCs = changeset.ReadOnlyChangeSet()
            cs, remainder = uJob.getTroveSource().createChangeSet(job, 
                                        recurse = False, withFiles = True,
                                        withFileContents = True,
                                        useDatabase = False)
            baseCs.merge(cs)
            if remainder:
                newCs = repos.createChangeSet(remainder, recurse = False,
                                              callback = callback)
                baseCs.merge(newCs)

            return baseCs

        def _applyCs(cs, uJob, removeHints = {}):
            try:
                rb = self.db.commitChangeSet(cs, uJob,
                                    replaceFiles = replaceFiles,
                                    tagScript = tagScript, test = test, 
                                    justDatabase = justDatabase,
                                    journal = journal, callback = callback,
                                    localRollbacks = localRollbacks,
                                    removeHints = removeHints,
                                    autoPinList = autoPinList,
                                    threshold = threshold,)
            except database.CommitError, e:
                raise UpdateError, "changeset cannot be applied"

            return rb

        def _createAllCs(q, allJobs, uJob, cfg, stopSelf):
	    # reopen the local database so we don't share a sqlite object
	    # with the main thread
	    db = database.Database(cfg.root, cfg.dbPath)
	    repos = NetworkRepositoryClient(cfg.repositoryMap,
					    localRepository = db)
            callback.setAbortEvent(stopSelf)

            for i, job in enumerate(allJobs):
                if stopSelf.isSet():
                    return

                callback.setChangesetHunk(i + 1, len(allJobs))
                newCs = _createCs(repos, job, uJob)
                q.put(newCs)

            callback.setAbortEvent(None)

            q.put(None)

            # returning terminates the thread

        # def applyUpdate -- body begins here

        allJobs = uJob.getJobs()
        if len(allJobs) == 1:
            # this handles change sets which include change set files
            callback.setChangesetHunk(0, 0)
            newCs = _createCs(self.repos, allJobs[0], uJob, standalone = True)
            callback.setUpdateHunk(0, 0)
            callback.setUpdateJob(allJobs[0])
            _applyCs(newCs, uJob)
            callback.updateDone()
        else:
            # build a set of everything which is being removed
            removeHints = set()
            for job in allJobs:
                removeHints.update([ (x[0], x[1][0], x[1][1])
                                        for x in job if x[1][0] is not None ])
                
            if not self.cfg.threaded:
                for i, job in enumerate(allJobs):
                    callback.setChangesetHunk(i + 1, len(allJobs))
                    newCs = _createCs(self.repos, job, uJob)
                    callback.setUpdateHunk(i + 1, len(allJobs))
                    callback.setUpdateJob(job)
                    _applyCs(newCs, uJob, removeHints = removeHints)
                    callback.updateDone()
            else:
                import Queue
                from threading import Event, Thread

                csQueue = Queue.Queue(5)
                stopDownloadEvent = Event()

                downloadThread = Thread(None, _createAllCs, args = 
                            (csQueue, allJobs, uJob, self.cfg, 
                             stopDownloadEvent))
                downloadThread.start()

                try:
                    i = 0
                    while True:
                        try:
                            newCs = csQueue.get(True, 10)
                        except Queue.Empty:
                            if downloadThread.isAlive():
                                continue
                            log.warning('download thread terminated '
                                        'unexpectedly')
                            break
                        if newCs is None:
                            break
                        i += 1
                        callback.setUpdateHunk(i, len(allJobs))
                        callback.setUpdateJob(allJobs[i - 1])
                        _applyCs(newCs, uJob, removeHints = removeHints)
                        callback.updateDone()
                finally:
                    stopDownloadEvent.set()
                    # the download thread _should_ respond to the stopDownloadEvent
                    # in ~5 seconds.
                    downloadThread.join(20)

                    if downloadThread.isAlive():
                        log.warning('timeout waiting for download thread to '
                                    'terminate -- closing database and exiting')
                        log.warning('the following traceback _may_ be related')
                        self.db.close()
                        tb = sys.exc_info()[2]
                        if tb:
                            tb = traceback.format_tb(tb)
                            print >>sys.stderr, ''.join(tb)
                        # this will kill the download thread as well
                        os.kill(os.getpid(), 15)
                    else:
                        # DEBUGGING NOTE: if you need to debug update code not
                        # related to threading, the easiest thing is to add 
                        # 'threaded False' to your conary config.
                        pass


class ClientError(Exception):
    """Base class for client errors"""

class UpdateError(ClientError):
    """Base class for update errors"""
    def display(self):
        return str(self)

class NoNewTrovesError(UpdateError):
    def __str__(self):
        return "no new troves were found"

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
        for (troveInfo, depSet) in self.failures:
            res.append("    %s:\n\t%s" %  \
                       (troveInfo[0], "\n\t".join(str(depSet).split("\n"))))
        return '\n'.join(res)

class EraseDepFailure(DepResolutionFailure):
    """ Unable to resolve dependencies due to erase """
    def getFailures(self):
        return self.failures

    def __str__(self):
        res = []
        res.append("Troves being removed create unresolved dependencies:")
        for (reqBy, depSet, providedBy) in self.failures:
            res.append("    %s requires %s:\n\t%s" %
                       (reqBy[0], ' or '.join(x[0] for x in providedBy),
                        "\n\t".join(str(depSet).split("\n"))))
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
        for ((reqName, reqVersion, reqFlavor), suggList) in self.suggMap.iteritems():
            res.append("    %s -> %s" % \
              (reqName, " ".join(["%s(%s)" % 
              (x[0], x[1].trailingRevision().asString()) for x in suggList])))
        return '\n'.join(res)

class InstallPathConflicts(UpdateError):

    def __str__(self):
        res = []
        res.append("Troves being installed appear to conflict:")
        for name, l in sorted(self.conflicts.iteritems()):
            res.append("   %s -> %s" % (name, 
                           " ".join([ "%s[%s]->%s[%s]" %
                                        (x[0][0].asString(),
                                         deps.formatFlavor(x[0][1]),
                                         x[1][0].asString(),
                                         deps.formatFlavor(x[1][1]))
                                     for x in l ])))

        return '\n'.join(res)
    
    def __init__(self, conflicts):
        self.conflicts = conflicts

