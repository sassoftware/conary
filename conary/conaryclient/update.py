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

from conary.callbacks import UpdateCallback
from conary import conarycfg
from conary.deps import deps
from conary.lib import log, util
from conary.lib import openpgpkey, openpgpfile
from conary.local import database
from conary.repository import changeset, trovesource
from conary.repository.netclient import NetworkRepositoryClient
from conary import trove, versions

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


            if installFlavor:
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
            else:
                flavoredList = [ (None, x) for x in troveTups ]

            trovesByNB = {}
            for installFlavor, (n,v,f) in flavoredList:
                b = v.branch()
                myTimeStamp = v.timeStamps()[-1]
                if installFlavor is None:
                    myScore = 0
                else:
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

        def _checkDeps(jobSet, trvSrc, findOrdering, resolveDeps):

            while True:
                (depList, cannotResolve, changeSetList) = \
                                self.db.depCheck(jobSet, uJob.getTroveSource(),
                                                 findOrdering = findOrdering)

                if not cannotResolve or not resolveDeps:
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
        # end checkDeps "while True" loop here

        # def _resolveDependencies() begins here

        pathIdx = 0
        (depList, cannotResolve, changeSetList) = \
                    _checkDeps(jobSet, uJob.getTroveSource(),
                               findOrdering = split, 
                               resolveDeps = resolveDeps)
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
                # We found good suggestions, merge in those troves. Items
                # which are being removed by the current job cannot be 
                # removed again.
                beingRemoved = set((x[0], x[1][0], x[1][1]) for x in
                                    jobSet if x[1][0] is not None )

                newJob = self._updateChangeSet(troves, uJob,
                                          keepExisting = False,
                                          recurse = False,
                                          ineligible = beingRemoved,
                                          checkPrimaryPins = False)
                assert(not (newJob & jobSet))
                jobSet.update(newJob)

                lastCheck = depList
                (depList, cannotResolve, changeSetList) = \
                            _checkDeps(jobSet, uJob.getTroveSource(),
                                       findOrdering = split, 
                                       resolveDeps = resolveDeps)
                if lastCheck != depList:
                    pathIdx = 0
            else:
                # we didnt find any suggestions; go on to the next label
                # in the search path
                pathIdx += 1

        return (depList, suggMap, cannotResolve, changeSetList)

    def _processRedirects(self, uJob, jobSet, recurse):
        """
        Looks for redirects in the change set, and returns a list of troves
        which need to be included in the update.  This returns redirectHack,
        which maps targets of redirections to the sources of those 
        redirections.
        """

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

    def _mergeGroupChanges(self, uJob, primaryJobList, transitiveClosure,
                           redirectHack, recurse, ineligible, 
                           checkPrimaryPins):


	def _findErasures(primaryErases, newJob, referencedTroves, recurse):
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

            jobQueue = util.IterableQueue()
            # The order of this chain matters. It's important that we handle
            # all of the erasures we already know about before getting to the
            # ones which are implicit. That gets the state right for ones
            # which are explicit (since arriving there implicitly gets
            # ignored). Handling updates from newJob first prevents duplicates
            # from primaryErases
            for job, isPrimary in itertools.chain(
                        itertools.izip(newJob, itertools.repeat(False)),
                        itertools.izip(primaryErases, itertools.repeat(True)), 
                        jobQueue):

                oldInfo = (job[0], job[1][0], job[1][1])

                if oldInfo[1] is None: 
                    # skip new installs
                    continue  

                if oldInfo in nodeIdx:
                    # See the note above about chain order (this wouldn't
                    # work w/o it)
                    continue

                if not self.db.hasTrove(*oldInfo):
                    # no need to erase something we don't have installed
                    continue

                # XXX this needs to be batched
                pinned = self.db.trovesArePinned([ oldInfo ])[0]

                # erasures which are part of an
                # update are guaranteed to occur
                if job in newJob:
                    assert(job[2][0])
                    state = ERASE
                    fromUpdate = True
                else:
                    # If it's pinned, we keep it.
                    if pinned:
                        state = KEEP
                        if isPrimary and checkPrimaryPins:
                            raise UpdatePinnedTroveError(oldInfo)
                    elif isPrimary:
                        # primary updates are guaranteed to occur (if the
                        # trove is not pinned).
                        state = ERASE
                    else:
                        state = UNKNOWN

                    fromUpdate = False

                assert(oldInfo not in nodeIdx)
                nodeIdx[oldInfo] = len(nodeList)
                nodeList.append([ oldInfo, state, [], fromUpdate ])

                if not recurse: continue

                if not trove.troveIsCollection(oldInfo[0]): continue
                trv = self.db.getTrove(withFiles = False, pristine = False,
                                       *oldInfo)

                for inclInfo in trv.iterTroveList():
                    jobQueue.add(((inclInfo[0], inclInfo[1:], (None, None), 
                                  False), False))

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


        def _getPathHashes(trvSrc, db, trv, inDb = False):
            if not trv.isCollection(): return trv.getPathHashes()

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

        def _troveTransitiveClosure(db, itemList):
            itemQueue = util.IterableQueue()
            fullSet = set()
            for item in itertools.chain(itemList, itemQueue):
                if item in fullSet: continue
                fullSet.add(item)

                if trove.troveIsCollection(item[0]): continue
                trv = db.getTrove(withFiles = False, pristine = False, *item)

                for x in trv.iterTroveList():
                    itemQueue.add(x)

            return fullSet

        # def _mergeGroupChanges -- main body begins here
        erasePrimaries =    set(x for x in primaryJobList 
                                    if x[2][0] is None)
        relativePrimaries = set(x for x in primaryJobList 
                                    if x[2][0] is not None and
                                       not x[3])
        absolutePrimaries = set(x for x in primaryJobList 
                                    if x[3])
        assert(len(relativePrimaries) + len(absolutePrimaries) +
               len(erasePrimaries) == len(primaryJobList))

        troveSource = uJob.getTroveSource()

        # ineligible needs to be a transitive closure when recurse is set
        if recurse:
            ineligible = _troveTransitiveClosure(self.db, ineligible)

        # Build the trove which contains all of the absolute change sets
        # we may need to install. Build a set of all of the trove names
        # in that trove as well.
        availableTrove = trove.Trove("@update", versions.NewVersion(),
                                     deps.DependencySet(), None)
        names = set()
        for job in transitiveClosure:
            if job[2][0] is None: continue
            if not job[3]: continue
            if (job[0], job[2][0], job[2][1]) in ineligible: continue

            availableTrove.addTrove(job[0], job[2][0], job[2][1],
                                    presentOkay = True)
            names.add(job[0])

        avail = set(availableTrove.iterTroveList())

        # Build the set of all relative install jobs (transitive closure)
        relativeUpdateJobs = set(job for job in transitiveClosure if
                                    job[2][0] is not None and not job[3])
        
        # Look for relative updates whose sources are not currently installed
        relativeUpdates = [ ((x[0], x[1][0], x[1][1]), x)
                    for x in relativeUpdateJobs if x[1][0] is not None]
        isPresentList = self.db.hasTroves([ x[0] for x in relativeUpdates ])

        for (info, job), isPresent in itertools.izip(relativeUpdates,
                                                     isPresentList):
            if not isPresent:
                relativeUpdateJobs.remove(job)
                newTrove = job[0], job[2][0], job[2][1]

                if newTrove not in avail:
                    ineligible.add(newTrove)

        # Get all of the currently installed and referenced troves which
        # match something being installed absolute. Troves being removed
        # through a relative changeset aren't allowed to be removed by
        # something else.
        (installedNotReferenced, installedAndReferenced, 
         referencedNotInstalled) = self.db.db.getCompleteTroveSet(names)

        installedTroves = installedNotReferenced | installedAndReferenced

        installedTroves.difference_update(ineligible)
        installedTroves.difference_update(
                (job[0], job[1][0], job[1][1]) for job in relativeUpdateJobs)
        referencedNotInstalled.difference_update(ineligible)
        referencedNotInstalled.difference_update(
                (job[0], job[1][0], job[1][1]) for job in relativeUpdateJobs)


        # The job between referencedTroves and installedTroves tells us
        # a lot about what the user has done to his system. 
        installedTrove = trove.Trove("@exists", versions.NewVersion(),
                                     deps.DependencySet(), None)

        [ installedTrove.addTrove(*x) for x in installedTroves ]
        referencedTrove = trove.Trove("@exists", versions.NewVersion(),
                                      deps.DependencySet(), None)
        [ referencedTrove.addTrove(*x) for x in referencedNotInstalled ]
        localUpdates = installedTrove.diff(referencedTrove)[2]
        localUpdatesByPresent = \
                 dict( ((job[0], job[2][0], job[2][1]), job[1]) for
                        job in localUpdates if job[1][0] is not None and
                                               job[2][0] is not None)
        localUpdatesByMissing = \
                 dict( ((job[0], job[1][0], job[1][1]), job[2]) for
                        job in localUpdates if job[1][0] is not None and
                                               job[2][0] is not None)


        # Troves which were locally updated to version on the same branch
        # no longer need to be listed as referenced. The trove which replaced
        # it is always a better match for the new items (installed is better
        # than not installed as long as the branches are the same). This
        # doesn't apply if the trove which was originally installed is
        # part of this update though, as troves which are referenced and
        # part of the update are handled separately.
        for job in localUpdates:
            if job[1][0] is not None and job[2][0] is not None and \
                     job[1][0].branch() == job[2][0].branch() and \
                     (job[0], job[1][0], job[1][1]) not in avail:
                del localUpdatesByPresent[(job[0], job[2][0], job[2][1])]
                referencedNotInstalled.remove((job[0], job[1][0], job[1][1]))

        del installedTrove, referencedTrove, localUpdates

        # Build the set of the incoming troves which are either already
        # installed or already referenced. This is purely for consistency
        # checking later on
        alreadyInstalled = installedTroves & avail
        alreadyReferenced = referencedNotInstalled & avail

        del avail

        # Remove the alreadyReferenced set from both the troves which are
        # already installed. This lets us get a good match for such troves
        # if we decide to install them later.
        referencedNotInstalled.difference_update(alreadyReferenced)

        existsTrv = trove.Trove("@update", versions.NewVersion(), 
                                deps.DependencySet(), None)
        [ existsTrv.addTrove(*x) for x in installedTroves ]
        [ existsTrv.addTrove(*x) for x in referencedNotInstalled ]

        jobList = availableTrove.diff(existsTrv)[2]

        installJobs = [ x for x in jobList if x[1][0] is     None and
                                              x[2][0] is not None ]
        updateJobs = [ x for x in jobList if x[1][0] is not None and
                                             x[2][0] is not None ]
        pins = self.db.trovesArePinned([ (x[0], x[1][0], x[1][1]) 
                                                    for x in updateJobs ])
        jobByNew = dict( ((job[0], job[2][0], job[2][1]), (job[1], pin)) for
                        (job, pin) in itertools.izip(updateJobs, pins) )
        jobByNew.update(
                   dict( ((job[0], job[2][0], job[2][1]), (job[1], False)) for
                        job in installJobs))
        
        del jobList, installJobs, updateJobs

        # Relative jobs override pins and need to match up against the
        # right thing.
        jobByNew.update(
                    dict( ((job[0], job[2][0], job[2][1]), (job[1], False)) for
                        job in relativeUpdateJobs))

        # first True means its primary, second True means install this by 
        # default (after all, these are all primaries)

        # Primaries always followLocalChanges. It will be reset for children
        # of primaries with no local changes to follow.
        newTroves = [ ((x[0], x[2][0], x[2][1]), True, True, True, True) 
                            for x in itertools.chain(absolutePrimaries, 
                                                     relativePrimaries) ]

        newJob = set()

        while newTroves:
            newInfo, isPrimary, byDefault, \
                    respectBranchAffinity, followLocalChanges = newTroves.pop(0)

            if newInfo in alreadyInstalled:
                # No need to install it twice
                continue
            elif newInfo in alreadyReferenced:
                # meaning: this trove is referenced by something installed, 
                # but is not installed itself.

                if isPrimary:
                    # They really want it installed this time. We removed
                    # this entry from the already-installed @update trove
                    # so byJob already tells us the best match for it.
                    pass
                else:
                    # We already know about this trove, and decided we
                    # don't want it. We do want to keep the item which
                    # replaced it though.
                    if newInfo in localUpdatesByMissing:
                        info = (newInfo[0],) + localUpdatesByMissing[newInfo]
                        alreadyInstalled.add(info)
                    continue

            replaced, pinned = jobByNew[newInfo]
            replacedInfo = (newInfo[0], replaced[0], replaced[1])

            childrenFollowLocalChanges = False
            if replaced[0] is not None:
                if replacedInfo in referencedNotInstalled:
                    # the trove on the local system is one that's referenced
                    # but not installed, so, normally we would not install
                    # this trove.
                    # BUT if this is a primary, we always want to have the
                    # update happen.  In the case of a primary trove, 
                    # if the referenced trove is replaced by another trove 
                    # on the the system (by a localUpdate) then we remove 
                    # that trove instead.  If not, we just install this 
                    # trove as a fresh update. 

                    # If we're not a primary, we need to check 
                    # followLocalChanges (which means that the parent trove 
                    # followed a local change). If followLocalChanges
                    # is False, we don't want to install the new version
                    # of this trove. If it is set, we will install the
                    # new trove (replacing any locally changed trove).

                    if not followLocalChanges:
                        continue

                    replaced = localUpdatesByMissing.get(replacedInfo, 
                                                         (None, None))

                    if replaced[0] is None and not isPrimary:
                        # we can't find a trove that's actually installed that
                        # matches this referenced trove to change the update
                        # into.
                        continue

                    childrenFollowLocalChanges = True
                        
                    replacedInfo = (replacedInfo[0], replaced[0], replaced[1])

                elif not isPrimary and not redirectHack.get(newInfo, (None,)):
                    # an empty list in redirectHack means the trove we're
                    # redirecting from wasn't installed, so we shouldn't install
                    # this trove either
                    continue
                
                if (respectBranchAffinity 
                    and replacedInfo in localUpdatesByPresent 
                    and replacedInfo in installedNotReferenced):
                    # meaning of if statement above:
                    # 1. we are trying to preserve branch affinity 
                    #    (this is turned off if we have a primary above us
                    #     that switched branches, see below)
                    # 2. the update is removing a version that is seen as a 
                    #    local change to the system (the diff between
                    #    referencedNotInstalled and installedTroves matched
                    #    this trove up with something not installed). 
                    # 3. The update is removing a version that is not 
                    #    referenced by anything that's installed on the 
                    #    system.

                    notInstalledBranch = replacedInfo[1].branch()
                    localUpdateBranch = \
                            localUpdatesByPresent[replacedInfo][0].branch()
                    newBranch = newInfo[1].branch()
                
                    # The trove being removed was explicitly updated to the
                    # trove on localUpdateBranch. We don't want to replace 
                    # that trove if it was switched to a different branch, 
                    # unless the branch it was switched to is the same 
                    # as the branch for the newer version.
                    if (notInstalledBranch != localUpdateBranch and
                        notInstalledBranch != newBranch):
                        if isPrimary:
                            # Exception: if the trove that switched branches
                            # was listed on the command line, then the user
                            # is explicitly choosing to switch this trove to a 
                            # third branch.  For this trove at least, they 
                            # are overriding branch affinity.  We make sure
                            # child troves ignore branch affinity.
                            respectBranchAffinity = False
                        else:
                            continue
            elif not byDefault:
                # This trove is being newly installed, but it's not supposed
                # to be installed by default
                continue
            elif not isPrimary and self.cfg.excludeTroves.match(newInfo[0]):
                # New trove matches excludeTroves
                continue
            elif not isPrimary and not redirectHack.get(newInfo, (None,)):
                # an empty list in redirectHack means the trove we're
                # redirecting from wasn't installed, so we shouldn't install
                # this trove either
                continue

            trv = troveSource.getTrove(withFiles = False, *newInfo)

            if pinned:
                if replaced[0] is not None:

                    # try and install the two troves next to each other
                    assert(replacedInfo[1] is not None)
                    oldTrv = self.db.getTrove(withFiles = False, 
                                              pristine = False, *replacedInfo)
                    oldHashes = _getPathHashes(troveSource, self.db, oldTrv, 
                                               inDb = True)
                    newHashes = _getPathHashes(uJob.getTroveSource(), self.db,
                                               trv, inDb = False)

                    if newHashes.compatibleWith(oldHashes):
                        replaced = (None, None)
                        if isPrimary and checkPrimaryPins:
                            name = replacedInfo[0]
                            log.warning(
"""
Not removing old %s as part of update - it is pinned.
Installing new version of %s side-by-side instead.

To remove the old %s, run:
conary unpin '%s=%s%s'
conary erase '%s=%s%s'
""" % ((name, name, name) + replacedInfo + replacedInfo))
                    else:
                        if isPrimary and checkPrimaryPins:
                            raise UpdatePinnedTroveError(replacedInfo, newInfo)

                        continue

            newJob.add((newInfo[0], replaced, (newInfo[1], newInfo[2]), False))

            if not trv.isCollection(): continue
            if not recurse: continue

            if replaced[0] and replaced[0].branch() == newInfo[1].branch():
                # if this trove didn't switch branches, then we respect branch
                # affinity for all child troves even the primary trove above us
                # did switch.  We assume the user at some point switched this 
                # trove to the desired branch by hand already.
                respectBranchAffinity = True

            for info in trv.iterTroveList():
                if info in ineligible:
                    continue

                newTroves.append((info, False, 
                                  trv.includeTroveByDefault(*info),
                                  respectBranchAffinity,
                                  childrenFollowLocalChanges))

	eraseSet = _findErasures(erasePrimaries, newJob, alreadyInstalled, 
                                 recurse)
        assert(not x for x in newJob if x[2][0] is None)
        newJob.update(eraseSet)

        # items which were updated to redirects should be removed, no matter
        # what
        for info in set(itertools.chain(*redirectHack.values())):
            newJob.add((info[0], (info[1], info[2]), (None, None), False))

        return newJob

    def _updateChangeSet(self, itemList, uJob, keepExisting = None, 
                         recurse = True, updateMode = True, sync = False,
                         useAffinity = True, checkPrimaryPins = True,
                         forceJobClosure = False, ineligible = set()):
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

        def _jobTransitiveClosure(db, troveSource, jobSet):
            # This is an expensive operation. Use it carefully.
            jobQueue = util.IterableQueue()
            jobClosure = set()

            for job in itertools.chain(jobSet, jobQueue):
                if job in jobClosure:
                    continue

                if job[2][0] is None:
                    continue

                jobClosure.add(job)
                if not trove.troveIsCollection(job[0]): continue

                if job[1][0] is None:
                    oldTrv = None
                else:
                    oldTrv = db.getTroves((job[0], job[1][0], job[1][1]),
                                     withFiles = False, pristine = False)[0]
                    if oldTrv is None:
                        # XXX batching these would be much more efficient
                        oldTrv = troveSource.getTrove(job[0], job[1][0],
                                                      job[1][1], 
                                                      withFiles = False)

                newTrv = troveSource.getTrove(job[0], job[2][0], job[2][1],
                                              withFiles = False)

                recursiveJob = newTrv.diff(oldTrv, absolute = job[3])[2]
                for x in recursiveJob:
                    jobQueue.add(x)

            return jobClosure

        # def _updateChangeSet -- body starts here

        # This job describes updates from a networked repository. Duplicates
        # (including installing things already installed) are skipped.
        newJob = set()
        # These are items being removed.
        removeJob = set()
        # This is the full, transitive closure of the job
        transitiveClosure = set()

        toFind = {}
        toFindNoDb = {}
        for item in itemList:
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
            results.update(searchSource.findTroves(self.cfg.installLabelPath,
                                                   toFind))
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
        jobSet, oldItems = _separateInstalledItems(newJob)
        log.debug("items already installed: %s", oldItems)

        jobSet.update(removeJob)
        del newJob, removeJob

        # we now have two things
        #   1. oldItems -- items which we should not remove as a side effect
        #   2. jobSet -- job we need to create a change set for

        if not jobSet:
            raise NoNewTrovesError

        if jobSet:
            # FIXME changeSetSource: I should just be able to call 
            # csSource.createChangeSet but it can't handle recursive
            # createChangeSet calls, and you can only call createChangeSet
            # once on a csSource.  So, we avoid having to call it more than
            # once by checking to see if the changeSets are already in the
            # update job.
            hasTroves = uJob.getTroveSource().hasTroves(
                [ (x[0], x[2][0], x[2][1]) for x in jobSet ] )

            reposChangeSetList = set([ x[1] for x in
                              itertools.izip(hasTroves, jobSet)
                               if x[0] is not True ])

            if reposChangeSetList != jobSet:
                # we can't trust the closure from the changeset we're getting
                # since we're not getting everything for jobSet
                forceJobClosure = True

            csSource = trovesource.TroveSourceStack(
                                         uJob.getSearchSource(),
                                         self.repos)

            cs, notFound = csSource.createChangeSet(reposChangeSetList, 
                                                    withFiles = False,
                                                    recurse = recurse)
            assert(not notFound)
            uJob.getTroveSource().addChangeSet(cs)
            transitiveClosure.update(cs.getJobSet(primaries = False))
            del cs

        redirectHack = self._processRedirects(uJob, jobSet, recurse) 

        if forceJobClosure and recurse:
            # The transitiveClosure we computed can't be trusted; we need
            # to build another one. We could do this all the time, but it's
            # expensive
            transitiveClosure = _jobTransitiveClosure(self.db,
                                            trovesource.stack(
                                                uJob.getTroveSource(),
                                                self.repos), jobSet)
        elif forceJobClosure:
            transitiveClosure = jobSet
        # else we trust the transitiveClosure which was passed in

        newJob = self._mergeGroupChanges(uJob, jobSet, transitiveClosure,
                                         redirectHack, recurse, 
                                         oldItems | ineligible, 
                                         checkPrimaryPins)
        if not newJob:
            raise NoNewTrovesError

        uJob.setPrimaryJobs(jobSet)

        return newJob

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
                        checkPathConflicts = True, checkPrimaryPins = True,
                        resolveRepos = True):
        """
        Creates a changeset to update the system based on a set of trove update
        and erase operations. If self.cfg.autoResolve is set, dependencies
        within the job are automatically closed.

	@param itemList: A list of change specs: 
        (troveName, (oldVersionSpec, oldFlavor), (newVersionSpec, newFlavor),
        isAbsolute).  isAbsolute specifies whether to try to find an older
        version of trove on the system to replace if none is specified.
	If updateByDefault is True, trove names in itemList prefixed
	by a '-' will be erased. If updateByDefault is False, troves without a
	prefix will be erased, but troves prefixed by a '+' will be updated.
        @type itemList: [(troveName, (oldVer, oldFla), 
                         (newVer, newFla), isAbs), ...]
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
        @param checkPrimaryPins: If True, pins on primary troves raise a 
        warning if an update can be made while leaving the old trove in place,
        or an error, if the update/erase cannot be made without removing the 
        old trove.
        @param resolveRepos: If True, search the repository for resolution
        troves.
        @rtype: tuple
        """
        callback.preparingChangeSet()

        uJob = database.UpdateJob(self.db)

        useAffinity = False
        forceJobClosure = False
        splittable = True

        if fromChangesets:
            # when --from-file is used we need to explicitly compute the
            # transitive closure for our job. we normally trust the 
            # repository to give us the right thing, but that won't
            # work when we're pulling jobs out of the change set
            forceJobClosure = True
            splitabble = False

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

            uJob.setSearchSource(trovesource.stack(csSource, self.repos))
            splittable = False
        elif sync:
            uJob.setSearchSource(trovesource.ReferencedTrovesSource(self.db))
        else:
            uJob.setSearchSource(self.repos)
            useAffinity = True

        jobSet = self._updateChangeSet(itemList, uJob,
                                       keepExisting = keepExisting,
                                       recurse = recurse,
                                       updateMode = updateByDefault,
                                       useAffinity = useAffinity,
                                       checkPrimaryPins = checkPrimaryPins,
                                       forceJobClosure = forceJobClosure)
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
                    autoPinList = conarycfg.RegularExpressionList(),
                    threshold = 0):

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
                self.db.commitChangeSet(cs, uJob,
                                        replaceFiles = replaceFiles,
                                        tagScript = tagScript, test = test,
                                        justDatabase = justDatabase,
                                        journal = journal, callback = callback,
                                        localRollbacks = localRollbacks,
                                        removeHints = removeHints,
                                        autoPinList = autoPinList,
                                        threshold = threshold)
            except Exception, e:
                # an exception happened, clean up
                rb = uJob.getRollback()
                if rb:
                    # remove the last entry from this rollback set
                    # (which is the rollback entriy that roll back
                    # applying this changeset)
                    rb.removeLast()
                    # if there aren't any entries left in the rollback,
                    # remove it altogether, unless we're about to try again
                    if (rb.getCount() == 0):
                        self.db.removeLastRollback()
                # rollback the current transaction
                self.db.db.rollback()
                if isinstance(e, database.CommitError):
                    raise UpdateError, "changeset cannot be applied"
                raise

        def _createAllCs(q, allJobs, uJob, cfg, stopSelf):
	    # reopen the local database so we don't share a sqlite object
	    # with the main thread
	    db = database.Database(cfg.root, cfg.dbPath)
	    repos = NetworkRepositoryClient(cfg.repositoryMap,
                                            cfg.user,
					    localRepository = db)
            callback.setAbortEvent(stopSelf)

            for i, job in enumerate(allJobs):
                if stopSelf.isSet():
                    return

                callback.setChangesetHunk(i + 1, len(allJobs))
                newCs = _createCs(repos, job, uJob)

                while True:
                    # block for no more than 5 seconds so we can
                    # check to see if we should sbort
                    try:
                        q.put(newCs, True, 5)
                        break
                    except Queue.Full:
                        # if the queue is full, check to see if the
                        # other thread wants to quit
                        if stopSelf.isSet():
                            return

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
                            # get the next changeset object from the
                            # download thread.  Block for 10 seconds max
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
                    # the download thread _should_ respond to the
                    # stopDownloadEvent in ~5 seconds.
                    downloadThread.join(20)

                    if downloadThread.isAlive():
                        log.warning('timeout waiting for download '
                                    'thread to terminate -- closing '
                                    'database and exiting')
                        self.db.close()
                        tb = sys.exc_info()[2]
                        if tb:
                            log.warning('the following traceback may be '
                                        'related:')
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

class UpdatePinnedTroveError(UpdateError):
    """An attempt to update/erase a pinned trove."""
    def __init__(self, pinnedTrove, newVersion=None):
        self.pinnedTrove = pinnedTrove
        self.newVersion = newVersion
        
    def __str__(self):
        name = self.pinnedTrove[0]
        if self.newVersion:
            return """\
Not removing old %s as part of update - it is pinned.
Therefore, the new version cannot be installed.

To upgrade %s, run:
conary unpin '%s=%s[%s]'
and then repeat your update command
""" % ((name, name) + self.pinnedTrove)
        else:
            return """\
Not erasing %s - it is pinned.

To erase this %s, run:
conary unpin '%s=%s[%s]'
conary erase '%s=%s[%s]'
""" % ((name, name) + self.pinnedTrove + self.pinnedTrove)

            

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

