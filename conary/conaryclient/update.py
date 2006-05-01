#
# Copyright (c) 2004-2005 rPath, Inc.
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

import itertools
import os
import traceback
import sys

from conary import conarycfg
from conary.callbacks import UpdateCallback
from conary.conaryclient import resolve
from conary.deps import deps
from conary.errors import ClientError
from conary.lib import log, util
from conary.local import database
from conary.repository import changeset, trovesource
from conary.repository.errors import TroveMissing
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
                             resolveDeps = True, useRepos = True,
                             resolveSource = None):
        return self.resolver.resolveDependencies(uJob, jobSet, split=split,
                                                 resolveDeps=resolveDeps,
                                                 useRepos=useRepos,
                                                 resolveSource=resolveSource)

    def _processRedirects(self, csSource, uJob, jobSet, transitiveClosure,
                          recurse):
        """
        Looks for redirects in the change set, and returns a list of troves
        which need to be included in the update.  This returns redirectHack,
        which maps targets of redirections to the sources of those 
        redirections.
        """

        troveSet = {}
        redirectHack = {}

        jobsToRemove = set()
        jobsToAdd = set()

        # We don't have to worry about non-primaries recursively included
        # from the job because groups can't include redirects, so any redirect
        # must either be a primary or have a parent who is a primary.
        #
        # All of this itertools stuff lets us iterate through the jobSet
        # with isPrimary set to True and then iterate through the jobs we
        # create here with isPrimary set to False.

        # The outer loop is to allow redirects to point to redirects. The
        # inner loop handles one set of troves.

        initialSet = itertools.izip(itertools.repeat(True), jobSet)
        while initialSet:
            alreadyHandled = set()
            nextSet = set()
            toDoSet = util.IterableQueue()
            for isPrimary, job in itertools.chain(initialSet, toDoSet):
                (name, (oldVersion, oldFlavor), (newVersion, newFlavor),
                    isAbsolute) = job
                item = (name, newVersion, newFlavor)

                if item in alreadyHandled:
                    continue
                alreadyHandled.add(item)

                if newVersion is None:
                    # Erasures don't involve redirects so they aren't 
                    # interesting.
                    continue

                trv = uJob.getTroveSource().getTrove(name, newVersion, 
                                                     newFlavor, 
                                                     withFiles = False)

                if not trv.isRedirect():
                    continue

                if item in redirectHack:
                    # this was from a redirect to a redirect -- the list
                    # of what needs to be removed as part of this redirect
                    # needs to move to the new target
                    redirectSourceList = redirectHack[item]
                    del redirectHack[item]
                else:
                    redirectSourceList = []

                if isPrimary:
                    # Don't install a redirect
                    jobsToRemove.add(job)

                if not recurse:
                    raise UpdateError, \
                        "Redirect found with --no-recurse set: %s=%s[%s]" % item

                allTargets = [ (x[0], str(x[1]), x[2]) 
                                        for x in trv.iterRedirects() ]
                matches = self.repos.findTroves([], allTargets, self.cfg.flavor,
                                                affinityDatabase = self.db)
                if not matches:
                    assert(not allTargets)
                    l = redirectHack.setdefault(None, redirectSourceList)
                    l.append(item)
                else:
                    for matchList in matches.itervalues():
                        for match in matchList:
                            if match in redirectSourceList:
                                raise UpdateError, \
                                    "Redirect loop found which includes " \
                                    "troves %s, %s" % (item[0],
                                    ", ".join(x[0] for x in redirectSourceList))
                            assert(match not in redirectSourceList)
                            l = redirectHack.setdefault(match, 
                                                        redirectSourceList)
                            l.append(item)
                            redirectJob = (match[0], (None, None),
                                                     match[1:], True)
                            nextSet.add((isPrimary, redirectJob))
                            if isPrimary:
                                jobsToAdd.add(redirectJob)

                    for info in trv.iterTroveList(strongRefs = True):
                        toDoSet.add((False, 
                                     (info[0], (None, None), info[1:], True)))

            # The targets of redirects need to be loaded - but only
            # if they're not already in the job.
            nextSet = list(nextSet)
            hasTroves = uJob.getTroveSource().hasTroves([
                            (x[1][0], x[1][2][0], x[1][2][1]) for x in nextSet])
            nextSet = [ x[0] for x in itertools.izip(nextSet, hasTroves) \
                                                                  if not x[1] ]

            redirectCs, notFound = csSource.createChangeSet(
                    [ x[1] for x in nextSet ],
                    withFiles = False, recurse = True)
            uJob.getTroveSource().addChangeSet(redirectCs)
            transitiveClosure.update(redirectCs.getJobSet(primaries = False))

            initialSet = nextSet

        # We may remove some jobs which we add due to redirection chains.
        jobSet.update(jobsToAdd)
        jobSet.difference_update(jobsToRemove)

        for l in redirectHack.itervalues():
            outdated = self.db.outdatedTroves(l)
            del l[:]
            for (name, newVersion, newFlavor), \
                  (oldName, oldVersion, oldFlavor) in outdated.iteritems():
                if oldVersion is not None:
                    l.append((oldName, oldVersion, oldFlavor))

        return redirectHack

    def _mergeGroupChanges(self, uJob, primaryJobList, transitiveClosure,
                           redirectHack, recurse, ineligible, checkPrimaryPins,
                           installedPrimaries, installMissingRefs=False, 
                           updateOnly=False, respectBranchAffinity=True,
                           alwaysFollowLocalChanges=False,
                           removeNotByDefault = False):


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
                                       withDeps = False, *oldInfo)

                for inclInfo in trv.iterTroveList(strongRefs=True):
                    # we only use strong references when erasing.
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
            # to install - the only troves we list here are primary installs,
            # and they should never be erased.
            for info in referencedTroves:
                if info in nodeIdx:
                    node = nodeList[nodeIdx[info]]
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
            for info in trv.iterTroveList(strongRefs=True):
                # FIXME: should this include weak references?
                if inDb:
                    otherTrv = db.getTrove(withFiles = False, *info)
                elif trvSrc.hasTrove(*info):
                    otherTrv = trvSrc.getTrove(withFiles = False, *info)
                else:
                    # if the trove is not in the trove source, then it 
                    # can't be part of the update job.  This can happen 
                    # for example if you're just installing a package with
                    # no-recurse.
                    continue

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

                if not trove.troveIsCollection(item[0]): continue
                trv = db.getTrove(withFiles = False, pristine = False, *item)

                for x in trv.iterTroveList(strongRefs=True, weakRefs=True):
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

        log.debug('_mergeGroupChanges(recurse=%s,'
                      ' checkPrimaryPins=%s,'
                      ' installMissingRefs=%s, '
                      ' updateOnly=%s, '
                      ' respectBranchAffinity=%s,'
                      ' alwaysFollowLocalChanges=%s)' % 
                      (recurse, checkPrimaryPins, installMissingRefs,
                       updateOnly, respectBranchAffinity, 
                       alwaysFollowLocalChanges))

        troveSource = uJob.getTroveSource()


        # ineligible needs to be a transitive closure when recurse is set
        if recurse:
            ineligible = _troveTransitiveClosure(self.db, ineligible)

        for job in erasePrimaries:
            # an erase primary can't be part of an update (but their children
            # can, so add this after we've recursed)
            ineligible.add((job[0], job[1][0], job[1][1]))

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

        avail = set(availableTrove.iterTroveList(strongRefs=True))

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

        # skip relative installs that are already present.
        relativeInstalls = [ ((x[0], x[2][0], x[2][1]), x)
                         for x in relativeUpdateJobs if x[2][0] is not None]
        isPresentList = self.db.hasTroves([ x[0] for x in relativeInstalls ])
        for (newTrove, job), isPresent in itertools.izip(relativeInstalls,
                                                         isPresentList):
            if isPresent:
                relativeUpdateJobs.remove(job)
                ineligible.add(newTrove)
                if job[1][0]:
                    # this used to be a relative upgrade, but the target
                    # is installed, so turn it into an erase.
                    erasePrimaries.add((job[0], (job[1][0], job[1][1]), 
                                                (None, None), False))

        # Get all of the currently installed and referenced troves which
        # match something being installed absolute. Troves being removed
        # through a relative changeset aren't allowed to be removed by
        # something else.
        (installedNotReferenced, 
         installedAndReferenced, 
         referencedStrong,
         referencedWeak) = self.db.db.getCompleteTroveSet(names)

        installedTroves = installedNotReferenced | installedAndReferenced
        referencedNotInstalled = referencedStrong | referencedWeak
        log.debug('referencedNotInstalled: %s' % (referencedNotInstalled,))
        log.debug('ineligible: %s' % (ineligible,))

        installedTroves.difference_update(ineligible)
        installedTroves.difference_update(
                (job[0], job[1][0], job[1][1]) for job in relativeUpdateJobs)
        referencedNotInstalled.difference_update(ineligible)
        referencedNotInstalled.difference_update(
                (job[0], job[1][0], job[1][1]) for job in relativeUpdateJobs)


        # The job between referencedTroves and installedTroves tells us
        # a lot about what the user has done to his system. 
        localUpdates = self.getPrimaryLocalUpdates(names)
        if localUpdates:
            localUpdates += self.getChildLocalUpdates(uJob.getSearchSource(),
                                                      localUpdates,
                                                      installedTroves,
                                                      referencedNotInstalled)
            # make some assertions about the local updates:
            # 1. a missing trove can only be a part of one local update
            # 2. a present trove can only be a part of one local update

            # although we needed parent updates to get the correct set of
            # local updates related to this job, we don't care local updates
            # that aren't related to troves in our job.
            localUpdates = [ x for x in localUpdates if x[0] in names ]

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

        # keep track of troves that are changes on the same branch, 
        # since those are still explicit user requests and might 
        # override implied updates that would downgrade this trove.
        sameBranchLocalUpdates = {}

        for job in sorted(localUpdates):
            if job[1][0] is not None and job[2][0] is not None:
                if (job[1][0].branch() == job[2][0].branch() and
                      (job[0], job[1][0], job[1][1]) not in avail):
                    del localUpdatesByPresent[(job[0], job[2][0], job[2][1])]
                    del localUpdatesByMissing[(job[0], job[1][0], job[1][1])]
                    referencedNotInstalled.remove((job[0], job[1][0], job[1][1]))
                    log.debug('reworking same-branch local update: %s' % (job,))
                    # track this update for since it means the user
                    # requested this version explicitly
                    sameBranchLocalUpdates[job[0], job[2][0], job[2][1]] = (job[1][0], job[1][1])
                else:
                    log.debug('local update: %s' % (job,))

        del localUpdates

        # Build the set of the incoming troves which are either already
        # installed or already referenced. 
        alreadyInstalled = (installedTroves & avail) | installedPrimaries
        alreadyReferenced = referencedNotInstalled & avail

        del avail


        existsTrv = trove.Trove("@update", versions.NewVersion(), 
                                deps.DependencySet(), None)
        [ existsTrv.addTrove(*x) for x in installedTroves ]
        [ existsTrv.addTrove(*x) for x in referencedNotInstalled ]

        jobList = availableTrove.diff(existsTrv)[2]

        # alreadyReferenced troves are in both the update set 
        # and the installed set.  They are a good match for themselves.
        jobList += [(x[0], (x[1], x[2]), (x[1], x[2]), 0) for x in alreadyReferenced ]

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

        respectFlavorAffinity = True
        # thew newTroves parameters are described below.
        newTroves = sorted(((x[0], x[2][0], x[2][1]), 
                            True, {}, False, False, False, None, 
                            respectBranchAffinity, 
                            respectFlavorAffinity, True,
                            True, updateOnly) 
                                for x in itertools.chain(absolutePrimaries,
                                                         relativePrimaries))

        newJob = set()
        notByDefaultRemovals = set()

        # ensure the user-specified respect branch affinity setting is not 
        # lost.
        neverRespectBranchAffinity = not respectBranchAffinity

        while newTroves:
            # newTroves tuple values
            # newInfo: the (n, v, f) of the trove to install
            # isPrimary: true if user specified this trove on the command line
            # byDefaultDict: mapping of trove tuple to byDefault setting, as 
            #                specified by the primary parent trove 
            # parentInstalled: True if the parent of this trove was installed.
            #                  Used to determine whether to install troves 
            #                  with weak references.
            # parentUpdated: True if the parent of this trove was installed.
            #                or updated.
            # primaryInstalled: True if the primary that led to this update
            #                   was an install.
            # branchHint:  if newInfo's parent trove switched branches, this
            #              provides the to/from information on that switch.
            #              If this child trove is making the same switch, we 
            #              allow it even if the switch is overriding branch
            #              affinity.
            # respectBranchAffinity: If true, we generally try to respect
            #              the user's choice to switch a trove from one branch
            #              to another.  We might not respect branch affinity
            #              if a) a primary trove update is overriding branch
            #              affinity, or b) the call to mergeGroupChanges
            #              had respectBranchAffinity False
            # respectFlavorAffinity: If true, we generally try to respect
            #              the user's choice to switch a trove from one flavor
            #              to another.  We might not respect flavor affinity
            #              for the same reasons we might not respect branch
            #              affinity.
            # installRedirects: If True, we install redirects even when they
            #              are not upgrades.
            # followLocalChanges: see the code where it is used for a 
            #              description.
            # updateOnly:  If true, only update troves, don't install them
            #              fresh.

            (newInfo, isPrimary, byDefaultDict, parentInstalled, parentUpdated,
             primaryInstalled, branchHint, respectBranchAffinity, 
             respectFlavorAffinity,
             installRedirects, followLocalChanges, updateOnly) = newTroves.pop(0)

            byDefault = isPrimary or byDefaultDict[newInfo]

            log.debug('''\
*******
%s=%s[%s]
primary: %s  byDefault:%s  parentUpdated: %s parentInstalled: %s primaryInstalled: %s updateOnly: %s
branchHint: %s
branchAffinity: %s   flavorAffinity: %s installRedirects: %s
followLocalChanges: %s

''' % (newInfo[0], newInfo[1], newInfo[2], isPrimary, byDefault, 
       parentUpdated, parentInstalled, primaryInstalled,
       updateOnly, branchHint, respectBranchAffinity,
       respectFlavorAffinity, installRedirects, followLocalChanges))

            trv = None
            jobAdded = False
            replaced = (None, None)
            recurseThis = True
            childrenFollowLocalChanges = alwaysFollowLocalChanges

            while True:
                # this loop should only be called once - it's basically a 
                # way to create a quick GOTO structure, without needing 
                # to call another function (which would be expensive in 
                # this loop).
                if newInfo in alreadyInstalled:
                    # No need to install it twice
                    # but count it as 'added' for the purposes of
                    # whether or not to recurse
                    jobAdded = True
                    log.debug('SKIP: already installed')
                    break
                elif newInfo in ineligible:
                    log.debug('SKIP: ineligible')
                    break
                elif newInfo in alreadyReferenced:
                    log.debug('new trove in alreadyReferenced')
                    # meaning: this trove is referenced by something 
                    # installed, but is not installed itself.

                    if isPrimary or installMissingRefs or primaryInstalled:
                        # They really want it installed this time. We removed
                        # this entry from the already-installed @update trove
                        # so localUpdates already tells us the best match for it.
                        pass
                    elif parentUpdated and newInfo in referencedWeak:
                        # The only link to this trove is a weak reference.
                        # A weak-only reference means an intermediate trove 
                        # was missing.  But parentUpdated says we've now
                        # updated an intermediate trove, so install
                        # this trove too.
                        pass
                    else:
                        # We already know about this trove, and decided we
                        # don't want it. We do want to keep the item which
                        # replaced it though.
                        if newInfo in localUpdatesByMissing:
                            info = ((newInfo[0],) 
                                     + localUpdatesByMissing[newInfo])
                            alreadyInstalled.add(info)
                            log.debug('local update - marking present part %s'
                                      'as already installed' % (info,))
                        log.debug('SKIP: already referenced')
                        break

                replaced, pinned = jobByNew[newInfo]
                replacedInfo = (newInfo[0], replaced[0], replaced[1])

                log.debug('replaces: %s' % (replacedInfo,))

                if replaced[0] is not None:
                    if newInfo in alreadyReferenced:
                        # This section is the corrolary to the section
                        # above.  We only enter here if we've decided
                        # to install this trove even though its 
                        # already referenced.  
                        if replacedInfo in referencedNotInstalled:
                            # don't allow this trove to not be installed
                            # because the trove its replacing is not installed.
                            # Find an installed update or just install the trove
                            # fresh.
                            replaced = localUpdatesByMissing.get(replacedInfo, 
                                                                 (None, None))
                            replacedInfo = (replacedInfo[0], replaced[0], 
                                            replaced[1])
                            log.debug('replaced is not installed, using local update %s instead' % (replacedInfo,))
                            if replaced[0]:
                                log.debug('following local changes')
                                childrenFollowLocalChanges = True
                    elif replacedInfo in referencedNotInstalled:
                        # the trove on the local system is one that's referenced
                        # but not installed, so, normally we would not install
                        # this trove.
                        # BUT if this is a primary (or in certain other cases)
                        # we always want to have the update happen.  
                        # In the case of a primary trove, 
                        # if the referenced trove is replaced by another trove 
                        # on the the system (by a localUpdate) then we remove 
                        # that trove instead.  If not, we just install this 
                        # trove as a fresh update. 
                        log.debug('replaced trove is not installed')

                        if not (parentInstalled
                                or followLocalChanges or installMissingRefs):
                            # followLocalChanges states that, even though
                            # the given trove is not a primary, we still want
                            # replace a localUpdate if available instead of 
                            # skipping the update.  This flag can be set if
                            # a) an ancestor of this trove is a primary trove
                            # that switched from a referencedNotInstalled
                            # to an installed trove or b) its passed in to
                            # the function that we _always_ follow local 
                            # changes.
                            log.debug('SKIP: not following local changes')
                            break

                        freshInstallOkay = (isPrimary or parentInstalled 
                                            or installMissingRefs)
                        # we always want to install the trove even if there's
                        # no local update to match to if it's a primary, or
                        # if the trove's parent was just installed 
                        # (if the parent was installed, we just added a
                        # strong reference, which overrides any other
                        # references that might suggest not to install it.)

                        replaced = localUpdatesByMissing.get(replacedInfo, 
                                                             (None, None))


                        if (replaced[0] is None and not freshInstallOkay):
                            log.debug('SKIP: not allowing fresh install')
                            break

                        childrenFollowLocalChanges = True

                        replacedInfo = (replacedInfo[0], replaced[0], 
                                        replaced[1])
                        log.debug('using local update to replace %s, following local changes' % (replacedInfo,))

                    elif not installRedirects:
                        if not redirectHack.get(newInfo, True):
                            # a parent redirect was added as an upgrade
                            # but this would be a new install of this child
                            # trove.  Skip it.
                            log.debug('SKIP: is a redirect that would be'
                                      ' a fresh install, but '
                                      ' installRedirects=False')
                            break
                    elif redirectHack.get(newInfo, False):
                        # we are upgrading a redirect, so don't allow any child
                        # redirects to be installed unless they have a matching
                        # trove to redirect on the system.
                        log.debug('INSTALL: upgrading redirect')
                        installRedirects = False

                    if replaced[0] and respectBranchAffinity: 
                        log.debug('checking branch affinity')
                        # do branch affinity checks

                        newBranch = newInfo[1].branch()
                        installedBranch = replacedInfo[1].branch()

                        if replacedInfo in localUpdatesByPresent:
                            notInstalledVer = \
                                        localUpdatesByPresent[replacedInfo][0]
                            notInstalledBranch = notInstalledVer.branch()
                            # create alreadyBranchSwitch variable for 
                            # readability
                            alreadyBranchSwitch = True
                        else:
                            notInstalledBranch = None
                            alreadyBranchSwitch = False


                        # Check to see if there's reason to be concerned
                        # about branch affinity.
                        if installedBranch == newBranch:
                            log.debug('not branch switch')
                            # we didn't switch branches.  No branch 
                            # affinity concerns.  If the user has made
                            # a local change that would make this new 
                            # install a downgrade, skip it.
                            if not isPrimary:
                                if replacedInfo in sameBranchLocalUpdates:
                                    notInstalledFlavor = \
                                        sameBranchLocalUpdates[replacedInfo][1]

                                if (newInfo[1] < replaced[0]
                                    and replacedInfo in sameBranchLocalUpdates):
                                    log.debug('SKIP: avoiding downgrade')

                                    # don't let this trove be erased, pretend
                                    # like it was explicitly requested.
                                    alreadyInstalled.add(replacedInfo)
                                    break
                        elif notInstalledBranch == installedBranch:
                            log.debug('INSTALL: branch switch is reversion')
                            # we are reverting back to the branch we were
                            # on before.  We don't worry about downgrades
                            # because we're already overriding the user's
                            # branch choice
                            pass
                        else:
                            log.debug('is a new branch switch')
                            # Either a) we've made a local change from branch 1
                            # to branch 2 and now we're updating to branch 3,
                            # or b) there's no local change but we're switching
                            # branches.

                            # Generally, we respect branch affinity and don't
                            # do branch switches.  There 
                            # are a few exceptions:
                            if isPrimary:
                                # the user explicitly asked to switch
                                # to this branch, so we have to honor it.

                                if alreadyBranchSwitch:
                                    # it turns out the _current_ installed
                                    # trove is a local change.  The user
                                    # is messing with branches too much -
                                    # don't bother with branch affinity.
                                    respectBranchAffinity = False
                                    log.debug('INSTALL: is a branch switch on top of a branch switch and is primary')
                                else:
                                    log.debug('INSTALL: is a new branch switch and is primary')
                            elif (installedBranch, newBranch) == branchHint:
                                # Exception: if the parent trove
                                # just made this move, then allow it.
                                log.debug('INSTALL: matches parent\'s branch switch')
                                pass
                            elif ((replacedInfo in installedAndReferenced
                                   or replacedInfo in sameBranchLocalUpdates)
                                  and not alreadyBranchSwitch
                                  and parentUpdated):
                                # Exception: The user has not switched this
                                # trove's branch explicitly, and now
                                # we have an implicit request to switch 
                                # the branch.
                                log.debug('INSTALL: implicit branch switch, parent installed')
                                pass
                            else:
                                # we're not installing this trove - 
                                # It doesn't match any of our exceptions.
                                # It could be that it's a trove with
                                # no references to it on the system
                                # (and so a branch switch would be strange)
                                # or it could be that it is a switch
                                # to a third branch by the user.
                                # Since we're rejecting the update due to 
                                # branch affinity, we don't consider any of its 
                                # child troves for updates either.
                                log.debug('SKIP: not installing branch switch')
                                recurseThis = False 
                                alreadyInstalled.add(replacedInfo)
                                break

                        if replaced[0] and respectFlavorAffinity:
                            if replacedInfo in localUpdatesByPresent:
                                notInstalledFlavor = \
                                        localUpdatesByPresent[replacedInfo][1]
                                # create alreadyBranchSwitch variable for 
                                # readability
                                alreadyFlavorSwitch = True
                            elif replacedInfo in sameBranchLocalUpdates:
                                notInstalledFlavor = \
                                        sameBranchLocalUpdates[replacedInfo][1]
                            else:
                                notInstalledFlavor = None

                            if (notInstalledFlavor
                                and not deps.compatibleFlavors(
                                                           notInstalledFlavor,
                                                           replacedInfo[2])
                                and not deps.compatibleFlavors(replacedInfo[2],
                                                               newInfo[2])):
                                if isPrimary:
                                    respectFlavorAffinity = False
                                else:
                                    log.debug('SKIP: Not reverting'
                                              ' incompatible flavor switch')
                                    recurseThis = False
                                    alreadyInstalled.add(replacedInfo)
                                    break



                # below are checks to see if a fresh install should completed.
                # Since its possible that an update from above could be 
                # converted into a fresh install, we start a new if/elif 
                # branch here.
                if replaced[0]:
                    # we are dealing with a replacement, we've already 
                    # decided it was okay above.
                    pass
                elif not byDefault:
                    # This trove is being newly installed, but it's not 
                    # supposed to be installed by default
                    log.debug('SKIP: not doing not-by-default fresh install')
                    break
                elif updateOnly:
                    # we're not installing trove, only updating installed
                    # troves.
                    log.debug('SKIP: not doing install due to updateOnly')
                    break
                elif not isPrimary and self.cfg.excludeTroves.match(newInfo[0]):
                    # New trove matches excludeTroves
                    log.debug('SKIP: trove matches excludeTroves')
                    break
                elif not installRedirects:
                    if not redirectHack.get(newInfo, True):
                        # a parent redirect was added as an upgrade
                        # but this would be a new install of this child
                        # trove.  Skip it.
                        log.debug('SKIP: redirect would be a fresh install')
                        break
                elif redirectHack.get(newInfo, False):
                    # we are upgrading a redirect, so don't allow any child
                    # redirects to be installed unless they have a matching
                    # trove to redirect on the system.
                    log.debug('installing redirect')
                    installRedirects = False
                
                if pinned:
                    if replaced[0] is not None:
                        log.debug('looking at pinned replaced trove')
                        try:
                            trv = troveSource.getTrove(withFiles = False, 
                                                       *newInfo)
                        except TroveMissing:
                            # we don't even actually have this trove available,
                            # making it difficult to install.
                            log.debug('SKIP: new trove is not in source,'
                                      ' cannot compare path hashes!')
                            recurseThis = False
                            break

                        # try and install the two troves next to each other
                        assert(replacedInfo[1] is not None)
                        oldTrv = self.db.getTrove(withFiles = False, 
                                                  pristine = False, 
                                                  *replacedInfo)
                        oldHashes = _getPathHashes(troveSource, self.db, 
                                                   oldTrv, inDb = True)
                        newHashes = _getPathHashes(uJob.getTroveSource(), 
                                                   self.db, trv, inDb = False)

                        if newHashes.compatibleWith(oldHashes):
                            log.debug('old and new versions are compatible')
                            replaced = (None, None)
                            if isPrimary and checkPrimaryPins:
                                name = replacedInfo[0]
                                log.warning(
"""
Not removing old %s as part of update - it is pinned.
Installing new version of %s side-by-side instead.

To remove the old %s, run:
conary unpin '%s=%s[%s]'
conary erase '%s=%s[%s]'
""" % ((name, name, name) + replacedInfo + replacedInfo))
                        else:
                            if not isPrimary:
                                recurseThis = False
                                break
                            elif checkPrimaryPins:
                                raise UpdatePinnedTroveError(replacedInfo, 
                                                             newInfo)

                job = (newInfo[0], replaced, (newInfo[1], newInfo[2]), False)
                log.debug('JOB ADDED: %s' % (job,))
                newJob.add(job)
                jobAdded = True
                break

            log.debug('recurseThis: %s\nrecurse: %s' % (recurseThis, recurse))

            if jobAdded and removeNotByDefault and not byDefault:
                job = (newInfo[0], replaced, (newInfo[1], newInfo[2]), False)
                newJob.discard(job)
                if replaced[0]:
                    notByDefaultRemovals.add(
                                (newInfo[0], replaced, (None, None), False))
                elif newInfo in alreadyInstalled:
                    notByDefaultRemovals.add(
                                (newInfo[0], (newInfo[1], newInfo[2]),
                                             (None, None), False))
                    alreadyInstalled.discard(newInfo)

            if not recurseThis: continue
            if not recurse: continue
            if not trove.troveIsCollection(newInfo[0]): continue


            branchHint = None
            if replaced[0] and replaced[0].branch() == newInfo[1].branch():
                # if this trove didn't switch branches, then we respect branch
                # affinity for all child troves even the primary trove above us
                # did switch.  We assume the user at some point switched this 
                # trove to the desired branch by hand already.
                log.debug('respecting branch affinity for children')
                if not neverRespectBranchAffinity:
                    respectBranchAffinity = True
            elif replaced[0]:
                branchHint = (replaced[0].branch(), newInfo[1].branch())

            if replaced[0] and deps.compatibleFlavors(replaced[1], newInfo[2]):
                log.debug('respecting flavor affinity for children')
                respectFlavorAffinity = True

            if trv is None:
                try:
                    trv = troveSource.getTrove(withFiles = False, *newInfo)
                except TroveMissing:
                    # it's possible that the trove source we're using
                    # contains references to troves that it does not 
                    # actually contain.  That's okay as long as the
                    # excluded trove is not actually trying to be
                    # installed.
                    if jobAdded:
                        raise
                    else:
                        continue

            if isPrimary:
                # byDefault status of troves is determined by the primary
                # trove.  
                byDefaultDict = dict((x[0], x[1]) \
                                            for x in trv.iterTroveListInfo())

            updateOnly = updateOnly or not jobAdded
            # for all children, we only want to install them as new _if_ we 
            # installed their parent.  If we did not install/upgrade foo, then
            # we do not install foo:runtime (though if it's installed, it
            # is reasonable to upgrade it).

            if isPrimary:
                primaryInstalled = jobAdded and not job[1][0]
            
            jobInstall = primaryInstalled or (jobAdded and not job[1][0])

            for info in sorted(trv.iterTroveList(strongRefs=True)):

                if not isPrimary:
                    if not jobAdded and info not in byDefaultDict:
                        continue

                    # support old-style collections.  _If_ this trove was not
                    # mentioned in its parent trove, then set its default
                    # value here. 
                    childByDefault = (trv.includeTroveByDefault(*info)
                                      and jobAdded)
                    byDefaultDict.setdefault(info, childByDefault)

                newTroves.append((info, False,
                                  byDefaultDict, jobInstall, jobAdded,
                                  primaryInstalled,
                                  branchHint, respectBranchAffinity,
                                  respectFlavorAffinity, installRedirects,
                                  childrenFollowLocalChanges,
                                  updateOnly))

        for job in notByDefaultRemovals:
            if job not in newJob:
                erasePrimaries.add((job[0], job[1], (None, None), False))

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
                         forceJobClosure = False, ineligible = set(),
                         syncChildren=False, updateOnly=False,
                         installMissing = False, removeNotByDefault = False):
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
        if syncChildren:
            installMissing = True

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

        if searchSource.requiresLabelPath():
            installLabelPath = self.cfg.installLabelPath
        else:
            installLabelPath = None

        if not useAffinity:
            results.update(searchSource.findTroves(installLabelPath, toFind))
        else:
            if toFind:
                log.debug("looking up troves w/ database affinity")
                results.update(searchSource.findTroves(
                                        installLabelPath, toFind, 
                                        self.cfg.flavor,
                                        affinityDatabase=self.db))
            if toFindNoDb:
                log.debug("looking up troves w/o database affinity")
                results.update(searchSource.findTroves(
                                           installLabelPath, 
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

        if not installMissing:
            jobSet, oldItems = _separateInstalledItems(newJob)
        else:
            # keep our original jobSet, we'll recurse through installed
            # items as well.
            jobSet, oldItems = newJob, _separateInstalledItems(newJob)[1]
            
        log.debug("items already installed: %s", oldItems)

        jobSet.update(removeJob)
        del newJob, removeJob

        # we now have two things
        #   1. oldItems -- items which we should not remove as a side effect
        #   2. jobSet -- job we need to create a change set for

        if not jobSet:
            raise NoNewTrovesError

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

        csSource = trovesource.stack(uJob.getSearchSource(),
                                     self.repos)

        cs, notFound = csSource.createChangeSet(reposChangeSetList, 
                                                withFiles = False,
                                                recurse = recurse)
        self._replaceIncomplete(cs, csSource, 
                                self.db, self.repos)
        assert(not notFound)
        uJob.getTroveSource().addChangeSet(cs)
        transitiveClosure.update(cs.getJobSet(primaries = False))
        del cs

        redirectHack = self._processRedirects(csSource, uJob, jobSet, 
                                              transitiveClosure, recurse) 

        if forceJobClosure and recurse:
            # The transitiveClosure we computed can't be trusted; we need
            # to build another one. We could do this all the time, but it's
            # expensive
            transitiveClosure = _jobTransitiveClosure(self.db,
                                            trovesource.stack(
                                                uJob.getTroveSource(),
                                                self.repos), jobSet)
            # Since we couldn't trust the transitive closure generated,
            # we need to check to see if any of the recursive troves we'll
            # need are not in the changeset.  This will be true 
            # of group changesets.
            transitiveJobs = list(transitiveClosure)
            hasTroves = uJob.getTroveSource().hasTroves(
                            (x[0], x[2][0], x[2][1]) for x in transitiveJobs)

            reposChangeSetList = set([ x[1] for x in
                              itertools.izip(hasTroves, transitiveJobs)
                               if x[0] is not True ])

            csSource = trovesource.stack(uJob.getSearchSource(),
                                         self.repos)
            cs, notFound = csSource.createChangeSet(reposChangeSetList, 
                                                    withFiles = False,
                                                    recurse = recurse)
            self._replaceIncomplete(cs, csSource, self.db, self.repos)
            #NOTE: we allow any missing recursive bits to be skipped.
            #They'll show up in notFound.
            #assert(not notFound)
            uJob.getTroveSource().addChangeSet(cs)
        elif forceJobClosure:
            transitiveClosure = jobSet
        # else we trust the transitiveClosure which was passed in

        if not installMissing:
            # we know that all the troves in jobSet are already installed
            # (i.e. in oldItems) when syncing.  We don't want to exclude 
            # their children from syncing
            ineligible = ineligible | oldItems

        newJob = self._mergeGroupChanges(uJob, jobSet, transitiveClosure,
                                 redirectHack, recurse, ineligible, 
                                 checkPrimaryPins, 
                                 installedPrimaries=oldItems, 
                                 installMissingRefs=installMissing,
                                 updateOnly=updateOnly,
                                 respectBranchAffinity=not installMissing,
                                 alwaysFollowLocalChanges=installMissing,
                                 removeNotByDefault = removeNotByDefault)

        if not newJob:
            raise NoNewTrovesError

        uJob.setPrimaryJobs(jobSet)

        return newJob

    def fullUpdateItemList(self):
        # ignore updates that just switch version, not flavor or 
        # branch
        items = ( x for x in self.getPrimaryLocalUpdates() 
                  if (x[1][0] is None
                      or not deps.compatibleFlavors(x[1][1], x[2][1])
                      or x[1][0].branch() != x[2][0].branch()))
        items = [ (x[0], x[2][0], x[2][1]) for x in items
                   if not x[2][0].isOnLocalHost() ]
        items = [ x[0] for x in itertools.izip(items,
                                               self.db.trovesArePinned(items))
                                                                  if not x[1] ]


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

    def getPrimaryLocalUpdates(self, troveNames=None):
        """
            Returns a set of changes (jobs) that explain how the user is likely
            to have modified their system to get it to its current state.

            The changes made are the top-level jobs, that is, if the user
            updated foo (which includes foo:runtime) from branch a to branch b,
            an update job for foo will be returned but not foo:runtime.

            If troveNames are specified, then the changes returned are those
            _related_ to the given trove name.  They may include changes 
            of troves with other names, however.  For example, if you 
            request changes for troves named foo, and foo is included by
            group-dist, and the only change related to foo you have made
            is installing group-dist, then a job showing the install of
            group-dist will be returned.

            @rtype: list of jobs
        """
        if troveNames is not None and not troveNames:
            return []

        allJobs = []        # allJobs is returned from this fn

        noParents = []      # troves with no parents that could be part of
                            # unknown local updates.

        troves = []         # troveId -> troveInfo map (troveId == index)
                            # contains (troveTup, isPresent, hasParent)

        maxId = 0           # next index for troves list
        troveIdsByInfo = {} # (name,ver,flavor) -> troveId 

        parentIds = {}      # name -> [parents of troves w/ name, troveIds]
        childIds = {}       # troveId -> childIds

        # 1. Create needed data structures
        #    troves, parentIds, childIds
        for (troveInfo, parentInfo, isPresent) \
                                in self.db.iterUpdateContainerInfo(troveNames):
            troveId = troveIdsByInfo.setdefault(troveInfo, maxId)
            if troveId == maxId:
                maxId += 1
                troves.append([troveInfo, isPresent, bool(parentInfo)])
            else:
                if isPresent:
                    troves[troveId][1] = True
                if parentInfo:
                    troves[troveId][2] = True

            parentId = troveIdsByInfo.setdefault(parentInfo, maxId)
            if parentId == maxId:
                maxId += 1
                troves.append([parentInfo, False, False])

            l = parentIds.setdefault(troveInfo[0], (set(), []))
            l[1].append(troveId)

            if parentId:
                childIds.setdefault(parentId, []).append(troveId)
                l[0].add(parentId)

        del troveIdsByInfo, maxId

        # remove troves that don't are not present and have no parents - they 
        # won't be part of local updates.
        allTroves = set(x[0] for x in enumerate(troves) if x[1][1] or x[1][2])
        [ x[0].intersection_update(allTroves) for x in parentIds.itervalues() ]
        del allTroves

        noParents = (x[1][1] for x in parentIds.iteritems() if not x[1][0])
        noParents = set(itertools.chain(*noParents))

        while noParents:
            exists = trove.Trove('@update', versions.NewVersion(),
                                 deps.DependencySet(), None)
            refd = trove.Trove('@update', versions.NewVersion(),
                               deps.DependencySet(), None)

            for troveId in noParents:
                info, isPresent, hasParent = troves[troveId] 
                if isPresent:
                    exists.addTrove(presentOkay=True, *info)
                else:
                    refd.addTrove(presentOkay=True, *info)

            updateJobs = [  ]

            allJobs.extend(x for x in exists.diff(refd)[2] if x[2][0])

            # we've created all local updates related to this set of
            # troves - remove them as parents of other troves to generate
            # next noParent set.
            toDiscard = {}
            for troveId in noParents:
                for childId in childIds.get(troveId, []):
                    toDiscard.setdefault(troves[childId][0][0],
                                         []).append(troveId)

            newNoParents = []
            for name, troveIds in toDiscard.iteritems():
                parentIds[name][0].difference_update(troveIds)
                if not parentIds[name][0]:
                    newNoParents.extend(parentIds[name][1])
            del toDiscard

            noParents = set(newNoParents) - noParents

        return allJobs

    def getChildLocalUpdates(self, searchSource, localUpdates,
                             installedTroves=None, missingTroves=None):
        """
            Given a set of primary local updates - the updates the user
            is likely to have typed at the command line, return their
            child updates.  Given a primary update from a -> b, we look 
            at the children of a and b and see if a child of a is not 
            installed where a child of b is, and assert that that update is 
            from childa -> childb.
        """
        localUpdates = [ x for x in localUpdates 
                         if x[1][0] and not x[1][0].isOnLocalHost() ]
        oldTroveTups = [ (x[0], x[1][0], x[1][1]) for x in localUpdates ]
        newTroveTups = [ (x[0], x[2][0], x[2][1]) for x in localUpdates ]

        oldTroveSource = trovesource.stack(searchSource, self.repos)
        oldTroves = oldTroveSource.getTroves(oldTroveTups, withFiles=False)
        newTroves = self.db.getTroves(newTroveTups, withFiles=False)

        if installedTroves is None:
            assert(missingTroves is None)
            chain = itertools.chain
            izip = itertools.izip
            childNew = list(set(chain(*(x.iterTroveList(strongRefs=True,
                                                           weakRefs=True)
                                                        for x in newTroves))))
            childOld = list(set(chain(*(x.iterTroveList(strongRefs=True,
                                                        weakRefs=True)
                                                        for x in oldTroves))))
            hasTroves = self.db.hasTroves(childNew + childOld)
            installedTroves = set(x[0] for x in izip(childNew, hasTroves) 
                                                if x[1])
            installedTroves.update(newTroveTups)

            hasTroves = hasTroves[len(childNew):]
            missingTroves = set(x[0] for x in izip(childOld, hasTroves) 
                                              if not x[1])
            missingTroves.update(oldTroveTups)
            del childNew, childOld, hasTroves
        else:
            assert(missingTroves is not None)
            installedTroves = installedTroves.copy()
            missingTroves = missingTroves.copy()

        allJobs = []
        for oldTrove, newTrove in itertools.izip(oldTroves, newTroves):
            # find the relevant local updates by performing a 
            # diff between oldTrove and a trove based on newTrove
            # that contains only those parts of newTrove that are actually
            # installed.

            notExistsOldTrove = trove.Trove('@update',
                                            versions.NewVersion(),
                                            deps.DependencySet())
            existsNewTrove = trove.Trove('@update',
                                         versions.NewVersion(),
                                         deps.DependencySet())

            # only create local updates between old troves that
            # don't exist and new troves that do.
            for tup, _, isStrong in oldTrove.iterTroveListInfo():
                if (tup in missingTroves and tup not in oldTroveTups
                    and not newTrove.hasTrove(*tup)):
                    notExistsOldTrove.addTrove(*tup)
            for tup, _, isStrong in newTrove.iterTroveListInfo():
                if (tup in installedTroves and tup not in newTroveTups
                    and not oldTrove.hasTrove(*tup)):
                    existsNewTrove.addTrove( *tup)

            newUpdateJobs = existsNewTrove.diff(notExistsOldTrove)[2]

            for newJob in newUpdateJobs:
                if not newJob[1][0] or not newJob[2][0]:
                    continue

                # no trove should be part of more than one update.
                installedTroves.remove((newJob[0], newJob[2][0], newJob[2][1]))
                missingTroves.remove((newJob[0], newJob[1][0], newJob[1][1]))
                allJobs.append(newJob)
        return allJobs

    def _replaceIncomplete(self, cs, localSource, db, repos):
        jobSet = [ (x.getName(), (x.getOldVersion(), x.getOldFlavor()),
                                 (x.getNewVersion(), x.getNewFlavor()))
                    for x in cs.iterNewTroveList() ]
    
        incompleteJobs = [ x for x in jobSet 
                           if x[1][0] and x[2][0]
                              and not x[2][0].isOnLocalHost()
                              and db.hasTrove(x[0], *x[1]) 
                              and db.troveIsIncomplete(x[0], *x[1]) ]
        if incompleteJobs:
            newTroves = repos.getTroves([(x[0], x[2][0], x[2][1])
                                         for x in incompleteJobs])
            oldTroves = localSource.getTroves([(x[0], x[1][0], x[1][1])
                                               for x in incompleteJobs])
            newCs = changeset.ChangeSet()
            for newT, oldT in itertools.izip(newTroves, oldTroves):
                oldT.troveInfo.incomplete.set(1)
                newT.troveInfo.incomplete.set(0)
                newCs.newTrove(newT.diff(oldT)[0])

            cs.merge(newCs)



    def updateChangeSet(self, itemList, keepExisting = False, recurse = True,
                        resolveDeps = True, test = False,
                        updateByDefault = True, callback = UpdateCallback(),
                        split = True, sync = False, fromChangesets = [],
                        checkPathConflicts = True, checkPrimaryPins = True,
                        resolveRepos = True, syncChildren = False, 
                        updateOnly = False, resolveGroupList=None, 
                        installMissing = False, removeNotByDefault = False):
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
        @param split: Split large update operations into separate jobs. As
                      of 1.0.10, this must be true (False broke how we
                      handle users and groups, which requires info- packages
                      to be installed first and in separate jobs)
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
        @param syncChildren: If True, sync child troves so that they match
        the references in the specified troves.
        @param updateOnly: If True, do not install missing troves, just
        update installed troves.
        @param installMissing: If True, always install missing troves
        @param removeNotByDefault: remove child troves that are not by default.
        @rtype: tuple
        """
        # FIXME: this API has gotten far out of hand.  Refactor when 
        # non backwards compatible API changes are acceptable. 
        # In particular. installMissing and updateOnly have similar meanings,
        # (but expanding updateOnly meaning would require making incompatible
        # changes), split has lost meaning, keepExisting is also practically 
        # meaningless at this level.
        assert(split)
        callback.preparingChangeSet()

        uJob = database.UpdateJob(self.db)

        useAffinity = False
        forceJobClosure = False
        resolveSource = None

        if fromChangesets:
            # when --from-file is used we need to explicitly compute the
            # transitive closure for our job. we normally trust the 
            # repository to give us the right thing, but that won't
            # work when we're pulling jobs out of the change set
            forceJobClosure = True

            csSource = trovesource.ChangesetFilesTroveSource(self.db,
                                                             storeDeps=True)
            for cs in fromChangesets:
                self._replaceIncomplete(cs, self.db, self.db, self.repos)
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
        elif sync:
            uJob.setSearchSource(trovesource.ReferencedTrovesSource(self.db))
        elif syncChildren:
            uJob.setSearchSource(self.db)
        else:
            uJob.setSearchSource(self.repos)
            useAffinity = True

        if resolveGroupList:
            resolveRepos = False
            if useAffinity:
                affinityDb = self.db
            else:
                affinityDb = None

            result = self.repos.findTroves(self.cfg.installLabelPath,
                                           resolveGroupList,
                                           self.cfg.flavor,
                                           affinityDatabase=affinityDb)
            groupTups = list(itertools.chain(*result.itervalues()))
            groupTroves = self.repos.getTroves(groupTups, withFiles=False)
            resolveSource = resolve.DepResolutionByTroveList(self.cfg, self.db,
                                                             groupTroves)

        jobSet = self._updateChangeSet(itemList, uJob,
                                       keepExisting = keepExisting,
                                       recurse = recurse,
                                       updateMode = updateByDefault,
                                       useAffinity = useAffinity,
                                       checkPrimaryPins = checkPrimaryPins,
                                       forceJobClosure = forceJobClosure,
                                       syncChildren = syncChildren,
                                       updateOnly = updateOnly,
                                       installMissing = installMissing,
                                       removeNotByDefault = removeNotByDefault)
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
        (depList, suggMap, cannotResolve, splitJob, keepList) = \
            self._resolveDependencies(uJob, jobSet, split = split,
                                      resolveDeps = resolveDeps,
                                      useRepos = resolveRepos,
                                      resolveSource = resolveSource)

        if keepList:
            callback.done()
            for job, depSet, reqInfo in sorted(keepList):
                log.warning('keeping %s - required by at least %s' % (job[0], reqInfo[0]))

        if depList:
            raise DepResolutionFailure(depList, self.cfg)
        elif suggMap and not self.cfg.autoResolve:
            raise NeededTrovesFailure(suggMap, self.cfg)
        elif cannotResolve:
            raise EraseDepFailure(cannotResolve, self.cfg)

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

        startNew = True
        newJob = []
        for jobList in splitJob:
            if startNew:
                newJob = []
                startNew = False
                count = 0
                newJobIsInfo = False

            foundCollection = False

            count += len(jobList)
            isInfo = None                 # neither true nor false
            infoName = None
            for job in jobList:
                (name, (oldVersion, oldFlavor),
                       (newVersion, newFlavor), absolute) = job

                if newVersion is not None and ':' not in name:
                    foundCollection = True

                if name.startswith('info-'):
                    assert(isInfo is True or isInfo is None)
                    isInfo = True
                    if not infoName:
                        infoName = name.split(':')[0]
                else:
                    assert(isInfo is False or isInfo is None)
                    isInfo = False

            if (not isInfo or infoName != name) and newJobIsInfo is True:
                # We switched from installing info components to
                # installing fresh components. This has to go into
                # a separate job from the last one.
                # FIXME: We also require currently that each info 
                # job be for the same info trove - that is, can't
                # have info-foo and info-bar in the same update job
                # because info-foo might depend on info-bar being
                # installed already.  This should be fixed.
                uJob.addJob(newJob)
                count = len(jobList)
                newJob = list(jobList)             # make a copy
                newJobIsInfo = False
            else:
                newJobIsInfo = isInfo
                newJob += jobList

            if (foundCollection or 
                (updateThreshold and (count >= updateThreshold))): 
                uJob.addJob(newJob)
                startNew = True

        if not startNew:
            uJob.addJob(newJob)

        return (uJob, suggMap)

    def applyUpdate(self, uJob, replaceFiles = False, tagScript = None, 
                    test = False, justDatabase = False, journal = None, 
                    localRollbacks = False, callback = UpdateCallback(),
                    autoPinList = conarycfg.RegularExpressionList(),
                    threshold = 0):

        def _createCs(repos, db, jobSet, uJob, standalone = False):
            baseCs = changeset.ReadOnlyChangeSet()

            cs, remainder = uJob.getTroveSource().createChangeSet(jobSet,
                                        recurse = False, withFiles = True,
                                        withFileContents = True,
                                        useDatabase = False)
            baseCs.merge(cs)
            if remainder:
                newCs = repos.createChangeSet(remainder, recurse = False,
                                              callback = callback)
                baseCs.merge(newCs)

            self._replaceIncomplete(baseCs, db, db, repos)

            return baseCs

        def _applyCs(cs, uJob, removeHints = {}):
            # Before applying this job, reset the underlying changesets. This
            # lets us traverse user-supplied changesets multiple times.
            uJob.troveSource.reset()

            try:
                self.db.commitChangeSet(cs, uJob,
                        replaceFiles = replaceFiles, tagScript = tagScript, 
                        test = test, justDatabase = justDatabase,
                        journal = journal, callback = callback,
                        localRollbacks = localRollbacks,
                        removeHints = removeHints, autoPinList = autoPinList,
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
                    raise UpdateError, "changeset cannot be applied:\n%s" % e
                raise

        def _createAllCs(q, allJobs, uJob, cfg, stopSelf):
	    # reopen the local database so we don't share a sqlite object
	    # with the main thread
            # _createCs accesses the database through the uJob.troveSource,
            # so make sure that references this fresh db as well.
            db = database.Database(cfg.root, cfg.dbPath)
            uJob.troveSource.db = db
            repos = NetworkRepositoryClient(cfg.repositoryMap,
                                            cfg.user,
                                            downloadRateLimit =
                                                cfg.downloadRateLimit,
                                            uploadRateLimit =
                                                cfg.uploadRateLimit,
                                            localRepository = db)
            callback.setAbortEvent(stopSelf)

            for i, job in enumerate(allJobs):
                if stopSelf.isSet():
                    return

                callback.setChangesetHunk(i + 1, len(allJobs))
                newCs = _createCs(repos, db, job, uJob)

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
            newCs = _createCs(self.repos, self.db, allJobs[0], uJob, 
                              standalone = True)
            callback.setUpdateHunk(0, 0)
            callback.setUpdateJob(allJobs[0])
            _applyCs(newCs, uJob)
            callback.updateDone()
        else:
            # build a set of everything which is being removed
            removeHints = dict()
            for job in allJobs:
                # the None in this dict means that all files in this trove
                # should be overridden
                removeHints.update([ ((x[0], x[1][0], x[1][1]), None)
                                        for x in job if x[1][0] is not None ])

            if not self.cfg.threaded:
                for i, job in enumerate(allJobs):
                    callback.setChangesetHunk(i + 1, len(allJobs))
                    newCs = _createCs(self.repos, self.db, job, uJob)
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
    def formatNVF(self, troveTup, showVersion=True):
        if not self.cfg:
            return '%s=%s' % (troveTup[0], troveTup[1].trailingRevision())
        if self.cfg.fullVersions:
            version = troveTup[1]
        elif self.cfg.showLabels:
            version = '%s/%s' % (troveTup[1].branch().label(), 
                                 troveTup[1].trailingRevision())
        elif showVersion:
            version = troveTup[1].trailingRevision()
        else:
            version = ''

        if version:
            version = '=%s' % version

        if self.cfg.fullFlavors:
            flavor = '[%s]' % troveTup[2]
        else:
            flavor = ''

        return '%s%s%s' % (troveTup[0], version, flavor)

class DepResolutionFailure(DependencyFailure):
    """ Unable to resolve dependencies """
    def __init__(self, failures, cfg=None):
        self.failures = failures
        self.cfg = cfg

    def getFailures(self):
        return self.failures

    def __str__(self):
        res = ["The following dependencies could not be resolved:"]
        for (troveInfo, depSet) in self.failures:
            res.append("    %s:\n\t%s" %  \
                       (self.formatNVF(troveInfo),
                        "\n\t".join(str(depSet).split("\n"))))
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
                       (self.formatNVF(reqBy),
                        ' or '.join(self.formatNVF(x) for x in providedBy),
                        "\n\t".join(str(depSet).split("\n"))))
        return '\n'.join(res)

class NeededTrovesFailure(DependencyFailure):
    """ Dependencies needed and resolve wasn't used """
    def __init__(self, suggMap, cfg=None):
         self.suggMap = suggMap
         self.cfg = cfg

    def getSuggestions(self):
        return self.suggMap

    def __str__(self):
        res = []
        res.append("Additional troves are needed:")
        for (reqInfo, suggList) in self.suggMap.iteritems():
            res.append("    %s -> %s" % \
              (self.formatNVF(reqInfo),
               " ".join([self.formatNVF(x) for x in suggList])))
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

