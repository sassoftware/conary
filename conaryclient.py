#
# Copyright (c) 2004-2005 Specifix, Inc.
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
from repository import repository
from repository import changeset
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

class VersionSuppliedError(UpdateError):
    def __str__(self):
        return "version should not be specified when a Conary change set " \
               "is being installed"

class NoNewTrovesError(UpdateError):
    def __str__(self):
        return "no new troves were found"

class UpdateJob:

    def addChangeSet(self, cs):
        self.csList.append(cs)

    def getChangeSets(self):
        return self.csList

    def addLockMapping(self, name, lockedVersion, neededVersion):
        self.lockMapping.add((name, lockedVersion, neededVersion))
    
    def getLockMaps(self):
        return self.lockMapping

    def __init__(self):
        self.csList = []
        self.lockMapping = set()

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

# use a special sort because:
# l = [ False, -1, 1 ]
# l.sort()
# l == [ -1, False, 1 ]
# note that also False == 0 sometimes
#
# secondary scoring is done on the final timestamp (so ties get broken
# by an explicit rule)
def _scoreSort(x, y):
    if x[0] is False:
        return -1
    if y[0] is False:
        return 1
    rc = cmp(x[0], y[0])
    if rc:
        return rc

    return cmp(x[1], y[1])

class ConaryClient:
    def __init__(self, cfg = None):
        if cfg == None:
            cfg = conarycfg.ConaryConfiguration()
            cfg.initializeFlavors()
        
        cfg.installLabel = cfg.installLabelPath[0]
        self.cfg = cfg
        self.db = database.Database(cfg.root, cfg.dbPath)
        self.repos = NetworkRepositoryClient(cfg.repositoryMap,
                                             localRepository = self.db)

    def _rootChangeSet(self, cs, keepExisting = False):
	troveList = [ (x.getName(), x.getNewVersion(), 
		       x.getNewFlavor()) 
			    for x in cs.iterNewTroveList() ]

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

            troves = {}
            if sugg:
                for (troveName, depSet) in depList:
                    if sugg.has_key(depSet):
                        suggList = []
                        for choiceList in sugg[depSet]:
                            # XXX what if multiple troves are on this branch,
                            # but with different flavors? we could be
                            # (much) smarter here
                            scoredList = []

                            # set up a list of affinity troves for each choice
                            if keepExisting:
                                affTroveList = [[]] * len(choiceList)
                            else:
                                affTroveList = []
                                for choice in choiceList:
                                    try:
                                        affinityTroves = self.db.findTrove(
                                                                        None, 
                                                                    choice[0])
                                        affTroveList.append(affinityTroves)
                                    except repository.TroveNotFound:
                                        affTroveList.append([])

                            found = False
                            # iterate over flavorpath -- use suggestions 
                            # from first flavor on flavorpath that gets a match 
                            for flavor in self.cfg.flavor:

                                for choice, affinityTroves in itertools.izip(
                                                                 choiceList, 
                                                                 affTroveList):
                                    f = flavor.copy()
                                    if affinityTroves:
                                        f.union(affinityTroves[0][2],
                                        mergeType=deps.DEP_MERGE_TYPE_PREFS)
                                    scoredList.append((f.score(choice[2]), 
                                       choice[1].trailingRevision().getTimestamp(),
                                       choice))
                                scoredList.sort(_scoreSort)
                                if scoredList[-1][0] is not False:
                                    choice = scoredList[-1][-1]
                                    suggList.append(choice)

                                    l = suggMap.setdefault(troveName, [])
                                    l.append(choice)
                                    found = True
                                    break

                                if found:
                                    # break out of searching flavor path
                                    # move on to the next dep that needs
                                    # to be filled
                                    break

			troves.update(dict.fromkeys(suggList))

                troves = troves.keys()
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

    def _processRedirects(self, cs, recurse):
        # Looks for redirects in the change set, and returns a list of
        # troves which need to be included in the update. 
        troveSet = {}
        delDict = {}
        redirectHack = {}
        primaries = dict.fromkeys(cs.getPrimaryTroveList())

        for troveCs in cs.iterNewTroveList():
            if not troveCs.getIsRedirect():
                continue

            if not recurse:
                raise UpdateError,  "Redirect found with --no-recurse set"

            item = (troveCs.getName(), troveCs.getNewVersion(),
                    troveCs.getNewFlavor())

            # don't install the redirection itself
            delDict[item] = True

            # but do remove the trove this redirection replaces. if it
            # isn't installed, we don't want this redirection or the
            # item it points to
            if troveCs.getOldVersion():
                oldItem = (troveCs.getName(), troveCs.getOldVersion(),
                           troveCs.getOldFlavor())

                if self.db.hasTrove(*oldItem):
                    cs.oldTrove(*oldItem)
                    # make all removals due to redirects be primary
                    cs.addPrimaryTrove(*oldItem)
                else:
                    # erase the target(s) of the redirection
                    for (name, changeList) in troveCs.iterChangedTroves():
                        for (changeType, version, flavor, byDef) in changeList:
                            delDict[(name, version, flavor)] = True

            targets = []
            for (name, changeList) in troveCs.iterChangedTroves():
                for (changeType, version, flavor, byDef) in changeList:
                    if changeType == '-': continue
                    if (":" not in name and ":" not in item[0]) or \
                       (":"     in name and ":"     in item[0]):
                        l = redirectHack.setdefault((name, version, flavor), [])
                        l.append(item)
                        targets.append((name, version, flavor))

            if item in primaries:
                for target in targets:
                    cs.addPrimaryTrove(*target)

        for item in delDict.iterkeys():
            if cs.hasNewTrove(*item):
                cs.delNewTrove(*item)

        for l in redirectHack.itervalues():
	    outdated, eraseList = self.db.outdatedTroves(l)
            del l[:]
            for (name, newVersion, newFlavor), \
                  (oldName, oldVersion, oldFlavor) in outdated.iteritems():
                if oldVersion is not None:
                    l.append((oldName, oldVersion, oldFlavor))

        return redirectHack

    def _mergeGroupChanges(self, cs, uJob, redirectHack, keepExisting, recurse):

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
                if redirectHack.has_key(info):
                    l = redirectHack[info]
                    present = self.db.hasTroves(l)
                    if sum(present) == 0:
                        # sum of booleans -- tee-hee
                        delList.append(info)
                        break

            for info in delList:
                localTrv.delTrove(*(info + (False,)))

            # now look for other versions of these troves (which are the
            # versions we'd like to replace; keep-existing is handled
            # elsewhere)
            outdated, toErase = self.db.outdatedTroves([ x for x in 
                                                localTrv.iterTroveList() ])

            for info in localTrv.iterTroveList():
                # we don't worry about duplicates here; _alreadyInstalled
                # will handle that for us a bit later
                (odName, odVersion, odFlavor) = outdated.get(info, 
                                                         (None, None, None))
                if odVersion is None: continue
                assert(odName == info[0])
                oldTrv.addTrove(odName, odVersion, odFlavor)

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

	def _findErasures(cs, primaryErases, recurse):
	    nodeList = []
	    nodeIdx = {}
	    ERASE = 1
	    KEEP = 2
	    UNKNOWN = 3

            for trvCs in cs.iterNewTroveList():
                if trvCs.getOldVersion() is None: continue
                info = (trvCs.getName(), trvCs.getOldVersion(),
                        trvCs.getOldFlavor())
                nodeIdx[info] = len(nodeList)
                nodeList.append([ info, ERASE, [], True ])

	    # this will traceback for primaries which aren't installed, and
	    # (rightfully) ignores locking for priamry troves
	    trvs = self.db.getTroves(primaryErases, pristine = False)
	    troveList = [ (info, trv, None) for info, trv in 
				itertools.izip(primaryErases, trvs) ]
	    while troveList:
		info, trv, fromTrove = troveList.pop()

		if info not in nodeIdx:
		    nodeId = len(nodeList)
		    nodeIdx[info] = nodeId
		    nodeList.append([info, UNKNOWN, [], False])

		if fromTrove is None:
		    nodeList[nodeId][1] = ERASE
		else:
		    nodeList[fromTrove][2].append(nodeId)
		
		if not trv or not recurse:
		    continue

		refTroveInfo = [ x for x in trv.iterTroveList() ]
		present = self.db.hasTroves(refTroveInfo)
		locked = self.db.trovesAreLocked(refTroveInfo)
		areContainers = [ not(x[0].startswith('fileset-') or 
				    x[0].find(":") != -1)
				    for x in refTroveInfo ]

		contList = []
		for (info, isPresent, isLocked, isContainer) in \
			itertools.izip(refTroveInfo, present, locked, 
				       areContainers):
		    if not isPresent or isLocked: continue
		    if not isContainer:
			troveList.append((info, None, nodeId))
		    else:   
			contList.append(info)

		trvs = self.db.getTroves(contList, pristine = False)
		troveList += [ (info, trv, nodeId) for info, trv in
				    itertools.izip(contList, trvs) ]

	    needParents = [ (nodeId, info) for nodeId, (info, state, edges,
                                                        alreadyHandled)
				in enumerate(nodeList) if state == UNKNOWN ]
            keepNodes = []
	    while needParents:
		containers = self.db.findTroveContainers([ x[1] for x
							    in needParents ])
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
                            keepNodes.append(containerId)
                needParents = newNeedParents
		    
	    seen = [ False ] * len(nodeList)
            # DFS to mark troves as KEEP
            while keepNodes:
                nodeId = keepNodes.pop()
                if seen[nodeId]: continue
                seen[nodeId] = True
                nodeList[nodeId][1] = KEEP
                keepNodes += nodeList[nodeId][2] 
                
            # anything which isn't to KEEP is to erase, but skip those which
            # are already being removed by a trvCs
            eraseList = [ (x[0][0], (x[0][1], x[0][2]), (None, None), False)
                                for x in nodeList if x[1] != KEEP and
                                                     not x[3] ]
            
            return eraseList

        # Updates a change set by removing troves which don't need
        # to be updated due to local state. It also removes new troves which
        # don't need to be installed because their byDefault is False
        assert(not cs.isAbsolute())

        primaries = cs.getPrimaryTroveList()
        keepList = []
        origJob = []

        # XXX it's crazy that we have to use the name of the trove to
        # figure out if it's a collection or not, but these changesets
        # are sans files (for performance), so that's what we're left
        # with

        for trvCs in cs.iterNewTroveList():
            if trvCs.getOldVersion():
                origJob.append((trvCs.getName(), 
                                (trvCs.getOldVersion(), trvCs.getOldFlavor()),
                                (trvCs.getNewVersion(), trvCs.getNewFlavor()),
                                False))
            else:
                origJob.append((trvCs.getName(), (None, None),
                                (trvCs.getNewVersion(), trvCs.getNewFlavor()),
                                False))

            item = (trvCs.getName(), trvCs.getNewVersion(),
                    trvCs.getNewFlavor())

            if item in primaries:
                keepList.append(origJob[-1])

        newJobList = []
        deferredList = []

        while True:
            if not keepList and deferredList:
                newCs = self.repos.createChangeSet(deferredList, 
                                                   withFiles = False)
                cs.merge(newCs)
                origJob += deferredList
                keepList = deferredList
                deferredList = []
            if not keepList:
                break

            job = keepList.pop()
            newJobList.append(job)
            (trvName, (oldVersion, oldFlavor), (newVersion, newFlavor), abs) \
                                = job

            if not recurse:
                continue

            if trvName.startswith('fileset-') or trvName.find(":") != -1:
                continue

            # collections should be in the changeset already. after all, it's
            # supposed to be recursive
            trvCs = cs.getNewTroveVersion(trvName, newVersion, newFlavor)

            if oldFlavor is None:
                oldFlavorSet = deps.DependencySet()
            else:
                oldFlavorSet = oldFlavor

            if trvCs.getOldVersion() != oldVersion or \
                    trvCs.oldFlavor() != oldFlavorSet:
                deferredList.append(job)
                continue

            if oldVersion is None:
                # Read the comments at the top of _newBase if you hope
                # to understand any of this.
                newPristine = trove.Trove(trvName, newVersion, newFlavor, None)
                newPristine.applyChangeSet(trvCs)
                (oldTrv, pristineTrv, localTrv) = _newBase(newPristine)
                newTrv = pristineTrv.copy()
                newTrv.mergeCollections(localTrv, newPristine)
                finalCs, fileList, neededTroveList = newTrv.diff(oldTrv)
            else:
                oldTrv = self.db.getTrove(trvName, oldVersion, oldFlavor,
                                       pristine = True)
                localTrv = self.db.getTrove(trvName, oldVersion, oldFlavor,
                                            pristine = False)
                assert(not oldTrv.hasFiles())
                assert(not localTrv.hasFiles())

                newPristine = oldTrv.copy()
                newPristine.applyChangeSet(trvCs)
                newTrv = oldTrv.copy()

                newTrv.mergeCollections(localTrv, newPristine)
                finalCs, fileList, neededTroveList = newTrv.diff(localTrv)
                del oldTrv
                assert(not fileList)
                alreadyInstalled = []

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
                        assert((name, oldVersion, oldFlavor) in
                                    cs.getOldTroveList())
                    else:
                        keepList.append((name, (oldVersion, oldFlavor),
                                               (newVersion, newFlavor), False))

        erasePrimaryList = []
        for (name, version, flavor) in cs.getOldTroveList():
            origJob.append((name, (version, flavor), (None, None), False))

            if (name, version, flavor) in primaries:
		erasePrimaryList.append((name, version, flavor))

	newJobList +=_findErasures(cs, erasePrimaryList, recurse)

        origJob = set(origJob)
        newJob = set(newJobList)

        obsoleteJob = origJob - newJob
        removedTroves = {}
        for (name, (oldVersion, oldFlavor), (newVersion, newFlavor), absolute) \
                                                in obsoleteJob:
            if newVersion:
                if not oldVersion:
                    # we may need to put this back later (and going over the
                    # network for something we already have is 1. dumb and
                    # 2. inappropriate for changeset files being installed)
                    removedTroves[(name, newVersion, newFlavor)] = \
                        cs.getNewTroveVersion(name, newVersion, newFlavor)
                cs.delNewTrove(name, newVersion, newFlavor)
            else:
                cs.delOldTrove(name, oldVersion, oldFlavor)

        neededJob = newJob - origJob

        if keepExisting:
            # convert everything relative to new installs
            jobList = []
            for (name, (oldVersion, oldFlavor), (newVersion, newFlavor), 
                 absolute) in neededJob:
                if (name, newVersion, newFlavor) in removedTroves:
                    cs.newTrove(removedTroves[(name, newVersion, newFlavor)])
                else:
                    jobList.append((name, (None, None),
                                          (newVersion, newFlavor), absolute))

            for trvCs in cs.iterNewTroveList():
                if trvCs.getOldVersion() is not None:
                    jobList.append((name, (None, None),
                                          (trvCs.getNewVersion(), 
                                           trvCs.getNewFlavor()), absolute))
                    cs.delNewTrove(name, trvCs.getNewVersion(),
                                   trvCs.getNewFlavor())

            neededJob = jobList
        else:
            # Make sure a single trove doesn't get removed multiple times.
            # This can happen if (for example) a single trove is updated
            # to two new versions of that trove simultaneously. This assumes
            # that the cs passed in to us doesn't have this problem; we only
            # check neededJob for it
            removed = {}
            for trvCs in cs.iterNewTroveList():
                if trvCs.getOldVersion() is not None:
                    removed[(trvCs.getName(), trvCs.getOldVersion(),
                             trvCs.getOldFlavor())] = None

            jobList = []
            for job in neededJob:
                if job[1][0] is None:
                    jobList.append(job)
                    continue

                if removed.has_key((job[0], job[1][0], job[1][1])):
                    jobList.append((job[0], (None, None), job[2], job[3]))
                else:
                    jobList.append(job)
                    removed[(job[0], job[1][0], job[1][1])] = True

            neededJob = jobList

        return neededJob
            
    def _updateChangeSet(self, itemList, uJob, keepExisting = None, 
                         recurse = True, updateMode = True):
        """
        Updates a trove on the local system to the latest version 
        in the respository that the trove was initially installed from.

        @param itemList: List specifying the changes to apply. Each item
        in the list must be a ChangeSetFromFile, the name of a trove to
        update, a (name, versionString, flavor) tuple, or a 
        @type itemList: list
        """
        changeSetList = []
        newItems = []
        finalCs = UpdateChangeSet()
        splittable = True

        for item in itemList:
            if isinstance(item, changeset.ChangeSetFromFile):
                splittable = False
                if item.isAbsolute():
		    self._rootChangeSet(item, keepExisting = keepExisting)

                finalCs.merge(item, (changeset.ChangeSetFromFile, item))

                continue

            if type(item) == str:
                troveName = item
                versionStr = None
                flavor = None
            else:
                troveName = item[0]
                versionStr = item[1]
                flavor = item[2]

            isInstall = updateMode
            if troveName[0] == '-':
                isInstall = False
                troveName = troveName[1:]
            elif troveName[0] == '+':
                isInstall = True
                troveName = troveName[1:]

            if not isInstall:
                troves = self.db.findTrove([], troveName, 
                                           versionStr = versionStr, 
                                           reqFlavor = flavor)
                troves = self.db.getTroves(troves)
                for outerTrove in troves:
                    changeSetList.append((outerTrove.getName(), 
                        (outerTrove.getVersion(), outerTrove.getFlavor()),
                        (None, None), False))
                # skip ahead to the next itemList
                continue                    

            if isinstance(versionStr, versions.Version):
                assert(isinstance(flavor, deps.DependencySet))
                newItems.append((troveName, versionStr, flavor))
            elif (versionStr and versionStr[0] == '/'):
                # fully qualified versions don't need branch affinity
                # but they do use flavor affinity
                try:
                    l = self.repos.findTrove(None, 
                                              (troveName, versionStr, flavor), 
                                              self.cfg.flavor, 
                                              affinityDatabase=self.db)
                except repository.TroveNotFound, e:
                    raise NoNewTrovesError
                newItems += l
            else:
                if keepExisting:
                    # when using keepExisting, branch affinity doesn't make 
                    # sense - we are installing a new, generally unrelated 
                    # version of this trove
                    affinityDb = None
                else:
                    affinityDb = self.db

                l = self.repos.findTrove(self.cfg.installLabelPath, 
                                          (troveName, versionStr, flavor),
                                          self.cfg.flavor, 
                                          affinityDatabase = affinityDb)
                newItems += l
                # XXX where does this go now?                    
                # updating locally cooked troves needs a label override
                #if True in [isinstance(x, versions.CookLabel) or
                #            isinstance(x, versions.EmergeLabel)
                #            for x in labels]:
                #    if not versionStr:
                #        raise UpdateError, \
                #         "Package %s cooked locally; version, branch, or " \
                #         "label must be specified for update" % troveName
                #    else:
                #        labels = [ None ]
                #    
                #    pass

        if keepExisting:
            for (name, version, flavor) in newItems:
                changeSetList.append((name, (None, None), (version, flavor), 0))
            eraseList = []
        else:
            # everything which needs to be installed is in this list; if 
            # it's not here, it's a duplicate
            outdated, eraseList = self.db.outdatedTroves(newItems)
            for (name, newVersion, newFlavor), \
                  (oldName, oldVersion, oldFlavor) in outdated.iteritems():
                changeSetList.append((name, (oldVersion, oldFlavor),
                                            (newVersion, newFlavor), 0))

        if finalCs.empty  and not changeSetList:
            raise NoNewTrovesError

        if changeSetList:
            primaries = ([ (x[0], x[2][0], x[2][1]) for x in changeSetList
                                if x[2][0] is not None ] +
                         [ (x[0], x[1][0], x[1][1]) for x in changeSetList
                                if x[2][0] is     None ])
            cs = self.repos.createChangeSet(changeSetList, withFiles = False,
                                            recurse = recurse,
                                            primaryTroveList = primaries)
            finalCs.merge(cs, (self.repos.createChangeSet, changeSetList))

        redirectHack = self._processRedirects(finalCs, recurse) 

        mergeItemList = self._mergeGroupChanges(finalCs, uJob, redirectHack, 
                                                keepExisting, recurse)
        if mergeItemList:
            cs = self.repos.createChangeSet(mergeItemList, withFiles = False,
                                            primaryTroveList = [], 
                                            recurse = False)
            finalCs.merge(cs, (self.repos.createChangeSet, changeSetList))

        return finalCs, splittable

    def updateChangeSet(self, itemList, keepExisting = False, recurse = True,
                        depsRecurse = True, resolveDeps = True, test = False,
                        updateByDefault = True, callback = UpdateCallback(),
                        split = False):
        callback.preparingChangeSet()

        uJob = UpdateJob()

        finalCs, splittable = self._updateChangeSet(itemList, uJob,
                                        keepExisting = keepExisting,
                                        recurse = recurse,
                                        updateMode = updateByDefault)

        split = split and splittable

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

        if split:
            startNew = True
            for job in splitJob:
                if startNew:
                    newCs = changeset.ChangeSet()
                    startNew = False

                foundCollection = False

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

                if foundCollection:
                    uJob.addChangeSet(newCs)
                    startNew = True

            if not startNew:
                uJob.addChangeSet(newCs)
        else:
            uJob.addChangeSet(cs)

        return (uJob, depList, suggMap, cannotResolve)

    def applyUpdate(self, uJob, replaceFiles = False, tagScript = None, 
                    test = False, justDatabase = False, journal = None, 
                    localRollbacks = False, callback = UpdateCallback()):

        def _handleSingleChangeSet(theCs, uJob, rollback, removeHints = {}, 
                                   standalone = False):
            assert(not standalone or 
                   isinstance(theCs, changeset.ReadOnlyChangeSet))
            cs = changeset.ReadOnlyChangeSet()

            changedTroves = [ (x.getName(), 
                               (x.getOldVersion(), x.getOldFlavor()),
                               (x.getNewVersion(), x.getNewFlavor()), False)
                                   for x in theCs.iterNewTroveList() ]
            changedTroves += [ (x[0], (x[1], x[2]), (None, None), False) 
                                   for x in theCs.getOldTroveList() ]
            changedTroves = dict.fromkeys(changedTroves)

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
                            if changedTroves.has_key(item):
                                del changedTroves[item]
                            elif item[2][0]:
                                newCs.delNewTrove(item[0], item[2][0], 
                                                  item[2][1])
                            else:
                                newCs.delOldTrove(item[0], item[1][0],
                                                  item[1][1])
                        cs.merge(newCs)

            newCs = self.repos.createChangeSet(changedTroves.keys(), 
                                               recurse = False,
                                               callback = callback)
            cs.merge(newCs)

            try:
                rb = self.db.commitChangeSet(cs, uJob.getLockMaps(),
                                    replaceFiles = replaceFiles,
                                    tagScript = tagScript, test = test, 
                                    justDatabase = justDatabase,
                                    journal = journal, callback = callback,
                                    localRollbacks = localRollbacks,
                                    rollback = rollback,
                                    removeHints = removeHints)
            except database.CommitError, e:
                raise UpdateError, "changeset cannot be applied"

            return rb

        rollback = self.db.createRollback()
        csSet = uJob.getChangeSets()
        if isinstance(csSet[0], changeset.ReadOnlyChangeSet):
            # this handles change sets which include change set files
            assert(len(csSet) == 1)
            callback.setHunk(0, 0)
            _handleSingleChangeSet(csSet[0], uJob, rollback, standalone = True)
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

            for i, theCs in enumerate(csSet):
                callback.setHunk(i + 1, len(csSet))
                _handleSingleChangeSet(theCs, uJob, rollback, 
                                       removeHints = removeHints)

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
                newTroves = [ x for x in trove.iterTroveList() if x not in seen]
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
                    branchedVersion = \
                        trove.getVersion().createShadow(newLabel)
                else:
                    branchedVersion = \
                        trove.getVersion().createBranch(newLabel, 
                                                        withVerRel = 1)

                branchedTrove = trove.copy()
		branchedTrove.changeVersion(branchedVersion)

		for (name, version, flavor) in trove.iterTroveList():
                    if shadow:
                        branchedVersion = version.createShadow(newLabel)
                    else:
                        branchedVersion = version.createBranch(newLabel, 
                                                               withVerRel = 1)
		    branchedTrove.delTrove(name, version, flavor,
                                           missingOkay = False)
		    branchedTrove.addTrove(name, branchedVersion, flavor)

                key = (trove.getName(), branchedVersion, trove.getFlavor())
                branchedTroves[key] = branchedTrove.diff(None)[0]

            # check for duplicates - XXX this could be more efficient with
            # a better repository API
            queryDict = {}
            for (name, version, flavor) in branchedTroves.iterkeys():
                l = queryDict.setdefault(name, [])
                l.append(version)

            matches = self.repos.getAllTroveFlavors(queryDict)

            for (name, version, flavor), troveCs in branchedTroves.iteritems():
                if matches.has_key(name) and matches[name].has_key(version) \
                   and flavor in matches[name][version]:
                    # this trove has already been branched
                    dupList.append((trove.getName(), 
                                    trove.getVersion().branch()))
                else:
                    cs.newTrove(troveCs)
                    cs.addPrimaryTrove(name, version, flavor)
                    needsCommit = True

        if needsCommit:
            self.repos.commitChangeSet(cs)

	return dupList

    def _createChangeSetList(self, csList, recurse = True, 
                             skipNotByDefault = False, excludeList = []):
        primaryList = []
        for (name, (oldVersion, oldFlavor), (newVersion, newFlavor), abstract) \
                                                            in csList:
            if newVersion:
                primaryList.append((name, newVersion, newFlavor))
            else:
                primaryList.append((name, oldVersion, oldFlavor))

        cs = self.repos.createChangeSet(csList, recurse = recurse, 
                                        withFiles = False)

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
                for reStr, regExp in excludeList:
                    if regExp.match(name):
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
                        skipNotByDefault = True, excludeList = [],
                        callback = None, withFiles = False,
                        withFileContents = False):
        """
        Like self.createChangeSetFile(), but returns a change set object.
        withFiles and withFileContents are the same as for the underlying
        repository call.
        """
        (fullCsList, primaryList) = self._createChangeSetList(csList, 
                recurse = recurse, skipNotByDefault = skipNotByDefault, 
                excludeList = excludeList)

        return self.repos.createChangeSet(fullCsList, recurse = False,
                                       primaryTroveList = primaryList,
                                       callback = callback, 
                                       withFiles = withFiles,
                                       withFileContents = withFileContents)

    def createChangeSetFile(self, path, csList, recurse = True, 
                            skipNotByDefault = True, excludeList = [],
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
                excludeList = excludeList)

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

