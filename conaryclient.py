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
			    for x in cs.iterNewPackageList() ]

	if keepExisting:
	    outdated = None
	else:
	    # this ignores eraseList, just like we do when trove names
	    # are specified
	    outdated, eraseList = self.db.outdatedTroves(troveList)

	    for key, tup in outdated.items():
		outdated[key] = tup[1:3]

	cs.rootChangeSet(self.db, outdated)

    def _resolveDependencies(self, cs, keepExisting = None, depsRecurse = True):
        pathIdx = 0
        foundSuggestions = False
        (depList, cannotResolve) = self.db.depCheck(cs)[0:2]
        suggMap = {}

        while depList:
            sugg = self.repos.resolveDependencies(
                            self.cfg.installLabelPath[pathIdx], 
                            [ x[1] for x in depList ])

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
                    newCs = self._updateChangeSet(troves, 
                                                  keepExisting = keepExisting)
                    cs.merge(newCs)

                    (depList, cannotResolve) = self.db.depCheck(cs)[0:2]

            if troves and depsRecurse:
                pathIdx = 0
                foundSuggestions = False
            else:
                pathIdx += 1
                if troves:
                    foundSuggestions = True
                if pathIdx == len(self.cfg.installLabelPath):
                    if not foundSuggestions or not depsRecurse:
                        return (cs, depList, suggMap, cannotResolve)
                    pathIdx = 0
                    foundSuggestions = False

        return (cs, depList, suggMap, cannotResolve)

    def _processRedirects(self, cs):
        # Looks for redirects in the change set, and returns a list of
        # troves which need to be included in the update. Troves we
        # redirect to don't show up as primary troves (ever), which keeps
        # _mergeGroupChanges() from interacting with troves which are the
        # targets of redirections.
        troveSet = {}
        delDict = {}

        for troveCs in cs.iterNewPackageList():
            if not troveCs.getIsRedirect():
                continue

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
                    cs.oldPackage(*oldItem)
                else:
                    # erase the target(s) of the redirection
                    for (name, changeList) in troveCs.iterChangedTroves():
                        for (changeType, version, flavor, byDef) in changeList:
                            delDict[(name, version, flavor)] = True

            # look for troves being added by this redirect
            for (name, changeList) in troveCs.iterChangedTroves():
                for (changeType, version, flavor, byDef) in changeList:
                    if changeType == '+': 
                        troveSet[(name, version, flavor)] = True

        for item in delDict.iterkeys():
            if cs.hasNewPackage(*item):
                cs.delNewPackage(*item)

        # Troves in troveSet which are still in this changeset are ones
        # we really do need to install. We don't know what versions they
        # should be relative though; this removes them and depends on the
        # caller to add them again, relative to the right things
        addList = []
        for item in troveSet.keys():
            if not cs.hasNewPackage(*item): continue
            cs.delNewPackage(*item)
            addList.append(item)

        outdated, eraseList = self.db.outdatedTroves(addList)
        csList = []
        for (name, newVersion, newFlavor), \
              (oldName, oldVersion, oldFlavor) in outdated.iteritems():
            csList.append((name, (oldVersion, oldFlavor),
                                 (newVersion, newFlavor), False))
            # don't let things be listed as old for two different reasons
            if cs.hasOldPackage(name, oldVersion, oldFlavor):
                cs.delOldPackage(name, oldVersion, oldFlavor)

        return csList

    def _mergeGroupChanges(self, cs, keepExisting):
        # Updates a change set by removing troves which don't need
        # to be updated do to local state. It also removes troves which
        # don't need to be installed because they're new, but aren't to
        # be installed by default.
        assert(not cs.isAbsolute())

        primaries = cs.getPrimaryTroveList()
        inclusions = {}
        outdated = {}
        addList = []

        # find the troves which include other troves; they give useful
        # hints as to which ones should be excluded due to byDefault
        # flags
        for troveCs in cs.iterNewPackageList():
            for (name, changeList) in troveCs.iterChangedTroves():
                for (changeType, version, flavor, byDef) in changeList:
                    if changeType == '+':
                        if byDef:
                            inclusions[(name, version, flavor)] = True
                        else:
                            inclusions.setdefault((name, version, flavor), 
                                                  False)

        for troveCs in [ x for x in cs.iterNewPackageList() ]:
            item = (troveCs.getName(), troveCs.getNewVersion(),
                    troveCs.getNewFlavor())
            if item in primaries:
                continue 

            oldItem = (troveCs.getName(), troveCs.getOldVersion(), 
                       troveCs.getOldFlavor())
            if self.db.hasTrove(*item):
                # this trove is already installed. don't install it again
                cs.delNewPackage(*item)
            elif not oldItem[1]:
                # it's a new trove
                if not inclusions.get(item, True):
                    # it was included by something else, but not by default
                    cs.delNewPackage(*item)
                else:
                    # check the exclude list
                    skipped = False
                    for reStr, regExp in self.cfg.excludeTroves:
                        if regExp.match(item[0]):
                            cs.delNewPackage(*item)
                            skipped = True
                            break

                    if not skipped and self.db.hasPackage(oldItem[0]) \
                                   and not keepExisting \
                                   and not outdated.has_key(oldItem):
                        # we have a different version of the trove already
                        # installed. we need to change this to be relative to
                        # the version already installed which is on the same
			# branch (unless that version # is being removed by 
			# something else in the change # set) 
			versionList = self.db.getTroveVersionList(oldItem[0])
			for version in versionList:
			    if version.branch() == item[1].branch():
				cs.delNewPackage(*item)
				addList.append(item)
				break
            elif not self.db.hasTrove(*oldItem):
                # the old version isn't present, so we don't want this
                # one either
                cs.delNewPackage(*item)

        # remove troves from the old package list which aren't currently
        # installed. also remove ones which are supposed to have been
	# installed by this change set
	delList = []
        for item in cs.getOldPackageList():
            if not self.db.hasTrove(*item) or inclusions.has_key(item):
		delList.append(item)

	for item in delList:
	    cs.delOldPackage(*item)

        removeSet = dict.fromkeys(
            [ (x.getName(), x.getOldVersion(), x.getOldFlavor() )
                            for x in cs.iterNewPackageList() ])

        outdated, eraseList = self.db.outdatedTroves(addList)
        csList = []
        for (name, newVersion, newFlavor), \
              (oldName, oldVersion, oldFlavor) in outdated.iteritems():
            # don't let multiple items remove the same old item
            if removeSet.has_key((name, oldVersion, oldFlavor)):
                csList.append((name, (None, None),
                                     (newVersion, newFlavor), False))
                continue

            if cs.hasOldPackage(name, oldVersion, oldFlavor):
                cs.delOldPackage(name, oldVersion, oldFlavor)

            removeSet[(name, oldVersion, oldFlavor)] = True
            csList.append((name, (oldVersion, oldFlavor),
                                 (newVersion, newFlavor), False))

        return csList
            
    def _updateChangeSet(self, itemList, keepExisting = None, recurse = True,
                         updateMode = True):
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

        for item in itemList:
            if isinstance(item, changeset.ChangeSetFromFile):
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
		    if recurse:
			 for trove in self.db.walkTroveSet(outerTrove, 
							  ignoreMissing = True):
			     changeSetList.append((trove.getName(), 
				 (trove.getVersion(), trove.getFlavor()),
				 (None, None), False))
		    else:
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
            cs = self.repos.createChangeSet(changeSetList, withFiles = False,
                                            recurse = recurse)
            finalCs.merge(cs, (self.repos.createChangeSet, changeSetList))

        # we need to iterate here to handle redirects to redirects to...
        redirectCsList = self._processRedirects(finalCs) 
        while redirectCsList:
            cs = self.repos.createChangeSet(redirectCsList, withFiles = False,
                                            primaryTroveList = [], 
                                            recurse = False)
            newRedirectCsList = self._processRedirects(cs)
            finalCs.merge(cs, (self.repos.createChangeSet, redirectCsList))
            redirectCsList = newRedirectCsList

        mergeItemList = self._mergeGroupChanges(finalCs, keepExisting)
        if mergeItemList:
            cs = self.repos.createChangeSet(mergeItemList, withFiles = False,
                                            primaryTroveList = [], 
                                            recurse = False)
            finalCs.merge(cs, (self.repos.createChangeSet, changeSetList))

        return finalCs

    def updateChangeSet(self, itemList, keepExisting = False, recurse = True,
                        depsRecurse = True, resolveDeps = True, test = False,
                        updateByDefault = True, callback = UpdateCallback()):
        callback.preparingChangeSet()

        finalCs = self._updateChangeSet(itemList, 
                                        keepExisting = keepExisting,
                                        recurse = recurse,
                                        updateMode = updateByDefault)

        if not resolveDeps:
            return (finalCs, [], {}, [])

        callback.resolvingDependencies()

        return self._resolveDependencies(finalCs, keepExisting = keepExisting, 
                                         depsRecurse = depsRecurse)

    def applyUpdate(self, theCs, replaceFiles = False, tagScript = None, 
                    keepExisting = None, test = False, justDatabase = False,
                    journal = None, localRollbacks = False, 
                    callback = UpdateCallback()):
        assert(isinstance(theCs, changeset.ReadOnlyChangeSet))
        cs = changeset.ReadOnlyChangeSet()

        changedTroves = [ (x.getName(), 
                           (x.getOldVersion(), x.getOldFlavor()),
                           (x.getNewVersion(), x.getNewFlavor()), False)
                               for x in theCs.iterNewPackageList() ]
        changedTroves += [ (x[0], (x[1], x[2]), (None, None), False) 
                               for x in theCs.getOldPackageList() ]
        changedTroves = dict.fromkeys(changedTroves)

        for (how, what) in theCs.contents:
            if how == changeset.ChangeSetFromFile:
                newCs = what

                troves = [ (x.getName(), 
                               (x.getOldVersion(), x.getOldFlavor()),
                               (x.getNewVersion(), x.getNewFlavor()), False)
                                    for x in newCs.iterNewPackageList() ]
                troves += [ (x[0], (x[1], x[2]), (None, None), False) 
                                    for x in newCs.getOldPackageList() ]

                for item in troves:
                    if changedTroves.has_key(item):
                        del changedTroves[item]
                    else:
                        newCs.delNewPackage(item[0], item[2][0], item[2][1])
                cs.merge(newCs)

        newCs = self.repos.createChangeSet(changedTroves.keys(), 
                                           recurse = False,
                                           callback = callback)
        cs.merge(newCs)

        try:
            self.db.commitChangeSet(cs, replaceFiles = replaceFiles,
                                    tagScript = tagScript, 
                                    keepExisting = keepExisting,
                                    test = test, justDatabase = justDatabase,
                                    journal = journal, callback = callback,
                                    localRollbacks = localRollbacks)
        except database.CommitError, e:
            raise UpdateError, "changeset cannot be applied"

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

        seen = {}
        dupList = []
        needsCommit = False

        newLabel = versions.Label(newLabel)

	while troveList:
            leavesByLabelOps = {}

            troves = self.repos.getTroves(troveList)
            troveList = []
            branchedTroves = {}

	    for trove in troves:
                key = (trove.getName(), trove.getVersion(), trove.getFlavor())
                if seen.has_key(key):
                    continue
                seen[key] = True

                # add contained troves to the todo-list
                troveList += [ x for x in trove.iterTroveList() ]

                if sourceTroves and not trove.getName().endswith(':source'):
                    # XXX this can go away once we don't care about
                    # pre-troveInfo troves
                    if not trove.getSourceName():
                        log.warning('%s has no source information' % 
                                    trove.getName())

                    troveList.append((trove.getSourceName(),
                                      trove.getVersion().getSourceVersion(),
                                      deps.DependencySet()))
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
                    cs.newPackage(troveCs)
                    cs.addPrimaryTrove(name, version, flavor)
                    needsCommit = True

        if needsCommit:
            self.repos.commitChangeSet(cs)

	return dupList

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

