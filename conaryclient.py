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
import os
from lib import util
import pickle

import conarycfg
import deps
import versions
import metadata
from deps import deps
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

class ConaryClient:
    def __init__(self, cfg = None):
        if cfg == None:
            cfg = conarycfg.ConaryConfiguration()
        
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

    def _resolveDependencies(self, cs, keepExisting = None, recurse = True):
        pathIdx = 0
        foundSuggestions = False
        (depList, cannotResolve) = self.db.depCheck(cs)
        suggMap = {}

        while depList and True:
            sugg = self.repos.resolveDependencies(
                            self.cfg.installLabelPath[pathIdx], 
                            [ x[1] for x in depList ])

            if sugg:
                troves = {}

                for (troveName, depSet) in depList:
                    if sugg.has_key(depSet):
                        suggList = []
                        for choiceList in sugg[depSet]:
                            # XXX what if multiple troves are on this branch,
                            # but with different flavors? we could be
                            # (much) smarter here
                            scoredList = []
                            for choice in choiceList:
                                try:
                                    affinityTroves =self.db.findTrove(choice[0])
                                except repository.TroveNotFound:
                                    affinityTroves = None

                                f = self.cfg.flavor.copy()

                                if affinityTroves:
                                    f.union(affinityTroves[0].getFlavor(), 
                                        mergeType = deps.DEP_MERGE_TYPE_PREFS)
                                scoredList.append((f.score(choice[2]), choice))

                            scoredList.sort()
                            if scoredList[-1][0] is not  None:
                                choice = scoredList[-1][1]
                                suggList.append(choice)

                                l = suggMap.setdefault(troveName, [])
                                l.append(choice)

			troves.update(dict.fromkeys(suggList))

                troves = troves.keys()
                newCs = self._updateChangeSet(troves, 
                                              keepExisting = keepExisting)
                cs.merge(newCs)

                (depList, cannotResolve) = self.db.depCheck(cs)

            if sugg and recurse:
                pathIdx = 0
                foundSuggestions = False
            else:
                pathIdx += 1
                if sugg:
                    foundSuggestions = True
                if pathIdx == len(self.cfg.installLabelPath):
                    if not foundSuggestions or not recurse:
                        return (cs, depList, suggMap, cannotResolve)
                    pathIdx = 0
                    foundSuggestions = False

        return (cs, depList, suggMap, cannotResolve)

    def _mergeGroupChanges(self, cs):
        # updates a change set by removing troves which don't need
        # to be updated do to local state
        assert(not cs.isAbsolute())

        for (trvName, trvVersion, trvFlavor) in cs.getPrimaryTroveList():
            primaryTroveCs = cs.getNewPackageVersion(trvName, trvVersion, 
                                                     trvFlavor)

            for (name, changeList) in primaryTroveCs.iterChangedTroves():
                for (changeType, version, flavor) in changeList:
                    if changeType == '-': 
                        # XXX GROUPS we should do something better here (like
                        # check this against the erase list in the
                        # changeset)
                        continue

                    troveCs = cs.getNewPackageVersion(name, version, flavor)

                    oldItem = (name, troveCs.getOldVersion(), 
                               troveCs.getOldFlavor())
                    if not oldItem[1]: 
                        # it's new -- it can stay as long as it isn't
                        # already installed and isn't in the exclude list
                        if self.db.hasTrove(name, version, flavor):
                            cs.delNewPackage(name, version, flavor)
                        else:
                            for reStr, regExp in self.cfg.excludeTroves:
                                if regExp.match(name):
                                    cs.delNewPackage(name, version, flavor)
                                    break
                    elif not self.db.hasTrove(*oldItem):
                        cs.delNewPackage(name, version, flavor)
                    elif self.db.hasTrove(name, version, flavor):
                        cs.delNewPackage(name, version, flavor)

    def _updateChangeSet(self, itemList, keepExisting = None, test = False):
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

            #if isinstance(versionStr, versions.Version):

            if isinstance(versionStr, versions.Version):
                assert(isinstance(flavor, deps.DependencySet))
                newItems.append((troveName, versionStr, flavor))
            elif versionStr and versionStr[0] == '/':
                # fully qualified versions don't need repository affinity
                # or the label search path
                try:
                    l = self.repos.findTrove(None, troveName, 
                                                   self.cfg.flavor, versionStr,
                                                   affinityDatabase = self.db,
                                                   flavor = flavor)
                except repository.TroveNotFound, e:
                    raise NoNewTrovesError
                newItems += l
            else:
                l = self.repos.findTrove(self.cfg.installLabelPath, 
                                               troveName, 
                                               self.cfg.flavor, versionStr,
                                               affinityDatabase = self.db,
                                               flavor = flavor)
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
            cs = self.repos.createChangeSet(changeSetList, withFiles = False)
            finalCs.merge(cs, (self.repos.createChangeSet, changeSetList))

        self._mergeGroupChanges(finalCs)

        return finalCs

    def updateChangeSet(self, itemList, keepExisting = False,
                        recurse = True, resolveDeps = True, test = False):
        if not test:
            self._prepareRoot()

        finalCs = self._updateChangeSet(itemList, keepExisting = keepExisting)

        if not resolveDeps:
            return (finalCs, [], {}, [])

        return self._resolveDependencies(finalCs, keepExisting = keepExisting, 
                                         recurse = recurse)

    def applyUpdate(self, theCs, replaceFiles = False, tagScript = None, 
                    keepExisting = None, test = False, justDatabase = False,
                    journal = None):
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
                                    for x in theCs.iterNewPackageList() ]
                troves += [ (x[0], (x[1], x[2]), (None, None), False) 
                                    for x in theCs.getOldPackageList() ]

                for item in troves:
                    if changedTroves.has_key(item):
                        del changedTroves[item]
                    else:
                        newCs.delNewPackage(x[0], x[2][0], x[2][1])
                cs.merge(newCs)

        newCs = self.repos.createChangeSet(changedTroves.keys(), 
                                           recurse = False)
        cs.merge(newCs)

        self.db.commitChangeSet(cs, replaceFiles = replaceFiles,
                                tagScript = tagScript, 
                                keepExisting = keepExisting,
                                test = test, justDatabase = justDatabase,
                                journal = journal)

    def eraseTrove(self, troveList, depCheck = True, tagScript = None,
                   test = False, justDatabase = False):
	cs = changeset.ChangeSet()

        for (troveName, versionStr, flavor) in troveList:
            troves = self.db.findTrove(troveName, versionStr)

            for outerTrove in troves:
                for trove in self.db.walkTroveSet(outerTrove, 
                                                 ignoreMissing = True):
                    if flavor is None or flavor.stronglySatisfies(
                                                        trove.getFlavor()):
                        cs.oldPackage(trove.getName(), trove.getVersion(), 
                                      trove.getFlavor())

        if depCheck:
            (depList, cannotResolve) = self.db.depCheck(cs)
            assert(not depList)
            if cannotResolve:
                return cannotResolve
            
	self.db.commitChangeSet(cs, tagScript = tagScript, test = test,
                                justDatabase = justDatabase)

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
            md .update(self.repos.getMetadata(troveList, label))
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

    def _prepareRoot(self):
        """
        Prepares the installation root for trove updates and change 
        set applications.
        """
        if not os.path.exists(self.cfg.root):
            util.mkdirChain(self.cfg.root)
        if not self.db.writeAccess():
            raise UpdateError, \
                "Write permission denied on conary database %s" % self.db.dbpath

