#
# Copyright (c) 2004 Specifix, Inc.
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
import versions
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
                for (troveName, depSet) in depList:
                    if sugg.has_key(depSet):
                        if suggMap.has_key(troveName):
                            suggMap[troveName] += sugg[depSet]
                        else:
                            suggMap[troveName] = sugg[depSet]

                troves = {}
                for suggList in suggMap.itervalues():
                    suggList = [ (x[0], x[1]) for x in suggList ]
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

    def _updateChangeSet(self, itemList, keepExisting = None):
        """
        Updates a trove on the local system to the latest version 
        in the respository that the trove was initially installed from.

        @param itemList: List specifying the changes to apply. Each item
        in the list must be a ChangeSetFromFile, the name of a trove to
        update, or a (name, versionString) tuple. 
        @type itemList: list
        """
        self._prepareRoot()

        changeSetList = []
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
            else:
                troveName = item[0]
                versionStr = item[1]

            if versionStr and versionStr[0] == '/':
                # fully qualified versions don't need repository affinity
                # or the label search path
                try:
                    newList = self.repos.findTrove(None, troveName, 
                                                   self.cfg.flavor, versionStr,
                                                   withFiles = False)
                except repository.PackageNotFound, e:
                    # we give an error for this later on
                    newList = []
            else:
                if self.db.hasPackage(troveName):
                    labels = [ x.getVersion().branch().label()
                               for x in self.db.findTrove(troveName) ]

                    # this removes duplicates
                    labels = {}.fromkeys(labels).keys()
                    
                    # updating locally cooked troves needs a label override
                    if True in [isinstance(x, versions.CookBranch) or
                                isinstance(x, versions.EmergeBranch)
                                for x in labels]:
                        if not versionStr:
                            raise UpdateError, \
                             "Package %s cooked locally; version, branch, or " \
                             "label must be specified for update" % troveName
                        else:
                            labels = [ None ]
                        
                else:
                    labels = self.cfg.installLabelPath

                newList = []
                for label in labels:
                    try:
                        newList += self.repos.findTrove(label, troveName, 
                                                        self.cfg.flavor, 
                                                        versionStr,
                                                        withFiles = False)
                    except repository.PackageNotFound, e:
                        pass

                if not newList:
                    raise repository.TroveMissing(troveName, labels)

            if keepExisting:
                for newTrove in newList:
                    changeSetList.append((newTrove.getName(), (None, None),
                                (newTrove.getVersion(), newTrove.getFlavor()), 
                                0))
                eraseList = []
            else:
                newItems = []
                for newTrove in newList:
                    newItems.append((newTrove.getName(), newTrove.getVersion(),
                                     newTrove.getFlavor()))

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

        return finalCs

    def updateChangeSet(self, itemList, keepExisting = False,
                        recurse = True, resolveDeps = True):
        finalCs = self._updateChangeSet(itemList, keepExisting = keepExisting)

        if not resolveDeps:
            return (finalCs, [], {}, [])

        return self._resolveDependencies(finalCs, keepExisting = keepExisting, 
                                         recurse = recurse)

    def applyChangeSet(self, cs, replaceFiles = False, tagScript = None, 
                       keepExisting = None):
	assert(0)
        assert(isinstance(cs, changeset.ChangeSet))

	assert(not cs.isAbsolute)
        self.db.commitChangeSet(cs, replaceFiles = replaceFiles,
                                tagScript = tagScript, 
                                keepExisting = keepExisting)

    def applyUpdate(self, theCs, replaceFiles = False, tagScript = None, 
                    keepExisting = None):
        assert(isinstance(theCs, changeset.ReadOnlyChangeSet))
        cs = changeset.ReadOnlyChangeSet()
        for (how, what) in theCs.contents:
            if how == self.repos.createChangeSet:
                newCs = self.repos.createChangeSet(what)
                cs.merge(newCs)
            else:
                assert(how == changeset.ChangeSetFromFile)
                cs.merge(what)

        self.db.commitChangeSet(cs, replaceFiles = replaceFiles,
                                tagScript = tagScript, 
                                keepExisting = keepExisting)

    def eraseTrove(self, troveList, tagScript = None):
        list = []
        for (troveName, versionStr) in troveList:
            troves = self.db.findTrove(troveName, versionStr)

            for t in troves:
                list.append((t.getName(), t.getVersion(), t.getFlavor()))
 
        self.db.eraseTroves(list, tagScript = tagScript)

    def getMetadata(self, troveList, label, cacheFile = None,
                    cacheOnly = False, saveOnly = False):
        metadata = {}
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
                        metadata[troveName] = cache[lStr][bStr][troveName]
                        troveList.remove((troveName, branch))

        # if the cache missed any, grab from the repos
        if not cacheOnly and troveList:
            metadata.update(self.repos.getMetadata(troveList, label))
            if metadata and cacheFile:
                try:
                    cacheFp = open(cacheFile, "rw")
                    cache = pickle.load(cacheFp)
                    cacheFp.close()
                except IOError, EOFError:
                    cache = {}

                cacheFp = open(cacheFile, "w")

                # filter down troveList to only contain items for which we found metadata
                cacheTroves = [x for x in troveList if x[0] in metadata]

                lStr = label.asString()
                for troveName, branch in cacheTroves:
                    bStr = branch.asString()

                    if lStr not in cache:
                        cache[lStr] = {}
                    if bStr not in cache[lStr]:
                        cache[lStr][bStr] = {}

                    cache[lStr][bStr][troveName] = metadata[troveName]

                pickle.dump(cache, cacheFp)
                cacheFp.close()

        return metadata


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

