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

class UpdateChangeSet(changeset.MergeableChangeSet):

    def merge(self, cs, src):
        changeset.MergeableChangeSet.merge(self, cs)
        self.contents.append(src)
        self.empty = False

    def __init__(self, *args):
        changeset.MergeableChangeSet.__init__(self, *args)
        self.contents = []
        self.empty = True

class ConaryClient:
    def __init__(self, repos = None, cfg = None):
        if cfg == None:
            cfg = conarycfg.ConaryConfiguration()
        if repos == None:
            repos = NetworkRepositoryClient(cfg.repositoryMap)
        
        cfg.installLabel = cfg.installLabelPath[0]
        self.repos = repos
        self.cfg = cfg
        self.db = database.Database(cfg.root, cfg.dbPath)

    def _resolveDependencies(self, cs, keepExisting = None, recurse = True):
        pathIdx = 0
        foundSuggestions = False
        depList = self.db.depCheck(cs)[1]
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
                cs.merge(newCs, (self.repos.createChangeSet, troves))

                depList = self.db.depCheck(cs)[1]

            if sugg and recurse:
                pathIdx = 0
                foundSuggestions = False
            else:
                pathIdx += 1
                foundSuggestions = True
                if pathIdx == len(self.cfg.installLabelPath):
                    if not foundSuggestions or not recurse:
                        return (cs, depList, suggMap)
                    pathIdx = 0
                    foundSuggestions = False

        return (cs, depList, suggMap)

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
                    item.rootChangeSet(self.db, keepExisting)

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
                    pass
            else:
                if self.db.hasPackage(troveName):
                    labels = [ x.getVersion().branch().label()
                               for x in self.db.findTrove(troveName) ]

                    # this removes duplicates
                    labels = {}.fromkeys(labels).keys()
                    
                    # check for locally-cooked troves
                    if True in [isinstance(x, versions.CookBranch) or
                                isinstance(x, versions.EmergeBranch)
                                for x in labels]:
                        raise UpdateError, \
                            "Package %s cooked locally, not updating" \
                                    % troveName
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
            return (finalCs, [], {})

        return self._resolveDependencies(finalCs, keepExisting = keepExisting, 
                                         recurse = recurse)

    def applyUpdate(self, theCs, replaceFiles = False,
                    tagScript = None, keepExisting = None, depCheck = True):
        cs = changeset.MergeableChangeSet()
        for (how, what) in theCs.contents:
            if how == self.repos.createChangeSet:
                newCs = self.repos.createChangeSet(what)
                cs.merge(newCs)
            else:
                assert(how == changeset.ChangeSetFromFile)
                cs.merge(how)

        self.db.commitChangeSet(cs, replaceFiles = replaceFiles,
                                tagScript = tagScript, 
                                keepExisting = keepExisting,
                                depCheck = depCheck)

    def eraseTrove(self, troveList, tagScript = None):
        list = []
        for (troveName, versionStr) in troveList:
            troves = self.db.findTrove(troveName, versionStr)

            for t in troves:
                list.append((t.getName(), t.getVersion(), t.getFlavor()))

        self.db.eraseTroves(list, tagScript = tagScript)

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

