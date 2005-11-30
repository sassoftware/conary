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

import os
import pickle

#conary imports
from conary import conarycfg, metadata
from conary.conaryclient import clone, update
from conary.deps import deps
from conary.lib import log, util
from conary.local import database
from conary.repository.netclient import NetworkRepositoryClient

# mixins for ConaryClient
from conary.conaryclient.branch import ClientBranch
from conary.conaryclient.clone import ClientClone
from conary.conaryclient.update import ClientUpdate

CloneError = clone.CloneError
CloneIncomplete = clone.CloneIncomplete
UpdateError = update.UpdateError
NoNewTrovesError = update.NoNewTrovesError
DependencyFailure = update.DependencyFailure
DepResolutionFailure = update.DepResolutionFailure
EraseDepFailure = update.EraseDepFailure
NeededTrovesFailure = update.NeededTrovesFailure
InstallPathConflicts = update.InstallPathConflicts

class TroveNotFound(Exception):
    def __init__(self, troveName):
        self.troveName = troveName
        
    def __str__(self):
        return "trove not found: %s" % self.troveName

class VersionSuppliedError(UpdateError):
    def __str__(self):
        return "version should not be specified when a Conary change set " \
               "is being installed"

class ConaryClient(ClientClone, ClientBranch, ClientUpdate):
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
        
        self.cfg = cfg
        self.db = database.Database(cfg.root, cfg.dbPath)
        self.repos = NetworkRepositoryClient(cfg.repositoryMap,
                                             localRepository = self.db)
        log.openSysLog(self.cfg.root, self.cfg.logFile)

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

        deleted = set()
        # filter out non-defaults
        if skipNotByDefault:
            # Find out if troves were included w/ byDefault set (one
            # byDefault beats any number of not byDefault)
            inclusions = {}
            for troveCs in cs.iterNewTroveList():
                for (name, changeList) in troveCs.iterChangedTroves():
                    for (changeType, version, flavor, byDef) in changeList:
                        if changeType == '+':
                            inclusions.setdefault((name, version, flavor), 0)
                            if byDef:
                                inclusions[(name, version, flavor)] +=1

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
                elif inclusions[item] or item in deleted:
                    # the item was included w/ byDefault set (or we might
                    # have already erased it from the changeset)
                    continue

                # troveCs was not included byDefault True anywhere.
                # It may include subcomponents with byDefault True, however.
                # 
                # Say troveCs represents an install of foo, byDefault False.

                # If foo:runtime is only included by foo, 
                # then we don't want foo:runtime either, even if foo:runtime
                # is included in foo byDefault True.
                # However, if foo:runtime is included in a higher-level group
                # byDefault True, then foo:runtime should be included. 
                # We track not-by-default references to foo:runtime in 
                # inclusions, and delete foo:runtime only when its last
                # byDefault referencer was deleted.

                toDelete = [troveCs]
                while toDelete:
                    troveCs = toDelete.pop()
                    item = (troveCs.getName(), troveCs.getNewVersion(),
                            troveCs.getNewFlavor())

                    deleted.add(item)
                    cs.delNewTrove(*item)

                    for (name, changeList) in troveCs.iterChangedTroves():
                        for (changeType, version, flavor, byDef) in changeList:
                            if changeType == '+' and byDef:
                                item = (name, version, flavor)
                                inclusions[item] -= 1
                                if not inclusions[item]:
                                    childCs = cs.getNewTroveVersion(*item)
                                    toDelete.append(childCs)

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

    def pinTroves(self, troveList, pin = True):
        self.db.pinTroves(troveList, pin = pin)

    def getConaryUrl(self, version, flavor):
        """
        returns url to a conary changeset for updating the local client to
        @param version: a conary client version object, L{versions.Version}
        @param flavor: a conary client flavor object, L{deps.deps.DependencySet}
        """        
        return self.repos.getConaryUrl(version, flavor)
    
