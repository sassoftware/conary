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
import util

import helper
import conarycfg
from local import database
from repository import repository
from repository import changeset

class ClientError(Exception):
    """Base class for client errors"""

class UpdateError(ClientError):
    """Base class for update errors"""

class VersionSuppliedError(UpdateError):
    def __str__(self):
        return "version should not be specified when a Conary change set is being installed"

class NoNewTrovesError(UpdateError):
    def __str__(self):
        return "no new troves found"

class ConaryClient:
    def __init__(self, repos, cfg = None):
        if cfg == None:
            cfg = conarycfg.ConaryConfiguration()
        
        self.repos = repos
        self.cfg = cfg
        self.db = database.Database(cfg.root, cfg.dbPath)

    def updateTrove(self, pkg, versionStr, replaceFiles = False,
                    tagScript = None, keepExisting = None):
        """Updates a trove on the local system to the latest version in the respository that
           the trove was initially installed from."""
        self._prepareRoot()

        if self.db.hasPackage(pkg):
            labels = [ x.getVersion().branch().label()
                       for x in self.db.findTrove(pkg) ]
            # this removes duplicates
            labels = {}.fromkeys(labels).keys()
        else:
            labels = [ self.cfg.installLabel ]

        newList = []
        for label in labels:
            try:
                newList += self.repos.findTrove(label, pkg, self.cfg.flavor, versionStr)
            except repository.PackageNotFound, e:
                pass

        if not newList:
            raise repository.TroveMissing(pkg, labels)

        list = []
        if keepExisting:
            for newTrove in newList:
                list.append((newTrove.getName(), (None, None),
                            (newTrove.getVersion(), newTrove.getFlavor()), 0))
            eraseList = []
        else:
            newItems = []
            for newTrove in newList:
                newItems.append((newTrove.getName(), newTrove.getVersion(),
                                 newTrove.getFlavor()))

            # everything which needs to be installed is in this list; if it's
            # not here, it's a duplicate
            outdated, eraseList = helper.outdatedTroves(self.db, newItems)
            for (name, newVersion, newFlavor), \
                    (oldName, oldVersion, oldFlavor) in outdated.iteritems():
                list.append((name, (oldVersion, oldFlavor),
                                   (newVersion, newFlavor), 0))

        if not list:
            raise NoNewTrovesError

        cs = self.repos.createChangeSet(list)
        list = [ x[0] for x in list ]

        if not list:
            raise NoNewTrovesError

        self.db.commitChangeSet(cs, replaceFiles = replaceFiles,
                                tagScript = tagScript, keepExisting = keepExisting)
 

    def applyChangeSet(self, pkg, replaceFiles = False, tagScript = None, keepExisting = False):
        """Applies a change set from a file to the system."""
        self._prepareRoot()
        
        cs = changeset.ChangeSetFromFile(pkg)
            
        if cs.isAbsolute():
            cs = self.db.rootChangeSet(cs)

        self.db.commitChangeSet(cs, replaceFiles = replaceFiles,
                                tagScript = tagScript, keepExisting = keepExisting)
 
    def _prepareRoot(self):
        """Prepares the installation root for trove updates and change set applications."""
        if not os.path.exists(self.cfg.root):
            util.mkdirChain(self.cfg.root)

