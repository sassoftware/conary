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
import callbacks
from deps import deps
from lib import log
from lib import util
from local import database
from repository import changeset
from repository import repository
from repository.filecontainer import BadContainer
import conaryclient
import os
import sys

# FIXME client should instantiated once per execution of the command line 
# conary client

class UpdateCallback(callbacks.LineOutput, callbacks.UpdateCallback):

    def preparingChangeSet(self):
        self._message("Preparing changeset...")

    def resolvingDependencies(self):
        self._message("Resolving dependencies...")

    def downloadingChangeSet(self, got, need):
        if need != 0:
            self._message("Downloading changeset (%d%% of %dk)..." 
                          % ((got * 100) / need , need / 1024))
            

    def requestingChangeSet(self):
        self._message("Requesting changeset...")

    def creatingRollback(self):
        self._message("Creating rollback...")

    def preparingUpdate(self):
        self._message("Preparing update...")

    def restoreFiles(self, size, totalSize):
        if totalSize != 0:
            self.restored += size
            self._message("Writing %dk of %dk (%d%%)..." 
                        % (self.restored / 1024 , totalSize / 1024,
                           (self.restored * 100) / totalSize))

    def removeFiles(self, fileNum, total):
        if total != 0:
            self._message("Removing %d of %d (%d%%)..."
                        % (fileNum , total, (fileNum * 100) / total))

    def runningPreTagHandlers(self):
        self._message("Running tag pre-scripts...")

    def runningPostTagHandlers(self):
        self._message("Running tag post-scripts...")

    def __init__(self):
        callbacks.LineOutput.__init__(self)
        self.restored = 0

def doUpdate(cfg, pkgList, replaceFiles = False, tagScript = None, 
                                  keepExisting = False, depCheck = True,
                                  depsRecurse = True, test = False,
                                  justDatabase = False, recurse = True,
                                  info = False, updateByDefault = True,
                                  callback = None):
    if not callback:
        callback = callbacks.UpdateCallback()

    client = conaryclient.ConaryClient(cfg)

    applyList = []

    if type(pkgList) is str:
        pkgList = ( pkgList, )
    for pkgStr in pkgList:
        if os.path.exists(pkgStr) and os.path.isfile(pkgStr):
            try:
                cs = changeset.ChangeSetFromFile(pkgStr)
            except BadContainer, msg:
                log.error("'%s' is not a valid conary changset: %s" % 
                          (pkgStr, msg))
                sys.exit(1)
            applyList.append(cs)
        else:
            if updateByDefault or pkgStr[0] == '+':
                applyList.append(parseTroveSpec(pkgStr, cfg.flavor))
            else:
                applyList.append(parseTroveSpec(pkgStr, None))

    # dedup
    applyList = {}.fromkeys(applyList).keys()

    try:
        (cs, depFailures, suggMap, brokenByErase) = \
            client.updateChangeSet(applyList, depsRecurse = depsRecurse,
                                   resolveDeps = depCheck,
                                   keepExisting = keepExisting,
                                   test = test, recurse = recurse,
                                   updateByDefault = updateByDefault,
                                   callback = callback)

        if brokenByErase:
            print "Troves being removed create unresolved dependencies:"
            for (troveName, depSet) in brokenByErase:
                print "    %s:\n\t%s" %  \
                        (troveName, "\n\t".join(str(depSet).split("\n")))
            return

        if depFailures:
            print "The following dependencies could not be resolved:"
            for (troveName, depSet) in depFailures:
                print "    %s:\n\t%s" %  \
                        (troveName, "\n\t".join(str(depSet).split("\n")))
            return
        elif (not cfg.autoResolve or brokenByErase) and suggMap:
            print "Additional troves are needed:"
            for (req, suggList) in suggMap.iteritems():
                print "    %s -> %s" % \
                  (req, " ".join(["%s(%s)" % 
                  (x[0], x[1].trailingRevision().asString()) for x in suggList]))
            return
        elif suggMap:
            print "Including extra troves to resolve dependencies:"
            print "   ",
            items = {}
            for suggList in suggMap.itervalues():
                # remove duplicates
                items.update(dict.fromkeys([(x[0], x[1]) for x in suggList]))

            items = items.keys()
            items.sort()
            print "%s" % (" ".join(["%s(%s)" % 
                           (x[0], x[1].trailingRevision().asString())
                           for x in items]))
            if info: return
            
        if info:
            new = []
            for x in cs.iterNewPackageList():
                oldVersion = x.getOldVersion()
                newVersion = x.getNewVersion()
                if oldVersion:
                    oldTVersion = oldVersion.trailingRevision()
                else:
                    # if there is no oldVersion, this is a new trove
                    new.append(("%s (%s)" % 
                                (x.getName(), 
                                 newVersion.trailingRevision().asString()),
				'N'))
                    continue
                    
                newTVersion = newVersion.trailingRevision()

                if oldVersion.branch() != newVersion.branch():
                    kind = 'B'
                elif oldTVersion.getVersion() != newTVersion.getVersion():
                    kind = 'V'
                elif oldTVersion.getSourceCount() != \
                                            newTVersion.getSourceCount():
                    kind = 'S'
                else:
                    kind = 'B'

                new.append(("%s (%s -> %s)" % 
                                (x.getName(), oldTVersion.asString(),
                                 newTVersion.asString()), kind))

	    new.sort()
	    new = [ "%s %s" % (x[1], x[0]) for x in new ]

            old = [ "%s (%s)" % (x[0], x[1].trailingRevision().asString()) 
                                for x in cs.getOldPackageList() ]
	    old.sort()
            if not new and not old:
                print "No troves are affected by this update."
            
            if new:
                print "Versions of the following troves will be updated:"
                print "\t", "\n\t".join(sorted(new))

            if new and old:
                print "\n",

            if old:
                print "Versions of the following troves will be removed:"
                print "\t", "\n\t".join(sorted(old))

            return

        client.applyUpdate(cs, replaceFiles, tagScript, keepExisting,
                           test = test, justDatabase = justDatabase,
                           localRollbacks = cfg.localRollbacks,
                           callback = callback)
    except conaryclient.UpdateError, e:
        log.error(e)
    except repository.CommitError, e:
        log.error(e)

def parseTroveSpec(specStr, defaultFlavor):
    if specStr.find('[') > 0 and specStr[-1] == ']':
        specStr = specStr[:-1]
        l = specStr.split('[')
        if len(l) != 2:
            raise TroveSpecError, "bad trove spec %s]" % specStr
        specStr, flavorSpec = l
        flavor = deps.parseFlavor(flavorSpec, mergeBase = defaultFlavor)
        if flavor is None:
            raise TroveSpecError, "bad flavor [%s]" % flavorSpec
    else:
        flavor = None

    if specStr.find("=") >= 0:
        l = specStr.split("=")
        if len(l) != 2:
            raise TroveSpecError, "too many ='s in %s" %specStr
        name, versionSpec = l
    else:
        name = specStr
        versionSpec = None

    return (name, versionSpec, flavor)

def toTroveSpec(name, versionStr, flavor):
    disp = [name]
    if versionStr:
        disp.extend(('=', versionStr))
    if flavor:
        disp.extend(('[', deps.formatFlavor(flavor), ']'))
    return ''.join(disp)

class TroveSpecError(Exception):

    pass

