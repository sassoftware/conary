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

    def done(self):
        self._message('')

    def _message(self, text):
        callbacks.LineOutput._message(self, text)

    def update(self):
        self.lock.acquire()
        t = ""

        if self.csText:
            t = self.csText + ' '

        if self.updateText:
            t += self.updateText

        self._message(t)
        self.lock.release()

    def updateMsg(self, text):
        if text and self.updateHunk is not None:
            text = "Job %d: %s%s" % (self.updateHunk[0], text[0].lower(),
                                     text[1:])
        self.updateText = text
        self.update()

    def csMsg(self, text):
        self.csText = text
        self.update()

    def preparingChangeSet(self):
        self.updateMsg("Preparing changeset")

    def resolvingDependencies(self):
        self.updateMsg("Resolving dependencies")

    def updateDone(self):
        self.updateText = None

    def downloadingChangeSet(self, got, need):
        if got == need:
            self.csText = None
        elif need != 0:
            if self.csHunk is None:
                self.csMsg("Downloading (%d%% of %dk)" 
                              % ((got * 100) / need , need / 1024))
            else:
                self.csMsg("Downloading %d of %d (%d%%)" 
                              % (self.csHunk + 
                                 (( (got * 100) / need), )))

        self.update()

    def requestingChangeSet(self):
        if self.csHunk is None:
            self.csMsg("Requesting changeset")
        else:
            self.csMsg("Requesting changeset %d of %d" % self.csHunk)

    def creatingRollback(self):
        self.updateMsg("Creating rollback")

    def preparingUpdate(self, troveNum, troveCount):
        self.updateMsg("Preparing update (%d of %d)" % 
		      (troveNum, troveCount))

    def restoreFiles(self, size, totalSize):
        if totalSize != 0:
            self.restored += size
            self.updateMsg("Writing %dk of %dk (%d%%)" 
                        % (self.restored / 1024 , totalSize / 1024,
                           (self.restored * 100) / totalSize))

    def removeFiles(self, fileNum, total):
        if total != 0:
            self.updateMsg("Removing %d of %d (%d%%)"
                        % (fileNum , total, (fileNum * 100) / total))

    def creatingDatabaseTransaction(self, troveNum, troveCount):
        self.updateMsg("Creating database transaction (%d of %d)" %
		      (troveNum, troveCount))

    def runningPreTagHandlers(self):
        self.updateMsg("Running tag pre-scripts")

    def runningPostTagHandlers(self):
        self.updateMsg("Running tag post-scripts")

    def setChangesetHunk(self, num, total):
        self.csHunk = (num, total)

    def setUpdateHunk(self, num, total):
        self.restored = 0
        self.updateHunk = (num, total)

    def __init__(self):
        callbacks.LineOutput.__init__(self)
        self.restored = 0
        self.csHunk = None
        self.updateHunk = None
        self.csText = None
        self.updateText = None
        import thread
        self.lock = thread.allocate_lock()

def displayUpdateInfo(updJob):
    totalJobs = len(updJob.getChangeSets())
    for num, cs in enumerate(updJob.getChangeSets()):
        if totalJobs > 1:
            print 'Job %d of %d:' %(num + 1, totalJobs)
            indent = '    '
        else:
            indent = '    '
        new = []
        for x in cs.iterNewTroveList():
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

        old = []
        old += [ "D %s (%s deleted)" % (x[0], x[1].trailingRevision().asString()) 
                 for x in cs.getOldTroveList() ]
        old.sort()

        if not new and not old:
            print indent + "Nothing is affected by this update."

        if new:
            print indent + ("\n%s" %indent).join(new)

        if old:
            print indent + ("\n%s" %indent).join(old)
    return

def doUpdate(cfg, pkgList, replaceFiles = False, tagScript = None, 
                                  keepExisting = False, depCheck = True,
                                  depsRecurse = True, test = False,
                                  justDatabase = False, recurse = True,
                                  info = False, updateByDefault = True,
                                  callback = None):
    if not callback:
        callback = callbacks.UpdateCallback()

    applyList = []

    if type(pkgList) is str:
        pkgList = ( pkgList, )
    for pkgStr in pkgList:
        if os.path.exists(pkgStr) and os.path.isfile(pkgStr):
            try:
                cs = changeset.ChangeSetFromFile(pkgStr)
            except BadContainer, msg:
                # ensure that it is obvious that a file is being referenced
                if pkgStr[0] not in './':
                    pkgStr = './' + pkgStr
                log.error("'%s' is not a valid conary changeset: %s" % 
                          (pkgStr, msg))
                sys.exit(1)
            applyList.append(cs)
        else:
            applyList.append(parseTroveSpec(pkgStr))

    # dedup
    applyList = set(applyList)
    try:
        _updateTroves(cfg, applyList, replaceFiles = replaceFiles, 
                      tagScript = tagScript, 
                      keepExisting = keepExisting, depCheck = depCheck,
                      depsRecurse = depsRecurse, test = test,
                      justDatabase = justDatabase, recurse = recurse,
                      info = info, updateByDefault = updateByDefault,
                      callback = callback)
    except conaryclient.DependencyFailure, e:
        # XXX print dependency errors because the testsuite 
        # prefers it
        print e
    except conaryclient.UpdateError, e:
        log.error(e)
    except repository.CommitError, e:
        log.error(e)
    except changeset.PathIdsConflictError, e:
        log.error(e)

def _updateTroves(cfg, applyList, replaceFiles = False, tagScript = None, 
                                  keepExisting = False, depCheck = True,
                                  depsRecurse = True, test = False,
                                  justDatabase = False, recurse = True,
                                  info = False, updateByDefault = True,
                                  callback = None):
    client = conaryclient.ConaryClient(cfg)

    if not info:
	client.checkWriteableRoot()


    (updJob, suggMap) = \
    client.updateChangeSet(applyList, depsRecurse = depsRecurse,
                           resolveDeps = depCheck,
                           keepExisting = keepExisting,
                           test = test, recurse = recurse,
                           updateByDefault = updateByDefault,
                           callback = callback, split = True)

    if info:
        callback.done()
        displayUpdateInfo(updJob)
        return

    if suggMap:
        callback.done()
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

        keepExisting = False

    client.applyUpdate(updJob, replaceFiles, tagScript, test = test, 
                       justDatabase = justDatabase,
                       localRollbacks = cfg.localRollbacks,
                       callback = callback)


def updateAll(cfg, info = False, depCheck = True):
    client = conaryclient.ConaryClient(cfg)
    cu = client.db.db.db.cursor()    
    cu.execute('select trovename from dbinstances left outer join trovetroves on dbinstances.instanceid=trovetroves.includedid join versions on dbinstances.versionid = versions.versionid where includedid is null and version not like "%local%";')
    names = [ x[0] for x in cu ]
    return doUpdate(cfg, names, info = info, depCheck = depCheck)

def changeLocks(cfg, troveStrList, lock = True):
    client = conaryclient.ConaryClient(cfg)
    troveList = [] 
    for item in troveStrList:
        name, ver, flv = parseTroveSpec(item)
        troves = client.db.findTrove([], name, versionStr = ver,
                                     reqFlavor = flv)
        troveList += troves

    client.lockTroves(troveList, lock = lock)

def parseTroveSpec(specStr):
    if specStr.find('[') > 0 and specStr[-1] == ']':
        specStr = specStr[:-1]
        l = specStr.split('[')
        if len(l) != 2:
            raise TroveSpecError, "bad trove spec %s]" % specStr
        specStr, flavorSpec = l
        flavor = deps.parseFlavor(flavorSpec)
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

