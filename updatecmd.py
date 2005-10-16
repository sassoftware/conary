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
#
import callbacks
from conaryclient.cmdline import parseTroveSpec
from conaryclient.cmdline import toTroveSpec
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
import thread

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

        if self.updateText:
	    if self.updateHunk is not None and self.updateHunk[1] != 1:
		if self.csText is None:
		    ofText = " of %d" % self.updateHunk[1]
		else:
		    ofText = ""

		job = "Job %d%s: %s%s" % (self.updateHunk[0], 
					  ofText,
					  self.updateText[0].lower(),
					  self.updateText[1:])

            t += self.updateText

        if self.csText:
            t = self.csText + ' '

	if t and len(t) < 76:
            t = t[:76]
	    t += '...'

        self._message(t)
        self.lock.release()

    def updateMsg(self, text):
        self.updateText = text
        self.update()

    def csMsg(self, text):
        self.csText = text
        self.update()

    def preparingChangeSet(self):
        self.updateMsg("Preparing changeset request")

    def resolvingDependencies(self):
        self.updateMsg("Resolving dependencies")

    def updateDone(self):
        self.lock.acquire()
        self._message('')
        self.updateText = None
        self.lock.release()

    def _downloading(self, msg, got, need):
        if got == need:
            self.csText = None
        elif need != 0:
            if self.csHunk[1] < 2 or not self.updateText:
                self.csMsg("%s (%d%% of %dk)"
                           % (msg, (got * 100) / need, need / 1024))
            else:
                self.csMsg("%s %d of %d (%d%%)"
                           % ((msg,) + self.csHunk + (((got * 100) / need),)))

        self.update()

    def downloadingFileContents(self, got, need):
        self._downloading('Downloading files for changeset', got, need)

    def downloadingChangeSet(self, got, need):
        self._downloading('Downloading', got, need)

    def requestingFileContents(self):
        if self.csHunk[1] < 2:
            self.csMsg("Requesting file contents")
        else:
            self.csMsg("Requesting file contents for changeset %d of %d" % self.csHunk)

    def requestingChangeSet(self):
        if self.csHunk[1] < 2:
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
        self.updateMsg("Running tag prescripts")

    def runningPostTagHandlers(self):
        self.updateMsg("Running tag post-scripts")

    def committingTransaction(self):
        self.updateMsg("Committing database transaction")

    def setChangesetHunk(self, num, total):
        self.csHunk = (num, total)

    def setUpdateHunk(self, num, total):
        self.restored = 0
        self.updateHunk = (num, total)

    def setUpdateJob(self, job):
        self.lock.acquire()
        if self.updateHunk[1] < 2:
            lines = [ 'Applying update job:' ]
        else:
            lines = [ 'Applying update job %d of %d:' %self.updateHunk ]
        indent = '    '
        lines.extend(formatUpdateJobInfo(job, indent = indent))
        for line in lines:
            self._message(line)
            self._message('\n')
        self.lock.release()

    def __init__(self):
        callbacks.UpdateCallback.__init__(self)
        callbacks.LineOutput.__init__(self)
        self.restored = 0
        self.csHunk = (0, 0)
        self.updateHunk = (0, 0)
        self.csText = None
        self.updateText = None
        self.lock = thread.allocate_lock()


def formatUpdateJobInfo(jobList, verbose=False, indent=''):
    new = []
    for name, (oldVersion, oldFlavor), (newVersion, newFlavor), absolute \
                                                        in jobList:
        if newVersion:
            newTVersion = newVersion.trailingRevision()
        if oldVersion:
            oldTVersion = oldVersion.trailingRevision()

        if verbose:
            if newVersion:
                newInfo = '%s[%s]' % (newVersion.asString(), 
                                      deps.formatFlavor(newFlavor))
            if oldVersion:
                oldInfo = '%s[%s]' % (oldVersion.asString(), 
                                      deps.formatFlavor(oldFlavor))
        else:
            if oldVersion:
                oldInfo = oldTVersion.asString()

            if newVersion:
                newInfo = newTVersion.asString()

        if not oldVersion:
            # if there is no oldVersion, this is a new trove
            new.append(("%s=%s" % (name, newInfo), 'Install'))
            continue
        elif not newVersion:
            # if there is no newVersion, this is a new trove
            new.append(("%s=%s" % (name, oldInfo), 'Erase  '))
            continue

        if oldVersion.branch() != newVersion.branch():
            # kind = 'Br'
            kind = 'Update '
            oldInfo = oldVersion.asString()
            newInfo = newVersion.asString()
        elif oldTVersion.getVersion() != newTVersion.getVersion():
            # kind = 'V'
            kind = 'Update '
        elif (oldTVersion.getSourceCount() !=
              newTVersion.getSourceCount()):
            # kind = 'S'
            kind = 'Update '
        else:
            # kind = 'B'
            kind = 'Update '
        if oldFlavor != newFlavor:
            flavors = deps.flavorDifferences([oldFlavor, newFlavor])
            oldFlavor = flavors[oldFlavor]
            newFlavor = flavors[newFlavor]
            if not verbose and oldFlavor:
                oldInfo = '%s[%s]' % (oldInfo, deps.formatFlavor(oldFlavor))
            if not verbose and newFlavor:
                newInfo = '%s[%s]' % (newInfo, deps.formatFlavor(newFlavor))

        new.append(("%s (%s -> %s)" % (name, oldInfo, newInfo), kind))

    new.sort()
    new = ("%s%s %s" % (indent, x[1], x[0]) for x in new)

    return new

def displayUpdateJobInfo(jobList, verbose=False):
    indent = '    '
    new = formatUpdateJobInfo(jobList, verbose=verbose, indent=indent)
    if new:
        print '\n'.join(new)

def displayUpdateInfo(updJob, verbose=False):
    totalJobs = len(updJob.getJobs())
    for num, job in enumerate(updJob.getJobs()):
        if totalJobs > 1:
            print 'Job %d of %d:' %(num + 1, totalJobs)
        displayUpdateJobInfo(job, verbose)
    return

def doUpdate(cfg, pkgList, replaceFiles = False, tagScript = None, 
                                  keepExisting = False, depCheck = True,
                                  test = False, justDatabase = False, 
                                  recurse = True, info = False, 
                                  updateByDefault = True, callback = None, 
                                  split = True, sync = False, fromFiles = [],
                                  checkBucketConflicts = True):
    if not callback:
        callback = callbacks.UpdateCallback()

    applyList = []

    if type(pkgList) is str:
        pkgList = ( pkgList, )

    # If keepExisting is true, we want our specifications to be relative
    # to nothing. If it's false, they should be absolute as updateChangeSet
    # interperts absolute jobs as ones which should be rooted (if there is
    # anything available to root them to).
    areAbsolute = not keepExisting

    fromChangesets = []
    
    for path in fromFiles:
        cs = changeset.ChangeSetFromFile(path)
        fromChangesets.append(cs)

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
            log.debug("found changeset file %s" % pkgStr)
        else:
            troveSpec = parseTroveSpec(pkgStr)
            if troveSpec[0][0] == '-':
                applyList.append((troveSpec[0], troveSpec[1:],
                                  (None, None), False))
            elif troveSpec[0][0] == '+':
                applyList.append((troveSpec[0], (None, None), 
                                  troveSpec[1:], areAbsolute))
            elif updateByDefault:
                applyList.append((troveSpec[0], (None, None), 
                                  troveSpec[1:], areAbsolute))
            else:
                applyList.append((troveSpec[0], troveSpec[1:],
                                  (None, None), False))
            log.debug("will look for %s", applyList[-1])

    # dedup
    applyList = set(applyList)
    try:
        _updateTroves(cfg, applyList, replaceFiles = replaceFiles, 
                      tagScript = tagScript, 
                      keepExisting = keepExisting, depCheck = depCheck,
                      test = test, justDatabase = justDatabase, 
                      recurse = recurse, info = info, 
                      updateByDefault = updateByDefault, callback = callback, 
                      split = split, sync = sync,
                      fromChangesets = fromChangesets,
                      checkBucketConflicts = checkBucketConflicts)
    except conaryclient.DependencyFailure, e:
        # XXX print dependency errors because the testsuite 
        # prefers it
        callback.done()
        print e
    except repository.TroveNotFound, e:
        log.error(e)
    except conaryclient.UpdateError, e:
        log.error(e)
    except repository.CommitError, e:
        log.error(e)
    except changeset.PathIdsConflictError, e:
        log.error(e)

def _updateTroves(cfg, applyList, replaceFiles = False, tagScript = None, 
                                  keepExisting = False, depCheck = True,
                                  test = False, justDatabase = False, 
                                  recurse = True, info = False, 
                                  updateByDefault = True, callback = None, 
                                  split=True, sync = False, 
                                  fromChangesets = [],
                                  checkBucketConflicts = True):

    client = conaryclient.ConaryClient(cfg)

    if not info:
	client.checkWriteableRoot()

    try:
        (updJob, suggMap) = \
        client.updateChangeSet(applyList, resolveDeps = depCheck,
                               keepExisting = keepExisting,
                               test = test, recurse = recurse,
                               updateByDefault = updateByDefault,
                               callback = callback, split = split,
                               sync = sync, fromChangesets = fromChangesets,
                               checkBucketConflicts = checkBucketConflicts)
    except:
        callback.done()
        raise

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
                       callback = callback, autoPinList = cfg.pinTroves)


def updateAll(cfg, info = False, depCheck = True, replaceFiles = False,
              test = False, showItems = False):
    client = conaryclient.ConaryClient(cfg)
    updateItems = client.fullUpdateItemList()

    applyList = [ (x[0], (None, None), x[1:], True) for x in updateItems ]

    if showItems:
        for (name, version, flavor) in sorted(updateItems, key=lambda x:x[0]):
            if version and flavor:
                print "%s=%s[%s]" % (name, version.asString(),
                                     deps.formatFlavor(flavor))
            elif flavor:
                print "%s[%s]" % (name, deps.formatFlavor(flavor))
            elif version:
                print "%s=%s" % (name, version.asString())
            else:
                print name
            
        return

    try:
        callback = UpdateCallback()
        _updateTroves(cfg, applyList, replaceFiles = replaceFiles, 
                      depCheck = depCheck, test = test, info = info, 
                      callback = callback)
    except conaryclient.DependencyFailure, e:
        log.error(e)
    except conaryclient.UpdateError, e:
        log.error(e)
    except repository.CommitError, e:
        log.error(e)
    except changeset.PathIdsConflictError, e:
        log.error(e)

def changePins(cfg, troveStrList, pin = True):
    client = conaryclient.ConaryClient(cfg)
    client.checkWriteableRoot()
    troveList = [] 
    for item in troveStrList:
        name, ver, flv = parseTroveSpec(item)
        troves = client.db.findTrove(None, (name, ver, flv))
        troveList += troves

    client.pinTroves(troveList, pin = pin)
