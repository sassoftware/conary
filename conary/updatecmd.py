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
import os
import sys
import thread
import urllib2

from conary import callbacks
from conary import conaryclient
from conary.deps import deps
from conary.lib import log
from conary.lib import util
from conary.local import database
from conary.repository import changeset
from conary.repository import errors
from conaryclient import cmdline
from conaryclient.cmdline import parseTroveSpec

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
        else: # no idea how much we need, just keep on counting...
            self.csMsg("%s (got %dk so far)" % (msg, got / 1024))

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
        # erase anything that is currently displayed
        self._message('')
        indent = '    '
        lines.extend(formatUpdateJobInfo(job, indent = indent))
        for line in lines:
            # don't use self._message since we are not expecting to erase
            # this line.
            self.out.write(line + '\n')
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


def formatJob(job, verbose=False):
    # format a single job entry
    (name, (oldVersion, oldFlavor),
           (newVersion, newFlavor), absolute) = job
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
        return "%s=%s" % (name, newInfo), 'Install'
    elif not newVersion:
        # if there is no newVersion, this is a new trove
        return "%s=%s" % (name, oldInfo), 'Erase  '

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

    return "%s (%s -> %s)" % (name, oldInfo, newInfo), kind

def formatCollapsedJobs(jobList):
    # take a list of jobs that starts with a package update job and
    # continues with componet update jobs (that are a part of the same
    # package) and return a formatted string representing the job
    # for both the packages and the components.
    if len(jobList) == 1:
        # if we only have one job, we're just doing something to the
        # package, not to any components.  Treat it as a single update
        return formatJob(jobList[0])

    # otherwise, form up a slightly modified job that collapses all
    # the components into the name of the package.
    pkg = jobList[0]
    pkgName = pkg[0]
    pkgLen = len(pkgName)
    collapsedName = '%s(%s)' % (pkgName,
                                # get the :component name for each component
                                ' '.join(x[0][pkgLen:] for x in jobList[1:]))
    return formatJob((collapsedName, ) + pkg[1:])

def formatUpdateJobInfo(jobList, verbose=False, indent=''):
    lines = []
    currentPkg = None
    collapsedInfo = []
    # use sorted() here to collapse component and package update info
    # line into one line.  We assume that foo sorts before foo:bar.
    for job in sorted(jobList):
        name = job[0]
        # if we're in the middle of processing a package, check to
        # see if this is a component of that package.  
        if currentPkg:
            if name.startswith(currentPkg[0]) and job[1:] == currentPkg[1]:
                # If it is, collapse the component into the package line.
                collapsedInfo.append(job)
                continue
            else:
                # otherwise, dump what we have so far
                lines.append(formatCollapsedJobs(collapsedInfo))
                collapsedInfo = []
                currentPkg = None

        # if we're not dealing with a component, and there is not
        # currently a package, set the current package as this job
        if not currentPkg and ':' not in name:
            collapsedInfo.append(job)
            currentPkg = (name + ':', job[1:])
            continue

        # this is a lone job. format it by itself.
        lines.append(formatJob(job, verbose=verbose))

    if collapsedInfo:
        # handle any left over collapsed info
        lines.append(formatCollapsedJobs(collapsedInfo))

    lines = ("%s%s %s" % (indent, x[1], x[0]) for x in lines)
    return lines

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

def doUpdate(cfg, changeSpecs, replaceFiles = False, tagScript = None, 
                               keepExisting = False, depCheck = True,
                               test = False, justDatabase = False, 
                               recurse = True, info = False, 
                               updateByDefault = True, callback = None, 
                               split = True, sync = False, fromFiles = [],
                               checkPathConflicts = True):
    if not callback:
        callback = callbacks.UpdateCallback()

    fromChangesets = []
    
    for path in fromFiles:
        cs = changeset.ChangeSetFromFile(path)
        fromChangesets.append(cs)

    applyList = cmdline.parseChangeList(changeSpecs, keepExisting, 
                                        updateByDefault, allowChangeSets=True)
    
    try:
        _updateTroves(cfg, applyList, replaceFiles = replaceFiles, 
                      tagScript = tagScript, 
                      keepExisting = keepExisting, depCheck = depCheck,
                      test = test, justDatabase = justDatabase, 
                      recurse = recurse, info = info, 
                      updateByDefault = updateByDefault, callback = callback, 
                      split = split, sync = sync,
                      fromChangesets = fromChangesets,
                      checkPathConflicts = checkPathConflicts)
    except conaryclient.DependencyFailure, e:
        # XXX print dependency errors because the testsuite 
        # prefers it
        callback.done()
        print e
    except errors.TroveNotFound, e:
        log.error(e)
    except conaryclient.UpdateError, e:
        log.error(e)
    except errors.CommitError, e:
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
                                  checkPathConflicts = True, 
                                  ignorePrimaryPins = True):

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
                               checkPathConflicts = checkPathConflicts,
                               ignorePrimaryPins = ignorePrimaryPins)
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

    if cfg.interactive:
        print 'The following updates will be performed:'
        displayUpdateInfo(updJob)
        okay = cmdline.askYn('continue with update? [Y/n]', default=True)

        if not okay:
            return

    client.applyUpdate(updJob, replaceFiles, tagScript, test = test, 
                       justDatabase = justDatabase,
                       localRollbacks = cfg.localRollbacks,
                       callback = callback, autoPinList = cfg.pinTroves, threshold = cfg.trustThreshold)


# we grab a url from the repo based on our version and flavor,
# download the changeset it points to and update it
def updateConary(cfg, conaryVersion):
    def _urlNotFound(url, msg = None):
        print >> sys.stderr, "While attempting to download from", url.url
        print >> sys.stderr, "ERROR: Could not download the conary changeset."
        if msg is not None:
            print >> sys.stderr, "Server Error Code:", msg.code, msg.msg        
        url.close()
        return -1    
    # first, grab the label of the installed conary client
    db = database.Database(cfg.root, cfg.dbPath)    
    troves = db.trovesByName("conary")

    # filter based on the version of conary this is (after all, we should
    # try to update ourself; not something else)
    troves = [ x for x in troves if 
                   x[1].trailingRevision().getVersion() == conaryVersion ]

    # FIXME: what should we do if this comes back as having more than
    # one version of conary installed?
    assert(len(troves)==1)
    (name, version, flavor) = troves[0]   
    client = conaryclient.ConaryClient(cfg)
    csUrl = client.getConaryUrl(version, flavor)
    if csUrl == "":
        print "There is no update available for your conary client version"
        return
    try:
        url = urllib2.urlopen(csUrl)
    except urllib2.HTTPError, msg:
        return _urlNotFound(url, msg)
    csSize = 0
    if url.info().has_key("content-length"):
        csSize = int(url.info()["content-length"])
        
    # check that we can make updates before bothering with downloading this
    client.checkWriteableRoot()

    # download the changeset
    (fd, path) = util.mkstemp()
    os.unlink(path)
    dst = os.fdopen(fd, "r+")
    callback = UpdateCallback()
    dlSize = util.copyfileobj(
        url, dst, bufSize = 16*1024,
        callback = lambda x, m=csSize: callback.downloadingChangeSet(x, m)
        )
    if not dlSize:
        return _urlNotFound(url)
   
    url.close()
    cs = changeset.ChangeSetFromFile(dst)
    # try to apply this changeset, with as much resemblance to a --force
    # option as we can flag in the applyUpdate call
    try:
        (job, other) = client.updateChangeSet(set([cs]), callback=callback)
    except:
        callback.done()
        raise
    return client.applyUpdate(job, localRollbacks = cfg.localRollbacks,
                              callback = callback, replaceFiles = True)
    
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
                      callback = callback, ignorePrimaryPins = False)
    except conaryclient.DependencyFailure, e:
        log.error(e)
    except conaryclient.UpdateError, e:
        log.error(e)
    except errors.CommitError, e:
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
