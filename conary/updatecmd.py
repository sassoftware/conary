#
# Copyright (c) 2004-2007 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
import os
import itertools
import sys
import threading
import urllib2

from conary import callbacks
from conary import conaryclient
from conary import display
from conary import errors
from conary import versions
from conary.deps import deps
from conary.lib import log
from conary.lib import util
from conary.local import database
from conary.repository import changeset
from conaryclient import cmdline
from conaryclient.cmdline import parseTroveSpec

# FIXME client should instantiated once per execution of the command line 
# conary client

class CriticalUpdateInfo(conaryclient.CriticalUpdateInfo):
    criticalTroveRegexps = ['conary:.*']

def locked(method):
    # this decorator used to be defined in UpdateCallback
    # The problem is you cannot subclass UpdateCallback and use the decorator
    # because python complains it is an unbound function.
    # And you can't define it as @staticmethod either, it would break the
    # decorated functions.
    # Somewhat related (staticmethod objects not callable) topic:
    # http://mail.python.org/pipermail/python-dev/2006-March/061948.html

    def wrapper(self, *args, **kwargs):
        self.lock.acquire()
        try:
            return method(self, *args, **kwargs)
        finally:
            self.lock.release()

    return wrapper

class UpdateCallback(callbacks.LineOutput, callbacks.UpdateCallback):

    def done(self):
        """
        @see: callbacks.UpdateCallback.done
        """
        self._message('')

    def _message(self, text):
        """
        Called when this callback object needs to output progress information.
        The information is written to stdout.

        @return: None
        """
        callbacks.LineOutput._message(self, text)

    def update(self):
        """
        Called by this callback object to update the status.  This method
        sanitizes text.  This method is not thread safe - obtain a lock before
        calling.

        @return: None
        """

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

    @locked
    def updateMsg(self, text):
        """
        Called when the update thread has status updates.

        @param text: new status text
        @type text: string

        @return: None
        """
        self.updateText = text
        self.update()

    @locked
    def csMsg(self, text):
        """
        Called when the download thread has status updates.

        @param text: new status text
        @type text: string

        @return: None
        """

        self.csText = text
        self.update()

    def preparingChangeSet(self):
        """
        @see: callbacks.ChangesetCallback.preparingChangeSet
        """
        self.updateMsg("Preparing changeset request")

    def resolvingDependencies(self):
        """
        @see: callbacks.UpdateCallback.resolvingDependencies
        """
        self.updateMsg("Resolving dependencies")

    @locked
    def updateDone(self):
        """
        @see: callbacks.UpdateCallback.updateDone
        """
        self._message('')
        self.updateText = None

    @locked
    def _downloading(self, msg, got, rate, need):
        """
        Called by this callback object to handle different kinds of
        download-related progress information.  This method puts together
        download rate information.

        @param msg: status message
        @type msg: string
        @param got: number of bytes retrieved so far
        @type got: integer
        @param rate: bytes per second
        @type rate: integer
        @param need: number of bytes total to be retrieved
        @type need: integer
        @return: None
        """
        # This function acquires a lock just because it looks at self.csHunk
        # and self.updateText directly. Otherwise, self.csMsg will acquire the
        # lock (which is now reentrant)
        if got == need:
            self.csMsg(None)
        elif need != 0:
            if self.csHunk[1] < 2 or not self.updateText:
                self.csMsg("%s %dKB (%d%%) of %dKB at %dKB/sec"
                           % (msg, got/1024, (got*100)/need, need/1024, rate/1024))
            else:
                self.csMsg("%s %d of %d: %dKB (%d%%) of %dKB at %dKB/sec"
                           % ((msg,) + self.csHunk + \
                              (got/1024, (got*100)/need, need/1024, rate/1024)))
        else: # no idea how much we need, just keep on counting...
            self.csMsg("%s (got %dKB at %dKB/s so far)" % (msg, got/1024, rate/1024))

    def downloadingFileContents(self, got, need):
        """
        @see: callbacks.ChangesetCallback.downloadingFileContents
        """
        self._downloading('Downloading files for changeset', got, self.rate, need)

    def downloadingChangeSet(self, got, need):
        """
        @see: callbacks.ChangesetCallback.downloadingChangeSet
        """
        self._downloading('Downloading', got, self.rate, need)

    def requestingFileContents(self):
        """
        @see: callbacks.ChangesetCallback.requestingFileContents
        """
        if self.csHunk[1] < 2:
            self.csMsg("Requesting file contents")
        else:
            self.csMsg("Requesting file contents for changeset %d of %d" % self.csHunk)

    def requestingChangeSet(self):
        """
        @see: callbacks.ChangesetCallback.requestingChangeSet
        """
        if self.csHunk[1] < 2:
            self.csMsg("Requesting changeset")
        else:
            self.csMsg("Requesting changeset %d of %d" % self.csHunk)

    def creatingRollback(self):
        """
        @see: callbacks.UpdateCallback.creatingRollback
        """
        self.updateMsg("Creating rollback")

    def preparingUpdate(self, troveNum, troveCount):
        """
        @see: callbacks.UpdateCallback.preparingUpdate
        """
        self.updateMsg("Preparing update (%d of %d)" % 
		      (troveNum, troveCount))

    @locked
    def restoreFiles(self, size, totalSize):
        """
        @see: callbacks.UpdateCallback.restoreFiles
        """
        # Locked, because we modify self.restored
        if totalSize != 0:
            self.restored += size
            self.updateMsg("Writing %dk of %dk (%d%%)" 
                        % (self.restored / 1024 , totalSize / 1024,
                           (self.restored * 100) / totalSize))

    def removeFiles(self, fileNum, total):
        """
        @see: callbacks.UpdateCallback.removeFiles
        """
        if total != 0:
            self.updateMsg("Removing %d of %d (%d%%)"
                        % (fileNum , total, (fileNum * 100) / total))

    def creatingDatabaseTransaction(self, troveNum, troveCount):
        """
        @see: callbacks.UpdateCallback.creatingDatabaseTransaction
        """
        self.updateMsg("Creating database transaction (%d of %d)" %
		      (troveNum, troveCount))

    def runningPreTagHandlers(self):
        """
        @see: callbacks.UpdateCallback.runningPreTagHandlers
        """
        self.updateMsg("Running tag prescripts")

    def runningPostTagHandlers(self):
        """
        @see: callbacks.UpdateCallback.runningPostTagHandlers
        """
        self.updateMsg("Running tag post-scripts")

    def committingTransaction(self):
        """
        @see: callbacks.UpdateCallback.committingTransaction
        """
        self.updateMsg("Committing database transaction")

    @locked
    def setChangesetHunk(self, num, total):
        """
        @see: callbacks.ChangesetCallback.setChangesetHunk
        """
        self.csHunk = (num, total)

    @locked
    def setUpdateHunk(self, num, total):
        """
        @see: callbacks.UpdateCallback.setUpdateHunk
        """
        self.restored = 0
        self.updateHunk = (num, total)

    @locked
    def setUpdateJob(self, jobs):
        """
        @see: callbacks.UpdateCallback.setUpdateJob
        """
        self._message('')
        if self.updateHunk[1] < 2:
            self.out.write('Applying update job:\n')
        else:
            self.out.write('Applying update job %d of %d:\n' % self.updateHunk)
        # erase anything that is currently displayed
        self._message('')
        self.formatter.prepareJobs(jobs)
        for line in self.formatter.formatJobTups(jobs, indent='    '):
            self.out.write(line + '\n')

    @locked
    def tagHandlerOutput(self, tag, msg, stderr = False):
        """
        @see: callbacks.UpdateCallback.tagHandlerOutput
        """
        self._message('')
        self.out.write('[%s] %s\n' % (tag, msg))

    @locked
    def troveScriptOutput(self, typ, msg):
        """
        @see: callbacks.UpdateCallback.troveScriptOutput
        """
        self._message('')
        self.out.write("[%s] %s" % (typ, msg))

    @locked
    def troveScriptFailure(self, typ, errcode):
        """
        @see: callbacks.UpdateCallback.troveScriptFailure
        """
        self._message('')
        self.out.write("[%s] %s" % (typ, errcode))

    def __init__(self, cfg=None):
        """
        Initialize this callback object.
        @param cfg: Conary configuration
        @type cfg: A ConaryConfiguration object.
        @return: None
        """
        callbacks.UpdateCallback.__init__(self)
        if cfg:
            self.setTrustThreshold(cfg.trustThreshold)
        callbacks.LineOutput.__init__(self)
        self.restored = 0
        self.csHunk = (0, 0)
        self.updateHunk = (0, 0)
        self.csText = None
        self.updateText = None
        self.lock = threading.RLock()

        if cfg:
            fullVersions = cfg.fullVersions
            showFlavors = cfg.fullFlavors
            showLabels = cfg.showLabels
            baseFlavors = cfg.flavor
            showComponents = cfg.showComponents
            db = conaryclient.ConaryClient(cfg).db
        else:
            fullVersions = showFlavors = showLabels = db = baseFlavors = None
            showComponents = None

        self.formatter = display.JobTupFormatter(affinityDb=db)
        self.formatter.dcfg.setTroveDisplay(fullVersions=fullVersions,
                                            fullFlavors=showFlavors,
                                            showLabels=showLabels,
                                            baseFlavors=baseFlavors,
                                            showComponents=showComponents)
        self.formatter.dcfg.setJobDisplay(compressJobs=not showComponents)

def displayChangedJobs(addedJobs, removedJobs, cfg):
    db = conaryclient.ConaryClient(cfg).db
    formatter = display.JobTupFormatter(affinityDb=db)
    formatter.dcfg.setTroveDisplay(fullVersions=cfg.fullVersions,
                                   fullFlavors=cfg.fullFlavors,
                                   showLabels=cfg.showLabels,
                                   baseFlavors=cfg.flavor,
                                   showComponents=cfg.showComponents)
    formatter.dcfg.setJobDisplay(compressJobs=not cfg.showComponents)
    formatter.prepareJobLists([removedJobs | addedJobs])

    if removedJobs:
        print 'No longer part of job:'
        for line in formatter.formatJobTups(removedJobs, indent='    '):
            print line
    if addedJobs:
        print 'Added to job:'
        for line in formatter.formatJobTups(addedJobs, indent='    '):
            print line

def displayUpdateInfo(updJob, cfg):
    jobLists = updJob.getJobs()
    db = conaryclient.ConaryClient(cfg).db

    formatter = display.JobTupFormatter(affinityDb=db)
    formatter.dcfg.setTroveDisplay(fullVersions=cfg.fullVersions,
                                   fullFlavors=cfg.fullFlavors,
                                   showLabels=cfg.showLabels,
                                   baseFlavors=cfg.flavor,
                                   showComponents=cfg.showComponents)
    formatter.dcfg.setJobDisplay(compressJobs=not cfg.showComponents)
    formatter.prepareJobLists(jobLists)

    totalJobs = len(jobLists)
    for num, job in enumerate(jobLists):
        if totalJobs > 1:
            if num in updJob.getCriticalJobs():
                print '** ',
            print 'Job %d of %d:' % (num + 1, totalJobs)
        for line in formatter.formatJobTups(job, indent='    '):
            print line
    if updJob.getCriticalJobs():
        criticalJobs = updJob.getCriticalJobs()
        if len(criticalJobs) > 1:
            jobPlural = 's'
        else:
            jobPlural = ''
        jobList = ', '.join([str(x + 1) for x in criticalJobs])
        print
        print '** The update will restart itself after job%s %s and continue updating' % (jobPlural, jobList)
    return

def doUpdate(cfg, changeSpecs, **kwargs):
    callback = kwargs.get('callback', None)
    if not callback:
        callback = callbacks.UpdateCallback(trustThreshold=cfg.trustThreshold)
        kwargs['callback'] = callback
    else:
        callback.setTrustThreshold(cfg.trustThreshold)

    syncChildren = kwargs.get('syncChildren', False)
    syncUpdate = kwargs.pop('syncUpdate', False)
    restartInfo = kwargs.get('restartInfo', None)

    if syncChildren or syncUpdate:
        installMissing = True
    else:
        installMissing = False

    kwargs['installMissing'] = installMissing

    fromChangesets = []
    for path in kwargs.pop('fromFiles', []):
        cs = changeset.ChangeSetFromFile(path)
        fromChangesets.append(cs)

    kwargs['fromChangesets'] = fromChangesets

    # Look for items which look like files in the applyList and convert
    # them into fromChangesets w/ the primary sets
    for item in changeSpecs[:]:
        if os.access(item, os.R_OK):
            try:
                cs = changeset.ChangeSetFromFile(item)
            except:
                continue

            fromChangesets.append(cs)
            changeSpecs.remove(item)
            for trvInfo in cs.getPrimaryTroveList():
                changeSpecs.append("%s=%s[%s]" % (trvInfo[0],
                      trvInfo[1].asString(), deps.formatFlavor(trvInfo[2])))

    if kwargs.get('restartInfo', None):
        # We don't care about applyList, we will set it later
        applyList = None
    else:
        keepExisting = kwargs.get('keepExisting')
        updateByDefault = kwargs.get('updateByDefault', True)
        applyList = cmdline.parseChangeList(changeSpecs, keepExisting,
                                            updateByDefault,
                                            allowChangeSets=True)

    _updateTroves(cfg, applyList, **kwargs)
    # XXX fixme
    # Clean up after ourselves
    if restartInfo:
        util.rmtree(restartInfo, ignore_errors=True)


def _updateTroves(cfg, applyList, **kwargs):
    # Take out the apply-related keyword arguments
    applyDefaults = dict(
                        replaceFiles = False,
                        replaceManagedFiles = False,
                        replaceUnmanagedFiles = False,
                        replaceModifiedFiles = False,
                        replaceModifiedConfigFiles = False,
                        tagScript = None,
                        justDatabase = False,
                        info = False,
                        keepJournal = False,
                        noRestart = False,
    )
    applyKwargs = {}
    for k in applyDefaults:
        if k in kwargs:
            applyKwargs[k] = kwargs.pop(k)

    callback = kwargs.pop('callback')
    applyKwargs['test'] = kwargs.get('test', False)
    applyKwargs['localRollbacks'] = cfg.localRollbacks
    applyKwargs['autoPinList'] = cfg.pinTroves

    noRestart = applyKwargs.get('noRestart', False)

    client = conaryclient.ConaryClient(cfg)
    client.setUpdateCallback(callback)
    migrate = kwargs.get('migrate', False)
    forceMigrate = kwargs.pop('forceMigrate', False)
    restartInfo = kwargs.get('restartInfo', None)

    # Initialize the critical update set
    applyCriticalOnly = kwargs.get('applyCriticalOnly', False)
    kwargs['criticalUpdateInfo'] = CriticalUpdateInfo(applyCriticalOnly)

    info = applyKwargs.pop('info', False)

    # Rename depCheck to resolveDeps
    depCheck = kwargs.pop('depCheck', True)
    kwargs['resolveDeps'] = depCheck

    if not info:
        client.checkWriteableRoot()

    if migrate and not info and not cfg.interactive and not forceMigrate:
        print ('Migrate must be run with --interactive'
               ' because it now has the potential to damage your'
               ' system irreparably if used incorrectly.')
        return

    updJob = client.newUpdateJob()

    try:
        suggMap = client.prepareUpdateJob(updJob, applyList, **kwargs)
    except:
        callback.done()
        raise

    if info:
        callback.done()
        displayUpdateInfo(updJob, cfg)
        if restartInfo:
            callback.done()
            newJobs = set(itertools.chain(*updJob.getJobs()))
            oldJobs = set(updJob.getItemList())
            addedJobs = newJobs - oldJobs
            removedJobs = oldJobs - newJobs
            if addedJobs or removedJobs:
                print
                print 'NOTE: after critical updates were applied, the contents of the update were recalculated:'
                print
                displayChangedJobs(addedJobs, removedJobs, cfg)
        return

    if suggMap:
        callback.done()
        dcfg = display.DisplayConfig()
        dcfg.setTroveDisplay(fullFlavors = cfg.fullFlavors,
                             fullVersions = cfg.fullVersions,
                             showLabels = cfg.showLabels)
        formatter = display.TroveTupFormatter(dcfg)

        print "Including extra troves to resolve dependencies:"
        print "   ",

        items = sorted(set(formatter.formatNVF(*x)
                       for x in itertools.chain(*suggMap.itervalues())))
        print " ".join(items)

    askInteractive = cfg.interactive
    if restartInfo:
        callback.done()
        newJobs = set(itertools.chain(*updJob.getJobs()))
        oldJobs = set(updJob.getItemList())
        addedJobs = newJobs - oldJobs
        removedJobs = oldJobs - newJobs

        if addedJobs or removedJobs:
            print 'NOTE: after critical updates were applied, the contents of the update were recalculated:'
            displayChangedJobs(addedJobs, removedJobs, cfg)
        else:
            askInteractive = False
    elif askInteractive:
        print 'The following updates will be performed:'
        displayUpdateInfo(updJob, cfg)
    if migrate and cfg.interactive:
        print ('Migrate erases all troves not referenced in the groups'
               ' specified.')

    if askInteractive:
        if migrate:
            values = 'migrate', '[y/N]'
            default = False
        else:
            values = 'update', '[Y/n]'
            default = True
        okay = cmdline.askYn('continue with %s? %s' % values, default=default)
        if not okay:
            return

    if not noRestart and updJob.getCriticalJobs():
        print "Performing critical system updates, will then restart update."

    restartDir = client.applyUpdateJob(updJob, **applyKwargs)

    if restartDir:
        params = sys.argv

        # Write command line to disk
        import xmlrpclib
        cmdlinefile = open(os.path.join(restartDir, 'cmdline'), "w")
        cmdlinefile.write(xmlrpclib.dumps((params, ), methodresponse = True))
        cmdlinefile.close()

        # CNY-980: we should have the whole script of changes to perform in
        # the restart directory (in the job list); if in migrate mode, re-exec
        # as regular update
        if migrate and 'migrate' in params:
            params[params.index('migrate')] = 'update'

        params.extend(['--restart-info=%s' % restartDir])
        raise errors.ReexecRequired(
                'Critical update completed, rerunning command...', params,
                restartDir)

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

    if len(troves) > 1:
        # filter based on the version of conary this is (after all, we should
        # try to update ourself; not something else)
        troves = [ x for x in troves if 
                   x[1].trailingRevision().getVersion() == conaryVersion ]

    # FIXME: if no conary troves are found to be installed, should we
    # attempt a recover/install anyway?
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
    callback = UpdateCallback(cfg)
    dlSize = util.copyfileobj(
        url, dst, bufSize = 16*1024,
        callback = lambda x, r, m=csSize: callback.downloadingChangeSet(x, r, m)
        )
    if not dlSize:
        return _urlNotFound(url)
    client.setUpdateCallback(callback)
    url.close()
    cs = changeset.ChangeSetFromFile(dst)
    # try to apply this changeset, with as much resemblance to a --force
    # option as we can flag in the applyUpdate call
    try:
        (job, other) = client.updateChangeSet(set([cs]))
    except:
        callback.done()
        raise
    return client.applyUpdate(job, localRollbacks = cfg.localRollbacks,
                              replaceFiles = True)

def updateAll(cfg, **kwargs):
    showItems = kwargs.pop('showItems', False)
    restartInfo = kwargs.get('restartInfo', None)
    migrate = kwargs.pop('migrate', False)
    kwargs['installMissing'] = kwargs['removeNotByDefault'] = migrate
    kwargs['callback'] = UpdateCallback(cfg)

    client = conaryclient.ConaryClient(cfg)
    if restartInfo:
        updateItems = []
        applyList = None
    else:
        updateItems = client.fullUpdateItemList()
        applyList = [ (x[0], (None, None), x[1:], True) for x in updateItems ]

    if showItems:
        for (name, version, flavor) in sorted(updateItems, key=lambda x:x[0]):
            if version and (flavor is not None) and not flavor.isEmpty():
                print "'%s=%s[%s]'" % (name, version.asString(), deps.formatFlavor(flavor))
            elif (flavor is not None) and not flavor.isEmpty():
                print "'%s[%s]'" % (name, deps.formatFlavor(flavor))
            elif version:
                print "%s=%s" % (name, version.asString())
            else:
                print name

        return

    _updateTroves(cfg, applyList, **kwargs)
    # Clean up after ourselves
    if restartInfo:
        util.rmtree(restartInfo, ignore_errors=True)

def changePins(cfg, troveStrList, pin = True):
    client = conaryclient.ConaryClient(cfg)
    client.checkWriteableRoot()
    troveList = [] 
    for item in troveStrList:
        name, ver, flv = parseTroveSpec(item)
        troves = client.db.findTrove(None, (name, ver, flv))
        troveList += troves

    client.pinTroves(troveList, pin = pin)

def revert(cfg):
    conaryclient.ConaryClient.revertJournal(cfg)
