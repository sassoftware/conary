# Copyright (c) 2005-2008 rPath, Inc.
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

import sys
import traceback
import types

from conary import errors
from conary.lib import log

def passExceptions(f):
    f._passExceptions = True
    return f

def exceptionProtection(method, exceptionCallback):
    def wrapper(*args, **kwargs):
        if hasattr(method, '_passExceptions') and method._passExceptions:
            return method(*args, **kwargs)

        try:
            return method(*args, **kwargs)
        except Exception, e:
            exc_info = sys.exc_info()
            if errors.exceptionIsUncatchable(e):
                raise
            exceptionCallback(exc_info)

    return wrapper

class Callback(object):

    def _exceptionOccured(self, exc_info):
        etype, e, tb = exc_info
        # format the exception
        msg = '%s' % etype.__name__
        s = str(e)
        if s:
            msg += ': %s' % s
        # get the line info that raised the exception
        inner = tb.tb_next
        while inner.tb_next:
            inner = inner.tb_next
        filename = inner.tb_frame.f_code.co_filename
        linenum = inner.tb_frame.f_lineno
        log.warning('Unhandled exception occurred when invoking callback:\n'
                    '%s:%s\n'
                    ' %s', filename, linenum, msg)
        # log the full traceback if debugging (--debug=all)
        log.debug(''.join(traceback.format_exception(*exc_info)))
        if not hasattr(self, 'exceptions'):
            self.exceptions = []
        self.exceptions.append(e)

    def __getattribute__(self, name):
        item = object.__getattribute__(self, name)
        if name[0] == '_':
            return item
        elif not isinstance(item, types.MethodType):
            return item

        return exceptionProtection(item, self._exceptionOccured)

    def cancelOperation(self):
        """Return True if we should cancel the operation as soon as it is
        safely possible"""
        if not hasattr(self, 'exceptions'):
            return False
        for exc in self.exceptions:
            if hasattr(exc, 'cancelOperation'):
                return exc.cancelOperation
        return False

    def __init__(self):
        self.exceptions = []

class ChangesetCallback(Callback):

    def preparingChangeSet(self):
        """
        Called before an update begins and before it looks for the requested
        troves.

        @return: None
        """
        pass

    def requestingChangeSet(self):
        """
        Called right before requesting a changeset from a repository.
        @return: None
        """
        pass

    def sendingChangeset(self, sent, total):
        pass

    def setRate(self, rate):
        self.rate = rate

    def downloadingChangeSet(self, got, need):
        """
        Called when downloading a changeset.
        @param got: number of bytes received so far.
        @type got: integer
        @param need: number of bytes total to be retrieved.
        @type need: integer
        @return: None
        """
        pass

    def requestingFileContents(self):
        """
        Called right before requesting file contents from a repository.
        @return: None
        """
        pass

    def downloadingFileContents(self, got, need):
        """
        Called when downloading file contents.
        @param got: number of bytes received so far.
        @type got: integer
        @param need: number of bytes total to be retrieved
        @type need: integer
        @return: None
        """
        pass

    def setChangesetHunk(self, hunk, hunkCount):
        """
        Called when creating changesets, such as when downloading changesets.
        @param hunk: the number of the changeset being created (starts at 1)
        @type hunk: integer
        @param hunkCount: total number of changesets to be created.
        @type hunkCount: integer
        @return: None
        """
        pass

    def checkAbort(self):
        pass

    def done(self):
        pass

    def error(self, msg, *args, **kwargs):
        """Error handling callback

        @param msg: A message to display
        @type msg: str
        @keyword exc_text: Traceback text that should be printed verbatim
        @type exc_text: str
        """
        exc_text = kwargs.pop('exc_text', None)
        # Append the traceback to the message
        if exc_text:
            msg += "\n%s"
            args += (exc_text, )
        return log.error(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        """Warning handling callback

        @param msg: A message to display
        @type msg: str
        @keyword exc_text: Traceback text that should be printed verbatim
        @type exc_text: str
        """
        exc_text = kwargs.pop('exc_text', None)
        # Append the traceback to the message
        if exc_text:
            msg += "\n%s"
            args += (exc_text, )
        return log.warning(msg, *args, **kwargs)

    def missingFiles(self, missingFiles):
        """This callback gets called if missing files were detected in the
        upstream server
        @param missingFiles: a list of tuples:
          (troveName, troveVersion, troveFlavor, pathId, path, fileId, version)
        """
        return False

    def __init__(self):
        Callback.__init__(self)
        self.rate = 0

class CookCallback(ChangesetCallback):

    def buildingChangeset(self):
        pass

    def findingTroves(self, num):
        pass

    def gettingTroveDefinitions(self, num):
        pass

    def buildingGroup(self, groupName, idx, total):
        pass

    def groupBuilt(self):
        pass

    def groupResolvingDependencies(self):
        pass

    def groupCheckingDependencies(self):
        pass

    def groupCheckingPaths(self, current):
        pass

    def groupDeterminingPathConflicts(self, total):
        pass

class UpdateCallback(ChangesetCallback):

    def resolvingDependencies(self):
        """
        Called after requested troves have been found and before it resolves
        dependencies.

        @return: None
        """
        pass

    def creatingRollback(self):
        """
        Called when a local rollback changeset is being created.
        @return: None
        """
        pass

    def preparingUpdate(self, troveNum, troveCount):
        """
        Called while preparing to apply a given trove to the local file system.
        @param troveNum: the number of the trove currently being examined
        (starts at 1)
        @type troveNum: integer
        @param troveCount: the total number of troves to be applied.
        @type troveCount: integer
        @return None
        """
        pass

    def creatingDatabaseTransaction(self, troveNum, troveCount):
        """
        Called when creating a database transaction for each trove.
        @param troveNum: the number of the trove currently being examined
        (starts at 1)
        @type troveNum: integer
        @param troveCount: the total number of troves.
        @type troveCount: integer
        @return: None
        """
        pass

    def restoreFiles(self, size, totalSize):
        """
        Called right before writing a file to the file system.
        @param size: number of bytes in the current file
        @type size: integer
        @param totalSize: total number of bytes to be written in the current
        file system job
        @type totalSize: integer
        @return: None
        """
        pass

    def removeFiles(self, fileNum, total):
        """
        Called right before removing each file during an update or rollback.
        @param fileNum: the number of the file being removed (starts at 1).
        @type fileNum: integer
        @param total: total number of files to be removed.
        @type total: integer
        @return: None
        """
        pass

    def runningPreTagHandlers(self):
        """
        Called right before running the pre action of tag handlers.
        @return: None
        """
        pass

    def runningPostTagHandlers(self):
        """
        Called right before running the post action of tag handlers.
        @return: None
        """
        pass

    def committingTransaction(self):
        """
        Called right before committing a database transaction.  This is called
        at the end of each update job.
        @return: None
        """
        pass

    def updateDone(self):
        """
        Called when each update job finishes.  Recall that an update operation
        may be split into multiple jobs.

        @return: None
        """
        pass

    def tagHandlerOutput(self, tag, msg, stderr = False):
        """
        Called when a tag handler outputs text to stdout or stderr.  This
        method is called once for each line that's output.
        @param tag: name of the tag handler
        @type tag: string
        @param msg: line that was output
        @type msg: string
        @param stderr: whether this was output to stderr.  False indicates
        this was output to stdout.
        @type stderr: boolean
        @return: None
        """
        print "[%s] %s" % (tag, msg),

    def troveScriptOutput(self, typ, msg):
        """
        Called for each line of output generated by the trove script execution.
        @param typ: contains the name of the trove followed by stage, where
        stage is one of "postrollback", "postupdate", "postinstall",
        "preupdate", e.g. "group-dist postupdate"
        @type typ: string
        @param msg: the line output by the trove script.
        @type msg: string
        @return: None
        """
        print "[%s] %s" % (typ, msg)

    def troveScriptStarted(self, typ):
        """Called when the script starts to execute"""
        pass

    def troveScriptFinished(self, typ):
        """Called upon a successful execution of the script.
        If the script failed, scriptFailure is called instead"""
        pass

    def troveScriptFailure(self, typ, errcode):
        """
        Called if the script execution fails
        @param typ: name of the script followed by stage.
        @type typ: string
        @param errcode: non-zero error code returned by the trove script.
        @type errcode: integer
        @return: None
        """
        print "[%s] %s" % (typ, errcode)

    def setUpdateHunk(self, hunk, hunkCount):
        """
        Called before applying a given update job.
        @param hunk: the number of the update job being applied (starts at 1)
        @type hunk: integer
        @param hunkCount: the total number of update jobs.
        @type hunkCount: integer
        @return: None
        """
        pass

    def setUpdateJob(self, job):
        """
        Called right before applying the given update job.
        @param job: the update job about to be applied.
        @type job: a set, where each item is a tuple containing C{(troveName,
        (oldVersionSpec, oldFlavor), (newVersionSpec, newFlavor), isAbsolute)}
        @see conaryclient.update.ClientUpdate.prepareUpdateJob
        @return: None
        """
        pass

    def done(self):
        """
        Called after an update.

        More specifically, when:
         - an update finishes
         - a fatal exception occurs before an update
         - the info option is passed in and after the job set is determined
         - extra troves are resolved in after the job set is determined
         - after restarting an update that contains critical troves
        """
        pass

    def checkAbort(self):
        return (self.abortEvent and self.abortEvent.isSet()) or self.cancelOperation()

    def setAbortEvent(self, event = None):
        self.abortEvent = event

    def verifyTroveSignatures(self, trv):
        # @rtype: (int, list)
        # @raise DigitalSignatureVerificationError: 

        # Default implementation - you can override it if you want to handle
        # the exception yourself
        return trv.verifyDigitalSignatures(threshold=self.trustThreshold,
                                           keyCache=self.keyCache)

    def setTrustThreshold(self, trustThreshold):
        self.trustThreshold = trustThreshold

    def __init__(self, trustThreshold=0, keyCache=None):
        ChangesetCallback.__init__(self)
        self.abortEvent = None
        self.trustThreshold = trustThreshold
        self.keyCache = keyCache

class SignatureCallback(Callback):

    def signTrove(self, got, need):
        pass

class FetchCallback(Callback):

    def setRate(self, rate):
        self.rate = rate

    def fetch(self, got, need):
        pass

    def __init__(self):
        Callback.__init__(self)
        self.rate = 0

class KeyCacheCallback(Callback):

    def getPublicKey(self, keyId, serverName, warn=False):
        return False

    def __init__(self, repos = None, cfg = None):
        Callback.__init__(self)
        self.repos = repos
        self.cfg = cfg

class CloneCallback(ChangesetCallback):
    def __init__(self, cfg=None):
        self.cfg = cfg

    def getCloneChangeLog(self, trv):
        return trv.getChangeLog()

    def determiningCloneTroves(self, current=0, total=0):
        pass

    def determiningTargets(self, current=0, total=0):
        pass

    def targetSources(self, current=0, total=0):
        pass

    def targetBinaries(self, current=0, total=0):
        pass

    def checkNeedsFulfilled(self, current=0, total=0):
        pass

    def rewriteTrove(self, current=0, total=0):
        pass

    def buildingChangeset(self, current=0, total=0):
        pass

    def requestingFiles(self, number):
        pass

    def requestingFileContentsWithCount(self, count):
        pass

    def gettingCloneData(self):
        pass


class CallbackRateWrapper:
    def __init__(self, callback, fn, total):
        self._callback = callback
        self.fn = fn
        self.total = total

        # zero counters
        callback.setRate(0)
        fn(0, total)

    def callback(self, amount, rate):
        self._callback.setRate(rate)
        self.fn(amount, self.total)

class LineOutput:
    last = 0
    out = sys.stdout

    def _message(self, msg):
        if self.out.isatty():
            self.out.write("\r")
            self.out.write(msg)
            if len(msg) < self.last:
                i = self.last - len(msg)
                self.out.write(" " * i + "\b" * i)
            self.out.flush()
            self.lastMessage = msg
            self.last = len(msg)

    def __del__(self):
        if self.last:
            self._message("")
            print >> self.out, "\r",
            self.out.flush()

    def __init__(self, f = sys.stdout):
        self.last = 0
        self.lastMessage = ''
        self.out = f
