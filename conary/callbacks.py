# Copyright (c) 2005-2006 rPath, Inc.
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
        pass

    def requestingChangeSet(self):
        pass

    def sendingChangeset(self, sent, total):
        pass

    def setRate(self, rate):
        self.rate = rate

    def downloadingChangeSet(self, got, need):
        pass

    def requestingFileContents(self):
        pass

    def downloadingFileContents(self, got, need):
        pass

    def setChangesetHunk(self, hunk, hunkCount):
        pass

    def checkAbort(self):
        pass

    def done(self):
        pass

    def error(self, msg, *args, **kwargs):
        """Error handling callback
        If the optional keyword argument exc_text is passed, its value should
        be printed verbatim since it is traceback information.
        """
        exc_text = kwargs.pop('exc_text', None)
        # Append the traceback to the message
        if exc_text:
            msg += "\n%s"
            args += (exc_text, )
        return log.error(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        """Warning handling callback
        If the optional keyword argument exc_text is passed, its value should
        be printed verbatim since it is traceback information.
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
        pass

    def creatingRollback(self):
        pass

    def preparingUpdate(self, troveNum, troveCount):
        pass

    def creatingDatabaseTransaction(self, troveNum, troveCount):
        pass

    def restoreFiles(self, size, totalSize):
        pass

    def removeFiles(self, fileNum, total):
        pass

    def runningPreTagHandlers(self):
        pass

    def runningPostTagHandlers(self):
        pass

    def committingTransaction(self):
        pass

    def updateDone(self):
        pass

    def tagHandlerOutput(self, tag, msg, stderr = False):
        print "[%s] %s" % (tag, msg),

    def troveScriptOutput(self, typ, msg):
        """Called for each line of output generated by the script execution.
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
        """Called if the script execution fails"""
        print "[%s] %s" % (typ, errcode)

    def setUpdateHunk(self, hunk, hunkCount):
        pass

    def setUpdateJob(self, job):
        pass

    def done(self):
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

    def setPublicPath(self, path):
        """Set the path to the public keyring"""
        self.pubRing = path

    def __init__(self, repos = None, cfg = None, pubRing = ''):
        Callback.__init__(self)
        self.repos = repos
        self.pubRing = pubRing
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
