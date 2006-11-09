# Copyright (c) 2005-2006 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import sys
import types

from conary import errors
from conary.lib import log

def exceptionProtection(method, exceptionCallback):
    def wrapper(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except Exception, e:
            if errors.exceptionIsUncatchable(e):
                raise

            exceptionCallback(e)

    return wrapper

class Callback(object):

    def _exceptionOccured(self, e):
        log.warning("%s: %s" % (e.__class__.__name__, e))
        self.exceptions.append(e)

    def __getattribute__(self, name):
        item = object.__getattribute__(self, name)
        if name[0] == '_':
            return item
        elif not isinstance(item, types.MethodType):
            return item

        return exceptionProtection(item, self._exceptionOccured)

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

    def setUpdateHunk(self, hunk, hunkCount):
        pass

    def setUpdateJob(self, job):
        pass

    def done(self):
        pass

    def checkAbort(self):
        return (self.abortEvent and self.abortEvent.isSet()) or self.exceptions

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

    def __init__(self, repositoryMap = None, pubRing = ''):
        Callback.__init__(self)
        self.repositoryMap = repositoryMap
        self.pubRing = pubRing

class CloneCallback(ChangesetCallback):
    def __init__(self, cfg=None):
        self.cfg = cfg

    def getCloneChangeLog(self, trv):
        return trv.getChangeLog()

    def determiningCloneTroves(self):
        pass

    def determiningTargets(self):
        pass

    def rewritingFileVersions(self):
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
            self.last = len(msg)

    def __del__(self):
        if self.last:
            self._message("")
            print >> self.out, "\r",
            self.out.flush()

    def __init__(self, f = sys.stdout):
        self.last = 0
        self.out = f
