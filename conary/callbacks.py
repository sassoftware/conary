#!/usr/bin/python 
#
# Copyright (c) 2005 rPath, Inc.
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

import sys

class Callback:
    pass

class ChangesetCallback:

    def requestingChangeSet(self):
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

    def __init__(self):
        self.rate = 0

class CookCallback(ChangesetCallback):

    def setCurrentRate(self, rate):
        pass

    def buildingChangeset(self):
        pass

    def sendingChangeset(self, sent, total):
        pass

    def findingTroves(self, num):
        pass

    def gettingTroveDefinitions(self, num):
        pass

    def groupResolvingDependencies(self):
        pass

    def groupCheckingDependencies(self):
        pass

    def groupCheckingPaths(self, current):
        pass

    def groupDeterminingPathConflicts(self, total):
        pass

    def done(self):
        pass


class UpdateCallback(ChangesetCallback):

    def preparingChangeSet(self):
        pass

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
        return (self.abortEvent and self.abortEvent.isSet())

    def setAbortEvent(self, event = None):
        self.abortEvent = event

    def __init__(self):
        self.abortEvent = None

class SignatureCallback:

    def signTrove(self, got, need):
        pass

    def __init__(self):
        pass

class FetchCallback:

    def setRate(self, rate):
        self.rate = rate

    def fetch(self, got, need):
        pass

    def __init__(self):
        self.rate = 0

class KeyCacheCallback:

    def getPublicKey(self, keyId, serverName):
        return False

    def __init__(self, repositoryMap = None, pubRing = ''):
        self.repositoryMap = repositoryMap
        self.pubRing = pubRing

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
