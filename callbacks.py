import sys

class Callback:
    pass

class ChangesetCallback:

    def downloadingChangeSet(self, got, need):
        pass

    def requestingChangeSet(self):
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


class LineOutput:

    def _message(self, msg):
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
            print "\r",

    def __init__(self, f = sys.stdout):
        self.last = 0
        self.out = f
