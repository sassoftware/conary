
class Callback:
    def downloadingChangeSet(self, got, need):
        pass

    def requestingChangeSet(self):
        pass

class UpdateCallback(Callback):

    def preparingChangeSet(self):
        pass

    def resolvingDependencies(self):
        pass

    def creatingRollback(self):
        pass

    def preparingUpdate(self):
        pass

    def restoreFiles(self, size, totalSize):
        pass

    def removeFiles(self, fileNum, total):
        pass

    def runningPreTagHandlers(self):
        pass

    def runningPostTagHandlers(self):
        pass
