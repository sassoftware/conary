import changeset
import dbhash
import files
import os
import repository

class Database(repository.Repository):

    def storeFileFromChangeset(self, chgSet, file, pathToFile, skipContents):
	file.restore(chgSet, self.root + pathToFile, skipContents)
	if isinstance(file, files.RegularFile):
	    self.fileIdMap[file.sha1()] = pathToFile

    def pullFileContents(self, fileId, targetFile):
	srcFile = open(self.root + self.fileIdMap[fileId], "r")
	targetFile.write(srcFile.read())
	srcFile.close()

    def pullFileContentsObject(self, fileId):
	return open(self.root + self.fileIdMap[fileId], "r")

    def close(self):
	if self.fileIdMap:
	    self.fileIdMap = None
	repository.Repository.close(self)

    def open(self, mode):
	repository.Repository.open(self, mode)
	self.fileIdMap = dbhash.open(self.top + "/fileid.db", mode)
	self.rollbackCache = self.top + "/rollbacks"
	self.rollbackStatus = self.rollbackCache + "/status"
	if not os.path.exists(self.rollbackCache):
	    os.mkdir(self.rollbackCache)
	if not os.path.exists(self.rollbackStatus):
	    self.firstRollback = 0
	    self.lastRollback = -1
	    self.writeRollbackStatus()
	else:
	    self.readRollbackStatus()

    def addRollback(self, changeset):
	fn = self.rollbackCache + ("/r.%d" % (self.lastRollback + 1))
	changeset.writeToFile(fn)

	self.lastRollback += 1
	self.writeRollbackStatus()

    # name looks like "r.%d"
    def removeRollback(self, name):
	rollback = int(name[2:])
	os.unlink(self.rollbackCache + "/" + name)
	if rollback == self.lastRollback:
	    self.lastRollback -= 1
	    self.writeRollbackStatus()

    def writeRollbackStatus(self):
	newStatus = self.rollbackCache + ".new"

	f = open(newStatus, "w")
	f.write("%s %d\n" % (self.firstRollback, self.lastRollback))
	f.close()

	os.rename(newStatus, self.rollbackStatus)

    def getRollbackList(self):
	list = []
	for i in range(self.firstRollback, self.lastRollback + 1):
	    list.append("r.%d" % i)

	return list

    def readRollbackStatus(self):
	f = open(self.rollbackStatus)
	(first, last) = f.read()[:-1].split()
	self.firstRollback = int(first)
	self.lastRollback = int(last)
	f.close()

    def hasRollback(self, name):
	try:
	    num = int(name[2:])
	except ValueError:
	    return False

	if (num >= self.firstRollback and num <= self.lastRollback):
	    return True
	
	return False

    def getRollback(self, name):
	if not self.hasRollback(name): return None

	return changeset.ChangeSetFromFile(self.rollbackCache + "/" + name)

    def applyRollbackList(self, sourcepath, names):
	last = self.lastRollback
	for name in names:
	    if not self.hasRollback(name):
		raise KeyError(name)

	    num = int(name[2:])
	    if num != last:
		raise RollbackOrderError(name)
	    last -= 1

	for name in names:
	    cs = self.getRollback(name)
	    self.commitChangeSet(sourcepath, cs, eraseOld = 1)
	    self.removeRollback(name)

    def __init__(self, root, path, mode = "c"):
	self.root = root
	fullPath = root + "/" + path
	repository.Repository.__init__(self, fullPath, mode)

