#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import changeset
import dbhash
import files
import os
import repository

class Database(repository.LocalRepository):

    # takes an abstract change set and creates a differential change set 
    # against a branch of the repository
    def rootChangeSet(self, absSet, branch):
	assert(absSet.isAbstract())

	(pkgList, fileList, fileMap, oldFileList, oldPackageList) = \
	    self._buildChangeSetJob(absSet)

	cs = changeset.ChangeSetFromAbstractChangeSet(absSet)

	# we want to look up file objects by fileId
	idToFile = {}
	for (fileId, fileVersion, file, saveContents) in fileList:
	    idToFile[fileId] = (fileVersion, file)

	for (pkgName, newPkg, newVersion) in pkgList:
	    # FIXME
	    #
	    # this shouldn't be against branch, it should be against
	    # the version of the package already installed on the
	    # system. unfortunately we can't represent that yet. 
	    oldVersion = self.pkgLatestVersion(pkgName, branch)
	    if not oldVersion:
		# FIXME
		return None
	    else:
		old = self.getPackageVersion(pkgName, oldVersion)

	    (pkgChgSet, filesNeeded) = newPkg.diff(old, oldVersion, newVersion)
	    cs.addPackage(pkgChgSet)

	    for (fileId, oldVersion, newVersion) in filesNeeded:
		filedb = self._getFileDB(fileId)

		(ver, newFile) = idToFile[fileId]
		assert(ver.equal(newVersion))

		oldFile = None
		if oldVersion:
		    oldFile = filedb.getVersion(oldVersion)

		(filecs, hash) = changeset.fileChangeSet(fileId, oldFile, 
							 newFile)

		cs.addFile(fileId, oldVersion, newVersion, filecs)
		if hash: cs.addFileContents(hash)

	return cs

    def storeFileFromChangeset(self, chgSet, file, pathToFile, skipContents):
	file.restore(chgSet, self.root + pathToFile, skipContents)
	if isinstance(file, files.RegularFile):
	    self.fileIdMap[file.sha1()] = pathToFile

	    # archive config files; we might want them later
	    if file.isConfig():
		f = chgSet.getFileContents(file.sha1())
		file.archive(self, f)
		f.close()

    def pullFileContents(self, fileId, targetFile):
	srcFile = open(self.root + self.fileIdMap[fileId], "r")
	targetFile.write(srcFile.read())
	srcFile.close()

    def pullFileContentsObject(self, fileId):
	return open(self.root + self.fileIdMap[fileId], "r")

    def close(self):
	if self.fileIdMap is not None:
            self.fileIdMap.close()
	    self.fileIdMap = None
	repository.LocalRepository.close(self)

    def open(self, mode):
	repository.LocalRepository.open(self, mode)
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

    def __init__(self, root, path, mode = "r"):
	self.root = root
	fullPath = root + "/" + path
	repository.LocalRepository.__init__(self, fullPath, mode)

# Exception classes

class RollbackError(repository.RepositoryError):

    """Base class for exceptions related to applying rollbacks"""

class RollbackOrderError(RollbackError):

    """Raised when an attempt is made to apply rollbacks in the
       wrong order"""

    def __repr__(self):
	return "rollback %s can not be applied out of order" % self.name

    def __str__(self):
	return repr(self)

    def __init__(self, rollbackName):
	"""Create new new RollbackOrderError
	@param rollbackName: string represeting the name of the rollback
	which was trying to be applied out of order"""
	self.name = rollbackName
