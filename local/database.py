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

	# this has an empty source path template, which is only used to
	# construct the eraseFiles list anyway
	(pkgList, fileList, fileMap, oldFileList, oldPackageList, 
	    eraseList, eraseFiles) = self._buildChangeSetJob("", absSet)

	# abstract change sets cannot have eraseLists
	assert(not eraseList)
	assert(not eraseFiles)

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
		# new package; the Package.diff() right after this never
		# sets the abstract flag, so the right thing happens
		old = None
	    else:
		old = self.getPackageVersion(pkgName, oldVersion)

	    (pkgChgSet, filesNeeded) = newPkg.diff(old)
	    cs.newPackage(pkgChgSet)

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

    def removeFile(self, file, pathToFile):
	file.remove(self.root + pathToFile)
	if isinstance(file, files.RegularFile):
	    key = file.sha1()
	    l = self.fileIdMap[key].split("\n")
	    if len(l) == 1:
		del self.fileIdMap[key]
	    else:
		del l[l.index(pathToFile)]
		self.fileIdMap[key] = "\n".join(l)

	    if file.isConfig():
		self.contentsStore.removeFile(file.sha1())

    def storeFileFromChangeset(self, chgSet, file, pathToFile, skipContents):
	file.restore(chgSet, self.root + pathToFile, skipContents)
	if isinstance(file, files.RegularFile):
	    key = file.sha1()
	    if self.fileIdMap.has_key(key):
		self.fileIdMap[key] += pathToFile
	    else:
		self.fileIdMap[key] = pathToFile

	    # archive config files; we might want them later
	    if file.isConfig():
		f = chgSet.getFileContents(file.sha1())
		file.archive(self, f)
		f.close()

    def pullFileContentsObject(self, fileId):
	# just pick the first path; we don't care which one we use
	path = self.fileIdMap[fileId].split('\n')[0]
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
		raise RollbackDoesNotExist(name)

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

class RollbackDoesNotExist(RollbackError):

    """Raised when the system tries to access a rollback which isn't in
       the database"""

    def __repr__(self):
	return "rollback %s does not exist" % self.name

    def __str__(self):
	return repr(self)

    def __init__(self, rollbackName):
	"""Create new new RollbackOrderError
	@param rollbackName: string represeting the name of the rollback
	which does not exist"""
	self.name = rollbackName
