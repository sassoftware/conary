#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

# implements the SRS system repository

import changeset
import datastore
import dbhash
import fcntl
import files
import os
import package
import util
import versioned

class Repository:

    # returns (pkgList, fileList, fileMap, oldFileList, oldPackageList) tuple, 
    # which forms a todo for how to apply the change set; that tuple is used to
    # either commit the change set or to reroot the change set against
    # another target
    def _buildChangeSetJob(self, cs):
	pkgList = []
	fileMap = {}
	fileList = []
	oldFileList = []
	oldPackageList = []

	# build todo set
	for csPkg in cs.getPackageList():
	    newVersion = csPkg.getNewVersion()
	    old = csPkg.getOldVersion()

	    if self.hasPackage(csPkg.getName()):
		pkgSet = self._getPackageSet(csPkg.getName())

		if pkgSet.hasVersion(newVersion):
		    raise KeyError, "version %s for %s exists" % \
			    (newVersion.asString(), csPkg.getName())
	    else:
		pkgSet = None

	    if old:
		oldPackageList.append((csPkg.getName(), old))
		newPkg = pkgSet.getVersion(old)
		newPkg.changeVersion(newVersion)
	    else:
		newPkg = package.Package(csPkg.name, newVersion)

	    newFileMap = newPkg.applyChangeSet(csPkg)
	    pkgList.append((csPkg.getName(), newPkg, newVersion))
	    fileMap.update(newFileMap)

	    if old:
		oldPackage = self.getPackageVersion(csPkg.getName(), old)
		for fileId in csPkg.getOldFileList():
		    version = oldPackage.getFile(fileId)[1]
		    oldFileList.append((fileId, version))

		for (fileId, path, version) in csPkg.getChangedFileList():
		    version = oldPackage.getFile(fileId)[1]
		    oldFileList.append((fileId, version))

	# Create the file objects we'll need for the commit. This handles
	# files which were added and files which have changed
	for (fileId, (oldVer, newVer, infoLine)) in cs.getFileList():
	    if oldVer:
		fileDB = self._getFileDB(fileId)
		file = fileDB.getVersion(oldVer)
		file.applyChange(infoLine)
		del fileDB
	    else:
		file = files.FileFromInfoLine(infoLine, fileId)

	    assert(newVer.equal(fileMap[fileId][1]))
	    fileList.append((fileId, newVer, file))

	return (pkgList, fileList, fileMap, oldFileList, oldPackageList)

    def commitChangeSet(self, sourcePathTemplate, cs, eraseOld = 0):
	(pkgList, fileList, fileMap, oldFileList, oldPackageList) = \
	    self._buildChangeSetJob(cs)

	# we can't erase the oldVersion for abstract change sets
	assert(not(cs.isAbstract() and eraseOld))
	
	# commit changes
	pkgsDone = []
	filesDone = []
	filesToArchive = {}
	try:
	    for (pkgName, newPkg, newVersion) in pkgList:
		pkgSet = self._getPackageSet(pkgName)
		pkgSet.addVersion(newVersion, newPkg)
		pkgsDone.append((pkgSet, newVersion))

	    for (fileId, fileVersion, file) in fileList:
		infoFile = self._getFileDB(fileId)
		pathInPkg = fileMap[fileId][0]
		pkgName = fileMap[fileId][2]

		# this version may already exist, abstract change sets
		# include redundant files quite often
		if not infoFile.hasVersion(fileVersion):
		    infoFile.addVersion(fileVersion, file)
		    infoFile.close()
		    filesDone.append(fileId)
		    filesToArchive[pathInPkg] = ((file, pathInPkg, pkgName))

	    # sort paths and store in order (to make sure that directories
	    # are stored before the files that reside in them in the case of
	    # restore to a local file system
	    pathsToArchive = filesToArchive.keys()
	    pathsToArchive.sort()
	    for pathInPkg in pathsToArchive:
		(file, path, pkgName) = filesToArchive[pathInPkg]
		if isinstance(file, files.SourceFile):
		    basePkgName = pkgName.split(':')[-2]
		    d = { 'pkgname' : basePkgName }
		    path = (sourcePathTemplate) % d + "/" + path

		self.storeFileFromChangeset(cs, file, path)
	except:
	    # something went wrong; try to unwind our commits
	    for fileId in filesDone:
		infoFile = self._getFileDB(fileId)
		(path, fileVersion) = fileMap[fileId][0:2]
		infoFile.eraseVersion(fileVersion)
		infoFile.close()

	    for (pkgSet, newVersion) in pkgsDone:
		pkgSet.eraseVersion(newVersion)
		pkgSet.close()

	    raise 

	# at this point the new version is in the repository, and we
	# can't undo that anymore. if erasing the old version fails, we
	# need to just commit the inverse change set; fortunately erasing
	# rarely fails
	for (fileId, version) in oldFileList:
	    filesDB = self._getFileDB(fileId)
	    filesDB.eraseVersion(version)
	    filesDB.close()

	for (pkgName, pkgVersion) in oldPackageList:
	    pkgSet = self._getPackageSet(pkgName)
	    pkgSet.eraseVersion(pkgVersion)
	    pkgSet.close()

    # packageList is a list of (pkgName, oldVersion, newVersion) tuples
    def createChangeSet(self, packageList):
	cs = changeset.ChangeSetFromRepository(self)

	for (packageName, oldVersion, newVersion) in packageList:
	    pkgSet = self._getPackageSet(packageName)

	    new = pkgSet.getVersion(newVersion)
	 
	    if oldVersion:
		old = pkgSet.getVersion(oldVersion)
	    else:
		old = None

	    (pkgChgSet, filesNeeded) = new.diff(old, oldVersion, newVersion)
	    cs.addPackage(pkgChgSet)

	    for (fileId, oldVersion, newVersion) in filesNeeded:
		filedb = self._getFileDB(fileId)

		oldFile = None
		if oldVersion:
		    oldFile = filedb.getVersion(oldVersion)
		newFile = filedb.getVersion(newVersion)

		(filecs, hash) = changeset.fileChangeSet(fileId, oldFile, 
							 newFile)

		cs.addFile(fileId, oldVersion, newVersion, filecs)
		if hash: cs.addFileContents(hash)

	return cs

    # takes an abstract change set and creates a differential change set 
    # against a branch of the repository
    def rootChangeSet(self, absSet, branch):
	assert(absSet.isAbstract())

	(pkgList, fileList, fileMap, oldFileList, oldPackageList) = \
	    self._buildChangeSetJob(absSet)

	cs = changeset.ChangeSetFromAbstractChangeSet(absSet)

	# we want to look up file objects by fileId
	idToFile = {}
	for (fileId, fileVersion, file) in fileList:
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

    def _getPackageSet(self, name):
	return _PackageSet(self.pkgDB, name)

    def _getFileDB(self, fileId):
	return _FileDB(self.fileDB, fileId)

    def pullFileContents(self, fileId, targetFile):
	srcFile = self.contentsStore.openFile(fileId)
	targetFile.write(srcFile.read())
	srcFile.close()

    def pullFileContentsObject(self, fileId):
	return self.contentsStore.openFile(fileId)

    def newFileContents(self, fileId, srcFile):
	targetFile = self.contentsStore.newFile(fileId)
	targetFile.write(srcFile.read())
	targetFile.close()

    def hasFileContents(self, fileId):
	return self.contentsStore.hasFile(fileId)

    def getPackageList(self, groupName = ""):
	if self.pkgDB.hasFile(groupName):
	    return [ groupName ]

	allPackages = self.pkgDB.fileList()
	list = []
	groupName = groupName + ":"

	for pkgName in allPackages:
	    if pkgName.startswith(groupName):
		list.append(pkgName)

	list.sort()

	return list

    def hasPackage(self, pkg):
	return self.pkgDB.hasFile(pkg)

    def hasPackageVersion(self, pkgName, version):
	return self._getPackageSet(pkgName).hasVersion(version)

    def pkgLatestVersion(self, pkgName, branch):
	return self._getPackageSet(pkgName).getLatestVersion(branch)

    def getLatestPackage(self, pkgName, branch):
	return self._getPackageSet(pkgName).getLatestPackage(branch)

    def getPackageVersion(self, pkgName, version):
	return self._getPackageSet(pkgName).getVersion(version)

    def getPackageVersionList(self, pkgName):
	return self._getPackageSet(pkgName).versionList()

    def fileLatestVersion(self, fileId, branch):
	fileDB = self._getFileDB(fileId)
	return fileDB.getLatestVersion(branch)
	
    def getFileVersion(self, fileId, version):
	fileDB = self._getFileDB(fileId)
	return fileDB.getVersion(version)

    def storeFileFromChangeset(self, chgSet, file, pathToFile):
	if isinstance(file, files.RegularFile):
	    f = chgSet.getFileContents(file.sha1())
	    file.archive(self, f)
	    f.close()

    def open(self, mode):
	if self.pkgDB:
	    self.close()

	self.lockfd = os.open(self.top + "/lock", os.O_CREAT | os.O_RDWR)

	if (mode == "r"):
	    fcntl.lockf(self.lockfd, fcntl.LOCK_SH)
	else:
	    fcntl.lockf(self.lockfd, fcntl.LOCK_EX)

	self.pkgDB = versioned.FileIndexedDatabase(self.top + "/pkgs.db")
	self.fileDB = versioned.Database(self.top + "/files.db")

	self.mode = mode

    def close(self):
	if self.pkgDB:
	    self.pkgDB = None
	    self.fileDB = None
	    os.close(self.lockfd)

    def __del__(self):
	self.close()

    def __init__(self, path, mode = "c"):
	self.top = path
	self.pkgDB = None
	
	self.contentsDB = self.top + "/contents"
	util.mkdirChain(self.contentsDB)

	self.contentsStore = datastore.DataStore(self.contentsDB)

	self.open(mode)

# This is a repository which includes a mapping from a sha1 to a path
class Database(Repository):

    def storeFileFromChangeset(self, chgSet, file, pathToFile):
	file.restore(chgSet, self.root + pathToFile)
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
	Repository.close(self)

    def open(self, mode):
	Repository.open(self, mode)
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
	Repository.__init__(self, fullPath, mode)

# this is a set of all of the versions of a single packages 
class _PackageSet:
    def getVersion(self, version):
	f1 = self.f.getVersion(version)
	p = package.PackageFromFile(self.name, f1, version)
	f1.close()
	return p

    def hasVersion(self, version):
	return self.f.hasVersion(version)

    def eraseVersion(self, version):
	self.f.eraseVersion(version)

    def addVersion(self, version, package):
	self.f.addVersion(version, package.formatString())

    def versionList(self):
	return self.f.versionList()

    def getLatestPackage(self, branch):
	return self.getVersion(self.f.findLatestVersion(branch))

    def getLatestVersion(self, branch):
	return self.f.findLatestVersion(branch)

    def close(self):
	self.f = None

    def __del__(self):
	self.f = None

    def __init__(self, db, name):
	self.name = name
	self.f = db.openFile(name)

class _FileDB:

    def getLatestVersion(self, branch):
	return self.f.findLatestVersion(branch)

    def addVersion(self, version, file):
	if self.f.hasVersion(version):
	    raise KeyError, "duplicate version for database"
	else:
	    if file.id() != self.fileId:
		raise KeyError, "file id mismatch for file database"
	
	self.f.addVersion(version, "%s\n" % file.infoLine())

    def getVersion(self, version):
	f1 = self.f.getVersion(version)
	file = files.FileFromInfoLine(f1.read(), self.fileId)
	f1.close()
	return file

    def hasVersion(self, version):
	return self.f.hasVersion(version)

    def eraseVersion(self, version):
	self.f.eraseVersion(version)

    def close(self):
	self.f = None

    def __del__(self):
	self.close()

    def __init__(self, db, fileId):
	self.f = db.openFile(fileId)
	self.fileId = fileId

# Exception classes

class RepositoryError(Exception):

    """Base class for exceptions from the system repository"""
    pass

class RollbackError(RepositoryError):

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
