#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import changeset
import dbhash
import files
import os
import repository
import versions

# Every item which is inserted into the database really goes in twice;
# once as is, and then a LocalBranch is immediately created which reflects
# what's in the file system. The "as is" copy doesn't include data for
# any file except configuration files (which means while they can be used
# as the source for a change set, they can't be used as the target)
#
# The LocalBranch for each item in the database has normal header information,
# but the on-disk files are the real source of information on the package,
# including file types, hashes, etc

class Database(repository.LocalRepository):

    # If the request is for the head element of the local branch, we need
    # to be a bit careful with the file list. It currently contains the
    # branched version of each file, but we want to contain the non-branch
    # version if the branch (that is, the local filesystem) hasn't changed
    def getPackageVersion(self, name, version):
	pkg = repository.LocalRepository.getPackageVersion(self, name, version)
	if not version.isLocal(): return pkg
	head = self.pkgLatestVersion(name, version.branch())
	if not version.equal(head): return pkg

	for (fileId, path, fileVersion) in pkg.fileList():
	    parentVersion = fileVersion.parent()

	    dbFile = self.getFileVersion(fileId, parentVersion)
	    localFile = self.getFileVersion(fileId, fileVersion, path = path)

	    if dbFile.same(localFile):
		pkg.updateFile(fileId, path, fileVersion.parent())

	return pkg

    # like getPackageVersion, we need to look to the filesystem for file
    # versions which are at the head of our local branch; this means that
    # we require the path to get information on some versions
    def getFileVersion(self, fileId, version, path = None, withContents = 0):
	(file, contents) = repository.LocalRepository.getFileVersion(
				    self, fileId, version, withContents = 1)
	head = self.fileLatestVersion(fileId, version.branch())

	if not version.isLocal() or not version.equal(head):
	    if withContents:
		return (file, contents)
	    return file

	assert(path)

	if isinstance(file, files.SourceFile):
	    localFile = files.FileFromFilesystem(self.root + path, fileId,
						 type = "src")
	else:
	    localFile = files.FileFromFilesystem(self.root + path, fileId)

	localFile.flags(file.flags())

	if withContents:
	    if isinstance(file, files.RegularFile): 
		cont = repository.FileContentsFromFilesystem(self.root + path)
	    else:
		cont = None

	    return (localFile, cont)

	return localFile

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

    def commitChangeSet(self, cs, makeRollback = 1, sourcePath = "/"):
	assert(not cs.isAbstract())

	map = ( ( None, sourcePath + "/" ), )
	cs.remapPaths(map)

	# create the change set from the version originally installed
	# to the one installed on this system
	list = []
	for pkg in cs.getNewPackageList():
	    name = pkg.getName()
	    old = pkg.getOldVersion()
	    if self.hasPackage(name) and old:
		branch = old.fork(versions.LocalBranch(), sameVerRel = 0)
		new = self.pkgLatestVersion(name, branch)
		list.append((name, old, new, 0))

	localChanges = self.createChangeSet(list)

	job = DatabaseChangeSetJob(self, cs)
	undo = DatabaseChangeSetUndo(self)

	if makeRollback:
	    inverse = cs.invert(self, availableFiles = 1)
	    self.addRollback(inverse)

	try:
	    job.commit(undo, self.root)
	except:
	    # this won't work it things got too far, but it won't hurt
	    # anything either
	    undo.undo()
	    raise

    # this is called when a Repository wants to store a file; we never
    # want to do this; we copy files onto the filesystem after we've
    # created the LocalBranch
    def storeFileFromChangeset(self, chgSet, file, restoreContents):
	if file.isConfig():
	    return repository.LocalRepository.storeFileFromChangeset(self, 
				    chgSet, file, restoreContents)

    def close(self):
	repository.LocalRepository.close(self)

    def open(self, mode):
	repository.LocalRepository.open(self, mode)
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

	return changeset.ChangeSetFromFile(self.rollbackCache + "/" + name,
					   justContentsForConfig = 1)

    def applyRollbackList(self, names):
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
	    self.commitChangeSet(cs, makeRollback = 0)
	    self.removeRollback(name)

    def __init__(self, root, path, mode = "r"):
	self.root = root
	fullPath = root + "/" + path
	repository.LocalRepository.__init__(self, fullPath, mode)

# This builds a job which applies both a change set and the local changes
# which are needed.
class DatabaseChangeSetJob(repository.ChangeSetJob):

    def createChangeSet(self, packageList):
	raise NotImplemented

    def oldPackage(self, pkg):
	self.oldPackages.append(pkg)

    def oldPackageList(self):
	return self.oldPackages

    def oldFile(self, fileId, fileVersion, fileObj):
	self.oldFiles.append((fileId, fileVersion, fileObj))

    def oldFileList(self):
	return self.oldFiles

    def addStaleFile(self, path, fileObj):
	self.staleFiles.append((path, fileObj))

    def staleFileList(self):
	self.staleFiles.sort()
	return self.staleFiles

    def commit(self, undo, root):
	repository.ChangeSetJob.commit(self, undo)

	for pkg in self.oldPackageList():
	    self.repos.erasePackageVersion(pkg.getName(), pkg.getVersion())
	    undo.removedPackage(pkg)

	for (fileId, fileVersion, fileObj) in self.oldFileList():
	    self.repos.eraseFileVersion(fileId, fileVersion)
	    undo.removedFile(fileId, fileVersion, fileObj)

	# the undo object won't work after this point, but a rollback
	# should work fine (even if some extraneous errors get reported)
	undo.reset()

	# write files to the filesystem, finally
	for (path, newFile) in self.pathList:
	    fileObj = newFile.file()
	    fileObj.restore(self.cs, root + path, newFile.restoreContents())

	# remove paths which are no longer valid
	for (path, file) in self.staleFileList():
	    file.remove(root + path)

	# time to remove files from the repository
	for (fileId, fileVersion, fileObj) in self.oldFileList():
	    if fileObj.isConfig():
		self.repos.removeFileContents(fileObj.sha1())

    # remove the specified file and it's local branch
    def removeFile(self, fileId, version, path):
	# we need this object in case of an undo
	fileObj = self.repos.getFileVersion(fileId, version)

	branch = version.fork(versions.LocalBranch(), sameVerRel = 1)

	self.oldFile(fileId, version, fileObj)
	self.oldFile(fileId, branch, fileObj)

	if not self.containsFilePath(path):
	    self.addStaleFile(path, fileObj)

    def __init__(self, repos, cs):
	repository.ChangeSetJob.__init__(self, repos, cs)

	# list of packages which need to be removed
	self.oldPackages = []
	self.oldFiles = []
	self.staleFiles = []

	# Make local branches for each package and let them get committed 
	# with the rest of this change set; note that the local branch
	# of the package uses the local branch of the files
	#
	# iterate over a copy of this list as the real list keeps
	# growing through the loop
	list = self.newPackageList()[:]
	for newPkg in list:
	    branchPkg = newPkg.copy()
	    ver = branchPkg.getVersion().fork(versions.LocalBranch(), 
					      sameVerRel = 1)
	    branchPkg.changeVersion(ver)

	    for (fileId, path, version) in branchPkg.fileList():
		ver = version.fork(versions.LocalBranch(), sameVerRel = 1)
		branchPkg.updateFile(fileId, path, ver)

	    self.addPackage(branchPkg)

	# remove old versions of the packages which are being added; make sure
	# we get both the version being replaced and the local branch for that
	# version
	# 
	# while we're here, package change sets may mark some files as removed;
	# we need to remember to remove those files and their local branches.
	# package change sets also know when file paths have changed, and the
	# old paths are candidates for removal (and should be removed unless
	# something else in this change set caused those paths to be owned
	# again)

	for csPkg in cs.getNewPackageList():
	    name = csPkg.getName()
	    oldVersion = csPkg.getOldVersion()

	    if not oldVersion:
		# we know this isn't an abstract change set (since this
		# class can't handle abstract change sets, and asserts
		# the away at the top of __init__() ), so this must be
		# a new package. no need to erase any old stuff then!
		continue

	    oldBranch = oldVersion.fork(versions.LocalBranch(), sameVerRel = 1)

	    assert(repos.hasPackageVersion(name, oldVersion))
	    assert(repos.hasPackageVersion(name, oldBranch))

	    self.oldPackage(repos.getPackageVersion(name, oldVersion))
	    self.oldPackage(repos.getPackageVersion(name, oldBranch))

	    pkg = repos.getPackageVersion(name, oldVersion)

	    for fileId in csPkg.getOldFileList():
		(oldPath, oldFileVersion) = pkg.getFile(fileId)
		self.removeFile(fileId, oldFileVersion, oldPath)

	    for (fileId, newPath, newVersion) in csPkg.getChangedFileList():
		if newPath:
		    # find the old path for this file
		    (oldPath, oldFileVersion) = pkg.getFile(fileId)
		    if not self.containsFilePath(oldPath):
			# the path has been orphaned
			fileObj = self.repos.getFileVersion(fileId, 
							    oldFileVersion)
			self.addStaleFile(oldPath, fileObj)

	# for each file we are going to create, create the file as
	# well as the local branch; while we're at it erase the old
	# version of that file, and the old branch
	# 
	# also build up a list of file paths so we can sort it to write
	# files onto the filesystem in the right order later on
	l = self.newFileList()[:]
	self.pathList = []
	for f in l:
	    self.pathList.append((f.path(), f))

	    newFile = f.copy()
	    ver = newFile.version().fork(versions.LocalBranch(), sameVerRel = 1)
	    newFile.changeVersion(ver)
	    self.addFile(newFile)

	    oldVersion = cs.getFileOldVersion(f.fileId())
	    if not oldVersion:
		# this is a new file; there is no old version to erase
		continue

	    oldBranch = oldVersion.fork(versions.LocalBranch(), sameVerRel = 1)

	    self.oldFile(f.fileId(), oldVersion, 
			 repos.getFileVersion(f.fileId(), oldVersion))
	    self.oldFile(f.fileId(), oldBranch,
			 repos.getFileVersion(f.fileId(), oldBranch))

	self.pathList.sort()

class DatabaseChangeSetUndo(repository.ChangeSetUndo):

    def undo(self):
	for pkg in self.removedPackages:
	    self.repos.addPackage(pkg)

	for (fileId, fileVersion, fileObj) in self.removedFiles:
	    self.repos.addFileVersion(fileId, fileVersion, fileObj)

	repository.ChangeSetUndo.undo(self)

    def removedPackage(self, pkg):
	self.removedPackages.append(pkg)

    def removedFile(self, fileId, fileVersion, fileObj):
	self.removedFiles.append((fileId, fileVersion, fileObj))

    def reset(self):
	repository.ChangeSetUndo.reset(self)
	self.removedPackages = []
	self.removedFiles = []

    def __init__(self, repos):
	self.repos = repos
	self.reset()

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
