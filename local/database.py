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

# Many comments in this file only make sense if you consider the case
# where something is being updated from version A to B, and the local
# branches are called A.local and B.local; this terminology is used throughout
# the comments in this file

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
	head = self.fileLatestVersion(fileId, version.branch())
	if not version.isLocal() or (head and not version.equal(head)):
	    (file, contents) = repository.LocalRepository.getFileVersion(
					self, fileId, version, withContents = 1)

	    if withContents:
		return (file, contents)
	    return file

	assert(path)

	# we can't get the file flags or know if it's a source file by looking
	# in the filesystem; we don't let the user change those for the local
	# branch either
	parentV = version.parent()
	file = repository.LocalRepository.getFileVersion(self, fileId, parentV)
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

	# create the change set from A->A.local; this forms part of the
	# rollback
	list = []
	for pkg in cs.getNewPackageList():
	    name = pkg.getName()
	    old = pkg.getOldVersion()
	    if self.hasPackage(name) and old:
		branch = old.fork(versions.LocalBranch(), sameVerRel = 0)
		new = self.pkgLatestVersion(name, branch)
		list.append((name, old, new, 0))

	localChanges = self.createChangeSet(list)

	#if makeRollback:
	#    inverse = cs.invert(self, availableFiles = 1)
	#    self.addRollback(inverse)

	# Build and commit A->B
	job = repository.ChangeSetJob(self, cs)
	undo = repository.ChangeSetUndo(self)

	try:
	    job.commit(undo)
	except:
	    # this won't work it things got too far, but it won't hurt
	    # anything either
	    undo.undo()
	    raise

	# Create B->B.local. This starts by retargeting A->A.local at
	# B (which exists in the database thanks to the commit above),
	# and is filled out by ensuring that every package has a branch
	# in the local tree
	try:
	    dbUndo = DatabaseChangeSetUndo(self)
	    dbJob = DatabaseChangeSetJob(self, localChanges, job)
	    dbJob.commit(dbUndo, self.root)
	except:
	    dbUndo.undo()
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
	paths = self.paths.keys()
	paths.sort()
	for path in paths:
	    (newFile, csWithContents) = self.paths[path]
	    fileObj = newFile.file()
	    fileObj.restore(csWithContents, root + path, 
			    newFile.restoreContents())

	# remove paths which are no longer valid
	for (path, file) in self.staleFileList():
	    file.remove(root + path)

	# time to remove files from the repository
	for (fileId, fileVersion, fileObj) in self.oldFileList():
	    if fileObj.isConfig():
		self.repos.removeFileContents(fileObj.sha1())

    # remove the specified file and it's local branch
    def removeFile(self, origJob, fileId, version, path):
	# we need this object in case of an undo
	fileObj = self.repos.getFileVersion(fileId, version)

	self.oldFile(fileId, version, fileObj)

	# if we're just going to create this again, don't bother
	# removing it
	if not origJob.containsFilePath(path):
	    self.addStaleFile(path, fileObj)

    def __init__(self, repos, localCs, origJob):
	# list of packages which need to be removed
	self.oldPackages = []
	self.oldFiles = []
	self.staleFiles = []

	assert(not origJob.cs.isAbstract())

	# Make sure every package in the original change set has a local
	# branch with the right versions. The target of things in localCS
	# needs to be the new local branch, and the source is the same
	# as the target of the original CS (this is creating B->B.local
	# from A->A.local when origJob is A->B; A, A.local, and B are all
	# available)
	for Bpkg in origJob.newPackageList():
	    name = Bpkg.getName()
	    Bver = Bpkg.getVersion()
	    Bloc = Bver.fork(versions.LocalBranch(), sameVerRel = 1)
	    if not localCs.hasNewPackage(name):
		BlocPkg = Bpkg.copy()
		BlocPkg.changeVersion(Bloc)
		BlocCs = BlocPkg.diff(Bpkg)[0]
	    else:
		BlocCs = localCs.getNewPackage(name)
		BlocCs.changeOldVersion(Bver)
		BlocCs.changeNewVersion(Bloc)
	    # this overwrites the package if it already exists in the
	    # change set
	    localCs.newPackage(BlocCs)

	repository.ChangeSetJob.__init__(self, repos, localCs)

	# walk through every package we're about to commit, and update
	# the file list to reflect that the files are on the local branch,
	# unless they are already on a local branch from migrating into
	# this changeset from localCs
	for branchPkg in self.newPackageList():
	    for (fileId, path, version) in branchPkg.fileList():
		if not version.isLocal():
		    ver = version.fork(versions.LocalBranch(), sameVerRel = 1)
		    branchPkg.updateFile(fileId, path, ver)

	# get the list of files which need to be created; files which
	# are on the new jobs newFileList don't need to be created; they
	# are already in the filesystem (as members of A.local, and now
	# they'll be members of B.local as well)
	skipPaths = {}
	for f in self.newFileList():
	    skipPaths[f.path()] = 1

	self.paths = {}
	for f in origJob.newFileList():
	    if not skipPaths.has_key(f.path()):
		self.paths[f.path()] = (f, origJob.cs)

	# at this point, self is job which does all of the creation of
	# new bits. we need self to perform the removal of the old bits
	# as well

	# remove old versions of the packages which are being added; make sure
	# we get both the version being replaced and the local branch for that
	# version
	# 
	# while we're here, package change sets may mark some files as removed;
	# we need to remember to remove those files, and make the paths for
	# those files candidates for removal package change sets also know 
	# when file paths have changed, and those old paths are also candidates
	# for removal

	for csPkg in origJob.cs.getNewPackageList():
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
		self.removeFile(origJob, fileId, oldFileVersion, oldPath)

	    for (fileId, newPath, newVersion) in csPkg.getChangedFileList():
		if newPath:
		    # find the old path for this file
		    (oldPath, oldFileVersion) = pkg.getFile(fileId)
		    if not self.containsFilePath(oldPath):
			# the path has been orphaned
			fileObj = self.repos.getFileVersion(fileId, 
							    oldFileVersion)
			self.addStaleFile(oldPath, fileObj)

	# for each file which has changed, erase the old version of that
	# file from the repository
	for f in origJob.newFileList():
	    oldVersion = origJob.cs.getFileOldVersion(f.fileId())
	    if not oldVersion:
		# this is a new file; there is no old version to erase
		continue

	    self.oldFile(f.fileId(), oldVersion, 
			 repos.getFileVersion(f.fileId(), oldVersion))

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
