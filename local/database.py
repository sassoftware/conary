#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import changeset
import filecontents
import files
import log
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

    createBranches = 1

    # If the request is for the head element of the local branch, we need
    # to be a bit careful with the file list. It currently contains the
    # branched version of each file, but we want to contain the non-branch
    # version if the branch (that is, the local filesystem) hasn't changed
    def getPackageVersion(self, name, version):
	pkg = repository.LocalRepository.getPackageVersion(self, name, version)
	if not version.isLocal(): return pkg

	for (fileId, path, fileVersion) in pkg.fileList():
	    parentVersion = fileVersion.parent()

	    dbFile = self.getFileVersion(fileId, parentVersion)
	    localFile = self.getFileVersion(fileId, fileVersion, path = path)

	    if dbFile.same(localFile):
		pkg.updateFile(fileId, path, fileVersion.parent())

	return pkg

    # like getPackageVersion, we need to look to the filesystem for file
    # versions which are on the local branch
    def getFileVersion(self, fileId, version, path = None, withContents = 0):
	if not version.isLocal():
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
						 type = "src",
						 possibleMatch = file)
	else:
	    localFile = files.FileFromFilesystem(self.root + path, fileId,
						 possibleMatch = file)

	localFile.flags(file.flags())

	if withContents:
	    if isinstance(file, files.RegularFile): 
		cont = filecontents.FromFilesystem(self.root + path)
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
	job = repository.ChangeSetJob(self, absSet)

	# abstract change sets cannot have eraseLists
	#assert(not eraseList)
	#assert(not eraseFiles)

	cs = changeset.ChangeSetFromAbstractChangeSet(absSet)

	for newPkg in job.newPackageList():
	    # FIXME
	    #
	    # this shouldn't be against branch, it should be against
	    # the version of the package already installed on the
	    # system. unfortunately we can't represent that yet. 
	    pkgName = newPkg.getName()
	    oldVersion = self.pkgLatestVersion(pkgName, branch)
	    if not oldVersion:
		# new package; the Package.diff() right after this never
		# sets the abstract flag, so the right thing happens
		old = None
	    else:
		old = self.getPackageVersion(pkgName, oldVersion)

	    # we ignore pkgsNeeded; it doesn't mean much in this case
	    (pkgChgSet, filesNeeded, pkgsNeeded) =	    \
		    newPkg.diff(old, abstract = 0)
	    cs.newPackage(pkgChgSet)

	    for (fileId, oldVersion, newVersion, newPath) in filesNeeded:
		fileObj = job.getFile(fileId)
		assert(newVersion.equal(fileObj.version()))
		
		oldFile = None
		if oldVersion:
		    oldFile = self.getFileVersion(fileId, oldVersion)

		(filecs, hash) = changeset.fileChangeSet(fileId, oldFile, 
							 fileObj.file())

		cs.addFile(fileId, oldVersion, newVersion, filecs)
		if hash: 
		    cs.addFileContents(fileId)

	assert(not cs.validate())

	return cs

    # local changes includes the A->A.local portion of a rollback; if it
    # doesn't exist we need to compute that and save a rollback for this
    # transaction
    def commitChangeSet(self, cs, localRollback = None):
	assert(not cs.isAbstract())

	for pkg in cs.getNewPackageList():
	    if pkg.name.endswith(":sources"): raise SourcePackageInstall

	if not localRollback:
	    # create the change set from A->A.local
	    list = []
	    for pkg in cs.getNewPackageList():
		name = pkg.getName()
		old = pkg.getOldVersion()
		if self.hasPackage(name) and old:
		    branch = old.fork(versions.LocalBranch(), sameVerRel = 0)
		    new = self.pkgLatestVersion(name, branch)
		    assert(new)
		    list.append((name, old, new, 0))

	    localChanges = self.createChangeSet(list)

	    # rollbacks have two pieces, B->A and A->A.local; applying
	    # both of them gets us back where we started
	    inverse = cs.makeRollback(self, configFiles = 1)
	    self.addRollback(inverse, localChanges)
	else:
	    localChanges = localRollback

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
	    dbUndo = repository.ChangeSetUndo(self)
	    if localRollback:
		dbJob = DatabaseChangeSetJob(self, localChanges, job,
					     retargetLocal = 0)
	    else:
		dbJob = DatabaseChangeSetJob(self, localChanges, job,
					     retargetLocal = 1)
	    dbJob.commit(dbUndo, self.root)
	except:
	    dbUndo.undo()
	    undo.undo()
	    raise

    # this is called when a Repository wants to store a file; we never
    # want to do this; we copy files onto the filesystem after we've
    # created the LocalBranch
    def storeFileFromContents(self, contents, file, restoreContents):
	if file.isConfig():
	    return repository.LocalRepository.storeFileFromContents(self, 
				contents, file, restoreContents)

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

    def addRollback(self, reposChangeset, localChangeset):
	rpFn = self.rollbackCache + ("/rb.r.%d" % (self.lastRollback + 1))
	reposChangeset.writeToFile(rpFn)

	localFn = self.rollbackCache + ("/rb.l.%d" % (self.lastRollback + 1))
	localChangeset.writeToFile(localFn)

	self.lastRollback += 1
	self.writeRollbackStatus()

    # name looks like "r.%d"
    def removeRollback(self, name):
	rollback = int(name[2:])
	os.unlink(self.rollbackCache + "/rb.r.%d" % rollback)
	os.unlink(self.rollbackCache + "/rb.l.%d" % rollback)
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

	num = int(name[2:])

	rc = []
	for ch in [ "r", "l" ]:
	    name = self.rollbackCache + "/" + "rb.%c.%d" % (ch, num)
	    rc.append(changeset.ChangeSetFromFile(name,
						  justContentsForConfig = 1))

	return rc

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
	    (repostCs, localCs) = self.getRollback(name)
	    self.commitChangeSet(repostCs, localRollback = localCs)
	    self.removeRollback(name)

    def __init__(self, root, path, mode = "r"):
	self.root = root
	fullPath = root + "/" + path
	repository.LocalRepository.__init__(self, fullPath, mode)

# This builds a job which applies both a change set and the local changes
# which are needed.
class DatabaseChangeSetJob(repository.ChangeSetJob):

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
	    newFile = self.paths[path]
	    fileObj = newFile.file()
	    cont = newFile.getContents()
	    fileObj.restore(cont, root + path, newFile.restoreContents())

	    if isinstance(cont, filecontents.WithFailedHunks):
		log.warning("nonmerged changes to %s saved in %s.conflicts",
			    path, path)
		cont.getHunks().write(root + path + ".conflicts", 
			    "current", "missing")

	# remove paths which are no longer valid
	list = self.staleFileList()
	list.sort()
	list.reverse()
	for (path, file) in list:
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

    # If retargetLocal is set, then localCs is for A->A.local whlie
    # origJob is A->B, so localCs needs to be changed to be B->B.local.
    # Otherwise, we're applying a rollback and origJob is B->A and
    # localCs is A->A.local, so it doesn't need retargeting.
    def __init__(self, repos, localCs, origJob, retargetLocal = 1):
	assert(not origJob.cs.isAbstract())

	if retargetLocal:
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

		#if localCs.hasNewPackage(name):
		#    BlocCs = localCs.getNewPackage(name)
		#    BlocCs.changeOldVersion(Bver)
		#    BlocCs.changeNewVersion(Bloc)
		#else:
		BlocPkg = Bpkg.copy()
		BlocPkg.changeVersion(Bloc)

		# look through the files in BlocPkg and point them at
		# the correct branch if the localCs creates a new version
		# of that file
		for (fileId, path, BfileVer) in BlocPkg.fileList():
		    if localCs.hasFile(fileId):
			BfileLoc = BfileVer.fork(versions.LocalBranch(),
						 sameVerRel = 1)
			BlocPkg.addFile(fileId, path, BfileLoc)

		BlocCs = BlocPkg.diff(Bpkg)[0]

		# this overwrites the package if it already exists in the
		# change set
		localCs.newPackage(BlocCs)

	    # look at each file which as changed from A->A.local; we need
	    # to get a list of changes from B->B.local (all of which are
	    # changes represented in the filesystem! they aren't represented
	    # anyplace in the database as the local branch of a file
	    # corresponds exactly to that file's instantiation in the
	    # filesystem)
	    for (fileId, (Aver, Aloc, csInfo)) in localCs.getFileList():
		if not origJob.files.has_key(fileId): 
		    # this file has disappeared between A and B or didn't
		    # change in A->B. that means we should leave it alone
		    # (and possible remove it, but that's handled later)
		    continue

		Bfile = origJob.files[fileId].file()
		Bver = origJob.files[fileId].version()

		if Aver.equal(Bver):
		    # the file hasn't changed between A and B, but is has
		    # changed between Aloc and Bloc; this means that Aloc is
		    # the right version of the file is use for Bloc as well
		    #
		    # A->B shouldn't have a change entry for this file
		    assert(not origJob.files.has_key(fileId))
		else:
		    # the file has changed between A and B, as well as
		    # between Aloc and Bloc. this situation requires
		    # taking the changes from A->Aloc, and applying them to
		    # version B of the file. package B will reference the
		    # right version of the file, since it references Bver
		    # (so package Bloc will reference file version BverLoc)

		    # XXX this could have "conflict" written all over it! we're
		    # just blindly using our local changes

		    Bloc = Bver.fork(versions.LocalBranch(), sameVerRel = 1)

		    BlocFile = Bfile.copy()
		    BlocFile.applyChange(csInfo)
		    localCs.addFile(fileId, Bver, Bloc, csInfo)

	repository.ChangeSetJob.__init__(self, repos, localCs)

	# whatever files in the final local package don't reference
	# local files need to (the only ones that do reference local
	# files are those which have had changes merged from A->A.local)
	for branchPkg in self.newPackageList():
	    for (fileId, path, version) in branchPkg.fileList():
		if not version.isLocal():
		    ver = version.fork(versions.LocalBranch(), sameVerRel = 1)
		    branchPkg.updateFile(fileId, path, ver)

	# get a list of the files which need to be created as part of
	# B->B.local
	self.paths = {}

	for f in origJob.newFileList():
	    self.paths[f.path()] = f

	for f in self.newFileList():
	    # even if the B->B.local changeset doesn't have any contents
	    # to restore (meaning that A->A.local didn't change contents),
	    # A->B may have specified changes in contents. metadata gets
	    # handled properly from fixups before popularing self, but
	    # contents aren't metadata and can't be handled before calling
	    # our parent __init__
	    path = f.path()
	    if self.paths.has_key(path) and not f.restoreContents():
		AtoB = self.paths[path]
		if AtoB.restoreContents():
		    newf = repository.ChangeSetJobFile(f.fileId(), f.file(),
				f.version(), path, AtoB.getContents(), 1)
		    self.paths[path] = newf
		else:
		    self.paths[path] = f
	    else:
		self.paths[path] = f

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
	
	# handle removals; the origJob records what removals are requested,
	# but that object won't peform any removals
	self.staleFiles += origJob.staleFiles
	self.oldPackages += origJob.oldPackages
	self.oldFiles += origJob.oldFiles
		
# Exception classes

class DatabaseError(Exception):
    """Base class for exceptions from the system database"""

    def __str__(self):
	return self.str

    def __init__(self, str = None):
	self.str = str

class RollbackError(Database):

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

class SourcePackageInstall(DatabaseError):

    def __str__(self):
	return "cannot install a source package onto the local system"

