#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

# implements a db-based repository

import changeset
import copy
import datastore
import fcntl
import filecontents
import files
import log
import os
import package
import patch
import repository
import util
import versioned
import versions
import bsddb

from repository import AbstractRepository
from versioned import VersionedFile
from versioned import IndexedVersionedFile
class FilesystemRepository(AbstractRepository):

    createBranches = 1

    def _getPackageSet(self, name):
        try:
            return _PackageSet(self.pkgDB, name)
        except versioned.MissingFileError, e:
            raise repository.PackageMissing(e.name)

    def _getFileDB(self, fileId):
	return _FileDB(self.fileDB, fileId)

    ### Package access functions

    def iterAllTroveNames(self):
	return self.pkgDB.iterFileList()

    def hasPackage(self, pkg):
	return self.pkgDB.hasFile(pkg)

    def hasPackageVersion(self, pkgName, version):
	return self._getPackageSet(pkgName).hasVersion(version)

    def pkgLatestVersion(self, pkgName, branch):
	return self._getPackageSet(pkgName).findLatestVersion(branch)

    def getLatestPackage(self, pkgName, branch):
	return self._getPackageSet(pkgName).getLatestPackage(branch)

    def getPackageVersion(self, pkgName, version):
	try:
	    return self._getPackageSet(pkgName).getVersion(version)
	except KeyError:
	    raise repository.PackageMissing(pkgName, version)

    def erasePackageVersion(self, pkgName, version):
	ps = self._getPackageSet(pkgName)
	ps.eraseVersion(version)

    def addPackage(self, pkg):
	ps = self._getPackageSet(pkg.getName())
	ps.addVersion(pkg.getVersion(), pkg)

    def getPackageLabelBranches(self, pkgName, nick):
	return self._getPackageSet(pkgName).mapBranchNickname(nick)

    def getPackageVersionList(self, pkgName):
	return self._getPackageSet(pkgName).fullVersionList()

    def getPackageBranchList(self, pkgName):
	return self._getPackageSet(pkgName).branchList()

    def createTroveBranch(self, pkgName, branch):
	log.debug("creating branch %s for %s", branch.asString(), pkgName)
	if not self.hasPackage(pkgName):
	    raise repository.PackageMissing, pkgName
	return self._getPackageSet(pkgName).createBranch(branch)

    ### File functions

    def getFileVersion(self, fileId, version, path = None, withContents = 0):
	fileDB = self._getFileDB(fileId)
	file = fileDB.getVersion(version)
	if withContents:
	    if file.hasContents:
		cont = filecontents.FromRepository(self, file.sha1(), 
						   file.size())
	    else:
		cont = None

	    return (file, cont)

	return file

    def addFileVersion(self, fileId, version, file):
	fileDB = self._getFileDB(fileId)
	fileDB.addVersion(version, file)

    def hasFileVersion(self, fileId, version):
	fileDB = self._getFileDB(fileId)
	return fileDB.hasVersion(version)

    def eraseFileVersion(self, fileId, version):
	fileDB = self._getFileDB(fileId)
	fileDB.eraseVersion(version)

    def createFileBranch(self, fileId, branch):
	log.debug("creating branch %s for %s" % (branch.asString(), fileId))
	return self._getFileDB(fileId).createBranch(branch)

    ###

    def __del__(self):
	self.close()

    def storeFileFromContents(self, contents, file, restoreContents):
	if file.hasContents:
	    if restoreContents:
		f = contents.get()
		targetFile = self.contentsStore.newFile(file.sha1())

		# if targetFile is None the file is already in the store
		if targetFile:
		    util.copyfileobj(f, targetFile)
		    targetFile.close()
	    else:
		# the file doesn't have any contents, so it must exist
		# in the data store already; we still need to increment
		# the reference count for it
		self.contentsStore.addFileReference(file.sha1())

	    return 1
	
	return 0

    def removeFileContents(self, sha1):
	self.contentsStore.removeFile(sha1)

    def pullFileContentsObject(self, fileId):
	return self.contentsStore.openFile(fileId)

    def hasFileContents(self, fileId):
	return self.contentsStore.hasFile(fileId)

    def createBranch(self, newBranch, where, troveName = None):
	"""
	Creates a branch for the troves in the repository. This
	operations is recursive, with any required troves and files
	also getting branched. Duplicate branches can be created,
	but only if one of the following is true:
	 
	  1. where specifies a particular version to branch from
	  2. the branch does not yet exist and where is a branch nickname which matches multiple existing branches

	Where specifies the node branches are created from for the
	trove troveName (or all of the troves if troveName is empty).
	Any troves or files branched due to inclusion in a branched
	trove will be branched at the version required by the object
	including it. If different versions of objects are included
	from multiple places, bad things will happen (an incomplete
	branch will be formed). More complicated algorithms for branch
	will fix this, but it's not clear doing so is necessary.

	@param newBranch: Nickname of the new branch
	@type newBranch: versions.BranchName
	@param where: Where the branch should be created from
	@type where: versions.Version or versions.BranchName
	@param troveName: Name of the trove to branch; none if all
	troves in the repository should be branched.
	@type troveName: str
	"""
	if not troveName:
	    troveList = self.iterAllTroveNames()
	else:
	    troveList = [ troveName ]

	troveList = [ (x, where) for x in troveList ]

	branchedTroves = {}
	branchedFiles = {}

	while troveList:
	    troveName = troveList[0][0]
	    location = troveList[0][1]
	    del troveList[0]

	    if branchedTroves.has_key(troveName): continue
	    branchedTroves[troveName] = 1
	    if not self.hasPackage(troveName):
		log.warning("package %s does not exist" % troveName)
		continue

	    list = []
	    if isinstance(location, versions.Version):
		list.append(location)
	    else:
		branchList = self.getPackageLabelBranches(troveName, location)
		for branch in branchList:
		    v = self.pkgLatestVersion(troveName, branch)
		    list.append(v)

	    for version in list:
		pkg = self.getPackageVersion(troveName, version)
		branchedVersion = version.fork(newBranch, sameVerRel = 0)
		self.createTroveBranch(troveName, branchedVersion)

		for (fileId, path, version) in pkg.iterFileList():
		    if branchedFiles.has_key(fileId): continue
		    branchedFiles[fileId] = 1

		    branchedVersion = version.fork(newBranch, sameVerRel = 0)
		    self.createFileBranch(fileId, branchedVersion)

		for (name, version) in pkg.iterPackageList():
		    troveList.append((name, version))
		    
    def open(self, mode):
	if self.pkgDB:
	    self.close()

        flags = os.O_RDONLY
        if mode != 'r':
            flags |= os.O_CREAT | os.O_RDWR
        try:
            self.lockfd = os.open(self.top + '/lock', flags)
        except OSError, e:
            raise repository.OpenError('Unable to open lock file %s for '
		'%s: %s' % (self.top + '/lock', 
		mode == 'r' and 'read' or 'read/write',
                e.strerror))

        try:
            if mode == 'r':
                fcntl.lockf(self.lockfd, fcntl.LOCK_SH)
            else:
                fcntl.lockf(self.lockfd, fcntl.LOCK_EX)
        except IOError, e:
            raise repository.OpenError('Unable to obtain %s lock: %s' % (
                mode == 'r' and 'shared' or 'exclusive', e.strerror))

	self.pkgDB = None

        try:
            self.pkgDB = versioned.FileIndexedDatabase(self.top + "/pkgs.db", 
						   self.createBranches, mode)
            self.fileDB = versioned.Database(self.top + "/files.db", 
					     self.createBranches, mode)
        # XXX this should be translated into a generic versioned.DatabaseError
        except Exception, e:
            # an error occured, close our databases and relinquish the lock
            if self.pkgDB is not None:
                self.pkgDB.close()
                self.pkgDB = None

	    fcntl.lockf(self.lockfd, fcntl.LOCK_UN)
            os.close(self.lockfd)
            self.lockfd = -1
            raise repository.OpenError('Unable to open repository: %s' % str(e))

	self.mode = mode

    def createChangeSet(self, packageList):
	"""
	packageList is a list of (pkgName, oldVersion, newVersion, abstract) 
	tuples. 

	if oldVersion == None and abstract == 0, then the package is assumed
	to be new for the purposes of the change set

	if newVersion == None then the package is being removed
	"""
	cs = changeset.ChangeSetFromRepository(self)
	for (name, v1, v2, abstract) in packageList:
	    cs.addPrimaryPackage(name, v2)

	dupFilter = {}

	# don't use a for in here since we grow packageList inside of
	# this loop
	packageCounter = 0
	while packageCounter < len(packageList):
	    (packageName, oldVersion, newVersion, abstract) = \
		packageList[packageCounter]
	    packageCounter += 1

	    # make sure we haven't already generated this changeset; since
	    # packages can be included from other packages we could try
	    # to generate quite a few duplicates
	    if dupFilter.has_key(packageName):
		match = False
		for (otherOld, otherNew) in dupFilter[packageName]:
		    if not otherOld and not oldVersion:
			same = True
		    elif not otherOld and oldVersion:
			same = False
		    elif otherOld and not oldVersion:
			same = False
		    else:
			same = otherOld == newVersion

		    if same and otherNew == newVersion:
			match = True
			break
		
		if match: continue

		dupFilter[packageName].append((oldVersion, newVersion))
	    else:
		dupFilter[packageName] = [ (oldVersion, newVersion) ]

	    if not newVersion:
		# remove this package and any subpackages
		old = self.getPackageVersion(packageName, oldVersion)
		cs.oldPackage(packageName, oldVersion)
		for (name, version) in old.iterPackageList():
		    packageList.append((name, version, None, abstract))
		    
		continue
		    
	    new = self.getPackageVersion(packageName, newVersion)
	 
	    if oldVersion:
		old = self.getPackageVersion(packageName, oldVersion)
	    else:
		old = None

	    (pkgChgSet, filesNeeded, pkgsNeeded) = \
				new.diff(old, abstract = abstract)

	    for (pkgName, old, new) in pkgsNeeded:
		packageList.append((pkgName, old, new, abstract))

	    cs.newPackage(pkgChgSet)

	    for (fileId, oldVersion, newVersion, newPath) in filesNeeded:
		if oldVersion:
		    oldFile = self.getFileVersion(fileId, oldVersion)
		    (oldFile, oldCont) = self.getFileVersion(fileId, 
				oldVersion, path = newPath, withContents = 1)
		else:
		    oldFile = None
		    oldCont = None

		(newFile, newCont) = self.getFileVersion(fileId, newVersion,
					    path = newPath, withContents = 1)

		(filecs, hash) = changeset.fileChangeSet(fileId, oldFile, 
							 newFile)

		cs.addFile(fileId, oldVersion, newVersion, filecs)

		if hash:
		    (contType, cont) = changeset.fileContentsDiff(oldFile, 
						oldCont, newFile, newCont)
		    cs.addFileContents(fileId, contType, cont, 
				       newFile.isConfig())

	return cs

    def buildJob(self, changeSet):
	"""
	Returns a ChangeSetJob object representing what needs to be done
	to apply the changeset to this repository.
	"""
	return ChangeSetJob(self, changeSet)

    def commitChangeSet(self, cs):
	job = self.buildJob(cs)

	try:
	    job.commit()
	except:
	    job.undo()
	    raise

    def close(self):
	if self.pkgDB is not None:
            self.pkgDB.close()
            self.fileDB.close()
	    self.pkgDB = None
	    self.fileDB = None
	    os.close(self.lockfd)
            self.lockfd = -1

    def __init__(self, path, mode = "r"):
        self.lockfd = -1
	self.top = path
	self.pkgDB = None
	
	self.contentsDB = self.top + "/contents"

	try:
	    util.mkdirChain(self.contentsDB)
	except OSError, e:
	    raise repository.OpenError(str(e))
	    
	self.contentsStore = datastore.DataStore(self.contentsDB)

        self.open(mode)

	AbstractRepository.__init__(self)

# this is a set of all of the versions of a single packages 
class _PackageSetClass(IndexedVersionedFile):
    def getVersion(self, version):
	f1 = IndexedVersionedFile.getVersion(self, version)
	p = package.PackageFromFile(self.name, f1, version)
	f1.close()
	return p

    def addVersion(self, version, package):
	IndexedVersionedFile.addVersion(self, version, package.formatString())

    def fullVersionList(self):
	branches = self.branchList()
	rc = []
	for branch in branches:
	    rc += self.versionList(branch)

	return rc

    def getLatestPackage(self, branch):
	ver = self.findLatestVersion(branch)
	if not ver:
	    raise repository.PackageMissing(self.name, branch)

	return self.getVersion(ver)

    def __init__(self, db, name, createBranches, dbFile):
	IndexedVersionedFile.__init__(self, db, name, createBranches, dbFile)
	self.name = name

def _PackageSet(db, name):
    return db.openFile(name, fileClass = _PackageSetClass)

class _FileDBClass(VersionedFile):

    def addVersion(self, version, file):
	if self.hasVersion(version):
	    raise KeyError, "duplicate version for database"
	else:
	    if file.id() != self.fileId:
		raise KeyError, "file id mismatch for file database"
	
	VersionedFile.addVersion(self, version, "%s\n" % file.freeze())

    def getVersion(self, version):
	f1 = VersionedFile.getVersion(self, version)
	file = files.ThawFile(f1.read(), self.fileId)
	f1.close()
	return file

    def __init__(self, db, fileId, createBranches):
	VersionedFile.__init__(self, db, fileId, createBranches)
	self.fileId = fileId

def _FileDB(db, fileId):
    return db.openFile(fileId, fileClass = _FileDBClass)

class ChangeSetJobFile:

    def version(self):
	return self.theVersion

    def changeVersion(self, ver):
	self.theVersion = ver

    def restoreContents(self):
	return self.theRestoreContents

    def file(self):
	return self.theFile

    def changeFile(self, fileObj):
	self.theFile = fileObj

    def path(self):
	return self.thePath

    def fileId(self):
	return self.theFileId

    def copy(self):
	return copy.deepcopy(self)

    def getContents(self):
	if self.fileContents == "":
	    return None
	elif self.fileContents:
	    return self.fileContents
	
	return self.changeSet.getFileContents(self.theFileId)[1]

    # overrideContents = None means use contents from changeset
    # overrideContents = "" means there are no contents
    def __init__(self, changeSet, fileId, file, version, path, 
		 overrideContents, restoreContents):
	self.theVersion = version
	self.theFile = file
	self.theRestoreContents = restoreContents
	self.fileContents = overrideContents
	self.changeSet = changeSet
	self.thePath = path
	self.theFileId = fileId

# ChangeSetJob provides a to-do list for applying a change set; file
# remappings should have been applied to the change set before it gets
# this far
class ChangeSetJob:

    def addPackage(self, pkg):
	self.packages.append(pkg)

    def newPackageList(self):
	return self.packages

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

    def addFile(self, fileObject):
	self.files[fileObject.fileId()] = fileObject
	self.filePaths[fileObject.path] = 1

    def getFile(self, fileId):
	return self.files[fileId]

    def containsFilePath(self, path):
	return self.filePaths.has_key(path)

    def newFileList(self):
	return self.files.values()

    # the undo object is kept up-to-date with what needs to be done to undo
    # the work completed so far; the caller can use a try/except block to
    # cause an undo to happen if an error occurs the change set is needed to
    # access the file contents; it's not used for anything else
    def commit(self):
	# commit changes
	filesToArchive = []

	for newPkg in self.newPackageList():
	    self.repos.addPackage(newPkg)
	    self.undoObj.addedPackage(newPkg)

	# we need to do this in order of fileids to make sure we walk
	# through change set streams in the right order
	fileIds = self.files.keys()
	fileIds.sort()
	for fileId in fileIds:
	    newFile = self.files[fileId]
	    file = newFile.file()

	    if not self.repos.hasFileVersion(fileId, newFile.version()):
		self.repos.addFileVersion(fileId, newFile.version(), file)
		self.undoObj.addedFile(newFile)

		path = newFile.path()

	    # Note that the order doesn't matter, we're just copying
	    # files into the repository. Restore the file pointer to
	    # the beginning of the file as we may want to commit this
	    # file to multiple locations.
	    if self.repos.storeFileFromContents(newFile.getContents(), file, 
						 newFile.restoreContents()):
		self.undoObj.addedFileContents(file.sha1())

	# This doesn't actually remove anything! we never allow bits
	# to get erased from repositories. The database, which is a child
	# of this object, does allow removals.
	
    def undo(self):
	self.undoObj.undo()

    def __init__(self, repos, cs):
	self.repos = repos
	self.cs = cs
	self.undoObj = ChangeSetUndo(repos)

	self.packages = []
	self.files = {}
	self.filePaths = {}
	self.oldPackages = []
	self.oldFiles = []
	self.staleFiles = []

	fileMap = {}

	# create the package objects which need to be installed; the
	# file objects which map up with them are created later, but
	# we do need a map from fileId to the path and version of the
	# file we need, so build up a dictionary with that information
	for csPkg in cs.iterNewPackageList():
	    newVersion = csPkg.getNewVersion()
	    old = csPkg.getOldVersion()
	    pkgName = csPkg.getName()

	    if repos.hasPackage(pkgName):
		if repos.hasPackageVersion(pkgName, newVersion):
		    raise repository.CommitError, \
			   "version %s for %s is already installed" % \
			    (newVersion.asString(), csPkg.getName())

	    if old:
		newPkg = repos.getPackageVersion(pkgName, old)
		newPkg.changeVersion(newVersion)
	    else:
		newPkg = package.Package(csPkg.name, newVersion)

	    newFileMap = newPkg.applyChangeSet(csPkg)

	    self.addPackage(newPkg)
	    fileMap.update(newFileMap)

	# Create the file objects we'll need for the commit. This handles
	# files which were added and files which have changed
	for (fileId, (oldVer, newVer, infoLine)) in cs.getFileList():
	    restoreContents = 1
	    if oldVer:
		oldfile = repos.getFileVersion(fileId, oldVer)
		file = repos.getFileVersion(fileId, oldVer)
		file.applyChange(infoLine)
		
		if file.hasContents and oldfile.hasContents and	    \
		   file.sha1() == oldfile.sha1():
		    restoreContents = 0
	    else:
		# this is for new files
		file = files.ThawFile(infoLine, fileId)

	    # we should have had a package which requires this (new) version
	    # of the file
	    assert(newVer == fileMap[fileId][1])

	    if file.hasContents and restoreContents:
		fileContents = None
		contType = cs.getFileContentsType(fileId)
		if contType == changeset.ChangedFileTypes.diff:
		    (contType, fileContents) = cs.getFileContents(fileId)
		    # the content for this file is in the form of a diff,
		    # which we need to apply against the file in the repository
		    assert(oldVer)
		    f = repos.pullFileContentsObject(oldfile.sha1())
		    oldLines = f.readlines()
		    diff = fileContents.get().readlines()
		    (newLines, failedHunks) = patch.patch(oldLines, diff)
		    fileContents = filecontents.FromString("".join(newLines))

		    if failedHunks:
			fileContents = filecontents.WithFailedHunks(
					    fileContents, failedHunks)
	    else:
		# this means there are no contents to restore (None
		# means get the contents from the change set)
		fileContents = ""

	    path = fileMap[fileId][0]
	    self.addFile(ChangeSetJobFile(cs, fileId, file, newVer, path, 
					  fileContents, restoreContents))

	for (pkgName, version) in cs.getOldPackageList():
	    pkg = self.repos.getPackageVersion(pkgName, version)
	    self.oldPackage(pkg)

	    for (fileId, path, version) in pkg.iterFileList():
		file = self.repos.getFileVersion(fileId, version)
		self.oldFile(fileId, version, file)
		
		if not self.containsFilePath(path):
		    self.addStaleFile(path, file)
	#import sys
	#sys.exit(0)

class ChangeSetUndo:

    def undo(self):
	# something went wrong; try to unwind our commits. the order
	# on this matters greatly!
	for pkg in self.removedPackages:
	    self.repos.addPackage(pkg)

	for (fileId, fileVersion, fileObj) in self.removedFiles:
	    self.repos.addFileVersion(fileId, fileVersion, fileObj)

	for newFile in self.filesDone:
	    self.repos.eraseFileVersion(newFile.fileId(), newFile.version())

	for pkg in self.pkgsDone:
	    self.repos.erasePackageVersion(pkg.getName(), pkg.getVersion())

	for sha1 in self.filesStored:
	    self.repos.removeFileContents(sha1)

	self.reset()

    def addedPackage(self, pkg):
	self.pkgsDone.append(pkg)

    def addedFile(self, file):
	self.filesDone.append(file)

    def addedFileContents(self, sha1):
	self.filesStored.append(sha1)

    def removedPackage(self, pkg):
	self.removedPackages.append(pkg)

    def removedFile(self, fileId, fileVersion, fileObj):
	self.removedFiles.append((fileId, fileVersion, fileObj))

    def reset(self):
	self.filesDone = []
	self.pkgsDone = []
	self.filesStored = []
	self.removedPackages = []
	self.removedFiles = []
	self.groupsDone = []

    def __init__(self, repos):
	self.reset()
	self.repos = repos


