#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

# implements the SRS system repository

import changeset
import copy
import datastore
import difflib
import fcntl
import filecontents
import files
import os
import package
import patch
import util
import versioned
import bsddb

from versioned import VersionedFile

class Repository:

    createBranches = 1

    def _getPackageSet(self, name):
	return _PackageSet(self.pkgDB, name)

    def _getFileDB(self, fileId):
	return _FileDB(self.fileDB, fileId)

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

    def getFullVersion(self, pkgName, version):
	return self._getPackageSet(pkgName).getFullVersion(version)

    def hasPackageVersion(self, pkgName, version):
	return self._getPackageSet(pkgName).hasVersion(version)

    def pkgLatestVersion(self, pkgName, branch):
	return self._getPackageSet(pkgName).findLatestVersion(branch)

    def getLatestPackage(self, pkgName, branch):
	return self._getPackageSet(pkgName).getLatestPackage(branch)

    def getPackageVersion(self, pkgName, version):
	return self._getPackageSet(pkgName).getVersion(version)

    def erasePackageVersion(self, pkgName, version):
	ps = self._getPackageSet(pkgName)
	ps.eraseVersion(version)

    def addPackage(self, pkg):
	ps = self._getPackageSet(pkg.getName())
	ps.addVersion(pkg.getVersion(), pkg)

    def getPackageVersionList(self, pkgName):
	return self._getPackageSet(pkgName).fullVersionList()

    def fileLatestVersion(self, fileId, branch):
	fileDB = self._getFileDB(fileId)
	return fileDB.findLatestVersion(branch)
	
    def getFileVersion(self, fileId, version, path = None, withContents = 0):
	fileDB = self._getFileDB(fileId)
	file = fileDB.getVersion(version)
	if withContents:
	    if file.hasContents:
		cont = filecontents.FromRepository(self, file.sha1())
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

    def storeFileFromContents(self, chgSet, file, restoreContents):
	raise NotImplemented

    def __del__(self):
	self.close()

    def __init__(self):
	pass

class LocalRepository(Repository):

    def storeFileFromContents(self, contents, file, restoreContents):
	if file.hasContents:
	    if restoreContents:
		f = contents.get()
		targetFile = self.contentsStore.newFile(file.sha1())

		# if targetFile is None the file is already in the store
		if targetFile:
		    util.copyfileobj(f, targetFile)
		    f.seek(0)
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

    def open(self, mode):
	if self.pkgDB:
	    self.close()

	self.lockfd = os.open(self.top + "/lock", os.O_CREAT | os.O_RDWR)

	if mode == "r":
	    fcntl.lockf(self.lockfd, fcntl.LOCK_SH)
	else:
	    fcntl.lockf(self.lockfd, fcntl.LOCK_EX)

        try:
            self.pkgDB = versioned.FileIndexedDatabase(self.top + "/pkgs.db", 
						   self.createBranches, mode)
            self.fileDB = versioned.Database(self.top + "/files.db", 
					     self.createBranches, mode)
        # XXX this should be translated into a generic versioned.DatabaseError
        except bsddb.error:
            # an error occured, close our databases and relinquish the lock
            if self.pkgDB is not None:
                self.pkgDB.close()
                self.pkgDB = None
	    fcntl.lockf(self.lockfd, fcntl.LOCK_UN)
            os.close(self.lockfd)
            self.lockfd = -1
            raise

	self.mode = mode

    # packageList is a list of (pkgName, oldVersion, newVersion, abstract) 
    # tuples
    #
    # if oldVersion == None and abstract == 0, then the package is assumed
    # to be new for the purposes of the change set
    def createChangeSet(self, packageList):
	cs = changeset.ChangeSetFromRepository(self)

	for (packageName, oldVersion, newVersion, abstract) in packageList:
	    # look up these versions to get versions w/ timestamps
	    new = self.getPackageVersion(packageName, newVersion)
	 
	    if oldVersion:
		old = self.getPackageVersion(packageName, oldVersion)
	    else:
		old = None

	    (pkgChgSet, filesNeeded) = new.diff(old, abstract = abstract)

	    # there were no changes
	    #if not filesNeeded: continue

	    cs.newPackage(pkgChgSet)

	    for (fileId, oldVersion, newVersion, newPath) in filesNeeded:
		oldFile = None
		if oldVersion:
		    oldFile = self.getFileVersion(fileId, oldVersion)
		    (oldFile, oldCont) = self.getFileVersion(fileId, 
				oldVersion, path = newPath, withContents = 1)
		(newFile, newCont) = self.getFileVersion(fileId, newVersion,
					    path = newPath, withContents = 1)

		(filecs, hash) = changeset.fileChangeSet(fileId, oldFile, 
							 newFile)

		cs.addFile(fileId, oldVersion, newVersion, filecs)

		if hash:
		    if oldFile and oldFile.isConfig() and newFile.isConfig():
			diff = difflib.unified_diff(oldCont.get().readlines(),
						    newCont.get().readlines(),
						    "old", "new")
			diff.next()
			diff.next()
			cont = filecontents.FromString("".join(diff))
			cs.addFileContents(hash, 
				    changeset.ChangedFileTypes.diff, cont)
		    else:
			cs.addFileContents(hash, 
				changeset.ChangedFileTypes.file, newCont)

	return cs

    def commitChangeSet(self, cs):
	job = ChangeSetJob(self, cs)
	undo = ChangeSetUndo(self)

	try:
	    job.commit(undo)
	except:
	    undo.undo()
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
	util.mkdirChain(self.contentsDB)

	self.contentsStore = datastore.DataStore(self.contentsDB)

	self.open(mode)

	Repository.__init__(self)

# this is a set of all of the versions of a single packages 
class _PackageSetClass(VersionedFile):
    def getVersion(self, version):
	f1 = VersionedFile.getVersion(self, version)
	p = package.PackageFromFile(self.name, f1, version)
	f1.close()
	return p

    def addVersion(self, version, package):
	VersionedFile.addVersion(self, version, package.formatString())

    def fullVersionList(self):
	branches = self.f.branchList()
	rc = []
	for branch in branches:
	    rc += self.f.versionList(branch)

	return rc

    def getLatestPackage(self, branch):
	return self.getVersion(self.findLatestVersion(branch))

    def __init__(self, db, name, createBranches):
	VersionedFile.__init__(self, db, name, createBranches)
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
	
	VersionedFile.addVersion(self, version, "%s\n" % file.infoLine())

    def getVersion(self, version):
	f1 = VersionedFile.getVersion(self, version)
	file = files.FileFromInfoLine(f1.read(), self.fileId)
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
	return self.fileContents

    def __init__(self, fileId, file, version, path, fileContents,
		 restoreContents):
	self.theVersion = version
	self.theFile = file
	self.theRestoreContents = restoreContents
	self.fileContents = fileContents
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

    # the undo object it kept up-to-date with what needs to be done to undo
    # the work completed so far; the caller can use a try/except block to
    # cause an undo to happen if an error occurs the change set is needed to
    # access the file contents; it's not used for anything else
    def commit(self, undo):
	# commit changes
	filesToArchive = []

	for newPkg in self.newPackageList():
	    self.repos.addPackage(newPkg)
	    undo.addedPackage(newPkg)

	for newFile in self.newFileList():
	    file = newFile.file()
	    fileId = newFile.fileId()

	    if not self.repos.hasFileVersion(fileId, newFile.version()):
		self.repos.addFileVersion(fileId, newFile.version(), file)
		undo.addedFile(newFile)

		path = newFile.path()

	    # Note that the order doesn't matter, we're just copying
	    # files into the repository. Restore the file pointer to
	    # the beginning of the file as we may want to commit this
	    # file to multiple locations.
	    if self.repos.storeFileFromContents(newFile.getContents(), file, 
						 newFile.restoreContents()):
		undo.addedFileContents(file.sha1())

	# This doesn't actually remove anything! we never allow bits
	# to get erased from repositories. The database, which is a child
	# of this object, does allow removals.

    def __init__(self, repos, cs):
	self.repos = repos
	self.cs = cs

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
	for csPkg in cs.getNewPackageList():
	    newVersion = csPkg.getNewVersion()
	    old = csPkg.getOldVersion()
	    pkgName = csPkg.getName()

	    if repos.hasPackage(pkgName):
		if repos.hasPackageVersion(pkgName, newVersion):
		    raise CommitError, "version %s for %s is already installed" % \
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
		file = files.FileFromInfoLine(infoLine, fileId)

	    # we should have had a package which requires this (new) version
	    # of the file
	    assert(newVer.equal(fileMap[fileId][1]))

	    if file.hasContents and restoreContents:
		(contType, fileContents) = cs.getFileContents(file.sha1())
		if contType == changeset.ChangedFileTypes.diff:
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
		fileContents = None

	    path = fileMap[fileId][0]
	    self.addFile(ChangeSetJobFile(fileId, file, newVer, path, 
					  fileContents, restoreContents))

	for (pkgName, version) in cs.getOldPackageList():
	    pkg = self.repos.getPackageVersion(pkgName, version)
	    self.oldPackage(pkg)

	    for (fileId, path, version) in pkg.fileList():
		file = self.repos.getFileVersion(fileId, version)
		self.oldFile(fileId, version, file)
		
		if not self.containsFilePath(path):
		    self.addStaleFile(path, file)
	#import sys
	#sys.exit(0)

class ChangeSetUndo:

    def undo(self):
	# something went wrong; try to unwind our commits
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

    def __init__(self, repos):
	self.reset()
	self.repos = repos

class RepositoryError(Exception):
    """Base class for exceptions from the system repository"""

class CommitError(RepositoryError):

    def __repr__(self):
	return self.str

    def __str__(self):
	return repr(self)

    def __init__(self, str):
	self.str = str

