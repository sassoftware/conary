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
import versions
import bsddb

from repository import AbstractRepository
from localrep import trovestore

class FilesystemRepository(AbstractRepository):

    createBranches = 1

    ### Package access functions

    def iterAllTroveNames(self):
	return self.troveStore.iterTroveNames()

    def hasPackage(self, pkgName):
	return self.troveStore.hasTrove(pkgName)

    def hasPackageVersion(self, pkgName, version):
	return self.troveStore.hasTrove(pkgName, troveVersion = version)

    def pkgLatestVersion(self, pkgName, branch):
	return self.troveStore.troveLatestVersion(pkgName, branch)

    def getLatestPackage(self, pkgName, branch):
	return self.troveStore.getLatestTrove(pkgName, branch)

    def getPackageVersion(self, pkgName, version):
	try:
	    return self.troveStore.getTrove(pkgName, version)
	except KeyError:
	    raise repository.PackageMissing(pkgName, version)

    def erasePackageVersion(self, pkgName, version):
	self.troveStore.eraseTrove(pkgName, version)

    def addPackage(self, pkg):
	self.troveStore.addTrove(pkg)

    def commit(self):
	self.troveStore.commit()

    def branchesOfTroveLabel(self, troveName, label):
	return self.troveStore.branchesOfTroveLabel(troveName, label)

    def getPackageVersionList(self, troveName):
	return self.troveStore.iterTroveVersions(troveName)

    def getPackageBranchList(self, pkgName):
	return self.troveStore.iterTroveBranches(pkgName)

    def createTroveBranch(self, pkgName, branch):
	log.debug("creating branch %s for %s", branch.asString(), pkgName)
	if not self.hasPackage(pkgName):
	    raise repository.PackageMissing, pkgName
        return self.troveStore.createTroveBranch(pkgName, branch)

    def iterFilesInTrove(self, trove, sortByPath = False, withFiles = False):
	return self.troveStore.iterFilesInTrove(trove, sortByPath, withFiles)

    ### File functions

    def getFileVersion(self, fileId, version, path = None, withContents = 0):
	file = self.troveStore.getFile(fileId, version)
	if withContents:
	    if file.hasContents:
		cont = filecontents.FromRepository(self, file.contents.sha1(), 
						   file.contents.size())
	    else:
		cont = None

	    return (file, cont)

	return file

    def addFileVersion(self, fileId, version, file):
	self.troveStore.addFile(file, version)

    def hasFileVersion(self, fileId, version):
	self.troveStore.hasFile(fileId, version)

    def eraseFileVersion(self, fileId, version):
	self.troveStore.eraseFile(fileId, version)

    ###

    def __del__(self):
	self.close()

    def storeFileFromContents(self, contents, file, restoreContents):
	if file.hasContents:
	    if restoreContents:
		f = contents.get()
		targetFile = self.contentsStore.newFile(file.contents.sha1())

		# if targetFile is None the file is already in the store
		if targetFile:
		    util.copyfileobj(f, targetFile)
		    targetFile.close()
	    else:
		# the file doesn't have any contents, so it must exist
		# in the data store already; we still need to increment
		# the reference count for it
		self.contentsStore.addFileReference(file.contents.sha1())

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
	 
	  1. C{where} specifies a particular version to branch from
	  2. the branch does not yet exist and C{where} is a label which matches multiple existing branches

	Where specifies the node branches are created from for the
	trove troveName (or all of the troves if troveName is empty).
	Any troves or files branched due to inclusion in a branched
	trove will be branched at the version required by the object
	including it. If different versions of objects are included
	from multiple places, bad things will happen (an incomplete
	branch will be formed). More complicated algorithms for branch
	will fix this, but it's not clear doing so is necessary.

	@param newBranch: Label of the new branch
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
		branchList = self.branchesOfTroveLabel(troveName, location)
		if not branchList:
		    log.error("%s does not have branch %s", troveName, location)
		for branch in branchList:
		    v = self.pkgLatestVersion(troveName, branch)
		    list.append(v)

	    for version in list:
		pkg = self.getPackageVersion(troveName, version)
		branchedVersion = version.fork(newBranch, sameVerRel = 0)
		self.createTroveBranch(troveName, branchedVersion)

		for (name, version) in pkg.iterPackageList():
		    troveList.append((name, version))

        # commit branch to the repository
        self.commit()
		    
    def open(self, mode):
	if self.troveStore is not None:
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

        try:
	    self.troveStore = trovestore.TroveStore(self.sqlDB)
        except Exception, e:
	    fcntl.lockf(self.lockfd, fcntl.LOCK_UN)
            os.close(self.lockfd)
            self.lockfd = -1
            raise repository.OpenError('Unable to open repository: %s' % str(e))

	self.mode = mode

    def createChangeSet(self, packageList):
	"""
	packageList is a list of (pkgName, oldVersion, newVersion, absolute) 
	tuples. 

	if oldVersion == None and absolute == 0, then the package is assumed
	to be new for the purposes of the change set

	if newVersion == None then the package is being removed
	"""
	cs = changeset.ChangeSetFromRepository(self)
	for (name, v1, v2, absolute) in packageList:
	    cs.addPrimaryPackage(name, v2)

	dupFilter = {}

	# don't use a for in here since we grow packageList inside of
	# this loop
	packageCounter = 0
	while packageCounter < len(packageList):
	    (packageName, oldVersion, newVersion, absolute) = \
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
                    # it's possible that a component of a package
                    # was erased, make sure that it is installed
                    if self.hasPackageVersion(name, version):
                        packageList.append((name, version, None, absolute))
		    
		continue
		    
	    new = self.getPackageVersion(packageName, newVersion)
	 
	    if oldVersion:
		old = self.getPackageVersion(packageName, oldVersion)
	    else:
		old = None

	    (pkgChgSet, filesNeeded, pkgsNeeded) = \
				new.diff(old, absolute = absolute)

	    for (pkgName, old, new) in pkgsNeeded:
		packageList.append((pkgName, old, new, absolute))

	    cs.newPackage(pkgChgSet)

	    for (fileId, oldVersion, newVersion, newPath) in filesNeeded:
		if oldVersion:
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
				       newFile.flags.isConfig())

	return cs

    def buildJob(self, changeSet):
	"""
	Returns a ChangeSetJob object representing what needs to be done
	to apply the changeset to this repository.
	"""
	return ChangeSetJob(self, changeSet)

    def commitChangeSet(self, cs):
	job = self.buildJob(cs)
	job.commit()
	self.commit()

    def close(self):
	if self.troveStore is not None:
	    self.troveStore.db.close()
	    self.troveStore = None
	    os.close(self.lockfd)
            self.lockfd = -1

    def __init__(self, path, mode = "r"):
        self.lockfd = -1
	self.top = path
	self.troveStore = None
	
	self.contentsDB = self.top + "/contents"
	self.sqlDB = self.top + "/sqldb"

	try:
	    util.mkdirChain(self.contentsDB)
	except OSError, e:
	    raise repository.OpenError(str(e))
	    
	self.contentsStore = datastore.DataStore(self.contentsDB)

        self.open(mode)

	AbstractRepository.__init__(self)

class ChangeSetJobFile(object):

    __slots__ = [ "theVersion" , "theFile", "theRestoreContents",
		  "fileContents", "changeSet", "thePath", "theFileId" ]

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

    def commit(self):
	# commit changes
	filesToArchive = []

	# we need to do this in order of fileids to make sure we walk
	# through change set streams in the right order
	fileIds = self.files.keys()
	fileIds.sort()
	for fileId in fileIds:
	    newFile = self.files[fileId]
	    file = newFile.file()

	    if not self.repos.hasFileVersion(fileId, newFile.version()):
		self.repos.addFileVersion(fileId, newFile.version(), file)

		path = newFile.path()

	    # Note that the order doesn't matter, we're just copying
	    # files into the repository. Restore the file pointer to
	    # the beginning of the file as we may want to commit this
	    # file to multiple locations.
	    self.repos.storeFileFromContents(newFile.getContents(), file, 
					     newFile.restoreContents())

	for newPkg in self.newPackageList():
	    self.repos.addPackage(newPkg)

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
	for (fileId, (oldVer, newVer, diff)) in cs.getFileList():
	    restoreContents = 1
	    if oldVer:
		oldfile = repos.getFileVersion(fileId, oldVer)
		file = repos.getFileVersion(fileId, oldVer)
		file.twm(diff, oldfile)
		
		if file.hasContents and oldfile.hasContents and	    \
		   file.contents.sha1() == oldfile.contents.sha1():
		    restoreContents = 0
	    else:
		# this is for new files
		file = files.ThawFile(diff, fileId)

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
		    f = repos.pullFileContentsObject(oldfile.contents.sha1())
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
