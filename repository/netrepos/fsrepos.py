#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

# implements a db-based repository

import changeset
import copy
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

from repository import DataStoreRepository
from localrep import trovestore

class FilesystemRepository(DataStoreRepository):

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

    def getPackageVersion(self, pkgName, version, pristine = True):
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

    def getFileContents(self, file):
	return filecontents.FromRepository(self, file.contents.sha1(), 
					   file.contents.size())
	
    def addFileVersion(self, fileId, version, file):
	# don't add duplicated to this repository
	if not self.troveStore.hasFile(fileId, version):
	    self.troveStore.addFile(file, version)

    def eraseFileVersion(self, fileId, version):
	self.troveStore.eraseFile(fileId, version)

    ###

    def __del__(self):
	self.close()

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

    def buildJob(self, changeSet):
	"""
	Returns a ChangeSetJob object representing what needs to be done
	to apply the changeset to this repository.
	"""
	return ChangeSetJob(self, changeSet)

    def commitChangeSet(self, cs):
	job = self.buildJob(cs)
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
	
	self.sqlDB = self.top + "/sqldb"

	try:
	    util.mkdirChain(self.top)
	except OSError, e:
	    raise repository.OpenError(str(e))
	    
        self.open(mode)

	DataStoreRepository.__init__(self, path)

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

class ChangeSetJob:
    """
    ChangeSetJob provides a to-do list for applying a change set; file
    remappings should have been applied to the change set before it gets
    this far. Derivative classes can override these methods to change the
    behavior; for example, if addPackage is overridden no pacakges will
    make it to the database. The same holds for oldFile.
    """

    def addPackage(self, pkg):
	self.repos.addPackage(pkg)

    def oldPackage(self, pkg):
	pass

    def oldFile(self, fileId, fileVersion, fileObj):
	pass

    def addFile(self, fileObject):
	newFile = fileObject
	file = newFile.file()
	fileId = fileObject.fileId()

	# duplicates are filtered out (as necessary) by addFileVersion
	self.repos.addFileVersion(fileId, newFile.version(), file)

	# Note that the order doesn't matter, we're just copying
	# files into the repository. Restore the file pointer to
	# the beginning of the file as we may want to commit this
	# file to multiple locations.
	self.repos.storeFileFromContents(newFile.getContents(), file, 
					 newFile.restoreContents())

    def __init__(self, repos, cs):
	self.repos = repos
	self.cs = cs

	self.packagesToCommit = []

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
		newPkg = repos.getPackageVersion(pkgName, old, pristine = True)
		newPkg.changeVersion(newVersion)
	    else:
		newPkg = package.Package(csPkg.name, newVersion)

	    newFileMap = newPkg.applyChangeSet(csPkg)

	    self.packagesToCommit.append(newPkg)
	    fileMap.update(newFileMap)

	# Create the file objects we'll need for the commit. This handles
	# files which were added and files which have changed
	list = cs.getFileList()
	# sort this by fileid to ensure we pull files from the change
	# set in the right order
	list.sort()
	for (fileId, (oldVer, newVer, diff)) in list:
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

	for newPkg in self.packagesToCommit:
	    self.addPackage(newPkg)

