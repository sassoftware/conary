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
from repository import AbstractRepository
from localrep import trovestore

class FilesystemRepository(DataStoreRepository, AbstractRepository):

    createBranches = 1

    ### Package access functions

    def iterAllTroveNames(self):
	return self.troveStore.iterTroveNames()

    def getAllTroveLeafs(self, troveNameList):
	d = {}
	for troveName in troveNameList:
	    d[troveName] = [ versions.VersionFromString(x) for x in
				self.troveStore.iterAllTroveLeafs(troveName) ]
	return d

    def getTroveVersionList(self, troveNameList):
	d = {}
	for troveName in troveNameList:
	    d[troveName] = [ versions.VersionFromString(x) for x in
				self.troveStore.iterTroveVersions(troveName) ]

	return d

    def getTroveLeavesByLabel(self, troveNameList, label):
	d = {}
	labelStr = str(label)
	for troveName in troveNameList:
	    d[troveName] = [ versions.VersionFromString(x) for x in
			     self.troveStore.iterTroveLeafsByLabel(troveName,
								   labelStr) ]

	return d
	
    def getTroveVersionFlavors(self, troveDict):
	newD = {}
	for (troveName, versionList) in troveDict.iteritems():
	    innerD = {}
	    for version in versionList:
		innerD[version] = [ x for x in 
		    self.troveStore.iterTroveFlavors(troveName, version) ]
	    newD[troveName] = innerD

	return newD

    def hasPackage(self, pkgName):
	return self.troveStore.hasTrove(pkgName)

    def hasTrove(self, pkgName, version, flavor):
	return self.troveStore.hasTrove(pkgName, troveVersion = version,
					troveFlavor = flavor)

    def getTroveLatestVersion(self, pkgName, branch):
	return self.troveStore.troveLatestVersion(pkgName, branch)

    def getTrove(self, pkgName, version, flavor, pristine = True):
	try:
	    return self.troveStore.getTrove(pkgName, version, flavor)
	except KeyError:
	    raise repository.PackageMissing(pkgName, version)

    def eraseTrove(self, pkgName, version, flavor):
	self.troveStore.eraseTrove(pkgName, version, flavor)

    def addPackage(self, pkg):
	self.troveStore.addTrove(pkg)

    def commit(self):
	self.troveStore.commit()

    def branchesOfTroveLabel(self, troveName, label):
	return self.troveStore.branchesOfTroveLabel(troveName, label)

    def createTroveBranch(self, pkgName, branch):
	log.debug("creating branch %s for %s", branch.asString(), pkgName)
	if not self.hasPackage(pkgName):
	    raise repository.PackageMissing, pkgName
        return self.troveStore.createTroveBranch(pkgName, branch)

    def iterFilesInTrove(self, troveName, version, flavor,
                         sortByPath = False, withFiles = False):
	return self.troveStore.iterFilesInTrove(troveName, version, flavor,
                                                sortByPath, withFiles)

    ### File functions

    def getFileVersion(self, fileId, version, withContents = 0):
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
	# don't add duplicated to this repository
	if not self.troveStore.hasFile(fileId, version):
	    self.troveStore.addFile(file, version)

    def eraseFileVersion(self, fileId, version):
	self.troveStore.eraseFile(fileId, version)

    ###

    def __del__(self):
	self.close()

    def createBranch(self, newBranch, where, troveList = []):
	if not troveList:
	    troveList = self.iterAllTroveNames()

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
		    v = self.getTroveLatestVersion(troveName, branch)
		    list.append(v)

	    # XXX this probably doesn't get flavors right

	    d = self.getTroveVersionFlavors({troveName: list})
	    for (version, flavors) in d[troveName].iteritems():
		for flavor in flavors:
		    pkg = self.getTrove(troveName, version, flavor)
		    branchedVersion = version.fork(newBranch, sameVerRel = 0)
		    self.createTroveBranch(troveName, branchedVersion)

		    for (name, version, flavor) in pkg.iterTroveList():
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

    def commitChangeSet(self, cs):
	job = ChangeSetJob(self, cs)
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

    def addFile(self, newFile, storeContents = True):
	file = newFile.file()
	fileId = newFile.fileId()

	# duplicates are filtered out (as necessary) by addFileVersion
	self.repos.addFileVersion(fileId, newFile.version(), file)

	# Note that the order doesn't matter, we're just copying
	# files into the repository. Restore the file pointer to
	# the beginning of the file as we may want to commit this
	# file to multiple locations.
	if storeContents:
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

	    if repos.hasTrove(pkgName, newVersion, csPkg.getFlavor()):
		raise repository.CommitError, \
		       "version %s for %s is already installed" % \
			(newVersion.asString(), csPkg.getName())

	    if old:
		newPkg = repos.getTrove(pkgName, old, csPkg.getFlavor(),
					pristine = True)
		newPkg.changeVersion(newVersion)
	    else:
		newPkg = package.Trove(csPkg.getName(), newVersion,
					 csPkg.getFlavor())

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
		file = oldfile.copy()
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

		if repos.hasFileContents(file.contents.sha1()):
		    # if we already have the file in the data store we can
		    # get the contents from there
		    fileContents = filecontents.FromRepository(repos,
				    file.contents.sha1(), file.contents.size())
		    contType = changeset.ChangedFileTypes.file
		else:
		    contType = cs.getFileContentsType(fileId)
		    if contType == changeset.ChangedFileTypes.diff:
			# the content for this file is in the form of a diff,
			# which we need to apply against the file in the
			# repository
			assert(oldVer)
			(contType, fileContents) = cs.getFileContents(fileId)
			sha1 = oldfile.contents.sha1()
			f = repos.getFileContents((sha1,))[sha1]
			oldLines = f.readlines()
			del f
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

	for (pkgName, version, flavor) in cs.getOldPackageList():
	    pkg = self.repos.getTrove(pkgName, version, flavor)
	    self.oldPackage(pkg)

	    for (fileId, path, version) in pkg.iterFileList():
		file = self.repos.getFileVersion(fileId, version)
		self.oldFile(fileId, version, file)

	for newPkg in self.packagesToCommit:
	    self.addPackage(newPkg)

