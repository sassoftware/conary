#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

# defines the SRS system repository

import changeset
import datastore
import util

class AbstractRepository:
    #
    ### Package access functions

    def iterAllTroveNames(self):
	"""
	Returns a list of all of the troves contained in the repository.

	@rtype: list of str
	"""

	raise NotImplementedError

    def hasPackage(self, troveName):
	"""
	Tests to see if the repository contains any version of the named
	trove.

	@param troveName: trove name
	@type troveName: str
	@rtype: boolean
	"""
	raise NotImplementedError

    def hasPackageVersion(self, troveName, version):
	"""
	Tests if the repository contains a particular version of a trove.

	@param troveName: package name
	@type troveName: str
	@rtype: boolean
	"""
	raise NotImplementedError

    def pkgLatestVersion(self, troveName, branch):
	"""
	Returns the version of the latest version of a trove on a particular
	branch.

	@param troveName: package name
	@type troveName: str
	@param branch: branch
	@type branch: versions.Version
	@rtype: versions.Version
	"""

	raise NotImplementedError

    def getLatestPackage(self, troveName, branch):
	"""
	Returns the latest trove from a given branch.

	@param troveName: package name
	@type troveName: str
	@param branch: branch
	@type branch: versions.Version
	@rtype: package.Package
	"""
	raise NotImplementedError

    def getPackageVersion(self, troveName, version):
	"""
	Returns a particular version of a trove.

	@param troveName: package name
	@type troveName: str
	@param version: version
	@type version: versions.Version
	@rtype: package.Package
	"""
	raise NotImplementedError

    def branchesOfTroveLabel(self, troveName, label):
	"""
	Returns the full branch names which matcha  given label name
	for a trove.

	@param troveName: package name
	@type troveName: str
	@param label: label
	@type label: versions.BranchName
	@rtype: package.Package
	"""
	raise NotImplementedError

    def getPackageVersionList(self, troveName):
	"""
	Returns a list of all of the versions of a trove available
	in the repository.

	@param troveName: trove
	@type troveName: str
	@rtype: list of versions.Version
	"""

	raise NotImplementedError

    def getPackageBranchList(self, troveName):
	"""
	Returns a list of all of the branches for a particular trove.

	@param troveName: trove
	@type troveName: str
	@rtype: list of versions.Version
	"""
	raise NotImplementedError

    ### File functions

    def getFileVersion(self, fileId, version, path = None, withContents = 0):
	raise NotImplementedError

    def iterFilesInTrove(self, trove, sortByPath = False, withFiles = False):
	raise NotImplementedError

    def buildJob(self, changeSet):
	raise NotImplementedError

    def storeFileFromContents(self, contents, file, restoreContents):
	raise NotImplementedError

    def addFileVersion(self, fileId, version, file):
	raise NotImplementedError

    def addPackage(self, pkg):
	raise NotImplementedError

    def commit(self):
	raise NotImplementedError

    def eraseFileVersion(self, fileId, version):
	raise NotImplementedError

    def erasePackageVersion(self, pkgName, version):
	raise NotImplementedError

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

    def __init__(self):
	assert(self.__class__ != AbstractRepository)

class DataStoreRepository(AbstractRepository):

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

    def hasPackageVersion(self, pkgName, version):
	return self.contentsStore.hasFile(fileId)

    def __init__(self, path):
	fullPath = path + "/contents"
	util.mkdirChain(fullPath)
	self.contentsStore = datastore.DataStore(fullPath)

class RepositoryError(Exception):
    """Base class for exceptions from the system repository"""

class OpenError(RepositoryError):
    """Error occured opening the repository"""

class CommitError(RepositoryError):
    """Error occured commiting a package"""

class PackageMissing(RepositoryError):

    def __str__(self):
	if self.version:
	    if self.version.isBranch():
		return ("%s %s does not exist on branch %s" % \
		    (self.type, self.packageName, self.version.asString()))

	    return "version %s of %s %s does not exist" % \
		(self.version.asString(), self.type, self.packageName)
	else:
	    return "%s %s does not exist" % (self.type, self.packageName)

    def __init__(self, packageName, version = None):
	"""
	Initializes a PackageMissing exception.

	@param packageName: package which could not be found
	@type packageName: str
	@param version: version of the package which does not exist
	@type version: versions.Version
	"""
	self.packageName = packageName
	self.version = version
	self.type = "package"
