#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

# defines the SRS system repository

class AbstractRepository:
    #
    ### Package access functions

    def getAllTroveNames(self):
	"""
	Returns a list of all of the troves contained in the repository.

	@rtype: list of str
	"""

	raise NotImplemented

    def hasPackage(self, troveName):
	"""
	Tests to see if the repository contains any version of the named
	trove.

	@param troveName: trove name
	@type troveName: str
	@rtype: boolean
	"""
	raise NotImplemented

    def hasPackageVersion(self, troveName, version):
	"""
	Tests if the repository contains a particular version of a trove.

	@param troveName: package name
	@type troveName: str
	@rtype: boolean
	"""
	raise NotImplemented

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

	raise NotImplemented

    def getLatestPackage(self, troveName, branch):
	"""
	Returns the latest trove from a given branch.

	@param troveName: package name
	@type troveName: str
	@param branch: branch
	@type branch: versions.Version
	@rtype: package.Package
	"""
	raise NotImplemented

    def getPackageVersion(self, troveName, version):
	"""
	Returns a particular version of a trove.

	@param troveName: package name
	@type troveName: str
	@param branch: branch
	@type branch: versions.Version
	@rtype: package.Package
	"""
	raise NotImplemented

    def getPackageLabelBranches(self, troveName, label):
	"""
	Returns the full branch names which matcha  given label name
	for a trove.

	@param troveName: package name
	@type troveName: str
	@param label: label
	@type label: versions.BranchName
	@rtype: package.Package
	"""
	raise NotImplemented

    def getPackageVersionList(self, troveName):
	"""
	Returns a list of all of the versions of a trove available
	in the repository.

	@param troveName: trove
	@type troveName: str
	@rtype: list of versions.Version
	"""

	raise NotImplemented

    def getPackageBranchList(self, troveName):
	"""
	Returns a list of all of the branches for a particular trove.

	@param troveName: trove
	@type troveName: str
	@rtype: list of versions.Version
	"""
	raise NotImplemented

    ### File functions

    def getFileVersion(self, fileId, version, path = None, withContents = 0):
	raise NotImplemented

    def __init__(self):
	assert(self.__class__ != AbstractRepository)

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
