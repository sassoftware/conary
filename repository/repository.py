#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

# defines the SRS system repository

class AbstractRepository:
    #
    ### Package access functions

    def getAllTroveNames(self):
	raise NotImplemented

    def hasPackage(self, pkg):
	raise NotImplemented

    def pkgGetFullVersion(self, pkgName, version):
	raise NotImplemented

    def hasPackageVersion(self, pkgName, version):
	raise NotImplemented

    def pkgLatestVersion(self, pkgName, branch):
	raise NotImplemented

    def getLatestPackage(self, pkgName, branch):
	raise NotImplemented

    def getPackageVersion(self, pkgName, version):
	raise NotImplemented

    def getPackageNickList(self, pkgName, nick):
	raise NotImplemented

    def getPackageVersionList(self, pkgName):
	raise NotImplemented

    def getPackageBranchList(self, pkgName):
	raise NotImplemented

    ### File functions

    def fileLatestVersion(self, fileId, branch):
	raise NotImplemented
	
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
