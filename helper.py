#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Simple functions used throughout srs.
"""

import repository
import versions

def findPackage(repos, packageNamespace, defaultBranch, name, 
		versionStr = None, forceGroup = 0, oneMatch = 1):
    """
    Looks up a package in the given repository based on the name and
    version provided. If any errors are occured, PackageNotFound is
    raised with an appropriate error message. Multiple matches could
    be found if versionStr refers to a branch nickname.

    @param repos: Repository to look for the package in
    @type repos: repository.Repository
    @param packageNamespace: Default namespace for the package
    @type packageNamespace: str
    @param defaultBranch: Default branch if just a version/release is given
    @type defaultBranch: versions.Version
    @param name: Package name
    @type name: str
    @param versionStr: Package version
    @type versionStr: str
    @param forceGroup: If true the name should specify a group
    @type forceGroup: boolean
    @rtype: list of package.Package
    """

    if name[0] != ":":
	name = packageNamespace + ":" + name
    else:
	name = name

    if forceGroup:
	if name.count(":") != 2:
	    raise PackageNotFound, "group names may not include colons"

	last = name.split(":")[-1]
	if not last.startswith("group-"):
	    raise PackageNotFound,  \
		    "only groups may be checked out of the repository"

    # a version is a branch nickname if
    #   1. it exists
    #   2. it doesn't being with / (it isn't fully qualified)
    #   3. it only has one element (no /)
    #   4. it contains an @ sign
    if versionStr and versionStr[0] != "/" and  \
	    (versionStr.find("/") == -1) and versionStr.count("@"):
	try:
	    nick = versions.BranchName(versionStr)
	except versions.ParseError:
	    raise PackageMissing, "invalid version %s" % versionStr

	branchList = repos.getPackageNickList(name, nick)
	if not branchList:
	    raise PackageMissing, "branch %s does not exist for package %s" \
			% (str(nick), name)

	pkgList = []
	for branch in branchList:
	    pkgList.append(repos.getLatestPackage(name, branch))
    else:
	if (not versionStr or versionStr[0] != "/") and (not defaultBranch):
	    if not defaultBranch:
		raise PackageNotFound, \
		    "fully qualified version or branch nickname expected"

	if not versionStr:
	    version = defaultBranch
	else:
	    if versionStr[0] != "/":
		versionStr = defaultBranch.asString() + "/" + versionStr

	    version = versions.VersionFromString(versionStr)

	try:
	    if version.isBranch():
		pkg = repos.getLatestPackage(name, version)
	    else:
		pkg = repos.getPackageVersion(name, version)
	except repository.PackageMissing, e:  
	    raise PackageNotFound, str(e)

	pkgList = [ pkg ]

    return pkgList

class PackageNotFound(Exception):

    def __str__(self):
	return self.msg

    def __init__(self, str):
	self.msg = str
