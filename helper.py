#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Simple functions used throughout srs.
"""

import repository

def findPackage(repos, packageNamespace, defaultBranch, name, 
		versionStr = None, forceGroup = 0):
    """
    Looks up a package in the given repository based on the name and
    version provided. If any errors are occured, PackageNotFound is
    raised with an appropriate error message.

    @param repos: Repository to look for the package in
    @type repos: repository.Repository
    @param packageNamespace: Default namespace for the package
    @type packageNamespace: str
    @param defaultBranch: Default branch if just a version/release is given
    @type packageNamespace: versions.Version
    @param name: Package name
    @type name: str
    @param versionStr: Package version
    @type version: str
    @param forceGroup: If true the name should specify a group
    @type forceGroup: boolean
    @rtype: package.Package or None
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

    return pkg

class PackageNotFound(Exception):

    def __str__(self):
	return self.msg

    def __init__(self, str):
	self.msg = str
