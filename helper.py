#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Simple functions used throughout srs.
"""

import repository
import versions

def findPackage(repos, packageNamespace, defaultNick, name, 
		versionStr = None, forceGroup = 0):
    """
    Looks up a package in the given repository based on the name and
    version provided. If any errors are occured, PackageNotFound is
    raised with an appropriate error message. Multiple matches could
    be found if versionStr refers to a branch nickname.

    @param repos: Repository to look for the package in
    @type repos: repository.Repository
    @param packageNamespace: Default namespace for the package
    @type packageNamespace: str
    @param defaultNick: Nickname of the branch to use if no branch
    is specified
    @type defaultNick: versions.BranchName
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

    if not repos.hasPackage(name):
	raise PackageNotFound, "package %s does not exist" % name

    if forceGroup:
	if name.count(":") != 2:
	    raise PackageNotFound, "group names may not include colons"

	last = name.split(":")[-1]
	if not last.startswith("group-"):
	    raise PackageNotFound,  \
		    "only groups may be checked out of the repository"

    if not defaultNick:
	if versionStr[0] != "/" and (versionStr.find("/") != -1 or
				     versionStr.find("@") == -1):
	    raise PackageNotFound, \
		"fully qualified version or branch nickname " + \
		"expected instead of %s" % versionStr

    # a version is a branch nickname if
    #   1. it doesn't being with / (it isn't fully qualified)
    #   2. it only has one element (no /)
    #   3. it contains an @ sign
    if not versionStr or (versionStr[0] != "/" and  \
	# branch nickname was given
	    (versionStr.find("/") == -1) and versionStr.count("@")):

	if versionStr:
	    if versionStr[0] == "@":
		versionStr = packageNamespace[1:] + versionStr

	    try:
		nick = versions.BranchName(versionStr)
	    except versions.ParseError:
		raise repository.PackageMissing, "invalid version %s" % versionStr
	else:
	    nick = defaultNick

	branchList = repos.getPackageNickList(name, nick)
	if not branchList:
	    raise PackageNotFound, "branch %s does not exist for package %s" \
			% (str(nick), name)

	pkgList = []
	for branch in branchList:
	    pkgList.append(repos.getLatestPackage(name, branch))
    elif versionStr[0] != "/" and versionStr.find("/") == -1:
	# version/release was given
	branchList = repos.getPackageNickList(name, defaultNick)
	if not branchList:
	    raise PackageNotFound, \
			"branch %s does not exist for package %s" \
			% (str(defaultNick), name)
	
	try:
	    verRel = versions.VersionRelease(versionStr)
	except versions.ParseError, e:
	    raise PackageNotFound, str(e)

	pkgList = []
	for branch in branchList:
	    version = branch.copy()
	    version.appendVersionReleaseObject(verRel)
	    try:
		pkg = repos.getPackageVersion(name, version)
		pkgList.append(pkg)
	    except repository.PackageMissing, e:
		pass

	if not pkgList:
	    raise PackageNotFound, \
		"version %s of %s is not on any branch named %s" % \
		(versionStr, name, str(defaultNick))
    elif versionStr[0] != "/":
	# partial version string, we don't support this
	raise PackageNotFound, \
	    "incomplete version string %s not allowed" % versionStr
    else:
	try:
	    version = versions.VersionFromString(versionStr)
	except versions.ParseError:
	    raise PackageNotFound, str(e)

	try:
	    if version.isBranch():
		pkg = repos.getLatestPackage(name, version)
	    else:
		pkg = repos.getPackageVersion(name, version)
	except repository.PackageMissing, e:  
	    raise PackageNotFound, str(e)

	pkgList = [ pkg ]

    return pkgList

def fullBranchName(nameSpace, defaultNick, version, versionStr):
    """
    Converts a version string, and the version the string refers to
    (often returned by findPackage()) into the full branch name the
    node is on. This is different from version.branch() when versionStr
    refers to the head of an empty branch, in which case version() will
    be the version the branch was forked from rather then a version on
    that branch.

    @param nameSpace: repository branches are on when versionStr begins 
    with @ (may be none if versionStr doesn't begin with an @)
    @type nameSpace: str
    @param defaultNick: branch nickname we're on if versionStr is None
    (may be none if versionStr is not None)
    @type defaultNick: versions.BranchName
    @param version: version of the node versionStr resolved to
    @type version: versions.Version
    @param versionStr: string from the user; likely a very abbreviated version
    @type versionStr: str
    """
    if not versionStr or (versionStr[0] != "/" and  \
	# branch nickname was given
	    (versionStr.find("/") == -1) and versionStr.count("@")):
	if not versionStr:
	    nick = defaultNick
	elif versionStr[0] == "@":
	    nick = versions.BranchName(nameSpace, versionStr)
	else:
	    nick = versions.BranchName(versionStr)

	if version.branch().branchNickname().equal(nick):
	    return version.branch()
	else:
	    # this must be the node the branch was created at, otherwise
	    # we'd be on it
	    return version.fork(nick, sameVerRel = 0)
    elif version.isBranch():
	return version
	state.setTroveBranch(version)
    else:
	return version.branch()

def nextVersion(versionStr, currentVersion, currentBranch, binary = True):
    """
    Calculates the version to use for a newly built item which is about
    to be added to the repository.

    @param versionStr: version string from the recipe
    @type versionStr: string
    @param currentVersion: version of current head
    @type currentVersion: versions.Version
    @parm currentBranch: branch the new version should be on
    @type currentBranch: versions.Version
    @param binary: true if this version should use the binary build field
    @type binary: boolean
    """
    if not currentVersion:
	# new package
	newVersion = currentBranch.copy()
	newVersion.appendVersionRelease(versionStr, 1)
	if binary:
	    newVersion.incrementBuildCount()
    elif currentVersion.trailingVersion().getVersion() == versionStr and \
         currentBranch.equal(currentVersion.branch()):
	newVersion = currentVersion.copy()
	if binary:
	    newVersion.incrementBuildCount()
	else:
	    newVersion.incrementRelease()
    else:
	newVersion = currentBranch.copy()
	newVersion.appendVersionRelease(versionStr, 1)
	if binary:
	    newVersion.incrementBuildCount()

    return newVersion

class PackageNotFound(Exception):

    def __str__(self):
	return self.msg

    def __init__(self, str):
	self.msg = str
