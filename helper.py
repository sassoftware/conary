#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Simple functions used throughout srs.
"""

from repository import repository
import versions

def findPackage(repos, defaultLabel, name, versionStr = None, forceGroup = 0):
    """
    Looks up a package in the given repository based on the name and
    version provided. If any errors are occured, PackageNotFound is
    raised with an appropriate error message. Multiple matches could
    be found if versionStr refers to a label.

    @param repos: Repository to look for the package in
    @type repos: repository.Repository
    @param defaultLabel: Label of the branch to use if no branch
    is specified. If only a branch name is given (not a complete label),
    the repository name from this label is used as the repository
    name for the branch name to form a complete label.
    @type defaultLabel: versions.BranchName
    @param name: Package name
    @type name: str
    @param versionStr: Package version
    @type versionStr: str
    @param forceGroup: If true the name should specify a group
    @type forceGroup: boolean
    @rtype: list of package.Package
    """

    if not repos.hasPackage(name):
	raise PackageNotFound, "package %s does not exist" % name

    if forceGroup:
	if name.count(":") != 2:
	    raise PackageNotFound, "group and fileset names may not include colons"

	last = name.split(":")[-1]
	if not last.startswith("group-") and not last.startswith("fileset-"):
	    raise PackageNotFound,  \
		    "only groups and filesets may be checked out of the repository"

    if not defaultLabel:
	if versionStr[0] != "/" and (versionStr.find("/") != -1 or
				     versionStr.find("@") == -1):
	    raise PackageNotFound, \
		"fully qualified version or label " + \
		"expected instead of %s" % versionStr

    # a version is a label if
    #   1. it doesn't being with / (it isn't fully qualified)
    #   2. it only has one element (no /)
    #   3. it contains an @ sign
    if not versionStr or (versionStr[0] != "/" and  \
	# label was given
	    (versionStr.find("/") == -1) and versionStr.count("@")):

	if versionStr:
	    if versionStr[0] == "@" and defaultLabel:
		versionStr = defaultLabel.getHost() + versionStr

	    try:
		label = versions.BranchName(versionStr)
	    except versions.ParseError:
		raise repository.PackageMissing, "invalid version %s" % versionStr
	else:
	    label = defaultLabel

	branchList = repos.branchesOfTroveLabel(name, label)
	if not branchList:
	    raise PackageNotFound, "branch %s does not exist for package %s" \
			% (str(label), name)

	pkgList = []
	for branch in branchList:
	    pkgList.append(repos.getLatestPackage(name, branch))
    elif versionStr[0] != "/" and versionStr.find("/") == -1:
	# version/release was given
	branchList = repos.branchesOfTroveLabel(name, defaultLabel)
	if not branchList:
	    raise PackageNotFound, \
			"branch %s does not exist for package %s" \
			% (str(defaultLabel), name)
	
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
		(versionStr, name, str(defaultLabel))
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

def fullBranchName(defaultLabel, version, versionStr):
    """
    Converts a version string, and the version the string refers to
    (often returned by findPackage()) into the full branch name the
    node is on. This is different from version.branch() when versionStr
    refers to the head of an empty branch, in which case version() will
    be the version the branch was forked from rather then a version on
    that branch.

    @param defaultLabel: default label we're on if versionStr is None
    (may be none if versionStr is not None)
    @type defaultLabel: versions.BranchName
    @param version: version of the node versionStr resolved to
    @type version: versions.Version
    @param versionStr: string from the user; likely a very abbreviated version
    @type versionStr: str
    """
    if not versionStr or (versionStr[0] != "/" and  \
	# label was given
	    (versionStr.find("/") == -1) and versionStr.count("@")):
	if not versionStr:
	    label = defaultLabel
	elif versionStr[0] == "@":
            label = versions.BranchName(defaultLabel.getHost() + versionStr)
	else:
	    label = versions.BranchName(versionStr)

	if version.branch().label() == label:
	    return version.branch()
	else:
	    # this must be the node the branch was created at, otherwise
	    # we'd be on it
	    return version.fork(label, sameVerRel = 0)
    elif version.isBranch():
	return version
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
    @param currentBranch: branch the new version should be on
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
         currentBranch == currentVersion.branch():
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
