#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Simple functions used throughout srs.
"""

import repository
import repository.fsrepos
import repository.netclient
import versions

def openRepository(path):
    if path.startswith("http://"):
        repos = repository.netclient.NetworkRepositoryClient(path)
    else:
        repos = repository.fsrepos.FilesystemRepository(path)

    return repos

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

def nextVersion(repos, troveName, versionStr, troveFlavor, currentBranch, 
		binary = True):
    """
    Calculates the version to use for a newly built trove which is about
    to be added to the repository.

    @param repos: repository the trove will be part of
    @type repos: repository.AbstractRepository
    @param troveName: name of the trove being built
    @type troveName: str
    @param versionStr: version string from the recipe
    @type versionStr: string
    @param troveFlavor: flavor of the trove being built
    @type troveFlavor: deps.deps.DependencySet
    @param currentBranch: branch the new version should be on
    @type currentBranch: versions.Version
    @param binary: true if this version should use the binary build field
    @type binary: boolean
    """
    currentVersions = repos.getTroveFlavorsLatestVersion(troveName, 
							 currentBranch)

    if not troveFlavor:
        troveFlavor = None
    # find the latest version of this trove and the latest version of
    # this flavor of this trove
    latestForFlavor = None
    latest = None
    # this works because currentVersions is sorted earliest to latest
    for (version, flavor) in currentVersions:
	if flavor == troveFlavor:
	    latestForFlavor = version
	latest = version

    if not latest:
	# new package.
	newVersion = currentBranch.copy()
	newVersion.appendVersionRelease(versionStr, 1)
	if binary:
	    newVersion.incrementBuildCount()
    elif latestForFlavor != latest and \
		latest.trailingVersion().getVersion() == versionStr:
	# catching up to head
	newVersion = latest
    elif latest.trailingVersion().getVersion() == versionStr:
	# new build of existing version
	newVersion = latest.copy()
	if binary:
	    newVersion.incrementBuildCount()
	else:
	    newVersion.incrementRelease()
    else:
	# new version
	newVersion = currentBranch.copy()
	newVersion.appendVersionRelease(versionStr, 1)
	if binary:
	    newVersion.incrementBuildCount()

    return newVersion

def previousVersion(repos, troveName, troveVersion, troveFlavor):
    """
    Returns the trove version which will be outdated by installing
    the specified trove. If none will be outdated, None is returned.
    If we can't tell which version will be outdated, AmbiguousOperation
    is raised.

    @type repos: repository.Repository
    @type troveName: str
    @type troveVersion: versions.Version
    @type troveFlavor: deps.deps.DependencySet
    @rtype: versions.Version or None
    """

    oldVersion = None
    oldVersions = repos.getTroveVersionList(troveName)
    if len(oldVersions) > 1:
	# try and pick the one which looks like a good match
	# for the new version
	newBranch = troveVersion.branch()
	for ver in oldVersions:
	    if ver.branch() == newBranch:
		# make sure it's the right flavor
		flavors = repos.pkgVersionFlavors(troveName, ver)
		if troveFlavor in flavors:
		    oldVersion = ver
		    break

	if not oldVersion:
	    raise AmbiguousOperation
    elif oldVersions:
	# make sure it's the right flavor
	flavors = repos.pkgVersionFlavors(troveName, oldVersions[0])
	if troveFlavor in flavors:
	    oldVersion = oldVersions[0]

    return oldVersion

class PackageNotFound(Exception):

    def __str__(self):
	return self.msg

    def __init__(self, str):
	self.msg = str

class AmbiguousOperation(Exception):

    pass
