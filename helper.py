#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Simple functions used throughout srs.
"""

import log
import repository
import repository.fsrepos
import repository.netclient
import versions
import sys

def openRepository(path, mode):
    if path.startswith("http://"):
        repos = repository.netclient.NetworkRepositoryClient(path)
    else:
        repos = repository.fsrepos.FilesystemRepository(path, mode)

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
