# -*- mode: python -*-
#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
"""
Implements branch command line functionality.
"""

import versions
import log

def branch(repos, packageNamespace, branchName, branchFrom, troveName = None):
    if troveName and troveName[0] != ":":
	 troveName = packageNamespace + ":" + troveName

    if branchName[0] == "@":
	branchName = packageNamespace[1:] + branchName

    try:
	newBranch = versions.BranchName(branchName)

	if branchFrom[0] == "/":
	    branchSource = versions.VersionFromString(branchFrom)
	else:
	    if branchFrom[0] == "@":
		branchFrom = packageNamespace[1:] + branchFrom
	    branchSource = versions.BranchName(branchFrom)
    except versions.ParseError, e:
	log.error(str(e))
	return

    repos.createBranch(newBranch, branchSource, troveName)
	    

