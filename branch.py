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

def branch(repos, branchName, branchFrom, troveName = None):
    try:
	newBranch = versions.BranchName(branchName)

	if branchFrom[0] == "/":
	    branchSource = versions.VersionFromString(branchFrom)
	else:
	    branchSource = versions.BranchName(branchFrom)
    except versions.ParseError, e:
	log.error(str(e))
	return

    repos.createBranch(newBranch, branchSource, troveName)
	    

