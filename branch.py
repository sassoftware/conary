# -*- mode: python -*-
#
# Copyright (c) 2004 Specifix, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
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

    repos.createBranch(newBranch, branchSource, [troveName])
	    

