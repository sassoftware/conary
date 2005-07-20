# -*- mode: python -*-
#
# Copyright (c) 2005 rpath, Inc.
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
Implements branch and shadow command line functionality.
"""

import conaryclient
import versions
from lib import log
import updatecmd

def branch(repos, cfg, newLabel, troveSpec, makeShadow = False,
           sourceTroves = False):
    client = conaryclient.ConaryClient(cfg)

    (troveName, versionSpec, flavor) = updatecmd.parseTroveSpec(troveSpec)

    troveList = repos.findTrove(cfg.buildLabel, 
                                (troveName, versionSpec, flavor), 
                                cfg.buildFlavor)

    if makeShadow:
        dups = client.createShadow(newLabel, troveList,
                                   sourceTroves = sourceTroves)
    else:
        dups = client.createBranch(newLabel, troveList,
                                   sourceTroves = sourceTroves)

    for (name, branch) in dups:
        log.warning("%s already has branch %s", name, branch.asString())
