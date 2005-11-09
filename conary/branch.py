# -*- mode: python -*-
#
# Copyright (c) 2005 rPath, Inc.
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
import itertools

from conary import conaryclient
from conary import updatecmd
from conary import versions
from conary.lib import log
from conaryclient import cmdline

def _getBranchType(binaryOnly, sourceOnly):
    if binaryOnly and sourceOnly:
        raise OptionsError, ('Can only specify one of --binary-only and'
                             ' --source-only')
    if binaryOnly:
        return conaryclient.ConaryClient.BRANCH_BINARY_ONLY
    elif sourceOnly:
        return conaryclient.ConaryClient.BRANCH_SOURCE_ONLY
    else:
        return conaryclient.ConaryClient.BRANCH_ALL

def displayBranchJob(cs, shadow=False):
    if shadow:
        branchOp = 'Shadow'
    else:
        branchOp = 'Branch'

    indent = '   '
    for csTrove in cs.iterNewTroveList():
        newInfo = str(csTrove.getNewVersion())
        flavor = csTrove.getNewFlavor()
        if flavor:
            newInfo += '[%s]' % flavor

        print "%s%s  %-20s (%s)" % (indent, branchOp, csTrove.getName(),
                                        newInfo)
                                       

def branch(repos, cfg, newLabel, troveSpecs, makeShadow = False,
           sourceOnly = False, binaryOnly = False, info = False):
    branchType = _getBranchType(binaryOnly, sourceOnly)

    client = conaryclient.ConaryClient(cfg)

    troveSpecs = [ updatecmd.parseTroveSpec(x) for x in troveSpecs ]

    result = repos.findTroves(cfg.buildLabel, troveSpecs, cfg.buildFlavor)
    troveList = [ x for x in itertools.chain(*result.itervalues())]

    if makeShadow:
        dups, cs = client.createShadowChangeSet(newLabel, troveList, 
                                                branchType=branchType)
    else:
        dups, cs = client.createBranchChangeSet(newLabel, troveList, 
                                                branchType=branchType)

    for (name, branch) in dups:
        log.warning("%s already has branch %s", name, branch.asString())

    if not cs:
        return

    if cfg.interactive or info:
        if makeShadow:
            branchOps = 'shadows'
        else:
            branchOps = 'branches'

        print 'The following %s will be created:' % branchOps
        displayBranchJob(cs, shadow=makeShadow)

    if cfg.interactive:
        print
        okay = cmdline.askYn('continue with %s? [y/N]' % branchOps.lower(), 
                             default=False)
        if not okay: 
            return

    if not info:
        client.repos.commitChangeSet(cs)
