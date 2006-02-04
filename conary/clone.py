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
import itertools
import sys

from conary import versions
from conary.conaryclient import ConaryClient, cmdline
from conary.repository import netclient
from conary.build.cook import signAbsoluteChangeset
from conary.conarycfg import selectSignatureKey

def displayCloneJob(cs):
    
    indent = '   '
    for csTrove in cs.iterNewTroveList():
        newInfo = str(csTrove.getNewVersion())
        flavor = csTrove.getNewFlavor()
        if flavor:
            newInfo += '[%s]' % flavor

        print "%sClone  %-20s (%s)" % (indent, csTrove.getName(), newInfo)

def CloneTrove(cfg, targetBranch, troveSpecList, updateBuildInfo = True,
               info = False):
    client = ConaryClient(cfg)
    repos = client.getRepos()

    targetBranch = versions.VersionFromString(targetBranch)

    troveSpecs = [ cmdline.parseTroveSpec(x) for x in troveSpecList]
    cloneSources = repos.findTroves(cfg.installLabelPath, 
                                    troveSpecs, cfg.flavor)
    cloneSources = list(itertools.chain(*cloneSources.itervalues()))

    okay, cs = client.createCloneChangeSet(targetBranch, cloneSources,
                                           updateBuildInfo=updateBuildInfo)
    if not okay:
        return

    if cfg.interactive or info:
        print 'The following clones will be created:'
        displayCloneJob(cs)

    if cfg.interactive:
        print
        okay = cmdline.askYn('continue with clone? [y/N]', default=False)
        if not okay:
            return

    sigKey = selectSignatureKey(cfg, str(targetBranch.label()))
    signAbsoluteChangeset(cs, sigKey)

    if not info:
        client.repos.commitChangeSet(cs)
