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

from repository import netclient
from conaryclient import ConaryClient, cmdline
import updatecmd
import versions
import sys

def displayCloneJob(cs):
    
    indent = '   '
    for csTrove in cs.iterNewTroveList():
        newInfo = str(csTrove.getNewVersion())
        flavor = csTrove.getNewFlavor()
        if flavor:
            newInfo += '[%s]' % flavor

        print "%sClone  %-20s (%s)" % (indent, csTrove.getName(), newInfo)

def CloneTrove(cfg, targetBranch, troveSpecList):

    targetBranch = versions.VersionFromString(targetBranch)
    repos = netclient.NetworkRepositoryClient(cfg.repositoryMap)

    cloneSources = []

    for troveSpec in troveSpecList:
        parts = troveSpec.split('=', 1) 

        spec = updatecmd.parseTroveSpec(troveSpec)
        cloneSources += repos.findTrove(cfg.installLabelPath, spec)

    client = ConaryClient(cfg)
    okay, cs = client.createCloneJob(targetBranch, cloneSources)
    if not okay:
        return

    if cfg.interactive:
        print 'The following clones will be created:'
        displayCloneJob(cs)
        print
        okay = cmdline.askYn('continue with clone? [y/N]', default=False)
        if not okay:
            return

    client.repos.commitChangeSet(cs)
