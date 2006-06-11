#
# Copyright (c) 2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
import itertools

from conary import errors
from conary import versions
from conary.conaryclient import ConaryClient, cmdline
from conary.build.cook import signAbsoluteChangeset
from conary.conarycfg import selectSignatureKey
from conary.deps import deps

def displayCloneJob(cs):
    
    indent = '   '
    for csTrove in cs.iterNewTroveList():
        newInfo = str(csTrove.getNewVersion())
        flavor = csTrove.getNewFlavor()
        if not flavor.isEmpty():
            newInfo += '[%s]' % flavor

        print "%sClone  %-20s (%s)" % (indent, csTrove.getName(), newInfo)

def CloneTrove(cfg, targetBranch, troveSpecList, updateBuildInfo = True,
               info = False, cloneSources = False):
    client = ConaryClient(cfg)
    repos = client.getRepos()

    targetBranch = versions.VersionFromString(targetBranch)

    troveSpecs = [ cmdline.parseTroveSpec(x) for x in troveSpecList]

    componentSpecs = [ x[0] for x in troveSpecs 
                       if ':' in x[0] and x[0].split(':')[1] != 'source']
    if componentSpecs:
        raise errors.ParseError('Cannot clone components: %s' % ', '.join(componentSpecs))


    trovesToClone = repos.findTroves(cfg.installLabelPath, 
                                    troveSpecs, cfg.flavor)
    trovesToClone = list(itertools.chain(*trovesToClone.itervalues()))

    if cloneSources:
        binaries = [ x for x in trovesToClone if not x[0].endswith(':source')]
        seen = set(binaries)
        while binaries:
            troves = repos.getTroves(binaries, withFiles=False)
            binaries = []
            for trove in troves:
                trovesToClone.append((trove.getSourceName(),
                                      trove.getVersion().getSourceVersion(),
                                      deps.Flavor()))
                for troveTup in trove.iterTroveList(strongRefs=True,
                                                    weakRefs=True):
                    if troveTup not in seen:
                        binaries.append(troveTup)
            seen.update(binaries)

        trovesToClone = list(set(trovesToClone))

    okay, cs = client.createCloneChangeSet(targetBranch, trovesToClone,
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
