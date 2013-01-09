#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""
Implements branch and shadow command line functionality.
"""
import itertools

from conary import conaryclient
from conary import errors
from conary.cmds import updatecmd
from conary.lib import log
from conary.conaryclient import cmdline
from conary.conarycfg import selectSignatureKey

def _getBranchType(binaryOnly, sourceOnly):
    if binaryOnly and sourceOnly:
        raise errors.ParseError, ('Can only specify one of --binary-only and'
                                  ' --source-only')
    if binaryOnly:
        return conaryclient.ConaryClient.BRANCH_BINARY
    elif sourceOnly:
        return conaryclient.ConaryClient.BRANCH_SOURCE
    else:
        return conaryclient.ConaryClient.BRANCH_BINARY |        \
               conaryclient.ConaryClient.BRANCH_SOURCE

def displayBranchJob(cs, shadow=False):
    if shadow:
        branchOp = 'Shadow'
    else:
        branchOp = 'Branch'

    indent = '   '
    for csTrove in cs.iterNewTroveList():
        newInfo = str(csTrove.getNewVersion())
        flavor = csTrove.getNewFlavor()
        if flavor is not None:
            newInfo += '[%s]' % flavor

        print "%s%s  %-20s (%s)" % (indent, branchOp, csTrove.getName(),
                                        newInfo)


def branch(repos, cfg, newLabel, troveSpecs, makeShadow=False,
           sourceOnly=False, binaryOnly=False, allowEmptyShadow=False,
           info=False, forceBinary=False, ignoreConflicts=False,
           targetFile=None):
    branchType = _getBranchType(binaryOnly, sourceOnly)

    client = conaryclient.ConaryClient(cfg)

    troveSpecs = [ updatecmd.parseTroveSpec(x) for x in troveSpecs ]

    componentSpecs = [ x[0] for x in troveSpecs
                        if (':' in x[0] and x[0].split(':')[1] != 'source')]
    if componentSpecs:
        raise errors.ParseError('Cannot branch or shadow individual components: %s' % ', '.join(componentSpecs))

    result = repos.findTroves(cfg.buildLabel, troveSpecs, cfg.buildFlavor)
    troveList = [ x for x in itertools.chain(*result.itervalues())]

    sigKey = selectSignatureKey(cfg, newLabel)

    if makeShadow:
        dups, cs = client.createShadowChangeSet(newLabel, troveList,
                                                allowEmptyShadow=\
                                                    allowEmptyShadow,
                                                branchType=branchType,
                                                sigKeyId=sigKey)
    else:
        dups, cs = client.createBranchChangeSet(newLabel, troveList,
                                                branchType=branchType,
                                                sigKeyId = sigKey)

    for (name, branch) in dups:
        log.warning("%s already has branch %s", name, branch.asString())

    if not cs:
        return

    if makeShadow:
        branchOps = 'shadows'
    else:
        branchOps = 'branches'

    hasBinary = False
    for trvCs in cs.iterNewTroveList():
        if not trvCs.getName().endswith(':source'):
            hasBinary = True
            break

    if cfg.interactive or info:
        print 'The following %s will be created:' % branchOps
        displayBranchJob(cs, shadow=makeShadow)

    if cfg.interactive:
        print
        if hasBinary and branchType & client.BRANCH_BINARY:
            print 'WARNING: You have chosen to create binary %s. ' \
                  'This is not recommended\nwith this version of cvc.' \
                    % branchOps
            print
        okay = cmdline.askYn('Continue with %s? [y/N]' % branchOps.lower(),
                             default=False)
        if not okay:
            return
    elif (not forceBinary) and hasBinary and branchType & client.BRANCH_BINARY:
        print 'Creating binary %s is only allowed in interactive mode. ' \
              'Rerun cvc\nwith --interactive.' % branchOps
        return 1

    if not info:
        if targetFile:
            cs.writeToFile(targetFile)
        else:
            client.repos.commitChangeSet(cs)
