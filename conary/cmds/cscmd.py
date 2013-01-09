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


from conary import conaryclient
from conary.conaryclient import cmdline
from conary.lib import log

def computeTroveList(client, applyList):
    # As dumb as this may sound, the same trove may be present multiple times
    # in applyList, so remove duplicates
    toFind = set()
    for (n, (oldVer, oldFla), (newVer, newFla), isAbs) in applyList:
        if n[0] in ('-', '+'):
            n = n[1:]

        found = False
        if oldVer or (oldFla is not None):
            toFind.add((n, oldVer,oldFla))
            found = True

        if newVer or (newFla is not None):
            toFind.add((n, newVer, newFla))
            found = True

        if not found:
            toFind.add((n, None, None))

    repos = client.getRepos()
    results = repos.findTroves(client.cfg.installLabelPath, toFind,
                               client.cfg.flavor)

    for troveSpec, trovesFound in results.iteritems():
        if len(trovesFound) > 1:
            log.error("trove %s has multiple matches on "
                      "installLabelPath", troveSpec[0])

    primaryCsList = []

    for (n, (oldVer, oldFla), (newVer, newFla), isAbs) in applyList:
        if n[0] == '-':
            updateByDefault = False
        else:
            updateByDefault = True

        if n[0] in ('-', '+'):
            n = n[1:]

        found = False
        if oldVer or (oldFla is not None):
            oldVer, oldFla = results[n, oldVer, oldFla][0][1:]
            found = True

        if newVer or (newFla is not None):
            newVer, newFla = results[n, newVer, newFla][0][1:]
            found = True

        if not found:
            if updateByDefault:
                newVer, newFla = results[n, None, None][0][1:]
            else:
                oldVer, oldFla = results[n, None, None][0][1:]

        primaryCsList.append((n, (oldVer, oldFla), (newVer, newFla), isAbs))

    return primaryCsList

def ChangeSetCommand(cfg, troveSpecs, outFileName, recurse = True,
                     callback = None):
    client = conaryclient.ConaryClient(cfg)
    applyList = cmdline.parseChangeList(troveSpecs, allowChangeSets=False)

    primaryCsList = computeTroveList(client, applyList)

    client.createChangeSetFile(outFileName, primaryCsList, recurse = recurse,
                               callback = callback,
                               excludeList = cfg.excludeTroves)
