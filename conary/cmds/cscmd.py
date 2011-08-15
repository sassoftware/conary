#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
