#
# Copyright (c) 2004-2005 rPath, Inc.
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

from conary import conaryclient
from conary import versions
from conary.conaryclient import cmdline
from conary.lib import log
from conary.local import update
from conary.repository import errors
from conary.updatecmd import UpdateCallback

def ChangeSetCommand(cfg, troveSpecs, outFileName, recurse = True,
                     callback = None):
    client = conaryclient.ConaryClient(cfg)
    repos = client.getRepos()
    applyList = cmdline.parseChangeList(troveSpecs, allowChangeSets=False)

    toFind = []
    for (n, (oldVer, oldFla), (newVer, newFla), isAbs) in applyList:
        if n[0] in ('-', '+'):
            n = n[1:]

        found = False
        if oldVer or oldFla:
            toFind.append((n, oldVer,oldFla))
            found = True

        if newVer or newFla:
            toFind.append((n, newVer, newFla))
            found = True

        if not found:
            toFind.append((n, None, None))

    results = repos.findTroves(cfg.installLabelPath, toFind, cfg.flavor)

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
        if oldVer or oldFla:
            oldVer, oldFla = results[n, oldVer, oldFla][0][1:]
            found = True

        if newVer or newFla:
            newVer, newFla = results[n, newVer, newFla][0][1:]
            found = True

        if not found:
            if updateByDefault:
                newVer, newFla = results[n, None, None][0][1:]
            else:
                oldVer, oldFla = results[n, None, None][0][1:]

        primaryCsList.append((n, (oldVer, oldFla), (newVer, newFla), isAbs))

    client.createChangeSetFile(outFileName, primaryCsList, recurse = recurse, 
                               callback = callback, 
                               excludeList = cfg.excludeTroves)

def LocalChangeSetCommand(db, cfg, troveName, outFileName):
    try:
	troveList = db.trovesByName(troveName)
        troveList = db.getTroves(troveList)
    except errors.TroveNotFound, e:
	log.error(e)
	return

    list = []
    for outerTrove in troveList:
	for trove in db.walkTroveSet(outerTrove):
	    ver = trove.getVersion()
	    origTrove = db.getTrove(trove.getName(), ver, trove.getFlavor(), 
                                    pristine = True)
	    ver = ver.createBranch(versions.LocalLabel(), withVerRel = 1)
	    list.append((trove, origTrove, ver, 0))
	    
    result = update.buildLocalChanges(db, list, root = cfg.root,
                                      updateContainers = True)
    if not result: return
    cs = result[0]

    for outerTrove in troveList:
	cs.addPrimaryTrove(outerTrove.getName(), 
                           outerTrove.getVersion().createBranch(
            versions.LocalLabel(), withVerRel = 1),
                           outerTrove.getFlavor())

    for (changed, fsTrove) in result[1]:
	if changed:
	    break

    if not changed:
	log.error("there have been no local changes")
    else:
	cs.writeToFile(outFileName)
