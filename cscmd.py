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

import conaryclient
from conaryclient import cmdline
from lib import log
from local import update
from repository import errors
from updatecmd import UpdateCallback
import versions

def ChangeSetCommand(repos, cfg, troveSpecs, outFileName, recurse = True,
                     callback = None):
    client = conaryclient.ConaryClient(cfg)
    applyList = cmdline.parseChangeList(troveSpecs, repos, repos)

    toFind = []
    for (n, (oldVer, oldFla), (newVer, newFla), isAbs) in applyList:
        if oldVer is not None:
            toFind.append((n, oldVer,oldFla))
        if newVer is not None:
            toFind.append((n, newVer, newFla))

    results = repos.findTroves(cfg.installLabelPath, toFind, cfg.flavor)

    for troveSpec, trovesFound in results.iteritems():
        if len(trovesFound) > 1:
            log.error("trove %s has multiple matches on "
                      "installLabelPath", troveSpec[0])
            
    primaryCsList = []

    for (n, (oldVer, oldFla), (newVer, newFla), isAbs) in applyList:
        if oldVer is not None:
            oldVer, oldFla = results[n, oldVer, oldFla][0][1:]
        if newVer is not None:
            newVer, newFla = results[n, newVer, newFla][0][1:]
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
