#
# Copyright (c) 2004-2005 rpath, Inc.
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
from lib import log
from local import update
from repository import repository
from updatecmd import parseTroveSpec
from updatecmd import UpdateCallback
import versions

def ChangeSetCommand(repos, cfg, troveList, outFileName, recurse = True,
                     callback = None):
    client = conaryclient.ConaryClient(cfg)

    primaryCsList = []

    for item in troveList:
        l = item.split("--")

        if len(l) == 1:
            l = [''] + l
        elif len(l) != 2:
            log.error("one = expected in '%s' argument to changeset", item)
            return
        if l[0]:
            (troveName, oldVersionStr, oldFlavor) = parseTroveSpec(l[0])
        else:
            oldVersionStr = None
            oldFlavor = None
        if l[1]:
            if l[0]:
                l[1] = troveName + "=" + l[1]
            (troveName, newVersionStr, newFlavor) = parseTroveSpec(l[1])
        else:
            newVersionStr = None
            newFlavor = None

        if l[0]:
            troveList = repos.findTrove(cfg.installLabelPath, 
                                        (troveName, oldVersionStr, oldFlavor),
                                        cfg.flavor)
            if len(troveList) > 1:
                log.error("trove %s has multiple branches named %s",
                          troveName, oldVersionStr)

            oldVersion = troveList[0][1]
            oldFlavor = troveList[0][2]
        else:
            oldVersion = None

        if l[1]:
            troveList = repos.findTrove(cfg.installLabelPath, 
                                        (troveName, newVersionStr, newFlavor),
                                        cfg.flavor)
            if len(troveList) > 1:
                if newVersionStr:
                    log.error("trove %s has multiple branches named %s",
                              troveName, newVersionStr)
                else:
                    log.error("trove %s has multiple matches on installLabelPath",
                              troveName)

            newVersion = troveList[0][1]
            newFlavor = troveList[0][2]
        else:
            newVersion = None

        primaryCsList.append((troveName, (oldVersion, oldFlavor), 
                                         (newVersion, newFlavor),
                              not oldVersion))
        
    client.createChangeSetFile(outFileName, primaryCsList, recurse = recurse, 
                               callback = callback, 
                               excludeList = cfg.excludeTroves)

def LocalChangeSetCommand(db, cfg, troveName, outFileName):
    try:
	troveList = db.findTrove(None, troveName, None)
        troveList = db.getTroves(troveList)
    except repository.TroveNotFound, e:
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
