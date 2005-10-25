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
from lib import log
from local import update
from repository import errors
from updatecmd import parseTroveSpec
from updatecmd import UpdateCallback
import versions

def ChangeSetCommand(repos, cfg, troveList, outFileName, recurse = True,
                     callback = None):
    def _findTrove(troveName, versionStr, flavorStr):
        troveList = repos.findTrove(cfg.installLabelPath, 
                                    (troveName, versionStr, flavorStr),
                                    cfg.flavor)
        if len(troveList) > 1:
            log.error("trove %s has multiple matches on "
                      "installLabelPath", troveName)

        return (troveList[0][1], troveList[0][2])

    client = conaryclient.ConaryClient(cfg)

    primaryCsList = []

    for item in troveList:
        l = item.split("--")
        isAbstract = False
        add = True

        if len(l) == 1:
            (troveName, versionStr, flavor) = parseTroveSpec(l[0])

            if troveName[0] == '-':
                troveName = troveName[1:]
                oldVersion, oldFlavor = _findTrove(troveName, versionStr,
                                                   flavor)
                newVersion, newFlavor = None, None
            else:
                isAbstract = True
                if troveName[0] == '+':
                    troveName = troveName[1:]
                oldVersion, oldFlavor = None, None
                newVersion, newFlavor = _findTrove(troveName, versionStr,
                                                   flavor)
        elif len(l) != 2:
            log.error("one = expected in '%s' argument to changeset", item)
            return
        else:
            (troveName, oldVersionStr, oldFlavor) = parseTroveSpec(l[0])

            if l[1]:
                if l[0]:
                    l[1] = troveName + "=" + l[1]
                (troveName, newVersionStr, newFlavor) = parseTroveSpec(l[1])
                newVersion, newFlavor = _findTrove(troveName, newVersionStr, 
                                                   newFlavor)
            else:
                newVersion, newFlavor = None, None

            if newVersion and not oldVersionStr:
                # foo=--1.2
                oldVersion, oldFlavor = None, None
            else:
                # foo=1.1--1.2
                oldVersion, oldFlavor = _findTrove(troveName, oldVersionStr, 
                                                   oldFlavor)

        primaryCsList.append((troveName, (oldVersion, oldFlavor), 
                                         (newVersion, newFlavor),
                              isAbstract))

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
