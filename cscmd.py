#
# Copyright (c) 2004-2005 Specifix, Inc.
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

from lib import log
from local import update
from repository import repository
from updatecmd import parseTroveSpec
from updatecmd import UpdateCallback
import versions

def ChangeSetCommand(repos, cfg, troveList, outFileName, recurse = True,
                     callback = None):
    primaryCsList = []
    primaryList = []

    for item in troveList:
        l = item.split("--")

        if len(l) == 1:
            l = [''] + l
        elif len(l) != 2:
            log.error("one = expected in '%s' argument to changeset", item)
            return
        if l[0]:
            (troveName, oldVersionStr, oldFlavor) = parseTroveSpec(l[0],
                                                        cfg.flavor)
        else:
            oldVersionStr = None
            oldFlavor = None
        if l[1]:
            if l[0]:
                l[1] = troveName + "=" + l[1]
            (troveName, newVersionStr, newFlavor) = parseTroveSpec(l[1],
                                                        cfg.flavor)
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
        if newVersion:
            primaryList.append((troveName, newVersion, newFlavor))
        else:
            primaryList.append((troveName, oldVersion, oldFlavor))


    cs = repos.createChangeSet(primaryCsList, recurse = recurse, 
                               withFiles = False)

    # filter out non-defaults
    for (name, (oldVersion, oldFlavor), (newVersion, newFlavor), abstract) \
                                                            in primaryCsList:
        if not newVersion:
            # cannot have a non-default erase
            continue

        primaryTroveCs = cs.getNewPackageVersion(name, newVersion, newFlavor)

        for (name, changeList) in primaryTroveCs.iterChangedTroves():
            for (changeType, version, flavor, byDef) in changeList:
                if changeType == '+' and not byDef and \
                   (name, version, flavor) not in primaryList:
                    # it won't be here if recurse is False
                    if cs.hasNewPackage(name, version, flavor):
                        cs.delNewPackage(name, version, flavor)
        
    # now filter the excludeTroves list
    fullCsList = []
    for troveCs in cs.iterNewPackageList():
        name = troveCs.getName()
        newVersion = troveCs.getNewVersion()
        newFlavor = troveCs.getNewFlavor()

        skip = False

        # troves explicitly listed should never be excluded
        if (name, newVersion, newFlavor) not in primaryList:
            for reStr, regExp in cfg.excludeTroves:
                if regExp.match(name):
                    skip = True
        
    
        if not skip:
            fullCsList.append((name, 
                       (troveCs.getOldVersion(), troveCs.getOldFlavor()),
                       (newVersion,              newFlavor),
                   not troveCs.getOldVersion()))

    # exclude packages that are being erased as well
    for (name, oldVersion, oldFlavor) in cs.getOldPackageList():
        skip = False
        if (name, oldVersion, oldFlavor) not in primaryList:
            for reStr, regExp in cfg.excludeTroves:
                if regExp.match(name):
                    skip = True
        if not skip:
            fullCsList.append((name, 
                       (oldVersion, oldFlavor),
                       (None, None), True))

    # recreate primaryList without erase-only troves for the primary trove list
    primaryList = [ (x[0], x[2][0], x[2][1]) for x in primaryCsList \
                                                if x[2][0] is not None ]
    repos.createChangeSetFile(fullCsList, outFileName, recurse = False,
                              primaryTroveList = primaryList,
                              callback = callback)

def LocalChangeSetCommand(db, cfg, pkgName, outFileName):
    try:
	pkgList = db.findTrove(None, pkgName, None)
        pkgList = db.getTroves(pkgList)
    except repository.TroveNotFound, e:
	log.error(e)
	return

    list = []
    for outerPackage in pkgList:
	for pkg in db.walkTroveSet(outerPackage):
	    ver = pkg.getVersion()
	    origPkg = db.getTrove(pkg.getName(), ver, pkg.getFlavor(), 
				  pristine = True)
	    ver = ver.createBranch(versions.LocalLabel(), withVerRel = 1)
	    list.append((pkg, origPkg, ver, 0))
	    
    result = update.buildLocalChanges(db, list, root = cfg.root)
    if not result: return
    cs = result[0]

    for outerPackage in pkgList:
	cs.addPrimaryTrove(outerPackage.getName(), 
	    outerPackage.getVersion().createBranch(
		versions.LocalLabel(), withVerRel = 1),
	   outerPackage.getFlavor())

    for (changed, fsPkg) in result[1]:
	if changed:
	    break

    if not changed:
	log.error("there have been no local changes")
    else:
	cs.writeToFile(outFileName)
