#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

from local import update
from repository import repository
import log
import package
import versions

def ChangeSetCommand(repos, cfg, troveName, outFileName, oldVersionStr, \
	      newVersionStr):
    pkgList = repos.findTrove(cfg.installLabel, troveName, cfg.flavor,
			      newVersionStr)
    if len(pkgList) > 1:
	log.error("trove %s has multiple branches named %s",
		  troveName, newVersionStr)

    newVersion = pkgList[0].getVersion()

    if (oldVersionStr):
	pkgList = repos.findTrove(cfg.installLabel, troveName, pkgList[0].getFlavor(),
				  oldVersionStr)
	if len(pkgList) > 1:
	    log.error("trove %s has multiple branches named %s",
		      troveName, oldVersionStr)

	oldVersion = pkgList[0].getVersion()

    else:
	oldVersion = None

    list = [(troveName, pkgList[0].getFlavor(), oldVersion, newVersion, (not oldVersion))]

    cs = repos.createChangeSet(list)
    cs.writeToFile(outFileName)

def LocalChangeSetCommand(db, cfg, pkgName, outFileName):
    try:
	pkgList = db.findTrove(pkgName, None)
    except repository.PackageNotFound, e:
	log.error(e)
	return

    list = []
    for outerPackage in pkgList:
	for pkg in db.walkTroveSet(outerPackage):
	    ver = pkg.getVersion()
	    origPkg = db.getTrove(pkg.getName(), ver, pkg.getFlavor(), 
				  pristine = True)
	    ver = ver.fork(versions.LocalBranch(), sameVerRel = 1)
	    list.append((pkg, origPkg, ver))
	    
    result = update.buildLocalChanges(db, list, root = cfg.root)
    if not result: return
    cs = result[0]

    for outerPackage in pkgList:
	cs.addPrimaryPackage(outerPackage.getName(), 
	    outerPackage.getVersion().fork(
		versions.LocalBranch(), sameVerRel = 1),
	   outerPackage.getFlavor())

    for (changed, fsPkg) in result[1]:
	if changed:
	    break

    if not changed:
	log.error("there have been no local changes")
    else:
	cs.writeToFile(outFileName)
