#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

from local import update
from repository import repository
import log
import package
import versions

def ChangeSetCommand(repos, cfg, pkgName, outFileName, oldVersionStr, \
	      newVersionStr):
    newVersion = versions.VersionFromString(newVersionStr, cfg.defaultbranch)

    if (oldVersionStr):
	oldVersion = versions.VersionFromString(oldVersionStr, 
					        cfg.defaultbranch)
    else:
	oldVersion = None

    list = [(pkgName, None, oldVersion, newVersion, (not oldVersion))]

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
	for pkg in package.walkPackageSet(db, outerPackage):
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
