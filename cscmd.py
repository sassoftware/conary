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

from local import update
from repository import repository
from lib import log
import versions

def ChangeSetCommand(repos, cfg, troveList, outFileName):
    list = []

    for item in troveList:
        l = item.split("=")
        if len(l) == 1:
            newVersionStr = None
            oldVersionStr = None
            troveName = item
        elif len(l) != 2:
            log.error("one = expected in '%s' argument to changeset", item)
            return
        else:
            troveName = l[0]
            l = l[1].split("--")
            if len(l) == 1:
                newVersionStr = l[0]
                oldVersionStr = None
            elif len(l) == 2:
                oldVersionStr = l[0]
                newVersionStr = l[1]
            else:
                log.error("only one -- is allowed in '%s' argument to "
                          "changeset", item)
                return

        pkgList = repos.findTrove(cfg.installLabelPath, troveName, cfg.flavor,
                                  newVersionStr)
        if len(pkgList) > 1:
            if newVersionStr:
                log.error("trove %s has multiple branches named %s",
                          troveName, newVersionStr)
            else:
                log.error("trove %s has too many branches on installLabelPath",
                          troveName)

        newVersion = pkgList[0].getVersion()
        newFlavor = pkgList[0].getFlavor()

        if (oldVersionStr):
            pkgList = repos.findTrove(cfg.installLabelPath, troveName, 
                                      pkgList[0].getFlavor(), oldVersionStr)
            if len(pkgList) > 1:
                log.error("trove %s has multiple branches named %s",
                          troveName, oldVersionStr)

            oldVersion = pkgList[0].getVersion()
            oldFlavor = pkgList[0].getFlavor()
        else:
            oldVersion = None
            oldFlavor = None

        list.append((troveName, (oldVersion, oldFlavor), 
                                (newVersion, newFlavor),
                    not oldVersion))

    repos.createChangeSetFile(list, outFileName)

def LocalChangeSetCommand(db, cfg, pkgName, outFileName):
    try:
	pkgList = db.findTrove(pkgName, None)
    except repository.TroveNotFound, e:
	log.error(e)
	return

    list = []
    for outerPackage in pkgList:
	for pkg in db.walkTroveSet(outerPackage):
	    ver = pkg.getVersion()
	    origPkg = db.getTrove(pkg.getName(), ver, pkg.getFlavor(), 
				  pristine = True)
	    ver = ver.createBranch(versions.LocalBranch(), withVerRel = 1)
	    list.append((pkg, origPkg, ver, 0))
	    
    result = update.buildLocalChanges(db, list, root = cfg.root)
    if not result: return
    cs = result[0]

    for outerPackage in pkgList:
	cs.addPrimaryPackage(outerPackage.getName(), 
	    outerPackage.getVersion().createBranch(
		versions.LocalBranch(), withVerRel = 1),
	   outerPackage.getFlavor())

    for (changed, fsPkg) in result[1]:
	if changed:
	    break

    if not changed:
	log.error("there have been no local changes")
    else:
	cs.writeToFile(outFileName)
