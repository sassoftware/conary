#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import changeset
import os
import sys
import util
import versions

def doUpdate(repos, db, cfg, pkg, versionStr = None):
    if not os.path.exists(cfg.root):
        util.mkdirChain(cfg.root)
    
    if os.path.exists(pkg):
	if versionStr:
	    sys.stderr.write("Verison should not be specified when a SRS "
			     "change set is being installed.\n")
	    return 1

	cs = changeset.ChangeSetFromFile(pkg)
    else:
	if pkg and pkg[0] != ":":
	    pkg = cfg.packagenamespace + ":" + pkg

	if versionStr and versionStr[0] != "/":
	    versionStr = cfg.defaultbranch.asString() + "/" + versionStr

	if versionStr:
	    newVersion = versions.VersionFromString(versionStr)
	else:
	    newVersion = None

	list = []
	bail = 0
	for pkgName in repos.getPackageList(pkg):
	    if not newVersion:
		newVersion = repos.pkgLatestVersion(pkgName, cfg.defaultbranch)

	    if not repos.hasPackageVersion(pkgName, newVersion):
		sys.stderr.write("package %s does not contain version %s\n" %
				     (pkgName, newVersion.asString()))
		bail = 1
	    else:
		if db.hasPackage(pkgName):
		    currentVersion = db.pkgLatestVersion(pkgName, 
							 newVersion.branch())
		else:
		    currentVersion = None

		list.append((pkgName, currentVersion, newVersion))
	if bail:
	    return

	cs = repos.createChangeSet(list)

    if cs.isAbstract():
	db.commitChangeSet(cfg.sourcepath, cs, eraseOld = 0)
    else:
	inverse = cs.invert(db)
	db.addRollback(inverse)
	db.commitChangeSet(cfg.sourcepath, cs, eraseOld = 1)
