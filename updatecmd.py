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
    cs = None
    if not os.path.exists(cfg.root):
        util.mkdirChain(cfg.root)
    
    if os.path.exists(pkg):
        # there is a file, try to read it as a changeset file
	if versionStr:
	    sys.stderr.write("Verison should not be specified when a SRS "
			     "change set is being installed.\n")
	    return 1

        try:
            cs = changeset.ChangeSetFromFile(pkg)
        except KeyError:
            # invalid changeset file
            pass
        else:
            if cs.isAbstract():
                cs = db.rootChangeSet(cs, cfg.defaultbranch)

	    list = [ x.getName() for x  in cs.getNewPackageList() ]

    if not cs:
        # so far no changeset (either the path didn't exist or we could not
        # read it
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

		list.append((pkgName, currentVersion, newVersion, 0))
	if bail:
	    return

        if not list:
            sys.stderr.write("repository does not contain a package called %s\n" % pkg)
            return

	cs = repos.createChangeSet(list)

	# permute the list into a list of just package names
	list = map(lambda x: x[0], list)

    # create a change set between what is in the database and what is
    # on the disk
    localChanges = changeset.CreateAgainstLocal(cfg, db, list)

    inverse = cs.invert(db)
    db.addRollback(inverse)
    db.commitChangeSet(cfg.sourcepath, cs, eraseOld = 1)
