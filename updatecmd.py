#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import changeset
import database
import helper
import log
import os
import repository
import sys
import util

def doUpdate(repos, db, cfg, pkg, versionStr = None, replaceFiles = False):
    cs = None
    if not os.path.exists(cfg.root):
        util.mkdirChain(cfg.root)

    map = ( ( None, cfg.sourcepath + "/" ), )
    
    if os.path.exists(pkg) and os.path.isfile(pkg):
	# there is a file, try to read it as a changeset file

        try:
            cs = changeset.ChangeSetFromFile(pkg)
        except KeyError:
            # invalid changeset file
            pass
        else:
	    cs.remapPaths(map)

            if cs.isAbstract():
                cs = db.rootChangeSet(cs, cfg.defaultbranch)

	    list = [ x.getName() for x  in cs.getNewPackageList() ]
	    if versionStr:
		sys.stderr.write("Verison should not be specified when a SRS "
				 "change set is being installed.\n")
		return 1


    if not cs:
        # so far no changeset (either the path didn't exist or we could not
        # read it
	try:
	    pkgList = helper.findPackage(repos, cfg.packagenamespace, 
				     cfg.installbranch, pkg, versionStr)
	except helper.PackageNotFound, e:
	    log.error(str(e))
	    return

	list = []
	for pkg in pkgList:
	    if db.hasPackage(pkg.getName()):
		# currentVersion could be None
		currentVersion = db.pkgLatestVersion(pkg.getName(), 
						     pkg.getVersion().branch())
	    else:
		currentVersion = None

	    list.append((pkg.getName(), currentVersion, pkg.getVersion(), 0))

	cs = repos.createChangeSet(list)
	cs.remapPaths(map)

	list = [ x[0] for x in list ]

    try:
	db.commitChangeSet(cs, replaceFiles=replaceFiles)
    except database.SourcePackageInstall, e:
	log.error(e)
    except repository.CommitError, e:
	log.error(e)

def doErase(db, cfg, pkg, versionStr = None):
    try:
	pkgList = helper.findPackage(db, cfg.packagenamespace, 
				 cfg.installbranch, pkg, versionStr)
    except helper.PackageNotFound, e:
	log.error(str(e))
	return

    list = []
    for pkg in pkgList:
	list.append((pkg.getName(), pkg.getVersion(), None, False))

    cs = db.createChangeSet(list)
    db.commitChangeSet(cs)
