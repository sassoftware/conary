#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
from repository import changeset
from local import database
import helper
import log
import os
from repository import repository
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

            if cs.isAbsolute():
                cs = db.rootChangeSet(cs, cfg.defaultbranch)

	    list = [ x.getName() for x  in cs.iterNewPackageList() ]
	    if versionStr:
		sys.stderr.write("Verison should not be specified when a "
				 "SRS change set is being installed.\n")
		return 1


    if not cs:
        # so far no changeset (either the path didn't exist or we could not
        # read it
	try:
	    pkgList = helper.findPackage(repos, cfg.installbranch, pkg, 
					 versionStr)
	except helper.PackageNotFound, e:
	    log.error(str(e))
	    return

	list = []
	for pkg in pkgList:
	    if db.hasPackage(pkg.getName()):
		# currentVersion could be None
		currentVersionList = db.getPackageVersionList(pkg.getName())
		if len(currentVersionList) == 1:
		    currentVersion = currentVersionList[0]
		elif len(currentVersionList) == 0:
		    currentVersion = None
		else:
		    # there are multiple versions installed; rather then
		    # upgrade all of them look for one on the same branch
		    # as the one we're installing. if there's a match, great;
		    # if not, bail
		    currentVersion = db.pkgLatestVersion(pkg.getName(), 
						     pkg.getVersion().branch())
		    if not currentVersion:
			log.error("multiple versions of %s are installed and "
				  "none are on the same branch as the update") 
			return
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
	pkgList = helper.findPackage(db, cfg.installbranch, pkg, 
				     versionStr)
    except helper.PackageNotFound, e:
	log.error(str(e))
	return

    list = []
    for pkg in pkgList:
	list.append((pkg.getName(), pkg.getVersion(), None, False))

    cs = db.stash.createChangeSet(list)
    db.commitChangeSet(cs)
