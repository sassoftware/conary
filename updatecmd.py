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

def doUpdate(repos, db, cfg, pkg, versionStr = None, replaceFiles = False,
	     tagScript = None):
    cs = None
    if not os.path.exists(cfg.root):
        util.mkdirChain(cfg.root)

    if os.path.exists(pkg) and os.path.isfile(pkg):
	# there is a file, try to read it as a changeset file

        try:
            cs = changeset.ChangeSetFromFile(pkg)
        except:
            # invalid changeset file
            pass
        else:
            if cs.isAbsolute():
                cs = db.rootChangeSet(cs)

	    list = [ x.getName() for x  in cs.iterNewPackageList() ]
	    if versionStr:
		sys.stderr.write("Verison should not be specified when a "
				 "Conary change set is being installed.\n")
		return 1

    if not cs:
        # so far no changeset (either the path didn't exist or we could not
        # read it
	try:
	    pkgList = repos.findTrove(cfg.installLabel, pkg, cfg.flavor,
				      versionStr)
	except repository.PackageNotFound, e:
	    log.error(str(e))
	    return

	list = []
	for pkg in pkgList:
	    if db.hasTrove(pkg.getName(), pkg.getVersion(), pkg.getFlavor()):
		continue

	    currentVersion = helper.previousVersion(db, pkg.getName(),
						    pkg.getVersion(),
						    pkg.getFlavor())

	    list.append((pkg.getName(), pkg.getFlavor(), currentVersion, 
			 pkg.getVersion(), 0))


        if not list:
            log.warning("no new troves were found")
            return

	cs = repos.createChangeSet(list)
	list = [ x[0] for x in list ]

    if not list:
	log.warning("no new troves were found")
	return

    try:
	db.commitChangeSet(cs, replaceFiles = replaceFiles, 
			   tagScript = tagScript)
    except database.SourcePackageInstall, e:
	log.error(e)
    except repository.CommitError, e:
	log.error(e)

def doErase(db, cfg, pkg, versionStr = None):
    try:
	pkgList = db.findTrove(pkg, versionStr)
    except helper.PackageNotFound, e:
	log.error(str(e))
	return

    list = []
    for pkg in pkgList:
	list.append((pkg.getName(), pkg.getFlavor(), pkg.getVersion()))

    db.eraseTroves(list)
