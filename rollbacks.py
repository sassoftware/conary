#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import database
import log
import sys

def listRollbacks(db, cfg):
    for rollbackName in db.getRollbackList():
	print "%s:" % rollbackName

	rb = db.getRollback(rollbackName)
	for cs in rb:
	    list = []
	    for pkg in cs.getNewPackageList():
		name = package.stripNamespace(cfg.packagenamespace, 
					    pkg.getName())
		list.append((name, pkg))

	    list.sort()
	    for (name, pkg) in list:
		print "\t%s %s -> %s" % \
		    (name,
		     pkg.getOldVersion().asString(cfg.defaultbranch), 
		     pkg.getNewVersion().asString(cfg.defaultbranch))

	    list = []
	    for (pkg, version) in cs.getOldPackageList():
		name = package.stripNamespace(cfg.packagenamespace, pkg)
		list.append((name, version))

	    list.sort()
	    for (pkg, version) in list:
		print "\t%s %s added" %  \
				(package.stripNamespace(cfg.packagenamespace, 
							pkg), 
				 version.asString(cfg.defaultbranch))

	print

def apply(db, cfg, *names):
    try:
	db.applyRollbackList(names)
    except database.RollbackError, e:
	log.error("%s", e)
	sys.exit(1)
