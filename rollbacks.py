#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import database
import package
import repository
import sys

def listRollbacks(db, cfg):
    for rollbackName in db.getRollbackList():
	print "%s:" % rollbackName

	rb = db.getRollback(rollbackName)

	for pkg in rb.getNewPackageList():
	    print "\t%s %s -> %s" % \
		(package.stripNamespace(cfg.packagenamespace, pkg.getName()),
		 pkg.getNewVersion().asString(cfg.defaultbranch), 
		 pkg.getOldVersion().asString(cfg.defaultbranch))

	for (pkg, version) in rb.getOldPackageList():
	    print "\t%s %s added" % (pkg, version.asString(cfg.defaultbranch))

	print

def apply(db, cfg, *names):
    try:
	db.applyRollbackList(cfg.sourcepath, names)
    except database.RollbackError, e:
	sys.stderr.write("%s\n" % repr(e))	
