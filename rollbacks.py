#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import database
import package
import sys

def listRollbacks(db, cfg):
    for rollbackName in db.getRollbackList():
	print "%s:" % rollbackName

	rb = db.getRollback(rollbackName)
	for cs in rb:
	    for pkg in cs.getNewPackageList():
		print "\t%s %s -> %s" % \
		    (package.stripNamespace(cfg.packagenamespace, 
					    pkg.getName()),
		     pkg.getOldVersion().asString(cfg.defaultbranch), 
		     pkg.getNewVersion().asString(cfg.defaultbranch))

	    for (pkg, version) in cs.getOldPackageList():
		print "\t%s %s added" %  \
				(package.stripNamespace(cfg.packagenamespace, 
							pkg), 
				 version.asString(cfg.defaultbranch))

	print

def apply(db, cfg, *names):
    try:
	db.applyRollbackList(names)
    except database.RollbackError, e:
	sys.stderr.write("%s\n" % repr(e))	
