#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import package
import repository
import sys

def listRollbacks(db, cfg):
    for rollbackName in db.getRollbackList():
	print "%s:" % rollbackName

	rb = db.getRollback(rollbackName)

	for pkg in rb.getPackageList():
	    print "\t%s %s -> %s" % \
		(package.stripNamespace(cfg.packagenamespace, pkg.getName()),
		 pkg.getOldVersion().asString(cfg.defaultbranch), 
		 pkg.getNewVersion().asString(cfg.defaultbranch))

	print

def apply(db, cfg, *names):
    try:
	db.applyRollbackList(cfg.sourcepath, names)
    except repository.RollbackOrderError, e:
	sys.stderr.write("%s\n" % repr(e))	
    except KeyError, e:
	sys.stderr.write("rollback %s does not exist in the database\n" %
			 str(e))
