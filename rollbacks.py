import os
import package

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

