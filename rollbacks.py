#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

from local import database
import log

def listRollbacks(db, cfg):
    for rollbackName in db.getRollbackList():
	print "%s:" % rollbackName

	rb = db.getRollback(rollbackName)
	for cs in rb:
	    list = []
	    for pkg in cs.iterNewPackageList():
		list.append((pkg.getName(), pkg))

	    list.sort()
	    for (name, pkg) in list:
		if not pkg.getOldVersion():
		    print "\t%s %s removed" % (name,
			 pkg.getNewVersion().asString(cfg.defaultbranch))
		else:
		    print "\t%s %s -> %s" % \
			(name,
			 pkg.getOldVersion().asString(cfg.defaultbranch), 
			 pkg.getNewVersion().asString(cfg.defaultbranch))

	    list = []
	    for (pkg, version, flavor) in cs.getOldPackageList():
		list.append((pkg, version))

	    list.sort()
	    for (pkg, version) in list:
		print "\t%s %s added" %  \
				(pkg, version.asString(cfg.defaultbranch))

	print

def apply(db, cfg, *names):
    try:
	db.applyRollbackList(names)
    except database.RollbackError, e:
	log.error("%s", e)
	return 1

    return 0
