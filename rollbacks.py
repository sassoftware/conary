#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

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

def apply(db, cfg, *names):
    list = []
    for name in names:
	list.append((db.getRollback(name), name))

    for (rb, name) in list:
	db.commitChangeSet(cfg.sourcepath, rb, eraseOld = 1)
	db.removeRollback(name)
