#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

from local import database
import log

def listRollbacks(db, cfg):
    def verStr(cfg, version):
	if version.branch().label() == cfg.installlabel:
	    return version.trailingVersion().asString()
	return version.asString()

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
					       verStr(cfg, pkg.getNewVersion()))
		else:
		    print "\t%s %s -> %s" % \
			(name, 
			 verStr(cfg, pkg.getOldVersion()),
			 verStr(cfg, pkg.getNewVersion()))

	    list = []
	    for (pkg, version, flavor) in cs.getOldPackageList():
		list.append((pkg, version))

	    list.sort()
	    for (pkg, version) in list:
		print "\t%s %s added" %  (pkg, verStr(cfg, version))

	print

def apply(db, cfg, *names):
    try:
	db.applyRollbackList(names)
    except database.RollbackError, e:
	log.error("%s", e)
	return 1

    return 0
