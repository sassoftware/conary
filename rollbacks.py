#
# Copyright (c) 2004-2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

from local import database
from lib import log
from repository import changeset

def listRollbacks(db, cfg):
    def verStr(cfg, version):
	if version.onLocalLabel():
	    return "local"

	if version.branch().label() == cfg.installLabel:
	    return version.trailingRevision().asString()
	return version.asString()

    for rollbackName in reversed(db.getRollbackList()):
	print "%s:" % rollbackName

	rb = db.getRollback(rollbackName)
	for cs in rb.iterChangeSets():
            newList = []
            for pkg in cs.iterNewTroveList():
                newList.append((pkg.getName(), pkg.getOldVersion(),
                                pkg.getNewVersion()))
            oldList = [ x[0:2] for x in cs.getOldTroveList() ]

	    newList.sort()
	    oldList.sort()
	    for (name, oldVersion, newVersion) in newList:
		if not oldVersion:
		    print "\t%s %s removed" % (name, 
					       verStr(cfg, newVersion))
		else:
		    print "\t%s %s -> %s" % \
			(name, verStr(cfg, oldVersion),
			       verStr(cfg, newVersion))

	    for (name, version) in oldList:
		print "\t%s %s added" %  (name, verStr(cfg, version))

	print

def apply(db, repos, cfg, *names, **kwargs):
    replaceFiles = kwargs.get('replaceFiles', False)
    try:
	db.applyRollbackList(repos, names, replaceFiles=replaceFiles)
    except database.RollbackError, e:
	log.error("%s", e)
	return 1

    return 0
