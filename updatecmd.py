#
# Copyright (c) 2004 Specifix, Inc.
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
from repository import changeset
from local import database
import helper
import log
import os
from repository import repository
import sys
import trove
import util
import versions

def doUpdate(repos, cfg, pkg, versionStr = None, replaceFiles = False,
	     tagScript = None, keepExisting = False):
    db = database.Database(cfg.root, cfg.dbPath)

    cs = None
    if not os.path.exists(cfg.root):
        util.mkdirChain(cfg.root)

    import time
    start = time.time()

    if os.path.exists(pkg) and os.path.isfile(pkg):
	# there is a file, try to read it as a changeset file

        try:
            cs = changeset.ChangeSetFromFile(pkg)
        except KeyError:
            # invalid changeset file
            pass
        else:
            if cs.isAbsolute():
                try:
                    cs = db.rootChangeSet(cs)
                except repository.CommitError, e:
                    sys.stderr.write("%s\n" %str(e))
                    return 1

	    list = [ x.getName() for x  in cs.iterNewPackageList() ]
	    if versionStr:
		sys.stderr.write("Verison should not be specified when a "
				 "Conary change set is being installed.\n")
		return 1

	    eraseList = []

    if not cs:
        # so far no changeset (either the path didn't exist or we could not
        # read it
	try:
	    newList = repos.findTrove(cfg.installLabel, pkg, cfg.flavor,
				      versionStr)
	except repository.PackageNotFound, e:
	    log.error(str(e))
	    return

	list = []
	if keepExisting:
	    for newTrove in newList:
		list.append((newTrove.getName(), (None, None),
			   (newTrove.getVersion(), newTrove.getFlavor()), 0))
	    eraseList = []
	else:
	    newItems = []
	    for newTrove in newList:
		newItems.append((newTrove.getName(), newTrove.getVersion(),
				 newTrove.getFlavor()))

	    # everything which needs to be installed is in this list; if it's
	    # not here, it's a duplicate
	    outdated, eraseList = helper.outdatedTroves(db, newItems)
	    for (name, newVersion, newFlavor), \
		    (oldName, oldVersion, oldFlavor) in outdated.iteritems():
		list.append((name, (oldVersion, oldFlavor),
				   (newVersion, newFlavor), 0))

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
			   tagScript = tagScript, keepExisting = keepExisting)
    except database.SourcePackageInstall, e:
	log.error(e)
    except repository.CommitError, e:
	log.error(e)

    if eraseList:
	db.eraseTroves(eraseList, tagScript = tagScript)

def doErase(db, cfg, pkg, versionStr = None, tagScript = None):
    try:
	pkgList = db.findTrove(pkg, versionStr)
    except helper.PackageNotFound, e:
	log.error(str(e))
	return

    list = []
    for pkg in pkgList:
	list.append((pkg.getName(), pkg.getVersion(), pkg.getFlavor()))

    db.eraseTroves(list, tagScript = tagScript)
