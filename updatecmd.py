#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import changeset
import log
import os
import package
import repository
import sys
import util
import versions

def doUpdate(repos, db, cfg, pkg, versionStr = None):
    cs = None
    if not os.path.exists(cfg.root):
        util.mkdirChain(cfg.root)

    map = ( ( None, cfg.sourcepath + "/" ), )
    
    if os.path.exists(pkg):
        # there is a file, try to read it as a changeset file
	if versionStr:
	    sys.stderr.write("Verison should not be specified when a SRS "
			     "change set is being installed.\n")
	    return 1

        try:
            cs = changeset.ChangeSetFromFile(pkg)
        except KeyError:
            # invalid changeset file
            pass
        else:
	    cs.remapPaths(map)

            if cs.isAbstract():
                cs = db.rootChangeSet(cs, cfg.defaultbranch)

	    list = [ x.getName() for x  in cs.getNewPackageList() ]

    if not cs:
        # so far no changeset (either the path didn't exist or we could not
        # read it
	if pkg and pkg[0] != ":":
	    pkg = cfg.packagenamespace + ":" + pkg

	if versionStr and versionStr[0] != "/":
	    versionStr = cfg.defaultbranch.asString() + "/" + versionStr

	if versionStr:
	    newVersion = versions.VersionFromString(versionStr)
	else:
	    newVersion = None

	bail = 0

	if not repos.hasPackage(pkg):
            log.error("repository does not contain a package called %s" % 
		      (package.stripNamespace(cfg.packagenamespace, pkg)))
	    bail = 1
	else:
	    if not newVersion:
		newVersion = repos.pkgLatestVersion(pkg, cfg.defaultbranch)

	    if not newVersion or not repos.hasPackageVersion(pkg, newVersion):
		log.error("package %s does not contain version %s" %
			  (package.stripNamespace(cfg.packagenamespace, pkg), 
			  newVersion.asString(cfg.defaultbranch)))
		bail = 1

	    if db.hasPackage(pkg):
		# currentVersion could be None
		currentVersion = db.pkgLatestVersion(pkg, 
						     newVersion.branch())
	    else:
		currentVersion = None

	    list = [(pkg, currentVersion, newVersion, 0)]

	if bail:
	    return

	cs = repos.createChangeSet(list)
	cs.remapPaths(map)

	list = [ x[0] for x in list ]

    try:
	db.commitChangeSet(cs)
    except repository.CommitError, e:
	print e
