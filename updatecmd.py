#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import changeset
import commit
import os
import sys
import util
import versions

def doUpdate(repos, db, cfg, pkg, versionStr = None):
    if not os.path.exists(cfg.root):
        util.mkdirChain(cfg.root)
    
    if pkg and pkg[0] != "/":
	pkg = cfg.packagenamespace + "/" + pkg

    if versionStr and versionStr[0] != "/":
	versionStr = cfg.defaultbranch.asString() + "/" + versionStr

    if versionStr:
	newVersion = versions.VersionFromString(versionStr)
    else:
	newVersion = None

    list = []
    bail = 0
    mainPackageName = None
    for pkgName in repos.getPackageList(pkg):
	pkgSet = repos.getPackageSet(pkgName)

	if not newVersion:
	    newVersion = pkgSet.getLatestVersion(cfg.defaultbranch)

	if not pkgSet.hasVersion(newVersion):
	    sys.stderr.write("package %s does not contain version %s\n" %
				 (pkgName, version.asString()))
	    bail = 1
	else:
	    list.append((pkgName, None, newVersion))

    if bail:
	return

    cs = changeset.CreateFromRepository(repos, list)
    commit.commitChangeSet(db, cfg, cs)
