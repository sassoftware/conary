#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import changeset
import helper
import log
import package
import versions

def checkin(repos, cfg, file):
    f = open(file, "r")

    try:
	grp = package.GroupFromTextFile(f, cfg.packagenamespace, repos)
    except package.ParseError:
	return

    simpleVer = grp.getSimpleVersion()

    ver = repos.pkgLatestVersion(grp.getName(), cfg.defaultbranch)
    if not ver:
	ver = cfg.defaultbranch.copy()
	ver.appendVersionRelease(simpleVer, 1)
    elif ver.trailingVersion() == simpleVer:
	ver.incrementVersionRelease()
    else:
	ver = ver.branch()
	ver.appendVersionRelease(simpleVer, 1)

    grp.changeVersion(ver)
    changeSet = changeset.CreateFromFilesystem( [ (grp, {}) ] )
    repos.commitChangeSet(changeSet)

def checkout(repos, cfg, name, file, versionStr = None):
    try:
	pkgList = helper.findPackage(repos, cfg.packagenamespace, 
				     cfg.defaultbranch, name, versionStr, 
				     forceGroup = 1)
    except helper.PackageNotFound, e:
	log.error(str(e))
	return

    if len(pkgList) > 1:
	log.error("%s %s specified multiple packages" % (name, versionStr))
	return

    pkg = pkgList[0]

    f = open(file, "w")
    f.write("\n".join(pkg.getGroupFile()))
    f.write("\n")
