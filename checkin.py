#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import changeset
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
    if name[0] != ":":
	name = cfg.packagenamespace + ":" + name
    else:
	name = name

    if name.count(":") != 2:
	log.error("group names may not include colons")
	return

    last = name.split(":")[-1]
    if not last.startswith("group-"):
	log.error("only groups may be checked out of the repository")
	return

    if not versionStr:
	version = cfg.defaultbranch
    else:
	if versionStr[0] != "/":
	    versionStr = cfg.defaultbranch.asString() + "/" + versionStr
	version = versions.VersionFromString(versionStr)

    if version.isBranch():
	pkg = repos.getLatestPackage(name, version)
    else:
	pkg = repos.getPackageVersion(name, version)

    f = open(file, "w")
    f.write("\n".join(pkg.getGroupFile()))
    f.write("\n")
