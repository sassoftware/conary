#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import changeset
import helper
import log
import os
import package
import repository
import util
import versioned
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
    elif ver.trailingVersion().getVersion() == simpleVer:
	ver.incrementVersionRelease()
    else:
	ver = ver.branch()
	ver.appendVersionRelease(simpleVer, 1)

    grp.changeVersion(ver)
    changeSet = changeset.CreateFromFilesystem( [ (grp, {}) ] )
    repos.commitChangeSet(changeSet)

def checkout(repos, cfg, name, versionStr = None):
    # This doesn't use helper.findPackage as it doesn't want to allow
    # branches nicknames. Doing so would cause two problems. First, we could
    # get multiple matches for a single pacakge. Two, even if we got
    # a single match we wouldn't know where to check in changes. A nickname
    # branch doesn't work for checkins as it could refer to multiple
    # branches, even if it doesn't right now.
    if name[0] != ":":
	name = cfg.packagenamespace + ":" + name
    name = name + ":sources"

    if not versionStr:
	version = cfg.defaultbranch
    else:
	if versionStr != "/":
	    versionStr = cfg.defaultbranch.asString() + "/" + versionStr

	try:
	    version = versions.VersionFromString(versionStr)
	except versions.ParseError, e:
	    log.error(str(e))
	    return

    try:
	if version.isBranch():
	    trv = repos.getLatestPackage(name, version)
	else:
	    trv = repos.getPackageVersion(name, version)
    except versioned.MissingBranchError, e:
	log.error(str(e))
	return
    except repository.PackageMissing, e:
	log.error(str(e))
	return
	
    dir = trv.getName().split(":")[-2]

    if not os.path.isdir(dir):
	try:
	    os.mkdir(dir)
	except:
	    log.error("cannot create directory %s/%s", os.getcwd(), dir)
	    return

    for (fileId, path, version) in trv.fileList():
	fullPath = dir + "/" + path
	fileObj = repos.getFileVersion(fileId, version)
	src = repos.pullFileContentsObject(fileObj.sha1())
	dest = open(fullPath, "w")
	util.copyfileobj(src, dest)

    f = open(dir + "/" + "SRS", "w")
    f.write("name %s\n" % ":".join(trv.getName().split(":")[:-1]))
    f.write("version %s\n" % version.asString())
