#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import changeset
import helper
import log
import os
import package
import util

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
    try:
	pkgList = helper.findPackage(repos, cfg.packagenamespace, 
				     cfg.installbranch, name, versionStr)
    except helper.PackageNotFound, e:
	log.error(str(e))
	return

    if len(pkgList) > 1:
	log.error("%s %s specified multiple packages" % (name, versionStr))
	return

    mainTrove = pkgList[0]
    sourceTroveName = mainTrove.getName() + ":sources"
    try:
	trv = repos.getPackageVersion(sourceTroveName, mainTrove.getVersion())
    except repository.PackageMissing, e:
	log.error("version %s of package %s does not have a source package",
		  mainPkg.getVersion().asString(), mainTrove.getName())
	return

    dir = mainTrove.getName().split(":")[-1]

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
