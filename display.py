#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import package
import versions
import files

_pkgFormat  = "%-39s %s"
_fileFormat = "    %-35s %s"

def displayPkgs(repos, cfg, all = 0, ls = 0, pkg = "", versionStr = None):
    if pkg and pkg[0] != ":":
	pkg = cfg.packagenamespace + ":" + pkg

    for pkgName in repos.getPackageList(pkg):
	if versionStr or ls:
            displayPkgInfo(repos, cfg, pkgName, versionStr, ls)
            continue
	else:
	    if all:
		l = repos.getPackageVersionList(pkgName)
		versions.versionSort(l)
	    else:
		l = ( repos.pkgLatestVersion(pkgName, cfg.defaultbranch), )
	    
	    for version in l:
		print _pkgFormat % (
		    package.stripNamespace(cfg.packagenamespace, pkgName),
		    version.asString(cfg.defaultbranch))

def displayPkgInfo(repos, cfg, pkgName, versionStr, ls):
    if versionStr:
	if versionStr[0] != "/":
	    versionStr = cfg.defaultbranch.asString() + "/" + versionStr
	version = versions.VersionFromString(versionStr)

	if version.isBranch():
	    pkg = repos.getLatestPackage(pkgName, version)
	else:
	    pkg = repos.getPackageVersion(pkgName, version)
    else:
	version = repos.pkgLatestVersion(pkgName, cfg.defaultbranch)
	pkg = repos.getPackageVersion(pkgName, version)

    if not ls:
	print _pkgFormat % (
	    package.stripNamespace(cfg.packagenamespace, pkgName),
	    version.asString(cfg.defaultbranch))

	for (fileId, path, version) in pkg.fileList():
	    print _fileFormat % (path, version.asString(cfg.defaultbranch))
    else:
	for (fileId, path, version) in pkg.fileList():
	    filesDB = repos.getFileDB(fileId)
	    file = filesDB.getVersion(version)

	    if isinstance(file, files.SymbolicLink):
		name = "%s -> %s" %(path, file.linkTarget())
	    else:
		name = path

	    print "%s    1 %-8s %-8s %s %s %s" % \
		(file.modeString(), file.owner(), file.group(), 
		 file.sizeString(), file.timeString(), name)
