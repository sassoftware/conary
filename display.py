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
    if pkg and pkg[0] != "/":
	pkg = cfg.packagenamespace + "/" + pkg

    for pkgName in repos.getPackageList(pkg):
	pkgSet = repos.getPackageSet(pkgName)
	if versionStr or ls:
            displayPkgInfo(repos, cfg, pkgName, versionStr, ls)
            continue
	else:
	    if all:
		l = pkgSet.versionList()
		versions.versionSort(l)
	    else:
		l = ( pkgSet.getLatestVersion(cfg.defaultbranch), )
	    
	    for version in l:
		print _pkgFormat % (
		    package.stripNamespace(cfg.packagenamespace, pkgName),
		    version.asString(cfg.defaultbranch))

def displayPkgInfo(repos, cfg, pkgName, versionStr, ls):
    if pkgName[0] != "/":
	pkgName = cfg.packagenamespace + "/" + pkgName

    pkgSet = repos.getPackageSet(pkgName)

    if versionStr:
	if versionStr[0] != "/":
	    versionStr = cfg.defaultbranch.asString() + "/" + versionStr
	version = versions.VersionFromString(versionStr)

	if version.isBranch():
	    pkg = pkgSet.getLatestPackage(version)
	else:
	    pkg = pkgSet.getVersion(version)
    else:
	version = pkgSet.getLatestVersion(cfg.defaultbranch)
	pkg = pkgSet.getVersion(version)

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
