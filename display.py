#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import package
import versions

_pkgFormat  = "%-39s %s"
_fileFormat = "    %-35s %s"

def displayPkgs(repos, cfg, pkg = "", versionStr = None):
    if pkg and pkg[0] != "/":
	pkg = cfg.packagenamespace + "/" + pkg

    for pkgName in repos.getPackageList(pkg):
	pkgSet = repos.getPackageSet(pkgName)
	if versionStr:
            displayPkgInfo(repos, cfg, pkgName, versionStr)
            continue
        l = pkgSet.versionList()
        versions.versionSort(l)
        for version in l:
            print _pkgFormat % (
                package.stripNamespace(cfg.packagenamespace, pkgName),
                version.asString(cfg.defaultbranch))

def displayPkgInfo(repos, cfg, pkgName, versionStr):
    if pkgName[0] != "/":
	pkgName = cfg.packagenamespace + "/" + pkgName

    if versionStr[0] != "/":
	versionStr = cfg.defaultbranch.asString() + "/" + versionStr
    version = versions.VersionFromString(versionStr)

    pkgSet = repos.getPackageSet(pkgName)

    if version.isBranch():
	pkg = pkgSet.getLatestPackage(version)
    else:
	pkg = pkgSet.getVersion(version)

    print _pkgFormat % (
        package.stripNamespace(cfg.packagenamespace, pkgName),
        version.asString(cfg.defaultbranch))
    for (fileId, path, version) in pkg.fileList():
	print _fileFormat % (path, version.asString(cfg.defaultbranch))
