import package
import versions

def displayPkgs(repos, cfg, pkg = "", versionStr = None):
    if pkg and pkg[0] != "/":
	pkg = cfg.packagenamespace + "/" + pkg

    for pkgName in repos.getPackageList(pkg):
	pkgSet = repos.getPackageSet(pkgName)
	if not versionStr:
	    l = pkgSet.versionList()
	    versions.versionSort(l)
	    for version in l:
		    print "%-39s %s" % (
			package.stripNamespace(cfg.packagenamespace, pkgName),
			version.asString(cfg.defaultbranch)
		      )
	else:
	    displayPkgInfo(repos, cfg, pkgName, versionStr)

def displayPkgInfo(repos, cfg, pkgName, versionStr):
    if pkgName[0] != "/":
	pkgName = cfg.packagenamespace + "/" + pkgName

    if versionStr[0] != "/":
	versionStr = cfg.defaultbranch.asString() + "/" + versionStr
    version = versions.VersionFromString(versionStr)

    pkgSet = repos.getPackageSet(pkgName)
    pkg = pkgSet.getVersion(version)
    print "%-39s %s" % (
	    package.stripNamespace(cfg.packagenamespace, pkgName),
	    version.asString(cfg.defaultbranch)
	)
    for (id, path, version) in pkg.fileList():
	print "    %-35s %s" % (path, version.asString(cfg.defaultbranch))

