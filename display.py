#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import files
import helper
import log
import package
import versions

_pkgFormat  = "%-39s %s"
_fileFormat = "    %-35s %s"
_grpFormat  = "  %-37s %s"

def displayPkgs(repos, cfg, all = 0, ls = 0, pkg = "", versionStr = None):
    if pkg and pkg[0] != ":":
	pkg = cfg.packagenamespace + ":" + pkg

    if pkg:
	list = [ pkg ]
    else:
	list = repos.getAllTroveNames()
	list.sort()

    for pkgName in list:
	if versionStr or ls:
	    _displayPkgInfo(repos, cfg, pkgName, versionStr, ls)
	    continue
	else:
	    if all:
		l = repos.getPackageVersionList(pkgName)
	    else:
		l = _versionList(repos, pkgName)

	    for version in l:
		print _pkgFormat % (
		    package.stripNamespace(cfg.packagenamespace, pkgName),
		    version.asString(cfg.defaultbranch))

def _versionList(repos, pkgName):
    """
    Returns a list of the head of all non-empty branches for a package.

    @param repos: Repository to look for branches in
    @type repos: repository.Repository
    @param pkgName: Name of a package
    @type pkgName: str
    @rtype: list of str
    """

    branches = repos.getPackageBranchList(pkgName)
    l = []
    for branch in branches:
	if not branch.isLocal():
	    version = repos.pkgLatestVersion(pkgName, branch)
	    # filter out empty branches 
	    if version and version.onBranch(branch):
		l.append(version)

    return l

def _displayPkgInfo(repos, cfg, pkgName, versionStr, ls):
    try:
	pkgList = helper.findPackage(repos, cfg.packagenamespace, 
				     cfg.defaultbranch, pkgName, versionStr)
    except helper.PackageNotFound, e:
	log.error(str(e))
	return

    for pkg in pkgList:
	version = pkg.getVersion()

	if not ls:
	    print _pkgFormat % (
		package.stripNamespace(cfg.packagenamespace, pkgName),
		version.asString(cfg.defaultbranch))

	    for (pkgName, verList) in pkg.getPackageList():
		for ver in verList:
		    print _grpFormat % (
			    package.stripNamespace(cfg.packagenamespace, 
						   pkgName),
			    ver.asString(cfg.defaultbranch))

	    for (fileId, path, version) in pkg.fileList():
		print _fileFormat % (path, version.asString(cfg.defaultbranch))
	else:
	    for (fileId, path, version) in pkg.fileList():
		file = repos.getFileVersion(fileId, version, path = path)

		if isinstance(file, files.SymbolicLink):
		    name = "%s -> %s" %(path, file.linkTarget())
		else:
		    name = path

		print "%s    1 %-8s %-8s %s %s %s" % \
		    (file.modeString(), file.owner(), file.group(), 
		     file.sizeString(), file.timeString(), name)
