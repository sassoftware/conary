#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import files
import helper
import log
import package
import repository

_pkgFormat  = "%-39s %s"
_fileFormat = "    %-35s %s"
_grpFormat  = "  %-37s %s"

def displayPkgs(repos, cfg, all = False, ls = False, ids = False, sha1s = False,
		pkg = "", versionStr = None):
    if pkg:
	list = [ pkg ]
    else:
	list = [ x for x in repos.iterAllTroveNames() ]
	list.sort()

    for pkgName in list:
	if versionStr or ls or ids or sha1s:
	    _displayPkgInfo(repos, cfg, pkgName, versionStr, ls, ids, sha1s)
	    continue
	else:
	    if all:
		l = repos.getPackageVersionList(pkgName)
	    else:
                try:
                    l = _versionList(repos, pkgName)
                except repository.PackageMissing, e:
                    log.error(str(e))
                    return

	    for version in l:
		print _pkgFormat % (pkgName, 
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

def _displayPkgInfo(repos, cfg, pkgName, versionStr, ls, ids, sha1s):
    try:
	pkgList = helper.findPackage(repos, cfg.installbranch, pkgName, 
				     versionStr)
    except helper.PackageNotFound, e:
	log.error(str(e))
	return

    for pkg in pkgList:
	version = pkg.getVersion()

	if ls:
	    fileL = [ (x[1], x[0], x[2]) for x in pkg.iterFileList() ]
	    fileL.sort()
	    for (path, fileId, version) in fileL:
		file = repos.getFileVersion(fileId, version, path = path)

		if isinstance(file, files.SymbolicLink):
		    name = "%s -> %s" %(path, file.target.val())
		else:
		    name = path

		print "%s    1 %-8s %-8s %s %s %s" % \
		    (file.modeString(), file.inode.owner(), file.inode.group(), 
		     file.sizeString(), file.timeString(), name)
	elif ids:
	    for (fileId, path, version) in pkg.iterFileList():
		print "%s %s" % (fileId, path)
	elif sha1s:
	    for (fileId, path, version) in pkg.iterFileList():
		file = repos.getFileVersion(fileId, version, path = path)
		if file.hasContents:
		    print "%s %s" % (file.contents.sha1(), path)
	else:
	    print _pkgFormat % (pkgName, version.asString(cfg.defaultbranch))

	    for (pkgName, ver) in pkg.iterPackageList():
		print _grpFormat % (pkgName, ver.asString(cfg.defaultbranch))

	    fileL = [ (x[1], x[0], x[2]) for x in pkg.iterFileList() ]
	    fileL.sort()
	    for (path, fileId, version) in fileL:
		print _fileFormat % (path, version.asString(cfg.defaultbranch))
