#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

from repository import repository
import files
import helper
import log

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

    if versionStr or ls or ids or sha1s:
	if all:
	    log.error("--all cannot be used with queries which display file "
		      "lists")
	    return
	for pkgName in list:
	    _displayPkgInfo(repos, cfg, pkgName, versionStr, ls, ids, sha1s)
	    continue
    else:
	if all:
	    versions = repos.getTroveVersionList(list)
	else:
            versions = repos.getAllTroveLeafs(list)

	for pkgName in list:
            versionList = versions[pkgName]
            if not versionList:
                log.error('No versions for "%s" were found in the repository',
                          pkgName)
                continue
	    for version in versionList:
		print _pkgFormat % (pkgName, 
				    version.asString(cfg.defaultbranch))

def _displayPkgInfo(repos, cfg, pkgName, versionStr, ls, ids, sha1s):
    try:
	pkgList = repos.findTrove(cfg.installbranch, pkgName, versionStr)
    except repository.PackageNotFound, e:
	log.error(str(e))
	return

    for pkg in pkgList:
	version = pkg.getVersion()

	if ls:
	    fileL = [ (x[1], x[0], x[2]) for x in pkg.iterFileList() ]
	    fileL.sort()
	    iter = repos.iterFilesInTrove(pkg.getName(), pkg.getVersion(),
                                          pkg.getFlavor(), sortByPath = True, 
					  withFiles = True)
	    for (fileId, path, version, file) in iter:
		if isinstance(file, files.SymbolicLink):
		    name = "%s -> %s" %(path, file.target.value())
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
		file = repos.getFileVersion(fileId, version)
		if file.hasContents:
		    print "%s %s" % (file.contents.sha1(), path)
	else:
	    print _pkgFormat % (pkgName, version.asString(cfg.defaultbranch))

	    for (pkgName, ver, flavor) in pkg.iterTroveList():
		print _grpFormat % (pkgName, ver.asString(cfg.defaultbranch))

	    fileL = [ (x[1], x[0], x[2]) for x in pkg.iterFileList() ]
	    fileL.sort()
	    for (path, fileId, version) in fileL:
		print _fileFormat % (path, version.asString(cfg.defaultbranch))
