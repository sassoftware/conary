#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import files
import versions

_pkgFormat  = "%-39s %s"
_fileFormat = "    %-35s %s"
_grpFormat  = "  %-37s %s"

def displayPkgs(db, cfg, ls = False, ids = False, sha1s = False,
		pkg = "", versionStr = None):
    if pkg:
	list = [ pkg ]
    else:
	list = [ x for x in db.iterAllTroveNames() ]
	list.sort()

    for pkgName in list:
	if versionStr or ls or ids or sha1s:
	    _displayPkgInfo(db, cfg, pkgName, versionStr, ls, ids, sha1s)
	    continue
	else:
	    l = db.getPackageVersionList(pkgName)

	    for version in l:
		print _pkgFormat % (pkgName, 
				    version.asString(cfg.defaultbranch))

def _displayPkgInfo(db, cfg, pkgName, versionStr, ls, ids, sha1s):
    troveList = db.findTrove(pkgName, versionStr)

    for trove in troveList:
	version = trove.getVersion()

	if ls:
	    iter = db.iterFilesInTrove(trove.getName(), trove.getVersion(),
                                       trove.getFlavor(),
                                       sortByPath = True, withFiles = True)
	    for (fileId, path, version, file) in iter:
		if isinstance(file, files.SymbolicLink):
		    name = "%s -> %s" %(path, file.target.value())
		else:
		    name = path

		print "%s    1 %-8s %-8s %s %s %s" % \
		    (file.modeString(), file.inode.owner(), file.inode.group(), 
		     file.sizeString(), file.timeString(), name)
	elif ids:
	    for (fileId, path, version) in trove.iterFileList():
		print "%s %s" % (fileId, path)
	elif sha1s:
	    for (fileId, path, version) in trove.iterFileList():
		file = db.getFileVersion(fileId, version)
		if file.hasContents:
		    print "%s %s" % (file.contents.sha1(), path)
	else:
	    print _pkgFormat % (troveName, version.asString(cfg.defaultbranch))

	    for (troveName, ver, flavor) in trove.iterTroveList():
		print _grpFormat % (troveName, ver.asString(cfg.defaultbranch))

	    fileL = [ (x[1], x[0], x[2]) for x in trove.iterFileList() ]
	    fileL.sort()
	    for (path, fileId, version) in fileL:
		print _fileFormat % (path, version.asString(cfg.defaultbranch))
