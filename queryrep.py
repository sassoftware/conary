#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

from repository import repository
import files
import log

_troveFormat  = "%-39s %s"
_fileFormat = "    %-35s %s"
_grpFormat  = "  %-37s %s"

def displayTroves(repos, cfg, all = False, ls = False, ids = False,
                  sha1s = False, trove = "", versionStr = None):
    if trove:
	troves = [ trove ]
    else:
	# this returns a sorted list
	troves = [ x for x in repos.iterAllTroveNames() ]

    if versionStr or ls or ids or sha1s:
	if all:
	    log.error("--all cannot be used with queries which display file "
		      "lists")
	    return
	for troveName in troves:
	    _displayTroveInfo(repos, cfg, troveName, versionStr, ls, ids, sha1s)
	    continue
    else:
	if all:
	    versions = repos.getTroveVersionList(troves)
	else:
            versions = repos.getAllTroveLeafs(troves)

	flavors = repos.getTroveVersionFlavors(versions)

	for troveName in troves:
            if not flavors[troveName]:
                log.error('No versions for "%s" were found in the repository',
                          troveName)
                continue

	    for version in flavors[troveName]:
		for flavor in flavors[troveName][version]:
		    if all:
			print "%-30s %-15s %s" % (troveName, flavor,
					    version.asString(cfg.defaultbranch))
		    elif not all and (flavor is None or cfg.flavor.satisfies(flavor)):
			print _troveFormat % (troveName, 
					    version.asString(cfg.defaultbranch))

def _displayTroveInfo(repos, cfg, troveName, versionStr, ls, ids, sha1s):
    try:
	troveList = repos.findTrove(cfg.installbranch, troveName, 
				    cfg.flavor, versionStr)
    except repository.PackageNotFound, e:
	log.error(str(e))
	return

    for trove in troveList:
	version = trove.getVersion()

	if ls:
	    fileL = [ (x[1], x[0], x[2]) for x in trove.iterFileList() ]
	    fileL.sort()
	    iter = repos.iterFilesInTrove(trove.getName(), trove.getVersion(),
                                          trove.getFlavor(), sortByPath = True, 
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
	    for (fileId, path, version) in trove.iterFileList():
		print "%s %s" % (fileId, path)
	elif sha1s:
	    for (fileId, path, version) in trove.iterFileList():
		file = repos.getFileVersion(fileId, version)
		if file.hasContents:
		    print "%s %s" % (file.contents.sha1(), path)
	else:
	    print _troveFormat % (troveName, version.asString(cfg.defaultbranch))

	    for (troveName, ver, flavor) in trove.iterTroveList():
		print _grpFormat % (troveName, ver.asString(cfg.defaultbranch))

	    fileL = [ (x[1], x[0], x[2]) for x in trove.iterFileList() ]
	    fileL.sort()
	    for (path, fileId, version) in fileL:
		print _fileFormat % (path, version.asString(cfg.defaultbranch))
