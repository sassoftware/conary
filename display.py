#
# Copyright (c) 2004 Specifix, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import files

_troveFormat  = "%-39s %s"
_fileFormat = "    %-35s %s"
_grpFormat  = "  %-37s %s"

def displayTroves(db, cfg, ls = False, ids = False, sha1s = False,
                  fullVersions = False, trove = "", versionStr = None):
    if trove:
	troves = [ trove ]
    else:
	troves = [ x for x in db.iterAllTroveNames() ]
	troves.sort()

    for troveName in troves:
	if versionStr or ls or ids or sha1s:
	    _displayTroveInfo(db, cfg, troveName, versionStr, ls, ids, sha1s,
			      fullVersions)
	    continue
	else:
	    l = db.getTroveVersionList(troveName)

	    for version in l:
		if fullVersions:
		    print _troveFormat % (troveName, version.asString())
		else:
		    print _troveFormat % (troveName, 
                                          version.trailingVersion().asString())

def _displayTroveInfo(db, cfg, troveName, versionStr, ls, ids, sha1s, 
		      fullVersions):
    troveList = db.findTrove(troveName, versionStr)

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
	    if fullVersions:
		print _troveFormat % (troveName, version.asString())
	    else:
		print _troveFormat % (troveName, 
				      version.trailingVersion().asString())

	    for (troveName, ver, flavor) in trove.iterTroveList():
		if fullVersions:
		    print _grpFormat % (troveName, ver.asString())
		else:
		    print _grpFormat % (troveName, 
					ver.trailingVersion().asString())

	    fileL = [ (x[1], x[0], x[2]) for x in trove.iterFileList() ]
	    fileL.sort()
	    for (path, fileId, version) in fileL:
		if fullVersions:
		    print _fileFormat % (path, version.asString())
		else:
		    print _fileFormat % (path, 
					 version.trailingVersion().asString())
