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
"""
Provides the output for the "conary repquery" command
"""

from repository import repository
import files
import log
import time

_troveFormat  = "%-39s %s"
_fileFormat = "    %-35s %s"
_grpFormat  = "  %-37s %s"

def displayTroves(repos, cfg, all = False, ls = False, ids = False,
                  sha1s = False, leaves = False, fullVersions = False,
		  info = False, tags = False, trove = "", versionStr = None):
    if trove:
	troves = [ trove ]
    else:
	# this returns a sorted list
	troves = [ x for x in 
		    repos.iterAllTroveNames(cfg.installLabel.getHost()) ]

    if versionStr or ls or ids or sha1s or info or tags:
	if all:
	    log.error("--all cannot be used with queries which display file "
		      "lists")
	    return
	for troveName in troves:
	    _displayTroveInfo(repos, cfg, troveName, versionStr, ls, ids, sha1s,
			      info, tags, fullVersions)
	    continue
    else:
	if all:
	    versions = repos.getTroveVersionList(cfg.installLabel.getHost(),
						 troves)
	elif leaves:
            versions = repos.getAllTroveLeafs(cfg.installLabel.getHost(), 
					      troves)
	else:
            versions = repos.getTroveLeavesByLabel(troves, cfg.installLabel)

	flavors = repos.getTroveVersionFlavors(versions)

	for troveName in troves:
            if not flavors[troveName]:
                log.error('No versions for "%s" were found in the repository',
                          troveName)
                continue

	    versionStrs = {}
	    if not fullVersions:
		# find the version strings to display for this trove; first
		# choice is just the version/release pair, if there are
		# conflicts try label/verrel, and if that doesn't work
		# conflicts display everything
		short = {}
		for version in flavors[troveName]:
		    v = version.trailingVersion().asString()
		    versionStrs[version] = v
		    if short.has_key(v):
			versionStrs = {}
			break
		    short[v] = True

		if not versionStrs:
		    short = {}
		    for version in flavors[troveName]:
			if version.hasParent():
			    v = version.branch().label().asString() + '/' + \
				version.trailingVersion().asString()
			else:
			    v = version.asString()

			versionStrs[version] = v
			if short.has_key(v):
			    versionStrs = {}
			    break
			short[v] = True

	    if not versionStrs:
		for version in flavors[troveName]:
		    versionStrs[version] = version.asString()

	    for version in flavors[troveName]:
		for flavor in flavors[troveName][version]:
		    if all:
			print "%-30s %-15s %s" % (troveName, flavor,
						  versionStrs[version])
		    elif not all and (flavor is None or cfg.flavor.satisfies(flavor)):
			print _troveFormat % (troveName, 
					      versionStrs[version])

def _displayTroveInfo(repos, cfg, troveName, versionStr, ls, ids, sha1s,
		      info, tags, fullVersions):
    try:
	troveList = repos.findTrove(cfg.installLabel, troveName, 
				    cfg.flavor, versionStr)
    except repository.PackageNotFound, e:
	log.error(str(e))
	return

    for trove in troveList:
	version = trove.getVersion()

	if ls:
	    outerTrove = trove
	    for trove in repos.walkTroveSet(outerTrove):
		iter = repos.iterFilesInTrove(trove.getName(), 
				trove.getVersion(), trove.getFlavor(), 
				sortByPath = True, withFiles = True)
		for (fileId, path, version, file) in iter:
		    if isinstance(file, files.SymbolicLink):
			name = "%s -> %s" %(path, file.target.value())
		    else:
			name = path

		    print "%s    1 %-8s %-8s %s %s %s" % \
			(file.modeString(), file.inode.owner(), 
			 file.inode.group(), 
			 file.sizeString(), file.timeString(), name)
	elif ids:
	    for (fileId, path, version) in trove.iterFileList():
		print "%s %s" % (fileId, path)
	elif tags:
	    iter = repos.iterFilesInTrove(trove.getName(), trove.getVersion(),
                                          trove.getFlavor(), sortByPath = True, 
					  withFiles = True)

	    for (fileId, path, version, fObj) in iter:
		print "%-59s %s" % (path, " ".join(fObj.tags))
	elif sha1s:
	    for (fileId, path, version) in trove.iterFileList():
		file = repos.getFileVersion(fileId, version)
		if file.hasContents:
		    print "%s %s" % (file.contents.sha1(), path)
	elif info:
	    buildTime = time.strftime("%c",
				time.localtime(version.timeStamps()[-1]))
	    print "%-30s %s" % \
		(("Name      : %s" % troveName,
		 ("Build time: %s" % buildTime)))

	    if fullVersions:
		print "Version   :", version.asString()
		print "Label     : %s" % version.branch().label().asString()

	    else:
		print "%-30s %s" % \
		    (("Version   : %s" % version.trailingVersion().asString()),
		     ("Label     : %s" % version.branch().label().asString()))

	    cl = trove.getChangeLog()
	    if cl:
		print "Change log: %s (%s)" % (cl.name, cl.contact)
		lines = cl.message.split("\n")[:-1]
		for l in lines:
		    print "    %s" % l
	else:
	    if fullVersions or len(troveList) > 1:
		print _troveFormat % (troveName, version.asString())
	    else:
		print _troveFormat % (troveName, 
				      version.trailingVersion().asString())

	    for (troveName, ver, flavor) in trove.iterTroveList():
		if fullVersions or ver.branch() != version.branch():
		    print _grpFormat % (troveName, ver.asString())
		else:
		    print _grpFormat % (troveName, 
					ver.trailingVersion().asString())

	    fileL = [ (x[1], x[0], x[2]) for x in trove.iterFileList() ]
	    fileL.sort()
	    for (path, fileId, ver) in fileL:
		if fullVersions or ver.branch() != version.branch():
		    print _fileFormat % (path, ver.asString())
		else:
		    print _fileFormat % (path, 
					 ver.trailingVersion().asString())
