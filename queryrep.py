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
from lib import log
import time

from lib.sha1helper import sha1ToString

_troveFormat  = "%-39s %s"
_fileFormat = "    %-35s %s"
_grpFormat  = "  %-37s %s"

def displayTroves(repos, cfg, troveList = [], all = False, ls = False, 
                  ids = False, sha1s = False, leaves = False, 
                  fullVersions = False, info = False, tags = False):
    hasVersions = False

    if troveList:
        troves = []
        for item in troveList:
            i = item.find("=") 
            if i == -1:
                troves.append((item, None))
            else:
                hasVersions = True
                l = item.split("=")
                if len(l) > 2:
                    log.error("bad version string: %s", "=".join(l[1:]))
                    return
                    
                troves.append(tuple(l))
    else:
	# this returns a sorted list
        troves = []
        hosts = {}
        for label in cfg.installLabelPath:
            host = label.getHost()
            if hosts.has_key(host):
                continue
            hosts[host] = True

            troves += [ (x, None) for x in repos.iterAllTroveNames(host) ]

    if hasVersions or ls or ids or sha1s or info or tags:
	if all:
	    log.error("--all cannot be used with queries which display file "
		      "lists")
	    return
	for troveName, versionStr in troves:
	    _displayTroveInfo(repos, cfg, troveName, versionStr, ls, ids, sha1s,
			      info, tags, fullVersions)
	    continue
    else:
	if all or leaves:
            repositories = {}
            allHosts = [ x.getHost() for x in cfg.installLabelPath ]
            for (name, versionStr) in troves:
                if versionStr and versionStr[0] != '@':
                    hostList = versions.Label(versionStr).getHost()
                else:
                    hostList = allHosts
                    
                for host in hostList:
                    if repositories.has_key(host):
                        repositories[host].append(name)
                    else:
                        repositories[host] = [ name ]

            if all:
                fn = repos.getTroveVersionList
            else:
                fn = repos.getAllTroveLeafs

            versions = {}
            for host, names in repositories.iteritems():
                d = fn(host, names)
                for (name, verList) in d.iteritems():
                    if not versions.has_key(name):
                        versions[name] = verList
                    else:
                        versions[name] += (verList)
	else:
            versions = {}
            for label in cfg.installLabelPath:
                d = repos.getTroveLeavesByLabel([ x[0] for x in troves], label)
                for (name, verList) in d.iteritems():
                    if not versions.has_key(name):
                        versions[name] = verList
                    else:
                        versions[name] += (verList)

	flavors = repos.getTroveVersionFlavors(versions)

	for troveName, versionStr in troves:
            if not flavors[troveName]:
		if all or leaves:
		    log.error('No versions for "%s" were found in the '
			      'repository', troveName)
		else:
		    log.error('No versions with labels "%s" for "%s" were '
			      'found in the repository.', 
			      " ".join([ x.asString() for x 
                                            in cfg.installLabelPath ]),
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
	troveList = repos.findTrove(cfg.installLabelPath, troveName, 
				    cfg.flavor, versionStr,
                                    acrossRepositories = True)
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
		print "%s %s" % (sha1ToString(fileId), path)
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
		    print "%s %s" % (sha1ToString(file.contents.sha1()), path)
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
