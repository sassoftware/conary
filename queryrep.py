#
# Copyright (c) 2004-2005 Specifix, Inc.
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

from deps import deps
from repository import repository
import display
import files
from lib import log
import metadata
import time
import versions

from lib.sha1helper import sha1ToString

_troveFormat  = display._troveFormat
_troveFormatWithFlavor = display._troveFormatWithFlavor
_fileFormat = display._fileFormat
_grpFormat  = display._grpFormat

def displayTroves(repos, cfg, troveList = [], all = False, ls = False, 
                  ids = False, sha1s = False, leaves = False, 
                  fullVersions = False, info = False, tags = False,
                  deps = False):
    hasVersions = False
    hasFlavors = False

    if troveList:
        (troves, hasVersions, hasFlavors) = \
                    display.parseTroveStrings(troveList, cfg.flavor)
    else:
	# this returns a sorted list
        troves = []
        for label in cfg.installLabelPath:
            troves += [ (x, None, None) for x in repos.troveNames(label) ]
            troves.sort()

    if hasVersions or hasFlavors or ls or ids or sha1s or info or tags or deps:
	if all:
	    log.error("--all cannot be used with queries which display file "
		      "lists")
	    return
	for troveName, versionStr, flavor in troves:
	    _displayTroveInfo(repos, cfg, troveName, versionStr, ls, ids, sha1s,
			      info, tags, deps, fullVersions, flavor)
	    continue
    else:
	if all or leaves:
            repositories = {}
            allHosts = [ x.getHost() for x in cfg.installLabelPath ]
            for (name, versionStr, flavor) in troves:
                if versionStr and versionStr[0] != '@':
                    hostList = versions.Label(versionStr).getHost()
                else:
                    hostList = allHosts
                    
                for host in hostList:
                    d = repositories.setdefault(host, {})
                    l = d.setdefault(name, [])
                    l.append(flavor)

            if all:
                fn = repos.getTroveVersionList
            else:
                fn = repos.getAllTroveLeaves

            flavors = {}
            for host, names in repositories.iteritems():
                d = fn(host, names)
                repos.queryMerge(flavors, d)
	else:
            flavors = {}
            for label in cfg.installLabelPath:
                d = repos.getTroveLeavesByLabel([ x[0] for x in troves], label,
                                                flavorFilter = cfg.flavor)
                repos.queryMerge(flavors, d)

        displayc = display.DisplayCache()
	for troveName, versionStr, flavor in troves:
            if not flavors.has_key(troveName):
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

            displayc.cache(troveName, flavors[troveName], fullVersions)

            versionList = flavors[troveName].keys()
            versionList.sort()
            
	    for version in reversed(versionList):
		for flavor in flavors[troveName][version]:
		    if all:
			print _troveFormatWithFlavor %(
                            troveName, displayc[troveName, version],
                            display._formatFlavor(flavor))
		    elif not all and (flavor is None or cfg.flavor.satisfies(flavor)):
			print _troveFormat % (troveName, 
					      displayc[troveName, version])
            displayc.clearCache(troveName)

                

def _displayTroveInfo(repos, cfg, troveName, versionStr, ls, ids, sha1s,
		      info, tags, showDeps, fullVersions, flavor):
    withFiles = ids

    if flavor is None:
        flavor = cfg.flavor

    try:
	troveList = repos.findTrove(cfg.installLabelPath, troveName, 
				    flavor, versionStr,
                                    acrossRepositories = True,
                                    withFiles = withFiles)
    except repository.TroveNotFound, e:
	log.error(str(e))
	return

    # FIXME use TroveInfo here
    if ':' in troveName:
        package = troveName[:troveName.find(':')]
    else:
        package = troveName
    sourceName = package + ":source"
    try:
        sourceTrove = repos.findTrove(cfg.installLabelPath, sourceName,
                                      cfg.flavor, versionStr,
                                      acrossRepositories = True,
                                      withFiles = False)[0]
    except repository.TroveNotFound, e:
        sourceTrove = None

    for trove in troveList:
	version = trove.getVersion()
        if ls or tags or sha1s or ids:
            outerTrove = trove
            for trove in repos.walkTroveSet(outerTrove):
                iter = repos.iterFilesInTrove(trove.getName(), 
                            trove.getVersion(), trove.getFlavor(), 
                            sortByPath = True, withFiles = True)
                for (pathId, path, fileId, version, fObj) in iter:
                    display.printFile(fObj, path, verbose=ls, tags=tags, 
                                      sha1s=sha1s, pathId=pathId, pathIds=ids)
	elif info:
            if trove.getBuildTime():
                buildTime = time.strftime("%c",
                                    time.localtime(trove.getBuildTime()))
            else:
                buildTime = "(unknown)"

            if trove.getSize():
                size = "%s" % trove.getSize()
            else:
                size = "(unknown)"

	    print "%-30s %s" % \
		(("Name      : %s" % trove.getName(),
		 ("Build time: %s" % buildTime)))

	    if fullVersions:
		print "Version   :", version.asString()
		print "Label     : %s" % version.branch().label().asString()

	    else:
		print "%-30s %s" % \
		    (("Version   : %s" % version.trailingRevision().asString()),
		     ("Label     : %s" % version.branch().label().asString()))

            print "Size      : %s" % size
            print "Flavor    : %s" % deps.formatFlavor(trove.getFlavor())

            if sourceTrove:
                metadata.showDetails(repos, cfg, sourceTrove.getName(), version.branch())

                cl = sourceTrove.getChangeLog()
                if cl:
                    print "Change log: %s (%s)" % (cl.getName(), cl.getContact())
                    lines = cl.getMessage().split("\n")[:-1]
                    for l in lines:
                        print "    " + l
        elif showDeps:
            for name, dep in (('Provides', trove.provides),
                              ('Requires', trove.requires)):
                print '%s:' %name
                if not dep:
                    print '     None'
                else:
                    lines = str(dep).split('\n')
                    for l in lines:
                        print '    ', l
	else:
	    if fullVersions or len(troveList) > 1:
		print _troveFormat % (trove.getName(), version.asString())
	    else:
		print _troveFormat % (trove.getName(), 
				      version.trailingRevision().asString())

	    for (troveName, ver, flavor) in trove.iterTroveList():
		if fullVersions or ver.branch() != version.branch():
		    print _grpFormat % (troveName, ver.asString())
		else:
		    print _grpFormat % (troveName, 
					ver.trailingRevision().asString())

	    iter = repos.iterFilesInTrove(trove.getName(), trove.getVersion(),
                                          trove.getFlavor(), sortByPath = True, 
					  withFiles = False)
	    for (pathId, path, fileId, ver) in iter:
		if fullVersions or ver.branch() != version.branch():
		    print _fileFormat % (path, ver.asString())
		else:
		    print _fileFormat % (path, 
					 ver.trailingRevision().asString())
