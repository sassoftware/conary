#
# Copyright (c) 2004-2005 rPath, Inc.
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
_formatFlavor  = display._formatFlavor
_displayOneTrove  = display._displayOneTrove
_troveFormatWithFlavor = display._troveFormatWithFlavor
_fileFormat = display._fileFormat
_grpFormat  = display._grpFormat

def displayTroves(repos, cfg, troveList = [], all = False, ls = False, 
                  ids = False, sha1s = False, leaves = False, 
                  fullVersions = False, info = False, tags = False,
                  deps = False, showBuildReqs = False, showFlavors = False):
    hasVersions = False
    hasFlavors = False

    emptyList = (not troveList)

    if troveList:
        (troves, hasVersions, hasFlavors) = \
                    display.parseTroveStrings(troveList)
    else:
	# this returns a sorted list
        troves = []
        for label in cfg.installLabelPath:
            troves += [ (x, None, None) for x in repos.troveNames(label) ]
            troves.sort()

    if True in (hasVersions, hasFlavors, ls, ids, sha1s, info, tags, deps,
                showBuildReqs):
	if all:
	    log.error("--all cannot be used with queries which display file "
		      "lists")
	    return
	for troveName, versionStr, flavor in troves:
	    _displayTroveInfo(repos, cfg, troveName, versionStr, ls, ids, sha1s,
			      info, tags, deps, fullVersions, flavor, 
                              showBuildReqs, showFlavors)
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
                d = dict.fromkeys([ x[0] for x in troves ],
                                  { label : cfg.flavor } )
                d = repos.getTroveLeavesByLabel(d, bestFlavor = True)
                repos.queryMerge(flavors, d)

        displayc = display.DisplayCache()
	for troveName, versionStr, flavor in troves:
            if not flavors.has_key(troveName):
		if all or leaves:
		    log.error('No versions for "%s" were found in the '
			      'repository', troveName)
		elif troveList:
                    # only display this error if the user has actually 
                    # requested a specific trove, otherwise, missing
                    # troves are a result of flavor filtering 
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
		    if all or len(flavors[troveName][version]) > 1 or showFlavors:
			print _troveFormatWithFlavor %(
                            troveName, displayc[troveName, version],
                            display._formatFlavor(flavor))
		    else:
                        if flavor is not None:
                            found = False
                            for installFlavor in cfg.flavor:
                                if installFlavor.satisfies(flavor):
                                    found = True
                                    break
                            if not found:
                                continue
			print _troveFormat % (troveName, 
					      displayc[troveName, version])
            displayc.clearCache(troveName)


def _displayTroveInfo(repos, cfg, troveName, versionStr, ls, ids, sha1s,
		      info, tags, showDeps, fullVersions, flavor, 
                      showBuildReqs, showFlavors):
    withFiles = ids

    try:
	troveList = repos.findTrove(cfg.installLabelPath, 
                                    (troveName, versionStr, flavor),
                                    cfg.flavor, 
                                    acrossLabels = True,
                                    acrossFlavors = True)
    except repository.TroveNotFound, e:
	log.error(str(e))
	return

    for (troveName, troveVersion, troveFlavor) in troveList:
        trove = repos.getTrove(troveName, troveVersion, troveFlavor, 
                               withFiles = withFiles)
        sourceName = trove.getSourceName()
        if sourceName:
            try:
                sourceTrove = repos.getTrove(sourceName, 
                        troveVersion.getSourceVersion(), deps.DependencySet(),
                        withFiles = False)
            except repository.TroveMissing:
                sourceTrove = None
        elif troveName.endswith(':source'):
            sourceTrove = trove
        else:
            sourceTrove = None

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
		print "Version   :", troveVersion.asString()
		print "Label     : %s" % \
                            troveVersion.branch().label().asString()

	    else:
		print "%-30s %s" % \
		    (("Version   : %s" % 
                                troveVersion.trailingRevision().asString()),
		     ("Label     : %s" % 
                                troveVersion.branch().label().asString()))

            print "%-30s" % ("Size      : %s" % size)
            print "Flavor    : %s" % deps.formatFlavor(trove.getFlavor())



            metadata.showDetails(repos, cfg, trove.getName(),
                                 troveVersion.branch(),
                                 sourceTrove)
            
            if sourceTrove:
                cl = sourceTrove.getChangeLog()
                if cl:
                    print "Change log: %s (%s)" % (cl.getName(), 
                                                   cl.getContact())
                    lines = cl.getMessage().split("\n")[:-1]
                    for l in lines:
                        print "    " + l
	elif showBuildReqs:
            for (n,v,f) in sorted(trove.getBuildRequirements()):
                _displayOneTrove(n,v,f, fullVersions, showFlavors)
        elif showDeps:
            troveList = [trove]
            while troveList:
                trove = troveList[0]
                troveList = troveList[1:]
                if trove.isCollection():
                    newTroves = sorted(
                                [ x for x in repos.walkTroveSet(trove)][1:], 
                                key=lambda y: y.getName())
                    troveList = newTroves + troveList
                print trove.getName()
                for name, dep in (('Provides', trove.provides.deps),
                                  ('Requires', trove.requires.deps)):
                    print '  %s:' %name
                    if not dep:
                        print '     None'
                    else:
                        lines = str(dep).split('\n')
                        for l in lines:
                            print '    ', l
                print
	else:
            _displayOneTrove(trove.getName(),trove.getVersion(),
                             trove.getFlavor(), 
                             fullVersions or len(troveList) > 1, 
                             showFlavors)
	    for (name, ver, flavor) in sorted(trove.iterTroveList()):
                _displayOneTrove(name, ver, flavor,
                        fullVersions or ver.branch() != troveVersion.branch(),
                        showFlavors, format=_grpFormat)

	    iter = repos.iterFilesInTrove(troveName, troveVersion, troveFlavor,
                                          sortByPath = True, withFiles = False)
	    for (pathId, path, fileId, ver) in iter:
		if fullVersions or ver.branch() != troveVersion.branch():
		    print _fileFormat % (path, ver.asString())
		else:
		    print _fileFormat % (path, 
					 ver.trailingRevision().asString())
