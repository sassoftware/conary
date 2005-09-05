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
Provides the output for the "conary query" command
"""

import files
import os
from lib import util
from lib import log
import time

from deps import deps
from lib.sha1helper import sha1ToString, md5ToString
from repository import repository
import updatecmd

_troveFormat  = "%-39s %s"
_troveFormatWithFlavor  = "%-39s %s%s"
_fileFormat = "    %-35s %s"
_grpFormat  = "  %-37s %s"
_grpFormatWithFlavor  = "  %-37s %s%s"
_chgFormat  = "  --> %-33s %s"

class DisplayCache:

    def __init__(self):
        self._cache = {}

    def cache(self, troveName, versionList, fullVersions=False):
        """Add entries to the DisplayCache
        
        Cache the correct display output, given a version list for a
        trove.  The assumption is that the shortest version string
        should be given, starting with the verison-release, followed
        by the label/version-release, and finally falling back and
        displaying the entire version
        """
        _cache = self._cache
        short = {}
        passed = True
        # find the version strings to display for this trove; first
        # choice is just the version/release pair, if there are
        # conflicts try label/verrel, and if that doesn't work
        # conflicts display everything
        if troveName not in _cache:

            _cache[troveName] = {}
        if fullVersions:
            for version in versionList:
                _cache[troveName][version] = version.asString()
            return
        passed = True
        for version in versionList:
            v = version.trailingRevision().asString()
            _cache[troveName][version] = v
            if short.has_key(v):
                # two versions have the same version/release
                passed = False
                break
            short[v] = True

        if not passed:
            short = {}
            passed = True
            for version in versionList:
                if version.hasParentVersion():
                    v = version.branch().label().asString() + '/' + \
                        version.trailingRevision().asString()
                else:
                    v = version.asString()

                _cache[troveName][version] = v
                if short.has_key(v):
                    # two versions have the same label
                    passed = False 
                    break
                short[v] = True

        if not passed:
            for version in versionList:
                _cache[troveName][version] = version.asString()

    def clearCache(self, name):
        """Cleans out the cache entry for a trove for garbage collection"""
        del self._cache[name]

    def cacheAll(self, troveDict, fullVersions=False):
        """Add entries to the DisplayCache from a dictionary
        
        Prepares the version cache to choose the correct 
        version to display. TroveDict should be a trove[name] => [versions]
        dict. 
        XXX eventually this should be a [name] => [(version,flavor)]
        So that we can print the short version if they differ in flavors
        """
        for troveName in troveDict.keys():
            self.cache(troveName, troveDict[troveName], fullVersions)
        
    def __getitem__(self, (troveName, version)):
        return self._cache[troveName][version]

    def __call__(self, troveName, version):
        return self._cache[troveName][version]

def printFile(fileObj, path, prefix='', verbose=True, tags=False, sha1s=False,
              pathId=None, pathIds=False):
    taglist = ''
    sha1 = ''
    id = ''

    if verbose and isinstance(fileObj, files.SymbolicLink):
        name = "%s -> %s" % (path, fileObj.target())
    else:
        name = path
    if tags and fileObj.tags:
        taglist = ' [' + ' '.join(fileObj.tags) + ']' 
    if sha1s:
        if hasattr(fileObj, 'contents') and fileObj.contents:
            sha1 = sha1ToString(fileObj.contents.sha1()) + ' '
        else:
            sha1 = ' '*41

    if pathIds and pathId:
        id = md5ToString(pathId) + ' ' + sha1ToString(fileObj.fileId()) + ', '
    if verbose: 
        print "%s%s%s%s    1 %-8s %-8s %s %s %s%s" % \
          (prefix, id, sha1, fileObj.modeString(), fileObj.inode.owner(), fileObj.inode.group(), 
            fileObj.sizeString(), fileObj.timeString(), name, taglist)
    else:
        print "%s%s%s%s" % (id, sha1,path, taglist)

def parseTroveStrings(troveNameList):
    troveNames = []
    hasVersions = False
    hasFlavors = False
    for item in troveNameList:
        (name, version, flavor) = updatecmd.parseTroveSpec(item)

        if version is not None:
            hasVersions = True
        if flavor is not None:
            hasFlavors = True
        troveNames.append((name, version, flavor))

    return troveNames, hasVersions, hasFlavors

def _formatFlavor(flavor):
    if flavor:
        return '\n   ' + deps.formatFlavor(flavor)
    else:
        return '\n   None'

def _displayOneTrove(n,v,f, fullVersions, showFlavor, format=_troveFormat):
    params = [n]
    if fullVersions:
        params.append(v.asString())
    else:
        params.append(v.trailingRevision().asString())

    if showFlavor:
        params.append(_formatFlavor(f))
        format = format + '%s'
    print format % tuple(params)

def _printOneTroveName(db, troveName, troveDict, fullVersions, info):
    displayC = DisplayCache()
    displayC.cache(troveName, troveDict[troveName].keys(), fullVersions)
    for version in troveDict[troveName]:
        if info or len(troveDict[troveName][version]) > 1:
            # if there is more than one instance of a version
            # installed, show the flavor information
            for flavor in troveDict[troveName][version]:
                print _troveFormatWithFlavor %(troveName,
                                               displayC[troveName, version],
                                               _formatFlavor(flavor))
        else:
            print _troveFormat %(troveName, displayC[troveName, version])

def displayTroves(db, troveNameList = [], pathList = [], ls = False, 
                  ids = False, sha1s = False, fullVersions = False, 
                  tags = False, info=False, deps=False, showBuildReqs = False,
                  showFlavors = False, showDiff = False):
    (troveNames, hasVersions, hasFlavors) = \
        parseTroveStrings(troveNameList)

    pathList = [os.path.abspath(util.normpath(x)) for x in pathList]
    
    if not troveNames and not pathList:
	troveNames = [ (x, None, None) for x in db.iterAllTroveNames() ]
	troveNames.sort()

    if True not in (hasVersions, hasFlavors, ls, ids, sha1s, tags, deps, 
                    showBuildReqs, info, showDiff):
        troveDict = {}
        for path in pathList:
            for trove in db.iterTrovesByPath(path):
                n, v, f = trove.getName(), trove.getVersion(), trove.getFlavor()
                if n not in troveDict:
                    troveDict[n] = {}
                if v not in troveDict[n]:
                    troveDict[n][v] = []
                if f not in troveDict[n][v]:
                    troveDict[n][v].append(f)
        # now convert into a trove dict
        # throw away name, version variables, we know they're empty
        # because hasVersions and hasFlavors are false
        leaves = {}
        for troveName, version, flavor in troveNames:
            leaves[troveName] = db.getTroveVersionList(troveName)
        flavors = db.getAllTroveFlavors(leaves)
        db.queryMerge(flavors, troveDict) 
        for troveName in sorted(flavors.iterkeys()):
            if not flavors[troveName]: 
                log.error("%s is not installed", troveName)
            _printOneTroveName(db, troveName, flavors, fullVersions, 
                                                       info=info or showFlavors)
        return
    for path in pathList:
        for trove in db.iterTrovesByPath(path):
	    localTrv = db.getTrove(trove.getName(), trove.getVersion(),
				   trove.getFlavor(), pristine = False)
	    _displayTroveInfo(db, trove, localTrv, 
			      ls, ids, sha1s, fullVersions, tags, 
                              info, deps, showBuildReqs, showFlavors,
                              showDiff)

    for (troveName, versionStr, flavor) in troveNames:
        try:
            for (n,v,f) in db.findTrove(None, 
                                        (troveName, versionStr, flavor)):
                # db.getTrove returns the pristine trove by default
                trv = db.getTrove(n,v,f)
                localTrv = db.getTrove(n,v,f, pristine = False)
                _displayTroveInfo(db, trv, localTrv, ls, ids, sha1s, 
				  fullVersions, tags, info, deps, 
                                  showBuildReqs, showFlavors, showDiff)
        except repository.TroveNotFound:
            if versionStr:
                log.error("version %s of trove %s is not installed",
                          versionStr, troveName)
            else:
                log.error("trove %s is not installed", troveName)
        
def _displayTroveInfo(db, trove, localTrv, ls, ids, sha1s, 
                      fullVersions, tags, info, showDeps, showBuildReqs,
                      showFlavors, showDiff):

    version = trove.getVersion()
    flavor = trove.getFlavor()

    if ls or tags:
        outerTrove = trove
        for trove in db.walkTroveSet(outerTrove):
            iter = db.iterFilesInTrove(trove.getName(), trove.getVersion(),
                                       trove.getFlavor(),
                                       sortByPath = True, withFiles = True)
            for (pathId, path, fileId, version, file) in iter:
                if tags: 
                    if not file.tags:
                        continue
                    taglist = '[' + ' '.join(file.tags) + ']'
                    print "%-40s   %s" % (path, taglist)
                    continue
                printFile(file, path)
    elif ids:
        for (pathId, path, fileId, version) in trove.iterFileList():
            print "%s %s, %s" % (md5ToString(pathId), sha1ToString(fileId), path)
    elif sha1s:
        for (pathId, path, fileId, version) in trove.iterFileList():
            file = db.getFileVersion(pathId, fileId, version)
            if file.hasContents:
                print "%s %s" % (sha1ToString(file.contents.sha1()), path)
    elif info:
        troveVersion = trove.getVersion()
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

        print "%-30s %s" % \
            (("Size      : %s" % size,
             ("Pinned    : %s" % db.trovesArePinned([ (trove.getName(), 
                              trove.getVersion(), trove.getFlavor()) ])[0])))
        print "Flavor    : %s" % deps.formatFlavor(trove.getFlavor())
        print "Requires  : %s" % trove.getRequires()
    elif showBuildReqs:
        for (n,v,f) in sorted(trove.getBuildRequirements()):
            params = [n]
            if fullVersions:
                params.append(v.asString())
            else:
                params.append(v.trailingRevision().asString())
            if showFlavors:
                params.append(_formatFlavor(f))
                format = _troveFormatWithFlavor
            else:
                format = _troveFormat
            print format % tuple(params)
    elif showDeps:
        troveList = [trove]
        while troveList:
            trove = troveList[0]
            troveList = troveList[1:]
            newTroves = sorted([ x for x in db.walkTroveSet(trove)][1:], 
                                key=lambda y: y.getName())
            troveList = newTroves + troveList

            print trove.getName()
            for name, dep in (('Provides', trove.getProvides()),
                              ('Requires', trove.getRequires())):
                print '  %s:' %name
                if not dep:
                    print '     None'
                else:
                    lines = str(dep).split('\n')
                    for l in lines:
                        print '    ', l
            print 
    else:
        _displayOneTrove(trove.getName(), trove.getVersion(),
                         trove.getFlavor(), fullVersions,
                         showFlavors)
	changes = localTrv.diff(trove)[2]
	changesByOld = dict(((x[0], x[1], x[3]), x) for x in changes)
        troveList = trove.iterTroveList()
        # XXX we _could_ display the local trove version for conary q,
        # but that would be a change in behavior...
        #if showDiff:
        #    troveList = trove.iterTroveList()
        #else:
        #    troveList = localTrv.iterTroveList()
        for (troveName, ver, fla) in sorted(troveList):
            if not showDiff:
                _displayOneTrove(troveName, ver, fla,
                             fullVersions or ver.branch() != version.branch(),
                             showFlavors, format=_grpFormat)
            else:
                change = changesByOld.get((troveName, ver, fla), None)
                if change: 
                    newVer, newFla = change[2], change[4]
                    needFlavor = (showFlavors or 
                                  (newFla is not None and newFla != fla))
                else:
                    needFlavor = showFlavors

                _displayOneTrove(troveName, ver, fla,
                             fullVersions or ver.branch() != version.branch(),
                             needFlavor, format=_grpFormat)
                change = changesByOld.get((troveName, ver, fla), None)
                if change: 
                    if newVer is None:
                        try:
                            tups = db.trovesByName(troveName)
                        except:
                            print '  --> (Deleted or Not Installed)'
                        else:
                            print ('  --> Not linked to parent trove - potential'
                                   ' replacements:')
                            for (dummy, newVer, newFla) in tups:
                                _displayOneTrove(troveName, newVer, newFla,
                                 fullVersions or newVer.branch() != ver.branch(),
                                 needFlavor, format=_chgFormat)
                    else:
                        _displayOneTrove(troveName, newVer, newFla,
                             fullVersions or newVer.branch() != ver.branch(),
                                 needFlavor, format=_chgFormat)


	    
        if showDiff:
            fileL = [ (x[1], x[0], x[2], x[3]) for x in trove.iterFileList() ]
        else:
            fileL = [ (x[1], x[0], x[2], x[3]) for x in localTrv.iterFileList()]
        fileL.sort()
        for (path, pathId, fileId, version) in fileL:
            if fullVersions:
                print _fileFormat % (path, version.asString())
            else:
                print _fileFormat % (path, 
                                     version.trailingRevision().asString())
