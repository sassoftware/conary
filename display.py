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
Provides the output for the "conary query" command
"""

import files
import os
from lib import util
from lib import log

from lib.sha1helper import sha1ToString
from repository import repository

_troveFormat  = "%-39s %s"
_fileFormat = "    %-35s %s"
_grpFormat  = "  %-37s %s"

class DisplayCache:

    def __init__(self):
        self._cache = {}

    def cache(self, troveName, versionList, fullVersions=False):
        """ Cache the correct display output, given a version list 
            for a trove.  The assumption is that the shortest 
            version string should be given, starting with the verison-release,
            followed by the label/version-release, and finally 
            falling back and displaying the entire version """
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
            v = version.trailingVersion().asString()
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
                if version.hasParent():
                    v = version.branch().label().asString() + '/' + \
                        version.trailingVersion().asString()
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
        """ Cleans out the cache entry for a trove for garbage collection """
        del self._cache[name]

    def cacheAll(self, troveDict, fullVersions=False):
        """ Prepares the version cache to choose the correct 
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
              fileId=None, fileIds=False):
    taglist = ''
    sha1 = ''
    id = ''

    if verbose and isinstance(fileObj, files.SymbolicLink):
        name = "%s -> %s" % (path, fileObj.target.value())
    else:
        name = path
    if tags and fileObj.tags:
        taglist = ' [' + ' '.join(fileObj.tags) + ']' 
    if sha1s:
        if hasattr(fileObj, 'contents') and fileObj.contents:
            sha1 = sha1ToString(fileObj.contents.sha1()) + ' '
        else:
            sha1 = ' '*41

    if fileIds and fileId:
        id = sha1ToString(fileId) + ' '
    if verbose: 
        print "%s%s%s%s    1 %-8s %-8s %s %s %s%s" % \
          (prefix, id, sha1, fileObj.modeString(), fileObj.inode.owner(), fileObj.inode.group(), 
            fileObj.sizeString(), fileObj.timeString(), name, taglist)
    else:
        print "%s%s%s%s" % (id, sha1,path, taglist)

def parseTroveStrings(troveNameList):
    troveNames = []
    hasVersions = False
    for item in troveNameList:
        if item.find("=") != -1:
            l = item.split("=")
            if len(l) > 2:
                log.error("versions may not contain =")
                return
            troveNames.append(tuple(l))
            hasVersions = True
        else:
            troveNames.append((item, None))
    return troveNames, hasVersions


def displayTroves(db, troveNameList = [], pathList = [], ls = False, 
                  ids = False, sha1s = False, fullVersions = False, 
                  tags = False):
   
    (troveNames, hasVersions) = parseTroveStrings(troveNameList)
    pathList = [os.path.abspath(util.normpath(x)) for x in pathList]
    
    if not troveNames and not pathList:
	troveNames = [ (x, None) for x in db.iterAllTroveNames() ]
	troveNames.sort()

    if not hasVersions and not ls and not ids and not sha1s and not tags:
        for path in pathList:
            for trove in db.iterTrovesByPath(path):
                troveNames.append((trove.getName(), [ trove.getVersion() ]))

        displayC = DisplayCache()
        for troveName, versionList in troveNames:
            if not versionList:
                versionList = db.getTroveVersionList(troveName)
                if not versionList:
                    log.error("trove %s is not installed", troveName)
                    continue

            displayC.cache(troveName, versionList, fullVersions)

            for version in versionList:
                print _troveFormat % (troveName, displayC[troveName, version])
            displayC.clearCache(troveName)
        return

    for path in pathList:
        for trove in db.iterTrovesByPath(path):
	    _displayTroveInfo(db, trove, ls, ids, sha1s, fullVersions)

    for (troveName, versionStr) in troveNames:
        try:
            for trove in db.findTrove(troveName, versionStr):
                _displayTroveInfo(db, trove, ls, ids, sha1s, fullVersions, tags)
        except repository.PackageNotFound:
            if versionStr:
                log.error("version %s of trove %s is not installed",
                          versionStr, troveName)
            else:
                log.error("trove %s is not installed", troveName)
        
def _displayTroveInfo(db, trove, ls, ids, sha1s, fullVersions, tags):

    version = trove.getVersion()

    if ls or tags:
        outerTrove = trove
        for trove in db.walkTroveSet(outerTrove):
            iter = db.iterFilesInTrove(trove.getName(), trove.getVersion(),
                                       trove.getFlavor(),
                                       sortByPath = True, withFiles = True)
            for (fileId, path, version, file) in iter:
                if tags: 
                    if not file.tags:
                        continue
                    taglist = '[' + ' '.join(file.tags) + ']'
                    print "%-40s   %s" % (path, taglist)
                    continue
                printFile(file, path)
    elif ids:
        for (fileId, path, version) in trove.iterFileList():
            print "%s %s" % (sha1ToString(fileId), path)
    elif sha1s:
        for (fileId, path, version) in trove.iterFileList():
            file = db.getFileVersion(fileId, version)
            if file.hasContents:
                print "%s %s" % (sha1ToString(file.contents.sha1()), path)
    else:
        if fullVersions:
            print _troveFormat % (trove.getName(), version.asString())
        else:
            print _troveFormat % (trove.getName(), 
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
