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
from lib import log

from lib.sha1helper import sha1ToString
from repository import repository

_troveFormat  = "%-39s %s"
_fileFormat = "    %-35s %s"
_grpFormat  = "  %-37s %s"

def displayTroves(db, troveNameList = [], pathList = [], ls = False, 
                  ids = False, sha1s = False, fullVersions = False, 
                  tags = False):
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

    if not troveNames and not pathList:
	troveNames = [ (x, None) for x in db.iterAllTroveNames() ]
	troveNames.sort()

    if not hasVersions and not ls and not ids and not sha1s and not tags:
        for path in pathList:
            for trove in db.iterTrovesByPath(path):
                troveNames.append((trove.getName(), [ trove.getVersion() ]))

        for troveName, versionList in troveNames:
            if not versionList:
                versionList = db.getTroveVersionList(troveName)
                if not versionList:
                    log.error("trove %s is not installed", troveName)
                    continue

            for version in versionList:
                if fullVersions:
                    print _troveFormat % (troveName, version.asString())
                else:
                    print _troveFormat % (troveName, 
                                          version.trailingVersion().asString())
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
