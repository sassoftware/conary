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
Provides the output for the "conary showcs" command
"""

import time
import sys

#conary
import display
import files
from lib import log
from lib.sha1helper import sha1ToString
from repository import repository


def usage():
    print "conary showcs   <changeset> [trove[=version]]"
    print "showcs flags:   "
    print "                --full-versions   Print full version strings instead of "
    print "                                  attempting to shorten them" 
    print "                --deps            Print dependency information about the troves"
    print "                --ls              (Recursive) list file contents"
    print "                --show-changes    For modifications, show the old "
    print "                                  file version next to new one"
    print "                --tags            Show tagged files (use with ls to "
    print "                                  show tagged and untagged)"
    print "                --sha1s           Show sha1s for files"
    print "                --ids             Show fileids"
    print "                --all             Combine above tags"
    print ""



def displayChangeSet(db, repos, cs, troveList, cfg, ls = False, tags = False,  
                     fullVersions=False, showChanges=False,
                    all=False, deps=False, sha1s=False, ids=False):
    (troves, hasVersions) = getTroves(cs, troveList) 
    if all:
        showChanges = ls = tags = fullVersions = deps = True
    
    if not (ls or tags or sha1s or ids):
        if hasVersions or deps:
            troves = includeChildTroves(cs, troves)
        # create display cache containing appropriate version strings 
        displayC = createDisplayCache(cs, troves, fullVersions)
        # with no options, just display basic trove info
	for (troveName, version, flavor), indent in troves:
            trove = cs.newPackages[(troveName, version, flavor)]
            displayTroveHeader(trove, indent, displayC, fullVersions)
            if deps:
                depformat('Requires', trove.getRequires())
                depformat('Provides', trove.getProvides())
    else:
        troves = includeChildTroves(cs, troves)
        displayC = createDisplayCache(cs, troves, fullVersions)
        first = True
        for pkg, indent in troves:
            trove = cs.newPackages[pkg]
            # only print the header if we are going to print some more
            # information or it is a primary trove in the changeset
            if (pkg in cs.primaryTroveList) or trove.newFiles or trove.changedFiles:
                displayTroveHeader(trove, indent, displayC, fullVersions)
            else:
                continue
            printedData = False
            fileList = {}
            # create a file list of each file type
            for (pathId, path, fileId, version) in trove.getNewFileList():
                fileList[pathId] = ('New', pathId, path, fileId, version)
            for (pathId, path, fileId, version) in trove.getChangedFileList():
                fileList[pathId] = ('Mod', pathId, path, fileId, version)
            for pathId in trove.getOldFileList():
                fileList[pathId] = ('Del', pathId, None, None, None)
            if trove.changedFiles or trove.oldFiles:
                oldTrove = getOldTrove(trove, db, repos)
                if not oldTrove:
                    print (
                    """*** WARNING: Cannot find changeset trove %s on 
                       local system, or in repository list,
                       not printing information about this trove""") % trove.getName()
                    continue
            pathIds = fileList.keys()
            pathIds.sort()
            filesByPath = {}
            paths = {}
            # files stored in changesets are sorted by pathId, and must be
            # retrieved in that order.  But we want to display them by 
            # path.  So, retrieve the info from the changeset by pathId
            # and stored it in a dict to be retrieved after sorting by
            # path
            for pathId in pathIds:
                (cType, pathId, path, fileId, version) = fileList[pathId]

                if cType == 'New':
                    change = cs.getFileChange(None, fileId)
                    fileList[pathId] = (cType, pathId, path, fileId, version,
                                                                        change)
                elif cType == 'Mod':
                    (oldPath, oldFileId, oldVersion) = oldTrove.getFile(pathId)
                    filecs = cs.getFileChange(oldFileId, fileId)
                    fileList[pathId] = (cType, pathId, path, fileId, version,
                                        oldPath, oldFileId, filecs)
                    if not path:
                        path = oldPath
                elif cType == 'Del':
                    (path, fileId, version) = oldTrove.getFile(pathId)
                    fileList[pathId] = (cType, pathId, path, fileId, version)
                if path not in paths:
                    paths[path] = [pathId]
                else:
                    paths[path].append(pathId)

            pathNames = paths.keys()
            pathNames.sort()
            for path in pathNames:
                for pathId in paths[path]:
                    cType = fileList[pathId][0]
                    if cType == 'Del':
                        (cType, pathId, path, 
                         fileId, version) = fileList[pathId]
                        fileObj = getFileVersion(pathId, fileId, version, 
                                                                 db, repos)
                    elif cType == 'Mod':
                        (cType, pathId, path, fileId, version, 
                         oldPath, oldFileId, filecs) = fileList[pathId]
                        if showChanges or filecs[0] == '\x01':
                            # don't grab the old file object if we don't
                            # need it for displaying or for a three-way 
                            # merge to retrieve new file object
                             oldFileObj = getFileVersion(pathId, oldFileId, 
                                                         version, db, repos)
                        if filecs[0] == '\x01':
                            # file was stored as a diff
                            fileObj = oldFileObj.copy()
                            assert(oldFileObj.fileId() == oldFileId)
                            fileObj.twm(filecs, fileObj)
                            assert(oldFileObj.fileId() == oldFileId)
                        else:
                            fileObj = files.ThawFile(filecs, pathId)
                        if showChanges:
                            # special option for showing both old and new 
                            # version of changed files
                            printChangedFile(indent + ' ', fileObj, path, 
                                oldFileObj, oldPath, tags=tags, sha1s=sha1s, 
                                pathId=pathId, pathIds=ids)
                            continue
                        if path is None:
                            path = oldPath
                    elif cType == 'New':
                        (cType, pathId, path, 
                             fileId, version, change) = fileList[pathId]
                        fileObj = files.ThawFile(change, pathId)
                    if tags and not ls and not fileObj.tags:
                        continue
                    prefix = indent + ' ' + cType + '  '
                    display.printFile(fileObj, path, prefix=prefix, verbose=ls, 
                                                     tags=tags, sha1s=sha1s,
                                                     pathId=pathId, pathIds=ids)
                    printedData = True
            if printedData:
                print
            if deps:
                depformat('Requires', trove.getRequires())
                depformat('Provides', trove.getProvides())
        for (troveName, version, flavor) in cs.oldPackages:
            print "remove %s %s" % (troveName, version.asString())

def printChangedFile(indent, f, path, oldF, oldPath, tags=False, sha1s=False, pathIds=False, pathId=None ):
    display.printFile(oldF, oldPath, prefix=indent+'Mod  ', tags=tags, 
                        sha1s=sha1s, pathIds=pathIds, pathId=pathId)
    #only print out data that has changed on the second line
    #otherwise, print out blank space
    mode = owner = group = size = time = name = ''
    if path != None:
        if isinstance(f, files.SymbolicLink):
            name = "%s -> %s" % (path, f.target.value())
        else:
            name = path
    elif isinstance(f, files.SymbolicLink):
        if not isinstance(oldF, files.SymbolicLink):
            name = "%s -> %s" % (oldPath, f.target.value())
        elif f.target.value() != oldF.target.value():
                name = "%s -> %s" % (oldPath, f.target.value())
    space = ''
    if pathIds and pathId:
        space += ' '*33
    if sha1s:
        if hasattr(oldF, 'contents') and oldF.contents:
            oldSha1 = oldF.contents.sha1()
        if hasattr(f, 'contents') and f.contents:
            sha1 = f.contents.sha1()
        if sha1 != oldSha1:
            sha1 = sha1ToString(sha1) + ' '
        else:
            sha1 = ' '*41
    else:
        sha1 = ''

    if f.modeString() != oldF.modeString():
        mode = f.modeString()
    if f.inode.owner() != oldF.inode.owner():
        owner = f.inode.owner()
    if f.inode.group() != oldF.inode.group():
        group = f.inode.group()
    if f.sizeString() != oldF.sizeString():
        size = f.sizeString()
    if f.timeString() != oldF.timeString():
        time = f.timeString()
    if not tags or not f.tags:
        taglist = ''
    else:
        taglist = ' [' + ' '.join(f.tags) + ']' 
    print "%s---> %s%s%-10s      %-8s %-8s %8s %11s %s%s" % \
      (indent, space, sha1, mode, owner, group, size, time, name, taglist)

def depformat(name, dep):
    print '\t%s: %s' %(name, str(dep).replace('\n', '\n\t%s'
                                            %(' '* (len(name)+2))))
def displayTroveHeader(trove, indent, displayC, fullVersions):
    """ Displays very basic information about the trove """
    troveName = trove.getName()
    version = trove.getNewVersion()
    if trove.getIsRedirect():
        verFrom = " (redirect)"
    else:
        verFrom = ""

    if trove.isAbsolute():
        verFrom +=  " (absolute)"
    elif trove.getOldVersion():
        verFrom += " (from %s)" % displayC[troveName,trove.getOldVersion()]
    else:
        verFrom +=  " (new) "
    print "%-30s %-15s" % (indent + troveName, 
                           displayC[troveName, version] + verFrom)
    if trove.getOldFlavor() != trove.getNewFlavor():
        if trove.getOldFlavor():
            depformat('Old Flavor', trove.getOldFlavor())
        if trove.getNewFlavor():
            depformat('New Flavor', trove.getNewFlavor())

def createDisplayCache(cs, troves, fullVersions):
    troveDict = {}
    for (troveName, version, flavor), indent in troves:
        if troveName not in troveDict:
            troveDict[troveName] = []
        troveDict[troveName].append(version)
        trove = cs.newPackages[(troveName, version, flavor)]
        if trove.getOldVersion():
            troveDict[troveName].append(trove.getOldVersion())
    displayC = display.DisplayCache()
    displayC.cacheAll(troveDict, fullVersions)
    return displayC

def includeChildTroves(cs, troves):
    newList = []
    for pkg, indent in troves:
        newList.append((pkg, indent))
        trove = cs.newPackages[pkg]
        for subTroveName, changes in  trove.iterChangedTroves():
            (type, version, flavor) = changes[0]
            newList.append(((subTroveName, version, flavor), indent + ' '))
    return newList

def getTroves(cs, troveList):
    if not troveList:
        # create a list of all troves in this changeset, but only 
        # display those that are either primary packages, or are not
        # contained by any primary packages 
        # (other packages will be picked up if we are asked to recurse)
        allTroves = {}
        for trove in cs.iterNewPackageList():
            allTroves[trove.getName(), trove.getNewVersion(), \
                                                trove.getNewFlavor()] = True
        ppl =  cs.getPrimaryPackageList()
        if ppl:
            troveList = []
            for (name, version, flavor) in ppl:
                del allTroves[name, version, flavor]
                troveList.append((name, version, flavor))
                trove = cs.getNewPackageVersion(name, version, flavor)
                for subTroveName, changes in  trove.iterChangedTroves():
                    (type, version, flavor) = changes[0]
                    del allTroves[subTroveName, version, flavor]
            troveList.extend(allTroves.keys())
        else:
            print "Note: changeset has no primary troves, showing all troves"
            troveList = allTroves.keys()
        troveList.sort(lambda a, b: cmp(a[0], b[0]))
        return ([ (x, '') for x in troveList], False)
    hasVersions = False
    troveDefs = []
    for item in troveList:
        i = item.find("=") 
        if i == -1:
            troveDefs.append((item, None))
        else:
            hasVersions = True
            l = item.split("=")
            if len(l) > 2:
                log.error("bad version string: %s", "=".join(l[1:]))
                return
            troveDefs.append(tuple(l))

    troves = {}
    for reqName, reqVersion in troveDefs:
        for (troveName, version, flavor) in cs.newPackages:
            for pkg in cs.newPackages:
                if (pkg[0] == reqName or
                     (reqName[0] == ':' and pkg[0].endswith(reqName))):
                    if not reqVersion:
                        troves[pkg] = True
                        break
                    elif pkg[1].trailingRevision().asString() == reqVersion:
                        troves[pkg] = True
                        break
    if not troves:
        print "Trove(s) '%s' not found in changeset" % "', '".join(troveList) 
    troves = troves.keys()
    troves.sort()
    return ([ (x, '') for x in troves], hasVersions)

def getOldTrove(trove, db, repos):
    oldTrove = None
    if db is not None:
        try:
            # get the pristine version from the database, since that's
            # what the changeset was created against
            oldTrove = db.getTrove(trove.getName(), trove.getOldVersion(), 
                                    trove.getOldFlavor(), pristine=True)
        except repository.TroveMissing:
            pass
    if oldTrove is None:
        try:
            oldTrove = repos.getTrove(trove.getName(), trove.getOldVersion(), 
                                      trove.getOldFlavor())
        except repository.TroveMissing:
            pass
    return oldTrove

def getFileVersion(pathId, fileId, version, db, repos):
    fileObj = None
    if db:
        try:
            fileObj = db.getFileVersion(pathId, fileId, version)
        except KeyError:
            pass
    if not fileObj:
        try:
            fileObj = repos.getFileVersion(pathId, fileId, version) 
        except KeyError:
            pass
    return fileObj
