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

import display
import files
from lib import log
import time
from repository import repository
import sys

from lib.sha1helper import sha1ToString


def usage():
    print "conary showcs   <changeset> [trove]"
    print "showcs flags:   "
    print "                --full-versions   Print full version strings instead of attempting to shorten them" 
    print "                --info            Print dependency information about the troves"
    print "                --ls              (Recursive) list file contents"
    print "                --show-changes    For modifications, show the old file version next to new one"
    print "                --tags            Show tagged files (use with ls to show tagged and untagged)"
    print "                --sha1s           Show sha1s for files"
    print "                --ids             Show fileids"
    print "                --all             Combine above tags"
    print ""



def displayChangeSet(db, repos, cs, troveList, cfg, ls = False, tags = False,  
                     info=False, fullVersions=False, showChanges=False,
                    all=False, deps=False, sha1s=False, ids=False):
    (troves, hasVersions) = getTroves(cs, troveList) 
    if all:
        ls = tags = fullVersions = info = deps = True
    
    if not (ls or tags or sha1s or ids):
        if hasVersions:
            troves = includeChildTroves(cs, troves)
        # create display cache containing appropriate version strings 
        displayC = createDisplayCache(cs, troves, fullVersions)
        # with no options, just display basic trove info
	for (troveName, version, flavor), indent in troves:
            trove = cs.newPackages[(troveName, version, flavor)]
            displayTroveHeader(trove, indent, displayC, fullVersions)
            if info:
                if trove.getRequires():
                    depformat('Requires', trove.getRequires())
                if trove.getProvides():
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
            fileList = []
            # create a file list of each file type
            for (fileId, path, version) in trove.getNewFileList():
                fileList.append(('New', fileId, path, version))
            for (fileId, path, version) in trove.getChangedFileList():
                fileList.append(('Mod', fileId, path, version))
            for fileId in trove.getOldFileList():
                fileList.append(('Del', fileId, None, None))
            if trove.changedFiles or trove.oldFiles:
                oldTrove = getOldTrove(trove, db, repos)
                if not oldTrove:
                    print (
                    """*** WARNING: Cannot find changeset trove %s on 
                       local system, or in repository list,
                       not printing information about this trove""") % trove.getName()
                    continue

            for (cType, fileId, path, version) in fileList:
                if cType == 'New':
                    # when file is in changeset, grab it locally
                    change = cs.getFileChange(fileId)
                    fileObj = files.ThawFile(change, fileId)
                elif cType == 'Mod':
                    fileObj = getFileVersion(fileId, version, db, repos) 
                    (oldPath, oldVersion) = oldTrove.getFile(fileId)
                    oldFileObj = getFileVersion(fileId, oldVersion, db, repos)
                    if showChanges:
                        # special option for showing both old and new version
                        # of changed files
                        printChangedFile(indent + ' ', fileObj, path, 
                            oldFileObj, oldPath, tags=tags, sha1s=sha1s, 
                            fileId=fileId, fileIds=ids)
                        continue
                    if not path:
                        path = oldPath
                elif cType == 'Del':
                    (oldPath, oldVersion) = oldTrove.getFile(fileId)
                    fileObj = getFileVersion(fileId, oldVersion, db, repos)
                    path = oldPath
                if tags and not ls and not fileObj.tags:
                    continue
                prefix = indent + ' ' + cType + '  '
                display.printFile(fileObj, path, prefix=prefix, verbose=ls, 
                                                 tags=tags, sha1s=sha1s,
                                                 fileId=fileId, fileIds=ids)
                printedData = True
            if printedData:
                print
            if info:
                if trove.getRequires():
                    depformat('Requires', trove.getRequires())
                if trove.getProvides():
                    depformat('Provides', trove.getProvides())
        for (troveName, version, flavor) in cs.oldPackages:
            print "remove %s %s" % (troveName, version.asString())

def printChangedFile(indent, f, path, oldF, oldPath, tags=False, sha1s=False, fileIds=False, fileId=None ):
    display.printFile(oldF, oldPath, prefix=indent+'Mod  ', tags=tags, 
                        sha1s=sha1s, fileIds=fileIds, fileId=fileId)
    #only print out data that has changed on the second line
    #otherwise, print out blank space
    mode = owner = group = size = time = name = ''
    if path != None:
        if isinstance(f, files.SymbolicLink):
            name = "%s -> %s" % (path, f.target.value())
        else:
            name = path
    space = ''
    if fileIds and fileId:
        space += ' '*41
    if sha1s:
        space += ' '*41

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
    if not tags or not fileObj.tags:
        taglist = ''
    else:
        taglist = ' [' + ' '.join(fileObj.tags) + ']' 
    print "%s---> %s%-10s      %-8s %-8s %8s %11s %s%s" % \
      (indent, space, mode, owner, group, size, time, name, taglist)

def depformat(name, dep):
    print '\t%s: %s' %(name, str(dep).replace('\n', '\n\t%s'
                                            %(' '* (len(name)+2))))
def displayTroveHeader(trove, indent, displayC, fullVersions):
    """ Displays very basic information about the trove """
    troveName = trove.getName()
    version = trove.getNewVersion()
    if trove.isAbsolute():
        verFrom =  " (absolute)"
    elif trove.getOldVersion():
        verFrom = " (from %s)" % displayC[troveName,trove.getOldVersion()]
    else:
        verFrom =  " (new) "
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
        ppl =  cs.getPrimaryPackageList()
        if ppl:
            troveList = [ x for x in ppl]
        else:
            print "Note: changeset has no primary troves, showing all troves"
            troveList = [ x for x in cs.newPackages]
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

    for (troveName, version, flavor) in cs.newPackages:
        troves = []
        for pkg in cs.newPackages:
            if pkg[0] == troveDefs[0][0]:
                if not troveDefs[0][1]:
                    troves = [pkg]
                    break
                elif pkg[1].trailingVersion().asString() == troveDefs[0][1]:
                    troves = [pkg]
                    break
        if not troves:
            print "No such troves %s in changeset" % troveList 
    return ([ (x, '') for x in troves], hasVersions)

def getOldTrove(trove, db, repos):
    try:
        oldTrove = db.getTrove(trove.getName(), trove.getOldVersion(), 
                                trove.getOldFlavor())
    except repository.TroveMissing:
        try:
            oldTrove = repos.getTrove(trove.getName(), trove.getOldVersion(), 
                                      trove.getOldFlavor())
        except repository.TroveMissing:
            return None
    return oldTrove

def getFileVersion(fileId, version, db, repos):
    try:
        fileObj = db.getFileVersion(fileId, version)
    except KeyError:
        try:
            fileObj = repos.getFileVersion(fileId, version) 
        except KeyError:
            return None
    return fileObj
