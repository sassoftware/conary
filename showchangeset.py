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

def displayChangeSet(db, repos, cs, troveList, cfg, ls = False, tags = False,  
                     info=False, fullVersions=False, showChanges=False):
    (troves, hasVersions) = getTroves(cs, troveList) 
    
    if not (ls or tags):
        if hasVersions:
            includeChildTroves(cs, troves)
        # create display cache containing appropriate version strings 
        displayC = createDisplayCache(cs, troves, fullVersions)
        indent = ''
        # with no options, just display basic trove info
	for (troveName, version, flavor) in troves:
            trove = cs.newPackages[(troveName, version, flavor)]
            displayTroveHeader(trove, indent, displayC, fullVersions)
            indent = '  ' 
            if info:
                if trove.getRequires():
                    depformat('Requires', trove.getRequires())
                if trove.getProvides():
                    depformat('Provides', trove.getProvides())
    elif tags:
        # recurse over child troves
        includeChildTroves(cs, troves)
        for pkg in troves:
            trove = cs.newPackages[pkg]
            # XXX do we want to list tags on changed files as well?
            for (fileId, path, version) in trove.newFiles:
                change = cs.getFileChange(fileId)
                fileObj = files.ThawFile(change, fileId)
                if not fileObj.tags:
                    continue
                taglist = '[' + ' '.join(fileObj.tags) + ']' 
                print "%-59s %s" % (path, taglist)
    elif ls:
        includeChildTroves(cs, troves)
        displayC = createDisplayCache(cs, troves, fullVersions)
        indent = ''
        first = True
        for pkg in troves:
            trove = cs.newPackages[pkg]
            # only print the header if we are going to print some more
            # information or it is a primary trove in the changeset
            if (pkg in cs.primaryTroveList) or trove.newFiles:
                displayTroveHeader(trove, indent, displayC, fullVersions)
            else:
                continue
            printedData = False
            prefix = indent + 'New  '
            for (fileId, path, version) in trove.newFiles:
                # whoe file is in changeset, grab it locally
                change = cs.getFileChange(fileId)
                fileObj = files.ThawFile(change, fileId)
                display.printFile(fileObj, path, prefix=prefix)
                printedData = True
            if trove.changedFiles or trove.oldFiles:
                # try to grab the old version of the trove from 
                # the local DB and then from the repository
                try:
                    oldTrove = db.getTrove(trove.getName(), 
                                           trove.getOldVersion(), 
                                           trove.getOldFlavor())
                    troveLoc = db
                except repository.TroveMissing:
                    try:
                        oldTrove = repos.getTrove(trove.getName(), 
                                                  trove.getOldVersion(), 
                                                  trove.getOldFlavor())
                        troveLoc = repos
                    except repository.TroveMissing:
                        log.warning("cannot find changeset trove %s on " 
                              "local system, or in repository list;"
                              "not printing information about this " 
                              "trove" % trove.getName())
                        continue
                for (fileId, path, version) in trove.changedFiles:
                    # XXX we don't know where to actually get the new file
                    # version from -- it may not actually be anywhere if it is
                    # a local changeset
                    fileObj = troveLoc.getFileVersion(fileId, version)
                    if showChanges:
                        (oldPath, oldVersion) = oldTrove.getFile(fileId)
                        oldFileObj = troveLoc.getFileVersion(fileId, oldVersion)
                        printChangedFile(indent, fileObj, path, oldFileObj, 
                                        oldPath)
                    else:
                        if path is None:
                            (oldPath, oldVersion) = oldTrove.getFile(fileId)
                            path = oldPath
                        display.printFile(fileObj, path, 
                                          prefix=indent + 'Mod  ')
                    printedData = True
                prefix = indent + 'Del  '
                for fileId in trove.oldFiles:
                    (oldPath, oldVersion) = oldTrove.getFile(fileId)
                    fileObj = troveLoc.getFileVersion(fileId, oldVersion)
                    display.printFile(fileObj, oldPath, prefix=prefix)
                    printedData = True
            if info:
                if trove.getRequires():
                    depformat('Requires', trove.getRequires())
                if trove.getProvides():
                    depformat('Provides', trove.getProvides())
            indent = '  '
        for (troveName, version, flavor) in cs.oldPackages:
            print "remove %s %s" % (troveName, version.asString())

def printChangedFile(indent, f, path, oldF, oldPath):
    display.printFile(oldF, oldPath, prefix=indent+'Mod  ')
    #only print out data that has changed on the second line
    #otherwise, print out blank space
    mode = owner = group = size = time = name = ''
    if path != None:
        if isinstance(f, files.SymbolicLink):
            name = "%s -> %s" % (path, f.target.value())
        else:
            name = path
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
    print "%s---> %-10s      %-8s %-8s %8s %11s %s" % \
      (indent, mode, owner, group, size, time, name)

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
    for (troveName, version, flavor) in troves:
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
    for pkg in troves:
        trove = cs.newPackages[pkg]
        for subTroveName, changes in  trove.iterChangedTroves():
            (type, version, flavor) = changes[0]
            if type == '+':
                troves.append((subTroveName, version, flavor))

def getTroves(cs, troveList):
    if not troveList:
        return (cs.getPrimaryPackageList(), False)

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

    for (troveName, version, flavor) in cs.getPrimaryPackageList():
        troves = []
        for pkg in cs.newPackages.iterkeys():
            if pkg[0] == troveDefs[0][0]:
                if not troveDefs[0][1]:
                    troves = [pkg]
                    break
                elif pkg[1].trailingVersion().asString() == troveDefs[0][1]:
                    troves = [pkg]
                    break
        if not troves:
            print "No such troves %s in changeset" % troveList 

    return (troves, hasVersions)

