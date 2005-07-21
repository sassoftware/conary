#
# Copyright (c) 2005 rpath, Inc.
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

from build import cook
from deps import deps
from repository import changeset
from repository import netclient
import updatecmd
from lib import log
import versions
import sys

def CloneTrove(cfg, targetBranch, troveSpec):

    def _createVersion(targetBranchVersionList, sourceVersion):
        assert(targetBranchVersionList)
        # sort oldest to newest
        targetBranchVersionList.sort()
        upstream = sourceVersion.trailingRevision().getVersion()

        # find the newest version in the list which shares the same version
        # as the new one we're going to commit
        match = None
        for possibleVersion in targetBranchVersionList:
            if possibleVersion.trailingRevision().getVersion() == upstream:
                match = possibleVersion

        if not match:
            match = targetBranchVersionList[0].branch().createVersion(
                        versions.Revision("%s-0" % 
                            sourceVersion.trailingRevision().getVersion()))

        match.incrementSourceCount()
                             
        return match

    def _isUphill(ver, uphill):
        uphillBranch = uphill.branch()
        verBranch = ver.branch()
        if uphillBranch == verBranch:
            return True

        while verBranch.hasParentBranch():
            verBranch = verBranch.parentBranch()
            if uphillBranch == verBranch:
                return True
        
        return False 

    parts = troveSpec.split('=', 1) 
    troveName = parts[0]
    if len(parts) == 1:
        versionSpec = None
    else:
        versionSpec = parts[1]

    if troveName.startswith("fileset"):
        print "File sets can not be clone."
        sys.exit(1)
    elif troveName.startswith("group"):
        print "Groups can not be cloned."
        sys.exit(1)
    elif not troveName.endswith(":source"):
        print "Source components are required for cloning."
        sys.exit(1)

    targetBranch = versions.VersionFromString(targetBranch)

    repos = netclient.NetworkRepositoryClient(cfg.repositoryMap)

    srcTroveList = repos.findTrove(cfg.installLabelPath, 
                                (troveName, versionSpec, None))
    assert(len(srcTroveList) == 1)
    srcTroveName, srcTroveVersion = srcTroveList[0][:2]

    allTroveInfo = repos.getTrovesBySource(srcTroveName, srcTroveVersion)
    try:
        currentVersionList = repos.getTroveVersionsByBranch(
            { srcTroveName : { targetBranch : None } } )[srcTroveName].keys()
    except KeyError:
        print "No versions of %s exist on branch %s." \
                    % (srcTroveName, targetBranch.asString()) 
        return 1

    allTroveInfo.append((srcTroveName, srcTroveVersion, deps.DependencySet()))
    allTroves = repos.getTroves(allTroveInfo)

    currentVersionList.sort()
    newSourceVersion = _createVersion(currentVersionList, srcTroveVersion)
    newBinaryVersion = newSourceVersion.copy()
    newBinaryVersion.incrementBuildCount()

    if not _isUphill(srcTroveVersion, newSourceVersion):
        log.error("clone only supports cloning troves to parent branches")
        return 1
    elif srcTroveVersion.branch().label().getHost() != \
         newSourceVersion.branch().label().getHost():
        log.error("clone only supports cloning troves within a single "
                  "repository")
        return 1
    
    # This works because it's a package. That means that all of the troves we
    # enounter, even referenced ones, will have the same version

    cs = changeset.ChangeSet()
    uphillCache = {}

    for trv in allTroves:
        trv.troveInfo.clonedFrom.set(trv.getVersion())

        oldVersion = trv.getVersion()
        if oldVersion == srcTroveVersion:
            newVersion = newSourceVersion
        else:
            newVersion = newBinaryVersion
            
        trv.changeVersion(newVersion)

        for (name, version, flavor) in trv.iterTroveList():
            byDefault = trv.includeTroveByDefault(name, version, flavor)
            trv.delTrove(name, version, flavor, False)
            trv.addTrove(name, newVersion, flavor, byDefault = byDefault)

        for (pathId, path, fileId, version) in trv.iterFileList():
            changeVersion = uphillCache.get(version, None)
            if changeVersion is None:
                changeVersion = _isUphill(version, newVersion)
                uphillCache[version] = changeVersion

            if changeVersion:
                trv.updateFile(pathId, path, newVersion, fileId)

        trvCs = trv.diff(None, absolute = True)[0]
        cs.newTrove(trvCs)

        if ":" not in trv.getName():
            cs.addPrimaryTrove(trv.getName(), trv.getVersion(), 
                               trv.getFlavor())

    repos.commitChangeSet(cs)
