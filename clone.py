#
# Copyright (c) 2005 rPath, Inc.
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
import itertools
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
        print "File sets cannot be cloned."
        sys.exit(1)
    elif troveName.startswith("group"):
        print "Groups cannot be cloned."
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
    
    # This works because it's a package. That means that all of the troves we
    # enounter, even referenced ones, will have the same version

    cs = changeset.ChangeSet()
    uphillCache = {}

    newVersionHost = newSourceVersion.branch().label().getHost()
    allFilesNeeded = list()

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

            if version.branch().label().getHost() != newVersionHost:
                allFilesNeeded.append((pathId, fileId, version))

        trvCs = trv.diff(None, absolute = True)[0]
        cs.newTrove(trvCs)

        if ":" not in trv.getName():
            cs.addPrimaryTrove(trv.getName(), trv.getVersion(), 
                               trv.getFlavor())

    # the list(set()) removes duplicates
    newFilesNeeded = []
    for (pathId, newFileId, newFileVersion) in list(set(allFilesNeeded)):

        fileHost = newFileVersion.branch().label().getHost()
        if fileHost == newVersionHost:
            # the file is already present in the repository
            continue

        newFilesNeeded.append((pathId, newFileId, newFileVersion))

    fileObjs = repos.getFileVersions(newFilesNeeded)
    contentsNeeded = []
    pathIdsNeeded = []
    
    for (pathId, newFileId, newFileVersion), fileObj in \
                        itertools.izip(newFilesNeeded, fileObjs):
        (filecs, contentsHash) = changeset.fileChangeSet(pathId, None, fileObj)
        cs.addFile(None, newFileId, filecs)
        
        if fileObj.hasContents:
            contentsNeeded.append((newFileId, newFileVersion))
            pathIdsNeeded.append(pathId)

    contents = repos.getFileContents(contentsNeeded)
    for pathId, (fileId, fileVersion), fileCont, fileObj in \
            itertools.izip(pathIdsNeeded, contentsNeeded, contents, fileObjs):
        cs.addFileContents(pathId, changeset.ChangedFileTypes.file, 
                           fileCont, cfgFile = fileObj.flags.isConfig(), 
                           compressed = False)

    repos.commitChangeSet(cs)
