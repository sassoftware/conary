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

def CloneTrove(cfg, targetBranch, troveSpecList):

    def _createSourceVersion(targetBranchVersionList, sourceVersion):
        assert(targetBranchVersionList)
        # sort oldest to newest
        targetBranchVersionList.sort()
        upstream = sourceVersion.trailingRevision().getVersion()

        # find the newest version in the list which shares the same version
        # as the new one we're going to commit (the list is sorted oldest
        # to newest)
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

    def _createBinaryVersions(versionMap, repos, srcVersion, infoList ):
        # this works on a single flavor at a time
        singleFlavor = list(set(x[2] for x in infoList))
        assert(len(singleFlavor) == 1)
        singleFlavor = singleFlavor[0]

        srcBranch = srcVersion.branch()

        q = {}
        for name, cloneSourceVersion, flavor in infoList:
            q[name] = { srcBranch : [ flavor ] }

        currentVersions = repos.getTroveLeavesByBranch(q, bestFlavor = True)
        dupCheck = {}

        for name, versionDict in currentVersions.iteritems():
            lastVersion = versionDict.keys()[0]
            assert(len(versionDict[lastVersion]) == 1)
            assert(versionDict[lastVersion][0] == singleFlavor)
            if lastVersion.getSourceVersion() == srcVersion:
                dupCheck[name] = lastVersion

        trvs = repos.getTroves([ (name, version, singleFlavor) for
                                    name, version in dupCheck.iteritems() ],
                               withFiles = False)

        for trv in trvs:
            assert(trv.getFlavor() == singleFlavor)
            name = trv.getName()
            info = (name, trv.troveInfo.clonedFrom(), trv.getFlavor())
            if info in infoList:
                # no need to reclone this one
                infoList.remove(info)
                versionMap[info] = trv.getVersion()

        if not infoList:
            return ([], None)

        buildVersion = cook.nextVersion(repos, 
                            [ x[0] for x in infoList ], srcVersion, flavor)
        return infoList, buildVersion

    targetBranch = versions.VersionFromString(targetBranch)
    repos = netclient.NetworkRepositoryClient(cfg.repositoryMap)

    cloneSources = []

    for troveSpec in troveSpecList:
        parts = troveSpec.split('=', 1) 
        troveName = parts[0]

        if troveName.startswith("fileset"):
            print "File sets cannot be cloned."
            sys.exit(1)
        elif troveName.startswith("group"):
            print "Groups cannot be cloned."
            sys.exit(1)


        spec = updatecmd.parseTroveSpec(troveSpec)
        cloneSources += repos.findTrove(cfg.installLabelPath, spec)

    # get the transitive closure
    allTroveInfo = set()
    allTroves = dict()
    while cloneSources:
        needed = []

        for info in cloneSources:
            if info not in allTroveInfo:
                needed.append(info)
                allTroveInfo.add(info)

        troves = repos.getTroves(needed, withFiles = False)
        allTroves.update(x for x in itertools.izip(needed, troves))
        cloneSources = [ x for x in itertools.chain(
                            *(t.iterTroveList() for t in troves)) ]

    # split out the binary and sources
    sourceTroveInfo = [ x for x in allTroveInfo 
                                if x[0].endswith(':source') ]
    binaryTroveInfo = [ x for x in allTroveInfo 
                                if not x[0].endswith(':source') ]

    del allTroveInfo
    versionMap = {}
    cloneJob = []

    # start off by finding new version numbers for the sources
    for info in sourceTroveInfo:
        name, version = info[:2]

        try:
            currentVersionList = repos.getTroveVersionsByBranch(
                { name : { targetBranch : None } } )[name].keys()
        except KeyError:
            print "No versions of %s exist on branch %s." \
                        % (name, targetBranch.asString()) 
            return 1

        currentVersionList.sort()

        # if the latest version of the source trove was cloned from the version
        # being cloned, we don't need to reclone the source
        trv = repos.getTrove(name, currentVersionList[-1],
                             deps.DependencySet(), withFiles = False)
        if trv.troveInfo.clonedFrom() == version:
            versionMap[info] = trv.getVersion()
        else:
            versionMap[info] = _createSourceVersion(currentVersionList, version)
            cloneJob.append((info, versionMap[info]))

    # now go through the binaries; sort them into buckets based on the
    # source trove each came from. we can't clone troves which came
    # from multiple versions of the same source
    trovesBySource = {}
    for info in binaryTroveInfo:
        trv = allTroves[info]
        source = trv.getSourceName()
        # old troves don't have source info
        assert(source is not None)

        l = trovesBySource.setdefault(trv.getSourceName(), 
                               (trv.getVersion().getSourceVersion(), []))
        if l[0] != trv.getVersion().getSourceVersion():
            log.error("Clone operation needs multiple vesrions of %s"
                        % trv.getSourceName())
        l[1].append(info)
        
    # this could be parallelized -- may not be worth the effort though
    for srcTroveName, (sourceVersion, infoList) in trovesBySource.iteritems():
        newSourceVersion = versionMap.get(
                (srcTroveName, sourceVersion, deps.DependencySet()), None)
        if newSourceVersion is None:
            # we're not cloning the source at the same time; try and fine
            # the source version which was used when the source was cloned
            try:
                currentVersionList = repos.getTroveVersionsByBranch(
                  { srcTroveName : { targetBranch : None } } ) \
                            [srcTroveName].keys()
            except KeyError:
                print "No versions of %s exist on branch %s." \
                            % (srcTroveName, targetBranch.asString()) 
                return 1

            trv = repos.getTrove(srcTroveName, currentVersionList[-1],
                                 deps.DependencySet(), withFiles = False)
            if trv.troveInfo.clonedFrom() == sourceVersion:
                newSourceVersion = trv.getVersion()
            else:
                log.error("Cannot find cloned source for %s=%s" %
                            (srcTroveName, sourceVersion.asString()))
                return 1

        # we know newSourceVersion is right at this point. now find the new
        # binary version for each flavor
        byFlavor = dict()
        for info in infoList:
            byFlavor.setdefault(info[2], []).append(info)

        for flavor, infoList in byFlavor.iteritems():
            cloneList, newBinaryVersion = \
                        _createBinaryVersions(versionMap, repos, 
                                              newSourceVersion, infoList)
            versionMap.update(
                dict((x, newBinaryVersion) for x in cloneList))
            cloneJob += [ (x, newBinaryVersion) for x in cloneList ]
            
    # check versions
    for info, newVersion in cloneJob:
        if not _isUphill(info[1], newVersion):
            log.error("clone only supports cloning troves to parent branches")
            return 1

    if not cloneJob:
        log.warning("Nothing to clone!")
        return 1

    allTroves = repos.getTroves([ x[0] for x in cloneJob ])

    cs = changeset.ChangeSet()

    allFilesNeeded = list()

    for (info, newVersion), trv in itertools.izip(cloneJob, allTroves):
        newVersionHost = newVersion.branch().label().getHost()

        trv.troveInfo.clonedFrom.set(trv.getVersion())

        oldVersion = trv.getVersion()
        trv.changeVersion(newVersion)

        # this loop only works for packages!
        for (name, version, flavor) in trv.iterTroveList():
            byDefault = trv.includeTroveByDefault(name, version, flavor)
            trv.delTrove(name, version, flavor, False)
            trv.addTrove(name, newVersion, flavor, byDefault = byDefault)

        for (pathId, path, fileId, version) in trv.iterFileList():
            changeVersion = _isUphill(version, newVersion)

            if changeVersion:
                trv.updateFile(pathId, path, newVersion, fileId)

            if version.branch().label().getHost() != newVersionHost:
                allFilesNeeded.append((pathId, fileId, version))

        # reset the signatures, because all the versions have now
        # changed, thus invalidating the old sha1 hash
        trv.troveInfo.sigs.reset()
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
