#
# Copyright (c) 2008 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.

import os

from conary.build import nextversion
from conary.deps import deps
from conary.repository import changeset
from conary.repository import filecontents
from conary import files
from conary import trove
from conary import versions

def makePathId():
    """returns 16 random bytes, for use as a pathId"""
    return os.urandom(16)

class ClientNewTrove:
    def _createTroves(self, troveAndPathList):
        cs = changeset.ChangeSet()
        troveList = [ x[0] for x in troveAndPathList ]
        previousVersionMap = self._targetNewTroves(troveList)
        self._addAllNewFiles(cs, troveAndPathList, previousVersionMap)
        for trove in troveList:
            trove.computeDigests()
            trvCs = trove.diff(None, absolute=True)[0]
            cs.newTrove(trvCs)
        return cs

    def createSourceTrove(self, name, label, upstreamVersion, pathDict,
                          changeLog, factory = None,
                          pkgCreatorData = None):
        """
            Create a source trove.
            @param name: trove name of source components.
            @type name: str
            @param label: trove label
            @type label: str
            @param upstreamVersion: upstream version of source component
            @type upstreamVersion: str
            @param pathDict: Dictionary mapping path strings to
            conaryclient.filetypes._File objects, which represent the contents
            of each file
            @type pathDict: dict(str: conaryclient.filetypes._File)
            @param changeLog: Change log associated with this source trove.
            @type changeLog: changelog.ChangeLog
            @param factory: designate a factory associated with this source
            trove.
            @type factory: str
            @type pkgCreatorData: arbitrary string set in
            _TROVEINFO_TAG_PKGCREATORDATA
            @type pkgCreatorData: str
        """
        if not name.endswith(':source'):
            raise RuntimeError('Only source components allowed')
        versionStr = '/%s/%s-1' % (label, upstreamVersion)
        version = versions.VersionFromString(versionStr).copy()
        version.resetTimeStamps()
        troveObj = trove.Trove(name, version, deps.Flavor(),
                               changeLog = changeLog)
        troveObj.setFactory(factory)
        if pkgCreatorData:
            troveObj.troveInfo.pkgCreatorData.set(pkgCreatorData)

        return self._createTroves([(troveObj, pathDict)])


    def _targetNewTroves(self, troveList):
        # construct a map of the troveSpecs about to be created
        # to pre-existing troves
        repos = self.getRepos()
        previousVersionMap = {}
        versionDict = {}
        troveSpecs = {}
        trovesSeen = set()
        for troveObj in troveList:
            name, version, flavor = troveObj.getNameVersionFlavor()
            if not name.endswith(':source'):
                raise RuntimeError('Only source components allowed')
            versionSpec = '%s/%s' % (version.trailingLabel(),
                                     version.trailingRevision().getVersion())
            if (name, versionSpec) in trovesSeen:
                raise RuntimeError('Cannot create multiple versions of %s with same version' % name)

            trovesSeen.add((name, versionSpec))
            troveSpecs[name, str(version.trailingLabel()), None] = troveObj

        results = repos.findTroves(None, troveSpecs, None, allowMissing=True,
                                   getLeaves=False)
        for troveSpec, troveObj in troveSpecs.iteritems():
            branch = troveObj.getVersion().branch()
            revision = troveObj.getVersion().trailingRevision()
            tupList = results.get(troveSpec, [])
            newVersion = nextversion.nextSourceVersion(branch, revision, [x[1] for x in tupList])
            troveObj.changeVersion(newVersion)
            if tupList:
                # add the latest source component to the previousVersionMap
                previousVersionMap[troveObj.getNameVersionFlavor()] = sorted(tupList, key = lambda x: x[1])[-1]
        return previousVersionMap

    def _addAllNewFiles(self, cs, troveAndPathList, previousVersionMap):
        repos = self.getRepos()
        existingTroves = repos.getTroves(previousVersionMap.values(),
                                         withFiles=True)
        troveDict = dict(zip(previousVersionMap.values(), existingTroves))
        for trove, pathDict in troveAndPathList:
            existingTroveTup = previousVersionMap.get(
                                        trove.getNameVersionFlavor(), None)
            if existingTroveTup:
                existingTrove = troveDict[existingTroveTup]
            else:
                existingTrove = None
            self._addNewFiles(cs, trove, pathDict, existingTrove)

    def _removeOldPathIds(self, troveObj):
        allPathIds = [x[0] for x in  troveObj.iterFileList()]
        for pathId in allPathIds:
            troveObj.removePath(pathId)

    def _addNewFiles(self, cs, trove, pathDict, existingTrove):
        existingPaths = {}
        pathIds = {}
        if existingTrove:
            for pathId, path, fileId, fileVer in existingTrove.iterFileList():
                existingPaths[path] = (fileId, pathId, fileVer)
        self._removeOldPathIds(trove)

        for path, fileObj in pathDict.iteritems():
            if path in existingPaths:
                oldFileId, pathId, oldFileVer = existingPaths[path]
            else:
                pathId = makePathId()
                oldFileId = oldFileVer = None
            f = fileObj.get(pathId)
            f.flags.isSource(set = True)
            newFileId = f.fileId()
            if oldFileId == newFileId:
                newFileVer = oldFileVer
            else:
                newFileVer = trove.getVersion()
                cs.addFile(None, newFileId, f.freeze())

                contentType = changeset.ChangedFileTypes.file
                contents = hasattr(fileObj, 'contents') and fileObj.contents
                if contents:
                    cs.addFileContents(pathId, newFileId, contentType,
                            contents, cfgFile = f.flags.isConfig())

            trove.addFile(pathId, path, newFileVer, newFileId)

    def getFilesFromTrove(self, name, version, flavor, fileList=None, trv=None):
        repos = self.getRepos()
        if not fileList:
            fileList = []
            trv = repos.getTrove(name, version, flavor, withFiles=True)
            fileObs = repos.getFileVersions([(x[0], x[2], x[3]) \
                    for x in trv.iterFileList()])
            for idx, (pathId, path, fileId, fileVer) in \
                    enumerate(trv.iterFileList()):
                fileObj = fileObs[idx]
                if fileObj.hasContents:
                    fileList.append((path))
        contents = repos.getFileContentsFromTrove(name, version, flavor,
                                                  fileList)
        return dict((x[0], x[1].get()) for x in zip(fileList, contents))

