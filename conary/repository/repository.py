#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


# defines the Conary repository

import itertools
import time

from conary.repository import changeset, errors, filecontents
from conary import files, trove
from conary.lib import log, patch, sha1helper, util


class AbstractTroveDatabase:

    def commitChangeSet(self, cs):
        raise NotImplementedError

    def getFileVersion(self, pathId, fileId, version, withContents = 0):
        """
        Returns the file object for the given (pathId, fileId, version).
        """
        raise NotImplementedError

    def getFileVersions(self, l):
        """
        Returns the file objects for the (pathId, fileId, version) pairs in
        list; the order returns is the same order in the list.

        @param l:
        @type l: list
        @rtype list
        """
        for x in l:
            yield self.getFileVersion(*x)

    def getFileContents(self, fileList):
        # troveName, troveVersion, pathId, fileVersion, fileObj

        raise NotImplementedError

    def getTrove(self, troveName, version, flavor, withFiles=True):
        """
        Returns the trove which matches (troveName, version, flavor). If
        the trove does not exist, TroveMissing is raised.

        @param troveName: trove name
        @type troveName: str
        @param version: version
        @type version: versions.Version
        @param flavor: flavor
        @type flavor: deps.deps.Flavor
        @rtype: trove.Trove
        """
        raise NotImplementedError

    def getTroves(self, troveList):
        """
        Returns a list of trove objects which parallels troveList. troveList
        is a list of (troveName, version, flavor) tuples. Version can
        a version or a branch; if it's a branch the latest version of the
        trove on that branch is returned. If there is no match for a
        particular tuple, None is placed in the return list for that tuple.
        """
        raise NotImplementedError

    def iterAllTroveNames(self, serverName):
        """
        Returns a list of all of the troves contained in a repository.

        @param serverName: name of the server containing troves
        @type serverName: str
        @rtype: list of str
        """
        raise NotImplementedError

    def iterFilesInTrove(self, troveName, version, flavor,
                         sortByPath = False, withFiles = False):
        """
        Returns a generator for (pathId, path, fileId, version) tuples for all
        of the files in the trove. This is equivlent to trove.iterFileList(),
        but if withFiles is set this is *much* more efficient.

        @param withFiles: if set, the file object for the file is
        created and returned as the fourth element in the tuple.
        """
        raise NotImplementedError

class IdealRepository(AbstractTroveDatabase):

    def createBranch(self, newBranch, where, troveList = []):
        """
        Creates a branch for the troves in the repository. This
        operations is recursive, with any required troves and files
        also getting branched. Duplicate branches can be created,
        but only if one of the following is true:

          1. C{where} specifies a particular version to branch from
          2. the branch does not yet exist and C{where} is a label which matches multiple existing branches

        C{where} specifies the node branches are created from for the
        troves in C{troveList} (or all of the troves if C{troveList}
        is empty). Any troves or files branched due to inclusion in a
        branched trove will be branched at the version required by the
        object including it. If different versions of objects are
        included from multiple places, bad things will happen (an
        incomplete branch will be formed). More complicated algorithms
        for branch will fix this, but it's not clear doing so is
        necessary.

        @param newBranch: Label of the new branch
        @type newBranch: versions.Label
        @param where: Where the branch should be created from
        @type where: versions.Version or versions.Label
        @param troveList: Name of the troves to branch; empty list if all
        troves in the repository should be branched.
        @type troveList: list of str
        """
        raise NotImplementedError

    def getTroveVersionList(self, troveNameList):
        """
        Returns a dictionary indexed by the items in troveNameList. Each
        item in the dictionary is a list of all of the versions for that
        trove. If no versions are available for a particular trove,
        the dictionary entry for that trove's name is left empty.

        @param troveNameList: list trove names
        @type troveNameList: list of str
        @rtype: dict of lists
        """
        raise NotImplementedError

    def getAllTroveLeaves(self, troveNameList):
        """
        Returns a dictionary indexed by the items in troveNameList. Each
        item in the dictionary is a list of all of the leaf versions for
        that trove. If no branches are available for a particular trove,
        the dictionary entry for that trove's name is left empty.

        @param troveNameList: trove names
        @type troveNameList: list of str
        @rtype: dict of lists
        """
        raise NotImplementedError

    def getTroveLeavesByLabel(self, troveNameList, label):
        """
        Returns a dictionary indexed by the items in troveNameList. Each
        item in the dictionary is a list of all of the leaf versions for
        that trove which are on a branch w/ the given label. If a trove
        does not have any branches for the given label, the version list
        for that trove name will be empty. The versions returned include
        timestamps.

        @param troveNameList: trove names
        @type troveNameList: list of str
        @param label: label
        @type label: versions.Label
        @rtype: dict of lists
        """
        raise NotImplementedError

    def getTroveVersionsByLabel(self, troveNameList, label):
        """
        Returns a dictionary indexed by troveNameList. Each item in the
        dictionary is a list of all of the versions of that trove
        on the given branch, and newer versions appear later in the list.

        @param troveNameList: trove names
        @type troveNameList: list of str
        @param label: label
        @type label: versions.Label
        @rtype: dict of lists
        """
        raise NotImplementedError

    def getTroveLatestVersion(self, troveName, branch):
        """
        Returns the version of the latest version of a trove on a particular
        branch. If that branch doesn't exist for the trove, TroveMissing
        is raised. The version returned includes timestamps.

        @param troveName: trove name
        @type troveName: str
        @param branch: branch
        @type branch: versions.Version
        @rtype: versions.Version
        """
        raise NotImplementedError


    def getAllTroveFlavors(self, troveDict):
        """
        Converts a dictionary of the format retured by getAllTroveLeaves()
        to contain dicts of { version : flavorList } sets instead of
        containing lists of versions. The flavorList lists all of the
        flavors available for that vesrion of the trove.

        @type troveDict: dict
        @rtype: dict
        """
        raise NotImplementedError

    def queryMerge(self, target, source):
        """
        Merges the result of getTroveLatestVersions (and friends) into
        target.
        """
        for (name, verDict) in source.iteritems():
            if not target.has_key(name):
                target[name] = verDict
            else:
                for (version, flavorList) in verDict.iteritems():
                    if not target[name].has_key(version):
                        target[name][version] = flavorList
                    else:
                        target[name][version] += flavorList

class AbstractRepository(IdealRepository):
    ### Trove access functions

    def hasTroveByName(self, troveName):
        """
        Tests to see if the repository contains any version of the named
        trove.

        @param troveName: trove name
        @type troveName: str
        @rtype: boolean
        """
        raise NotImplementedError

    def hasTrove(self, troveName, version, flavor):
        """
        Tests if the repository contains a particular version of a trove.

        @param troveName: trove name
        @type troveName: str
        @rtype: boolean
        """
        raise NotImplementedError

    def getTroveInfo(self, infoType, troveList):
        """
        Returns a list of trove infoType streams for a list of (name, version, flavor)
        troves. if the trove does not exist, a TroveMissing exception is raised. If the
        requested infoType does not exist for a trove the returned list will have None at
        the corresponding position.

        @param infoType: trove._TROVE_INFO_*
        @type infoType: integer
        @param troveList: (name, versions.Version, deps.Flavor) of the troves needed.
        @type troveList: list of tuples
        @rtype: list of Stream objects or None
        """
        raise NotImplementedError

    def getTroveReferences(self, troveInfoList):
        """
        troveInfoList is a list of (name, version, flavor) tuples. For
        each (name, version, flavor) specied, return a list of the troves
        (groups and packages) which reference it (either strong or weak)
        (the user must have permission to see the referencing trove, but
        not the trove being referenced).
        """

    def getTroveDescendants(self, troveList):
        """
        troveList is a list of (name, branch, flavor) tuples. For each
        item, return the full version and flavor of each trove named
        Name which exists on a downstream branch from the branch
        passed in and is of the specified flavor. If the flavor is not
        specified, all matches should be returned. Only troves the
        user has permission to view should be returned.
        """

    ### File functions

    def __init__(self):
        assert(self.__class__ != AbstractRepository)

class ChangeSetJob:
    """
    ChangeSetJob provides a to-do list for applying a change set; file
    remappings should have been applied to the change set before it gets
    this far. Derivative classes can override these methods to change the
    behavior; for example, if addTrove is overridden no packages will
    make it to the database. The same holds for oldTrove.
    """

    storeOnlyConfigFiles = False

    def addTrove(self, oldTroveSpec, trove, trvCs, hidden = False):
        return self.repos.addTrove(trove, trvCs, hidden = hidden,
                                   oldTroveSpec = oldTroveSpec)

    def addTroveDone(self, troveId, mirror=False):
        self.repos.addTroveDone(troveId, mirror=mirror)

    def oldTrove(self, *args):
        pass

    def markTroveRemoved(self, name, version, flavor):
        raise NotImplementedError

    def invalidateRollbacks(self, set = None):
        if set is not None:
            self.invalidateRollbacksFlag = set
        else:
            return self.invalidateRollbacksFlag

    def addFileContents(self, sha1, fileContents, restoreContents, isConfig,
                        precompressed = False):
        # Note that the order doesn't matter, we're just copying
        # files into the repository. Restore the file pointer to
        # the beginning of the file as we may want to commit this
        # file to multiple locations.
        self.repos._storeFileFromContents(fileContents, sha1, restoreContents,
                                          precompressed = precompressed)

    def addFileVersion(self, troveInfo, pathId, path, fileId,
                       newVersion, fileStream = None, withContents = True):
        self.repos.addFileVersion(troveInfo, pathId, path, fileId, newVersion,
                                  fileStream = fileStream,
                                  withContents = withContents)

    def checkTroveCompleteness(self, trv):
        pass

    def checkTroveSignatures(self, trv, callback):
        assert(hasattr(callback, 'verifyTroveSignatures'))
        return callback.verifyTroveSignatures(trv)

    def _handleContents(self, pathId, fileId, fileStream,
                        configRestoreList, normalRestoreList,
                        oldFileId = None, oldVersion = None, oldfile = None,
                        restoreContents = True):
        # files with contents need to be tracked so we can stick
        # their contents in the archive "soon"; config files need
        # extra magic for tracking since we may have to merge
        # contents

        repos = self.repos

        if not fileStream or not restoreContents:
            # empty fileStream means there are no contents to restore
            return

        hasContents = (files.frozenFileHasContents(fileStream) and
                   not files.frozenFileFlags(fileStream).isEncapsulatedContent())
        if not hasContents:
            return

        fileFlags = files.frozenFileFlags(fileStream)
        if self.storeOnlyConfigFiles and not fileFlags.isConfig():
            return

        contentInfo = files.frozenFileContentInfo(fileStream)

        if fileFlags.isConfig():
            tup = (pathId, fileId, contentInfo.sha1(),
                   oldfile, fileId, oldVersion, oldFileId,
                   restoreContents)
            configRestoreList.append(tup)
        else:
            tup = (pathId, fileId, contentInfo.sha1(),
                   restoreContents)
            normalRestoreList.append(tup)

    def _containsFileContents(self, sha1iter):
        raise NotImplementedError

    def _filterRestoreList(self, configRestoreList, normalRestoreList):

        def filterOne(l, isConfig):
            newL = []
            inReposList = self._containsFileContents(tup[2] for tup in l)
            for tup, inRepos in itertools.izip(l, inReposList):
                if inRepos:
                    (pathId, fileId, sha1) = tup[0:3]
                    restoreContents = tup[-1]
                    # if we already have the file in the data store we can
                    # get the contents from there. This double store looks
                    # crazy, but we need it to get reference counting right.
                    fileContents = filecontents.FromDataStore(
                                     self.repos.contentsStore, sha1)
                    contType = changeset.ChangedFileTypes.file
                    self.addFileContents(sha1, fileContents,
                                         restoreContents, isConfig)
                else:
                    newL.append(tup)


            return newL

        configRestoreList = filterOne(configRestoreList, True)
        normalRestoreList = filterOne(normalRestoreList, False)

        return configRestoreList, normalRestoreList

    def _getCheckFilesList(self, csTrove, troveInfo, fileHostFilter,
            configRestoreList, normalRestoreList, restoreContents = True):
        checkFilesList = []

        for (pathId, path, fileId, newVersion) in csTrove.getNewFileList():
            if (fileHostFilter
                and newVersion.getHost() not in fileHostFilter):
                fileObj = None
                fileStream = None
            else:
                # New files don't always have streams in the changeset.
                # Filesets and clones don't include them for files which
                # are already known to be in the repository.
                fileStream = self.cs.getFileChange(None, fileId)

                if fileStream is None:
                    if not fileHostFilter:
                        # We are trying to commit to a database, but the
                        # diff returned nothing
                        raise KeyError

                    checkFilesList.append((pathId, fileId, newVersion))
                    fileObj = None
                else:
                    fileObj = files.ThawFile(fileStream, pathId)
                    if fileObj and fileObj.fileId() != fileId:
                        raise trove.TroveIntegrityError(csTrove.getName(),
                              csTrove.getNewVersion(), csTrove.getNewFlavor(),
                              "fileObj.fileId() != fileId in changeset "
                              "for pathId %s" %
                                    sha1helper.md5ToString(pathId))

            self.addFileVersion(troveInfo, pathId, path, fileId,
                                newVersion, fileStream = fileStream,
                                withContents = restoreContents)

            self._handleContents(pathId, fileId, fileStream, configRestoreList,
                                 normalRestoreList,
                                 restoreContents = restoreContents)

        return checkFilesList

    def _createInstallTroveObjects(self, fileHostFilter = [],
                                   callback = None, hidden = False,
                                   mirror = False, allowIncomplete = False,
                                   ):
        # create the trove objects which need to be installed; the
        # file objects which map up with them are created later, but
        # we do need a map from pathId to the path and version of the
        # file we need, so build up a dictionary with that information

        configRestoreList = []
        normalRestoreList = []
        checkFilesList = []

        newList = [ x for x in self.cs.iterNewTroveList() ]
        repos = self.repos
        cs = self.cs

        oldTrovesNeeded = [ x.getOldNameVersionFlavor() for x in
                                newList if x.getOldVersion() ]
        oldTroveIter = repos.iterTroves(oldTrovesNeeded, hidden = True)

        troveNo = 0
        for csTrove in newList:
            if csTrove.troveType() == trove.TROVE_TYPE_REMOVED:
                # deal with these later on to ensure any changesets which
                # are relative to removed troves can be processed
                continue

            troveNo += 1

            if callback:
                callback.creatingDatabaseTransaction(troveNo, len(newList))

            newVersion = csTrove.getNewVersion()
            oldTroveVersion = csTrove.getOldVersion()
            oldTroveFlavor = csTrove.getOldFlavor()
            troveName = csTrove.getName()
            troveFlavor = csTrove.getNewFlavor()

            if repos.hasTrove(troveName, newVersion, troveFlavor):
                raise errors.CommitError, \
                       "version %s of %s already exists" % \
                        (newVersion.asString(), csTrove.getName())

            if oldTroveVersion:
                newTrove = oldTroveIter.next()
                assert(newTrove.getNameVersionFlavor() ==
                        csTrove.getOldNameVersionFlavor())
                self.oldTrove(newTrove, csTrove, troveName, oldTroveVersion,
                              oldTroveFlavor)

                oldCompatClass = newTrove.getCompatibilityClass()

                if csTrove.isRollbackFence(
                                   oldCompatibilityClass = oldCompatClass,
                                   update = True):
                    self.invalidateRollbacks(set = True)
            else:
                newTrove = trove.Trove(csTrove.getName(), newVersion,
                                       troveFlavor, csTrove.getChangeLog(),
                                       setVersion = False)
                # FIXME: we reset the trove version
                # since in this case we need to use the fileMap returned
                # from applyChangeSet
                allowIncomplete = True

            newFileMap = newTrove.applyChangeSet(csTrove,
                                     needNewFileMap=True,
                                     allowIncomplete=allowIncomplete)
            if newTrove.troveInfo.incomplete():
                log.warning('trove %s has schema version %s, which contains'
                        ' information not handled by this client.  This'
                        ' version of Conary understands schema version %s.'
                        ' Dropping extra information.  Please upgrade conary.',
                        newTrove.getName(), newTrove.troveInfo.troveVersion(),
                        trove.TROVE_VERSION)

            self.checkTroveCompleteness(newTrove)

            self.checkTroveSignatures(newTrove, callback=callback)

            if oldTroveVersion is not None:
                troveInfo = self.addTrove(
                        (troveName, oldTroveVersion, oldTroveFlavor), newTrove,
                        csTrove, hidden = hidden)
            else:
                troveInfo = self.addTrove(None, newTrove, csTrove,
                                          hidden = hidden)

            checkFilesList += self._getCheckFilesList(csTrove, troveInfo,
                fileHostFilter, configRestoreList, normalRestoreList,
                restoreContents=True)

            for (pathId, path, fileId, newVersion) in \
                            newTrove.iterFileList(members = True,
                                                  capsules = True):
                # handle files which haven't changed; we know which those
                # are because they're in the merged trove but they aren't
                # in the newFileMap
                if pathId in newFileMap:
                    continue

                self.addFileVersion(troveInfo, pathId, path, fileId,
                                    newVersion, withContents=True)

            filesNeeded = []
            for i, (pathId, path, fileId, newVersion) in enumerate(csTrove.getChangedFileList()):
                tup = newFileMap[pathId]
                (oldPath, oldFileId, oldVersion) = tup[-3:]
                if path is None:
                    path = oldPath
                if fileId is None:
                    oldFileId = fileId
                if newVersion is None:
                    newVersion = oldVersion

                if (fileHostFilter
                    and newVersion.getHost() not in fileHostFilter):
                    fileStream = None
                elif (oldVersion == newVersion and oldFileId == fileId):
                    # the file didn't change between versions; we can just
                    # ignore it
                    fileStream = None
                else:
                    fileStream = cs.getFileChange(oldFileId, fileId)

                    if fileStream and fileStream[0] == "\x01":
                        if len(fileStream) != 2:
                            # This is awful, but this is how we say a file
                            # stream didn't change. Omitting it or at least ''
                            # would be nicer, but would break clients.
                            filesNeeded.append((i, (pathId, oldFileId,
                                                    oldVersion)))
                            continue

                        fileStream = None

                # None is the file object
                self.addFileVersion(troveInfo, pathId, path, fileId,
                                    newVersion, fileStream = fileStream,
                                    withContents=True)

                if fileStream is not None:
                    self._handleContents(pathId, fileId, fileStream,
                                    configRestoreList, normalRestoreList,
                                    oldFileId = oldFileId,
                                    oldVersion = oldVersion,
                                    oldfile = None,
                                    restoreContents=True)

            oldFileObjects = list(repos.getFileVersions(
                                        [ x[1] for x in filesNeeded ]))

            for i, (pathId, path, fileId, newVersion) in enumerate(csTrove.getChangedFileList()):
                if not filesNeeded or filesNeeded[0][0] != i:
                    continue
                filesNeeded.pop(0)

                tup = newFileMap[pathId]
                (oldPath, oldFileId, oldVersion) = tup[-3:]
                if path is None:
                    path = oldPath
                if fileId is None:
                    oldFileId = fileId
                if newVersion is None:
                    newVersion = oldVersion

                restoreContents = True

                diff = cs.getFileChange(oldFileId, fileId)

                # stored as a diff (the file type is the same
                # and (for *repository* commits) the file
                # is in the same repository between versions
                oldfile = oldFileObjects.pop(0)
                fileObj = oldfile.copy()
                fileObj.twm(diff, oldfile)
                assert(fileObj.pathId() == pathId)
                fileStream = fileObj.freeze()

                if (not mirror) and (
                    fileObj.hasContents and fileObj.contents.sha1() == oldfile.contents.sha1()
                    and not (fileObj.flags.isConfig() and not oldfile.flags.isConfig())):
                    # don't restore the contents here. we don't
                    # need them, and they may be relative to
                    # something from a different repository
                    restoreContents = False

                if fileObj and fileObj.fileId() != fileId:
                    raise trove.TroveIntegrityError(csTrove.getName(),
                          csTrove.getNewVersion(), csTrove.getNewFlavor(),
                          "fileObj.fileId() != fileId in changeset")

                self.addFileVersion(troveInfo, pathId, path, fileId,
                                    newVersion, fileStream = fileStream,
                                    withContents = restoreContents)

                self._handleContents(pathId, fileId, fileStream,
                                configRestoreList, normalRestoreList,
                                oldFileId = oldFileId,
                                oldVersion = oldVersion,
                                oldfile = oldfile,
                                restoreContents = restoreContents)

            del newFileMap
            self.addTroveDone(troveInfo, mirror=mirror)

        try:
            # we need to actualize this, not just get a generator
            list(repos.getFileVersions(checkFilesList))
        except errors.FileStreamMissing, e:
            info = [ x for x in checkFilesList if x[1] == e.fileId ]
            (pathId, fileId) = info[0][0:2]
            # Missing from the repo; raise exception
            raise errors.IntegrityError(
                "Incomplete changeset specified: missing pathId %s "
                "fileId %s" % (sha1helper.md5ToString(pathId),
                               sha1helper.sha1ToString(fileId)))

        return troveNo, configRestoreList, normalRestoreList

    @staticmethod
    def ptrCmp(a, b):
        if a[0] < b[0]:
            return -1
        elif a[0] > b[0]:
            return 1
        elif not a[1] or not b[1]:
            # just ptrId's are being used
            return 0
        elif a[1] < b[1]:
            return -1
        elif a[1] > b[1]:
            return 1

        return 0

    def __init__(self, repos, cs, fileHostFilter = [], callback = None,
                 resetTimestamps = False, allowIncomplete = False,
                 hidden = False, mirror = False,
                 preRestored=None,
                 ):

        self.repos = repos
        self.cs = cs
        self.invalidateRollbacksFlag = False

        newList = [ x for x in cs.iterNewTroveList() ]

        if resetTimestamps:
            now = time.time()
            slots = {}
            for trvCs in newList:
                slot = (trvCs.getName(), trvCs.getNewVersion().branch())
                slots.setdefault(slot, set()).add(trvCs)
            for slot, troves in slots.iteritems():
                # The latest trove in each LatestCache slot is reset to the
                # current server time. This avoids client clock skew causing
                # new troves to be older than existing troves. All other
                # versions in that slot must have a lower timestamp than the
                # latest one.

                # First, map out the latest timestamp for each unique version
                # in this slot. This establishes an order to the versions.
                nodes = {}
                for trvCs in troves:
                    ver = trvCs.getNewVersion()
                    nodes[str(ver)] = max(nodes.get(str(ver), 0),
                            ver.trailingRevision().timeStamp)
                # Now reset all the timestamps, using now for the latest node
                # and a lesser value for each preceding one.
                newStamp = now
                for ver, stamp in sorted(nodes.items(), key=lambda x: x[1],
                        reverse=True):
                    for trvCs in troves:
                        if str(trvCs.getNewVersion()) == ver:
                            trvCs.getNewVersion().trailingRevision(
                                    ).timeStamp = newStamp
                    newStamp -= 1

        troveNo, configRestoreList, normalRestoreList = \
            self._createInstallTroveObjects(fileHostFilter = fileHostFilter,
                                            callback = callback,
                                            mirror = mirror, hidden = hidden,
                                            allowIncomplete = allowIncomplete,
                                            )
        configRestoreList, normalRestoreList = \
            self._filterRestoreList(configRestoreList, normalRestoreList)

        # use a key to select data up to, but not including, the first
        # version.  We can't sort on version because we don't have timestamps
        configRestoreList.sort(key=lambda x: x[0:5])
        normalRestoreList.sort(key=lambda x: x[0:3])

        self._restoreConfig(cs, configRestoreList)
        self._restoreNormal(cs, normalRestoreList, preRestored)

        #del configRestoreList
        #del normalRestoreList

        for csTrove in newList:
            if csTrove.troveType() != trove.TROVE_TYPE_REMOVED:
                continue

            troveNo += 1

            if callback:
                callback.creatingDatabaseTransaction(troveNo, len(newList))

            self.markTroveRemoved(csTrove.getName(), csTrove.getNewVersion(),
                                  csTrove.getNewFlavor())

        for (troveName, version, flavor) in cs.getOldTroveList():
            trv = self.repos.getTrove(troveName, version, flavor)
            self.oldTrove(trv, None, troveName, version, flavor)

    def _restoreConfig(self, cs, configRestoreList):
        # config files are cached, so we don't have to worry about not
        # restoring the same fileId/pathId twice
        for (pathId, newFileId, sha1, oldfile, newFileId,
             oldVersion, oldFileId, restoreContents) in configRestoreList:
            if cs.configFileIsDiff(pathId, newFileId):
                (contType, fileContents) = cs.getFileContents(pathId, newFileId)

                # the content for this file is in the form of a
                # diff, which we need to apply against the file in
                # the repository
                assert(oldVersion)

                try:
                    f = self.repos.getFileContents(
                                    [(oldFileId, oldVersion, oldfile)])[0].get()
                except KeyError:
                    raise errors.IntegrityError(
                        "Missing file contents for pathId %s, fileId %s" % (
                                        sha1helper.md5ToString(pathId),
                                        sha1helper.sha1ToString(oldFileId)))

                oldLines = f.readlines()
                f.close()
                del f
                diff = fileContents.get().readlines()
                (newLines, failedHunks) = patch.patch(oldLines,
                                                      diff)
                fileContents = filecontents.FromString(
                                                "".join(newLines))

                assert(not failedHunks)
            else:
                # config files are not always available compressed (due
                # to the config file cache)
                fileContents = filecontents.FromChangeSet(cs, pathId, newFileId)

            self.addFileContents(sha1, fileContents, restoreContents, 1)

    def _restoreNormal(self, cs, normalRestoreList, preRestored):
        ptrRestores = []
        ptrRefsAdded = {}
        lastRestore = None         # restore each pathId,fileId combo once
        while normalRestoreList:
            (pathId, fileId, sha1, restoreContents) = normalRestoreList.pop(0)
            if preRestored is not None and sha1 in preRestored:
                continue
            if (pathId, fileId) == lastRestore:
                continue

            lastRestore = (pathId, fileId)

            try:
                (contType, fileContents) = cs.getFileContents(pathId, fileId,
                                                              compressed = True)
            except KeyError:
                raise errors.IntegrityError(
                        "Missing file contents for pathId %s, fileId %s" % (
                                        sha1helper.md5ToString(pathId),
                                        sha1helper.sha1ToString(fileId)))
            if contType == changeset.ChangedFileTypes.ptr:
                ptrRestores.append(sha1)
                target = util.decompressString(fileContents.get().read())

                if util.tupleListBsearchInsert(normalRestoreList,
                                (target[:16], target[16:], sha1, True),
                                self.ptrCmp):
                    # Item was inserted. This creates a reference in the
                    # datastore; keep track of it to prevent a duplicate
                    # reference count.
                    ptrRefsAdded[sha1] = True

                continue

            assert(contType == changeset.ChangedFileTypes.file)
            self.addFileContents(sha1, fileContents, restoreContents, 0,
                                 precompressed = True)

        for sha1 in ptrRestores:
            # Increment the reference count for items which were ptr's
            # to a different file.
            if sha1 in ptrRefsAdded:
                del ptrRefsAdded[sha1]
            else:
                self.addFileContents(sha1, None, False, 0)
