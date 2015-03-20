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


# implements a db-based repository

import cPickle
import errno
import itertools
import os
import sys
import tempfile
import traceback

from conary import files, trove, callbacks
from conary.deps import deps
from conary.lib import util, openpgpfile, openpgpkey
from conary.repository import changeset, errors, filecontents
from conary.repository.datastore import (DataStoreRepository, DataStore,
        DataStoreSet, ShallowDataStore, FlatDataStore)
from conary.repository.repository import AbstractRepository
from conary.repository.repository import ChangeSetJob
from conary.repository.netrepos.repo_cfg import CfgContentStore


class FilesystemChangeSetJob(ChangeSetJob):
    def __init__(self, repos, cs, *args, **kw):
        self.mirror = kw.get('mirror', False)
        self.requireSigs = kw.pop('requireSigs', False)
        self.callback = kw.get('callback', False)

        self.addTroveSetStart(repos, cs)
        ChangeSetJob.__init__(self, repos, cs, *args, **kw)
        repos.troveStore.addTroveSetDone(self.callback)

    def addTroveSetStart(self, repos, cs):
        newDirNames = set()
        newBaseNames = set()
        oldTroves = []
        for i, csTrove in enumerate(cs.iterNewTroveList()):
            if csTrove.getOldVersion():
                oldTroves.append(csTrove.getOldNameVersionFlavor())

            for fileInfo in itertools.chain(
                                csTrove.getNewFileList(raw = True),
                                csTrove.getChangedFileList(raw = True)):
                if fileInfo[1] is None:
                    continue

                newDirNames.add(fileInfo[1])
                newBaseNames.add(fileInfo[2])

        repos.troveStore.addTroveSetStart(oldTroves, newDirNames, newBaseNames)

    def _containsFileContents(self, sha1iter):
        return self.repos.troveStore.hasFileContents(sha1iter)

    def markTroveRemoved(self, name, version, flavor):
        self.repos.markTroveRemoved(name, version, flavor)

    def checkTroveCompleteness(self, trv):
        if not self.mirror and not trv.troveInfo.sigs.sha1():
            raise errors.TroveChecksumMissing(trv.getName(), trv.getVersion(),
                                              trv.getFlavor())
        if trv.troveInfo.incomplete():
            if trv.troveInfo.troveVersion() > trove.TROVE_VERSION:
                raise errors.TroveSchemaError(trv.getName(), trv.getVersion(),
                                              trv.getFlavor(),
                                              trv.troveInfo.troveVersion(),
                                              trove.TROVE_VERSION)
            else:
                nvf = trv.getName(), trv.getVersion(), trv.getFlavor(),
                err =  'Attempted to commit incomplete trove %s=%s[%s]' % nvf
                raise errors.TroveIntegrityError(error=err, *nvf)

    def checkTroveSignatures(self, trv, callback):
        assert(hasattr(callback, 'verifyTroveSignatures'))
        if callback.keyCache is None:
            callback.keyCache = openpgpkey.getKeyCache()
        for fingerprint, timestamp, sig in trv.troveInfo.sigs.digitalSigs.iter():
            try:
                pubKey = callback.keyCache.getPublicKey(fingerprint)
                if pubKey.isRevoked():
                    raise openpgpfile.IncompatibleKey('Key %s is revoked'
                                                      %pubKey.getFingerprint())
                expirationTime = pubKey.getTimestamp()
                if expirationTime and expirationTime < timestamp:
                    raise openpgpfile.IncompatibleKey('Key %s is expired'
                                                      %pubKey.getFingerprint())
            except openpgpfile.KeyNotFound:
                # missing keys could be okay; that depends on the threshold
                # we've set. it's the callbacks problem in any case.
                pass

        res = ChangeSetJob.checkTroveSignatures(self, trv, callback)
        if len(res[1]) and self.requireSigs:
            raise openpgpfile.KeyNotFound('Repository does not recognize '
                                          'key: %s'% res[1][0])

    def _filterRestoreList(self, configRestoreList, normalRestoreList):
        # The base class version of this method will re-store contents already
        # in the repository for refcounting purposes, but repository datastores
        # do not refcount. This one just checks if contents exist.
        def filterOne(restoreList):
            inReposList = self._containsFileContents(tup[2]
                    for tup in restoreList)
            return [x for (x, inRepos) in zip(restoreList, inReposList)
                    if not inRepos]

        configRestoreList = filterOne(configRestoreList)
        normalRestoreList = filterOne(normalRestoreList)
        return configRestoreList, normalRestoreList


class UpdateCallback(callbacks.UpdateCallback):
    def __init__(self, statusPath, trustThreshold, keyCache):
        self.path = statusPath
        if statusPath:
            self.tmpDir = os.path.dirname(statusPath)
        callbacks.UpdateCallback.__init__(self, trustThreshold, keyCache)

    def _dumpStatus(self, *args):
        if self.path:
            # make the new status dump in a temp location
            # for atomicity
            (fd, path) = tempfile.mkstemp(dir = self.tmpDir,
                                          suffix = '.commit-status')
            buf = cPickle.dumps(args)
            os.write(fd, buf)
            os.close(fd)
            os.rename(path, self.path)

    def creatingDatabaseTransaction(self, *args):
        self._dumpStatus('creatingDatabaseTransaction', *args)

    def updatingDatabase(self, *args):
        self._dumpStatus('updatingDatabase', *args)

class FilesystemRepository(DataStoreRepository, AbstractRepository):

    def __init__(self, serverNameList, troveStore, contentsDir, repositoryMap,
                 requireSigs = False, paranoidCommits = False):
        self.serverNameList = serverNameList
        self.paranoidCommits = paranoidCommits
        map = dict(repositoryMap)
        for serverName in serverNameList:
            map[serverName] = self
        self.troveStore = troveStore
        self.requireSigs = requireSigs

        storeType, paths = contentsDir
        if storeType == CfgContentStore.LEGACY:
            storeClass = DataStore
        elif storeType == CfgContentStore.SHALLOW:
            storeClass = ShallowDataStore
        elif storeType == CfgContentStore.FLAT:
            storeClass = FlatDataStore
        else:
            raise ValueError("Invalid contentsDir type %r" % (storeType,))

        stores = []
        for path in paths:
            util.mkdirChain(path)
            stores.append(storeClass(path))
        if len(stores) == 1:
            store = stores[0]
        else:
            store = DataStoreSet(*stores)

        DataStoreRepository.__init__(self, dataStore = store)
        AbstractRepository.__init__(self)

    def close(self):
        if self.troveStore is not None:
            self.troveStore.db.close()
            self.troveStore = None

    ### Package access functions

    def thawFlavor(self, flavor):
        return deps.ThawFlavor(flavor)

    def hasTrove(self, pkgName, version, flavor):
        return self.troveStore.hasTrove(pkgName, troveVersion = version,
                                        troveFlavor = flavor)

    def getTrove(self, pkgName, version, flavor, pristine = True,
                 withFiles = True, hidden = False):
        return self.troveStore.getTrove(
            pkgName, version, flavor, withFiles = withFiles,
            hidden = hidden)

    def iterTroves(self, troveList, withFiles = True, hidden = False):
        return self.troveStore.iterTroves(troveList, withFiles = withFiles,
                                          hidden = hidden)

    def getParentTroves(self, troveList):
        return self.troveStore.getParentTroves(troveList)

    def addTrove(self, trv, trvCs, hidden = False, oldTroveSpec = None):
        return self.troveStore.addTrove(trv, trvCs, hidden = hidden)

    def addTroveDone(self, pkg, mirror=False):
        self.troveStore.addTroveDone(pkg, mirror=mirror)

    ### File functions

    def getFileVersion(self, pathId, fileId, fileVersion, withContents = 0):
        # the get trove netclient provides doesn't work with a
        # FilesystemRepository (it needs to create a change set which gets
        # passed)
        if fileVersion.getHost() not in self.serverNameList:
            raise errors.RepositoryMismatch(self.serverNameList,
                    fileVersion.getHost())

        fileObj = self.troveStore.getFile(pathId, fileId)
        if withContents:
            if fileObj.hasContents:
                cont = filecontents.FromDataStore(self.contentsStore,
                                                    file.contents.sha1())
            else:
                cont = None

            return (fileObj, cont)

        return fileObj

    def getFileVersions(self, fileList, withContents = False):
        # this is for compatibility with <= 1.0.13
        crossRepos = False
        for (pathId, fileId, fileVersion) in fileList:
            if fileVersion.getHost() not in self.serverNameList:
                crossRepos = True

        if crossRepos:
            for x in fileList:
                yield self.getFileVersion(withContents = withContents, *x)
        else:
            fileDict = self.troveStore.getFiles(fileList)
            for x in fileList:
                # (pathId, fileId) lookup
                try:
                    fileObj = fileDict[x[0:2]]
                except KeyError:
                    raise errors.FileStreamMissing(x[1])

                if withContents:
                    if file.hasContents:
                        cont = filecontents.FromDataStore(self.contentsStore,
                                                          file.contents.sha1())
                    else:
                        cont = None

                    yield (fileObj, cont)

                yield fileObj

    def addFileVersion(self, troveInfo, pathId, path, fileId,
                       fileVersion, fileStream = None, withContents = True):
        troveInfo.addFile(pathId, path, fileId, fileVersion,
                          fileStream = fileStream, withContents = withContents)

    ###

    def commitChangeSet(self, cs, mirror=False, hidden=False, serialize=False,
                        callback=None,
                        statusPath = None):
        # let's make sure commiting this change set is a sane thing to attempt
        for trvCs in cs.iterNewTroveList():
            v = trvCs.getNewVersion()
            if v.isOnLocalHost():
                label = v.branch().label()
                raise errors.CommitError('can not commit items on '
                                         '%s label' %(label.asString()))
        if self.requireSigs:
            threshold = openpgpfile.TRUST_FULL
        else:
            threshold = openpgpfile.TRUST_UNTRUSTED
        # Callback for signature verification and progress
        if statusPath:
            assert not callback
            callback = UpdateCallback(statusPath=statusPath,
                    trustThreshold=threshold,
                    keyCache=self.troveStore.keyTable.keyCache)
        # Restore contents first, before any shared database resources get
        # locked.
        preRestored = set()
        for sha1, fobj in cs.iterRegularFileContents():
            cont = filecontents.FromFile(fobj, compressed=True)
            self._storeFileFromContents(cont, sha1, restoreContents=True,
                    precompressed=True)
            preRestored.add(sha1)
        cs.reset()
        self.troveStore.begin(serialize)
        try:
            # reset time stamps only if we're not mirroring.
            FilesystemChangeSetJob(self, cs, self.serverNameList,
                                   resetTimestamps = not mirror,
                                   callback=callback,
                                   mirror = mirror,
                                   hidden = hidden,
                                   requireSigs = self.requireSigs,
                                   preRestored=preRestored,
                                   )
        except (openpgpfile.KeyNotFound, errors.DigitalSignatureVerificationError):
            # don't be quite so noisy, this is a common error
            self.troveStore.rollback()
            raise
        except:
            print >> sys.stderr, "exception occurred while committing change set"
            print >> sys.stderr, ''.join(traceback.format_exception(*sys.exc_info()))
            print >> sys.stderr, "attempting rollback"
            self.troveStore.rollback()
            raise
        else:
            if self.paranoidCommits:
                for trvCs in cs.iterNewTroveList():
                    newTuple = trvCs.getNewNameVersionFlavor()
                    if newTuple[1] is None:
                        continue

                    trv = self.getTrove(withFiles = True, *newTuple)
                    assert(trv.verifyDigests())
            self.troveStore.commit()

    def markTroveRemoved(self, name, version, flavor):
        sha1s = self.troveStore.markTroveRemoved(name, version, flavor)
        for sha1 in sha1s:
            try:
                self.contentsStore.removeFile(sha1)
            except OSError, e:
                if e.errno != errno.ENOENT:
                    raise

    def getFileContents(self, itemList):
        contents = []

        for item in itemList:
            (fileId, fileVersion) = item[0:2]

            # the get trove netclient provides doesn't work with a
            # FilesystemRepository (it needs to create a change set which gets
            # passed)
            if fileVersion.getHost() in self.serverNameList:
                fileObj = item[2]
                cont = filecontents.FromDataStore(self.contentsStore,
                                                  fileObj.contents.sha1())
            else:
                raise errors.RepositoryMismatch(self.serverNameList,
                        fileVersion.getHost())
            contents.append(cont)

        return contents

    def createChangeSet(self, origTroveList, recurse = True,
                        withFiles = True, withFileContents = True,
                        excludeAutoSource = False,
                        mirrorMode = False, roleIds = None):
        """
        @param origTroveList: a list of
        C{(troveName, flavor, oldVersion, newVersion, absolute)} tuples.

        If C{oldVersion == None} and C{absolute == 0}, then the trove is
        assumed to be new for the purposes of the change set.

        If C{newVersion == None} then the trove is being removed.

        If recurse is set, this yields one result for the entire troveList.

        If recurse is not set, it yields one result per troveList entry.
        """
        cs = changeset.ChangeSet()
        externalTroveList = []
        externalFileList = []
        removedTroveList = []

        dupFilter = set()

        # make a copy to remove things from
        troveList = origTroveList[:]

        # def createChangeSet begins here

        troveWrapper = _TroveListWrapper(troveList, self.troveStore, withFiles,
                                         roleIds = roleIds)

        for (job, old, new, streams) in troveWrapper:
            (troveName, (oldVersion, oldFlavor),
                         (newVersion, newFlavor), absolute) = job

            # make sure we haven't already generated this changeset; since
            # troves can be included from other troves we could try
            # to generate quite a few duplicates
            if job in dupFilter:
                continue
            else:
                dupFilter.add(job)

            done = False
            if not newVersion:
                if oldVersion.getHost() not in self.serverNameList:
                    externalTroveList.append((troveName,
                                         (oldVersion, oldFlavor),
                                         (None, None), absolute))
                else:
                    # remove this trove and any trove contained in it
                    cs.oldTrove(troveName, oldVersion, oldFlavor)
                    for (name, version, flavor) in \
                                            old.iterTroveList(strongRefs=True):
                        troveWrapper.append((name, (version, flavor),
                                                   (None, None), absolute),
                                            False)
                done = True
            elif (newVersion.getHost() not in self.serverNameList
                or (oldVersion and
                    oldVersion.getHost() not in self.serverNameList)):
                # don't try to make changesets between repositories; the
                # client can do that itself

                # we don't generate chagnesets between removed and
                # present troves; that's up to the client
                externalTroveList.append((troveName, (oldVersion, oldFlavor),
                                     (newVersion, newFlavor), absolute))
                done = True
            elif (oldVersion and old.type() == trove.TROVE_TYPE_REMOVED):
                removedTroveList.append((troveName, (oldVersion, oldFlavor),
                                        (newVersion, newFlavor), absolute))
                done = True

            if done:
                if not recurse:
                    yield (cs, externalTroveList, externalFileList,
                           removedTroveList)

                    cs = changeset.ChangeSet()
                    externalTroveList = []
                    externalFileList = []
                    removedTroveList = []

                continue

            (troveChgSet, filesNeeded, pkgsNeeded) = \
                                new.diff(old, absolute = absolute)

            if recurse:
                for refJob in pkgsNeeded:
                    refOldVersion = refJob[1][0]
                    refNewVersion = refJob[2][0]
                    if (refNewVersion and
                           (refNewVersion.getHost() not in self.serverNameList)
                        or (refOldVersion and
                            refOldVersion.getHost() not in self.serverNameList)
                       ):
                        # don't try to make changesets between repositories; the
                        # client can do that itself
                        externalTroveList.append(refJob)
                    else:
                        troveWrapper.append(refJob, True)

            cs.newTrove(troveChgSet)

            if job in origTroveList and job[2][0] is not None:
                # add the primary w/ timestamps on the version
                try:
                    primary = troveChgSet.getNewNameVersionFlavor()
                    cs.addPrimaryTrove(*primary)
                except KeyError:
                    # primary troves could be in the externalTroveList, in
                    # which case they aren't primries
                    pass

            # sort the set of files we need into bins based on the server
            # name
            getList = []
            localFilesNeeded = []

            for (pathId, oldFileId, oldFileVersion, newFileId, newFileVersion) in filesNeeded:
                # if either the old or new file version is on a different
                # repository, creating this diff is someone else's problem
                if (newFileVersion.getHost() not in self.serverNameList
                    or (oldFileVersion and
                        oldFileVersion.getHost() not in self.serverNameList)):
                    externalFileList.append((pathId, troveName,
                         (oldVersion, oldFlavor, oldFileId, oldFileVersion),
                         (newVersion, newFlavor, newFileId, newFileVersion)))
                else:
                    localFilesNeeded.append((pathId, oldFileId, oldFileVersion,
                                             newFileId, newFileVersion))
                    if oldFileVersion:
                        getList.append((pathId, oldFileId, oldFileVersion))
                    getList.append((pathId, newFileId, newFileVersion))

            # Walk this in reverse order. This may seem odd, but the
            # order in the final changeset is set by sorting that happens
            # in the change set object itself. The only reason we sort
            # here at all is to make sure PTR file types come before the
            # file they refer to. Reverse shorting makes this a bit easier.
            localFilesNeeded.sort()
            localFilesNeeded.reverse()

            ptrTable = {}
            for (pathId, oldFileId, oldFileVersion, newFileId, \
                 newFileVersion) in localFilesNeeded:
                oldFile = None
                if oldFileVersion:
                    oldFile = files.ThawFile(streams[oldFileId], pathId)

                oldCont = None
                newCont = None

                newFile = files.ThawFile(streams[newFileId], pathId)

                # Skip identical fileids when mirroring, but always use
                # absolute file changes if there is any difference. See note
                # below.
                forceAbsolute = (mirrorMode and oldFileId
                        and oldFileId != newFileId)
                if forceAbsolute:
                    (filecs, contentsHash) = changeset.fileChangeSet(pathId,
                                                                     None,
                                                                     newFile)
                else:
                    (filecs, contentsHash) = changeset.fileChangeSet(pathId,
                                                                     oldFile,
                                                                     newFile)

                cs.addFile(oldFileId, newFileId, filecs)

                if (not withFileContents
                    or (excludeAutoSource and newFile.flags.isAutoSource())
                    or (newFile.flags.isEncapsulatedContent()
                        and not newFile.flags.isCapsuleOverride())):
                    continue

                # this test catches files which have changed from not
                # config files to config files; these need to be included
                # unconditionally so we always have the pristine contents
                # to include in the local database
                # Also include contents of config files when mirroring if the
                # fileid changed, even if the SHA-1 did not.
                # cf CNY-1570, CNY-1699, CNY-2210
                if (contentsHash
                        or (oldFile and newFile.flags.isConfig()
                            and not oldFile.flags.isConfig())
                        or (forceAbsolute and newFile.hasContents)
                        ):
                    if oldFileVersion and oldFile.hasContents:
                        oldCont = self.getFileContents(
                            [ (oldFileId, oldFileVersion, oldFile) ])[0]

                    newCont = self.getFileContents(
                            [ (newFileId, newFileVersion, newFile) ])[0]

                    (contType, cont) = changeset.fileContentsDiff(oldFile,
                                                oldCont, newFile, newCont,
                                                mirrorMode = mirrorMode)

                    # we don't let config files be ptr types; if they were
                    # they could be ptrs to things which aren't config files,
                    # which would completely hose the sort order we use. this
                    # could be relaxed someday to let them be ptr's to other
                    # config files
                    if not newFile.flags.isConfig() and \
                                contType == changeset.ChangedFileTypes.file:
                        contentsHash = newFile.contents.sha1()
                        ptr = ptrTable.get(contentsHash, None)
                        if ptr is not None:
                            contType = changeset.ChangedFileTypes.ptr
                            cont = filecontents.FromString(ptr)
                        else:
                            ptrTable[contentsHash] = pathId + newFileId

                    if not newFile.flags.isConfig() and \
                                contType == changeset.ChangedFileTypes.file:
                        cont = filecontents.CompressedFromDataStore(
                                              self.contentsStore,
                                              newFile.contents.sha1())
                        compressed = True
                    else:
                        compressed = False

                    # ptr entries are not compressed, whether or not they
                    # are config files. override the compressed rule from
                    # above
                    if contType == changeset.ChangedFileTypes.ptr:
                        compressed = False

                    cs.addFileContents(pathId, newFileId, contType, cont,
                                       newFile.flags.isConfig(),
                                       compressed = compressed)

            if not recurse:
                yield cs, externalTroveList, externalFileList, removedTroveList

                cs = changeset.ChangeSet()
                externalTroveList = []
                externalFileList = []
                removedTroveList = []

        if recurse:
            yield cs, externalTroveList, externalFileList, removedTroveList

class _TroveListWrapper:
    def _handleJob(self, job, recursed, idx):
        t = self.trvIterator.next()

        if t is not None:
            if self.withFiles:
                t, streams = t
            else:
                streams = {}

        if t is None:
            if recursed:
                # synthesize a removed trove for this missing
                # trove
                t = trove.Trove(job[0], job[idx][0], job[idx][1],
                                type=trove.TROVE_TYPE_REMOVED)
                t.setIsMissing(True)
                t.computeDigests()

                # synthesize empty filestreams
                streams = {}
            else:
                # drain the iterator, in order to complete
                # the sql queries
                for x in self.trvIterator: pass
                raise errors.TroveMissing(job[0], job[idx][0])

        return t, streams

    def next(self):
        if not self.l and self.new:
            # self.l (and self.trvIterator) are empty; look to
            # self.new for new jobs we need

            troveList = []
            for job, recursed in self.new:
                # do we need the old trove?
                if job[1][0] is not None:
                    troveList.append((job[0], job[1][0], job[1][1]))

                # do we need the new trove?
                if job[2][0] is not None:
                    troveList.append((job[0], job[2][0], job[2][1]))

            # flip to the new job set and it's trove iterator, and
            # reset self.new for later additions
            self.trvIterator = self.troveStore.iterTroves(
                        troveList, withFiles = self.withFiles,
                        withFileStreams = self.withFiles,
                        permCheckFilter = self._permCheck,
                        hidden=True,
                        )
            self.l = self.new
            self.new = []

        if self.l:
            job, recursed = self.l.pop(0)

            # Does it have an old job?
            if job[1][0] is None:
                old = None
                oldStreams = {}
            else:
                old, oldStreams = self._handleJob(job, recursed, 1)

            # Does it have a new job
            if job[2][0] is None:
                new = None
                newStreams = {}
            else:
                new, newStreams = self._handleJob(job, recursed, 2)

            newStreams.update(oldStreams)
            return job, old, new, newStreams
        else:
            raise StopIteration

    def _permCheck(self, cu, instanceTblName):
        # returns a list of instance id's we're allowed to see
        sql = """
        DELETE FROM %s WHERE instanceId NOT IN
            (SELECT DISTINCT ugi.instanceId
             FROM %s JOIN UserGroupInstancesCache as ugi ON
             %s.instanceId = ugi.instanceId
             WHERE
                ugi.userGroupId IN (%s))
        """ % (instanceTblName, instanceTblName, instanceTblName,
               ",".join("%d" % x for x in self.roleIds))
        cu.execute(sql, start_transaction = False)

    def __iter__(self):
        while True:
            yield self.next()

    def append(self, item, recurse):
        self.new.append((item, recurse))

    def __init__(self, l, troveStore, withFiles, roleIds = None):
        self.trvIterator = None
        self.new = [ (x, False) for x in l ]
        self.l = []
        self.troveStore = troveStore
        self.withFiles = withFiles
        self.roleIds = roleIds
