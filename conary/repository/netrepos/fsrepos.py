#
# Copyright (c) 2004-2009 rPath, Inc.
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
#

# implements a db-based repository

import errno
import traceback
import sys

from conary import files, trove, callbacks
from conary.deps import deps
from conary.lib import util, openpgpfile, sha1helper, openpgpkey
from conary.repository import changeset, errors, filecontents
from conary.repository.datastore import DataStoreRepository, DataStore
from conary.repository.datastore import DataStoreSet
from conary.repository.repository import AbstractRepository
from conary.repository.repository import ChangeSetJob
from conary.repository import netclient
from conary.server import schema

class FilesystemChangeSetJob(ChangeSetJob):
    def __init__(self, repos, cs, *args, **kw):
        self.mirror = kw.get('mirror', False)
        self.requireSigs = kw.pop('requireSigs', False)

        repos.troveStore.addTroveSetStart()
        ChangeSetJob.__init__(self, repos, cs, *args, **kw)
        repos.troveStore.addTroveSetDone()

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

class FilesystemRepository(DataStoreRepository, AbstractRepository):

    def __init__(self, serverNameList, troveStore, contentsDir, repositoryMap,
                 requireSigs = False, paranoidCommits = False):
	self.serverNameList = serverNameList
        self.paranoidCommits = paranoidCommits
	map = dict(repositoryMap)
        for serverName in serverNameList:
            map[serverName] = self
        # XXX this client needs to die
        from conary import conarycfg
        self.reposSet = netclient.NetworkRepositoryClient(map,
                                    conarycfg.UserInformation())
	self.troveStore = troveStore

        self.requireSigs = requireSigs
        for dir in contentsDir:
            util.mkdirChain(dir)

        if len(contentsDir) == 1:
            store = DataStore(contentsDir[0])
        else:
            storeList = []
            for dir in contentsDir:
                storeList.append(DataStore(dir))

            store = DataStoreSet(*storeList)

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
            # XXX This code is not needed as of version 1.0.14 of the client.
	    assert(not withContents)
	    return self.reposSet.getFileVersion(pathId, fileId, fileVersion)

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
                        excludeCapsuleContents = False):
	# let's make sure commiting this change set is a sane thing to attempt
	for pkg in cs.iterNewTroveList():
	    v = pkg.getNewVersion()
            if v.isOnLocalHost():
                label = v.branch().label()
		raise errors.CommitError('can not commit items on '
                                         '%s label' %(label.asString()))
        self.troveStore.begin(serialize)
        if self.requireSigs:
            threshold = openpgpfile.TRUST_FULL
        else:
            threshold = openpgpfile.TRUST_UNTRUSTED
        # Callback for signature verification
        callback = callbacks.UpdateCallback(trustThreshold=threshold,
                            keyCache=self.troveStore.keyTable.keyCache)
        try:
            # reset time stamps only if we're not mirroring.
            FilesystemChangeSetJob(self, cs, self.serverNameList,
                                   resetTimestamps = not mirror,
                                   callback=callback,
                                   mirror = mirror,
                                   hidden = hidden,
                                   excludeCapsuleContents =
                                        excludeCapsuleContents,
                                   requireSigs = self.requireSigs)
        except openpgpfile.KeyNotFound:
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
                self.contentsStore.removeFile(sha1helper.sha1ToString(sha1))
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
                # XXX This code is not needed as of version 1.0.14 of the 
                # client.
                #
                # a bit of sleight of hand here... we look for this file in
                # the trove it was first built in
                #
                # this could cause us to run out of file descriptors on large
                # troves. it might be better to close the file and return
                # a filecontents object?
                cont = self.reposSet.getFileContents([ item ])[0]

            contents.append(cont)

        return contents

    def createChangeSet(self, origTroveList, recurse = True,
                        withFiles = True, withFileContents = True,
                        excludeCapsuleContents = False,
                        excludeAutoSource = False,
                        mirrorMode = False, roleIds = None):
	"""
	@param troveList: a list of (troveName, flavor, oldVersion, newVersion,
        absolute) tuples.

	if oldVersion == None and absolute == 0, then the trove is assumed
	to be new for the purposes of the change set

	if newVersion == None then the trove is being removed

        if recurse is set, this yields one result for the entire troveList.
        If recurse is not set, it yields one result per troveList entry.

        @param excludeCapsuleContents: If True, troves which include capsules
        have all of their content excluded from the changeset no matter how
        withFileContents is set.
	"""
	cs = changeset.ChangeSet()
        externalTroveList = []
        externalFileList = []
        removedTroveList = []

	dupFilter = set()
        resultList = []

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
	    serverIdx = {}
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
		    #oldFile = idIdx[(pathId, oldFileId)]
		    oldFile = files.ThawFile(streams[oldFileId], pathId)

		oldCont = None
		newCont = None

		#newFile = idIdx[(pathId, newFileId)]
		newFile = files.ThawFile(streams[newFileId], pathId)

                if mirrorMode:
                    (filecs, contentsHash) = changeset.fileChangeSet(pathId,
                                                                     None,
                                                                     newFile)
                else:
                    (filecs, contentsHash) = changeset.fileChangeSet(pathId,
                                                                     oldFile,
                                                                     newFile)

		cs.addFile(oldFileId, newFileId, filecs)

                if (excludeCapsuleContents and new.troveInfo.capsule.type and
                               new.troveInfo.capsule.type()):
                    continue
                if not withFileContents or (excludeAutoSource and
                   newFile.flags.isAutoSource()) or newFile.flags.isPayload():
                    continue

		# this test catches files which have changed from not
		# config files to config files; these need to be included
		# unconditionally so we always have the pristine contents
		# to include in the local database
		if ((mirrorMode and newFile.hasContents) or contentsHash or
                             (oldFile and newFile.flags.isConfig()
                                      and not oldFile.flags.isConfig())):
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
                        permCheckFilter = self._permCheck)
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

