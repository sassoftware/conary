#
# Copyright (c) 2004-2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import gzip
import sha
import zlib
from StringIO import StringIO

from conary.repository import errors, repository, datastore
from conary.local import schema
from conary import files

class LocalRepositoryChangeSetJob(repository.ChangeSetJob):

    storeOnlyConfigFiles = True

    """
    Removals have to be batched (for now at least); if we do them too
    soon the code which merges into the filesystem won't be able to get
    to the old version of things.
    """

    def addTrove(self, oldTroveSpec, trove):
        info = trove.getNameVersionFlavor()
        pin = self.autoPinList.match(trove.getName())
	return (info, self.repos.addTrove(trove, pin = pin))

    def addFileVersion(self, troveId, pathId, fileObj, path, fileId, 
                       newVersion, fileStream = None):
        isPresent = not self.pathRemovedCheck(troveId[0], pathId)
        self.repos.addFileVersion(troveId[1], pathId, fileObj, path,
                                  fileId, newVersion,
                                  fileStream = fileStream,
                                  isPresent = isPresent)

    def addTroveDone(self, troveId):
        self.trovesAdded.append(self.repos.addTroveDone(troveId[1]))

    def oldTrove(self, oldTrove, trvCs, name, version, flavor):
        # trvCs is None for an erase, !None for an update
        self.oldTroves.append((name, version, flavor))

        # while we're here, trove change sets may mark some files as removed;
        # we need to remember to remove those files, and make the paths for
        # those files candidates for removal. trove change sets also know 
        # when file paths have changed, and those old paths are also candidates
        # for removal
        if trvCs:
            for pathId in trvCs.getOldFileList():
                if not oldTrove.hasFile(pathId):
                    # the file has already been removed from the non-pristine
                    # version of this trove in the database, so there is
                    # nothing to do
                    continue
                (oldPath, oldFileId, oldFileVersion) = oldTrove.getFile(pathId)
                self.removeFile(pathId, oldFileId)
        else:
            # a pure erasure; remove all of the files
            for (pathId, path, fileId, version) in oldTrove.iterFileList():
                self.removeFile(pathId, fileId)

    def oldTroveList(self):
	return self.oldTroves

    def oldFile(self, pathId, fileId, sha1):
	self.oldFiles.append((pathId, fileId, sha1))

    def oldFileList(self):
	return self.oldFiles

    def addFile(self, troveId, pathId, fileObj, path, fileId, version,
                oldFileId = None):
	repository.ChangeSetJob.addFile(self, troveId, pathId, fileObj, path, 
					fileId, version)

	if oldFileId:
            self.removeFile(pathId, oldFileId)

    def addFileContents(self, sha1, newVer, fileContents, restoreContents,
			isConfig, precompressed = False):
	if isConfig:
	    repository.ChangeSetJob.addFileContents(self, sha1, newVer, 
			     fileContents, restoreContents, isConfig, 
                             precompressed = precompressed)

    # remove the specified file 
    def removeFile(self, pathId, fileId):
        stream = self.repos.getFileStream(fileId)
        sha1 = None
        if files.frozenFileHasContents(stream):
            flags = files.frozenFileFlags(stream)
            if flags.isConfig():
                contentInfo = files.frozenFileContentInfo(stream)
                sha1 = contentInfo.sha1()

        self.oldFile(pathId, fileId, sha1)

    # If retargetLocal is set, then localCs is for A->A.local whlie
    # origJob is A->B, so localCs needs to be changed to be B->B.local.
    # Otherwise, we're applying a rollback and origJob is B->A and
    # localCs is A->A.local, so it doesn't need retargeting.
    def __init__(self, repos, cs, callback, autoPinList, 
                 filePriorityPath,
                 allowIncomplete = False, pathRemovedCheck = None,
                 replaceFiles = False):
	assert(not cs.isAbsolute())

	self.cs = cs
	self.repos = repos
	self.oldTroves = []
	self.oldFiles = []
        self.trovesAdded = []
        self.autoPinList = autoPinList
        self.pathRemovedCheck = pathRemovedCheck

	repository.ChangeSetJob.__init__(self, repos, cs, callback = callback,
                                         allowIncomplete=allowIncomplete)

        for name, version, flavor in self.oldTroveList():
            self.repos.eraseTrove(name, version, flavor)

        for (pathId, fileVersion, sha1) in self.oldFileList():
	    self.repos.eraseFileVersion(pathId, fileVersion)

        # this raises an exception if this install would create conflicts
        self.repos.db.db.checkPathConflicts(self.trovesAdded, 
                                            filePriorityPath, replaceFiles)

        for (pathId, fileVersion, sha1) in self.oldFileList():
            if sha1 is not None:
		self.repos._removeFileContents(sha1)

class SqlDataStore(datastore.AbstractDataStore):

    """
    Implements a DataStore interface on a sql database. File contents are
    stored directly in the sql database.
    """

    def hasFile(self, hash):
        cu = self.db.cursor()
        cu.execute("SELECT COUNT(*) FROM DataStore WHERE hash=?", hash)
        return (cu.next()[0] != 0)

    def decrementCount(self, hash):
	"""
	Decrements the count by one; it it becomes 1, the count file
	is removed. If it becomes zero, the contents are removed.
	"""
        cu = self.db.cursor()
        cu.execute("SELECT count FROM DataStore WHERE hash=?", hash)
        count = cu.next()[0]
        if count == 1:
            cu.execute("DELETE FROM DataStore WHERE hash=?", hash)
        else:
            count -= 1
            cu.execute("UPDATE DataStore SET count=? WHERE hash=?", 
                       count, hash)

    def incrementCount(self, hash, fileObj = None, precompressed = True):
	"""
	Increments the count by one.  If it becomes one (the file is
        new), the contents of fileObj are stored into that path.
	"""
        cu = self.db.cursor()
        cu.execute("SELECT COUNT(*) FROM DataStore WHERE hash=?", hash)
        exists = cu.next()[0]

        if exists:
            cu.execute("UPDATE DataStore SET count=count+1 WHERE hash=?",
                       hash)
        else:
            if precompressed:
                # it's precompressed as a gzip stream, and we need a
                # zlib stream. just decompress it.
                gzObj = gzip.GzipFile(mode = "r", fileobj = fileObj)
                rawData = gzObj.read()
                del gzObj
            else:
                rawData = fileObj.read()

            data = zlib.compress(rawData)
            digest = sha.new()
            digest.update(rawData)
            if digest.hexdigest() != hash:
                raise errors.IntegrityError

            cu.execute("INSERT INTO DataStore VALUES(?, 1, ?)",
                       hash, data)

    # add one to the reference count for a file which already exists
    # in the archive
    def addFileReference(self, hash):
	self.incrementCount(hash)

    # file should be a python file object seek'd to the beginning
    # this messes up the file pointer
    def addFile(self, f, hash, precompressed = True):
	self.incrementCount(hash, fileObj = f, precompressed = precompressed)

    # returns a python file object for the file requested
    def openFile(self, hash, mode = "r"):
        cu = self.db.cursor()
        cu.execute("SELECT data FROM DataStore WHERE hash=?", hash)
        data = cu.next()[0]
        data = zlib.decompress(data)
        return StringIO(data)

    def removeFile(self, hash):
	self.decrementCount(hash)

    def __init__(self, db):
        self.db = db
        schema.createDataStore(db)

def markAddedFiles(db, cs):
    """
    Mark files added by this changeset as present -- they should already
    be in the database.
    """
    for trvCs in cs.iterNewTroveList():
        # we only need the pathIds
        pathIds = [ x[0] for x in trvCs.getNewFileList() ]
        db.restorePathIdsToTrove(trvCs.getName(), trvCs.getOldVersion(),
                                 trvCs.getOldFlavor(), pathIds)
