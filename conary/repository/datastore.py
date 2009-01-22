#
# Copyright (c) 2004-2008 rPath, Inc.
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

"""
Provides a data storage mechanism for files which are indexed by a hash
index.

The hash can be any arbitrary string of at least 5 bytes in length;
keys are assumed to be unique.
"""

import errno
import gzip
import itertools
import os
import tempfile

from conary.lib import misc, util
from conary.lib import digestlib
from conary.lib import sha1helper
from conary.repository import errors, filecontents

class AbstractDataStore:

    @staticmethod
    def _writeFile(fileObj, outFds, precompressed, computeSha1):
        if precompressed and hasattr(fileObj, '_fdInfo'):
            (fd, start, size) = fileObj._fdInfo()
            pid = os.getpid()
            realHash = misc.sha1Copy((fd, start, size), outFds)
            for x in outFds:
                os.close(x)

            return realHash
        else:
            for fd in outFds:
                outFileObj = os.fdopen(fd, "w")
                contentSha1 = digestlib.sha1()
                if precompressed and computeSha1:
                    tee = Tee(fileObj, outFileObj)
                    uncompObj = gzip.GzipFile(mode = "r", fileobj = tee)
                    s = uncompObj.read(128 * 1024)
                    while s:
                        contentSha1.update(s)
                        s = uncompObj.read(128 * 1024)
                    uncompObj.close()
                elif precompressed:
                    util.copyfileobj(fileObj, outFileObj)
                else:
                    dest = gzip.GzipFile(mode = "w", fileobj = outFileObj)
                    util.copyfileobj(fileObj, dest, digest = contentSha1)
                    dest.close()

                # this closes tmpFd for us
                outFileObj.close()
                fileObj.seek(0)

            return contentSha1.digest()

    def hasFile(self, hash):
        raise NotImplementedError

    def addFile(self, f, hash, precompressed = False):
        raise NotImplementedError

    def addFileReference(self, hash):
        raise NotImplementedError

    def openFile(self, hash, mode = "r"):
        raise NotImplementedError

    def openRawFile(self, hash):
        raise NotImplementedError

    def removeFile(self, hash):
        raise NotImplementedError

class Tee:
    """
    The Tee class takes two file objects.  Reads are done on the
    input file object.  All data read is written to the output
    file object.
    """
    def __init__(self, inf, outf):
        self.inf = inf
        self.outf = outf

    def read(self, *args, **kw):
        buf = self.inf.read(*args, **kw)
        self.outf.write(buf)
        return buf

    def seek(self, *args, **kw):
        self.outf.seek(*args, **kw)
        return self.inf.seek(*args, **kw)

    def tell(self, *args, **kw):
        return self.inf.tell(*args, **kw)

class DataStore(AbstractDataStore):

    def hashToPath(self, hash):
	if (len(hash) < 5):
	    raise KeyError, ("invalid hash %s" % hash)

	return os.sep.join((self.top, hash[0:2], hash[2:4], hash[4:]))

    def hasFile(self, hash):
	path = self.hashToPath(hash)
	return os.path.exists(path)

    def addFileReference(self, hash):
        # this is for file reference counting, which we don't support
        return

    def makeDir(self, path):
        d = os.path.dirname(path)
	shortPath = d[:-3]

        for _dir in (shortPath, d):
            try:
                os.mkdir(_dir)
            except OSError, e:
                if e.errno != errno.EEXIST:
                    raise

    # file should be a python file object seek'd to the beginning
    # this messes up the file pointer
    def addFile(self, fileObj, hash, precompressed = False, 
                integrityCheck = True):
	path = self.hashToPath(hash)
        self.makeDir(path)
        if os.path.exists(path): return

        tmpFd, tmpName = tempfile.mkstemp(suffix = ".new", 
                                          dir = os.path.dirname(path))

        realHash = self._writeFile(fileObj, [ tmpFd ], precompressed,
                                   computeSha1 = integrityCheck)

        if integrityCheck and realHash != sha1helper.sha1FromString(hash):
            os.unlink(tmpName)
            raise errors.IntegrityError

        os.rename(tmpName, path)

    # returns a python file object for the file requested
    def openFile(self, hash, mode = "r"):
	path = self.hashToPath(hash)
	f = open(path, "r")

	gzfile = gzip.GzipFile(path, mode)
	return gzfile

    # returns a python file object for the file requested
    def openRawFile(self, hash):
	path = self.hashToPath(hash)
	f = open(path, "r")
	return f

    def removeFile(self, hash):
        path = self.hashToPath(hash)
        os.unlink(path)

    def __init__(self, topPath):
	self.top = topPath

	if (not os.path.isdir(self.top)):
	    raise IOError, ("path is not a directory: %s" % topPath)

class OverlayDataStoreSet:

    """
    The first data store is used for writing; all of them are checked
    for reading.
    """

    def hashToPath(self, hash):
 	for store in self.stores:
 	    if store.hasFile(hash):
 		return store.hashToPath(hash)

        return False

    def hasFile(self, hash):
 	for store in self.stores:
 	    if store.hasFile(hash):
 		return True
 
 	return False

    def addFile(self, f, hash, precompressed = False):
 	self.stores[0].addFile(f, hash, precompressed = precompressed)

    def addFileReference(self, hash):
 	self.stores[0].addFileReference(hash)

    def openFile(self, hash, mode = "r"):
 	for store in self.stores:
 	    if store.hasFile(hash):
 		return store.openFile(hash, mode = mode)
 
 	assert(0)

    def openRawFile(self, hash):
 	for store in self.stores:
 	    if store.hasFile(hash):
 		return store.openRawFile(hash)
 
 	assert(0)

    def removeFile(self, hash):
        assert(0)

    def __init__(self, *storeList):
        self.stores = storeList
        self.storeIter = itertools.cycle(self.stores)

class DataStoreSet(AbstractDataStore):

    """
    Duplicates data across multiple content stores.
    """

    def hashToPath(self, hash):
        store = self.storeIter.next()
        return store.hashToPath(hash)

    def hasFile(self, hash):
        store = self.storeIter.next()
        return store.hasFile(hash)

    def addFile(self, f, hash, precompressed = False):
        tmpFileList = []
        for store in self.stores:
            path = self.hashToPath(hash)
            store.makeDir(path)
            if os.path.exists(path): return

            tmpFd, tmpPath = tempfile.mkstemp(suffix = ".new", 
                                              dir = os.path.dirname(path))
            tmpFileList.append((tmpFd, tmpPath, path))

        # fd's close as a side effect. yikes.
        realHash = self._writeFile(f, [ x[0] for x in tmpFileList ],
                                   precompressed, computeSha1 = True)

        if realHash != sha1helper.sha1FromString(hash):
            for fd, tmpPath, path in tmpFileList:
                os.unlink(tmpPath)

            raise errors.IntegrityError

        for fd, tmpPath, path in tmpFileList:
            os.rename(tmpPath, path)

    def addFileReference(self, hash):
        for store in self.stores:
            store.addFileReference(hash)

    def openFile(self, hash, mode = "r"):
        store = self.storeIter.next()
        return store.openFile(hash, mode = mode)

    def openRawFile(self, hash):
        store = self.storeIter.next()
        return store.openRawFile(hash)

    def removeFile(self, hash):
        for store in self.stores:
            store.removeFile(hash)

    def __init__(self, *storeList):
        self.stores = storeList
        self.storeIter = itertools.cycle(self.stores)

class DataStoreRepository:

    """
    Mix-in class which lets a TroveDatabase use a Datastore object for
    storing and retrieving files. These functions aren't provided by
    network repositories.
    """

    def _storeFileFromContents(self, contents, sha1, restoreContents,
                               precompressed = False):
	if restoreContents:
	    self.contentsStore.addFile(contents.get(), 
				       sha1helper.sha1ToString(sha1),
                                       precompressed = precompressed)
	else:
	    # the file doesn't have any contents, so it must exist
	    # in the data store already; we still need to increment
	    # the reference count for it
	    self.contentsStore.addFileReference(sha1helper.sha1ToString(sha1))

	return 1

    def _removeFileContents(self, sha1):
	self.contentsStore.removeFile(sha1helper.sha1ToString(sha1))

    def _getFileObject(self, sha1):
	return self.contentsStore.openFile(sha1helper.sha1ToString(sha1))

    def _hasFileContents(self, sha1):
	return self.contentsStore.hasFile(sha1helper.sha1ToString(sha1))

    def getFileContents(self, fileList):
        contentList = []

        for item in fileList:
            (fileId, fileVersion) = item[0:2]
            if len(item) == 3:
                fileObj = item[2]
            else:
                # XXX this is broken code, we have no findFileVersion()
                # method
                fileObj = self.findFileVersion(fileId)

            if fileObj and self._hasFileContents(fileObj.contents.sha1()):
                # Only config files are stored in this data store
                cont = filecontents.FromDataStore(self.contentsStore,
                                                  fileObj.contents.sha1())
            else:
                cont = None

            contentList.append(cont)

        return contentList

    def __init__(self, dataStore = None):
	self.contentsStore = dataStore

