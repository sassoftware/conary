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


"""
Provides a data storage mechanism for files which are indexed by a hash
index.

The hash can be any arbitrary string of at least 5 bytes in length;
keys are assumed to be unique.
"""

import base64
import errno
import gzip
import itertools
import os
import tempfile

from conary.lib import util
from conary.lib import digestlib
from conary.lib import sha1helper
from conary.lib.ext import digest_uncompress
from conary.repository import errors, filecontents


_cached_umask = None


class AbstractDataStore:

    @staticmethod
    def _fchmod(fd, mode=0666):
        global _cached_umask
        if _cached_umask is None:
            # The only way to get the current umask is to change the umask and
            # then change it back.
            _cached_umask = os.umask(022)
            os.umask(_cached_umask)
        util.fchmod(fd, mode & ~_cached_umask)

    @classmethod
    def _writeFile(cls, fileObj, outFds, precompressed, computeSha1):
        if precompressed and hasattr(fileObj, '_fdInfo'):
            (fd, start, size) = fileObj._fdInfo()
            realHash = digest_uncompress.sha1Copy((fd, start, size), outFds)
            for x in outFds:
                cls._fchmod(x)
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
                cls._fchmod(fd)
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
        # New consumers should pass a binary hash, but for backwards
        # compatibility (with rmake) continue to accept hashes that are already
        # encoded. Proxy code also passes in hashes with suffixes on them,
        # which should probably be normalized further.
        if len(hash) < 40:
            hash = sha1helper.sha1ToString(hash)
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

        if integrityCheck and realHash != hash:
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

class ShallowDataStore(DataStore):

    def hashToPath(self, hash):
        # proxy code passes in hex digests with version suffixes, so just pass
        # that through.
        if len(hash) < 40:
            hash = sha1helper.sha1ToString(hash)
        if (len(hash) < 5):
            raise KeyError, ("invalid hash %s" % hash)

        return os.sep.join((self.top, hash[0:2], hash[2:]))

    def makeDir(self, path):
        d = os.path.dirname(path)
        try:
            os.mkdir(d)
        except OSError, e:
            if e.args[0] != errno.EEXIST:
                raise


class FlatDataStore(DataStore):

    def hashToPath(self, hash):
        assert len(hash) == 20
        hash = base64.urlsafe_b64encode(hash)
        # Omit trailing padding
        return os.sep.join((self.top, hash[:27]))

    def makeDir(self, path):
        pass


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

        if realHash != hash:
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
            self.contentsStore.addFile(contents.get(), sha1,
                                       precompressed = precompressed)
        else:
            # the file doesn't have any contents, so it must exist
            # in the data store already; we still need to increment
            # the reference count for it
            self.contentsStore.addFileReference(sha1)

        return 1

    def _removeFileContents(self, sha1):
        self.contentsStore.removeFile(sha1)

    def _getFileObject(self, sha1):
        return self.contentsStore.openFile(sha1)

    def _hasFileContents(self, sha1):
        return self.contentsStore.hasFile(sha1)

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
