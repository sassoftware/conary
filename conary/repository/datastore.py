#
# Copyright (c) 2004-2005 rPath, Inc.
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

"""
Provides a data storage mechanism for files which are indexed by a hash
index.

The hash can be any arbitrary string of at least 5 bytes in length;
keys are assumed to be unique.
"""

import errno
import fcntl
import gzip
import os
import sha

from conary.lib import log
from conary.lib import util
from conary.lib import sha1helper
from conary.repository import errors, filecontents

class DataStore:

    def hashToPath(self, hash):
	if (len(hash) < 5):
	    raise KeyError, ("invalid hash %s" % hash)

	return os.sep.join((self.top, hash[0:2], hash[2:4], hash[4:]))

    def hasFile(self, hash):
	path = self.hashToPath(hash)
	return os.path.exists(path)

    def decrementCount(self, path):
	"""
	Decrements the count by one; it it becomes 1, the count file
	is removed. If it becomes zero, the contents are removed.
	"""
        countPath = path + "#"

	# use the count file for locking, *even if it doesn't exist*
        # first ensure that the directory exists
        self.makeDir(path)
	countFile = os.open(countPath, os.O_RDWR | os.O_CREAT)
	fcntl.lockf(countFile, fcntl.LOCK_EX)
        
	val = os.read(countFile, 100)
	if not val:
	    # no count file, remove the file
            try:
                os.unlink(path)
            except OSError, e:
                if e.errno == errno.ENOENT:
                    # the contents have already been erased
                    log.warning("attempted to remove %s from the data store, but it was missing", path)
                else:
                    raise
	    # someone may try to recreate the file in here, but it should
	    # work fine. even if multiple processes try to, one will create
	    # the file and the rest will block on the countFile. once
	    # we unlink it, everything will get moving again.
	    os.unlink(countPath)
	else:
	    val = int(val[:-1])
	    if val == 1:
		os.unlink(countPath)
	    else:
		val -= 1
		os.lseek(countFile, 0, 0)
		os.ftruncate(countFile, 0)
		os.write(countFile, "%d\n" % val)

	os.close(countFile)

    def incrementCount(self, path, fileObj = None, precompressed = False):
	"""
	Increments the count by one.  it becomes one, the contents
	of fileObj are stored into that path. Return the new count.
	"""
        countPath = path + "#"

        self.makeDir(path)
	if os.path.exists(path):
	    # if the path exists, it must be correct since we move the
	    # contents into place atomicly. all we need to do is
	    # increment the count
	    countFile = os.open(countPath, os.O_RDWR | os.O_CREAT)
	    fcntl.lockf(countFile, fcntl.LOCK_EX)

	    val = os.read(countFile, 100)
	    if not val:
		val = 0
	    else:
		val = int(val[:-1])

	    val += 1
	    os.lseek(countFile, 0, 0)
	    os.ftruncate(countFile, 0)
	    os.write(countFile, "%d\n" % val)
	    os.close(countFile)
            return (val, None)
	else:
	    # new file, try to be the one who creates it
	    newPath = path + ".new"

	    fd = os.open(newPath, os.O_RDWR | os.O_CREAT)

	    # get a write lock on the file
	    fcntl.lockf(fd, fcntl.LOCK_EX)

	    # if the .new file doesn't exist anymore, someone else must
	    # have gotten the write lock before we did, created the
	    # file, and then moved it into place. when this happens
	    # we need to update the count instead
	    
	    if not os.path.exists(newPath):
		os.close(fd)
		return self.incrementCount(path, fileObj = fileObj,
                                           precompressed = precompressed)

            fObj = os.fdopen(fd, "r+")
            if precompressed:
                # this requires fileObj to be seekable. it's just easier
                # that way
                contentSha1 = sha.new()
                uncompObj = gzip.GzipFile(mode = "r", fileobj = fileObj)
                s = uncompObj.read(128 * 1024)
                while s:
                    contentSha1.update(s)
                    s = uncompObj.read(128 * 1024)
                fileObj.seek(0)
                
                util.copyfileobj(fileObj, fObj)
                uncompObj.close()
            else:
                dest = gzip.GzipFile(mode = "w", fileobj = fObj)
                contentSha1 = sha.new()
                util.copyfileobj(fileObj, dest, digest = contentSha1)
                dest.close()

            os.rename(newPath, path)
	    # this closes fd for us
	    fObj.close()
            return (1, contentSha1.hexdigest())

    # add one to the reference count for a file which already exists
    # in the archive
    def addFileReference(self, hash):
	path = self.hashToPath(hash)
	self.incrementCount(path)

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
    def addFile(self, f, hash, precompressed = False):
	path = self.hashToPath(hash)
        self.makeDir(path)
	newCount, sha1 = self.incrementCount(path, fileObj = f,
                                             precompressed = precompressed)
        if sha1 and sha1 != hash:
            raise errors.IntegrityError

        if newCount == 1 and self.logFile:
            open(self.logFile, "a").write(path + "\n")

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
	self.decrementCount(path)

	try:
            # XXX remove the next level up as well
	    os.rmdir(os.path.dirname(path))
	except OSError:
	    # if this fails there are probably just other files
	    # in that directory; just ignore it
	    pass

    def __init__(self, topPath, logFile = None):
	self.top = topPath
        self.logFile = logFile

	if (not os.path.isdir(self.top)):
	    raise IOError, ("path is not a directory: %s" % topPath)

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
            
            if fileObj:
                cont = filecontents.FromDataStore(self.contentsStore,
                                                  fileObj.contents.sha1())
            else:
                cont = ""

            contentList.append(cont)

        return contentList

    def __init__(self, dataStore = None):
	self.contentsStore = dataStore

