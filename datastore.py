#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
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
import struct
import util

class DataStore:

    def hashToPath(self, hash):
	if (len(hash) < 5):
	    raise KeyError, ("invalid hash %s" % hash)

	dir = os.sep.join((self.top, hash[0:2], hash[2:4]))
	name = os.sep.join((dir, hash[4:]))
	return (dir, name)

    def hasFile(self, hash):
	path = self.hashToPath(hash)[1]
	return os.path.exists(path)

    def modifyCount(self, path, amount):
        countPath = path + "#"
        oldFd = -1
        # get the current count
	if os.path.exists(countPath):
	    oldFd = os.open(countPath, os.O_RDWR)
            # exclusive lock the existing file so that we are the
            # only process that reads the current state.
            fcntl.lockf(oldFd, fcntl.LOCK_EX)
            oldF = os.fdopen(oldFd)
	    # cut off the trailing \n
	    count = int(oldF.read()[:-1])
	elif os.path.exists(path):
	    count = 1
	else:
	    count = 0

        # modify the count
        count += amount

        # write out the new count
        if count <= 1:
            # the count file exists and needs to be removed
            if oldFd != -1:
                os.unlink(countPath)
                os.close(oldFd)
	    return count

        fd = os.open(countPath + '.new', os.O_CREAT | os.O_WRONLY)
        fcntl.lockf(fd, fcntl.LOCK_EX)
        os.ftruncate(fd, 0)
	os.write(fd, "%d\n" % count)
	os.close(fd)
	os.rename(countPath + ".new", countPath)

        # close the fd on the existing file
        if oldFd != -1:
            os.close(oldFd)
        return count

    def readCount(self, path):
        # XXX this code is not used anymore
	if os.path.exists(path + "#"):
	    fd = os.open(path + "#", os.O_RDONLY)
            fcntl.lockf(fd, fcntl.LOCK_SH)
            f = os.fdopen(fd)
	    # cut off the trailing \n
	    count = int(f.read()[:-1])
            os.close(fd)
	elif os.path.exists(path):
	    count = 1
	else:
	    count = 0

	return count

    # add one to the reference count for a file which already exists
    # in the archive
    def addFileReference(self, hash):
	(dir, path) = self.hashToPath(hash)
	self.modifyCount(path, 1)
	return

    # list addFile, but this returns a file pointer which can be used
    # to write the contents into the file; if it returns None the file
    # is already in the archive
    def newFile(self, hash):
	(dir, path) = self.hashToPath(hash)

	count = self.modifyCount(path, 1)
	if count > 1:
            # the file already exists, nothing to do here.
	    return

	shortPath = dir[:-3]
	if not os.path.exists(shortPath):
	    os.mkdir(shortPath)
	if not os.path.exists(dir):
	    os.mkdir(dir)

	return gzip.GzipFile(path, "w", 9)

    # file should be a python file object seek'd to the beginning
    # this messes up the file pointer
    def addFile(self, file, hash):
	dest = self.newFile(hash)

	if not dest: return		# it already exits
        util.copyfileobj(file, dest)

    # returns a python file object for the file requested
    def openFile(self, hash, mode = "r"):
	path = self.hashToPath(hash)[1]
	f = open(path, "r")

	# read in the size of the file
	f.seek(-4, 2)
	size = f.read(4)
	f.seek(0)

	# we need the size to create a file container to pass over
	# the wire for getFileContents()
	size = struct.unpack("<i", size)[0]
	gzfile = gzip.GzipFile(path, mode)
	gzfile.fullSize = size
	return gzfile

    def removeFile(self, hash):
	(dir, path) = self.hashToPath(hash)

        if self.modifyCount(path, -1) > 0:
	    return

	os.unlink(path)

	try:
	    os.rmdir(dir)
	except OSError:
	    # if this fails there are probably just other files
	    # in that directory; just ignore it
	    pass

    def __init__(self, topPath):
	self.top = topPath
	if (not os.path.isdir(self.top)):
	    raise IOError, ("path is not a directory: %s" % topPath)
