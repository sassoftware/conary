#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

# provides data storage mechanism for files which are indexed by a hash
# index. the hash can be any arbitrary string of at least 5 bytes in length;
# it is assumed the keys are unique

import os

class DataStore:

    def hashToPath(self, hash):
	if (len(hash) < 5):
	    raise KeyError, ("invalid hash %s" % hash)

	dir = self.top + "/" + hash[0:2] + "/" + hash[2:4] 
	name = dir + "/" + hash[4:]
	return (dir, name)

    def hasFile(self, hash):
	path = self.hashToPath(hash)[1]
	return os.path.exists(path)

    def writeCount(self, path, newCount):
	path = path + "#"

	if newCount <= 1:
	    os.unlink(path)
	    return
	    
	f = open(path + ".new", "w")
	f.write("%d\n" % newCount)
	f.close()
	os.rename(path + ".new", path)

    def readCount(self, path):
	if os.path.exists(path + "#"):
	    f = open(path + "#")
	    # cut off the trailing \n
	    count = int(f.read()[:-1])
	    f.close()
	elif os.path.exists(path):
	    count = 1
	else:
	    count = 0

	return count

    # add one to the reference count for a file which already exists
    # in the archive
    def addFileReference(self, hash):
	count = self.readCount(hash)
	self.writeCount(hash, count + 1)
	return

    # list addFile, but this returns a file pointer which can be used
    # to write the contents into the file; if it returns None the file
    # is already in the archive
    def newFile(self, hash):
	(dir, path) = self.hashToPath(hash)

	count = self.readCount(path)
	if count:
	    self.writeCount(path, count + 1)
	    return

	shortPath = dir[:-3]
	if not os.path.exists(shortPath):
	    os.mkdir(shortPath)
	if not os.path.exists(dir):
	    os.mkdir(dir)

	dest = open(path, "w")
	return dest

    # file should be a python file object seek'd to the beginning
    # this messes up the file pointer
    def addFile(self, file, hash):
	dest = self.newFile(hash)

	if not dest: return		# it already exits
	dest.write(file.read())

    # returns a python file object for the file requested
    def openFile(self, hash, mode = "r"):
	path = self.hashToPath(hash)[1]
	return open(path, mode)

    def removeFile(self, hash):
	(dir, path) = self.hashToPath(hash)

	count = self.readCount(path)
	if count > 1:
	    self.writeCount(path, count - 1)
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
