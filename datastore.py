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

    # list addFile, but this returns a file pointer which can be used
    # to write the contents into the file
    def newFile(self, hash):
	(dir, path) = self.hashToPath(hash)
	if os.path.exists(path):
	    raise KeyError, "duplicate hash"
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
	try:
	    dest.write(file.read())
	finally:
	    dest.close()

    # returns a python file object for the file requested
    def openFile(self, hash):
	path = self.hashToPath(hash)[1]
	return open(path, "r+")

    def removeFile(self, hash):
	(dir, path) = self.hashToPath(hash)
	os.unlink(path)
	try:
	    os.rmdir(dir)
	except IOError:
	    # if this fails there are probably just other files
	    # in that directory; just ignore it
	    pass

    def __init__(self, topPath):
	self.top = topPath
	if (not os.path.isdir(self.top)):
	    raise IOError, ("path is not a directory: %s" % topPath)
