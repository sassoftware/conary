#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
from filecontainer import FileContainer
import __builtin__

# implements a simple versioned file on top of a FileContainer; the version
# string is used as the file name, and branch marks are stored as extra data 
# for the file

class File:

    def open(self, filename, mode):
	f = __builtin__.open(filename, mode)
	self.container = FileContainer(f)
	f.close()

    def close(self):
	self.container.close()

    def getVersion(self, version):
	return self.container.getFile(version)

    # data can be a string, which is written into the new version, or
    # a file-type object, whose contents are copied into the new version
    def addVersion(self, version, data):
	self.container.addFile(version, data, "")

    def hasVersion(self, version):
	return self.container.has_key(version)

    def versionList(self):
	return self.container.fileList()

    def __init__(self, filename, mode):
	return self.open(filename, mode);

def open(filename, mode):
    return File(filename, mode)

def latest(versionList):
    # for now the lastest version is the last in this list

    list = versionList[:]
    list.sort()
    return list[-1]


