#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import os
import versioned

SEEK_SET=0
SEEK_CUR=1
SEEK_END=2

class FileContents:

    def __init__(self):
	if self.__class__ == FileContents:
	    raise NotImplementedError

class FromRepository(FileContents):

    def get(self):
	return self.repos.pullFileContentsObject(self.fileId)

    def size(self):
	return self.theSize

    def __init__(self, repos, fileId, size):
	self.repos = repos
	self.fileId = fileId
	self.theSize = size

class FromFilesystem(FileContents):

    def get(self):
	return open(self.path, "r")

    def size(self):
	return os.stat(self.path).st_size

    def __init__(self, path):
	self.path = path

class FromChangeSet(FileContents):

    def get(self):
	return self.cs.getFileContents(self.fileId)[1].get()

    def size(self):
	return self.cs.getFileSize(self.fileId)

    def __init__(self, cs, fileId):
	self.cs = cs
	self.fileId = fileId

class FromString(FileContents):

    def get(self):
	return versioned.FalseFile(self.str)

    def size(self):
	return len(self.str)

    def __init__(self, str):
	self.str = str

class FromFile(FileContents):

    def size(self):
	pos = self.f.tell()
	size = self.f.seek(0, SEEK_END)
	self.f.seek(pos, SEEK_SET)
        return size

    def get(self):
	return self.f

    def __init__(self, f):
	self.f = f

class WithFailedHunks(FileContents):

    def get(self):
	return self.fc.get()

    def getHunks(self):
	return self.hunks

    def size(self):
	return self.fc.size()

    def __init__(self, fc, hunks):
	self.fc = fc
	self.hunks = hunks
