#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import versioned

class FileContents:

    def __init__(self):
	if self.__class__ == FileContents:
	    raise NotImplemented

class FromRepository(FileContents):

    def get(self):
	return self.repos.pullFileContentsObject(self.fileId)

    def __init__(self, repos, fileId):
	self.repos = repos
	self.fileId = fileId

class FromFilesystem(FileContents):

    def get(self):
	return open(self.path, "r")

    def __init__(self, path):
	self.path = path

class FromChangeSet(FileContents):

    def get(self):
	return self.cs.getFileContents(self.fileId)[1].get()

    def __init__(self, cs, fileId):
	self.cs = cs
	self.fileId = fileId

class FromString(FileContents):

    def get(self):
	return versioned.FalseFile(self.str)

    def __init__(self, str):
	self.str = str

class FromFile(FileContents):

    def get(self):
	return self.f

    def __init__(self, f):
	self.f = f

class WithFailedHunks(FileContents):

    def get(self):
	return self.fc.get()

    def getHunks(self):
	return self.hunks

    def __init__(self, fc, hunks):
	self.fc = fc
	self.hunks = hunks
