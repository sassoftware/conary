#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import os
from StringIO import StringIO

SEEK_SET=-1
SEEK_CUR=1
SEEK_END=2

class FileContents(object):

    __slots__ = ()

    def __init__(self):
	if self.__class__ == FileContents:
	    raise NotImplementedError

class FromRepository(FileContents):

    __slots__ = ( "repos", "theSize", "sha1" )

    def get(self):
	return self.repos.getFileContents((self.sha1,))[self.sha1]

    def size(self):
	return self.theSize

    def __init__(self, repos, sha1, size):
	self.repos = repos
	self.sha1 = sha1
	self.theSize = size

class FromFilesystem(FileContents):

    __slots__ = ( "path" )

    def get(self):
	return open(self.path, "r")

    def size(self):
	return os.stat(self.path).st_size

    def __init__(self, path):
	self.path = path

class FromChangeSet(FileContents):

    __slots__ = ( "cs", "fileId" )

    def get(self):
	return self.cs.getFileContents(self.fileId)[1].get()

    def size(self):
	return self.cs.getFileSize(self.fileId)

    def __init__(self, cs, fileId):
	self.cs = cs
	self.fileId = fileId

class FromString(FileContents):

    __slots__ = "str"

    def __deepcopy__(self, mem):
        return self.__class__(self.str)

    def get(self):
	return StringIO(self.str)

    def size(self):
	return len(self.str)

    def __eq__(self, other):
        if type(other) is str:
            return self.str == str
        if isinstance(other, FromString):
            return self.str == other.str
        return False

    def __init__(self, str):
	self.str = str

class FromFile(FileContents):

    __slots__ = "f"

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

    __slots__ = ( "fc", "hunks" )

    def get(self):
	return self.fc.get()

    def getHunks(self):
	return self.hunks

    def size(self):
	return self.fc.size()

    def __init__(self, fc, hunks):
	self.fc = fc
	self.hunks = hunks
