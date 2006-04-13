#
# Copyright (c) 2004 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

from StringIO import StringIO

from conary.lib import sha1helper

SEEK_SET=-1
SEEK_CUR=1
SEEK_END=2

class FileContents(object):

    __slots__ = ()

    def copy(self):
        raise NotImplementedError

    def get(self):
        raise NotImplementedError

    def __init__(self):
	if self.__class__ == FileContents:
	    raise NotImplementedError

class FromDataStore(FileContents):

    __slots__ = ( "store", "theSize", "sha1" )

    def copy(self):
        return self.__class__(self.store, self.sha1, self.theSize)

    def get(self):
	return self.store.openFile(sha1helper.sha1ToString(self.sha1))

    def path(self):
        return self.store.hashToPath(sha1helper.sha1ToString(self.sha1))

    def __init__(self, store, sha1):
	self.store = store
	self.sha1 = sha1

class CompressedFromDataStore(FileContents):

    __slots__ = ( "store", "theSize", "sha1" )

    def getSha1(self):
        return self.sha1

    def copy(self):
        return self.__class__(self.store, self.sha1, self.theSize)

    def get(self):
	return self.store.openRawFile(sha1helper.sha1ToString(self.sha1))

    def path(self):
        return self.store.hashToPath(sha1helper.sha1ToString(self.sha1))

    def __init__(self, store, sha1):
	self.store = store
	self.sha1 = sha1

class FromFilesystem(FileContents):

    __slots__ = ( "path" )

    def get(self):
	return open(self.path, "r")

    def __init__(self, path):
	self.path = path

class FromChangeSet(FileContents):

    __slots__ = ( "cs", "pathId" )

    def copy(self):
        return self.__class__(self.cs, self.pathId)

    def get(self):
	return self.cs.getFileContents(self.pathId)[1].get()

    def __init__(self, cs, pathId):
	self.cs = cs
	self.pathId = pathId

class CompressedFromChangeSet(FromChangeSet):

    def get(self):
	return self.cs.getFileContents(self.pathId, compressed = True)[1].get()

class FromString(FileContents):

    __slots__ = "str"

    def copy(self):
        return self.__class__(self.str)

    def get(self):
	return StringIO(self.str)

    def __eq__(self, other):
        if type(other) is str:
            return self.str == str
        if isinstance(other, FromString):
            return self.str == other.str
        return False

    def __init__(self, str):
	self.str = str

class FromFile(FileContents):

    __slots__ = [ "f", "theSize" ]

    def copy(self):
        # XXX dup the file?
        return self.__class__(self.f)

    def get(self):
	return self.f

    def __init__(self, f):
	self.f = f

class FromGzFile(FileContents):

    __slots__ = "f"

    def copy(self):
        return self.__class__(self.f)

    def get(self):
	return self.f

    def __init__(self, f):
	self.f = f

class WithFailedHunks(FileContents):

    __slots__ = ( "fc", "hunks" )

    def copy(self):
        return self.__class__(self.fc, self.hunks)

    def get(self):
	return self.fc.get()

    def getHunks(self):
	return self.hunks

    def __init__(self, fc, hunks):
	self.fc = fc
	self.hunks = hunks
