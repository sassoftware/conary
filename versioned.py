#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
from filecontainer import FileContainer
import __builtin__
import time
import string
import versions
import types
import dbhash

_FILE_MAP = "FILEMAP"
_VERSION_INFO = "VINFO-%s-%s"
_BRANCH_MAP = "BMAP-%s"
_CONTENTS = "%s %s"

# implements a set of versioned files on top of a single hashed db file
#
# FileIndexedDatabase provides a list of files present, and stores that
# list in the _FILE_MAP entry
#
# Each file has a mapping of branch names to the head of that branch
#   stored as _BRANCH_MAP
# Each file/version pair has an info node which stores a reference to both
#   the parent and child of that version on the branch; they are stored
#   as frozen versions to allow them to be properly ordered. It also stores
#   the frozen version of the version uses info is being stored
# The contents of each file are stored as _CONTENTS
#
# the versions are expected to be Version objects as defined by the versions
# module

class FalseFile:

    def seek(self, count, whence = 0):
	if whence == 0:
	    self.pos = count
	elif whence == 1:
	    self.pos = self.pos + count
	elif whence == 2:
	    self.pos = self.len + count
	else:
	    raise IOError, "invalid whence for seek"

	if self.pos < 0:
	    self.pos = 0
	elif self.pos > self.len:
	    self.pos = self.len
	
	return self.pos

    def read(self, amount = - 1):
	if amount == -1 or (self.pos + amount > self.len):
	    oldpos = self.pos
	    self.pos = self.len
	    return self.contents[oldpos:]

	oldpos = self.pos
	self.pos = self.pos + amount
	return self.contents[oldpos:self.pos]

    def readLines(self):
	list = self.read().split('\n')
	list2 = []

	# cut off the last element (which wasn't newline terminated anyway)
	for item in list[:-1]:
	    list2.append(item + "\n")
	return list2

    def close(self):
	del self.contents

    def __init__(self, contents):
	self.contents = contents
	self.len = len(contents)
	self.pos = 0

class VersionedFile:

    # the branch map maps a fully qualified branch version to the latest
    # version on that branch; it's formatted as [<branch> <version>\n]+
    def readBranchMap(self):
	if self.branchMap: return

	self.branchMap = {}
	if not self.db.has_key(_BRANCH_MAP % self.key): return

	# the last entry has a \n, so splitting at \n creates an empty item
	# at the end
	branchList = self.db[_BRANCH_MAP % self.key].split('\n')[:-1]
	for mapString in branchList:
	    (branchString, versionString) = mapString.split()
	    self.branchMap[branchString] = \
		versions.ThawVersion(versionString)

    def writeBranchMap(self):
	str = "".join(map(lambda x: "%s %s\n" % 
					    (x, self.branchMap[x].freeze()), 
			    self.branchMap.keys()))
	self.db[_BRANCH_MAP % self.key] = str

    def getVersion(self, version):
	return FalseFile(self.db[_CONTENTS % (self.key, version.asString())])

    def findLatestVersion(self, branch):
	self.readBranchMap()

	branchStr = branch.asString()

	if not self.branchMap.has_key(branchStr): return None

	return self.branchMap[branchStr]

    # converts a version to one w/ a timestamp
    def getFullVersion(self, version):
	return self._getVersionInfo(version)[0]

    def _getVersionInfo(self, version):
	s = self.db[_VERSION_INFO % (self.key, version.asString())]
	l = s.split()

	v = versions.ThawVersion(l[0])

	if l[1] == "-":
	    previous = None
	else:
	    previous = versions.ThawVersion(l[1])

	if l[2] == "-":
	    next = None
	else:
	    next = versions.ThawVersion(l[2])

	return (v, previous, next)

    def _writeVersionInfo(self, node, parent, child):
	vStr = node.freeze()

	if parent:
	    pStr = parent.freeze()
	else:
	    pStr = "-"

	if child:
	    cStr = child.freeze()
	else:
	    cStr = "-"

	self.db[_VERSION_INFO % (self.key, node.asString())] = \
	    "%s %s %s" % (vStr, pStr, cStr)

    # data can be a string, which is written into the new version, or
    # a file-type object, whose contents are copied into the new version
    #
    # the new addition gets placed on the proper branch in the position
    # determined by the version's time stamp
    def addVersion(self, version, data):
	self.readBranchMap()

	versionStr = version.asString()
	branchStr = version.branch().asString()

	if type(data) is not str:
	    data = data.read()

	# start at the end of this branch and work backwards until we
	# find the right position to insert this node; this is quite
	# efficient for adding at the end of a branch, which is the
	# normal case
	if self.branchMap.has_key(branchStr):
	    curr = self.branchMap[branchStr]
	    next = None
	    while curr and curr.isAfter(version):
		next = curr
		curr = self._getVersionInfo(curr)[1]
	else:
	    curr = None
	    next = None

	# curr is the version we should be added after, None if we are
	# the first item on the list
	#
	# next is the item which immediately follows this one; this lets
	# us add at the head

	# this is (node, newParent, newChild)
	self._writeVersionInfo(version, curr, next)
	if curr:
	    (node, parent, child) = self._getVersionInfo(curr)
	    self._writeVersionInfo(curr, parent, version)
	if next:
	    (node, parent, child) = self._getVersionInfo(next)
	    self._writeVersionInfo(next, version, child)

	self.db[_CONTENTS % (self.key, versionStr)] = data

	# if this is the new head of the branch, update the branch map
	if not next:
	    self.branchMap[branchStr] = version
	    self.writeBranchMap()

	#self.db.sync()

    def eraseVersion(self, version):
	self.readBranchMap()

	versionStr = version.asString()
	branchStr = version.branch().asString()

	(node, prev, next) = self._getVersionInfo(version)

	# if this is the head of the branch we need to move the head back
	if self.branchMap[branchStr].equal(version):
	    # we were the only item, so the branch needs to be removed
	    if not prev:
		del self.branchMap[branchStr]
	    else:
		self.branchMap[branchStr] = prev

	    self.writeBranchMap()

	if prev:
	    thePrev = self._getVersionInfo(prev)[1]
	    self._writeVersionInfo(prev, thePrev, next)
	
	if next:
	    theNext = self._getVersionInfo(next)[2]
	    self._writeVersionInfo(next, prev, theNext)

	del self.db[_CONTENTS % (self.key, versionStr)]
	del self.db[_VERSION_INFO % (self.key, versionStr)]

	#self.db.sync()

    def hasVersion(self, version):
	return self.db.has_key(_VERSION_INFO % (self.key, version.asString()))

    # returns a list of version objects
    def versionList(self, branch):
	self.readBranchMap()

	curr = self.branchMap[branch.asString()]
	list = []
	while curr:
	    list.append(curr)
	    curr = self._getVersionInfo(curr)[1]
	
	return list

    def branchList(self):
	self.readBranchMap()
	return map(lambda x: x.branch(), self.branchMap.values())

    def __init__(self, db, filename):
	self.db = db
	self.key = filename
	self.branchMap = None

class Database:

    def openFile(self, file):
	return VersionedFile(self.db, file)

    def close(self):
	self.db.close()

    def __init__(self, path, mode = "r"):
	self.db = dbhash.open(path, mode)

class FileIndexedDatabase(Database):

    def openFile(self, file):
	if not self.files.has_key(file):
	    self.files[file] = 1
	    self.writeMap()

	return Database.openFile(self, file)

    def hasFile(self, file):
	return self.files.has_key(file)

    def readMap(self):
	self.files = {}

	if self.db.has_key(_FILE_MAP):
	    map = self.db[_FILE_MAP]
	    for line in map.split('\n'):
		self.files[line] = 1

    def writeMap(self):
	map = string.join(self.files.keys(), '\n')
	self.db[_FILE_MAP] = map
	#self.db.sync()

    def fileList(self):
	return self.files.keys()

    def __init__(self, path, mode = "r"):
	Database.__init__(self, path, mode)
	self.readMap()

