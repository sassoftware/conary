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
_VERSION_MAP = "VMAP-%s"
_BRANCH_MAP = "BMAP-%s"
_CONTENTS = "%s %s"

# implements a simple versioned file on top of a FileContainer; the version
# string is used as the file name, and branch marks are stored as extra data 
# for the file
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

    def readVersionMap(self):
	if self.versionMap != None: return

	self.versionMap = {}

	if not self.db.has_key(_VERSION_MAP % self.key):
	    return

	# chop off the emptry string which gets created after the final \n
	versionList = self.db[_VERSION_MAP % self.key].split('\n')[:-1]

	for mapString in versionList:
	    (versionString, versionTime) = mapString.split()
	    self.versionMap[versionString] = \
		(versions.VersionFromString(versionString), float(versionTime))

    def writeVersionMap(self):
	rc = ""
	for (versionString, (version, time)) in self.versionMap.items():
	    rc += "%s %.3f\n" % (versionString, time)
	self.db[_VERSION_MAP % self.key] = rc

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
		versions.VersionFromString(versionString)

    def writeBranchMap(self):
	str = "".join(map(lambda x: "%s %s\n" % 
					    (x, self.branchMap[x].asString()), 
			    self.branchMap.keys()))
	self.db[_BRANCH_MAP % self.key] = str

    def getVersion(self, version):
	return FalseFile(self.db[_CONTENTS % (self.key, version.asString())])

    def findLatestVersion(self, branch):
	matchesByTime = {}
	self.readBranchMap()

	branchStr = branch.asString()

	if not self.branchMap.has_key(branchStr): return None

	return self.branchMap[branchStr]

    # data can be a string, which is written into the new version, or
    # a file-type object, whose contents are copied into the new version
    #
    # the new addition becomes the latest version on the branch
    def addVersion(self, version, data):
	self.readVersionMap()
	self.readBranchMap()

	versionStr = version.asString()
	branchStr = version.branch().asString()

	if type(data) is not str:
	    data = data.read()
	self.db[_CONTENTS % (self.key, versionStr)] = data
	self.versionMap[versionStr] = (version, time.time())
	self.branchMap[branchStr] = version

	self.writeVersionMap()
	self.writeBranchMap()
	self.db.sync()

    def eraseVersion(self, version):
	self.readVersionMap()
	    
	versionStr = version.asString()
	del self.db[_CONTENTS % (self.key, versionStr)]
	del self.versionMap[versionStr]
	self.writeVersionMap()
	self.db.sync()

    def hasVersion(self, version):
	self.readVersionMap()
	return self.versionMap.has_key(version.asString())

    # returns a list of version objects
    def versionList(self):
	self.readVersionMap()
	    
	list = []
	for (version, time) in self.versionMap.values():
	    list.append(version)

	return list

    def __init__(self, db, filename):
	self.db = db
	self.key = filename
	self.versionMap = None
	self.branchMap = None

class Database:

    def openFile(self, file):
	return VersionedFile(self.db, file)

    def __init__(self, path):
	# FIXME: this needs locking
	self.db = dbhash.open(path, "c")

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
	self.db.sync()

    def fileList(self):
	return self.files.keys()

    def __init__(self, path):
	Database.__init__(self, path)
	self.readMap()

