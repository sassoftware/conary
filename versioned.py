#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
from filecontainer import FileContainer
import __builtin__
import time
import versions

# implements a simple versioned file on top of a FileContainer; the version
# string is used as the file name, and branch marks are stored as extra data 
# for the file
#
# the versions are expected to be Version objects as defined by the versions
# module

class File:

    def open(self, filename, mode):
	f = __builtin__.open(filename, mode)
	self.container = FileContainer(f)
	f.close()

	self.versionMap = {}
	for versionString in self.container.fileList():
	    self.versionMap[versionString] = \
		    versions.VersionFromString(versionString)

    def close(self):
	self.container.close()

    def getVersion(self, version):
	return self.container.getFile(version.asString())

    def findLatestVersion(self, branch):
	matchesByTime = {}
	for (verString, version) in self.versionMap.items():
	    if version.onBranch(branch):
		time = float(self.container.getTag(verString))
		matchesByTime[time] = version
	l = matchesByTime.keys()
	l.sort()
	if not l:
	    return None

	return matchesByTime[l[-1]]

    # data can be a string, which is written into the new version, or
    # a file-type object, whose contents are copied into the new version
    def addVersion(self, version, data):
	version.isBranch()

	versionStr = version.asString()
	self.container.addFile(versionStr, data, "%f" % time.time())
	self.versionMap[versionStr] = version

    def hasVersion(self, version):
	return self.versionMap.has_key(version.asString())

    def versionList(self):
	# returns a list of version objects
	return self.versionMap.values()

    def __init__(self, filename, mode):
	return self.open(filename, mode);

def open(filename, mode):
    return File(filename, mode)

def latest(versionList):
    # for now the lastest version is the last in this list

    list = versionList[:]
    list.sort()
    return list[-1]
