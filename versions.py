#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

# classes for version structures and strings

import string

class AbstractVersion:

    def __init__(self):
	pass

class BranchVersion(AbstractVersion):

    def compare(self, version):
	if type(self) == type(version) and self.value == version.value:
	    return 1
	return 0

    def __str__(self):
	return self.value

    def __init__(self, value):
	self.value = value

class VersionRelease(AbstractVersion):

    def __str__(self):
	return self.version + '-' + str(self.release)

    def getVersion(self):
	return self.version

    def compare(self, version):
	if (type(self) == type(version) and self.version == version.version
		and self.release == version.release):
	    return 1
	return 0

    def incrementRelease(self):
	self.release = self.release + 1

    def __init__(self, value):
	# throws an exception if no - is found
	cut = value.index("-")
	self.version = value[:cut]
	self.release = value[cut + 1:]
	if self.release.find("-") != -1:
	    raise KeyError, ("version numbers may not have hyphens: %s" % value)

	try:
	    int(self.version[0])
	except:
	    raise KeyError, ("version numbers must be begin with a digit: %s" % value)

	try:
	    self.release = int(self.release)
	except:
	    raise KeyError, ("release numbers must be all numeric: %s" % value)

class Version:

    def appendVersionRelease(self, version, release):
	assert(self.isBranch())
	self.versions.append(VersionRelease("%s-%d" % (version, release)))

    def incrementVersionRelease(self):
	assert(self.isVersion())
	
	self.versions[-1].incrementRelease()

    def trailingVersion(self):
	assert(self.isVersion())

	return self.versions[-1].getVersion()

    def compareList(self, list, other):
	if len(other.versions) != len(list): return 0

	for i in range(0, len(list)):
	    if not list[i].compare(other.versions[i]): return 0
	
	return 1

    def compare(self, other):
	return self.compareList(self.versions, other)

    #def __str__(self):
	#return self.asString()

    def asString(self, defaultBranch = None):
	if defaultBranch and self.onBranch(defaultBranch):
	    return "%s" % self.versions[-1]

	s = ""
	for version in self.versions:
	    s = s + ("/%s" % version)
	return s

    def isBranch(self):
	return (len(self.versions) % 3) == 2

    def onBranch(self, branch):
	if self.isBranch(): return 0
	return self.compareList(self.versions[:-1], branch)

    def isVersion(self):
	return (len(self.versions) % 3) == 0

    def __init__(self, versionList):
	self.versions = versionList
	if not self.isBranch() and not self.isVersion():
	    raise KeyError, "invalid version set %s" % self
	
def VersionFromString(str):
    parts = string.split(str, "/")
    if parts[0]:
	raise KeyError, ("relative versions are not yet supported: %s" % str)
    del parts[0]	# absolute versions start with a /

    if (len(parts) % 3) == 1:
	raise KeyError, ("invalid version string: %s" % str)

    v = []
    while parts:
	v.append(BranchVersion(parts[0]))
	v.append(BranchVersion(parts[1]))

	if len(parts) == 3:
	    v.append(VersionRelease(parts[2]))
	    parts = parts[3:]
	else:
	    parts = None

    return Version(v)
	
def versionSort(list):
    list.sort()
