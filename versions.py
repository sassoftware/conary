#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

# classes for version structures and strings

import string

class AbstractVersion:

    def __init__(self):
	pass

class VersionRelease(AbstractVersion):

    def __str__(self):
	return self.version + '-' + str(self.release)

    def getVersion(self):
	return self.version

    def equal(self, version):
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

class BranchName(AbstractVersion):

    def __str__(self):
	return self.host + '@' + str(self.branch)

    def equal(self, version):
	if (type(self) == type(version) and self.host == version.host
		and self.branch == version.branch):
	    return 1
	return 0

    def __init__(self, value):
	# throws an exception if no @ is found
	#cut = value.index("@")
	(self.host, self.branch) = string.split(value, "@", 1)
	#self.host = value[:cut]
	#self.branch = value[cut + 1:]
	if self.branch.find("@") != -1:
	    raise KeyError, ("branch names may not have @ signs: %s" % value)

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

    def listsEqual(self, list, other):
	if len(other.versions) != len(list): return 0

	for i in range(0, len(list)):
	    if not list[i].equal(other.versions[i]): return 0
	
	return 1

    def equal(self, other):
	return self.listsEqual(self.versions, other)

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
	return isinstance(self.versions[-1], BranchName)

    def onBranch(self, branch):
	if self.isBranch(): return 0
	return self.listsEqual(self.versions[:-1], branch)

    def branch(self):
	assert(not self.isBranch())
	return Version(self.versions[:-1])

    def isVersion(self):
	return isinstance(self.versions[-1], VersionRelease)

    def __init__(self, versionList):
	self.versions = versionList
	if not self.isBranch() and not self.isVersion():
	    raise KeyError, "invalid version set %s" % self
	
def VersionFromString(str, defaultBranch = None):
    if str[0] != "/":
	if not defaultBranch:
	    raise KeyError, "relative version given without a default branch"
	str = defaultBranch.asString() + "/" + str

    parts = string.split(str, "/")
    del parts[0]	# absolute versions start with a /

    v = []
    while parts:
	v.append(BranchName(parts[0]))

	if len(parts) >= 2:
	    v.append(VersionRelease(parts[1]))
	    parts = parts[2:]
	else:
	    parts = None

    return Version(v)
	
def versionSort(list):
    list.sort()
