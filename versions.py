#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

# classes for version structures and strings

import copy
import time
import string

class AbstractVersion:

    def __init__(self):
	pass

class AbstractBranch:

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
	    raise ParseError, ("version numbers may not have hyphens: %s" % value)

	try:
	    int(self.version[0])
	except:
	    raise ParseError, ("version numbers must be begin with a digit: %s" % value)

	try:
	    self.release = int(self.release)
	except:
	    raise ParseError, ("release numbers must be all numeric: %s" % value)

class BranchName(AbstractBranch):

    def __str__(self):
	return self.host + '@' + str(self.branch)

    def equal(self, version):
	if (type(self) == type(version) and self.host == version.host
		and self.branch == version.branch):
	    return 1
	return 0

    def __init__(self, value):
	if value.find("@") == -1:
	    raise ParseError, "@ expected between hostname and branch name"

	(self.host, self.branch) = value.split("@", 1)
	if self.branch.find("@") != -1:
	    raise ParseError, ("branch names may not have @ signs: %s" % value)

class LocalBranch(BranchName):

    def __init__(self):
	BranchName.__init__(self, "localhost@LOCAL")

class Version:

    def appendVersionRelease(self, version, release):
	assert(self.isBranch())
	self.versions.append(VersionRelease("%s-%d" % (version, release)))
	self.timeStamp = time.time()

    def incrementVersionRelease(self):
	assert(self.isVersion())
	
	self.versions[-1].incrementRelease()
	self.timeStamp = time.time()

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

    def asString(self, defaultBranch = None):
	list = self.versions
	s = "/"

	if defaultBranch and len(defaultBranch.versions) < len(self.versions):
	    start = Version(self.versions[0:len(defaultBranch.versions)], 0)
	    if start.equal(defaultBranch):
		list = self.versions[len(defaultBranch.versions):]
		s = ""

	for version in list:
	    s = s + ("%s/" % version)

	return s[:-1]

    def freeze(self, defaultBranch = None):
	return ("%.3f:" % self.timeStamp) + self.asString(defaultBranch)

    def isBranch(self):
	return isinstance(self.versions[-1], BranchName)

    # true as long as we're either on the local branch or part of the
    # name of the local branch
    def isLocal(self):
	return isinstance(self.versions[-1], LocalBranch) or \
	       isinstance(self.versions[-2], LocalBranch)

    def onBranch(self, branch):
	if self.isBranch(): return 0
	return self.listsEqual(self.versions[:-1], branch)

    def branch(self):
	assert(not self.isBranch())
	return Version(self.versions[:-1], 0)

    def branchNickname(self):
	assert(self.isBranch())
	return self.versions[-1]

    def parent(self):
	assert(self.isVersion())
	assert(len(self.versions) > 3)
	return Version(self.versions[:-2], 0)

    def isBefore(self, other):
	return self.timeStamp < other.timeStamp

    def isAfter(self, other):
	return self.timeStamp > other.timeStamp

    def isVersion(self):
	return isinstance(self.versions[-1], VersionRelease)

    def copy(self):
        return copy.deepcopy(self)

    def fork(self, branch, sameVerRel = 1):
	assert(isinstance(branch, AbstractBranch))
	newlist = [ branch ]

	if sameVerRel:
	    newlist.append(self.versions[-1])

	return Version(self.versions + newlist, time.time())

    def parseVersionString(self, ver, defaultBranch = None):
	if ver[0] != "/":
            # XXX broken code, no defaultBranch in this scope
	    if not defaultBranch:
		raise KeyError, "relative version given without a default " \
			        "branch"
	    ver = defaultBranch.asString() + "/" + ver

	parts = ver.split("/")
	del parts[0]	# absolute versions start with a /

	v = []
	while parts:
	    if parts[0] == "localhost@LOCAL":
		v.append(LocalBranch())
	    else:
		v.append(BranchName(parts[0]))

	    if len(parts) >= 2:
		v.append(VersionRelease(parts[1]))
		parts = parts[2:]
	    else:
		parts = None

	return v

    def __init__(self, versionList, timeStamp):
	self.versions = versionList
	self.timeStamp = timeStamp
	
class ThawVersion(Version):

    def __init__(self, fullString):
	(timeStr, ver) = fullString.split(":")

	timeVal = float(timeStr)
	v = self.parseVersionString(ver)

	Version.__init__(self, v, timeVal)

class VersionFromString(Version):

    def __init__(self, ver, defaultBranch = None):
	if ver[0] != "/":
	    ver = defaultBranch.asString() + "/" + ver

	v = self.parseVersionString(ver, defaultBranch)

	Version.__init__(self, v, 0)

class VersionsError(Exception):

    pass

class ParseError(VersionsError):

    def __repr__(self):
	return self.str

    def __str__(self):
	return self.str

    def __init__(self, str):
	self.str = str
