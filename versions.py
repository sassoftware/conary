#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""Classes for version structures and strings"""

import copy
import time
import string

class AbstractVersion:

    """
    Ancestor class for all versions (as opposed to branches)
    """

    def __init__(self):
	pass

class AbstractBranch:

    """
    Ancestor class for all branches (as opposed to versions)
    """

    def __init__(self):
	pass

class VersionRelease(AbstractVersion):

    """
    Version element for a version/release pair. These are formatted as
    "version-release", with no hyphen allowed in either portion. The
    release must be a simple integer or two integers separated by a
    decimal point.
    """

    def __str__(self):
	"""
	Returns a string representation of a version/release pair.
	"""
	if self.buildCount != None:
	    return "%s-%d.%d" % (self.version, self.release, self.buildCount)
	
	return self.version + '-' + str(self.release)

    def getVersion(self):
	"""
	Returns the version string of a version/release pair.
	"""

	return self.version

    def equal(self, version):
	"""
	Compares two version-type objects and tells if they are the same
	or not.

	@rtype: boolean
	"""
	if (type(self) == type(version) and self.version == version.version
		and self.release == version.release
		and self.buildCount == version.buildCount):
	    return 1
	return 0

    def incrementRelease(self):
	"""
	Incremements the release number.
	"""
	self.release += 1

    def incrementBuildCount(self):
	"""
	Incremements the build count
	"""
	if self.buildCount:
	    self.buildCount += 1
	else:
	    self.buildCount = 1

    def __init__(self, value):
	"""
	Initialize a VersionRelease object from a string representation
	of a version release. ParseError exceptions are thrown if the
	string representation is ill-formed.

	@param value: String representation of a VersionRelease
	@type value: string
	"""
	if value.find("@") != -1:
	    raise ParseError, "version/release pairs may not contain @ signs"
	cut = value.find("-")
	if cut == -1:
	    raise ParseError, ("version/release pair was expected")
	self.version = value[:cut]

	try:
	    int(self.version[0])
	except:
	    raise ParseError, \
		("version numbers must be begin with a digit: %s" % value)

	fullRelease = value[cut + 1:]
	cut = fullRelease.find(".") 
	if cut != -1:
	    self.release = fullRelease[:cut]
	    self.buildCount = fullRelease[cut + 1:]
	else:
	    self.release = fullRelease
	    self.buildCount = None

	try:
	    self.release = int(self.release)
	except:
	    raise ParseError, ("release numbers must be all numeric: %s" % value)
	if self.buildCount:
	    try:
		self.buildCount = int(self.buildCount)
	    except:
		raise ParseError, \
		    ("build count numbers must be all numeric: %s" % value)

class BranchName(AbstractBranch):

    """
    Stores a branch name, which is the same as a nickname. Branch names
    are of the form hostname@branch.
    """

    def __str__(self):
	"""
	Returns the string representation of a branch name.
	"""
	return self.host + '@' + str(self.branch)

    def getHost(self):
	return self.host

    def equal(self, version):
	"""
	Compares the BranchName object to another object, and returns
	true if they refer to the same branch.

	@param version: version to compare against
	@type version: instance
	@rtype: boolean
	"""
	if (self.__class__ == version.__class__
	     and self.host == version.host and self.branch == version.branch):
	    return 1
	return 0

    def __init__(self, value):
	"""
	Parses a branch name string into a BranchName object. A ParseError is
	thrown if the BranchName is not well formed.

	@param value: String representation of a BranchName
	@type value: str
	"""
	if value.find("@") == -1:
	    raise ParseError, "@ expected between hostname and branch name"
	if value.find("/") != -1:
	    raise ParseError, "/ should not appear in a branch name"

	(self.host, self.branch) = value.split("@", 1)
	if self.branch.find("@") != -1:
	    raise ParseError, ("branch names may not have @ signs: %s" % value)

class LocalBranch(BranchName):

    """
    Class defining the local branch.
    """

    def __init__(self):
	BranchName.__init__(self, "localhost@LOCAL")

class Version:

    """
    Class representing a version. Versions are a list of AbstractBranch,
    AbstractVersion sequences. If the last item is an AbstractBranch (meaning
    an odd number of objects are in the list, the version represents
    a branch. A version includes a time stamp, which is used for
    ordering.
    """

    def appendVersionRelease(self, version, release):
	"""
	Converts a branch to a version. The version/release passed in
	are converted to a VersionRelease object and appended to the
	branch this object represented. The time stamp is reset as
	a new version has been created.

	@param version: string representing a version
	@type version: str
	@param release: release number
	@type release: int
	"""
	assert(self.isBranch())
	self.versions.append(VersionRelease("%s-%d" % (version, release)))
	self.timeStamp = time.time()

    def appendVersionReleaseObject(self, verRel):
	"""
	Converts a branch to a version. The version/release passed in
	are appended to the branch this object represented. The time
	stamp is reset as a new version has been created.

	@param version: object for the version and release
	@type version: VersionRelease
	@param release: release number
	@type release: int
	"""
	assert(self.isBranch())
	self.versions.append(verRel)
	self.timeStamp = time.time()

    def incrementRelease(self):
	"""
	The release number for the final element in the version is
	incremented by one and the time stamp is reset.
	"""
	assert(self.isVersion())
	
	self.versions[-1].incrementRelease()
	self.timeStamp = time.time()

    def incrementBuildCount(self):
	"""
	The build count number for the final element in the version is
	incremented by one and the time stamp is reset.
	"""
	assert(self.isVersion())
	
	self.versions[-1].incrementBuildCount()
	self.timeStamp = time.time()

    def trailingVersion(self):
	"""
	Returns the AbstractVersion object at the end of the version.

	@rtype: AbstactVersion
	"""
	assert(self.isVersion())

	return self.versions[-1]

    def _listsEqual(self, list, other):
	if len(other.versions) != len(list): return 0

	for i in range(0, len(list)):
	    if not list[i].equal(other.versions[i]): return 0
	
	return 1

    def equal(self, other):
	"""
	Compares this object to another Version object to see if they
	are the same.

	@rtype: boolean
	"""
	return self._listsEqual(self.versions, other)

    def asString(self, defaultBranch = None):
	"""
	Returns a string representation of the version.

	@param defaultBranch: If set this is stripped fom the beginning
	of the version to give a shorter string representation.
	@type defaultBranch: Version
	@rtype: str
	"""
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
	"""
	Returns a complete string representation of the version, including
	the time stamp.

	@rtype: str
	"""
	return ("%.3f:" % self.timeStamp) + self.asString(defaultBranch)

    def isBranch(self):
	"""
	Tests whether or not the current object is a branch.

	@rtype: boolean
	"""
	return isinstance(self.versions[-1], BranchName)

    def isVersion(self):
	"""
	Tests whether or not the current object is a version (not a branch).

	@rtype: boolean
	"""
	return isinstance(self.versions[-1], VersionRelease)

    def isLocal(self):
    	"""
	Tests whether this is the local branch, or is a version on
	the local branch

	@rtype: boolean
	"""
	return isinstance(self.versions[-1], LocalBranch) or    \
	    (len(self.versions) > 1 and 
	     isinstance(self.versions[-2], LocalBranch))

    def onBranch(self, branch):
	"""
	Tests whether or not the current object is a version on the
	specified branch.

	@rtype: boolean
	"""
	if self.isBranch(): return 0
	return self._listsEqual(self.versions[:-1], branch)

    def branch(self):
	"""
	Returns the branch this version is part of.

	@rtype: Version
	"""
	assert(not self.isBranch())
	return Version(self.versions[:-1], 0)

    def branchNickname(self):
	"""
	Returns the BranchName object at the end of a branch. This is
	known as the branch nick name, as is used in VersionedFiles as
	an index.

	@rtype: BranchName
	"""
	assert(self.isBranch())
	return self.versions[-1]

    def parent(self):
	"""
	Returns the parent version for this version (the version this
	object's branch branched from.

	@rtype: Version
	"""
	assert(self.isVersion())
	assert(len(self.versions) > 3)
	return Version(self.versions[:-2], 0)

    def parentNode(self):
	"""
	Returns the parent version of a branch.

	@rtype: Version
	"""
	assert(self.isBranch())
	assert(len(self.versions) >= 3)
	return Version(self.versions[:-1], 0)

    def hasParent(self):
	"""
	Tests whether or not the current branch or version has a parent.
	True for all versions other then those on trunks.

	@rtype: boolean
	"""
	return(len(self.versions) >= 3)

    def isBefore(self, other):
	"""
	Tests whether the parameter is a version earlier then this object.

	@param other: Object to test against
	@type other: Version
	@rtype: boolean
	"""
	return self.timeStamp < other.timeStamp

    def isAfter(self, other):
	"""
	Tests whether the parameter is a version later then this object.

	@param other: Object to test against
	@type other: Version
	@rtype: boolean
	"""
	return self.timeStamp > other.timeStamp

    def copy(self):
	"""
	Returns a Version object which is a copy of this object. The
	result can be modified without affecting this object in any way.j

	@rtype: Version
	"""

        return copy.deepcopy(self)

    def fork(self, branch, sameVerRel = True):
	"""
	Creates a new branch from this version. 

	@param branch: Branch to create for this version
	@type branch: AbstractBranch
	@param sameVerRel: If set, the new branch is turned into a version
	on the branch using the same version and release as the original
	verison.
	@type sameVerRel: boolean
	@rtype: Version 
	"""
	assert(isinstance(branch, AbstractBranch))
	newlist = [ branch ]

	if sameVerRel:
	    newlist.append(self.versions[-1])

	return Version(self.versions + newlist, time.time())

    def parseVersionString(self, ver, defaultBranch = None):
	"""
	Converts a string representation of a version into a VersionRelease
	object.

	@param ver: version string
	@type ver: str
	@param defaultBranch: if provided and the ver parameter is not
	fully-qualified (it doesn't begin with a /), ver is taken to
	be relative to this branch.
	@type defaultBranch: Version
	"""
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

    """
    Creates a Version object from a list of AbstractBranch and AbstractVersion
    objects.
    """
    def __init__(self, versionList, timeStamp):
	self.versions = versionList
	self.timeStamp = timeStamp
	
class ThawVersion(Version):

    """
    Provides a version object from a frozen version string.
    """

    def __init__(self, fullString):
	"""
	Initializes a ThawVersion object. 

	@param fullString: Frozen representation of a Version object.
	@type fullString: str
	"""
	(timeStr, ver) = fullString.split(":")

	timeVal = float(timeStr)
	v = self.parseVersionString(ver)

	Version.__init__(self, v, timeVal)

class VersionFromString(Version):

    """
    Provides a version object from a string representation of a version.
    The time stamp is set to 0, so this object cannot be properly ordered
    with respect to other versions.
    """

    def __init__(self, ver, defaultBranch = None):
	"""
	Initializes a VersionFromString object. 

	@param ver: string representation of a version
	@type ver: str
	@param defaultBranch: if provided and the ver parameter is not
	fully-qualified (it doesn't begin with a /), ver is taken to
	be relative to this branch.
	@type defaultBranch: Version
	"""
	if ver[0] != "/":
	    ver = defaultBranch.asString() + "/" + ver

	v = self.parseVersionString(ver, defaultBranch)

	Version.__init__(self, v, 0)

class VersionsError(Exception):

    """
    Ancestor for all exceptions raised by the versions module.
    """

    pass

class ParseError(VersionsError):

    """
    Indicates that an error occured turning a string into an object
    in the versions module.
    """

    def __repr__(self):
	return self.str

    def __str__(self):
	return self.str

    def __init__(self, str):
	self.str = str
