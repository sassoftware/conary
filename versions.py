#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Classes for version structures. All of these types (except the abstract
ones) are hashable and implement __eq__().
"""

import copy
import time
import weakref

class AbstractVersion(object):

    """
    Ancestor class for all versions (as opposed to branches)
    """

    __slots__ = ( "__weakref__" )

    def __init__(self):
	pass

    def __eq__(self, them):
        raise NotImplementedError

    def __ne__(self, them):
	return not self.__eq__(them)

class NewVersion(AbstractVersion):

    """
    Class used as a marker for new (as yet undefined) versions.
    """

    __slots__ = ( )

    def asString(self, frozen = False):
	return "@NEW@"

    def freeze(self):
	return "@NEW@"

    def isLocal(self):
	return False

    def __hash__(self):
	return hash("@NEW@")

    def __eq__(self, other):
	return self.__class__ == other.__class__

    def timeStamps(self):
	return [ time.time() ]

    def branch(self):
	return None

class AbstractBranch(object):

    """
    Ancestor class for all branches (as opposed to versions)
    """

    __slots__ = ( "__weakref__" )

    def __init__(self):
	pass

    def __eq__(self, them):
        raise NotImplementedError

    def __ne__(self, them):
	return not self.__eq__(them)

class VersionRelease(AbstractVersion):

    """
    Version element for a version/release pair. These are formatted as
    "version-release", with no hyphen allowed in either portion. The
    release must be a simple integer or two integers separated by a
    decimal point.
    """

    __slots__ = ( "version", "release", "buildCount", "timeStamp" )

    def asString(self, versus = None, frozen = False):
	"""
	Returns a string representation of a version/release pair.
	"""
	if versus and self.version == versus.version:
	    rc = str(self.release)
	else:
	    rc = self.version + '-' + str(self.release)

	if self.buildCount != None:
	    rc += ".%d" % self.buildCount

	if frozen:
	    rc = self.freezeTimestamp() + ":" + rc

	return rc

    def freeze(self):
	return self.asString(frozen = True)

    def freezeTimestamp(self):
	"""
	Returns a binary representation of the files timestamp, which can
	be later used to restore the timestamp to the string'ified version
	of a version object.

	@rtype: str
	"""
	assert(self.timeStamp)
	return "%.3f" % self.timeStamp

    def thawTimestamp(self, str):
	"""
	Parses a frozen timestamp (from freezeTimestamp), and makes it
	the timestamp for this version.

	@param str: The frozen timestamp
	@type str: string
	"""
	self.timeStamp = float(str)

    def getVersion(self):
	"""
	Returns the version string of a version/release pair.
	"""

	return self.version

    def __eq__(self, version):
	if (type(self) == type(version) and self.version == version.version
		and self.release == version.release
		and self.buildCount == version.buildCount):
	    return 1
	return 0

    def __hash__(self):
	return hash(self.version) ^ hash(self.release) ^ hash(self.buildCount)

    def incrementRelease(self):
	"""
	Incremements the release number.
	"""
	self.release += 1
	self.timeStamp = time.time()

    def incrementBuildCount(self):
	"""
	Incremements the build count
	"""
	if self.buildCount:
	    self.buildCount += 1
	else:
	    self.buildCount = 1

	self.timeStamp = time.time()

    def __init__(self, value, template = None, frozen = False):
	"""
	Initialize a VersionRelease object from a string representation
	of a version release. ParseError exceptions are thrown if the
	string representation is ill-formed.

	@param value: String representation of a VersionRelease
	@type value: string
	"""
	self.timeStamp = 0

	if frozen:
	    (t, value) = value.split(':', 1)
	    self.thawTimestamp(t)

	if value.find(":") != -1:
	    raise ParseError, "version/release pairs may not contain colons"

	if value.find("@") != -1:
	    raise ParseError, "version/release pairs may not contain @ signs"
	cut = value.find("-")
	if cut == -1:
	    if not template:
		raise ParseError, ("version/release pair was expected")

	    self.version = template.version
	    fullRelease = value
	else:
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
    Stores a branch name, which is the same as a label. Branch names
    are of the form hostname@branch.
    """

    __slots__ = ( "host", "namespace", "branch" )

    def asString(self, versus = None, frozen = False):
	"""
	Returns the string representation of a branch name.
	"""
	if versus:
	    if self.host == versus.host:
		if self.namespace == versus.namespace:
		    return self.branch
		return self.namespace + ":" + self.branch

	return "%s@%s:%s" % (self.host, self.namespace, self.branch)

    def freeze(self):
	return self.asString()

    def getHost(self):
	return self.host

    def __eq__(self, version):
	if (isinstance(version, BranchName)
	     and self.host == version.host
	     and self.namespace == version.namespace
	     and self.branch == version.branch):
	    return 1
	return 0

    def __hash__(self):
	i = hash(self.host) ^ hash(self.namespace) ^ hash(self.branch)
	return i

    def __init__(self, value, template = None):
	"""
	Parses a branch name string into a BranchName object. A ParseError is
	thrown if the BranchName is not well formed.

	@param value: String representation of a BranchName
	@type value: str
	"""
	if value.find("/") != -1:
	    raise ParseError, "/ should not appear in a branch name"

	i = value.count(":")
	if i > 1:
	    raise ParseError, "unexpected colon"
	j = value.count("@")
	if j and not i:
	    raise ParseError, "@ sign can only be used with a colon"
	if j > 1:
	    raise ParseError, "unexpected @ sign"

	colon = value.find(":")
	at = value.find("@")

	if at > colon:
	    raise ParseError, "@ sign must occur before a colon"

	if value.find(":") == -1:
	    if not template:
		raise ParseError, "colon expected before branch name"
	    
	    self.host = template.host
	    self.namespace = template.namespace
	    self.branch = value
	else:
	    if value.find("@") == -1:
		if not template:
		    raise ParseError, "@ expected before branch namespace"
	    
		self.host = template.host
		(self.namespace, self.branch) = value.split(":")
	    else:
		(self.host, rest) = value.split("@", 1)
		(self.namespace, self.branch) = rest.split(":")

	if not self.namespace:
	    raise ParseError, ("namespace may not be empty: %s" % value)
	if not self.branch:
	    raise ParseError, ("branch names may not be empty: %s" % value)

class LocalBranch(BranchName):

    """
    Class defining the local branch.
    """

    def __init__(self):
	BranchName.__init__(self, "localhost@local:LOCAL")

class Version(AbstractVersion):

    """
    Class representing a version. Versions are a list of AbstractBranch,
    AbstractVersion sequences. If the last item is an AbstractBranch (meaning
    an odd number of objects are in the list, the version represents
    a branch. A version includes a time stamp, which is used for
    ordering.
    """

    __slots__ = ( "versions" )

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
	self.appendVersionReleaseObject(VersionRelease("%s-%d" % (version, release)))

    def appendVersionReleaseObject(self, verRel):
	"""
	Converts a branch to a version. The version/release passed in
	are appended to the branch this object represented. The time
	stamp is reset as a new version has been created.

	@param verRel: object for the version and release
	@type verRel: VersionRelease
	"""
	assert(self.isBranch())
	verRel.timeStamp = time.time()
	self.versions.append(verRel)

    def incrementRelease(self):
	"""
	The release number for the final element in the version is
	incremented by one and the time stamp is reset.
	"""
	assert(self.isVersion())
	
	self.versions[-1].incrementRelease()

    def incrementBuildCount(self):
	"""
	The build count number for the final element in the version is
	incremented by one and the time stamp is reset.
	"""
	assert(self.isVersion())
	
	self.versions[-1].incrementBuildCount()

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
	    if not list[i] == other.versions[i]: return 0
	
	return 1

    def __eq__(self, other):
	if not isinstance(other, Version): return False
	return self._listsEqual(self.versions, other)

    def __hash__(self):
	i = 0
	for ver in self.versions:
	    i ^= hash(ver)

	return i
	    
    def asString(self, defaultBranch = None, frozen = False):
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
	    start = Version(self.versions[0:len(defaultBranch.versions)])
	    if start == defaultBranch:
		list = self.versions[len(defaultBranch.versions):]
		s = ""

	oneAgo = None
	twoAgo = None
	for version in list:
	    s = s + ("%s/" % version.asString(twoAgo, frozen = frozen))
	    twoAgo = oneAgo
	    oneAgo = version

	return s[:-1]

    def freeze(self):
	"""
	Returns a complete string representation of the version, including
	the time stamp.

	@rtype: str
	"""
	return self.asString(frozen = True)

    def isBranch(self):
	"""
	Tests whether or not the current object is a branch.

	@rtype: boolean
	"""
	return isinstance(self.versions[-1], BranchName)

    def isTrunk(self):
	"""
	Tests whether or not the current object is a trunk branch.

	@rtype: boolean
	"""
	return len(self.versions) == 1

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
	return Version(self.versions[:-1])

    def label(self):
	"""
	Returns the BranchName object at the end of a branch. This is
	known as a label, as is used in VersionedFiles as an index.

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
	return Version(self.versions[:-2])

    def parentNode(self):
	"""
	Returns the parent version of a branch.

	@rtype: Version
	"""
	assert(self.isBranch())
	assert(len(self.versions) >= 3)
	return Version(self.versions[:-1])

    def hasParent(self):
	"""
	Tests whether or not the current branch or version has a parent.
	True for all versions other then those on trunks.

	@rtype: boolean
	"""
	return(len(self.versions) >= 3)

    def isAfter(self, other):
	"""
	Tests whether the parameter is a version later then this object.

	@param other: Object to test against
	@type other: Version
	@rtype: boolean
	"""
	assert(self.isVersion()            and other.isVersion)
	assert(self.versions[-1].timeStamp and other.versions[-1].timeStamp)
	return self.versions[-1].timeStamp  >  other.versions[-1].timeStamp

    def copy(self):
	"""
	Returns a Version object which is a copy of this object. The
	result can be modified without affecting this object in any way.j

	@rtype: Version
	"""

        return copy.deepcopy(self)

    def __deepcopy__(self, mem):
	return Version(copy.deepcopy(self.versions[:]))

    def canon(self):
	"""
	Returns the canonical version object for this object. For example,
	the canonical version of /label/ver1/label/ver1 is /label/ver1
	(as it's the top node on a branch).
	"""
	if self.isBranch() or len(self.versions) < 4: return self
	if self.versions[-1] == self.versions[-3]:
	    return self.parent()

	return self

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

	return Version(self.versions + newlist)

    def timeStamps(self):
	res = []
	for verRel in self.versions[1::2]:
	    res.append(verRel.timeStamp)

	return res

    def setTimeStamps(self, timeStamps):
	count = 1
	for stamp in timeStamps:
	    self.versions[count].timeStamp = stamp
	    count += 2

    def parseVersionString(self, ver, frozen):
	"""
	Converts a string representation of a version into a VersionRelease
	object.

	@param ver: version string
	@type ver: str
	"""
	parts = ver.split("/")
	del parts[0]	# absolute versions start with a /

	v = []
	lastVersion = None
	lastBranch = None
	while parts:
	    if parts[0] == "localhost@local:LOCAL":
		lastBranch = None
		v.append(LocalBranch())
	    else:
		lastBranch = BranchName(parts[0], template = lastBranch)
		v.append(lastBranch)

	    if len(parts) >= 2:
		lastVersion = VersionRelease(parts[1], template = lastVersion,
					     frozen = frozen)
		v.append(lastVersion)
		parts = parts[2:]
	    else:
		parts = None

	return v

    """
    Creates a Version object from a list of AbstractBranch and AbstractVersion
    objects.
    """
    def __init__(self, versionList):
	self.versions = versionList
	
def ThawVersion(ver):
    if ver == "@NEW@":
	return NewVersion()

    if thawedVersionCache.has_key(ver):
	return thawedVersionCache[ver]

    v = _VersionFromString(ver, frozen = True)
    thawedVersionCache[ver] = v
    return v

def VersionFromString(ver, defaultBranch = None, timeStamps = []):
    if ver == "@NEW@":
	return NewVersion()

    return _VersionFromString(ver, defaultBranch, timeStamps = timeStamps)

class _VersionFromString(Version):

    """
    Provides a version object from a string representation of a version.
    The time stamp is set to 0, so this object cannot be properly ordered
    with respect to other versions.
    """

    __slots__ = ()

    def __init__(self, ver, defaultBranch = None, frozen = False, 
		 timeStamps = []):
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

	v = self.parseVersionString(ver, frozen = frozen)

	Version.__init__(self, v)
	self.setTimeStamps(timeStamps)

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

    def __str__(self):
	return self.str

    def __init__(self, str):
	self.str = str

thawedVersionCache = weakref.WeakValueDictionary()
