#
# Copyright (c) 2004 Specifix, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
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
    Ancestor class for all versions (as opposed to labels)
    """

    __slots__ = ( "__weakref__" )

    def __eq__(self, them):
        raise NotImplementedError

    def __ne__(self, them):
	return not self.__eq__(them)

    def copy(self):
	return copy.deepcopy(self)

class AbstractLabel(object):

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
	    if versus and self.release == versus.release:
		if self.buildCount is None:
		    rc = str(self.release)
		else:
		    rc = ""
	    else:
		rc = str(self.release)
	else:
	    rc = self.version + '-' + str(self.release)

	if self.buildCount != None:
	    if rc:
		rc += "-%d" % self.buildCount
	    else:
		rc = str(self.buildCount)

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

    def getRelease(self):
	"""
	Returns the release number of a version/release pair.

        @rtype: int
	"""
	return self.release

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
	@type template: VersionRelease
	"""
	self.timeStamp = 0
	self.buildCount = None

	version = None
	release = None
	buildCount = None

	if frozen:
	    (t, value) = value.split(':', 1)
	    self.thawTimestamp(t)

	if value.find(":") != -1:
	    raise ParseError, "version/release pairs may not contain colons"

	if value.find("@") != -1:
	    raise ParseError, "version/release pairs may not contain @ signs"

	fields = value.split("-")
	if len(fields) > 3:
	    raise ParseError, ("too many fields in version/release set")

	if len(fields) == 1:
	    if template and template.buildCount is not None:
		self.version = template.version
		self.release = template.release
		buildCount = fields[0]
	    elif template:
		self.version = template.version
		release = fields[0]
	    else:
		raise ParseError, "bad version/release set %s" % value
	elif len(fields) == 2:
	    if template and template.buildCount is not None:
		self.version = template.version
		release = fields[0]
		buildCount = fields[1]
	    else:
		version = fields[0]
		release = fields[1]
	else:
	    (version, release, buildCount) = fields

	if version is not None:
	    try:
		int(version[0])
	    except:
		raise ParseError, \
		    ("version numbers must be begin with a digit: %s" % value)

	    self.version = version

	if release is not None:
	    try:
		self.release = int(release)
	    except:
		raise ParseError, \
		    ("release numbers must be all numeric: %s" % release)
	if buildCount is not None:
	    try:
		self.buildCount = int(buildCount)
	    except:
		raise ParseError, \
		    ("build count numbers must be all numeric: %s" % buildCount)

class Label(AbstractLabel):

    """
    Stores a label. Labels are of the form hostname@branch.
    """

    __slots__ = ( "host", "namespace", "branch" )

    def asString(self, versus = None, frozen = False):
	"""
	Returns the string representation of a label.
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

    def getNamespace(self):
	return self.namespace

    def getLabel(self):
	return self.branch

    def __eq__(self, version):
	if (isinstance(version, Label)
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
	Parses a label string into a Label object. A ParseError is
	thrown if the Label is not well formed.

	@param value: String representation of a Label
	@type value: str
	"""
	if value.find("/") != -1:
	    raise ParseError, "/ should not appear in a label"

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
		    raise ParseError, "@ expected before label namespace"
	    
		self.host = template.host
		(self.namespace, self.branch) = value.split(":")
	    else:
		(self.host, rest) = value.split("@", 1)
		(self.namespace, self.branch) = rest.split(":")

	if not self.namespace:
	    raise ParseError, ("namespace may not be empty: %s" % value)
	if not self.branch:
	    raise ParseError, ("branch tag not be empty: %s" % value)

class LocalBranch(Label):

    """
    Class defining the local branch.
    """

    def __init__(self):
	Label.__init__(self, "local@local:LOCAL")

class EmergeBranch(Label):

    """
    Class defining the emerge branch.
    """

    def __init__(self):
	Label.__init__(self, "local@local:EMERGE")

class CookBranch(Label):

    """
    Class defining the emerge branch.
    """

    def __init__(self):
	Label.__init__(self, "local@local:COOK")

class VersionSequence(object):

    __slots__ = ( "versions", "__weakref__" )

    """
    Abstract class representing a fully qualified version, branch, or
    shadow.
    """

    def compare(first, second):
        if first.isAfter(second):
            return 1
        elif first == second:
            return 0

        return -1

    compare = staticmethod(compare)

    def _listsEqual(self, list, other):
	if len(other.versions) != len(list): return 0

	for i in range(0, len(list)):
	    if not list[i] == other.versions[i]: return 0
	
	return 1

    def __eq__(self, other):
        if self.__class__ != other.__class__: return False
	return self._listsEqual(self.versions, other)

    def __ne__(self, other):
        return not self == other

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
	l = self.versions
	s = "/"

        assert(defaultBranch is None or isinstance(defaultBranch, Branch))

	if defaultBranch and len(defaultBranch.versions) < len(self.versions):
	    start = Branch(self.versions[0:len(defaultBranch.versions)])
	    if start == defaultBranch:
		l = self.versions[len(defaultBranch.versions):]
		s = ""

	oneAgo = None
	twoAgo = None
	for version in l:
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

    def copy(self):
	"""
        Returns an object which is a copy of this object. The result can be
        modified without affecting this object in any way.

	@rtype: VersionSequence
	"""

        return copy.deepcopy(self)

    def hasParent(self):
	"""
	Tests whether or not the current branch or version has a parent.
	True for all versions other then those on trunks.

	@rtype: boolean
	"""
	return(len(self.versions) >= 3)

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

    def __init__(self, versionList):
        """
        Creates a Version object from a list of AbstractLabel and
        AbstractVersion objects.
        """
	self.versions = versionList

class NewVersion(VersionSequence):

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

    def __init__(self):
        pass

class Version(VersionSequence):

    __slots__ = ()

    def incrementRelease(self):
	"""
	The release number for the final element in the version is
	incremented by one and the time stamp is reset.
	"""
	self.versions[-1].incrementRelease()

    def incrementBuildCount(self):
	"""
	The build count number for the final element in the version is
	incremented by one and the time stamp is reset.
	"""
	self.versions[-1].incrementBuildCount()

    def trailingVersion(self):
	"""
	Returns the AbstractVersion object at the end of the version.

	@rtype: AbstactVersion
	"""
	return self.versions[-1]

    def isLocal(self):
    	"""
	Tests whether this is the local branch, or is a version on
	the local branch

	@rtype: boolean
	"""
	return isinstance(self.versions[-2], LocalBranch)

    def branch(self):
	"""
	Returns the branch this version is part of.

	@rtype: Version
	"""
	return Branch(self.versions[:-1])

    def parent(self):
	"""
	Returns the parent version for this version (the version this
	object's branch branched from.

	@rtype: Version
	"""
	assert(len(self.versions) > 3)
	return Version(self.versions[:-2])

    def isAfter(self, other):
	"""
	Tests whether the parameter is a version later then this object.

	@param other: Object to test against
	@type other: Version
	@rtype: boolean
	"""
        assert(self.__class__ == other.__class__)
	assert(self.versions[-1].timeStamp and other.versions[-1].timeStamp)
	return self.versions[-1].timeStamp  >  other.versions[-1].timeStamp

    def __deepcopy__(self, mem):
	return Version(copy.deepcopy(self.versions[:]))

    def createBranch(self, branch, withVerRel = False):
	"""
	Creates a new branch from this version. 

	@param branch: Branch to create for this version
	@type branch: AbstractLabel
	@param withVerRel: If set, the new branch is turned into a version
	on the branch using the same version and release as the original
	verison.
	@type withVerRel: boolean
	@rtype: Version 
	"""
	assert(isinstance(branch, AbstractLabel))

	newlist = [ branch ]

	if withVerRel:
	    newlist.append(self.versions[-1].copy())
            return Version(self.versions + newlist)

        return Branch(self.versions + newlist)

    def getSourceBranch(self):
        """ Takes a binary branch and returns its associated source branch.
            (any trailing version info is left untouched).
            If source is branched off of <repo1>-2 into <repo2>, its new
            version will be <repo1>-2/<repo2>/2.  The corresponding build
            will be on branch <repo1>-2-0/<repo2>/2-1.
            getSourceBranch converts from the latter to the former.
            Always returns a copy of the branch, even when the two are
            equal.
        """
        v = self.copy()
        p = v.branch()

        if p.hasParent():
            p = p.parentNode()
            p.trailingVersion().buildCount = None
            while p.hasParent():
                p = p.parent()
                p.trailingVersion().buildCount = None

        return v

    def getBinaryBranch(self):
        """ 
        Takes a source branch and returns its associated binary branch.  (any
        trailing version info is left untouched).  If source is branched off of
        <repo1>-2 into <repo2>, its new version will be <repo1>-2/<repo2>/2.
        The corresponding build will be on branch <repo1>-2-0/<repo2>/2-1.
        getBinaryBranch converts from the former to the latter.  Always returns
        a copy of the branch, even when the two are equal.
        """
        v = self.copy()
        p = v.branch()

        if p.hasParent():
            p = p.parentNode()
            p.trailingVersion().buildCount = 0
            while p.hasParent():
                p = p.parent()
                p.trailingVersion().buildCount = 0
        return v

class Branch(VersionSequence):

    __slots__ = ()

    def __deepcopy__(self, mem):
	return Branch(copy.deepcopy(self.versions[:]))

    def label(self):
	"""
	Returns the Label object at the end of a branch. This is
	known as a label, as is used in VersionedFiles as an index.

	@rtype: Label
	"""
	return self.versions[-1]

    def parentNode(self):
	"""
	Returns the parent version of a branch.

	@rtype: Version
	"""
	assert(len(self.versions) >= 3)
	return Version(self.versions[:-1])

    def createVersion(self, verRel):
	"""
	Converts a branch to a version. The version/release passed in
	are appended to the branch this object represented. The time
	stamp is reset as a new version has been created.

	@param verRel: object for the version and release
	@type verRel: VersionRelease
	"""

	verRel.timeStamp = time.time()
        return Version(self.versions + [ verRel ])

def _parseVersionString(ver, frozen):
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
        lastBranch = Label(parts[0], template = lastBranch)
        if lastBranch.asString() == "local@local:LOCAL":
            lastBranch = None
            v.append(LocalBranch())
        elif lastBranch.asString() == "local@local:COOK":
            lastBranch = None
            v.append(CookBranch())
        elif lastBranch.asString() == "local@local:EMERGE":
            lastBranch = None
            v.append(EmergeBranch())
        else:
            v.append(lastBranch)

        if len(parts) >= 2:
            lastVersion = VersionRelease(parts[1], template = lastVersion,
                                         frozen = frozen)
            v.append(lastVersion)
            parts = parts[2:]
        else:
            parts = None

    return v
	
def ThawVersion(ver):
    if ver == "@NEW@":
	return NewVersion()

    v = thawedVersionCache.get(ver, None)
    if v is not None:
	return v

    v = _VersionFromString(ver, frozen = True)
    thawedVersionCache[ver] = v
    return v

def VersionFromString(ver, defaultBranch = None, timeStamps = []):
    if ver == "@NEW@":
	return NewVersion()

    v = stringVersionCache.get(ver, None)
    if v is not None and (not timeStamps or v.timeStamps() == timeStamps):
	return v

    v = _VersionFromString(ver, defaultBranch, timeStamps = timeStamps)
    stringVersionCache[ver] = v
    return v

def _VersionFromString(ver, defaultBranch = None, frozen = False, 
		       timeStamps = []):

    """
    Provides a version object from a string representation of a version.
    The time stamp is set to 0, so this object cannot be properly ordered
    with respect to other versions.

    @param ver: string representation of a version
    @type ver: str
    @param defaultBranch: if provided and the ver parameter is not
    fully-qualified (it doesn't begin with a /), ver is taken to
    be relative to this branch.
    @type defaultBranch: Version
    """
    if ver[0] != "/":
        ver = defaultBranch.asString() + "/" + ver

    vList = _parseVersionString(ver, frozen = frozen)

    if len(vList) % 2 == 0:
        ver = Version(vList)
    else:
        ver = Branch(vList)

    ver.setTimeStamps(timeStamps)

    return ver

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
stringVersionCache = weakref.WeakValueDictionary()
