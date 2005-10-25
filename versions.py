#
# Copyright (c) 2004-2005 rPath, Inc.
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

staticLabelTable = {}

class AbstractRevision(object):

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

class SerialNumber(object):

    """
    Provides source and binary serial numbers.
    """

    __slots__ = ( "numList" )

    def __cmp__(self, other):
        if self.__class__ != other.__class__:
            return False

        i = 0
        for i in range(min(len(self.numList), len(other.numList))):
            cmpVal = cmp(self.numList[i], other.numList[i])
            if cmpVal != 0:
                return cmpVal

        return cmp(len(self.numList), len(other.numList))

    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return False

        return self.numList == other.numList

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return ".".join((str(x) for x in self.numList))

    def __repr__(self):
        return "versions.SerialNumber('%s')" % str(self)

    def __hash__(self):
        hashVal = 0
        for item in self.numList:
            hashVal ^= hash(item) << 7

        return hashVal

    def __getstate__(self):
        return self.numList

    def __setstate__(self, val):
        self.numList = val

    def shadowCount(self):
        return len(self.numList) - 1

    def truncateShadowCount(self, count, fromEnd = False):
        count += 1

        if len(self.numList) <= count:
            return

        if fromEnd:
            self.numList = self.numList[-count:]
        else:
            self.numList = self.numList[:count]

    def increment(self, listLen):
        self.numList += [ 0 ] * ((listLen + 1) - len(self.numList))
        self.numList[-1] += 1

    def iterCounts(self):
        return iter(self.numList)

    def __deepcopy__(self, mem):
	return SerialNumber(str(self))

    def __init__(self, value):
        self.numList = [ int(x) for x in value.split(".") ]

class Revision(AbstractRevision):
    """
    Version element for a version, sourceCount, buildCount
    triplet. These are formatted as "version-sourceCount-buildCount",
    with no hyphens allowed in any portion. The sourceCount and
    buildCounts must be simple integers or two integers separated by a
    decimal point.
    """

    __slots__ = ( "version", "sourceCount", "buildCount", "timeStamp" )

    def __getstate__(self):
        return (self.version, self.sourceCount, self.buildCount,
                self.timeStamp)

    def __setstate__(self, val):
        (self.version, self.sourceCount, self.buildCount, self.timeStamp) = val

    def asString(self, versus = None, frozen = False):
	"""
	Returns a string representation of a Release.
	"""
	if versus and self.version == versus.version:
	    if versus and self.sourceCount == versus.sourceCount:
		if self.buildCount is None:
		    rc = str(self.sourceCount)
		else:
		    rc = ""
	    else:
		rc = str(self.sourceCount)
	else:
	    rc = self.version + '-' + str(self.sourceCount)

	if self.buildCount != None:
	    if rc:
		rc += "-%s" % self.buildCount
	    else:
		rc = str(self.buildCount)

	if frozen:
	    rc = self.freezeTimestamp() + ":" + rc

	return rc

    def __repr__(self):
        return "versions.Revision('%s')" % self.asString()

    def __str__(self):
        return self.asString()

    def freeze(self):
	return self.asString(frozen = True)

    def getTimestamp(self):
        """
        Returns the timestamp for this revision.

        @rtype: float
        """
	assert(self.timeStamp)
        return self.timeStamp

    def freezeTimestamp(self):
	"""
	Returns a binary representation of the revision's timestamp, which can
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
	Returns the version string of a Revision.

        @rtype: str
	"""

	return self.version

    def getSourceCount(self):
	"""
	Returns the source SerialNumber object of a Revision.

        @rtype: SerialNumber
	"""
	return self.sourceCount

    def getBuildCount(self):
	"""
	Returns the build SerialNumber object of a Revision.

        @rtype: SerialNumber
	"""
	return self.buildCount

    def freshlyBranched(self):
        """
        Resets the build and source counts to reflect this Revision
        as being freshly branched.
        """
        self.sourceCount.truncateShadowCount(0, fromEnd = True)
        if self.buildCount:
            self.buildCount.truncateShadowCount(0, fromEnd = True)

    def shadowCount(self):
        i = self.sourceCount.shadowCount()
        if i:
            return i

        if self.buildCount:
            return self.buildCount.shadowCount()

        return 0

    def shadowChangedUpstreamVersion(self):
        """ returns True if this revision is a) on a shadow 
            and b) the parent branch's source count is 0, 
            implying that the upstream version # has been changed
        """
        if (self.sourceCount.shadowCount() and
            [ x for x in self.sourceCount.iterCounts()][-2] == 0):
            # 0 means there's no corresponding parent
            # source with this version number
            return True
        return False

    def __eq__(self, version):
	if (type(self) == type(version) and self.version == version.version
		and self.sourceCount == version.sourceCount
		and self.buildCount == version.buildCount):
	    return 1
	return 0

    def __hash__(self):
	return (hash(self.version) ^ hash(self.sourceCount)
                ^ hash(self.buildCount))

    def _incrementSourceCount(self, shadowLength):
	"""
	Incremements the release number.
	"""
	self.sourceCount.increment(shadowLength)
	self.timeStamp = time.time()

    def _setBuildCount(self, buildCount):
	"""
	Sets the build count
	"""
	self.buildCount = buildCount

    def resetTimeStamp(self):
	self.timeStamp = time.time()

    def __init__(self, value, template = None, frozen = False):
	"""
	Initialize a Revision object from a string representation
	of a version release. ParseError exceptions are thrown if the
	string representation is ill-formed.

	@param value: String representation of a Revision
	@type value: string
        @param template: a Revision instance to use as the basis when
        parsing an abbreviated revision string.
	@type template: Revision
        @param frozen: indicates if timestamps should be parsed from
        the version string
        @type frozen: bool
	"""
	self.timeStamp = 0
        self.sourceCount = None
	self.buildCount = None

	version = None
	sourceCount = None
	buildCount = None

	if frozen:
	    (t, value) = value.split(':', 1)
	    self.thawTimestamp(t)

	if value.find(":") != -1:
	    raise ParseError("release strings may not contain colons")

	if value.find(",") != -1:
	    raise ParseError("release strings may not contain commas")

	if value.find(" ") != -1:
	    raise ParseError("release strings may not contain spaces")

	if value.find("@") != -1:
	    raise ParseError("release strings may not contain @ signs")

	fields = value.split("-")
	if len(fields) > 3:
	    raise ParseError("too many '-' characters in release string")

        # if the string we're parsing didn't include all of
        # version-sourceCount-buildCount AND we have a template to
        # work off of, we can use the template to build up a full
        # version from the abbreviated information given
        if len(fields) < 3 and template:
            # assume we're going to use the version and sourceCount
            # from the template (though we may change our mind)
            version = template.version
            self.sourceCount = template.sourceCount

            # if our template has a buildcount, it is a full
            # version-sourceCount-buildCount set.  The abbreviated
            # value we are parsing can either be sourceCount-buildCount
            # or just buildCount
            if template.buildCount is not None:
                buildCount = fields[-1]
                if len(fields) == 2:
                    # sourceCount-buildCount was provided
                    sourceCount = fields[0]
            else:
                # otherwise, the template only has version-sourceCount
                # so the value string we're parsing can provide
                # version-sourceCount just sourceCount
                sourceCount = fields[-1]
                if len(fields) == 2:
                    # version-sourceCount was provided
                    version = fields[0]
        else:
            if len(fields) == 1:
                version = fields[0]
            elif len(fields) == 2:
                version, sourceCount = fields
            elif len(fields) == 3:
                version, sourceCount, buildCount = fields

        if not version:
            raise ParseError("bad release string: %s" % value)

        self.version = version

	if sourceCount is not None:
	    try:
		self.sourceCount = SerialNumber(sourceCount)
	    except:
		raise ParseError("source count numbers must be all "
                                 "numeric: %s" % sourceCount)
	if buildCount is not None:
	    try:
		self.buildCount = SerialNumber(buildCount)
	    except:
		raise ParseError("build count numbers must be all"
                                 "numeric: %s" % buildCount)

        if self.sourceCount is None:
            raise ParseError("bad release string: %s" % value)

class Label(AbstractLabel):

    """
    Stores a label. Labels are of the form hostname@branch.
    """

    __slots__ = ( "host", "namespace", "branch" )

    def __getstate__(self):
        return (self.host, self.namespace, self.branch)

    def __setstate__(self, val):
        (self.host, self.namespace, self.branch) = val

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

    def __repr__(self):
        return "Label('%s')" % self.asString()

    def __str__(self):
        return self.asString()

    def __init__(self, value, template = None):
	"""
	Parses a label string into a Label object. A ParseError is
	thrown if the Label is not well formed.

	@param value: String representation of a Label
	@type value: str
	"""
	if value.find("/") != -1:
	    raise ParseError("/ should not appear in a label")

	i = value.count(":")
	if i > 1:
	    raise ParseError("unexpected colon")
	j = value.count("@")
	if j and not i:
	    raise ParseError("@ sign can only be used with a colon")
	if j > 1:
	    raise ParseError("unexpected @ sign")

	colon = value.find(":")
	at = value.find("@")

	if at > colon:
	    raise ParseError("@ sign must occur before a colon")

	if colon == -1:
	    if not template:
		raise ParseError("colon expected before branch name")

	    self.host = template.host
	    self.namespace = template.namespace
	    self.branch = value
	else:
	    if value.find("@") == -1:
		if not template:
		    raise ParseError("@ expected before label namespace")

		self.host = template.host
		(self.namespace, self.branch) = value.split(":")
	    else:
		(self.host, rest) = value.split("@", 1)
		(self.namespace, self.branch) = rest.split(":")

	if not self.namespace:
	    raise ParseError("namespace may not be empty: %s" % value)
	if not self.branch:
	    raise ParseError("branch tag not be empty: %s" % value)

class StaticLabel(Label):

    def __init__(self):
	Label.__init__(self, self.name)

class LocalLabel(StaticLabel):

    """
    Class defining the local branch.
    """

    name = "local@local:LOCAL"

class RollbackLabel(StaticLabel):

    """
    Class defining the local branch.
    """

    name = "local@local:ROLLBACK"

class EmergeLabel(StaticLabel):

    """
    Class defining the emerge branch.
    """

    name = "local@local:EMERGE"

class CookLabel(StaticLabel):

    """
    Class defining the emerge branch.
    """

    name = "local@local:COOK"

staticLabelTable[LocalLabel.name] = LocalLabel
staticLabelTable[EmergeLabel.name] = EmergeLabel
staticLabelTable[CookLabel.name] = CookLabel

class AbstractVersion(object):

    __slots__ = "__weakref__"

class VersionSequence(AbstractVersion):

    __slots__ = ( "versions", "hash", "strRep" )

    """
    Abstract class representing a fully qualified version, branch, or
    shadow.
    """

    def __getstate__(self):
        return self.versions

    def __setstate__(self, val):
        self.versions = val
        self.hash = None
        self.strRep = None

    def __cmp__(self, other):
        assert(self.__class__ == other.__class__)
	assert(self.versions[-1].timeStamp and other.versions[-1].timeStamp)
	return cmp(self.versions[-1].timeStamp, other.versions[-1].timeStamp)

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
        if self.hash is None:
            self.hash = 0
            for ver in self.versions:
                self.hash ^= hash(ver)

	return self.hash

    def closeness(self, other):
        """
        Measures the "closeness" (the inverse of the distance) between two
        versions of branches. If the two are exactly the same, 
        ZeroDivision results.
        """

        def _buildSet(seq):
            s = set()
            last = None
            for item in seq:
                if isinstance(item, AbstractLabel):
                    s.add(item)
                    if last is not None:
                        s.add((last, item))
                    last = item

            return s

        # Assemble sets based on the labels of each VersionSequence. The
        # sets consist of each label, and the transition from label to
        # label (which labels occur next to each other, modulo version
        # numbers). 
        ourSet = _buildSet(self.versions)
        otherSet = _buildSet(other.versions)

        common = ourSet & otherSet
        return (len(common) / (len(ourSet) + len(otherSet) - 
                                (len(common) * 2.0)))

    def asString(self, defaultBranch = None, frozen = False):
	"""
	Returns a string representation of the version.

	@param defaultBranch: If set this is stripped fom the beginning
	of the version to give a shorter string representation.
	@type defaultBranch: Version
	@rtype: str
	"""
        if self.strRep is not None and not defaultBranch and not frozen:
	    return self.strRep

	l = self.versions
        # this creates a leading /
        strL = [ '' ]

        assert(defaultBranch is None or isinstance(defaultBranch, Branch))

	if defaultBranch and len(defaultBranch.versions) < len(self.versions):
	    start = Branch(self.versions[0:len(defaultBranch.versions)])
	    if start == defaultBranch:
		l = self.versions[len(defaultBranch.versions):]
		strL = []

        lastLabel = None
        lastVersion = None
        expectLabel = isinstance(l[0], Label)

        for verPart in l:
            if expectLabel:
                strL.append(verPart.asString(lastLabel, frozen = frozen))
                lastLabel = verPart
                expectLabel = False
            elif isinstance(verPart, Label):
                # shadow
                strL.append('')
                strL.append(verPart.asString(lastLabel, frozen = frozen))
                lastLabel = verPart
            else:
                strL.append(verPart.asString(lastVersion, frozen = frozen))
                lastVersion = verPart
                expectLabel = True

	if not defaultBranch and not frozen:
	    self.strRep = "/".join(strL)
	    return self.strRep

	return "/".join(strL)

    def __repr__(self):
        return "VFS('%s')" % self.asString()

    def __str__(self):
        return self.asString()

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

    def timeStamps(self):
        return [ x.timeStamp for x in self.versions
                 if isinstance(x, AbstractRevision)]

    def setTimeStamps(self, timeStamps, clearCache=True):
        if clearCache:
            if max(self.timeStamps()) != 0:
                # we're changing the timeStamps, invalidate the cache
                frzStr = self.freeze()
                if self is thawedVersionCache.get(frzStr, None):
                    del thawedVersionCache[frzStr]

            # if it has timeStamps, its not allowed in the from-string cache
            stringVersionCache.pop(self.asString(), None)

        i = 0
        for item in self.versions:
            if isinstance(item, AbstractRevision):
                item.timeStamp = timeStamps[i]
                i += 1


    def iterLabels(self):
        """
        Iterates through the labels that are used in this version
        in order, from earliest to last.
        """
        for item in self.versions:
            if isinstance(item, Label):
                yield item

    def __init__(self, versionList):
        """
        Creates a Version object from a list of AbstractLabel and
        AbstractRevision objects.
        """
	self.versions = versionList
        self.hash = None
        self.strRep = None

class NewVersion(AbstractVersion):

    """
    Class used as a marker for new (as yet undefined) versions.
    """

    __slots__ = ( )

    def copy(self):
        return self.__class__()

    def asString(self, frozen = False):
	return "@NEW@"

    def freeze(self):
	return "@NEW@"

    def isOnLocalHost(self):
	return False

    def onLocalLabel(self):
	return False

    def onEmergeLabel(self):
	return False

    def onLocalCookLabel(self):
	return False

    def onRollbackLabel(self):
	return False

    def __hash__(self):
	return hash("@NEW@")

    def __eq__(self, other):
	return self.__class__ == other.__class__

    def __ne__(self, other):
	return self.__class__ != other.__class__

    def timeStamps(self):
	return [ time.time() ]

    def branch(self):
	return None

    def __repr__(self):
        return 'versions.NewVersion()'

    def __init__(self):
        pass

class Version(VersionSequence):

    __slots__ = ()

    def shadowLength(self):
        """
        Returns the shadow-depth since the last branch.

        @rtype: int
        """
        count = 0
        expectVersion = False

        iter = reversed(self.versions)
        iter.next()

        for item in iter:
            if expectVersion and isinstance(item, AbstractRevision):
                return count
            elif expectVersion:
                count += 1
            else:
                expectVersion = True

        return count

    def canonicalVersion(self):
        # returns the canonical version for this version. if this is a
        # shadow of a version, we return that original version
        v = self.copy()

        release = v.trailingRevision()
        shadowCount = release.sourceCount.shadowCount()
        if release.buildCount and \
                release.buildCount.shadowCount() > shadowCount:
            shadowCount = release.buildCount.shadowCount()

        stripCount = v.shadowLength() - shadowCount
        for i in range(stripCount):
            v = v.parentVersion()

        return v

    def hasParentVersion(self):
        # things which have parent versions are:
        #   1. sources which were branched or shadows
        #   2. binaries which were branched or shadowed
        #
        # built binaries don't have parent versions

        if len(self.versions) < 3:
            # too short
            return False

        trailing = self.versions[-1]
        if trailing.buildCount is None:
            if trailing.shadowChangedUpstreamVersion():
                return False
            else:
                return True

        # find the previous Revision object. If the shadow counts are
        # the same, this is a direct child
        iter = reversed(self.versions)
        # this skips the first one
        item = iter.next()
        item = iter.next()
        try:
            while not isinstance(item, AbstractRevision):
                item = iter.next()
        except StopIteration:
            if (trailing.sourceCount.shadowCount() < self.shadowLength()
                and not trailing.buildCount.shadowCount()):
                # this is a direct shadow of a binary trove -- it hasn't
                # been touched on the shadow
                return True
            # the source or binary has been touched on this shadow
            return False


        if item.buildCount and \
            item.buildCount.shadowCount() == \
                trailing.buildCount.shadowCount():
            return True

        return False

    def parentVersion(self):
	"""
	Returns the parent version of this version. Undoes shadowing and
        such to find it.

	@rtype: Version
	"""
        assert(self.hasParentVersion())

        # if this is a branch, finding the parent is easy
        if isinstance(self.versions[-3], AbstractRevision):
            return Version(self.versions[:-2])

        # this is a shadow. work a bit harder
        items = self.versions[:-2] + [ self.versions[-1].copy() ]

        shadowCount = self.shadowLength() - 1
        items[-1].sourceCount.truncateShadowCount(shadowCount)
        if items[-1].buildCount:
            items[-1].buildCount.truncateShadowCount(shadowCount)

	return Version(items)

    def incrementSourceCount(self):
	"""
	The release number for the final element in the version is
	incremented by one and the time stamp is reset.
	"""
        self.hash = None
        self.strRep = None
	self.versions[-1]._incrementSourceCount(self.shadowLength())

    def incrementBuildCount(self):
	"""
	Incremements the build count
	"""
        # if the source count is the right length for this shadow
        # depth, just increment the build count (without lengthing
        # it). if the source count is too short, make the build count
        # the right length for this shadow
        shadowLength = self.shadowLength()
        self.hash = None
        self.strRep = None

        sourceCount = self.versions[-1].getSourceCount()
        buildCount = self.versions[-1].getBuildCount()

        if sourceCount.shadowCount() == shadowLength:
            if buildCount:
                buildCount.increment(buildCount.shadowCount())
            else:
                buildCount = SerialNumber('1')
                self.versions[-1]._setBuildCount(buildCount)
        else:
            if buildCount:
                buildCount.increment(shadowLength)
            else:
                buildCount = SerialNumber(
                            ".".join([ '0' ] * shadowLength + [ '1' ] ))
                self.versions[-1]._setBuildCount(buildCount)

        self.versions[-1].resetTimeStamp()

    def trailingRevision(self):
	"""
	Returns the AbstractRevision object at the end of the version.

	@rtype: AbstactVersion
	"""
	return self.versions[-1]

    def isSourceVersion(self):
    	"""
	Tests whether this version is a source or binary version.

	@rtype: boolean
	"""
	return self.canonicalVersion().versions[-1].buildCount is None


    def isShadow(self):
        """ Returns True if this version is a shadow of another trove """
        return self.branch().isShadow()

    def isModifiedShadow(self):
        """ Returns True if this version is a shadow that has been modified
        """
        if self.isShadow():
            tr = self.trailingRevision()

            if tr.sourceCount.shadowCount():
                return True
            if tr.buildCount and tr.buildCount.shadowCount():
                return True

        return False

    def onLocalLabel(self):
    	"""
	Tests whether this is the local branch, or is a version on
	the local branch

	@rtype: boolean
	"""
	return isinstance(self.versions[-2], LocalLabel)

    def onRollbackLabel(self):
    	"""
	Tests whether this is the rollback branch, or is a version on
	the rollback branch

	@rtype: boolean
	"""
	return isinstance(self.versions[-2], RollbackLabel)

    def onEmergeLabel(self):
    	"""
	Tests whether this is the emerge branch, or is a version on
	the emerge branch

	@rtype: boolean
	"""
	return isinstance(self.versions[-2], EmergeLabel)

    def onLocalCookLabel(self):
    	"""
	Tests whether this is the local cook branch, or is a version on
	the local cook branch

	@rtype: boolean
	"""
	return isinstance(self.versions[-2], CookLabel)

    def isOnLocalHost(self):
    	"""
        Returns True if the label for this version has "local" as the
        server (signifying that this is a local version, not from a
        networked repository)

	@rtype: boolean
	"""
	return (self.onLocalCookLabel() or self.onEmergeLabel()
                or self.onLocalLabel() or self.onRollbackLabel())

    def branch(self):
	"""
	Returns the branch this version is part of.

	@rtype: Version
	"""
	return Branch(self.versions[:-1])

    def isAfter(self, other):
	"""
	Tests whether the parameter is a version later then this object.

	@param other: Object to test against
	@type other: Version
	@rtype: boolean
	"""
	return self > other

    def __deepcopy__(self, mem):
	return Version(copy.deepcopy(self.versions[:]))

    def createBranch(self, label, withVerRel = False):
	"""
	Creates a new label from this version.

	@param label: Branch to create for this version
	@type label: AbstractLabel
	@param withVerRel: If set, the new label is turned into a version
	on the label using the same version and release as the original
	verison.
	@type withVerRel: boolean
	@rtype: Version
	"""
	assert(isinstance(label, AbstractLabel))
        assert(self.versions[-2] != label)

	newlist = [ label ]

	if withVerRel:
	    newlist.append(self.versions[-1].copy())
            newlist[-1].freshlyBranched()
            return Version(copy.deepcopy(self.versions + newlist))

        return Branch(copy.deepcopy(self.versions + newlist))

    def createShadow(self, label):
	"""
	Creates a new shadow from this version.

	@param label: Branch to create for this version
	@type label: AbstractLabel
	@rtype: Version
	"""
	assert(isinstance(label, AbstractLabel))
        assert(self.versions[-2] != label)

        newRelease = self.versions[-1].copy()
	newRelease.timeStamp = time.time()

        newList = self.versions[:-1] + [ label ] + [ newRelease ]
        return Version(copy.deepcopy(newList))

    def isBranchedBinary(self):
        """
	Returns true if this version is a binary version that was branched/
        shadowed directly, instead of branching/shadowing a source and then
        cooking it

	@rtype: bool
	"""
        # ensure this version is branched and is actually a binary
        if not (self.hasParentVersion()
                and self.trailingRevision().buildCount):
            return False
        # check that its parent version is also a binary
        buildCount = self.parentVersion().trailingRevision().buildCount
        return buildCount is None or str(buildCount) != '0'

    def getSourceVersion(self):
        """
        Takes a binary version and returns its associated source
        version (any trailing version info is left untouched).  If
        source is branched off of <repo1>-2 into <repo2>, its new
        version will be <repo1>-2/<repo2>/2.  The corresponding build
        will be on branch <repo1>-2-0/<repo2>/2-1.  getSourceVersion
        converts from the latter to the former.  Always returns a copy
        of the version, even when the two are equal.
        """
        v = self.copy()
        # if a binary was branched/shadowed onto this label
        while v.isBranchedBinary():
            v = v.parentVersion()
        v = v.canonicalVersion()
        for item in v.versions:
            if isinstance(item, Revision):
                item.buildCount = None
        return v

    def getBinaryVersion(self):
        """
        Takes a source branch and returns its associated binary
        branch.  (any trailing version info is left untouched).  If
        source is branched off of <repo1>-2 into <repo2>, its new
        version will be <repo1>-2/<repo2>/2.  The corresponding build
        will be on branch <repo1>-2-0/<repo2>/2-1.  getBinaryVersion
        converts from the former to the latter.  Always returns a copy
        of the branch, even when the two are equal.
        """
        newV = self.copy()
        v = newV
        trailingRevisions = []
        while v.hasParentVersion():
            v = v.parentVersion()
            trailingRevisions.append(v.trailingRevision())
        for trailingRevision in trailingRevisions:
            assert(trailingRevision.buildCount is None)
            trailingRevision.buildCount = SerialNumber('0')
        return newV

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

    def parentBranch(self):
	"""
	Returns the parent branch of a branch.

	@rtype: Version
	"""
        items = self.versions[:-1]
        if isinstance(items[-1], Revision):
            del items[-1]

        assert(items)

	return Branch(items)

    def hasParentBranch(self):
        return len(self.versions) >= 2

    def isShadow(self):
        """ Returns True if this branch is a shadow of another branch """
        return self.hasParentBranch() and isinstance(self.versions[-2], Label)

    def createVersion(self, revision):
	"""
	Converts a branch to a version. The revision passed in
	are appended to the branch this object represented. The time
	stamp is reset as a new version has been created.

	@param verRel: object for the revision
	@type verRel: Revision
	"""

	revision.timeStamp = time.time()
        return Version(self.versions + [ revision ])

    def createShadow(self, label):
	"""
	Creates a new shadow from this branch.

	@param label: Label of the new shadow
	@type label: AbstractLabel
	@rtype: Version
	"""
	assert(isinstance(label, AbstractLabel))

	newlist = [ label ]
        return Branch(self.versions + newlist)

def _parseVersionString(ver, frozen):
    """
    Converts a string representation of a version into a Revision
    object.

    @param ver: version string
    @type ver: str
    """

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

    if timeStamps:
        # timeStamped VFSs are not allowed in the cache
        return _VersionFromString(ver, defaultBranch, timeStamps = timeStamps)

    v = stringVersionCache.get(ver, None)
    if v is None:
        v = _VersionFromString(ver, defaultBranch)
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

    parts = ver.split("/")
    del parts[0]	# absolute versions start with a /

    vList = []
    lastVersion = None
    lastBranch = None
    expectLabel = True
    justShadowed = False

    for part in parts:
        if expectLabel:
            lastBranch = Label(part, template = lastBranch)

            staticLabelClass = staticLabelTable.get(lastBranch.asString(),
                                                    None)
            if staticLabelClass is not None:
                vList.append(staticLabelClass())
            else:
                vList.append(lastBranch)
            expectLabel = False

            if justShadowed:
                justShadowed = False
            else:
                shadowCount = 0
        elif not part:
            # blank before a shadow
            expectLabel = True
            shadowCount += 1
            justShadowed = True
        else:
            expectLabel = True

            lastVersion = Revision(part, template = lastVersion,
                                         frozen = frozen)
            if lastVersion.shadowCount() > shadowCount:
                raise ParseError("too many shadow serial numbers "
                                 "in '%s'" % part)
            vList.append(lastVersion)

    if isinstance(vList[-1], AbstractRevision):
        ver = Version(vList)
    else:
        ver = Branch(vList)

    if timeStamps:
        ver.setTimeStamps(timeStamps, clearCache=False)

    return ver

def strToFrozen(verStr, timeStamps):
    """
    Converts a version string to a frozen version by applying the
    passed array of timestamps (which is an array of *strings*,
    not numbers). Basically no error checking is done.

    @param verStr: Version string
    @type verStr: str
    @param timeStamps: list of timestamps
    @type timeStamps: list of str
    """

    spl = verStr.split("/")
    nextIsVer = False
    ts = 0

    for i, s in enumerate(spl):
        if not s:
            nextIsVer = False
        elif not nextIsVer:
            nextIsVer = True
        else:
            nextIsVer = False
            spl[i] = timeStamps[ts] + ":" + s
            ts += 1

    assert(ts == len(timeStamps))
    return "/".join(spl)

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
