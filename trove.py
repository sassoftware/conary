#
# Copyright (c) 2004-2005 Specifix, Inc.
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
Implements troves (packages, components, etc.) for the repository
"""

import changelog
import copy
import files
from lib import sha1helper
import streams
import struct
import versions
from deps import deps
from changelog import AbstractChangeLog
from streams import StringStream
from streams import FrozenVersionStream
from streams import DependenciesStream
from streams import ByteStream

class Trove:
    """
    Packages are groups of files and other packages, which are included by
    reference. By convention, "package" often refers to a package with
    files but no other packages, while a "group" means a package with other
    packages but no files. While this object allows any mix of file and
    package inclusion, in practice conary doesn't allow it.
    """
    def copy(self):
	return copy.deepcopy(self)

    def getName(self):
        return self.name
    
    def getVersion(self):
        return self.version
    
    def changeVersion(self, version):
        self.version = version

    def changeFlavor(self, flavor):
        self.flavor = flavor

    def isRedirect(self):
        return self.redirect

    def addFile(self, pathId, path, version, fileId):
	assert(len(pathId) == 16)
	assert(fileId is None or len(fileId) == 20)
        assert(not self.redirect)
	self.idMap[pathId] = (path, fileId, version)

    # pathId is the only thing that must be here; the other fields could
    # be None
    def updateFile(self, pathId, path, version, fileId):
	(origPath, origFileId, origVersion) = self.idMap[pathId]

	if not path:
	    path = origPath

	if not version:
	    version = origVersion
	    
	if not fileId:
	    fileId = origFileId
	    
	self.idMap[pathId] = (path, fileId, version)

    def removeFile(self, pathId):   
	del self.idMap[pathId]

	return self.idMap.iteritems()

    def iterFileList(self):
	# don't use idMap.iteritems() here; we don't want to exposure
	# our internal format
	for (theId, (path, fileId, version)) in self.idMap.iteritems():
	    yield (theId, path, fileId, version)

    def getFile(self, pathId):
        x = self.idMap[pathId]
	return (x[0], x[1], x[2])

    def hasFile(self, pathId):
	return self.idMap.has_key(pathId)

    def addTrove(self, name, version, flavor, presentOkay = False):
	"""
	Adds a single version of a package.

	@param name: name of the package
	@type name: str
	@param version: version of the package
	@type version: versions.Version
	@param flavor: flavor of the package to include
	@type flavor: deps.deps.DependencySet
	@param presentOkay: replace if this is a duplicate, don't complain
	@type presentOkay: boolean
	"""
	if not presentOkay and self.packages.has_key((name, version, flavor)):
	    raise TroveError, "duplicate trove included in %s" % self.name
	self.packages[(name, version, flavor)] = True

    def delTrove(self, name, version, flavor, missingOkay):
	"""
	Removes a single version of a package.

	@param name: name of the package
	@type name: str
	@param version: version of the package
	@type version: versions.Version
	@param flavor: flavor of the package to include
	@type flavor: deps.deps.DependencySet
	@param missingOkay: should we raise an error if the version isn't
	part of this trove?
	@type missingOkay: boolean
	"""
	if self.packages.has_key((name, version, flavor)):
	    del self.packages[(name, version, flavor)]
	elif missingOkay:
	    pass
	else:
	    # FIXME, we should have better text here
	    raise TroveError

    def iterTroveList(self):
	"""
	Returns a generator for (packageName, version, flavor) ordered pairs, 
	listing all of the package in the group, along with their versions. 

	@rtype: list
	"""
	return self.packages.iterkeys()

    def hasTrove(self, name, version, flavor):
	return self.packages.has_key((name, version, flavor))

    # returns a dictionary mapping a pathId to a (path, version, pkgName) tuple
    def applyChangeSet(self, pkgCS):
	"""
	Updates the package from the changes specified in a change set.
	Returns a dictionary, indexed by pathId, which gives the
	(path, version, packageName) for that file.

	@param pkgCS: change set
	@type pkgCS: TroveChangeSet
	@rtype: dict
	"""

	self.redirect = pkgCS.getIsRedirect()
        if self.redirect:
            # we don't explicitly remove files for redirects
            self.idMap = {}

	fileMap = {}

	for (pathId, path, fileId, fileVersion) in pkgCS.getNewFileList():
	    self.addFile(pathId, path, fileVersion, fileId)
	    fileMap[pathId] = self.idMap[pathId] + (self.name, None, None, None)

	for (pathId, path, fileId, fileVersion) in pkgCS.getChangedFileList():
	    (oldPath, oldFileId, oldVersion) = self.idMap[pathId]
	    self.updateFile(pathId, path, fileVersion, fileId)
	    # look up the path/version in self.idMap as the ones here
	    # could be None
	    fileMap[pathId] = self.idMap[pathId] + (self.name, oldPath, oldFileId, oldVersion)

	for pathId in pkgCS.getOldFileList():
	    self.removeFile(pathId)

	self.mergeTroveListChanges(pkgCS.iterChangedTroves())
	self.flavor = pkgCS.getNewFlavor()
	self.changeLog = pkgCS.getChangeLog()
	self.setProvides(pkgCS.getProvides())
	self.setRequires(pkgCS.getRequires())
	self.changeVersion(pkgCS.getNewVersion())
	self.changeFlavor(pkgCS.getNewFlavor())

        if pkgCS.isAbsolute():
            self.troveInfo = TroveInfo(pkgCS.getTroveInfoDiff())
        else:
            self.troveInfo.twm(pkgCS.getTroveInfoDiff(), self.troveInfo)

	return fileMap

    def mergeTroveListChanges(self, changeList, redundantOkay = False):
	"""
	Merges a set of changes to the included package list into this
	package.

	@param changeList: A list or generator specifying a set of
	package changes; this is the same as returned by
	TroveChangeSet.iterChangedTroves()
	@type changeList: (name, list) tuple
	@param redundantOkay: Redundant changes are normally considered errors
	@type redundantOkay: boolean
	"""

	for (name, list) in changeList:
	    for (oper, version, flavor) in list:
		if oper == '+':
		    self.addTrove(name, version, flavor,
					   presentOkay = redundantOkay)

		elif oper == "-":
		    self.delTrove(name, version, flavor,
					   missingOkay = redundantOkay)
    
    def __eq__(self, them):
	"""
	Compare two troves for equality. This is an expensive operation,
	and shouldn't really be done. It's handy for testing the database
	though.
	"""
        if them is None:
            return False
	if self.getName() != them.getName():
	    return False
	if self.getVersion() != them.getVersion():
	    return False
	if self.getFlavor() != them.getFlavor():
	    return False
	if self.isRedirect() != them.isRedirect():
	    return False

	(csg, pcl, fcl) = self.diff(them)
	return (not pcl) and (not fcl) and (not csg.getOldFileList()) \
            and self.getRequires() == them.getRequires() \
            and self.getProvides() == them.getProvides() \
            and self.getTroveInfo() == them.getTroveInfo()

    def __ne__(self, them):
	return not self == them

    def diff(self, them, absolute = 0):
	"""
	Generates a change set between them (considered the old version) and
	this instance. We return the change set, a list of other package diffs
	which should be included for this change set to be complete, and a list
	of file change sets which need to be included.  The list of package
	changes is of the form (pkgName, oldVersion, newVersion, oldFlavor,
	newFlavor).  If absolute is True, oldVersion is always None and
	absolute diffs can be used.  Otherwise, absolute versions are not
	necessary, and oldVersion of None means the package is new. The list of
	file changes is a list of (pathId, oldVersion, newVersion, oldPath,
	newPath) tuples, where newPath is 
	the path to the file in this package.

	@param them: object to generate a change set from (may be None)
	@type them: Group
	@param absolute: tells if this is a new group or an absolute change
	when them is None
	@type absolute: boolean
	@rtype: (TroveChangeSet, fileChangeList, troveChangeList)
	"""

	assert(not them or self.name == them.name)

	# find all of the file ids which have been added, removed, and
	# stayed the same
	if them:
            troveInfoDiff = self.troveInfo.diff(them.troveInfo)
            if troveInfoDiff is None:
                troveInfoDiff = ""

	    themMap = them.idMap
	    chgSet = TroveChangeSet(self.name, self.changeLog,
				      them.getVersion(),	
				      self.getVersion(),
				      them.getFlavor(), self.getFlavor(),
				      absolute = False,
                                      isRedirect = self.redirect,
                                      troveInfoDiff = troveInfoDiff)
	else:
	    themMap = {}
	    chgSet = TroveChangeSet(self.name, self.changeLog,
				      None, self.getVersion(),
				      None, self.getFlavor(),
				      absolute = absolute,
                                      isRedirect = self.redirect,
                                      troveInfoDiff = self.troveInfo.freeze())

	# dependency and flavor information is always included in total;
	# this lets us do dependency checking w/o having to load packages
	# on the client
	chgSet.setRequires(self.requires)
	chgSet.setProvides(self.provides)

	removedIds = []
	addedIds = []
	sameIds = {}
	filesNeeded = []

        if not self.redirect:
            # we just ignore file information for redirects
            allIds = self.idMap.keys() + themMap.keys()
            for pathId in allIds:
                inSelf = self.idMap.has_key(pathId)
                inThem = themMap.has_key(pathId)
                if inSelf and inThem:
                    sameIds[pathId] = None
                elif inSelf:
                    addedIds.append(pathId)
                else:
                    removedIds.append(pathId)

            for pathId in removedIds:
                chgSet.oldFile(pathId)

            for pathId in addedIds:
                (selfPath, selfFileId, selfVersion) = self.idMap[pathId]
                filesNeeded.append((pathId, None, None, selfFileId, 
                                    selfVersion))
                chgSet.newFile(pathId, selfPath, selfFileId, selfVersion)

            for pathId in sameIds.keys():
                (selfPath, selfFileId, selfVersion) = self.idMap[pathId]
                (themPath, themFileId, themVersion) = themMap[pathId]

                newPath = None
                newVersion = None

                if selfPath != themPath:
                    newPath = selfPath

                if selfVersion != themVersion or themFileId != selfFileId:
                    newVersion = selfVersion
                    filesNeeded.append((pathId, themFileId, themVersion, 
                                        selfFileId, selfVersion))

                if newPath or newVersion:
                    chgSet.changedFile(pathId, newPath, selfFileId, newVersion)

	# now handle the packages we include
	added = {}
	removed = {}

	for key in self.packages.iterkeys():
	    if them and them.packages.has_key(key): continue

	    (name, version, flavor) = key
	    chgSet.newTroveVersion(name, version, flavor)

            d = added.setdefault(name, {})
            l = d.setdefault(flavor, [])
            l.append(version)

	if them:
	    for key in them.packages.iterkeys():
		if self.packages.has_key(key): continue

		(name, version, flavor) = key
		chgSet.oldTroveVersion(name, version, flavor)
                d = removed.setdefault(name, {})
                l = d.setdefault(flavor, [])
                l.append(version)

	pkgList = []

	if absolute:
	    for name in added.keys():
		for flavor in added[name]:
		    for version in added[name][flavor]:
			pkgList.append((name, None, version, None, flavor))
	    return (chgSet, filesNeeded, pkgList)

	# use added and removed to assemble a list of package diffs which need
	# to go along with this change set

	for name in added.keys(): 
	    if not removed.has_key(name):
		# there isn't anything which disappeared that has the same
		# name; this must be a new addition
		for newFlavor in added[name]:
		    for version in added[name][newFlavor]:
			pkgList.append((name, None, version, None, newFlavor))

		del added[name]

	changePair = []
	# we know everything added now has a matching name in removed; let's
	# try and match up the flavors. first of all we'll look for exact
	# matches
	for name in added.keys():
	    for newFlavor in added[name].keys():
		if removed[name].has_key(newFlavor):
		    # we have a name/flavor match
		    changePair.append((name, added[name][newFlavor], newFlavor,
				       removed[name][newFlavor], newFlavor))
		    del added[name][newFlavor]
		    del removed[name][newFlavor]

	    if not added[name]:
		del added[name]
	    if not removed[name]:
		del removed[name]

	# for things that are left, see if we can match flavors based on
	# the architecture
	for name in added.keys():
            if not removed.has_key(name):
                # nothing to match up against
                continue

	    for newFlavor in added[name].keys():
		if not newFlavor:
		    # this isn't going to match anything well
		    continue

		match = None

		# first check for matches which are a superset of the old
		# flavor, then for ones which are a subset of the old flavor
		for oldFlavor in removed[name]:
		    if not oldFlavor:
			continue

		    if newFlavor.satisfies(oldFlavor):
			match = removed[name][oldFlavor]
			del removed[name][oldFlavor]
			break

		if match:
		    changePair.append((name, added[name][newFlavor], newFlavor, 
				       match, oldFlavor))
		    del added[name][newFlavor]
		    continue

		for oldFlavor in removed[name].keys():
		    if not oldFlavor:
			continue

		    if oldFlavor.satisfies(newFlavor):
			match = removed[name][oldFlavor]
			del removed[name][oldFlavor]
			break

		if match:
		    changePair.append((name, added[name][newFlavor], newFlavor, 
				       match, oldFlavor))
		    del added[name][newFlavor]

	    if not added[name]:
		del added[name]
	    if not removed[name]:
		del removed[name]
	    
	for name in added.keys():
	    if len(added[name]) == 1 and removed.has_key(name) and \
                        len(removed[name]) == 1:
		# one of each? they *must* be a good match...
		newFlavor = added[name].keys()[0]
		oldFlavor = removed[name].keys()[0]
		changePair.append((name, added[name][newFlavor], newFlavor, 
				   removed[name][oldFlavor], oldFlavor))
		del added[name]
		del removed[name]
		continue

	for name in added.keys():
	    for newFlavor in added[name].keys():
		# no good match. that's too bad
		changePair.append((name, added[name][newFlavor], newFlavor, 
				   None, None))

	for (name, newVersionList, newFlavor, oldVersionList, oldFlavor) \
		    in changePair:
	    assert(newVersionList)

	    if not oldVersionList:
		for newVersion in newVersionList:
		    pkgList.append((name, None, newVersion, 
					  None, newFlavor))
                continue

	    # for each new version of a package, try and generate the diff
	    # between that package and the version of the package which was
	    # removed which was on the same branch. if that's not possible,
	    # see if the parent of the package was removed, and use that as
	    # the diff. if we can't do that and only one version of this
	    # package is being obsoleted, use that for the diff. if we
	    # can't do that either, throw up our hands in a fit of pique

	    for version in newVersionList:
		branch = version.branch()
		if branch.hasParentBranch():
		    parent = branch.parentBranch()
		else:
		    parent = None

		if not oldVersionList:
		    # no nice match, that's too bad
		    pkgList.append((name, None, version, None, newFlavor))
		elif len(oldVersionList) == 1:
		    pkgList.append((name, oldVersionList[0], version, 
				    oldFlavor, newFlavor))
		    del oldVersionList[0]
		else:
		    sameBranch = None
		    parentVersion = None
		    childNode = None
		    childBranch = None

		    for other in oldVersionList:
			if other.branch() == branch:
			    sameBranch = other
			if parent and other == parent:
			    parentVersion = other
			if other.hasParentVersion():
			    if other.parentVersion() == version:
				childNode = other
                        if other.branch().hasParentBranch():
                            if other.branch().parentBranch() == branch:
                                childBranch = other

		    # none is a sentinel
		    priority = [ sameBranch, parentVersion, childNode, 
				 childBranch, None ]

		    for match in priority:
			if match is not None:
			    break

		    if match is not None:
			oldVersionList.remove(match)
			pkgList.append((name, match, version, 
					oldFlavor, newFlavor))
		    else:
			# Here's the fit of pique. This shouldn't happen
			# except for the most ill-formed of groups.
			raise IOError, "ick. yuck. blech. ptooey."

	    # remove old versions which didn't get matches
	    for oldVersion in oldVersionList:
		pkgList.append((name, oldVersion, None, oldFlavor, None))

        for name, flavorList in removed.iteritems():
            for flavor, versionList in flavorList.iteritems():
                for version in versionList:
                    pkgList.append((name, version, None, flavor, None))

	return (chgSet, filesNeeded, pkgList)

    def setProvides(self, provides):
        self.provides = provides

    def setRequires(self, requires):
        self.requires = requires

    def getProvides(self):
        return self.provides

    def getRequires(self):
        return self.requires

    def getFlavor(self):
        return self.flavor

    def getChangeLog(self):
        return self.changeLog

    def getTroveInfo(self):
        return self.troveInfo

    def __init__(self, name, version, flavor, changeLog, isRedirect = False):
        assert(flavor is not None)
	self.idMap = {}
	self.name = name
	self.version = version
	self.flavor = flavor
	self.packages = {}
        self.provides = None
        self.requires = None
	self.changeLog = changeLog
        self.redirect = isRedirect
        self.troveInfo = TroveInfo()

class ReferencedTroveSet(dict, streams.InfoStream):

    def freeze(self, skipSet = None):
	l = []
	for name, troveList in self.iteritems():
	    subL = []
	    for (change, version, flavor) in troveList:
		version = version.freeze()
		if flavor:
		    flavor = flavor.freeze()
		else:
		    flavor = "-"

		subL.append(change)
		subL.append(version)
		subL.append(flavor)

	    l.append(name)
	    l += subL
	    l.append("")

	return "\0".join(l)

    def thaw(self, data):
	if not data: return
	self.clear()

	l = data.split("\0")
	i = 0

	while i < len(l):
	    name = l[i]
	    self[name] = []

	    i += 1
	    while l[i]:
		change = l[i]
		version = versions.ThawVersion(l[i + 1])
		flavor = l[i + 2]

		if flavor == "-":
		    flavor = deps.DependencySet()
		else:
		    flavor = deps.ThawDependencySet(flavor)

		self[name].append((change, version, flavor))
		i += 3

	    i += 1

    def __init__(self, data = None):
	dict.__init__(self)
	if data is not None:
	    self.thaw(data)

class OldFileStream(list, streams.InfoStream):

    def freeze(self, skipSet = None):
	return "".join(self)

    def thaw(self, data):
	i = 0
	del self[:]
	while i < len(data):
	    self.append(data[i:i+16])
	    i += 16
	assert(i == len(data))

    def __init__(self, data = None):
	list.__init__(self)
	if data is not None:
	    self.thaw(data)

_TROVEINFO_TAG_SIZE        = 0
_TROVEINFO_TAG_SOURCENAME  = 1

class TroveInfo(streams.StreamSet):
    ignoreUnknown = True
    streamDict = {
        _TROVEINFO_TAG_SIZE       : ( streams.LongLongStream, 'size'       ),
        _TROVEINFO_TAG_SOURCENAME : ( streams.StringStream  , 'sourceName' )
    }

class ReferencedFileList(list, streams.InfoStream):

    def freeze(self, skipSet = None):
	l = []

	for (pathId, path, fileId, version) in self:
	    l.append(pathId)
	    if not path:
		path = ""

	    l.append(struct.pack("!H", len(path)))
	    l.append(path)

	    if not fileId:
		fileId = ""

	    l.append(struct.pack("!H", len(fileId)))
	    l.append(fileId)

	    if version:
		version = version.asString()
	    else:
		version = ""

	    l.append(struct.pack("!H", len(version)))
	    l.append(version)

	return "".join(l)

    def thaw(self, data):
	del self[:]
	if not data:
	    return

	i = 0
	while i < len(data):
	    pathId = data[i:i+16]
	    i += 16

	    pathLen = struct.unpack("!H", data[i:i+2])[0]
	    i += 2
	    if pathLen:
		path = data[i:i + pathLen]
		i += pathLen
	    else:
		path = None

	    fileIdLen = struct.unpack("!H", data[i:i+2])[0]
	    i += 2
	    if fileIdLen:
                assert(fileIdLen == 20)
		fileId = data[i:i+20]
		i += fileIdLen
	    else:
		fileIdLen = None

	    versionLen = struct.unpack("!H", data[i:i+2])[0]
	    i += 2
	    if versionLen:
		version = versions.VersionFromString(data[i:i + versionLen])
		i += versionLen
	    else:
		version = None

	    self.append((pathId, path, fileId, version))

    def __init__(self, data = None):
	list.__init__(self)
	if data is not None:
	    self.thaw(data)

_STREAM_TCS_NAME	    =  0
_STREAM_TCS_OLD_VERSION	    =  1
_STREAM_TCS_NEW_VERSION	    =  2
_STREAM_TCS_REQUIRES	    =  3
_STREAM_TCS_PROVIDES	    =  4
_STREAM_TCS_CHANGE_LOG	    =  5
_STREAM_TCS_OLD_FILES	    =  6
_STREAM_TCS_TYPE	    =  7
_STREAM_TCS_TROVE_CHANGES   =  8
_STREAM_TCS_NEW_FILES       =  9
_STREAM_TCS_CHG_FILES       = 10
_STREAM_TCS_OLD_FLAVOR      = 11
_STREAM_TCS_NEW_FLAVOR      = 12
_STREAM_TCS_IS_REDIRECT     = 13
_STREAM_TCS_TROVEINFO       = 14

_TCS_TYPE_ABSOLUTE = 1
_TCS_TYPE_RELATIVE = 2

class AbstractTroveChangeSet(streams.LargeStreamSet):

    streamDict = { 
	_STREAM_TCS_NAME	: (StringStream,         "name"          ),
        _STREAM_TCS_OLD_VERSION : (FrozenVersionStream,  "oldVersion"    ),
        _STREAM_TCS_NEW_VERSION : (FrozenVersionStream,  "newVersion"    ),
        _STREAM_TCS_REQUIRES    : (DependenciesStream,   "requires"      ),
        _STREAM_TCS_PROVIDES    : (DependenciesStream,   "provides"      ),
        _STREAM_TCS_CHANGE_LOG  : (AbstractChangeLog,    "changeLog"     ),
        _STREAM_TCS_OLD_FILES   : (OldFileStream,	 "oldFiles"      ),
        _STREAM_TCS_TYPE        : (streams.IntStream,    "tcsType"       ),
        _STREAM_TCS_TROVE_CHANGES:(ReferencedTroveSet,   "packages"      ),
        _STREAM_TCS_NEW_FILES   : (ReferencedFileList,   "newFiles"      ),
        _STREAM_TCS_CHG_FILES   : (ReferencedFileList,   "changedFiles"  ),
        _STREAM_TCS_OLD_FLAVOR  : (DependenciesStream,   "oldFlavor"     ),
        _STREAM_TCS_NEW_FLAVOR  : (DependenciesStream,   "newFlavor"     ),
        _STREAM_TCS_IS_REDIRECT : (ByteStream,           "isRedirect"    ),
        _STREAM_TCS_TROVEINFO   : (StringStream,         "troveInfoDiff" ),
    }

    ignoreUnknown = True

    """
    Represents the changes between two packages and forms part of a
    ChangeSet. 
    """

    def isAbsolute(self):
	return self.tcsType.value() == _TCS_TYPE_ABSOLUTE

    def newFile(self, pathId, path, fileId, version):
	self.newFiles.append((pathId, path, fileId, version))

    def getNewFileList(self):
	return self.newFiles

    def oldFile(self, pathId):
	self.oldFiles.append(pathId)

    def getOldFileList(self):
	return self.oldFiles

    def getName(self):
	return self.name.value()

    def getTroveInfoDiff(self):
        return self.troveInfoDiff.value()

    def getChangeLog(self):
	return self.changeLog

    def changeOldVersion(self, version):
	self.oldVersion.set(version)

    def changeNewVersion(self, version):
	self.newVersion.set(version)

    def changeChangeLog(self, cl):
	self.changeLog = cl

    def getOldVersion(self):
	return self.oldVersion.value()

    def getNewVersion(self):
	return self.newVersion.value()

    # path and/or version can be None
    def changedFile(self, pathId, path, fileId, version):
	self.changedFiles.append((pathId, path, fileId, version))

    def getChangedFileList(self):
	return self.changedFiles

    def iterChangedTroves(self):
	return self.packages.iteritems()

    def newTroveVersion(self, name, version, flavor):
	"""
	Adds a version of a package which appeared in newVersion.

	@param name: name of the package
	@type name: str
	@param version: new version
	@type version: versions.Version
	@param flavor: new flavor
	@type flavor: deps.deps.DependencySet
	"""

        l = self.packages.setdefault(name, [])
	l.append(('+', version, flavor))

    def updateChangedPackage(self, name, flavor, old, new):
	"""
	Removes package (name, flavor, old version) from the changed list and
	adds package (name, flavor, version) new to the list (with the same 
	change type).

	@param name: name of the package
	@type name: str
	@param flavor: flavor of the package
	@type flavor: deps.deps.DependencySet
	@param old: version to remove from the changed list
	@type old: versions.VersionString
	@param new: version to add to the changed list
	@type new: versions.VersionString
	"""
	for (theName, list) in self.packages.iteritems():
	    if theName != name: continue
	    for (i, (change, ver)) in enumerate(list):
		if ver == old:
		    list[i] = (change, new)
		    return

    def oldTroveVersion(self, name, version, flavor):
	"""
	Adds a version of a package which appeared in oldVersion.

	@param name: name of the package
	@type name: str
	@param version: old version
	@type version: versions.Version
	@param flavor: old flavor
	@type flavor: deps.deps.DependencySet
	"""
        l = self.packages.setdefault(name, [])
        l.append(('-', version, flavor))

    def formatToFile(self, changeSet, f):
	f.write("%s " % self.getName())

	if self.isAbsolute():
	    f.write("absolute ")
	elif self.getOldVersion():
	    f.write("from %s to " % self.getOldVersion().asString())
	else:
	    f.write("new ")

	f.write("%s\n" % self.getNewVersion().asString())

        def depformat(name, dep, f):
            f.write('\t%s: %s\n' %(name,
                                   str(dep).replace('\n', '\n\t%s'
                                                    %(' '* (len(name)+2)))))
        if self.getRequires():
            depformat('Requires', self.getRequires(), f)
        if self.getProvides():
            depformat('Provides', self.getProvides(), f)
        if self.getOldFlavor():
            depformat('Old Flavor', self.getOldFlavor(), f)
        if self.getNewFlavor():
            depformat('New Flavor', self.getNewFlavor(), f)

	for (pathId, path, fileId, version) in self.newFiles:
	    #f.write("\tadded (%s(.*)%s)\n" % (pathId[:6], pathId[-6:]))
            change = changeSet.getFileChange(None, fileId)
            fileobj = files.ThawFile(change, pathId)
            
	    if isinstance(fileobj, files.SymbolicLink):
		name = "%s -> %s" % (path, fileobj.target.value())
	    else:
		name = path
	    
            f.write("\t%s    1 %-8s %-8s %s %s %s\n" % 
                    (fileobj.modeString(), fileobj.inode.owner(),
                     fileobj.inode.group(), fileobj.sizeString(),
                     fileobj.timeString(), name))

	for (pathId, path, fileId, version) in self.changedFiles:
	    pathIdStr = sha1helper.md5ToString(pathId)
	    if path:
		f.write("\tchanged %s (%s(.*)%s)\n" % 
			(path, pathIdStr[:6], pathIdStr[-6:]))
	    else:
		f.write("\tchanged %s\n" % pathIdStr)
	    oldFileId, change = changeSet._findFileChange(fileId)
	    f.write("\t\t%s\n" % " ".join(files.fieldsChanged(change)))

	for pathId in self.oldFiles:
	    pathIdStr = sha1helper.md5ToString(pathId)
	    f.write("\tremoved %s(.*)%s\n" % (pathIdStr[:6], pathIdStr[-6:]))

	for name in self.packages.keys():
	    list = [ x[0] + x[1].asString() for x in self.packages[name] ]
	    f.write("\t" + name + " " + " ".join(list) + "\n")

    def setProvides(self, provides):
	self.provides.set(provides)

    def setIsRedirect(self, val):
        assert(type(val) == bool)
        self.isRedirect.set(val)

    def getIsRedirect(self):
        return self.isRedirect.value()

    def getProvides(self):
        p = self.provides.value()
        if not p:
            return None
        return p

    def setRequires(self, requires):
	self.requires.set(requires)

    def getRequires(self):
        r = self.requires.value()
        if not r:
            return None
        return r

    def getOldFlavor(self):
        return self.oldFlavor.value()

    def getNewFlavor(self):
        return self.newFlavor.value()

class TroveChangeSet(AbstractTroveChangeSet):

    def __init__(self, name, changeLog, oldVersion, newVersion, 
		 oldFlavor, newFlavor, absolute = 0, isRedirect = False,
                 troveInfoDiff = None):
	AbstractTroveChangeSet.__init__(self)
	assert(isinstance(newVersion, versions.VersionSequence))
	assert(isinstance(newFlavor, deps.DependencySet))
	assert(oldFlavor is None or isinstance(oldFlavor, deps.DependencySet))
	self.name.set(name)
	self.oldVersion.set(oldVersion)
	self.newVersion.set(newVersion)
	if changeLog:
	    self.changeLog = changeLog
	if absolute:
	    self.tcsType.set(_TCS_TYPE_ABSOLUTE)
	else:
	    self.tcsType.set(_TCS_TYPE_RELATIVE)
        self.provides.set(None)
        self.requires.set(None)
	self.oldFlavor.set(oldFlavor)
	self.newFlavor.set(newFlavor)
        self.isRedirect.set(isRedirect)
        assert(troveInfoDiff is not None)
        self.troveInfoDiff.set(troveInfoDiff)

class ThawTroveChangeSet(AbstractTroveChangeSet):

    def __init__(self, buf):
	AbstractTroveChangeSet.__init__(self, buf)

	# we can't represent the different between an empty flavor and
        # no flavor; the oldFlavor is the only place this matters, and
        # we can infer the answer from oldVersion
        if self.oldVersion.value() is None:
	    self.oldFlavor.set(None)
	
class TroveError(Exception):

    """
    Ancestor for all exceptions raised by the package module.
    """

    pass

class ParseError(TroveError):

    """
    Indicates that an error occured parsing a group file.
    """

    pass

class PatchError(TroveError):

    """
    Indicates that an error occured parsing a group file.
    """

    pass

