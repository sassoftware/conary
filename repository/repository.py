#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

# defines the SRS system repository

import changeset
import datastore
import tempfile
import util
import versions

class AbstractTroveDatabase:

    def commitChangeSet(self, cs):
	raise NotImplementedError

    def createBranch(self, newBranch, where, troveList = []):
	"""
	Creates a branch for the troves in the repository. This
	operations is recursive, with any required troves and files
	also getting branched. Duplicate branches can be created,
	but only if one of the following is true:
	 
	  1. C{where} specifies a particular version to branch from
	  2. the branch does not yet exist and C{where} is a label which matches multiple existing branches

	C{where} specifies the node branches are created from for the
	troves in C{troveList} (or all of the troves if C{troveList}
	is empty). Any troves or files branched due to inclusion in a
	branched trove will be branched at the version required by the
	object including it. If different versions of objects are
	included from multiple places, bad things will happen (an
	incomplete branch will be formed). More complicated algorithms
	for branch will fix this, but it's not clear doing so is
	necessary.

	@param newBranch: Label of the new branch
	@type newBranch: versions.BranchName
	@param where: Where the branch should be created from
	@type where: versions.Version or versions.BranchName
	@param troveList: Name of the troves to branch; empty list if all
	troves in the repository should be branched.
	@type troveList: list of str
	"""
	raise NotImplementedError

    def createChangeSet(self, packageList, recurse = True, withFiles = True):
	"""
	packageList is a list of (pkgName, flavor, oldVersion, newVersion, 
        absolute) tuples. 

	if oldVersion == None and absolute == 0, then the package is assumed
	to be new for the purposes of the change set

	if newVersion == None then the package is being removed
	"""
	cs = changeset.ChangeSetFromRepository(self)
	for (name, flavor, v1, v2, absolute) in packageList:
	    cs.addPrimaryPackage(name, v2, flavor)

	dupFilter = {}

	# don't use a for in here since we grow packageList inside of
	# this loop
	while packageList:
	    (packageName, flavor, oldVersion, newVersion, absolute) = \
		packageList[0]
	    del packageList[0]

	    # make sure we haven't already generated this changeset; since
	    # packages can be included from other packages we could try
	    # to generate quite a few duplicates
	    if dupFilter.has_key((packageName, flavor)):
		match = False
		for (otherOld, otherNew) in dupFilter[(packageName, flavor)]:
		    if not otherOld and not oldVersion:
			same = True
		    elif not otherOld and oldVersion:
			same = False
		    elif otherOld and not oldVersion:
			same = False
		    else:
			same = otherOld == newVersion

		    if same and otherNew == newVersion:
			match = True
			break
		
		if match: continue

		dupFilter[(packageName, flavor)].append(
					    (oldVersion, newVersion))
	    else:
		dupFilter[(packageName, flavor)] = [ (oldVersion, newVersion) ]

	    if not newVersion:
		# remove this package and any subpackages
		old = self.getTrove(packageName, oldVersion, flavor)
		cs.oldPackage(packageName, oldVersion, flavor)
		for (name, version, flavor) in old.iterTroveList():
                    # it's possible that a component of a package
                    # was erased, make sure that it is installed
                    if self.hasTrove(name, version, flavor):
                        packageList.append((name, flavor, version, None, 
					    absolute))
		    
		continue
		    
	    new = self.getTrove(packageName, newVersion, flavor)
	 
	    if oldVersion:
		old = self.getTrove(packageName, oldVersion, flavor)
	    else:
		old = None

	    (pkgChgSet, filesNeeded, pkgsNeeded) = \
				new.diff(old, absolute = absolute)

	    if recurse:
		for (pkgName, old, new, flavor) in pkgsNeeded:
		    packageList.append((pkgName, flavor, old, new, absolute))

	    cs.newPackage(pkgChgSet)

	    for (fileId, oldVersion, newVersion, newPath) in filesNeeded:
		if oldVersion:
		    (oldFile, oldCont) = self.getFileVersion(fileId, 
				oldVersion, withContents = 1)
		else:
		    oldFile = None
		    oldCont = None

		(newFile, newCont) = self.getFileVersion(fileId, newVersion,
					    withContents = 1)

		(filecs, hash) = changeset.fileChangeSet(fileId, oldFile, 
							 newFile)

		cs.addFile(fileId, oldVersion, newVersion, filecs)

		if hash and withFiles:
		    (contType, cont) = changeset.fileContentsDiff(oldFile, 
						oldCont, newFile, newCont)
		    cs.addFileContents(fileId, contType, cont, 
				       newFile.flags.isConfig())

	return cs

    def findTrove(repos, defaultLabel, name, versionStr = None):
	"""
	Looks up a package in the given repository based on the name and
	version provided. If any errors are occured, PackageNotFound is
	raised with an appropriate error message. Multiple matches could
	be found if versionStr refers to a label.

	@param defaultLabel: Label of the branch to use if no branch
	is specified. If only a branch name is given (not a complete label),
	the repository name from this label is used as the repository
	name for the branch name to form a complete label.
	@type defaultLabel: versions.BranchName
	@param name: Package name
	@type name: str
	@param versionStr: Package version
	@type versionStr: str
	@rtype: list of package.Package
	"""
	raise NotImplementedError

    def getFileVersion(self, fileId, version, withContents = 0):
	"""
	Returns the file object for the given (fileId, version).
	"""
	raise NotImplementedError

    def getFileContents(self, sha1List):
	"""
	Retrieves the files w/ the sha1s in the parameter list. If
	an item in the list is a tuple, the first item in the tuple
	should be the sha1 and the second the path the file should
	be written to. If item in the tuple is a string, it should be
	just a sha1 to retrieve.

	A dict indexed by sha1's is returned. For sha1s which were given 
	file names, the dict contains the file name the file was stored
	in. For sha1s without file names, the dict contains an open file
	object for the contents of the file (the file will have already
	been unlinked, and has no file name in this case).

	@param sha1List: files to retrieve
	@type sha1List: list
	@rtype: list
	"""

    def getTrove(self, troveName, version, flavor):
	"""
	Returns the trove which matches (troveName, version, flavor). If
	the trove does not exist, PackageMissing is raised.

	@param troveName: package name
	@type troveName: str
	@param version: version
	@type version: versions.Version
	@param flavor: flavor
	@type flavor: deps.deps.DependencySet
	@rtype: package.Package
	"""
	raise NotImplementedError

    def getTroves(self, troveList):
	"""
	Returns a list of trove objects which parallels troveList. troveList 
	is a list of (troveName, version, flavor) tuples. Version can
	a version or a branch; if it's a branch the latest version of the
	trove on that branch is returned. If there is no match for a
	particular tuple, None is placed in the retur nlist for that tuple.
	"""
	rc = []
	for item in troveList:
	    try:
		rc.append(self.getTrove(*item))
	    except PackageMissing:
		rc.append(None)

	return rc

    def iterAllTroveNames(self):
	"""
	Returns a list of all of the troves contained in the repository.

	@rtype: list of str
	"""
	raise NotImplementedError

    def iterFilesInTrove(self, troveName, version, flavor,
                         sortByPath = False, withFiles = False):
	"""
	Returns a generator for (fileId, path, version) tuples for all
	of the files in the trove. This is equivlent to trove.iterFileList(),
	but if withFiles is set this is *much* more efficient.

	@param withFiles: if set, the file object for the file is 
	created and returned as the fourth element in the tuple.
	"""
	raise NotImplementedError

class IdealRepository(AbstractTroveDatabase):

    def getTroveVersionList(self, troveNameList):
	"""
	Returns a dictionary indexed by the items in troveNameList. Each
	item in the dictionary is a list of all of the versions for that 
	trove. If no versions are available for a particular trove,
	the dictionary entry for that trove's name is left empty.

	@param troveNameList: list trove names
	@type troveNameList: list of str
	@rtype: dict of lists
	"""
	raise NotImplementedError

    def getAllTroveLeafs(self, troveNameList):
	"""
	Returns a dictionary indexed by the items in troveNameList. Each
	item in the dictionary is a list of all of the leaf versions for
	that trove. If no branches are available for a particular trove,
	the dictionary entry for that trove's name is left empty.

	@param troveNameList: trove names
	@type troveNameList: list of str
	@rtype: dict of lists
	"""
	raise NotImplementedError

    def getTroveLeavesByLabel(self, troveNameList, label):
	"""
	Returns a dictionary indexed by the items in troveNameList. Each
	item in the dictionary is a list of all of the leaf versions for
	that trove which are on a branch w/ the given label. If a trove
	does not have any branches for the given label, the version list
	for that trove name will be empty.

	@param troveNameList: trove names
	@type troveNameList: list of str
	@param label: label
	@type label: versions.BranchName
	@rtype: dict of lists
	"""
	raise NotImplementedError

    def getTroveLatestVersion(self, troveName, branch):
	"""
	Returns the version of the latest version of a trove on a particular
	branch. If that branch doesn't exist for the trove, PackageMissing
	is raised.

	@param troveName: package name
	@type troveName: str
	@param branch: branch
	@type branch: versions.Version
	@rtype: versions.Version
	"""
	raise NotImplementedError

    def getTroveVersionFlavors(self, troveDict):
	"""
	Converts a dictionary of the format retured by getAllTroveLeafs()
	to contains dicts of { version : flavorList } sets instead of 
	containing lists of versions.

	@type troveDict: dict
	@rtype: dict
	"""
	raise NotImplementedError

    def findTrove(self, defaultLabel, name, versionStr = None):
	if not defaultLabel:
	    # if we don't have a default label, we need a fully qualified
	    # version string; make sure have it
	    if versionStr[0] != "/" and (versionStr.find("/") != -1 or
					 versionStr.find("@") == -1):
		raise PackageNotFound, \
		    "fully qualified version or label " + \
		    "expected instead of %s" % versionStr

	# a version is a label if
	#   1. it doesn't being with / (it isn't fully qualified)
	#   2. it only has one element (no /)
	#   3. it contains an @ sign
	if not versionStr or (versionStr[0] != "/" and  \
		(versionStr.find("/") == -1) and versionStr.count("@")):
	    # either the supplied version is a label or we're going to use
	    # the default

	    if versionStr:
		if versionStr[0] == "@" and defaultLabel:
		    versionStr = defaultLabel.getHost() + versionStr

		try:
		    label = versions.BranchName(versionStr)
		except versions.ParseError:
		    raise PackageMissing, "invalid version %s" % versionStr
	    else:
		label = defaultLabel

	    versionDict = self.getTroveLeavesByLabel([name], label)
	    if not versionDict:
		raise PackageNotFound, "branch %s does not exist for package %s" \
			    % (str(label), name)
	elif versionStr[0] != "/" and versionStr.find("/") == -1:
	    # version/release was given
	    try:
		verRel = versions.VersionRelease(versionStr)
	    except versions.ParseError, e:
		raise PackageNotFound, str(e)

	    # XXX this should restrict to the current label...
	    versionDict = self.getTroveVersionList([name])
	    for version in versionDict[name][:]:
		if version.trailingVersion() != verRel:
		    versionDict[name].remove(version)

	    if not versionDict:
		raise PackageNotFound, \
		    "version %s of %s is not on any branch named %s" % \
		    (versionStr, name, str(defaultLabel))
	elif versionStr[0] != "/":
	    # partial version string, we don't support this
	    raise PackageNotFound, \
		"incomplete version string %s not allowed" % versionStr
	else:
	    try:
		version = versions.VersionFromString(versionStr)
	    except versions.ParseError:
		raise PackageNotFound, str(e)

	    try:
		# XXX
		if version.isBranch():
		    version = self.getTroveLatestVersion(name, version)

		versionDict = { name : [ version] }
	    except PackageMissing, e:  
		raise PackageNotFound, str(e)

	flavorDict = self.getTroveVersionFlavors(versionDict)
	pkgList = []
	for version in flavorDict[name].iterkeys():
	    for flavor in flavorDict[name][version]:
		pkgList.append((name, version, flavor))

	if not pkgList:
	    raise PackageNotFound, "package %s does not exist" % name

	pkgList = self.getTroves(pkgList)

	return pkgList

class AbstractRepository(IdealRepository):
    ### Package access functions

    def hasPackage(self, troveName):
	"""
	Tests to see if the repository contains any version of the named
	trove.

	@param troveName: trove name
	@type troveName: str
	@rtype: boolean
	"""
	raise NotImplementedError

    def hasTrove(self, troveName, version, flavor):
	"""
	Tests if the repository contains a particular version of a trove.

	@param troveName: package name
	@type troveName: str
	@rtype: boolean
	"""
	raise NotImplementedError

    ### File functions

    def __init__(self):
	assert(self.__class__ != AbstractRepository)

class DataStoreRepository:

    """
    Mix-in class which lets a TroveDatabase use a Datastore object for
    storing and retrieving files.
    """

    def storeFileFromContents(self, contents, file, restoreContents):
	if file.hasContents:
	    if restoreContents:
		f = contents.get()
		self.contentsStore.addFile(f, file.contents.sha1())
	    else:
		# the file doesn't have any contents, so it must exist
		# in the data store already; we still need to increment
		# the reference count for it
		self.contentsStore.addFileReference(file.contents.sha1())

	    return 1
	
	return 0

    def removeFileContents(self, sha1):
	self.contentsStore.removeFile(sha1)

    def getFileContents(self, sha1List):
	d = {}
	for item in sha1List:
	    if type(item) == str:
		d[item] = self.contentsStore.openFile(item)
	    else:
		(sha1, path) = item
		outF = open(path, "w+")
		inF = self.contentsStore.openFile(sha1)
		util.copyfileobj(inF, outF)
		d[item] = path

	return d

    def hasFileContents(self, fileId):
	return self.contentsStore.hasFile(fileId)

    def __init__(self, path):
	fullPath = path + "/contents"
	util.mkdirChain(fullPath)
	self.contentsStore = datastore.DataStore(fullPath)

class RepositoryError(Exception):
    """Base class for exceptions from the system repository"""

class PackageNotFound(Exception):
    """Raised when findTrove failes"""

class OpenError(RepositoryError):
    """Error occured opening the repository"""

class CommitError(RepositoryError):
    """Error occured commiting a package"""

class PackageMissing(RepositoryError):

    def __str__(self):
	if self.version:
	    if self.version.isBranch():
		return ("%s %s does not exist on branch %s" % \
		    (self.type, self.packageName, self.version.asString()))

	    return "version %s of %s %s does not exist" % \
		(self.version.asString(), self.type, self.packageName)
	else:
	    return "%s %s does not exist" % (self.type, self.packageName)

    def __init__(self, packageName, version = None):
	"""
	Initializes a PackageMissing exception.

	@param packageName: package which could not be found
	@type packageName: str
	@param version: version of the package which does not exist
	@type version: versions.Version
	"""
	self.packageName = packageName
	self.version = version
	self.type = "package"
