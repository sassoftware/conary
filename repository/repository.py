#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

# defines the Conary repository

import changeset
import datastore
import deps.deps
import files
import package
import patch
import tempfile
import util
import versions

import filecontents

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

	# make a copy to remove things from
	packageList = packageList[:]

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

    def findTrove(self, defaultLabel, name, flavor, versionStr = None):
	"""
	Looks up a trove in the repository based on the name and
	version provided. If any errors occur, PackageNotFound is
	raised with an appropriate error message. Multiple matches
	could be found if versionStr refers to a label.

	@param defaultLabel: Label of the branch to use if no branch
	is specified. If only a branch name is given (not a complete label),
	the repository name from this label is used as the repository
	name for the branch name to form a complete label.
	@type defaultLabel: versions.BranchName
	@param name: Trove name
	@type name: str
	@param flavor: only troves compatible with this flavor will be returned
	@type flavor: deps.DependencySet
	@param versionStr: Trove version
	@type versionStr: str
	@rtype: list of package.Trove
	"""
	raise NotImplementedError

    def getFileVersion(self, fileId, version, withContents = 0):
	"""
	Returns the file object for the given (fileId, version).
	"""
	raise NotImplementedError

    def getFileContents(self, troveName, troveVersion, troveFlavor, path):
	"""
	Retrieves the files specified by the fileDict. The dictionary is
	indexed by (troveName, troveVersion, troveFlavor) tuples, and each
	element is a list.  If an item in the list is a tuple, the first item
	in the tuple should be the path from the trove to retrieve and the
	second the path the file should be written to. If item in the tuple is
	a string, it should be just a path to retrieve.

	A dict indexed by (troveName, troveVersion, troveFlavor, trovePath) is
	returned. For paths which were given file names, the dict contains the
	file name the file was stored in. For paths without file names, the
	dict contains an open file object for the contents of the file (the
	file will have already been unlinked, and has no file name in this
	case).

	@param fileList: files to retrieve
	@type fileList: list
	@rtype: dict
	"""
	raise NotImplementedError

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
	particular tuple, None is placed in the return list for that tuple.
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
	for that trove name will be empty. The versions returned include
	timestamps.

	@param troveNameList: trove names
	@type troveNameList: list of str
	@param label: label
	@type label: versions.BranchName
	@rtype: dict of lists
	"""
	raise NotImplementedError

    def getTroveVersionsByLabel(self, troveNameList, label):
	"""
	Returns a dictionary indexed by troveNameList. Each item in the
	dictionary is a list of all of the versions of that trove
	on the given branch, and newer versions appear later in the list.

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
	is raised. The version returned includes timestamps.

	@param troveName: package name
	@type troveName: str
	@param branch: branch
	@type branch: versions.Version
	@rtype: versions.Version
	"""
	raise NotImplementedError

    def getTroveFlavorsLatestVersion(self, troveName, branch):
	"""
	Returns a list of the most recent version for each flavor of a
        trove available on a particular branch. If that branch doesn't
        exist for the trove, an empty list is returned. The list is sorted
	by version, with earlier versions first. The versions returned
	by this function include time stamps.

	@param troveName: package name
	@type troveName: str
	@param branch: branch
	@type branch: versions.Version
	@rtype: list of (versions.Version, flavor) tuples
	"""
	raise NotImplementedError

    def getTroveVersionFlavors(self, troveDict):
	"""
	Converts a dictionary of the format retured by getAllTroveLeafs()
	to contain dicts of { version : flavorList } sets instead of 
	containing lists of versions.

	@type troveDict: dict
	@rtype: dict
	"""
	raise NotImplementedError

    def findTrove(self, defaultLabel, name, targetFlavor, versionStr = None):
	assert(not targetFlavor or 
	       isinstance(targetFlavor, deps.deps.DependencySet))

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

	    versionDict = self.getTroveVersionsByLabel([name], defaultLabel)
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
	    except versions.ParseError, e:
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
		if not flavor or (targetFlavor and 
				  targetFlavor.satisfies(flavor)):
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
    storing and retrieving files. These functions aren't provided by
    network repositories.
    """

    def _storeFileFromContents(self, contents, file, restoreContents):
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

    def _removeFileContents(self, sha1):
	self.contentsStore.removeFile(sha1)

    def _getFileObject(self, sha1):
	return self.contentsStore.openFile(sha1)

    def _hasFileContents(self, fileId):
	return self.contentsStore.hasFile(fileId)

    def getFileContents(self, troveName, troveVersion, troveFlavor, path):
	# this could be much more efficient; iterating over the files is
	# just silly
	for (fileId, tpath, tversion, fileObj) in \
		self.iterFilesInTrove(troveName, troveVersion, 
					    troveFlavor, withFiles = True):
	    if tpath != path: continue

	    inF = self.contentsStore.openFile(fileObj.contents.sha1())
	    return inF

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

class ChangeSetJobFile(object):

    __slots__ = [ "theVersion" , "theFile", "theRestoreContents",
		  "fileContents", "changeSet", "thePath", "theFileId" ]

    def version(self):
	return self.theVersion

    def changeVersion(self, ver):
	self.theVersion = ver

    def restoreContents(self):
	return self.theRestoreContents

    def file(self):
	return self.theFile

    def changeFile(self, fileObj):
	self.theFile = fileObj

    def path(self):
	return self.thePath

    def fileId(self):
	return self.theFileId

    def copy(self):
	return copy.deepcopy(self)

    def getContents(self):
	if self.fileContents == "":
	    return None
	elif self.fileContents:
	    return self.fileContents
	
	return self.changeSet.getFileContents(self.theFileId)[1]

    # overrideContents = None means use contents from changeset
    # overrideContents = "" means there are no contents
    def __init__(self, changeSet, fileId, file, version, path, 
		 overrideContents, restoreContents):
	self.theVersion = version
	self.theFile = file
	self.theRestoreContents = restoreContents
	self.fileContents = overrideContents
	self.changeSet = changeSet
	self.thePath = path
	self.theFileId = fileId

class ChangeSetJob:
    """
    ChangeSetJob provides a to-do list for applying a change set; file
    remappings should have been applied to the change set before it gets
    this far. Derivative classes can override these methods to change the
    behavior; for example, if addPackage is overridden no pacakges will
    make it to the database. The same holds for oldFile.
    """

    def addPackage(self, pkg):
	self.repos.addPackage(pkg)

    def oldPackage(self, pkg):
	pass

    def oldFile(self, fileId, fileVersion, fileObj):
	pass

    def addFile(self, newFile, storeContents = True):
	file = newFile.file()
	fileId = newFile.fileId()

	# duplicates are filtered out (as necessary) by addFileVersion
	self.repos.addFileVersion(fileId, newFile.version(), file)

	# Note that the order doesn't matter, we're just copying
	# files into the repository. Restore the file pointer to
	# the beginning of the file as we may want to commit this
	# file to multiple locations.
	if storeContents:
	    self.repos._storeFileFromContents(newFile.getContents(), file, 
					      newFile.restoreContents())

    def __init__(self, repos, cs):
	self.repos = repos
	self.cs = cs

	self.packagesToCommit = []

	fileMap = {}

	# create the package objects which need to be installed; the
	# file objects which map up with them are created later, but
	# we do need a map from fileId to the path and version of the
	# file we need, so build up a dictionary with that information
	for csPkg in cs.iterNewPackageList():
	    newVersion = csPkg.getNewVersion()
	    old = csPkg.getOldVersion()
	    pkgName = csPkg.getName()

	    if repos.hasTrove(pkgName, newVersion, csPkg.getFlavor()):
		raise CommitError, \
		       "version %s for %s is already installed" % \
			(newVersion.asString(), csPkg.getName())

	    if old:
		newPkg = repos.getTrove(pkgName, old, csPkg.getFlavor(),
					pristine = True)
		newPkg.changeVersion(newVersion)
	    else:
		newPkg = package.Trove(csPkg.getName(), newVersion,
				     csPkg.getFlavor(), csPkg.getChangeLog())

	    newFileMap = newPkg.applyChangeSet(csPkg)

	    self.packagesToCommit.append(newPkg)
	    fileMap.update(newFileMap)

	# Create the file objects we'll need for the commit. This handles
	# files which were added and files which have changed
	list = cs.getFileList()
	# sort this by fileid to ensure we pull files from the change
	# set in the right order
	list.sort()
	for (fileId, (oldVer, newVer, diff)) in list:
	    restoreContents = 1
	    if oldVer:
		oldfile = repos.getFileVersion(fileId, oldVer)
		file = oldfile.copy()
		file.twm(diff, oldfile)
		
		if file.hasContents and oldfile.hasContents and	    \
		   file.contents.sha1() == oldfile.contents.sha1():
		    restoreContents = 0
	    else:
		# this is for new files
		file = files.ThawFile(diff, fileId)

	    # we should have had a package which requires this (new) version
	    # of the file
	    assert(newVer == fileMap[fileId][1])

	    if file.hasContents and restoreContents:
		fileContents = None

		if repos._hasFileContents(file.contents.sha1()):
		    # if we already have the file in the data store we can
		    # get the contents from there
		    fileContents = filecontents.FromDataStore(
				     repos.contentsStore, file.contents.sha1(), 
				     file.contents.size())
		    contType = changeset.ChangedFileTypes.file
		else:
		    oldFile = fileMap[fileId][3]
		    contType = cs.getFileContentsType(fileId)
		    if contType == changeset.ChangedFileTypes.diff:
			# the content for this file is in the form of a diff,
			# which we need to apply against the file in the
			# repository
			assert(oldVer)
			(contType, fileContents) = cs.getFileContents(fileId)
			sha1 = oldfile.contents.sha1()
			f = repos._getFileObject(sha1)
			oldLines = f.readlines()
			del f
			diff = fileContents.get().readlines()
			(newLines, failedHunks) = patch.patch(oldLines, diff)
			fileContents = filecontents.FromString("".join(newLines))

			if failedHunks:
			    fileContents = filecontents.WithFailedHunks(
						fileContents, failedHunks)
	    else:
		# this means there are no contents to restore (None
		# means get the contents from the change set)
		fileContents = ""

	    path = fileMap[fileId][0]
	    self.addFile(ChangeSetJobFile(cs, fileId, file, newVer, path, 
					  fileContents, restoreContents))

	for (pkgName, version, flavor) in cs.getOldPackageList():
	    pkg = self.repos.getTrove(pkgName, version, flavor)
	    self.oldPackage(pkg)

	    for (fileId, path, version) in pkg.iterFileList():
		file = self.repos.getFileVersion(fileId, version)
		self.oldFile(fileId, version, file)

	for newPkg in self.packagesToCommit:
	    self.addPackage(newPkg)
