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

# defines the Conary repository

import changeset
import datastore
import deps.deps
import files
from lib import patch
from lib import sha1helper
import tempfile
import trove
from lib import util
import versions

import filecontents

class AbstractTroveDatabase:

    def commitChangeSet(self, cs):
	raise NotImplementedError

    def findTrove(self, labelPath, name, flavor, versionStr = None,
                  acrossRepositories = False, withFiles = True):
	"""
	Looks up a trove in the repository based on the name and
	version provided. If any errors occur, TroveNotFound is
	raised with an appropriate error message. Multiple matches
	could be found if versionStr refers to a label.

	@param labelPath: Path of labels to look on if no branch
	is specified. If only a branch name is given (not a complete label),
	the repository names from these labels are used as the repository
	name for the branch name to form a complete label.
	@type defaultLabel: list of versions.Label
	@param name: Trove name
	@type name: str
	@param flavor: only troves compatible with this flavor will be returned
	@type flavor: deps.DependencySet
	@param versionStr: Trove version
	@type versionStr: str
        @param acrossRepositories: normally findTrove only returns matches
        from a single repository (the first one with a match). if this is
        set it continues searching through all repositories
        @type acrossRepositories: boolean
        @param withFiles: File information is only returned if this is True
        @type withFiles: boolean
	@rtype: list of trove.Trove
	"""
	raise NotImplementedError

    def getFileVersion(self, fileId, version, withContents = 0):
	"""
	Returns the file object for the given (fileId, version).
	"""
	raise NotImplementedError

    def getFileVersions(self, l):
	"""
	Returns the file objects for the (fileId, version) pairs in
	list; the order returns is the same order in the list.

	@param l:
	@type list:
	@rtype list
	"""
	raise NotImplementedError

    def getFileContents(self, fileList):
        # troveName, troveVersion, fileId, fileVersion, fileObj

	raise NotImplementedError

    def getTrove(self, troveName, version, flavor):
	"""
	Returns the trove which matches (troveName, version, flavor). If
	the trove does not exist, TroveMissing is raised.

	@param troveName: package name
	@type troveName: str
	@param version: version
	@type version: versions.Version
	@param flavor: flavor
	@type flavor: deps.deps.DependencySet
	@rtype: trove.Package
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
	    except TroveMissing:
		rc.append(None)

	return rc

    def iterAllTroveNames(self, serverName):
	"""
	Returns a list of all of the troves contained in a repository.

        @param serverName: name of the server containing troves
        @type serverName: str
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

    def iterFilesInTroveAncestry(self, troveName, version, flavor):
	"""
        Returns a generator for (fileId, path, version, fileObj) tuples
        for all of the unique file paths ever contained in a trove
        throughout history. Only one tuple will be return per file
        path.  The newest version of each file will be returned.
        """
	raise NotImplementedError

    def walkTroveSet(self, trove, ignoreMissing = True):
	"""
	Generator returns all of the troves included by trove, including
	trove itself.
	"""
	yield trove
	seen = { trove.getName() : [ (trove.getVersion(),
				      trove.getFlavor()) ] }

	troveList = [x for x in trove.iterTroveList()]

	while troveList:
	    (name, version, flavor) = troveList[0]
	    del troveList[0]

	    if seen.has_key(name):
		match = False
		for (ver, fla) in seen[name]:
		    if version == ver and fla == flavor:
			match = True
			break
		if match: continue

		seen[name].append((version, flavor))
	    else:
		seen[name] = [ (version, flavor) ]

	    try:
		trove = self.getTrove(name, version, flavor)

		yield trove

		for (trove, version, flavor) in trove.iterTroveList():
		    troveList += [ x for x in trove.iterTroveList() ]
	    except TroveMissing:
		if not ignoreMissing:
		    raise

class IdealRepository(AbstractTroveDatabase):

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
	@type newBranch: versions.Label
	@param where: Where the branch should be created from
	@type where: versions.Version or versions.Label
	@param troveList: Name of the troves to branch; empty list if all
	troves in the repository should be branched.
	@type troveList: list of str
	"""
	raise NotImplementedError

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
	@type label: versions.Label
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
	@type label: versions.Label
	@rtype: dict of lists
	"""
	raise NotImplementedError

    def getTroveLatestVersion(self, troveName, branch):
	"""
	Returns the version of the latest version of a trove on a particular
	branch. If that branch doesn't exist for the trove, TroveMissing
	is raised. The version returned includes timestamps.

	@param troveName: trove name
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

	@param troveName: trove name
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

    def findTrove(self, labelPath, name, targetFlavor, versionStr = None,
                  acrossRepositories = False, withFiles = True):
	assert(not targetFlavor or 
	       isinstance(targetFlavor, deps.deps.DependencySet))

        if not type(labelPath) == list:
            labelPath = [ labelPath ]

	if not labelPath:
	    # if we don't have a label path, we need a fully qualified
	    # version string; make sure have it
	    if versionStr[0] != "/" and (versionStr.find("/") != -1 or
					 versionStr.find("@") == -1):
		raise TroveNotFound, \
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
            if versionStr and versionStr[0] != "@":
		try:
		    label = versions.Label(versionStr)
                    labelPath = [ label ]
		except versions.ParseError:
		    raise TroveMissing, "invalid version %s" % versionStr
            elif versionStr:
                # just a branch name was specified
                repositories = [ x.getHost() for x in labelPath ]
                labelPath = []
                for repository in repositories:
                    labelPath.append(versions.Label("%s%s" % 
                                                    (repository, versionStr)))

            versionDict = { name : [] }
            for label in labelPath:
                d = self.getTroveLeavesByLabel([name], label)
                if not d[name]:
                    continue
                elif not acrossRepositories:
                    versionDict = d
                    break
                else:
                    for name, versionList in d.iteritems():
                        versionDict[name] += versionList

	    if not versionDict[name]:
		raise TroveNotFound, \
                      ('"%s" was not found in the search path (%s)'
                       %(name, " ".join([ x.asString() for x in labelPath ])))

	elif versionStr[0] != "/" and versionStr.find("/") == -1:
	    # version/release was given
	    try:
		verRel = versions.VersionRelease(versionStr)
	    except versions.ParseError, e:
		raise TroveNotFound, str(e)

            versionDict = { name : [] }
            for label in labelPath:
                d = self.getTroveVersionsByLabel([name], label)
                for version in d[name][:]:
                    if version.trailingVersion() != verRel:
                        d[name].remove(version)

                if not d[name]:
                    continue
                elif not acrossRepositories:
                    versionDict = d
                    break
                else:
                    for name, versionList in d.iteritems():
                        versionDict[name] += versionList

	    if not versionDict[name]:
		raise TroveNotFound, \
		    "version %s of %s is not on found on path %s" % \
		    (versionStr, name, " ".join([x.asString() for x in labelPath]))
	elif versionStr[0] != "/":
	    # partial version string, we don't support this
	    raise TroveNotFound, \
		"incomplete version string %s not allowed" % versionStr
	else:
	    try:
		version = versions.VersionFromString(versionStr)
	    except versions.ParseError, e:
		raise TroveNotFound, str(e)

	    try:
		# XXX
		if version.isBranch():
		    version = self.getTroveLatestVersion(name, version)

		versionDict = { name : [ version ] }
	    except TroveMissing, e:  
		raise TroveNotFound, str(e)

	flavorDict = self.getTroveVersionFlavors(versionDict)
	pkgList = []
	for version in flavorDict[name].iterkeys():
	    for flavor in flavorDict[name][version]:
		if not flavor or (targetFlavor and 
				  targetFlavor.satisfies(flavor)):
		    pkgList.append((name, version, flavor))

	if not pkgList:
	    raise TroveNotFound, "trove %s does not exist" % name

	pkgList = self.getTroves(pkgList, withFiles = withFiles)

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

	@param troveName: trove name
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

    def _storeFileFromContents(self, contents, sha1, restoreContents):
	if restoreContents:
	    self.contentsStore.addFile(contents.get(), 
				       sha1helper.sha1ToString(sha1))
	else:
	    # the file doesn't have any contents, so it must exist
	    # in the data store already; we still need to increment
	    # the reference count for it
	    self.contentsStore.addFileReference(sha1helper.sha1ToString(sha1))

	return 1

    def _removeFileContents(self, sha1):
	self.contentsStore.removeFile(sha1helper.sha1ToString(sha1))

    def _getFileObject(self, sha1):
	return self.contentsStore.openFile(sha1helper.sha1ToString(sha1))

    def _hasFileContents(self, sha1):
	return self.contentsStore.hasFile(sha1helper.sha1ToString(sha1))

    def getFileContents(self, fileList):
        contentList = []

        for item in fileList:
            (troveName, troveVersion, fileId, fileVersion) = item[0:4]
            if len(item) == 5:
                fileObj = item[4]
            else:
                fileObj = self.findFileVersion(troveName, troveVersion, fileId,
                                               fileVersion)
            
            if fileObj:
                cont = filecontents.FromDataStore(self.contentsStore,
                                                  fileObj.contents.sha1(),
                                                  fileObj.contents.size())
            else:
                cont = ""

            contentList.append(cont)

        return contentList

    def __init__(self, path):
	fullPath = path + "/contents"
	util.mkdirChain(fullPath)
	self.contentsStore = datastore.DataStore(fullPath)

class ChangeSetJob:
    """
    ChangeSetJob provides a to-do list for applying a change set; file
    remappings should have been applied to the change set before it gets
    this far. Derivative classes can override these methods to change the
    behavior; for example, if addPackage is overridden no pacakges will
    make it to the database. The same holds for oldFile.
    """

    storeOnlyConfigFiles = False

    def addPackage(self, pkg):
	return self.repos.addPackage(pkg)

    def addPackageDone(self, pkgId):
	self.repos.addPackageDone(pkgId)

    def oldPackage(self, pkg):
	pass

    def oldFile(self, fileId, fileVersion, fileObj):
	pass

    def addFile(self, troveId, fileId, fileObj, path, version):
	self.repos.addFileVersion(troveId, fileId, fileObj, path, version)

    def addFileContents(self, sha1, fileVersion, fileContents, 
		restoreContents, isConfig):
	# Note that the order doesn't matter, we're just copying
	# files into the repository. Restore the file pointer to
	# the beginning of the file as we may want to commit this
	# file to multiple locations.
	self.repos._storeFileFromContents(fileContents, sha1, restoreContents)

    def __init__(self, repos, cs):
	self.repos = repos
	self.cs = cs

	configRestoreList = []
	normalRestoreList = []

	# create the package objects which need to be installed; the
	# file objects which map up with them are created later, but
	# we do need a map from fileId to the path and version of the
	# file we need, so build up a dictionary with that information
	for csPkg in cs.iterNewPackageList():
	    newVersion = csPkg.getNewVersion()
	    old = csPkg.getOldVersion()
	    oldTroveVersion = old
	    pkgName = csPkg.getName()
	    troveFlavor = csPkg.getNewFlavor()

	    if repos.hasTrove(pkgName, newVersion, troveFlavor):
		raise CommitError, \
		       "version %s of %s is already installed" % \
			(newVersion.asString(), csPkg.getName())

	    if old:
		newPkg = repos.getTrove(pkgName, old, csPkg.getOldFlavor(),
					pristine = True)
		newPkg.changeVersion(newVersion)
	    else:
		newPkg = trove.Trove(csPkg.getName(), newVersion,
				     troveFlavor, csPkg.getChangeLog())

	    newFileMap = newPkg.applyChangeSet(csPkg)

	    troveInfo = self.addPackage(newPkg)

	    for (fileId, path, newVersion) in newPkg.iterFileList():
		tuple = newFileMap.get(fileId, None)
		if tuple is not None:
		    (oldPath, oldVersion) = tuple[-2:]
		else:
		    oldVersion = None

		if tuple is None or oldVersion == newVersion:
		    # the file didn't change between versions; we can just
		    # ignore it
		    fileObj = None
		elif oldVersion == newVersion:
		    fileObj = None
		else:
		    diff = cs.getFileChange(fileId)
		    restoreContents = 1
		    if oldVersion:
			oldfile = repos.getFileVersion(fileId, oldVersion)
			fileObj = oldfile.copy()
			fileObj.twm(diff, oldfile)

			if fileObj.hasContents and oldfile.hasContents and \
			   fileObj.contents.sha1() == oldfile.contents.sha1() and \
			   not (fileObj.flags.isConfig() and not 
						    oldfile.flags.isConfig()):
			    restoreContents = 0
		    else:
			fileObj = files.ThawFile(diff, fileId)
			oldfile = None

		self.addFile(troveInfo, fileId, fileObj, path, newVersion)

		# files with contents need to be tracked so we can stick
		# there contents in the archive "soon"; config files need
		# extra magic for tracking since we may have to merge
		# contents
		if not fileObj or not fileObj.hasContents or		\
			    not restoreContents:
		    # this means there are no contents to restore
		    continue
		if self.storeOnlyConfigFiles and not fileObj.flags.isConfig():
		    continue

		# we already have the contents of this file... we can go
		# ahead and restore it reusing those contents
		if repos._hasFileContents(fileObj.contents.sha1()):
		    # if we already have the file in the data store we can
		    # get the contents from there
   		    fileContents = filecontents.FromDataStore(
 				     repos.contentsStore, 
 				     fileObj.contents.sha1(), 
 				     fileObj.contents.size())
 		    contType = changeset.ChangedFileTypes.file
 		    self.addFileContents(fileObj.contents.sha1(), newVersion, 
 					 fileContents, restoreContents, 
 					 fileObj.flags.isConfig())
		elif fileObj.flags.isConfig():
		    tup = (fileId, fileObj, oldPath, oldfile, pkgName,
			   oldTroveVersion, troveFlavor, newVersion, 
			   oldVersion, restoreContents)
		    configRestoreList.append(tup)
		else:
		    tup = (fileId, fileObj.contents.sha1(), newVersion, 
			   restoreContents)
		    normalRestoreList.append(tup)

	    del newFileMap
	    self.addPackageDone(troveInfo)

	configRestoreList.sort()
	normalRestoreList.sort()

	for (fileId, fileObj, oldPath, oldfile, pkgName, oldTroveVersion,
	     troveFlavor, newVersion, oldVersion, restoreContents) in \
							configRestoreList:
            if cs.configFileIsDiff(fileId):
                (contType, fileContents) = cs.getFileContents(fileId)

		assert(fileObj.flags.isConfig())
		# the content for this file is in the form of a
		# diff, which we need to apply against the file in
		# the repository
		assert(oldVersion)
		sha1 = oldfile.contents.sha1()

		f = self.repos.getFileContents([(pkgName, 
			    oldTroveVersion, fileId, oldVersion, 
                            oldfile)])[0].get()

		oldLines = f.readlines()
		del f
		diff = fileContents.get().readlines()
		(newLines, failedHunks) = patch.patch(oldLines, 
						      diff)
		fileContents = filecontents.FromString(
						"".join(newLines))

		if failedHunks:
		    fileContents = filecontents.WithFailedHunks(
					fileContents, failedHunks)
            else:
                fileContents = filecontents.FromChangeSet(cs, fileId)

	    self.addFileContents(fileObj.contents.sha1(), newVersion, 
				 fileContents, restoreContents, 1)

        # normalRestoreList is empty if storeOnlyConfigFiles
	normalRestoreList.sort()
        ptrRestores = []
	for (fileId, sha1, version, restoreContents) in normalRestoreList:
	    (contType, fileContents) = cs.getFileContents(fileId)
            if contType == changeset.ChangedFileTypes.ptr:
                ptrRestores.append(sha1)
                continue

	    assert(contType == changeset.ChangedFileTypes.file)
	    self.addFileContents(sha1, version, fileContents, restoreContents,
				 0)

        for sha1 in ptrRestores:
	    self.addFileContents(sha1, None, None, False, 0)

	del configRestoreList
	del normalRestoreList

	for (pkgName, version, flavor) in cs.getOldPackageList():
	    pkg = self.repos.getTrove(pkgName, version, flavor)
	    self.oldPackage(pkg)

	    for (fileId, path, version) in pkg.iterFileList():
		file = self.repos.getFileVersion(fileId, version)
		self.oldFile(fileId, version, file)

class RepositoryError(Exception):
    """Base class for exceptions from the system repository"""

class MethodNotSupported(RepositoryError):
    """Attempt to call a server method which does not exist"""

class TroveNotFound(Exception):
    """Raised when findTrove failes"""

# XXX deprecated exception name
PackageNotFound = TroveNotFound

class OpenError(RepositoryError):
    """Error occured opening the repository"""

class CommitError(RepositoryError):
    """Error occured commiting a trove"""

class DuplicateBranch(RepositoryError):
    """Error occured commiting a trove"""

class TroveMissing(RepositoryError):
    troveType = "trove"
    def __str__(self):
        if type(self.version) == list:
            return ('%s %s does not exist for any of '
                    'the following labels:\n    %s' %
                    (self.troveType, self.troveName,
                     "\n    ".join([x.asString() for x in self.version])))
        elif self.version:
            if self.version.isBranch():
                return ("%s %s does not exist on branch %s" % \
                    (self.troveType, self.troveName, self.version.asString()))

            return "version %s of %s %s does not exist" % \
                (self.version.asString(), self.troveType, self.troveName)
	else:
	    return "%s %s does not exist" % (self.troveType, self.troveName)

    def __init__(self, troveName, version = None):
	"""
	Initializes a TroveMissing exception.

	@param troveName: trove which could not be found
	@type troveName: str
	@param version: version of the trove which does not exist
	@type version: versions.Version
	"""
	self.troveName = troveName
	self.version = version
        if troveName.startswith('group-'):
            self.type = 'group'
        elif troveName.startswith('fileset-'):
            self.type = 'fileset'
        elif troveName.find(':') != -1:
            self.type = 'component'
        else:
            self.type = 'package'

