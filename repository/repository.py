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

# defines the Conary repository

import changeset
import datastore
import deps.deps
import files
from lib import patch
from lib import sha1helper
import sha
import tempfile
import trove
from lib import util
from lib import openpgpkey
from lib import openpgpfile
import versions

import filecontents

class AbstractTroveDatabase:

    def commitChangeSet(self, cs):
	raise NotImplementedError

    def findTrove(self, labelPath, (name, versionstr, flavor), 
                  defaultFlavor = None,
                  acrossRepositories = False, affinityDatabase = None):
	"""
	Looks up a trove in the repository based on the name and
	version provided. If any errors occur, TroveNotFound is
	raised with an appropriate error message. Multiple matches
	could be found if versionStr refers to a label, or if 
        affinityDatabase is used and there are multiple matches.

	@param labelPath: Path of labels to look on if no branch
	is specified. If only a branch name is given (not a complete label),
	the repository names from these labels are used as the repository
	name for the branch name to form a complete label.
	@param name: Trove name
	@type name: str
	@param versionStr: Trove version
	@type versionStr: str
	@param flavor: only troves compatible with this flavor will be returned
	@type flavor: deps.DependencySet
        @param acrossRepositories: normally findTrove only returns matches
        from a single repository (the first one with a match). if this is
        set it continues searching through all repositories
        @type acrossRepositories: boolean
	@rtype: list of (troveName, troveVersion, troveFlavor)
	"""
	raise NotImplementedError

    def getFileVersion(self, pathId, fileId, version, withContents = 0):
	"""
	Returns the file object for the given (pathId, fileId, version).
	"""
	raise NotImplementedError

    def getFileVersions(self, l):
	"""
	Returns the file objects for the (pathId, fileId, version) pairs in
	list; the order returns is the same order in the list.

	@param l:
	@type l: list
	@rtype list
	"""
	raise NotImplementedError

    def getFileContents(self, fileList):
        # troveName, troveVersion, pathId, fileVersion, fileObj

	raise NotImplementedError

    def getTrove(self, troveName, version, flavor):
	"""
	Returns the trove which matches (troveName, version, flavor). If
	the trove does not exist, TroveMissing is raised.

	@param troveName: trove name
	@type troveName: str
	@param version: version
	@type version: versions.Version
	@param flavor: flavor
	@type flavor: deps.deps.DependencySet
	@rtype: trove.Trove
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
	Returns a generator for (pathId, path, fileId, version) tuples for all
	of the files in the trove. This is equivlent to trove.iterFileList(),
	but if withFiles is set this is *much* more efficient.

	@param withFiles: if set, the file object for the file is 
	created and returned as the fourth element in the tuple.
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
                trv = self.getTrove(name, version, flavor)

                yield trv

                troveList += [ x for x in trv.iterTroveList() ]
	    except TroveMissing:
		if not ignoreMissing:
		    raise
	    except KeyError:
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

    def getAllTroveLeaves(self, troveNameList):
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


    def getAllTroveFlavors(self, troveDict):
	"""
	Converts a dictionary of the format retured by getAllTroveLeaves()
	to contain dicts of { version : flavorList } sets instead of 
	containing lists of versions. The flavorList lists all of the
        flavors available for that vesrion of the trove.

	@type troveDict: dict
	@rtype: dict
	"""
	raise NotImplementedError

    def queryMerge(self, target, source):
        """
        Merges the result of getTroveLatestVersions (and friends) into
        target.
        """
        for (name, verDict) in source.iteritems():
            if not target.has_key(name):
                target[name] = verDict
            else:
                for (version, flavorList) in verDict.iteritems():
                    if not target[name].has_key(version):
                        target[name][version] = flavorList
                    else:
                        target[name][version] += flavorList

class AbstractRepository(IdealRepository):
    ### Trove access functions

    def hasTroveByName(self, troveName):
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

class ChangeSetJob:
    """
    ChangeSetJob provides a to-do list for applying a change set; file
    remappings should have been applied to the change set before it gets
    this far. Derivative classes can override these methods to change the
    behavior; for example, if addTrove is overridden no pacakges will
    make it to the database. The same holds for oldFile.
    """

    storeOnlyConfigFiles = False

    def addTrove(self, oldTroveSpec, trove):
	return self.repos.addTrove(trove)

    def addTroveDone(self, troveId):
	self.repos.addTroveDone(troveId)

    def oldTrove(self, trove):
	pass

    def oldFile(self, pathId, fileVersion, fileObj):
	pass

    def addFileContents(self, sha1, fileVersion, fileContents, 
                        restoreContents, isConfig, precompressed = False):
	# Note that the order doesn't matter, we're just copying
	# files into the repository. Restore the file pointer to
	# the beginning of the file as we may want to commit this
	# file to multiple locations.
	self.repos._storeFileFromContents(fileContents, sha1, restoreContents,
                                          precompressed = precompressed)

    def checkTroveSignatures(self, trv, keyCache = None):
        if keyCache is None:
            keyCache = openpgpkey.getKeyCache()
        for fingerprint, timestamp, sig in trv.troveInfo.sigs.digitalSigs.iter():
            pubKey = keyCache.getPublicKey(fingerprint)
            if pubKey.isRevoked():
                raise openpgpfile.IncompatibleKey('Key %s is revoked'
                                                  %pubKey.getFingerprint())
            expirationTime = pubKey.getTimestamp()
            if expirationTime and expirationTime < timestamp:
                raise openpgpfile.IncompatibleKey('Key %s is expired'
                                                  %pubKey.getFingerprint())
        res = trv.verifyDigitalSignatures(keyCache=keyCache)
        if len(res[1]):
            raise openpgpfile.KeyNotFound('Repository does not recognize '
                                          'key: %s'% res[1][0])

    def __init__(self, repos, cs, fileHostFilter = [], callback = None,
                 resetTimestamps = False, keyCache = None):
	self.repos = repos
	self.cs = cs

	configRestoreList = []
	normalRestoreList = []

	newList = [ x for x in cs.iterNewTroveList() ]

        if resetTimestamps:
            # This depends intimiately on the versions cache. We don't
            # change the timestamps on each version, because the cache
            # ensures they are all a single underlying object. Slick,
            # but brittle?
            updated = {}

            for csTrove in newList:
                ver = csTrove.getNewVersion()
                if ver in updated:
                    pass
                else:
                    oldVer = ver.copy()
                    ver.trailingRevision().resetTimeStamp()
                    updated[oldVer] = ver

            del updated

	# create the trove objects which need to be installed; the
	# file objects which map up with them are created later, but
	# we do need a map from pathId to the path and version of the
	# file we need, so build up a dictionary with that information
	for i, csTrove in enumerate(newList):
	    if callback:
		callback.creatingDatabaseTransaction(i + 1, len(newList))

	    newVersion = csTrove.getNewVersion()
	    oldTroveVersion = csTrove.getOldVersion()
            oldTroveFlavor = csTrove.getOldFlavor()
	    troveName = csTrove.getName()
	    troveFlavor = csTrove.getNewFlavor()

	    if repos.hasTrove(troveName, newVersion, troveFlavor):
		raise CommitError, \
		       "version %s of %s is already installed" % \
			(newVersion.asString(), csTrove.getName())

	    if oldTroveVersion:
		newTrove = repos.getTrove(troveName, oldTroveVersion, 
                                          csTrove.getOldFlavor(), 
                                          pristine = True)
		newTrove.changeVersion(newVersion)
	    else:
		newTrove = trove.Trove(csTrove.getName(), newVersion,
                                        troveFlavor, csTrove.getChangeLog())

	    newFileMap = newTrove.applyChangeSet(csTrove)
            self.checkTroveSignatures(newTrove, keyCache=keyCache)

	    troveInfo = self.addTrove(
                    (troveName, oldTroveVersion, oldTroveFlavor), newTrove)

	    for (pathId, path, fileId, newVersion) in newTrove.iterFileList():
		tuple = newFileMap.get(pathId, None)
		if tuple is not None:
		    (oldPath, oldFileId, oldVersion) = tuple[-3:]
		else:
		    oldVersion = None
                    oldFileId = None

                if fileHostFilter and \
                 newVersion.branch().label().getHost() not in fileHostFilter:
                    fileObj = None
                    fileStream = None
		elif tuple is None or (oldVersion == newVersion and
                                       oldFileId == fileId):
		    # the file didn't change between versions; we can just
		    # ignore it
		    fileObj = None
                    fileStream = None
		else:
		    diff = cs.getFileChange(oldFileId, fileId)
                    if diff is None:
                        # XXX we really should check to make sure this file
                        # is already present rather then just blindly
                        # skipping over it. we do make sure fileHostFilter
                        # is empty though, keeping this skip from occuring
                        # on databases
                        fileObj = None
                        fileStream = None
                        if not fileHostFilter:
                            raise KeyError
                    else:
                        restoreContents = 1
                        if oldVersion:
                            oldfile = repos.getFileVersion(pathId, oldFileId,
                                                           oldVersion)
                            if diff[0] == "\x01":
                                # stored as a diff (the file type is the same)
                                fileObj = oldfile.copy()
                                fileObj.twm(diff, oldfile)
                                assert(fileObj.pathId() == pathId)
                                fileStream = fileObj.freeze()
                            else:
                                fileObj = files.ThawFile(diff, pathId)
                                fileStream = diff

                            if fileObj.hasContents and oldfile.hasContents and \
                               fileObj.contents.sha1() == oldfile.contents.sha1() and \
                               not (fileObj.flags.isConfig() and not 
                                                        oldfile.flags.isConfig()):
                                restoreContents = 0
                        else:
                            fileObj = files.ThawFile(diff, pathId)
                            fileStream = diff
                            oldfile = None

                if fileObj and fileObj.fileId() != fileId:
                    raise trove.TroveIntegrityError, \
                          "fileObj.fileId() != fileId in changeset"
                self.repos.addFileVersion(troveInfo, pathId, fileObj, path, 
                                          fileId, newVersion, 
                                          fileStream = fileStream)

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
 				     fileObj.contents.sha1())
 		    contType = changeset.ChangedFileTypes.file
 		    self.addFileContents(fileObj.contents.sha1(), newVersion, 
 					 fileContents, restoreContents, 
 					 fileObj.flags.isConfig())
		elif fileObj.flags.isConfig():
		    tup = (pathId, fileObj, oldPath, oldfile, troveName,
			   oldTroveVersion, troveFlavor, newVersion, 
                           fileId, oldVersion, oldFileId, restoreContents)
		    configRestoreList.append(tup)
		else:
		    tup = (pathId, fileObj.contents.sha1(), newVersion, 
			   restoreContents)
		    normalRestoreList.append(tup)

	    del newFileMap
	    self.addTroveDone(troveInfo)

	configRestoreList.sort()
	normalRestoreList.sort()

	for (pathId, fileObj, oldPath, oldfile, troveName, oldTroveVersion,
	     troveFlavor, newVersion, newFileId, oldVersion, 
             oldFileId, restoreContents) in configRestoreList:
            if cs.configFileIsDiff(pathId):
                (contType, fileContents) = cs.getFileContents(pathId)

		assert(fileObj.flags.isConfig())
		# the content for this file is in the form of a
		# diff, which we need to apply against the file in
		# the repository
		assert(oldVersion)
		sha1 = oldfile.contents.sha1()

		f = self.repos.getFileContents(
                                [(oldFileId, oldVersion, oldfile)])[0].get()

		oldLines = f.readlines()
		del f
		diff = fileContents.get().readlines()
		(newLines, failedHunks) = patch.patch(oldLines, 
						      diff)
		fileContents = filecontents.FromString(
						"".join(newLines))

		assert(not failedHunks)
            else:
                # config files are not always available compressed (due
                # to the config file cache)
                fileContents = filecontents.FromChangeSet(cs, pathId)

	    self.addFileContents(fileObj.contents.sha1(), newVersion, 
				 fileContents, restoreContents, 1)

        # normalRestoreList is empty if storeOnlyConfigFiles
	normalRestoreList.sort()
        ptrRestores = []
	for (pathId, sha1, version, restoreContents) in normalRestoreList:
	    (contType, fileContents) = cs.getFileContents(pathId,
                                                          compressed = True)
            if contType == changeset.ChangedFileTypes.ptr:
                ptrRestores.append(sha1)
                continue

	    assert(contType == changeset.ChangedFileTypes.file)
	    self.addFileContents(sha1, version, fileContents, restoreContents,
				 0, precompressed = True)

        for sha1 in ptrRestores:
	    self.addFileContents(sha1, None, None, False, 0)

	del configRestoreList
	del normalRestoreList

	for (troveName, version, flavor) in cs.getOldTroveList():
	    trv = self.repos.getTrove(troveName, version, flavor)
	    self.oldTrove(trv)

	    for (pathId, path, fileId, version) in trv.iterFileList():
		file = self.repos.getFileVersion(pathId, fileId, version)
		self.oldFile(pathId, version, file)

class RepositoryError(Exception):
    """Base class for exceptions from the system repository"""

class MethodNotSupported(RepositoryError):
    """Attempt to call a server method which does not exist"""

class TroveNotFound(Exception):
    """Raised when findTrove failes"""

class RepositoryLocked(RepositoryError):
    def __str__(self):
        return 'The repository is currently busy.  Try again in a few moments.'

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
            if isinstance(self.version, versions.Branch):
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

