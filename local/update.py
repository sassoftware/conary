#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved

"""
Handles all updates to the file system; files should never get changed
on the filesystem except by this module!

@var MERGE: Flag constant value.  If set, merge is attempted,
otherwise the changes from the changeset are used (this is for
rollbacks)
@var REPLACEFILES: Flag constant value.  If set, a file that is in
the way of a newly created file will be overwritten.  Otherwise an error
is produced.
"""

import changeset
import errno
import filecontents
import files
import log
import os
import package
import patch
import stat
import versions

MERGE = 1 << 0
REPLACEFILES = 1 << 1
        
class FilesystemJob:
    """
    Represents a set of actions which need to be applied to the filesystem.
    This is kept very simple to mimize the chance of mistakes or errors.
    """

    def _rename(self, oldPath, newPath, msg):
	self.renames.append((oldPath, newPath, msg))

    def _restore(self, fileObj, target, contents, msg):
	self.restores.append((fileObj, target, contents, msg))

    def _remove(self, fileObj, target, msg):
	self.removes[target] = (fileObj, msg)

    def _createFile(self, target, str, msg):
	self.newFiles.append((target, str, msg))

    def apply(self):
	for (oldPath, newPath, msg) in self.renames:
	    os.rename(oldPath, newPath)
	    log.debug(msg)

	for (fileObj, target, contents, msg) in self.restores:
	    fileObj.restore(contents, target, contents != None)
	    log.debug(msg)

	paths = self.removes.keys()
	paths.sort()
	paths.reverse()
	for target in paths:
	    (fileObj, msg) = self.removes[target]
	    fileObj.remove(target)
	    log.debug(msg)

	for (target, str, msg) in self.newFiles:
	    os.unlink(target)
	    f = open(target, "w")
	    f.write(str)
	    f.close()
	    log.debug(msg)

    def getErrorList(self):
	return self.errors

    def getNewPackageList(self):
	return self.newPackages

    def _singlePackage(self, repos, pkgCs, changeSet, basePkg, fsPkg, root,
		       flags):
	"""
	Build up the todo list for applying a single package to the
	filesystem. Returns a package object which represents what will
	end up in the filsystem after this object's apply() method is
	called.

	@param repos: the repository the files for basePkg are stored in
	@type repos: repository.Repository
	@param pkgCs: the package changeset to apply to the filesystem
	@type pkgCs: package.PackageChangeSet
	@param changeSet: the changeset pkgCs is part of
	@type changeSet: changeset.ChangeSet
	@param basePkg: the package the stuff in the filesystem came from
	@type basePkg: package.Package
	@param fsPkg: the package representing what's in the filesystem now
	@type fsPkg: package.Package
	@param root: root directory to apply changes to (this is ignored for
	source management, which uses the cwd)
	@type root: str
	@param flags: flags which modify update behavior.  See L{update}
        module variable summary for flags definitions.
	@type flags: int bitfield
	@rtype: package.Package
	"""
	if basePkg:
	    assert(pkgCs.getOldVersion().equal(basePkg.getVersion()))
	fullyUpdated = 1
	cwd = os.getcwd()

	if fsPkg:
	    fsPkg = fsPkg.copy()
	else:
	    fsPkg = package.Package(pkgCs.getName(), versions.NewVersion())

	for (fileId, headPath, headFileVersion) in pkgCs.getNewFileList():
	    if headPath[0] == '/':
		headRealPath = root + headPath
	    else:
		headRealPath = cwd + "/" + headPath

	    headFile = files.FileFromInfoLine(changeSet.getFileChange(fileId),
					      fileId)

            try:
                s = os.lstat(headRealPath)
                # if this file is a directory and the file on the file
                # system is a directory, we're OK
                if (isinstance(headFile, files.Directory)
                    and stat.S_ISDIR(s.st_mode)):
                    # FIXME: this isn't the right directory handling
                    # we will want to set ownership/permissions if
                    # they don't conflict with any already-installed package
                    continue
                elif not flags & REPLACEFILES:
                    self.errors.append("%s is in the way of a newly " 
                                       "created file" % headRealPath)
                    fullyUpdated = 0
                    continue
            except OSError:
                # the path doesn't exist, carry on with the restore
                pass

	    if headFile.hasContents:
		headFileContents = changeSet.getFileContents(fileId)[1]
	    else:
		headFileContents = None
	    self._restore(headFile, headRealPath, headFileContents,
	                  "creating %s" % headRealPath)
	    fsPkg.addFile(fileId, headPath, headFileVersion)

	for fileId in pkgCs.getOldFileList():
	    (path, version) = basePkg.getFile(fileId)
	    if not fsPkg.hasFile(fileId):
		log.debug("%s has already been removed" % path)
		continue

	    if path[0] == '/':
		realPath = root + path
	    else:
		realPath = cwd + "/" + path

	    if flags & MERGE:
		try:
		    # don't remove files if they've been changed locally
		    localFile = files.FileFromFilesystem(realPath, fileId)
		except OSError, exc:
		    # it's okay if the file is missing, it means we all agree
		    if exc.errno == errno.ENOENT:
			fsPkg.removeFile(fileId)
			continue
		    else:
			raise

	    oldFile = repos.getFileVersion(fileId, version)

	    if not oldFile.same(localFile, ignoreOwner = True):
		self.errors.append("%s has changed but has been removed "
				   "on head" % path)
		continue

	    self._remove(oldFile, realPath, "removing %s" % path)	
	    fsPkg.removeFile(fileId)

	for (fileId, headPath, headFileVersion) in pkgCs.getChangedFileList():
	    (fsPath, fsVersion) = fsPkg.getFile(fileId)
	    if fsPath[0] == "/":
		rootFixup = root
	    else:
		rootFixup = cwd + "/"

	    pathOkay = 1
	    contentsOkay = 1
	    finalPath = fsPath
	    # if headPath is none, the name hasn't changed in the repository
	    if headPath and headPath != fsPath:
		# the paths are different; if one of them matches the one
		# from the old package, take the other one as it is the one
		# which changed
		if basePkg.hasFile(fileId):
		    basePath = basePkg.getFile(fileId)[0]
		else:
		    basePath = None

		if (not flags & MERGE) or fsPath == basePath :
		    # the path changed in the repository, propage that change
		    self._rename(rootFixup + fsPath, rootFixup + headPath,
		                 "renaming %s to %s" % (fsPath, headPath))

		    fsPkg.addFile(fileId, headPath, fsVersion)
		    finalPath = headPath
		else:
		    pathOkay = 0
		    finalPath = fsPath	# let updates work still
		    self.errors.append("path conflict for %s (%s on head)" % 
                                       (fsPath, headPath))

	    realPath = rootFixup + finalPath

	    # headFileVersion is None for renames
	    if headFileVersion:
		# FIXME we should be able to inspect headChanges directly
		# to see if we need to go into the if statement which follows
		# this rather then having to look up the file from the old
		# pacakge for every file which has changed
	    
		fsFile = files.FileFromFilesystem(realPath, fileId)
		
		if not basePkg.hasFile(fileId):
		    # a file which was not in the base package was created
		    # on both the head of the branch and in the filesystem;
		    # this can happen during source management
		    self.errors.append("new file %s conflicts with file on "
                                       "head of branch" % realPath)
		    contentsOkay = 0
		else:
		    baseFileVersion = basePkg.getFile(fileId)[1]
		    (baseFile, baseFileContents) = repos.getFileVersion(fileId, 
					baseFileVersion, withContents = 1)
		
		headChanges = changeSet.getFileChange(fileId)
		headFile = baseFile.copy()
		headFile.applyChange(headChanges)
		fsFile.isConfig(headFile.isConfig())
		fsChanges = fsFile.diff(baseFile)

	    attributesChanged = False

	    if (basePkg and headFileVersion
                and not fsFile.same(headFile, ignoreOwner = True)):
		# something has changed for the file
		if flags & MERGE:
		    (conflicts, mergedChanges) = files.mergeChangeLines(
						    headChanges, fsChanges)
		    if mergedChanges and (not conflicts or
					  files.contentConflict(mergedChanges)):
			fsFile.applyChange(mergedChanges, ignoreContents = 1)
			attributesChanged = True
		    else:
			contentsOkay = False
			self.errors.append("file attributes conflict for %s"
						% realPath)
		else:
		    fsFile.applyChange(headChanges, ignoreContents = 1)
		    attributesChanged = True

	    else:
		conflicts = True
		mergedChanges = None

	    beenRestored = False

	    if headFileVersion and headFile.hasContents and \
	       fsFile.hasContents and fsFile.sha1() != headFile.sha1():
		# the contents have changed... let's see what to do

		# get the contents if the version on head has contents, and
		# either
		#	1. the version from the base package doesn't have 
		#	   contents, or
		#	2. the file changed between head and base
		# (if both are false, no contents would have been saved for
		# this file)
		if (headFile.hasContents
                    and (not baseFile.hasContents
                         or headFile.sha1() != baseFile.sha1())):
		    (headFileContType, headFileContents) = \
			    changeSet.getFileContents(fileId)
		else:
		    headFileContents = None

		if (not flags & MERGE) or fsFile.same(baseFile, ignoreOwner = True):
		    # the contents changed in just the repository, so take
		    # those changes
		    if headFileContType == changeset.ChangedFileTypes.diff:
			baseLines = repos.pullFileContentsObject(
						baseFile.sha1()).readlines()
			diff = headFileContents.get().readlines()
			(newLines, failedHunks) = patch.patch(baseLines, diff)
			assert(not failedHunks)
			headFileContents = \
			    filecontents.FromString("".join(newLines))

		    self._restore(fsFile, realPath, headFileContents,
                                  "replacing %s with contents "
                                  "from repository" % realPath)
		    beenRestored = True
		elif headFile.same(baseFile, ignoreOwner = True):
		    # it changed in just the filesystem, so leave that change
		    log.debug("preserving new contents of %s" % realPath)
		elif fsFile.isConfig() or headFile.isConfig():
		    # it changed in both the filesystem and the repository; our
		    # only hope is to generate a patch for what changed in the
		    # repository and try and apply it here
		    if headFileContType != changeset.ChangedFileTypes.diff:
			self.errors.append("unexpected content type for %s" % 
						realPath)
			contentsOkay = 0
		    else:
			cur = open(realPath, "r").readlines()
			diff = headFileContents.get().readlines()
			(newLines, failedHunks) = patch.patch(cur, diff)

			cont = filecontents.FromString("".join(newLines))
			self._restore(fsFile, realPath, cont,
			      "merging changes from repository into %s" % 
			      realPath)
			beenRestored = True

			if failedHunks:
			    self._createFile(
                                realPath + ".conflicts", 
                                failedHunks.asString(),
                                "conflicts from merging changes from " 
                                "head into %s saved as %s.conflicts" % 
                                (realPath, realPath))

			contentsOkay = 1
		else:
		    self.errors.append("file contents conflict for %s" % realPath)
		    contentsOkay = 0

	    if attributesChanged and not beenRestored:
		self._restore(fsFile, realPath, None,
		      "merging changes from repository into %s" % 
		      realPath)

	    if pathOkay and contentsOkay:
		# XXX this doesn't even attempt to merge file permissions
		# and such; the good part of that is differing owners don't
		# break things
		if not headFileVersion:
		    headFileVersion = fsPkg.getFile(fileId)[1]
		fsPkg.addFile(fileId, finalPath, headFileVersion)
	    else:
		fullyUpdated = 0

	if fullyUpdated:
	    fsPkg.changeVersion(pkgCs.getNewVersion())

	return fsPkg

    def __init__(self, repos, changeSet, fsPkgDict, root, flags = MERGE):
	"""
	Constructs the job for applying a change set to the filesystem.

	@param repos: the repository the current package and file information 
	is in
	@type repos: repository.Repository
	@param changeSet: the changeset to apply to the filesystem
	@type changeSet: changeset.ChangeSet
	@param fsPkgDict: dictionary mapping a package name to the package
	object representing what's currently stored in the filesystem
	@type fsPkgDict: dict of package.Package
	@param root: root directory to apply changes to (this is ignored for
	source management, which uses the cwd)
	@type root: str
	@param flags: flags which modify update behavior.  See L{update}
        module variable summary for flags definitions.
	@type flags: int bitfield
	"""
	self.renames = []
	self.restores = []
	self.removes = {}
	self.newPackages = []
	self.errors = []
	self.newFiles = []

	for pkgCs in changeSet.getNewPackageList():
	    # skip over empty change sets
	    if (not pkgCs.getNewFileList() and not pkgCs.getOldFileList()
                and not pkgCs.getChangedFileList()):
		continue

	    name = pkgCs.getName()
	    old = pkgCs.getOldVersion()
	    if old:
		basePkg = repos.getPackageVersion(name, old)
		pkg = self._singlePackage(repos, pkgCs, changeSet, basePkg, 
					  fsPkgDict[name], root, flags)
	    else:
		pkg = self._singlePackage(repos, pkgCs, changeSet, None, 
					  None, root, flags)

	    self.newPackages.append(pkg)

	for (name, oldVersion) in changeSet.getOldPackageList():
	    oldPkg = repos.getPackageVersion(name, oldVersion)
	    for (fileId, (path, version)) in oldPkg.iterFileList():
		fileObj = repos.getFileVersion(fileId, version)
		self._remove(fileObj, root + path,
			     "removing %s" % root + path)

def _localChanges(repos, changeSet, curPkg, srcPkg, newVersion, root = ""):
    """
    Populates a change set against the files in the filesystem and builds
    a package object which describes the files installed.  The return
    is a tuple with a boolean saying if anything changes and a package
    reflecting what's in the filesystem; the changeSet is updated as a
    side effect.

    @param repos: Repository this directory is against.
    @type repos: repository.Repository
    @param changeSet: Changeset to update with information for this package
    @type changeSet: changeset.ChangeSet
    @param curPkg: Package which is installed
    @type curPkg: package.Package
    @param srcPkg: Package to generate the change set against
    @type srcPkg: package.Package
    @param newVersion: version to use for the newly created package
    @type newVersion
    @param root: root directory the files are in (ignored for sources, which
    are assumed to be in the current directory)
    @type root: str
    """

    newPkg = curPkg.copy()
    newPkg.changeVersion(newVersion)

    for (fileId, (path, version)) in newPkg.iterFileList():
	if path[0] == '/':
	    realPath = root + path
	else:
	    realPath = os.getcwd() + "/" + path

	try:
	    os.lstat(realPath)
	except OSError:
	    log.error("%s is missing (use remove if this is intentional)" 
		% path)
	    return None

	if srcPkg and srcPkg.hasFile(fileId):
	    srcFileVersion = srcPkg.getFile(fileId)[1]
	    srcFile = repos.getFileVersion(fileId, srcFileVersion)
	else:
	    srcFile = None

	f = files.FileFromFilesystem(realPath, fileId,
				     possibleMatch = srcFile)

	if path.endswith(".recipe"):
	    f.isConfig(set = True)

	if not srcPkg or not srcPkg.hasFile(fileId):
	    # if we're committing against head, this better be a new file.
	    # if we're generating a diff against someplace else, it might not 
	    # be.
	    assert(srcPkg or isinstance(version, versions.NewVersion))
	    # new file, so this is easy
	    changeSet.addFile(fileId, None, newVersion, f.infoLine())
	    newPkg.addFile(fileId, path, newVersion)

	    if f.hasContents:
		newCont = filecontents.FromFilesystem(realPath)
		changeSet.addFileContents(fileId,
					  changeset.ChangedFileTypes.file,
					  newCont)
	    continue

	oldVersion = srcPkg.getFile(fileId)[1]	
	(oldFile, oldCont) = repos.getFileVersion(fileId, oldVersion,
						  withContents = 1)
        if not f.same(oldFile, ignoreOwner = True):
	    newPkg.addFile(fileId, path, newVersion)

	    (filecs, hash) = changeset.fileChangeSet(fileId, oldFile, f)
	    changeSet.addFile(fileId, oldVersion, newVersion, filecs)
	    if hash:
		newCont = filecontents.FromFilesystem(realPath)
		(contType, cont) = changeset.fileContentsDiff(oldFile, oldCont,
                                                              f, newCont)
						
		changeSet.addFileContents(fileId, contType, cont)

    (csPkg, filesNeeded, pkgsNeeded) = newPkg.diff(srcPkg)
    assert(not pkgsNeeded)
    changeSet.newPackage(csPkg)

    if (csPkg.getOldFileList() or csPkg.getChangedFileList()
        or csPkg.getNewFileList()):
	foundDifference = 1
    else:
	foundDifference = 0

    return (foundDifference, newPkg)

def buildLocalChanges(repos, pkgList, root = ""):
    """
    Builds a change set against a set of files currently installed
    and builds a package objects which describes the files installed.
    The return is a changeset and a list of tuples, each with a boolean 
    saying if anything changed for a package a package reflecting what's
    in the filesystem for that package.

    @param repos: Repository this directory is against.
    @type repos: repository.Repository
    @param pkgList: Specifies which pacakage to work on, and is a list
    of (curPkg, srcPkg, newVersion) tuples as defined in the parameter
    list for _localChanges()
    @param root: root directory the files are in (ignored for sources, which
    are assumed to be in the current directory)
    @type root: str
    """

    changeSet = changeset.ChangeSet()
    returnList = []
    for (curPkg, srcPkg, newVersion) in pkgList:
	result = _localChanges(repos, changeSet, curPkg, srcPkg, newVersion, 
			       root)
        if result is None:
            # an error occurred
            return None
	returnList.append(result)

    return (changeSet, returnList)
