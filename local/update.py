#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved

"""
Handles all updates to the file system; files should never get changed
on the filesystem except by this module!
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

def applyChangeSet(repos, changeSet, fsPkgDict, root):
    """
    Applies a change set to the filesystem.

    @param repos: the repository the current package and file information is in
    @type repos: repository.Repository
    @param changeSet: the changeset to apply to the filesystem
    @type changeSet: changeset.ChangeSet
    @param fsPkgDict: dictionary mapping a package name to the package
    object representing what's currently stored in the filesystem
    @type fsPkgDict: dict of package.Package
    @param root: root directory to apply changes to (this is ignored for
    source management, which uses the cwd)
    @type root: str
    """
    for pkgCs in changeSet.getNewPackageList():
	# skip over empty change sets
	if not pkgCs.getNewFileList() and not pkgCs.getOldFileList() and \
	   not pkgCs.getChangedFileList():
	    continue

	name = pkgCs.getName()
	old = pkgCs.getOldVersion()
	if old:
	    basePkg = repos.getPackageVersion(name, old)
	    _applyPackageChangeSet(repos, pkgCs, changeSet, basePkg, 
				   fsPkgDict[name], root)
	else:
	    _applyPackageChangeSet(repos, pkgCs, changeSet, None, None, root)

def _applyPackageChangeSet(repos, pkgCs, changeSet, basePkg, fsPkg, root):
    """
    Apply a single package change set to the filesystem. Returns a package
    object which specifies what's left on the system. 

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
	    if not isinstance(headFile, files.Directory) or \
	       not stat.S_ISDIR(s.st_mode):
		log.error("%s is in the way of a newly created file" % 
			  headRealPath)
		fullyUpdated = 0

	    # FIXME: this isn't the right directory handling
		
	    continue
	except OSError:
	    pass

	log.debug("creating %s" % headRealPath)

	if headFile.hasContents:
	    headFileContents = changeSet.getFileContents(fileId)[1]
	else:
	    headFileContents = None
	headFile.restore(headFileContents, headRealPath, 1)
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

	# don't remove files if they've been changed locally
	try:
	    localFile = files.FileFromFilesystem(realPath, fileId)
	except OSError, exc:
	    # it's okay if the file is missing, it just means we all agree
	    if exc.errno == errno.ENOENT:
		fsPkg.removeFile(fileId)
		continue
	    else:
		raise

	oldFile = repos.getFileVersion(fileId, version)

	if not oldFile.same(localFile, ignoreOwner = True):
	    log.error("%s has changed but has been removed on head" % path)
	    continue

	log.debug("removing %s" % path)	

	os.unlink(realPath)
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

	    if fsPath == basePath:
		# the path changed in the repository, propage that change
		log.debug("renaming %s to %s" % (fsPath, headPath))
		os.rename(rootFixup + fsPath, rootFixup + headPath)

		fsPkg.addFile(fileId, headPath, fsVersion)
		finalPath = headPath
	    else:
		pathOkay = 0
		finalPath = fsPath	# let updates work still
		log.error("path conflict for %s (%s on head)" % 
			  (fsPath, headPath))

	realPath = rootFixup + finalPath

	# headFileVersion is None for renames
	if headFileVersion:
	    # FIXME we should be able to inspect fileChanges directly
	    # to see if we need to go into the if statement which follows
	    # this rather then having to look up the file from the old
	    # pacakge for every file which has changed
	
	    fsFile = files.FileFromFilesystem(realPath, fileId)
	    
	    if not basePkg.hasFile(fileId):
		# a file which was not in the base package was created
		# on both the head of the branch and in the filesystem;
		# this can happen during source management
		log.error("new file %s conflicts with file on head of branch"
				% realPath)
		contentsOkay = 0
	    else:
		baseFileVersion = basePkg.getFile(fileId)[1]
		(baseFile, baseFileContents) = repos.getFileVersion(fileId, 
				    baseFileVersion, withContents = 1)
	    
	    fileChanges = changeSet.getFileChange(fileId)
	    headFile = baseFile.copy()
	    headFile.applyChange(fileChanges)

	    if headFile.hasContents:
		(headFileContType, headFileContents) = \
			changeSet.getFileContents(fileId)
	    else:
		headFileContents = None

	if basePkg and headFileVersion and not \
		    fsFile.same(headFile, ignoreOwner = True):
	    # the contents have changed... let's see what to do

	    if headFile.same(baseFile, ignoreOwner = True):
		# it changed in just the filesystem, so leave that change
		log.debug("preserving new contents of %s" % realPath)
	    elif fsFile.same(baseFile, ignoreOwner = True):
		# the contents changed in just the repository, so take
		# those changes
		log.debug("replacing %s with contents from repository" % 
				realPath)
		assert(headFileContents == changeset.ChangedFileTypes.file)
		headFile.restore(headFileContents, realPath, 1)
	    elif fsFile.isConfig() or headFile.isConfig():
		# it changed in both the filesystem and the repository; our
		# only hope is to generate a patch for what changed in the
		# repository and try and apply it here
		if headFileContType != changeset.ChangedFileTypes.diff:
		    log.error("contents conflict for %s" % realPath)
		    contentsOkay = 0
		else:
		    log.debug("merging changes from repository into %s" % realPath)
		    diff = headFileContents.get().readlines()
		    cur = open(realPath, "r").readlines()
		    (newLines, failedHunks) = patch.patch(cur, diff)

		    f = open(realPath, "w")
		    f.write("".join(newLines))

		    if failedHunks:
			log.warning("conflicts from merging changes from " +
			    "head into %s saved as %s.conflicts" % 
			    (realPath, realPath))
			failedHunks.write(realPath + ".conflicts", 
					  "current", "head")

		    contentsOkay = 1
	    else:
		log.error("files conflict for %s" % realPath)
		contentsOkay = 0

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
	    return

	if srcPkg and srcPkg.hasFile(fileId):
	    srcFileVersion = srcPkg.getFile(fileId)[1]
	    srcFile = repos.getFileVersion(fileId, srcFileVersion)
	    f = files.FileFromFilesystem(realPath, fileId,
					 possibleMatch = fileId)
	else:
	    f = files.FileFromFilesystem(realPath, fileId)

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

    if csPkg.getOldFileList() or csPkg.getChangedFileList() or \
       csPkg.getNewFileList():
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
	returnList.append(result)

    return (changeSet, returnList)
