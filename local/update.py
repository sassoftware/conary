#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved

"""
Handles all updates to the file system; files should never get changed
on the filesystem except by this module!
"""

import changeset
import errno
import files
import log
import os
import patch

def applyChangeSet(changeSet, root):
    """
    Applies a change set to the filesystem.

    @param changeSet: the changeset to apply to the filesystem
    @type changeSet: changeset.ChangeSet
    @param root: root directory to apply changes to (this is ignored for
    source management, which uses the cwd)
    @type root: str
    """
    for pkgCs in changeSet.getNewPackageList():
	_applySingleChangeSet(pkgCs, root)

def _applyPackageChangeSet(repos, pkgCs, basePkg, fsPkg, root):
    """
    Apply a single package change set to the filesystem. Returns a package
    object which specifies what's on the system. 

    @param repos: the repository the files for basePkg are stored in
    @type repos: repository.Repository
    @param pkgCs: the changeset to apply to the filesystem
    @type pkgCs: package.PackageChangeSet
    @param basePkg: the package the stuff in the filesystem came from
    @type basePkg: package.Package
    @param fsPkg: the package representing what's in the filesystem now
    @type basePkg: package.Package
    @param root: root directory to apply changes to (this is ignored for
    source management, which uses the cwd)
    @type root: str
    @rtype: package.Package
    """
    assert(pkgCs.getOldVersion().equal(basePkg.getVersion()))
    assert(pkgCs.getOldVersion().equal(fsPkg.getVersion()))
    fullyUpdated = 1
    cwd = os.getcwd()
    fsPkg = fsPkg.copy()

    for (fileId, headPath, headFileVersion) in pkgCs.getNewFileList():
	# this gets broken links right
	try:
	    os.lstat(headPath)
	    log.error("%s is in the way of a newly created file" % headPath)
	    fullyUpdated = 0
	    continue
	except:
	    pass

	log.info("creating %s" % headPath)
	(headFile, headFileContents) = \
		repos.getFileVersion(fileId, headFileVersion, withContents = 1)
	headFile.restore(headFileContents, cwd + '/' + headPath, 1)
	fsPkg.addFile(fileId, headPath, headFileVersion)

    for fileId in pkgCs.getOldFileList():
	(path, version) = basePkg.getFile(fileId)
	if not fsPkg.hasFile(fileId):
	    log.info("%s has already been removed" % path)
	    continue

	# don't remove files if they've been changed locally
	try:
	    localFile = files.FileFromFilesystem(path, fileId)
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

	log.info("removing %s" % path)	

	os.unlink(path)
	fsPkg.removeFile(fileId)

    for (fileId, headPath, headFileVersion) in pkgCs.getChangedFileList():
	(fsPath, fsVersion) = fsPkg.getFile(fileId)
	pathOkay = 1
	contentsOkay = 1
	realPath = fsPath
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
		log.info("renaming %s to %s" % (fsPath, headPath))
		os.rename(fsPath, headPath)
		fsPkg.addFile(fileId, headPath, fsVersion)
		realPath = headPath
	    else:
		pathOkay = 0
		realPath = fsPath	# let updates work still
		log.error("path conflict for %s (%s on head)" % 
			  (fsPath, headPath))
	
	# headFileVersion is None for renames
	if headFileVersion:
	    fsFile = files.FileFromFilesystem(realPath, fileId)
	    (headFile, headFileContents) = \
		    repos.getFileVersion(fileId, headFileVersion, 
					 withContents = 1)

	if headFileVersion and not fsFile.same(headFile, ignoreOwner = True):
	    # the contents have changed... let's see what to do
	    if basePkg.hasFile(fileId):
		baseFileVersion = basePkg.getFile(fileId)[1]
		(baseFile, baseFileContents) = repos.getFileVersion(fileId, 
				    baseFileVersion, withContents = 1)
	    else:
		baseFile = None

	    if not baseFile:
		log.error("new file %s conflicts with file on head of branch"
				% realPath)
		contentsOkay = 0
	    elif headFile.same(baseFile, ignoreOwner = True):
		# it changed in just the filesystem, so leave that change
		log.info("preserving new contents of %s" % realPath)
	    elif fsFile.same(baseFile, ignoreOwner = True):
		# the contents changed in just the repository, so take
		# those changes
		log.info("replacing %s with contents from repository" % 
				realPath)
		baseFile.restore(baseContents, realPath, 1)
	    elif fsFile.isConfig() or headFile.isConfig():
		# it changed in both the filesystem and the repository; our
		# only hope is to generate a patch for what changed in the
		# repository and try and apply it here
		(contType, cont) = changeset.fileContentsDiff(
			baseFile, baseFileContents,
			headFile, headFileContents)
		if contType != changeset.ChangedFileTypes.diff:
		    log.error("contents conflict for %s" % realPath)
		    contentsOkay = 0
		else:
		    log.info("merging changes from repository into %s" % realPath)
		    diff = cont.get().readlines()
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
	    fsPkg.addFile(fileId, realPath, headFileVersion)
	else:
	    fullyUpdated = 0

    if fullyUpdated:
	fsPkg.changeVersion(pkgCs.getNewVersion())

    return fsPkg
