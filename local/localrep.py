#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import repository

from repository import FilesystemRepository

# This builds a job which applies both a change set and the local changes
# which are needed.
class LocalRepository(FilesystemRepository):

    createBranches = 1

    # this is called when a Repository wants to store a file; we never
    # want to do this; we copy files onto the filesystem after we've
    # created the LocalBranch
    def storeFileFromContents(self, contents, file, restoreContents):
	if file.isConfig():
	    return FilesystemRepository.storeFileFromContents(self, 
				contents, file, restoreContents)

    def __init__(self, root, path, mode = "r"):
	self.root = root
	self.dbpath = path
	fullPath = root + "/" + path + "/repcache"
	FilesystemRepository.__init__(self, fullPath, mode)

class LocalRepositoryChangeSetJob(repository.ChangeSetJob):

    def removals(self, undo):
	for pkg in self.oldPackageList():
	    self.repos.erasePackageVersion(pkg.getName(), pkg.getVersion())
	    undo.removedPackage(pkg)

	for (fileId, fileVersion, fileObj) in self.oldFileList():
	    self.repos.eraseFileVersion(fileId, fileVersion)
	    undo.removedFile(fileId, fileVersion, fileObj)

	undo.reset()
	for (fileId, fileVersion, fileObj) in self.oldFileList():
	    if fileObj.hasContents and fileObj.isConfig():
		self.repos.removeFileContents(fileObj.sha1())

    # remove the specified file 
    def removeFile(self, fileId, version):
	# we need this object in case of an undo
	fileObj = self.repos.getFileVersion(fileId, version)
	self.oldFile(fileId, version, fileObj)

    # If retargetLocal is set, then localCs is for A->A.local whlie
    # origJob is A->B, so localCs needs to be changed to be B->B.local.
    # Otherwise, we're applying a rollback and origJob is B->A and
    # localCs is A->A.local, so it doesn't need retargeting.
    def __init__(self, repos, cs):
	assert(not cs.isAbstract())
	
	repository.ChangeSetJob.__init__(self, repos, cs)

	# remove old versions of the packages which are being added
	# 
	# while we're here, package change sets may mark some files as removed;
	# we need to remember to remove those files, and make the paths for
	# those files candidates for removal package change sets also know 
	# when file paths have changed, and those old paths are also candidates
	# for removal
	for csPkg in cs.getNewPackageList():
	    name = csPkg.getName()
	    oldVersion = csPkg.getOldVersion()

	    if not oldVersion:
		# we know this isn't an abstract change set (since this
		# class can't handle abstract change sets, and asserts
		# the away at the top of __init__() ), so this must be
		# a new package. no need to erase any old stuff then!
		continue

	    assert(repos.hasPackageVersion(name, oldVersion))

	    self.oldPackage(repos.getPackageVersion(name, oldVersion))

	    for fileId in csPkg.getOldFileList():
		(oldPath, oldFileVersion) = csPkg.getFile(fileId)
		self.removeFile(fileId, oldFileVersion)

	# for each file which has changed, erase the old version of that
	# file from the repository
	for f in self.newFileList():
	    oldVersion = cs.getFileOldVersion(f.fileId())
	    if not oldVersion:
		# this is a new file; there is no old version to erase
		continue

	    self.removeFile(f.fileId(), oldVersion)
