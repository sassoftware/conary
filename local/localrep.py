#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

from repository import repository

class LocalRepositoryChangeSetJob(repository.ChangeSetJob):

    """
    Removals have to be batched (for now at least); if we do them too
    soon the code which merges into the filesystem won't be able to get
    to the old version of things.
    """

    def addPackage(self, pkg):
	pkgCs = self.cs.getNewPackageVersion(pkg.getName(), pkg.getVersion(),
					     pkg.getFlavor())
	old = pkgCs.getOldVersion()
	self.repos.addPackage(pkg, oldVersion = old)

    def oldPackage(self, pkg):
	self.oldPackages.append(pkg)

    def oldPackageList(self):
	return self.oldPackages

    def oldFile(self, fileId, fileVersion, fileObj):
	self.oldFiles.append((fileId, fileVersion, fileObj))

    def oldFileList(self):
	return self.oldFiles

    def addFile(self, fileObject):
	repository.ChangeSetJob.addFile(self, fileObject, 
			 storeContents = fileObject.file().flags.isConfig())

	fileId = fileObject.fileId()
	oldVersion = self.cs.getFileOldVersion(fileId)
	if oldVersion:
	    self.removeFile(fileId, oldVersion)

    # remove the specified file 
    def removeFile(self, fileId, version):
	fileObj = self.repos.getFileVersion(fileId, version)
	self.oldFile(fileId, version, fileObj)

    # If retargetLocal is set, then localCs is for A->A.local whlie
    # origJob is A->B, so localCs needs to be changed to be B->B.local.
    # Otherwise, we're applying a rollback and origJob is B->A and
    # localCs is A->A.local, so it doesn't need retargeting.
    def __init__(self, repos, cs):
	assert(not cs.isAbsolute())

	self.cs = cs
	self.repos = repos
	self.oldPackages = []
	self.oldFiles = []

	# remove old versions of the packages which are being added. this has
	# to be done before FilesystemRepository.__init__() is called, as
	# it munges the database so we can no longer get to the old packages
	# 
	# while we're here, package change sets may mark some files as removed;
	# we need to remember to remove those files, and make the paths for
	# those files candidates for removal package change sets also know 
	# when file paths have changed, and those old paths are also candidates
	# for removal
	for csPkg in cs.iterNewPackageList():
	    name = csPkg.getName()
	    oldVersion = csPkg.getOldVersion()

	    if not oldVersion:
		# we know this isn't an absolute change set (since this
		# class can't handle absolute change sets, and asserts
		# the away at the top of __init__() ), so this must be
		# a new package. no need to erase any old stuff then!
		continue

	    assert(repos.hasTrove(name, oldVersion, csPkg.getFlavor()))

	    oldPkg = repos.getTrove(name, oldVersion, csPkg.getFlavor())
	    self.oldPackage(oldPkg)

	    for fileId in csPkg.getOldFileList():
		(oldPath, oldFileVersion) = oldPkg.getFile(fileId)
		self.removeFile(fileId, oldFileVersion)

	repository.ChangeSetJob.__init__(self, repos, cs)

	for pkg in self.oldPackageList():
	    self.repos.eraseTrove(pkg.getName(), pkg.getVersion(),
				  pkg.getFlavor())

	for (fileId, fileVersion, fileObj) in self.oldFileList():
	    self.repos.eraseFileVersion(fileId, fileVersion)

	for (fileId, fileVersion, fileObj) in self.oldFileList():
	    if fileObj.hasContents and fileObj.flags.isConfig():
		self.repos.removeFileContents(fileObj.contents.sha1())
