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

from repository import repository

class LocalRepositoryChangeSetJob(repository.ChangeSetJob):

    storeOnlyConfigFiles = True

    """
    Removals have to be batched (for now at least); if we do them too
    soon the code which merges into the filesystem won't be able to get
    to the old version of things.
    """

    def addPackage(self, pkg):
	pkgCs = self.cs.getNewPackageVersion(pkg.getName(), pkg.getVersion(),
					     pkg.getFlavor())
	old = pkgCs.getOldVersion()
	return self.repos.addPackage(pkg, oldVersion = old)

    def addPackageDone(self, troveId):
	pass

    def oldPackage(self, pkg):
	self.oldPackages.append(pkg)

    def oldPackageList(self):
	return self.oldPackages

    def oldFile(self, fileId, fileVersion, fileObj):
	self.oldFiles.append((fileId, fileVersion, fileObj))

    def oldFileList(self):
	return self.oldFiles

    def addFile(self, troveId, fileId, fileObj, path, version):
	repository.ChangeSetJob.addFile(self, troveId, fileId, fileObj, path, 
					version)

	if fileObj:
	    oldVersion = self.cs.getFileOldVersion(fileId)
	    if oldVersion:
		self.removeFile(fileId, oldVersion)

    def addFileContents(self, sha1, newVer, fileContents, restoreContents,
			isConfig):
	if isConfig:
	    repository.ChangeSetJob.addFileContents(self, sha1, newVer, 
			     fileContents, restoreContents, isConfig)

    # remove the specified file 
    def removeFile(self, fileId, version):
	fileObj = self.repos.getFileVersion(fileId, version)
	self.oldFile(fileId, version, fileObj)

    # If retargetLocal is set, then localCs is for A->A.local whlie
    # origJob is A->B, so localCs needs to be changed to be B->B.local.
    # Otherwise, we're applying a rollback and origJob is B->A and
    # localCs is A->A.local, so it doesn't need retargeting.
    def __init__(self, repos, cs, keepExisting):
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
	# those files candidates for removal. package change sets also know 
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

	    assert(repos.hasTrove(name, oldVersion, csPkg.getOldFlavor()))

	    oldPkg = repos.getTrove(name, oldVersion, csPkg.getOldFlavor())
	    self.oldPackage(oldPkg)

	    for fileId in csPkg.getOldFileList():
		(oldPath, oldFileVersion) = oldPkg.getFile(fileId)
		self.removeFile(fileId, oldFileVersion)

	repository.ChangeSetJob.__init__(self, repos, cs)

	if not keepExisting:
	    for pkg in self.oldPackageList():
		self.repos.eraseTrove(pkg.getName(), pkg.getVersion(),
				      pkg.getFlavor())

	for (fileId, fileVersion, fileObj) in self.oldFileList():
	    self.repos.eraseFileVersion(fileId, fileVersion)

	for (fileId, fileVersion, fileObj) in self.oldFileList():
	    if fileObj.hasContents and fileObj.flags.isConfig():
		self.repos._removeFileContents(fileObj.contents.sha1())
