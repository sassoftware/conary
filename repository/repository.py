#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

# implements the SRS system repository

import util
import package
import files
import datastore
import os
import versioned
import fcntl

class Repository:

    def getPackageSet(self, pkgName, mode = 0):
	return package.PackageSet(self.pkgDB, pkgName)

    def getFileDB(self, fileId):
	return files.FileDB(self.fileDB, fileId)

    def pullFileContents(self, fileId, targetFile):
	srcFile = self.contentsStore.openFile(fileId)
	targetFile.write(srcFile.read())
	srcFile.close()

    def pullFileContentsObject(self, fileId):
	return self.contentsStore.openFile(fileId)

    def newFileContents(self, fileId, srcFile):
	targetFile = self.contentsStore.newFile(fileId)
	targetFile.write(srcFile.read())
	targetFile.close()

    def hasFileContents(self, fileId):
	return self.contentsStore.hasFile(fileId)

    def getPackageList(self, groupName = ""):
	if self.pkgDB.hasFile(groupName):
	    return [ groupName ]

	allPackages = self.pkgDB.fileList()
	list = []
	groupName = groupName + "/"

	for pkgName in allPackages:
	    if pkgName.startswith(groupName):
		list.append(pkgName)

	list.sort()

	return list

    def hasPackage(self, pkg):
	return package.packageSetExists(self.pkgDB, pkg)
	
    def storeFileFromChangeset(self, chgSet, file, pathToFile):
	if isinstance(file, files.RegularFile):
	    f = chgSet.getFileContents(file.sha1())
	    file.archive(self, f)
	    f.close()

    def open(self, mode):
	self.close()

	self.lockfd = os.open(self.top + "/lock", os.O_CREAT | os.O_RDWR)

	if (mode == "r"):
	    fcntl.lockf(self.lockfd, fcntl.LOCK_SH)
	else:
	    fcntl.lockf(self.lockfd, fcntl.LOCK_EX)

	self.pkgDB = versioned.FileIndexedDatabase(self.top + "/pkgs.db")
	self.fileDB = versioned.Database(self.top + "/files.db")

    def close(self):
	if self.pkgDB:
	    self.pkgDB = None
	    self.fileDB = None
	    os.close(self.lockfd)

    def __del__(self):
	self.close()

    def __init__(self, path, mode = "c"):
	self.top = path
	self.pkgDB = None
	
	self.contentsDB = self.top + "/contents"
	util.mkdirChain(self.contentsDB)

	self.contentsStore = datastore.DataStore(self.contentsDB)

	self.open(mode)

# The database is a magic repository where files in the data store are quite
# often pointers to the actual file in the file system
class Database(Repository):

    def storeFileFromChangeset(self, chgSet, file, pathToFile):
	file.restore(chgSet, self.root + pathToFile)

    def __init__(self, root, path):
	self.root = root
	fullPath = root + "/" + path
	Repository.__init__(self, fullPath)
