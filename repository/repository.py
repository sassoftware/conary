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

class Repository:

    def getPackageSet(self, pkgName, mode = "r"):
	return package.PackageSet(self.pkgDB, pkgName, mode)

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
	if not os.path.exists(self.pkgDB + groupName):
	    return []
	elif os.path.isfile(self.pkgDB + groupName):
	    return [ groupName ]
	else:
	    return self.recurPackageList(self.pkgDB, groupName)

    def hasPackage(self, pkg):
	return os.path.exists(self.pkgDB + pkg)
	
    def recurPackageList(self, root, path):
	list = []
	for file in os.listdir(root + path):
	    if os.path.isdir(root + path + "/" + file):
		list = list + self.recurPackageList(root, path + "/" + file)
	    else:
		list.append(path + "/" + file)

	return list

    def storeFileFromChangeset(self, chgSet, file, pathToFile):
	if isinstance(file, files.RegularFile):
	    f = cs.getFileContents(file.sha1())
	    file.archive(self, f)
	    f.close()

    def __init__(self, path):
	self.top = path

	self.pkgDB = self.top + "/pkgs"
	self.fileDB = self.top + "/files"
	self.contentsDB = self.top + "/contents"

	util.mkdirChain(self.pkgDB, self.fileDB, self.contentsDB)

	self.contentsStore = datastore.DataStore(self.contentsDB)

# The database is a magic repository where files in the data store are quite
# often pointers to the actual file in the file system
class Database(Repository):

    def storeFileFromChangeset(self, chgSet, file, pathToFile):
	file.restore(chgSet, self.root + pathToFile)

    def __init__(self, root, path):
	self.root = root
	fullPath = root + "/" + path
	Repository.__init__(self, fullPath)
