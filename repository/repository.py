# implements the SRS system repository

import util
import package
import files
import datastore
import os

class Repository:

    def getPackageSet(self, pkgName, mode = "r"):
	return package.PackageSet(self.pkgDB, pkgName, mode)

    def getFileDB(self, id):
	return files.FileDB(self.fileDB, id)

    def pullFileContents(self, fileId, targetFile):
	srcFile = self.contentsStore.openFile(fileId)
	targetFile.write(srcFile.read())
	srcFile.close()

    def newFileContents(self, fileId, srcFile):
	targetFile = self.contentsStore.newFile(fileId)
	targetFile.write(srcFile.read())
	targetFile.close()

    def hasFileContents(self, fileId):
	return self.contentsStore.hasFile(fileId)

    def getPackageList(self, groupName = ""):
	if os.path.isfile(self.pkgDB + groupName):
	    return [ groupName ]
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

    def __init__(self, path):
	self.top = path

	self.pkgDB = self.top + "/pkgs"
	self.fileDB = self.top + "/files"
	self.contentsDB = self.top + "/contents"

	util.mkdirChain(self.pkgDB, self.fileDB, self.contentsDB)

	self.contentsStore = datastore.DataStore(self.contentsDB)
