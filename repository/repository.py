#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

# implements the SRS system repository

import datastore
import fcntl
import files
import os
import package
import string
import util
import versioned

class Repository:

    def commitChangeSet(self, sourcePathTemplate, cs):
	pkgList = []
	fileMap = {}

	# build todo set
	for pkg in cs.getPackageList():
	    newVersion = pkg.getNewVersion()
	    old = pkg.getOldVersion()
	
	    if self.hasPackage(pkg.getName()):
		pkgSet = self.getPackageSet(pkg.getName(), "r")

		if pkgSet.hasVersion(newVersion):
		    raise KeyError, "version %s for %s exists" % \
			    (newVersion.asString(), pkg.getName())
	    else:
		pkgSet = None

	    if old:
		newPkg = copy.deepcopy(pkgSet.getVersion(old))
	    else:
		newPkg = package.Package(pkg.name)

	    newFileMap = newPkg.applyChangeSet(self, pkg)
	    pkgList.append((pkg.getName(), newPkg, newVersion))
	    fileMap.update(newFileMap)

	# create the file objects we'll need for the commit
	fileList = []
	for (fileId, (oldVer, newVer, infoLine)) in cs.getFileList():
	    if oldVer:
		fileDB = self.getFileDB(fileId)
		file = copy.deepcopy(fileDB.getVersion(oldVer))
		file.applyChange(infoLine)
		del fileDB
	    else:
		file = files.FileFromInfoLine(infoLine, fileId)

	    assert(newVer.equal(fileMap[fileId][1]))
	    fileList.append((fileId, newVer, file))

	# commit changes
	pkgsDone = []
	filesDone = []
	filesToArchive = {}
	try:
	    for (pkgName, newPkg, newVersion) in pkgList:
		pkgSet = self.getPackageSet(pkgName, "w")
		pkgSet.addVersion(newVersion, newPkg)
		pkgsDone.append((pkgSet, newVersion))

	    for (fileId, fileVersion, file) in fileList:
		infoFile = self.getFileDB(fileId)
		pathInPkg = fileMap[fileId][0]
		pkgName = fileMap[fileId][2]

		# this version may already exist, abstract change sets
		# include redundant files quite often
		if not infoFile.hasVersion(fileVersion):
		    infoFile.addVersion(fileVersion, file)
		    infoFile.close()
		    filesDone.append(fileId)
		    filesToArchive[pathInPkg] = ((file, pathInPkg, pkgName))

	    # sort paths and store in order (to make sure that directories
	    # are stored before the files that reside in them in the case of
	    # restore to a local file system
	    pathsToArchive = filesToArchive.keys()
	    pathsToArchive.sort()
	    for pathInPkg in pathsToArchive:
		(file, path, pkgName) = filesToArchive[pathInPkg]
		if isinstance(file, files.SourceFile):
		    basePkgName = string.split(pkgName, ':')[-2]
		    d = { 'pkgname' : basePkgName }
		    path = (sourcePathTemplate) % d + "/" + path

		self.storeFileFromChangeset(cs, file, path)
	except:
	    # something went wrong; try to unwind our commits
	    for fileId in filesDone:
		infoFile = self.getFileDB(fileId)
		(path, fileVersion) = fileMap[fileId][0:2]
		infoFile.eraseVersion(fileVersion)

	    for (pkgSet, newVersion) in pkgsDone:
		pkgSet.eraseVersion(newVersion)

	    raise 

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
	groupName = groupName + ":"

	for pkgName in allPackages:
	    if pkgName.startswith(groupName):
		list.append(pkgName)

	list.sort()

	return list

    def hasPackage(self, pkg):
	return self.pkgDB.hasFile(pkg)
	
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

    def __init__(self, root, path, mode = "c"):
	self.root = root
	fullPath = root + "/" + path
	Repository.__init__(self, fullPath, mode)
