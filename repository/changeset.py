#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import filecontainer
import files
import package
import versions
import os

class ChangeSet:

    def isAbstract(self):
	return self.abstract

    def validate(self):
	for pkg in self.getPackageList():
	    # if this is abstract, we can't have any removed or changed files
	    if not pkg.getOldVersion():
		assert(not pkg.getChangedFileList())
		assert(not pkg.getOldFileList())

	    # new and changed files need to have a file entry for the right 
	    # version along with the contents for files which have any
	    list = pkg.getNewFileList() + pkg.getChangedFileList()
	    for (fileId, path, version) in pkg.getNewFileList():
		assert(self.files.has_key(fileId))
		(oldVersion, newVersion, info) = self.getFile(fileId)
		assert(newVersion.equal(version))

		file = files.FileFromInfoLine(info, fileId)
		if isinstance(file, files.RegularFile) and file.sha1():
		    assert(self.hasFileContents(file.sha1()))

	    # old files should not have any file entries
	    for fileId in pkg.getOldFileList():
		assert(not self.files.has_key(fileId))


    def getFileContents(self, fileId):
	raise NotImplementedError

    def hasFileContents(self, fileId):
	raise NotImplementedError

    def addFile(self, fileId, oldVersion, newVersion, csInfo):
	self.files[fileId] = (oldVersion, newVersion, csInfo)
    
    def addPackage(self, pkg):
	self.packages.append(pkg)
	if not pkg.getOldVersion():
	    self.abstract = 1

    def getPackageList(self):
	return self.packages

    def addFileContents(self, hash):
	self.fileContents.append(hash)

    def getFileList(self):
	return self.files.items()

    def formatToFile(self, cfg, f):
	for pkg in self.packages:
	    pkg.formatToFile(self, cfg, f)
	    print

    def getFileChange(self, fileId):
	return self.files[fileId][2]

    def getFile(self, fileId):
	return self.files[fileId]

    def headerAsString(self):
	rc = ""
	for pkg in self.getPackageList():
            rc += pkg.asString()
	
	for (fileId, (oldVersion, newVersion, csInfo)) in self.getFileList():
	    if oldVersion:
		oldStr = oldVersion.asString()
	    else:
		oldStr = "(none)"

	    rc += "SRS FILE CHANGESET %s %s %s\n%s\n" % \
			    (fileId, oldStr, newVersion.asString(), csInfo)
	
	return rc

    def writeToFile(self, outFileName):
	try:
	    outFile = open(outFileName, "w+")
	    csf = filecontainer.FileContainer(outFile)
	    outFile.close()

	    csf.addFile("SRSCHANGESET", self.headerAsString(), "")

	    for hash in self.fileContents:
		f = self.getFileContents(hash)
		csf.addFile(hash, f, "")
		f.close()

	    csf.close()
	except:
	    os.unlink(outFileName)
	    raise

    def invert(self, repos):
	assert(not self.abstract)

	inversion = ChangeSetFromRepository(repos)

	for pkgCs in self.getPackageList():
	    pkg = repos.getPackageVersion(pkgCs.getName(), 
					  pkgCs.getOldVersion())

	    invertedPkg = package.PackageChangeSet(pkgCs.getName(), 
			       pkgCs.getNewVersion(), pkgCs.getOldVersion())

	    for (fileId, path, version) in pkgCs.getNewFileList():
		invertedPkg.oldFile(fileId)

	    for fileId in pkgCs.getOldFileList():
		(path, version) = pkg.getFile(fileId)
		invertedPkg.newFile(fileId, path, version)

		origFile = repos.getFileVersion(fileId, version)
		inversion.addFile(fileId, None, version, origFile.diff(None))
		inversion.addFileContents(origFile.sha1())

	    for (fileId, newPath, newVersion) in pkgCs.getChangedFileList():
		(curPath, curVersion) = pkg.getFile(fileId)
		invertedPkg.changedFile(fileId, curPath, curVersion)

		(oldVersion, newVersion, csInfo) = self.getFile(fileId)
		assert(curVersion.equal(oldVersion))

		origFile = repos.getFileVersion(fileId, oldVersion)
		newFile = repos.getFileVersion(fileId, oldVersion)
		newFile.applyChange(csInfo)

		inversion.addFile(fileId, newVersion, oldVersion, 
				  origFile.diff(newFile))

		if origFile.sha1() != newFile.sha1():
		    inversion.addFileContents(origFile.sha1())

	    inversion.addPackage(invertedPkg)

	return inversion

    def __init__(self):
	assert(self.__class__ != ChangeSet)
	self.packages = []
	self.files = {}
	self.fileContents = []
	self.abstract = 0

class ChangeSetFromFilesystem(ChangeSet):

    def getFileContents(self, fileId):
	return open(self.fileMap[fileId])

    def hasFileContents(self, fileId):
	return self.fileMap.has_key(fileId)

    def addFilePointer(self, fileId, path):
	self.fileMap[fileId] = path

    def __init__(self):
	self.fileMap = {}
	ChangeSet.__init__(self)

class ChangeSetFromRepository(ChangeSet):

    def getFileContents(self, fileId):
	return self.repos.pullFileContentsObject(fileId)

    def hasFileContents(self, fileId):
	return self.repos.hasFileContents(fileId)

    def __init__(self, repos):
	self.repos = repos
	ChangeSet.__init__(self)

class ChangeSetFromFile(ChangeSet):

    def getFileContents(self, hash):
	return self.csf.getFile(hash)

    def hasFileContents(self, hash):
	return self.csf.hasFile(hash)

    def read(self, file):
	f = open(file, "r")
	self.csf = filecontainer.FileContainer(f)
	f.close()

	control = self.csf.getFile("SRSCHANGESET")

	lines = control.readLines()
	i = 0
	while i < len(lines):
	    header = lines[i][:-1]
	    i = i + 1

	    if header.startswith("SRS PKG CHANGESET "):
		(pkgName, oldVerStr, newVerStr, lineCount) = header.split()[3:7]

		if oldVerStr == "(none)":
		    # abstract change set
		    oldVersion = None
		else:
		    oldVersion = versions.VersionFromString(oldVerStr)

		newVersion = versions.VersionFromString(newVerStr)
		lineCount = int(lineCount)

		pkg = package.PackageChangeSet(pkgName, oldVersion, newVersion)

		end = i + lineCount
		while i < end:
		    pkg.parse(lines[i][:-1])
		    i = i + 1

		self.addPackage(pkg)
	    elif header.startswith("SRS FILE CHANGESET "):
		(fileId, oldVerStr, newVerStr) = header.split()[3:6]
		if oldVerStr == "(none)":
		    oldVersion = None
		else:
		    oldVersion = versions.VersionFromString(oldVerStr)
		newVersion = versions.VersionFromString(newVerStr)
		self.addFile(fileId, oldVersion, newVersion, lines[i][:-1])
		i = i + 1
	    else:
		raise IOError, "invalid line in change set %s" % file

	    header = control.read()

    def __init__(self, file):
	ChangeSet.__init__(self)
	self.read(file)
	self.validate()

# old may be None
def fileChangeSet(fileId, old, new):
    hash = None

    if old and old.__class__ == new.__class__:
	diff = new.diff(old)
	if isinstance(new, files.RegularFile) and      \
		  isinstance(old, files.RegularFile) \
		  and new.sha1() != old.sha1():
	    hash = new.sha1()
    else:
	# different classes; these are always written as abstract changes
	old = None
	diff = new.infoLine()
	if isinstance(new, files.RegularFile):
	    hash = new.sha1()

    return (diff, hash)

# this creates the changeset against None
#
# expects a list of (pkg, fileMap) tuples
#
def CreateFromFilesystem(pkgList):
    cs = ChangeSetFromFilesystem()

    for (pkg, fileMap) in pkgList:
        version = pkg.getVersion()
	(pkgChgSet, filesNeeded) = pkg.diff(None, None, version)
	cs.addPackage(pkgChgSet)

	for (fileId, oldVersion, newVersion) in filesNeeded:
	    (file, realPath, filePath) = fileMap[fileId]
	    (filecs, hash) = fileChangeSet(fileId, None, file)
	    cs.addFile(fileId, oldVersion, newVersion, filecs)

	    if hash:
		cs.addFilePointer(hash, realPath)

    return cs

def ChangeSetCommand(repos, cfg, packageName, outFileName, oldVersionStr, \
	      newVersionStr):
    if packageName[0] != ":":
	packageName = cfg.packagenamespace + ":" + packageName

    newVersion = versions.VersionFromString(newVersionStr, cfg.defaultbranch)

    if (oldVersionStr):
	oldVersion = versions.VersionFromString(oldVersionStr, 
					        cfg.defaultbranch)
    else:
	oldVersion = None

    list = []
    for name in repos.getPackageList(packageName):
	list.append((name, oldVersion, newVersion))

    cs = repos.createChangeSet(list)
    cs.writeToFile(outFileName)
