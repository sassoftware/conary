#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import filecontainer
import files
import package
import versions
import os
import repository

class ChangeSet:

    def isAbstract(self):
	return self.abstract

    def validate(self, justContentsForConfig = 0):
	for pkg in self.getNewPackageList():
	    # if this is abstract, we can't have any removed or changed files
	    if not pkg.getOldVersion():
		assert(not pkg.getChangedFileList())
		assert(not pkg.getOldFileList())

	    list = pkg.getNewFileList() + pkg.getChangedFileList()

	    # new and changed files need to have a file entry for the right 
	    # version along with the contents for files which have any
	    for (fileId, path, version) in list:
		assert(self.files.has_key(fileId))
		(oldVersion, newVersion, info) = self.files[fileId]
		assert(newVersion.equal(version))

		l = info.split()
		if (l[0] == "src" or l[0] == "f") and l[1] != "-":
		    if justContentsForConfig:
			if l[-1] == "-":
			    # XXX just hope for the best?
			    pass
			else:
			    flags = int(l[-1], 16)
			    if flags & files._FILE_FLAG_CONFIG:
				assert(self.hasFileContents(l[1]))
		    else:
			assert(self.hasFileContents(l[1]))

	    # old files should not have any file entries
	    for fileId in pkg.getOldFileList():
		assert(not self.files.has_key(fileId))


    def getFileContents(self, fileId):
	raise NotImplementedError

    def hasFileContents(self, fileId):
	raise NotImplementedError

    def addFile(self, fileId, oldVersion, newVersion, csInfo):
	assert(not oldVersion or oldVersion.timeStamp)
	assert(newVersion.timeStamp)
	self.files[fileId] = (oldVersion, newVersion, csInfo)
    
    def newPackage(self, csPkg):
	assert(not csPkg.getOldVersion() or csPkg.getOldVersion().timeStamp)
	assert(csPkg.getNewVersion().timeStamp)

	self.newPackages[csPkg.getName()] = csPkg
	if csPkg.isAbstract():
	    self.abstract = 1

    def oldPackage(self, name, version):
	assert(version.timeStamp)
	self.oldPackages.append((name, version))

    def getNewPackageList(self):
	return self.newPackages.values()

    def getNewPackage(self, name):
	return self.newPackages[name]

    def hasNewPackage(self, name):
	return self.newPackages.has_key(name)

    def getOldPackageList(self):
	return self.oldPackages

    def addFileContents(self, hash, contents):
	self.fileContents[hash] = contents

    def getFileContents(self, hash):
	return self.fileContents[hash].get()

    def hasFileContents(self, hash):
	return self.fileContents.has_key(hash)

    def getFileList(self):
	return self.files.items()

    def remapPaths(self, map):
	for pkgCs in self.newPackages.values():
	    pkgCs.remapPaths(map)

    def formatToFile(self, cfg, f):
	for pkg in self.newPackages.values():
	    pkg.formatToFile(self, cfg, f)
	for (pkgName, version) in self.oldPackages:
	    print pkgName, "removed", version.asString(cfg.defaultbranch)

    def getFileChange(self, fileId):
	return self.files[fileId][2]

    def getFileOldVersion(self, fileId):
	return self.files[fileId][0]

    def headerAsString(self):
	rc = ""
	for pkg in self.getNewPackageList():
            rc += pkg.freeze()

	for (pkgName, version) in self.getOldPackageList():
	    rc += "SRS PKG REMOVED %s %s\n" % (pkgName, version.freeze())
	
	for (fileId, (oldVersion, newVersion, csInfo)) in self.getFileList():
	    if oldVersion:
		oldStr = oldVersion.freeze()
	    else:
		oldStr = "(none)"

	    rc += "SRS FILE CHANGESET %s %s %s\n%s\n" % \
			    (fileId, oldStr, newVersion.freeze(), csInfo)
	
	return rc

    def writeToFile(self, outFileName):
	try:
	    outFile = open(outFileName, "w+")
	    csf = filecontainer.FileContainer(outFile)
	    outFile.close()

	    csf.addFile("SRSCHANGESET", self.headerAsString(), "")

	    for hash in self.fileContents.keys():
		f = self.getFileContents(hash)
		csf.addFile(hash, f, "")
		f.close()

	    csf.close()
	except:
	    os.unlink(outFileName)
	    raise

    # if availableFiles is set, this includes the contents that it can
    # find, but doesn't worry about files which it can't find
    def invert(self, repos, availableFiles = 0):
	assert(not self.abstract)
	# this is easy to fix if it turns out to be necessary
	assert(not self.oldPackages)

	inversion = ChangeSetFromRepository(repos)

	for pkgCs in self.getNewPackageList():
	    if not pkgCs.getOldVersion():
		# this was a new package, and the inverse of a new
		# package is an old package
		inversion.oldPackage(pkgCs.getName(), pkgCs.getNewVersion())
		continue

	    pkg = repos.getPackageVersion(pkgCs.getName(), 
					  pkgCs.getOldVersion())

	    # this is a modified package and needs to be inverted

	    invertedPkg = package.PackageChangeSet(pkgCs.getName(), 
			       pkgCs.getNewVersion(), pkgCs.getOldVersion())

	    for (fileId, path, version) in pkgCs.getNewFileList():
		invertedPkg.oldFile(fileId)

	    for fileId in pkgCs.getOldFileList():
		(path, version) = pkg.getFile(fileId)
		invertedPkg.newFile(fileId, path, version)

		origFile = repos.getFileVersion(fileId, version)
		inversion.addFile(fileId, None, version, origFile.diff(None))

		if (not availableFiles) or \
			    repos.hasFileContents(origFile.sha1()):
		    inversion.addFileContents(origFile.sha1())

	    for (fileId, newPath, newVersion) in pkgCs.getChangedFileList():
		(curPath, curVersion) = pkg.getFile(fileId)
		invertedPkg.changedFile(fileId, curPath, curVersion)

		(oldVersion, newVersion, csInfo) = self.files[fileId]
		assert(curVersion.equal(oldVersion))

		origFile = repos.getFileVersion(fileId, oldVersion)
		newFile = repos.getFileVersion(fileId, oldVersion)
		newFile.applyChange(csInfo)

		inversion.addFile(fileId, newVersion, oldVersion, 
				  origFile.diff(newFile))

		if origFile.sha1() != newFile.sha1():
		    if (not availableFiles) or \
				repos.hasFileContents(origFile.sha1()):
			inversion.addFileContents(origFile.sha1())

	    inversion.newPackage(invertedPkg)

	return inversion

    def __init__(self):
	self.newPackages = {}
	self.oldPackages = []
	self.files = {}
	self.fileContents = {}
	self.abstract = 0

class ChangeSetFromRepository(ChangeSet):

    def newPackage(self, pkg):
	# add the time stamps to the package version numbers
	if pkg.getOldVersion():
	    pkg.changeOldVersion(self.repos.getFullVersion(pkg.getName(),
							   pkg.getOldVersion()))
	pkg.changeNewVersion(self.repos.getFullVersion(pkg.getName(),
						       pkg.getNewVersion()))
	ChangeSet.newPackage(self, pkg)

    def __init__(self, repos):
	self.repos = repos
	ChangeSet.__init__(self)

class ChangeSetFromAbstractChangeSet(ChangeSet):

    def addFileContents(self, hash):
	return ChangeSet.addFileContents(hash, 
			    repostitory.FileContentsFromChangeSet(cs, hash))

    def __init__(self, absCS):
	self.absCS = absCS
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

	    if header.startswith("SRS PKG REMOVED "):
		(pkgName, verStr) = header.split()[3:6]
		version = versions.ThawVersion(verStr)
		self.oldPackage(pkgName, version)
	    elif header.startswith("SRS PKG "):
		l = header.split()

		pkgType = l[2]
		pkgName = l[3]

		if pkgType == "CHANGESET":
		    oldVersion = versions.ThawVersion(l[4])
		    rest = 5
		elif pkgType == "NEW" or pkgType == "ABSTRACT":
		    oldVersion = None
		    rest = 4
		else:
		    raise IOError, "invalid line in change set %s" % file

		newVersion = versions.ThawVersion(l[rest])
		lineCount = int(l[rest + 1])

		pkg = package.PackageChangeSet(pkgName, oldVersion, newVersion,
				       abstract = (pkgType == "ABSTRACT"))

		end = i + lineCount
		while i < end:
		    pkg.parse(lines[i][:-1])
		    i = i + 1

		self.newPackage(pkg)
	    elif header.startswith("SRS FILE CHANGESET "):
		(fileId, oldVerStr, newVerStr) = header.split()[3:6]
		if oldVerStr == "(none)":
		    oldVersion = None
		else:
		    oldVersion = versions.ThawVersion(oldVerStr)
		newVersion = versions.ThawVersion(newVerStr)
		self.addFile(fileId, oldVersion, newVersion, lines[i][:-1])
		i = i + 1
	    else:
		print header
		raise IOError, "invalid line in change set %s" % file

	    header = control.read()

    def __init__(self, file, justContentsForConfig = 0):
	ChangeSet.__init__(self)
	self.read(file)
	self.validate(justContentsForConfig)

# old may be None
def fileChangeSet(fileId, old, new):
    hash = None

    if old and old.__class__ == new.__class__:
	diff = new.diff(old)
	if isinstance(new, files.RegularFile) and      \
		  isinstance(old, files.RegularFile)   \
		  and new.sha1() != old.sha1():
	    hash = new.sha1()
    else:
	# different classes; these are always written as abstract changes
	old = None
	diff = new.infoLine()
	if isinstance(new, files.RegularFile):
	    hash = new.sha1()

    return (diff, hash)

# this creates an abstract changeset
#
# expects a list of (pkg, fileMap) tuples
#
def CreateFromFilesystem(pkgList):
    cs = ChangeSet()

    for (pkg, fileMap) in pkgList:
        version = pkg.getVersion()
	(pkgChgSet, filesNeeded) = pkg.diff(None, abstract = 1)
	cs.newPackage(pkgChgSet)

	for (fileId, oldVersion, newVersion, path) in filesNeeded:
	    (file, realPath, filePath) = fileMap[fileId]
	    (filecs, hash) = fileChangeSet(fileId, None, file)
	    cs.addFile(fileId, oldVersion, newVersion, filecs)

	    if hash:
		cs.addFileContents(hash, 
			  repository.FileContentsFromFilesystem(realPath))

    return cs

# creates a change set from the version of a package installed in the
# database against the files installed on the local system
def CreateAgainstLocal(cfg, db, pkgList):
    cs = ChangeSetFromFilesystem()

    for pkgName in pkgList:
	allVersions = db.getPackageVersionList(pkgName)
	if not allVersions: continue

	assert(len(allVersions) == 1)
	dbPkg = db.getPackageVersion(pkgName, allVersions[0])
	localPkg = db.getPackageVersion(pkgName, allVersions[0])

	localVersion = allVersions[0].fork(versions.LocalBranch(), 
					   sameVerRel = 1)
	localPkg.changeVersion(localVersion)

	changedFiles = {}
	for (fileId, path, version) in localPkg.fileList():
	    dbFile = db.getFileVersion(fileId, version)

	    if isinstance(dbFile, files.SourceFile):
		shortName = pkgName.split(':')[-2]
		srcPath = cfg.sourcepath % {'pkgname': shortName } 
		realPath = cfg.root + srcPath + "/" + path
		localFile = files.FileFromFilesystem(realPath, fileId, "src")
	    else:
		realPath = cfg.root + path
		localFile = files.FileFromFilesystem(realPath, fileId)

	    localFile.flags(dbFile.flags())

	    if not dbFile.same(localFile):
		fileVersion = version.fork(versions.LocalBranch(), 
					   sameVerRel = 1)
		localPkg.updateFile(fileId, path, fileVersion)
		changedFiles[fileId] = (dbFile, localFile, realPath)

	(pkgChgSet, filesNeeded) = localPkg.diff(dbPkg)
	cs.newPackage(pkgChgSet)

	for (fileId, oldVersion, newVersion, path) in filesNeeded:
	    (dbFile, localFile, fullPath)  = changedFiles[fileId]
	    (filecs, hash) = fileChangeSet(fileId, dbFile, localFile)
	    cs.addFile(fileId, oldVersion, newVersion, filecs)

	    if hash:
		cs.addFilePointer(hash, fullPath)

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
	list.append((name, oldVersion, newVersion, (not oldVersion)))

    cs = repos.createChangeSet(list)
    cs.writeToFile(outFileName)
