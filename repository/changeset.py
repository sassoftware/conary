#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import difflib
import enum
import filecontainer
import filecontents
import files
import helper
import log
import os
import package
import patch
import repository
import struct
import update
import versioned
import versions

ChangedFileTypes = enum.EnumeratedType("cft", "file", "diff")

class FileInfo(files.TupleStream):

    # fileId, oldVersion, newVersion, csInfo
    makeup = (("fileId", files.StringStream, 40), 
	      ("oldVersion", files.StringStream, "!H"),
	      ("newVersion", files.StringStream, "!H"), 
	      ("csInfo", files.StringStream, "B"))

class ChangeSet:

    def isAbsolute(self):
	return self.absolute

    def isLocal(self):
	return self.local

    def validate(self, justContentsForConfig = 0):
	for pkg in self.iterNewPackageList():
	    # if this is absolute, we can't have any removed or changed files
	    if not pkg.getOldVersion():
		assert(not pkg.getChangedFileList())
		assert(not pkg.getOldFileList())

	    list = pkg.getNewFileList() + pkg.getChangedFileList()

	    # new and changed files need to have a file entry for the right 
	    # version along with the contents for files which have any
	    for (fileId, path, version) in list:
		assert(self.files.has_key(fileId))
		(oldVersion, newVersion, info) = self.files[fileId]
		assert(newVersion == version)

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
			assert(self.hasFileContents(fileId))

	    # old files should not have any file entries
	    for fileId in pkg.getOldFileList():
		assert(not self.files.has_key(fileId))

    def addPrimaryPackage(self, name, version):
	self.primaryPackageList.append((name, version))

    def getPrimaryPackageList(self):
	return self.primaryPackageList

    def newPackage(self, csPkg):
	old = csPkg.getOldVersion()
	new = csPkg.getNewVersion()
	assert(not old or old.timeStamp)
	assert(new.timeStamp)

	self.newPackages[(csPkg.getName(), csPkg.getNewVersion())] = csPkg

	if csPkg.isAbsolute():
	    self.absolute = 1
	if (old and old.isLocal()) or new.isLocal():
	    self.local = 1

    def delNewPackage(self, name, version):
	del self.newPackages[(name, version)]

    def oldPackage(self, name, version):
	assert(version.timeStamp)
	self.oldPackages.append((name, version))

    def iterNewPackageList(self):
	return self.newPackages.itervalues()

    def getNewPackageVersion(self, name, version):
	return self.newPackages[(name, version)]

    def getOldPackageList(self):
	return self.oldPackages

    def addFileContents(self, fileId, contType, contents, sortEarly):
	if sortEarly:
	    self.earlyFileContents[fileId] = (contType, contents)
	else:
	    self.lateFileContents[fileId] = (contType, contents)

    def getFileContents(self, fileId):
	if self.lateFileContents.has_key(fileId):
	    return self.lateFileContents[fileId]

	return self.earlyFileContents[fileId]

    def getFileContentsType(self, fileId):
	return self.getFileContents(fileId)[0]

    def hasFileContents(self, hash):
	return self.earlyFileContents.has_key(hash) or \
	        self.lateFileContents.has_key(hash)

    def addFile(self, fileId, oldVersion, newVersion, csInfo):
	assert(not oldVersion or oldVersion.timeStamp)
	assert(newVersion.timeStamp)
	self.files[fileId] = (oldVersion, newVersion, csInfo)

	if oldVersion and oldVersion.isLocal():
	    self.local = 1
	if newVersion.isLocal():
	    self.local = 1

    def getFileList(self):
	return self.files.items()

    def getFile(self, fileId):
	return self.files[fileId]

    def hasFile(self, fileId):
	return self.files.has_key(fileId)

    def remapPaths(self, map):
	for pkgCs in self.newPackages.itervalues():
	    dict = { 'pkgname' : pkgCs.getName().split(":")[0],
		     'branchnick' : 
			str(pkgCs.getNewVersion().branch().branchNickname()) }
	    pkgCs.remapPaths(map, dict)

    def formatToFile(self, cfg, f):
	f.write("primary packages:\n")
	for (pkgName, version) in self.primaryPackageList:
	    f.write("\t%s %s\n" % (pkgName, version.asString()))
	f.write("\n")

	for pkg in self.newPackages.itervalues():
	    pkg.formatToFile(self, cfg, f)
	for (pkgName, version) in self.oldPackages:
	    f.write("removed %s %s\n" %
		    (pkgName, version.asString(cfg.defaultbranch)))

    def dump(self):
	import srscfg, sys
	cfg = srscfg.SrsConfiguration()
	self.formatToFile(cfg, sys.stdout)

    def getFileChange(self, fileId):
	return self.files[fileId][2]

    def getFileOldVersion(self, fileId):
	return self.files[fileId][0]

    def hasFileChange(self, fileId):
	return self.files.has_key(fileId)

    def headerAsString(self):
	rc = []

	primaries = [ ]
	for (name, version) in self.primaryPackageList:
	    primaries.append(name)
	    primaries.append(version.asString())
	primaries = "\0".join(primaries)
	
	for pkg in self.iterNewPackageList():
	    s = pkg.freeze()
	    rc.append("PKG %d\n" % len(s))
            rc.append(s)

	for (pkgName, version) in self.getOldPackageList():
	    rc.append("PKG RMVD %s %s\n" % (pkgName, version.freeze()))
	
	fileList = [ None,]
	totalLen = 0
	for (fileId, (oldVersion, newVersion, csInfo)) in self.files.iteritems():
	    if oldVersion:
		oldStr = oldVersion.freeze()
	    else:
		oldStr = "(none)"

	    s = FileInfo(fileId, oldStr, newVersion.freeze(), csInfo).freeze()
	    fileList.append(struct.pack("!I", len(s)) + s)
	    totalLen += len(fileList[-1])

	fileList[0] = "FILES %d\n" % totalLen
	
	return "PRIMARIES %d\n%s%s%s" % \
		    (len(primaries), primaries,
		     "".join(rc),
		     "".join(fileList))

    def writeContents(self, csf, contents, early):
	# these are kept sorted so we know which one comes next
	idList = contents.keys()
	idList.sort()

	if early:
	    tag = "1 "
	else:
	    tag = "0 "

	for hash in idList:
	    (contType, f) = contents[hash]
	    csf.addFile(hash, f, tag + contType[4:], f.size())

    def writeToFile(self, outFileName):
	try:
	    outFile = open(outFileName, "w+")
	    csf = filecontainer.FileContainer(outFile)
	    outFile.close()

	    str = self.headerAsString()
	    csf.addFile("SRSCHANGESET", 
			versioned.FalseFile(str), "", len(str))

	    self.writeContents(csf, self.earlyFileContents, True)
	    self.writeContents(csf, self.lateFileContents, False)

	    csf.close()
	except:
	    os.unlink(outFileName)
	    raise

    # if availableFiles is set, this includes the contents that it can
    # find, but doesn't worry about files which it can't find
    def makeRollback(self, db, configFiles = 0):
	assert(not self.absolute)

	rollback = ChangeSetFromRepository(db)

	for pkgCs in self.iterNewPackageList():
	    if not pkgCs.getOldVersion():
		# this was a new package, and the inverse of a new
		# package is an old package
		rollback.oldPackage(pkgCs.getName(), pkgCs.getNewVersion())
		continue

	    pkg = db.getPackageVersion(pkgCs.getName(), 
					  pkgCs.getOldVersion())

	    # this is a modified package and needs to be inverted

	    invertedPkg = package.PackageChangeSet(pkgCs.getName(), 
			       pkgCs.getNewVersion(), pkgCs.getOldVersion())

	    for (name, list) in pkgCs.iterChangedPackages():
		for (oper, version) in list:
		    if oper == '+':
			invertedPkg.oldPackageVersion(name, version)
		    elif oper == "-":
			invertedPkg.newPackageVersion(name, version)

	    for (fileId, path, version) in pkgCs.getNewFileList():
		invertedPkg.oldFile(fileId)

	    for fileId in pkgCs.getOldFileList():
		(path, version) = pkg.getFile(fileId)
		invertedPkg.newFile(fileId, path, version)

		origFile = db.getFileVersion(fileId, version)
		rollback.addFile(fileId, None, version, origFile.diff(None))

		if not origFile.hasContents:
		    continue

		# We only have the contents of config files available
		# from the db. Files which aren't in the db
		# we'll gather from the filesystem *as long as they have
		# not changed*. If they have changed, they'll show up as
		# members of the local branch, and their contents will be
		# saved as part of that change set.
		if origFile.flags.isConfig():
		    cont = filecontents.FromRepository(db, 
						       origFile.contents.sha1(),
						       origFile.size())
		    rollback.addFileContents(fileId,
					     ChangedFileTypes.file, cont, 1)
		else:
		    fullPath = db.root + path

		    fsFile = files.FileFromFilesystem(fullPath, fileId,
				possibleMatch = origFile)

		    if fsFile == origFile:
			cont = filecontents.FromFilesystem(fullPath)
			rollback.addFileContents(fileId,
						 ChangedFileTypes.file, cont, 0)

	    for (fileId, newPath, newVersion) in pkgCs.getChangedFileList():
		if not pkg.hasFile(fileId):
		    # the file has been removed from the local system; we
		    # don't need to restore it on a rollback
		    continue
		(curPath, curVersion) = pkg.getFile(fileId)

		if newPath:
		    invertedPkg.changedFile(fileId, curPath, curVersion)
		else:
		    invertedPkg.changedFile(fileId, None, curVersion)

		(oldVersion, newVersion, csInfo) = self.files[fileId]
		assert(curVersion == oldVersion)

		origFile = db.getFileVersion(fileId, oldVersion)
		newFile = db.getFileVersion(fileId, oldVersion)
		newFile.twm(csInfo, origFile)

		rollback.addFile(fileId, newVersion, oldVersion, 
				  origFile.diff(newFile))

		if not isinstance(origFile, files.RegularFile):
		    continue

		# If a config file has changed between versions, save
		# it; if it hasn't changed the unmodified version will
		# still be available from the database when the rollback
		# gets applied. We may be able to get away with just reversing
		# a diff rather then saving the full contents
		if (origFile.contents.sha1() != newFile.contents.sha1()) and \
		   (origFile.flags.isConfig() or newFile.flags.isConfig()):
		    (contType, cont) = self.getFileContents(newFile.id())
		    if contType == ChangedFileTypes.diff:
			f = cont.get()
			diff = "".join(patch.reverse(f.readlines()))
			f.seek(0)
			cont = filecontents.FromString(diff)
			rollback.addFileContents(fileId,
						 ChangedFileTypes.diff, cont, 1)
		    else:
			cont = filecontents.FromRepository(db, 
				    origFile.contents.sha1(), origFile.size())
			rollback.addFileContents(fileId,
						 ChangedFileTypes.file, cont,
						 newFile.flags.isConfig())
		elif origFile.contents.sha1() != newFile.contents.sha1():
		    # this file changed, so we need the contents
		    fullPath = db.root + curPath
		    fsFile = files.FileFromFilesystem(fullPath, fileId,
				possibleMatch = origFile)

		    if fsFile.contents.sha1() == origFile.contents.sha1():
			# the contents in the file system are right
			cont = filecontents.FromFilesystem(fullPath)
		    else:
			# the contents in the file system are wrong; insert
			# a placeholder and let the local change set worry
			# about getting this right
			cont = filecontents.FromString("")

		    rollback.addFileContents(fileId,
					     ChangedFileTypes.file, cont,
					     origFile.flags.isConfig() or
					     newFile.flags.isConfig())

	    rollback.newPackage(invertedPkg)

	for (name, version) in self.getOldPackageList():
	    pkg = db.getPackageVersion(name, version)
	    pkgDiff = pkg.diff(None)[0]
	    rollback.newPackage(pkgDiff)
	    for (fileId, path, fileVersion) in pkg.iterFileList():
		fileObj = db.getFileVersion(fileId, fileVersion)
		rollback.addFile(fileId, None, fileVersion, fileObj.freeze())
		if fileObj.hasContents:
		    fullPath = db.root + path
		    fsFile = files.FileFromFilesystem(fullPath, fileId,
				possibleMatch = fileObj)

		    if fsFile.sha1() == fileObj.sha1():
			# the contents in the file system are right
			cont = filecontents.FromFilesystem(fullPath)
		    else:
			# the contents in the file system are wrong; insert
			# a placeholder and let the local change set worry
			# about getting this right
			cont = filecontents.FromString("")

		    rollback.addFileContents(fileId,
					     ChangedFileTypes.file, cont,
					     fsFile.flags.isConfig())

	return rollback

    def setTargetBranch(self, repos, targetBranchLabel):
	"""
	Retargets this changeset to create packages and files on
	branch targetBranchName off of the source node.

	@param repos: repository which will be committed to
	@type repos: repository.Repository
	@param targetBranchLabel: label of the branch to commit to
	@type targetBranchLabel: versions.BranchName
	"""
	assert(not targetBranchLabel == versions.LocalBranch())

	packageVersions = {}

	for pkgCs in self.iterNewPackageList():
	    name = pkgCs.getName()
	    oldVer = pkgCs.getOldVersion()
	    ver = pkgCs.getNewVersion()
	    # what to do about versions for new packages?
	    assert(oldVer)

	    newVer = oldVer.fork(targetBranchLabel, sameVerRel = 0)
	    newVer.appendVersionReleaseObject(ver.trailingVersion())

	    # try and reuse the version number we created; if
	    # it's already in use we won't be able to though
	    try:
		repos.getPackageVersion(name, newVer)
	    except repository.PackageMissing: 
		pass
	    else:
		branch = oldVer.fork(targetBranchLabel, sameVerRel = 0)
		newVer = repos.pkgLatestVersion(name, branch)

	    pkgCs.changeNewVersion(newVer)
	    if not packageVersions.has_key(name):
		packageVersions[name] = []
	    packageVersions[name].append((ver, newVer))

	    # files on the local branch get remapped; others don't
	    if self.isLocal(): 
		for (listMethod, addMethod) in [
			(pkgCs.getChangedFileList, pkgCs.changedFile),
			(pkgCs.getNewFileList, pkgCs.newFile) ]:
		    for (fileId, path, fileVersion) in listMethod():
			if fileVersion != "-" and fileVersion.isLocal():
			    addMethod(fileId, path, newVer)
			    oldVer = self.getFileOldVersion(fileId)
			    csInfo = self.getFileChange(fileId)
			    # this replaces the existing file 
			    self.addFile(fileId, oldVer, newVer, csInfo)

	for pkgCs in self.iterNewPackageList():
	    # the implemented of updateChangedPackage makes this whole thing
	    # O(n^2) (n is the number of packages changed in pkgCs), which is
	    # just silly. if large groups are added like this the effect could
	    # become noticeable
	    for (name, list) in pkgCs.iterChangedPackages():
		if not packageVersions.has_key(name): continue
		for (change, version) in list:
		    if change != '+': continue

		    for (oldVer, newVer) in packageVersions[name]:
			if oldVer == version:
			    pkgCs.updateChangedPackage(name, oldVer, newVer)

	# this has to be true, I think...
	self.local = 0

    def __init__(self):
	self.newPackages = {}
	self.oldPackages = []
	self.files = {}
	self.earlyFileContents = {}
	self.lateFileContents = {}
	self.primaryPackageList = []
	self.absolute = 0
	self.local = 0

class ChangeSetFromRepository(ChangeSet):

    def newPackage(self, pkg):
	# add the time stamps to the package version numbers
	if pkg.getOldVersion():
	    assert(pkg.getOldVersion().timeStamp)
	assert(pkg.getNewVersion().timeStamp)
	ChangeSet.newPackage(self, pkg)

    def __init__(self, repos):
	self.repos = repos
	ChangeSet.__init__(self)

class ChangeSetFromAbsoluteChangeSet(ChangeSet):

    def __init__(self, absCS):
	self.absCS = absCS
	ChangeSet.__init__(self)

class ChangeSetFromFile(ChangeSet):

    def getFileSize(self, fileId):
	return self.csf.getSize(fileId)

    def getFileContentsType(self, fileId):
	tagInfo = self.csf.getTag(fileId).split()
	return "cft-" + tagInfo[1]

    def getFileContents(self, fileId):
	if self.configCache.has_key(fileId):
	    (tag, str) = self.configCache[fileId]
	    return (tag, filecontents.FromString(str))

	f = self.csf.getFile(fileId)
	tagInfo = self.csf.getTag(fileId).split()
	tag = "cft-" + tagInfo[1]

	assert(tagInfo[0] == "0")
	return (tag, filecontents.FromFile(f))

    def hasFileContents(self, hash):
	return self.csf.hasFile(hash)

    def read(self, file):
	f = open(file, "r")
	self.csf = filecontainer.FileContainer(f)
	f.close()

	control = self.csf.getFile("SRSCHANGESET")

	line = control.readline()
	while line:
	    header = line[:-1]

	    if header.startswith("PRIMARIES "):
		size = int(header.split()[1])
		if size:
		    buf = control.read(size)
		    items = buf.split("\0")

		    assert(len(items) % 2 == 0)
		    i = 0
		    while i < len(items):
			name = items[i]
			version = items[i + 1]
			i += 2
			version = versions.VersionFromString(version)
			self.primaryPackageList.append((name, version))
	    elif header.startswith("PKG RMVD "):
		(pkgName, verStr) = header.split()[2:5]
		version = versions.ThawVersion(verStr)
		self.oldPackage(pkgName, version)
	    elif header.startswith("PKG "):
		size = int(header.split()[1])
		buf = control.read(size)
		lines = buf.split("\n")[:-1]
		pkg = package.ThawPackageChangeSet(lines)
		self.newPackage(pkg)
	    elif header.startswith("FILES "):
		size = int(header.split()[1])

		buf = control.read(size)
		i = 0
		while i < len(buf):
		    size = struct.unpack("!I", buf[i:i+4])[0]
		    i += 4
		    info = FileInfo(buf[i:i+size])
		    i += size
		    
		    oldVerStr = info.oldVersion()

		    if oldVerStr == "(none)":
			oldVersion = None
		    else:
			oldVersion = versions.ThawVersion(oldVerStr)
		    newVersion = versions.ThawVersion(info.newVersion())
		    self.addFile(info.fileId(), oldVersion, newVersion, 
				 info.csInfo())
	    else:
		print header
		raise IOError, "invalid line in change set %s" % file

	    line = control.readline()

	# pull in the config files
	idList = []
	for fileId in self.csf.iterFileList():
	    if fileId == "SRSCHANGESET": continue
	    tags = self.csf.getTag(fileId)
	    if tags.startswith("0 "): continue
	    idList.append(fileId)
	
	idList.sort()

	for fileId in idList:
	    tags = self.csf.getTag(fileId)
	    tag = "cft-" + tags.split()[1]
	    self.configCache[fileId] = (tag, self.csf.getFile(fileId).read())

    def __init__(self, file, justContentsForConfig = 0, skipValidate = 1):
	ChangeSet.__init__(self)
	self.configCache = {}
	self.read(file)
	if not skipValidate:
	    self.validate(justContentsForConfig)

# old may be None
def fileChangeSet(fileId, old, new):
    hash = None

    if old and old.__class__ == new.__class__:
	diff = new.diff(old)
	if isinstance(new, files.RegularFile) and      \
		  isinstance(old, files.RegularFile)   \
		  and new.contents.sha1() != old.contents.sha1():
	    hash = new.contents.sha1()
    else:
	# different classes; these are always written as absolute changes
	old = None
	diff = new.freeze()
	if isinstance(new, files.RegularFile):
	    hash = new.contents.sha1()

    return (diff, hash)

def fileContentsDiff(oldFile, oldCont, newFile, newCont):
    if oldFile and oldFile.flags.isConfig() and newFile.flags.isConfig():
	diff = difflib.unified_diff(oldCont.get().readlines(),
				    newCont.get().readlines(),
				    "old", "new")
	diff.next()
	diff.next()
	cont = filecontents.FromString("".join(diff))
	contType = ChangedFileTypes.diff
    else:
	cont = newCont
	contType = ChangedFileTypes.file

    return (contType, cont)

# this creates an absolute changeset
#
# expects a list of (pkg, fileMap) tuples
#
def CreateFromFilesystem(pkgList):
    cs = ChangeSet()

    for (pkg, fileMap) in pkgList:
        version = pkg.getVersion()
	(pkgChgSet, filesNeeded, pkgsNeeded) = pkg.diff(None, absolute = 1)
	cs.newPackage(pkgChgSet)

	for (fileId, oldVersion, newVersion, path) in filesNeeded:
	    (file, realPath, filePath) = fileMap[fileId]
	    (filecs, hash) = fileChangeSet(fileId, None, file)
	    cs.addFile(fileId, oldVersion, newVersion, filecs)

	    if hash:
		cs.addFileContents(fileId, ChangedFileTypes.file,
			  filecontents.FromFilesystem(realPath),
			  file.flags.isConfig())

    return cs

def ChangeSetCommand(repos, cfg, pkgName, outFileName, oldVersionStr, \
	      newVersionStr):
    newVersion = versions.VersionFromString(newVersionStr, cfg.defaultbranch)

    if (oldVersionStr):
	oldVersion = versions.VersionFromString(oldVersionStr, 
					        cfg.defaultbranch)
    else:
	oldVersion = None

    list = [(pkgName, oldVersion, newVersion, (not oldVersion))]

    cs = repos.createChangeSet(list)
    cs.writeToFile(outFileName)

def LocalChangeSetCommand(db, cfg, pkgName, outFileName):
    try:
	pkgList = helper.findPackage(db, cfg.installbranch, pkgName, None)
    except helper.PackageNotFound, e:
	log.error(e)
	return

    list = []
    dupFilter = {}
    i = 0
    for outerPackage in pkgList:
	for pkg in package.walkPackageSet(db, outerPackage):
	    ver = pkg.getVersion()
	    origPkg = db.getPackageVersion(pkg.getName(), ver, pristine = True)
	    ver = ver.fork(versions.LocalBranch(), sameVerRel = 1)
	    list.append((pkg, origPkg, ver))
	    
    result = update.buildLocalChanges(db, list, root = cfg.root)
    if not result: return
    cs = result[0]

    for outerPackage in pkgList:
	cs.addPrimaryPackage(outerPackage.getName(), 
	  outerPackage.getVersion().fork(
		versions.LocalBranch(), sameVerRel = 1))

    hasChanges = False
    for (changed, fsPkg) in result[1]:
	if changed:
	    hasChanges = True
	    break

    if not changed:
	log.error("there have been no local changes")
    else:
	cs.writeToFile(outFileName)
