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
import update
import versioned
import versions

ChangedFileTypes = enum.EnumeratedType("cft", "file", "diff")

class ChangeSet:

    def isAbstract(self):
	return self.abstract

    def isLocal(self):
	return self.local

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
			assert(self.hasFileContents(fileId))

	    # old files should not have any file entries
	    for fileId in pkg.getOldFileList():
		assert(not self.files.has_key(fileId))

    def newPackage(self, csPkg):
	old = csPkg.getOldVersion()
	new = csPkg.getNewVersion()
	assert(not old or old.timeStamp)
	assert(new.timeStamp)

	self.newPackages[csPkg.getName()] = csPkg

	if csPkg.isAbstract():
	    self.abstract = 1
	if (old and old.isLocal()) or new.isLocal():
	    self.local = 1

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
	for pkgCs in self.newPackages.values():
	    dict = { 'pkgname' : pkgCs.getName().split(":")[0],
		     'branchnick' : 
			str(pkgCs.getNewVersion().branch().branchNickname()) }
	    pkgCs.remapPaths(map, dict)

    def formatToFile(self, cfg, f):
	for pkg in self.newPackages.values():
	    pkg.formatToFile(self, cfg, f)
	for (pkgName, version) in self.oldPackages:
	    print pkgName, "removed", version.asString(cfg.defaultbranch)

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
	for pkg in self.getNewPackageList():
            rc.append(pkg.freeze())

	for (pkgName, version) in self.getOldPackageList():
	    rc.append("SRS PKG REMOVED %s %s\n" % (pkgName, version.freeze()))
	
	for (fileId, (oldVersion, newVersion, csInfo)) in self.files.iteritems():
	    if oldVersion:
		oldStr = oldVersion.freeze()
	    else:
		oldStr = "(none)"

	    rc.append("SRS FILE CHANGESET %s %s %s\n%s\n" %
                      (fileId, oldStr, newVersion.freeze(), csInfo))
	
	return "".join(rc)

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
	    csf.addFile(hash, f.get(), tag + contType[4:], f.size())

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
	assert(not self.abstract)

	rollback = ChangeSetFromRepository(db)

	for pkgCs in self.getNewPackageList():
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

	    for (name, list) in pkgCs.getChangedPackages():
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
		if origFile.isConfig():
		    cont = filecontents.FromRepository(db, origFile.sha1(),
						       origFile.size())
		    rollback.addFileContents(fileId,
					     ChangedFileTypes.file, cont, 1)
		else:
		    if isinstance(origFile, files.SourceFile):
			type = "src"
		    else:
			type = None

		    fullPath = db.root + path

		    fsFile = files.FileFromFilesystem(fullPath, fileId,
				type = type, possibleMatch = origFile)

		    if fsFile.same(origFile):
			cont = filecontents.FromFilesystem(fullPath)
			rollback.addFileContents(fileId,
						 ChangedFileTypes.file, cont, 0)

	    for (fileId, newPath, newVersion) in pkgCs.getChangedFileList():
		(curPath, curVersion) = pkg.getFile(fileId)

		if newPath:
		    invertedPkg.changedFile(fileId, curPath, curVersion)
		else:
		    invertedPkg.changedFile(fileId, None, curVersion)

		(oldVersion, newVersion, csInfo) = self.files[fileId]
		assert(curVersion.equal(oldVersion))

		origFile = db.getFileVersion(fileId, oldVersion)
		newFile = db.getFileVersion(fileId, oldVersion)
		newFile.applyChange(csInfo)

		rollback.addFile(fileId, newVersion, oldVersion, 
				  origFile.diff(newFile))

		if not isinstance(origFile, files.RegularFile):
		    continue

		# If a config file has changed between versions, save
		# it; if it hasn't changed the unmodified version will
		# still be available from the database when the rollback
		# gets applied. We may be able to get away with just reversing
		# a diff rather then saving the full contents
		if (origFile.sha1() != newFile.sha1()) and	    \
		   (origFile.isConfig() or newFile.isConfig()):
		    (contType, cont) = self.getFileContents(newFile.id())
		    if contType == ChangedFileTypes.diff:
			f = cont.get()
			diff = "".join(patch.reverse(f.readlines()))
			f.seek(0)
			cont = filecontents.FromString(diff)
			rollback.addFileContents(fileId,
						 ChangedFileTypes.diff, cont, 1)
		    else:
			cont = filecontents.FromRepository(db, origFile.sha1(),
							   origFile.size())
			rollback.addFileContents(fileId,
						 ChangedFileTypes.file, cont,
						 newFile.isConfig())
		elif origFile.sha1() != newFile.sha1():
		    # this file changed, so we need the contents
		    if isinstance(origFile, files.SourceFile):
			type = "src"
		    else:
			type = None

		    fullPath = db.root + curPath
		    fsFile = files.FileFromFilesystem(fullPath, fileId,
				type = type, possibleMatch = origFile)

		    if fsFile.sha1() == origFile.sha1():
			# the contents in the file system are right
			cont = filecontents.FromFilesystem(fullPath)
		    else:
			# the contents in the file system are wrong; insert
			# a placeholder and let the local change set worry
			# about getting this right
			cont = filecontents.FromString("")

		    rollback.addFileContents(fileId,
					     ChangedFileTypes.file, cont,
					     origFile.isConfig() or
					     newFile.isConfig())

	    rollback.newPackage(invertedPkg)

	for (name, version) in self.getOldPackageList():
	    pkg = db.getPackageVersion(name, version)
	    pkgDiff = pkg.diff(None)[0]
	    rollback.newPackage(pkgDiff)
	    for (fileId, (path, fileVersion)) in pkg.iterFileList():
		fileObj = db.getFileVersion(fileId, fileVersion)
		rollback.addFile(fileId, None, fileVersion, fileObj.infoLine())
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
					     fsFile.isConfig())

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
	assert(not targetBranchLabel.equal(versions.LocalBranch()))

	packageVersions = {}

	for pkgCs in self.getNewPackageList():
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

	for pkgCs in self.getNewPackageList():
	    # the implemented of updateChangedPackage makes this whole thing
	    # O(n^2) (n is the number of packages changed in pkgCs), which is
	    # just silly. if large groups are added like this the effect could
	    # become noticeable
	    for (name, list) in pkgCs.getChangedPackages():
		if not packageVersions.has_key(name): continue
		for (change, version) in list:
		    if change != '+': continue

		    for (oldVer, newVer) in packageVersions[name]:
			if oldVer.equal(version):
			    pkgCs.updateChangedPackage(name, oldVer, newVer)

	# this has to be true, I think...
	self.local = 0

    def __init__(self):
	self.newPackages = {}
	self.oldPackages = []
	self.files = {}
	self.earlyFileContents = {}
	self.lateFileContents = {}
	self.abstract = 0
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

class ChangeSetFromAbstractChangeSet(ChangeSet):

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

	lines = control.readlines()
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
		  and new.sha1() != old.sha1():
	    hash = new.sha1()
    else:
	# different classes; these are always written as abstract changes
	old = None
	diff = new.infoLine()
	if isinstance(new, files.RegularFile):
	    hash = new.sha1()

    return (diff, hash)

def fileContentsDiff(oldFile, oldCont, newFile, newCont):
    if oldFile and oldFile.isConfig() and newFile.isConfig():
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

# this creates an abstract changeset
#
# expects a list of (pkg, fileMap) tuples
#
def CreateFromFilesystem(pkgList):
    cs = ChangeSet()

    for (pkg, fileMap) in pkgList:
        version = pkg.getVersion()
	(pkgChgSet, filesNeeded, pkgsNeeded) = pkg.diff(None, abstract = 1)
	cs.newPackage(pkgChgSet)

	for (fileId, oldVersion, newVersion, path) in filesNeeded:
	    (file, realPath, filePath) = fileMap[fileId]
	    (filecs, hash) = fileChangeSet(fileId, None, file)
	    cs.addFile(fileId, oldVersion, newVersion, filecs)

	    if hash:
		cs.addFileContents(fileId, ChangedFileTypes.file,
			  filecontents.FromFilesystem(realPath),
			  file.isConfig())

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
    pkgList = [ (x.getName(), x.getVersion()) for x in pkgList ]
    while pkgList:
	(name, ver) = pkgList[0]
	del pkgList[0]
	match = False
	if dupFilter.has_key(name):
	    for version in dupFilter[name]:
		if version.equal(ver):
		    match = True
		    break

	if not match:
	    if dupFilter.has_key(name):
		dupFilter[name].append(ver)
	    else:
		dupFilter[name] = [ ver ]
	    pkg = db.getPackageVersion(name, ver)
	    ver = ver.fork(versions.LocalBranch(), sameVerRel = 1)
	    list.append((pkg, pkg, ver))
	    
	    for (otherPkg, verList) in pkg.getPackageList():
		for otherVer in verList:
		    pkgList.append((otherPkg, otherVer))

    result = update.buildLocalChanges(db, list, root = cfg.root)
    if not result: return
    cs = result[0]
    hasChanges = False
    for (changed, fsPkg) in result[1]:
	if changed:
	    hasChanges = True
	    break

    if not changed:
	log.error("there have been no local changes")
    else:
	cs.writeToFile(outFileName)
