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

from deps import deps
import difflib
import enum
import filecontainer
import filecontents
import files
import os
import patch
import repository
import streams
import struct
import trove
import versions

from StringIO import StringIO

ChangedFileTypes = enum.EnumeratedType("cft", "file", "diff")

class FileInfo(streams.TupleStream):

    __slots__ = []

    # fileId, oldVersion, newVersion, csInfo
    makeup = (("fileId", streams.StringStream, 40), 
	      ("oldVersion", streams.StringStream, "!H"),
	      ("newVersion", streams.StringStream, "!H"), 
	      ("csInfo", streams.StringStream, "B"))

    def fileId(self):
        return self.items[0].value()

    def setFileId(self, value):
        return self.items[0].set(value)

    def oldVersion(self):
        return self.items[1].value()

    def setOldVersion(self, value):
        return self.items[1].set(value)

    def newVersion(self):
        return self.items[2].value()

    def setNewVersion(self, value):
        return self.items[2].set(value)

    def csInfo(self):
        return self.items[3].value()

    def setCsInfo(self, value):
        return self.items[3].set(value)

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

    def addPrimaryPackage(self, name, version, flavor):
	self.primaryPackageList.append((name, version, flavor))

    def getPrimaryPackageList(self):
	return self.primaryPackageList

    def newPackage(self, csPkg):
	old = csPkg.getOldVersion()
	new = csPkg.getNewVersion()
	assert(not old or min(old.timeStamps()) > 0)
	assert(min(new.timeStamps()) > 0)

	self.newPackages[(csPkg.getName(), csPkg.getNewVersion(),
		          csPkg.getNewFlavor())] = csPkg

	if csPkg.isAbsolute():
	    self.absolute = 1
	if (old and old.isLocal()) or new.isLocal():
	    self.local = 1

    def delNewPackage(self, name, version, flavor):
	del self.newPackages[(name, version, flavor)]

    def oldPackage(self, name, version, flavor):
	assert(min(version.timeStamps()) > 0)
	self.oldPackages.append((name, version, flavor))

    def iterNewPackageList(self):
	return self.newPackages.itervalues()

    def getNewPackageVersion(self, name, version, flavor):
	return self.newPackages[(name, version, flavor)]

    def getOldPackageList(self):
	return self.oldPackages

    def addFileContents(self, fileId, contType, contents, sortEarly):
	if sortEarly:
	    self.earlyFileContents[fileId] = (contType, contents)
	else:
	    self.lateFileContents[fileId] = (contType, contents)

    def getFileContents(self, fileId, withSize = False):
	if self.lateFileContents.has_key(fileId):
	    cont = self.lateFileContents[fileId]
	else:
	    cont = self.earlyFileContents[fileId]

	if not withSize:
	    return cont

	return cont + (cont.size(), )

    def hasFileContents(self, hash):
	return self.earlyFileContents.has_key(hash) or \
	        self.lateFileContents.has_key(hash)

    def addFile(self, fileId, oldVersion, newVersion, csInfo):
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

    def formatToFile(self, cfg, f):
	f.write("primary packages:\n")
	for (pkgName, version, flavor) in self.primaryPackageList:
	    if flavor:
		f.write("\t%s %s %s\n" % (pkgName, version.asString(), 
					  flavor.freeze()))
	    else:
		f.write("\t%s %s\n" % (pkgName, version.asString()))
	f.write("\n")

	for pkg in self.newPackages.itervalues():
	    pkg.formatToFile(self, f)
	for (pkgName, version, flavor) in self.oldPackages:
	    f.write("remove %s %s\n" %
		    (pkgName, version.asString()))

    def getFileChange(self, fileId):
	return self.files[fileId][2]

    def getFileOldVersion(self, fileId):
	return self.files[fileId][0]

    def hasFileChange(self, fileId):
	return self.files.has_key(fileId)

    def headerAsString(self):
	rc = []

	primaries = [ ]
	for (name, version, flavor) in self.primaryPackageList:
	    primaries.append(name)
	    primaries.append(version.asString())
	    if flavor:
		primaries.append(flavor.freeze())
	    else:
		primaries.append("")
	primaries = "\0".join(primaries)
	
	for pkg in self.iterNewPackageList():
	    s = pkg.freeze()
	    rc.append("PKG %d\n" % len(s))
            rc.append(s)

	for (pkgName, version, flavor) in self.getOldPackageList():
	    if flavor:
		rc.append("PKG RMVD %s %s %s\n" % 
			  (pkgName, version.freeze(), flavor.freeze()))
	    else:
		rc.append("PKG RMVD %s %s\n" % 
			  (pkgName, version.freeze()))
	
	fileList = [ None,]
	totalLen = 0
	for (fileId, (oldVersion, newVersion, csInfo)) in self.files.iteritems():
	    if oldVersion:
		oldStr = oldVersion.asString()
	    else:
		oldStr = "(none)"

	    s = FileInfo(fileId, oldStr, newVersion.asString(), csInfo).freeze()
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
	    csf.addFile(hash, f, tag + contType[4:])

    def writeAllContents(self, csf):
	self.writeContents(csf, self.earlyFileContents, True)
	self.writeContents(csf, self.lateFileContents, False)

    def writeToFile(self, outFileName):
	try:
	    outFile = open(outFileName, "w+")
	    csf = filecontainer.FileContainer(outFile)
	    outFile.close()

	    str = self.headerAsString()
	    csf.addFile("CONARYCHANGESET", filecontents.FromString(str), "")
	    self.writeAllContents(csf)
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
		rollback.oldPackage(pkgCs.getName(), pkgCs.getNewVersion(), 
				    pkgCs.getNewFlavor())
		continue

	    pkg = db.getTrove(pkgCs.getName(), pkgCs.getOldVersion(),
			      pkgCs.getOldFlavor())

	    # this is a modified package and needs to be inverted

	    invertedPkg = trove.TroveChangeSet(pkgCs.getName(), 
			       pkg.getChangeLog(),
			       pkgCs.getNewVersion(), pkgCs.getOldVersion(),
			       pkgCs.getNewFlavor(), pkgCs.getOldFlavor())

	    for (name, list) in pkgCs.iterChangedTroves():
		for (oper, version, flavor) in list:
		    if oper == '+':
			invertedPkg.oldTroveVersion(name, version, flavor)
		    elif oper == "-":
			invertedPkg.newTroveVersion(name, version, flavor)

	    for (fileId, path, version) in pkgCs.getNewFileList():
		invertedPkg.oldFile(fileId)

	    for fileId in pkgCs.getOldFileList():
		(path, version) = pkg.getFile(fileId)
		invertedPkg.newFile(fileId, path, version)

		origFile = db.getFileVersion(fileId, version)
		rollback.addFile(fileId, None, version, origFile.freeze())

		if not origFile.hasContents:
		    continue

		# We only have the contents of config files available
		# from the db. Files which aren't in the db
		# we'll gather from the filesystem *as long as they have
		# not changed*. If they have changed, they'll show up as
		# members of the local branch, and their contents will be
		# saved as part of that change set.
		if origFile.flags.isConfig():
		    cont = filecontents.FromDataStore(db.contentsStore, 
						       origFile.contents.sha1(),

						       origFile.contents.size())
		    rollback.addFileContents(fileId,
					     ChangedFileTypes.file, cont, 1)
		else:
		    fullPath = db.root + path

		    fsFile = files.FileFromFilesystem(fullPath, fileId,
				possibleMatch = origFile)

		    if fsFile == origFile:
			cont = filecontents.FromFilesystem(fullPath)
		    else:
			# a file which was removed in this changeset is
			# missing from the files; we need to to put an
			# empty file in here so we can apply the rollback
			cont = filecontents.FromString("")

		    rollback.addFileContents(fileId, ChangedFileTypes.file, 
					     cont, 0)

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
		   origFile.flags.isConfig():
		    (contType, cont) = self.getFileContents(newFile.id())
		    if contType == ChangedFileTypes.diff:
			f = cont.get()
			diff = "".join(patch.reverse(f.readlines()))
			f.seek(0)
			cont = filecontents.FromString(diff)
			rollback.addFileContents(fileId,
						 ChangedFileTypes.diff, cont, 1)
		    else:
			cont = filecontents.FromDataStore(db.contentsStore, 
				    origFile.contents.sha1(),
                                    origFile.contents.size())
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

	for (name, version, flavor) in self.getOldPackageList():
	    pkg = db.getTrove(name, version, flavor)
	    pkgDiff = pkg.diff(None)[0]
	    rollback.newPackage(pkgDiff)
	    for (fileId, path, fileVersion) in pkg.iterFileList():
		fileObj = db.getFileVersion(fileId, fileVersion)
		rollback.addFile(fileId, None, fileVersion, fileObj.freeze())
		if fileObj.hasContents:
		    fullPath = db.root + path

		    if os.path.exists(fullPath):
			fsFile = files.FileFromFilesystem(fullPath, fileId,
				    possibleMatch = fileObj)
		    else:
			fsFile = None

		    if fsFile and \
			    fsFile.contents.sha1() == fileObj.contents.sha1():
			# the contents in the file system are right
			cont = filecontents.FromFilesystem(fullPath)
		    else:
			# the contents in the file system are wrong; insert
			# a placeholder and let the local change set worry
			# about getting this right
			cont = filecontents.FromString("")

		    rollback.addFileContents(fileId,
					     ChangedFileTypes.file, cont,
					     fileObj.flags.isConfig())

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
		repos.getTrove(name, newVer, pkgCs.getFlavor())
	    except repository.TroveMissing: 
		pass
	    else:
		branch = oldVer.fork(targetBranchLabel, sameVerRel = 0)
		newVer = repos.getTroveLatestVersion(name, branch)

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
	    assert(min(pkg.getOldVersion().timeStamps()) > 0)
	assert(min(pkg.getNewVersion().timeStamps()) > 0)
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

    def getFileContents(self, fileId, withSize = False):
	if self.configCache.has_key(fileId):
	    (tag, str) = self.configCache[fileId]
	    cont = filecontents.FromString(str)
	    size = len(str)
	else:
	    (tagInfo, f, size) = self.csf.getFile(fileId)
	    tag = "cft-" + tagInfo.split()[1]
	    cont = filecontents.FromFile(f)

	    if tagInfo[0] == "1":
		str = cont.get().read()
		self.configCache[fileId] = (tag, str)
		cont = filecontents.FromString(str)
		size = len(str)

	if withSize:
	    return (tag, cont, size)
	else:
	    return (tag, cont)

    def hasFileContents(self, hash):
	return self.csf.hasFile(hash)

    def read(self, file):
	f = open(file, "r")
	self.csf = filecontainer.FileContainer(f)
	f.close()
	#return

	(tagInfo, control, size) = self.csf.getFile("CONARYCHANGESET")

	line = control.readline()
	while line:
	    header = line[:-1]

	    if header.startswith("PRIMARIES "):
		size = int(header.split()[1])
		if size:
		    buf = control.read(size)
		    items = buf.split("\0")

		    assert(len(items) % 3 == 0)
		    i = 0
		    while i < len(items):
			name = items[i]
			version = items[i + 1]
			flavor = items[i + 2]
			i += 3
			version = versions.VersionFromString(version)
			if flavor:
			    flavor = deps.ThawDependencySet(flavor)
			else:
			    flavor = None
			self.primaryPackageList.append((name, version, flavor))
	    elif header.startswith("PKG RMVD "):
		fields = header.split()
		(pkgName, verStr) = fields[2:4]
		version = versions.ThawVersion(verStr)
		if len(fields) == 5:
		    flavor = deps.ThawDependencySet(fields[4])
		else:
		    flavor = None
		self.oldPackage(pkgName, version, flavor)
	    elif header.startswith("PKG "):
		size = int(header.split()[1])
		buf = control.read(size)
		pkg = trove.ThawTroveChangeSet(buf)
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
			oldVersion = versions.VersionFromString(oldVerStr)
		    newVersion = versions.VersionFromString(info.newVersion())
		    self.addFile(info.fileId(), oldVersion, newVersion, 
				 info.csInfo())
	    else:
		print header
		raise IOError, "invalid line in change set %s" % file

	    line = control.readline()

    def writeAllContents(self, csf):
	rc = self.csf.getNextFile()
	while rc is not None:
	    (fileId, tag, f, size) = rc
	    cont = filecontents.FromFile(f, size)
	    csf.addFile(fileId, cont, tag)

	    rc = self.csf.getNextFile()

    def __init__(self, file, justContentsForConfig = 0, skipValidate = 1):
	ChangeSet.__init__(self)
	self.configCache = {}
	self.earlyFileContents = None
	self.lateFileContents = None
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
	first = oldCont.get().readlines()
	second = newCont.get().readlines()

	# XXX difflib (and probably our patch as well) don't work properly
	# for files w/o trailing newlines
	if first and first[-1][-1] == '\n' and \
		    second and second[-1][-1] == '\n':
	    diff = difflib.unified_diff(first, second, 
					newCont.get().readlines(),
					"old", "new")
	    diff.next()
	    diff.next()
	    cont = filecontents.FromString("".join(diff))
	    contType = ChangedFileTypes.diff
	else:
	    cont = filecontents.FromString("".join(second))
	    contType = ChangedFileTypes.file
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

	for (fileId, oldVersion, newVersion, oldPath, path) in filesNeeded:
	    (file, realPath, filePath) = fileMap[fileId]
	    (filecs, hash) = fileChangeSet(fileId, None, file)
	    cs.addFile(fileId, oldVersion, newVersion, filecs)

	    if hash:
		cs.addFileContents(fileId, ChangedFileTypes.file,
			  filecontents.FromFilesystem(realPath),
			  file.flags.isConfig())

    return cs

