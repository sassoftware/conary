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
import lib.enum
import errno
import filecontainer
import filecontents
import files
import os
from lib import patch
import repository
from lib import sha1helper
import streams
import struct
import trove
from lib import util
import versions

from StringIO import StringIO

ChangedFileTypes = lib.enum.EnumeratedType("cft", "file", "diff", "ptr")

class FileInfo(streams.TupleStream):

    __slots__ = []

    # fileId, oldVersion, newVersion, csInfo
    makeup = (("fileId", streams.Sha1Stream, 20), 
	      ("oldVersion", streams.StringStream, "!H"),
	      ("newVersion", streams.StringStream, "!H"), 
	      ("csInfo", streams.StringStream, "B"))

    def fileId(self):
        return self.items[0]

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

    def __init__(self, first = None, oldStr = None, newVer = None, chg = None):
	if oldStr is None:
	    streams.TupleStream.__init__(self, first)
	else:
	    streams.TupleStream.__init__(self, first, oldStr, newVer, chg)

class ChangeSetNewPackageList(dict, streams.InfoStream):

    def freeze(self):
	l = []
	for pkg in self.itervalues():
	    s = pkg.freeze()
	    l.append(struct.pack("!I", len(s)))
	    l.append(s)
	
	return "".join(l)

    def thaw(self, data):
	i = 0
	while i < len(data):
	    size = struct.unpack("!I", data[i : i + 4])[0]
	    i += 4
	    s = data[i: i + size]
	    i += size
	    trvCs = trove.ThawTroveChangeSet(s)
	    
	    self[(trvCs.getName(), trvCs.getNewVersion(),
					  trvCs.getNewFlavor())] = trvCs

    def __init__(self, data = None):
	if data:
	    self.thaw(data)
	    
class ChangeSetFileDict(dict, streams.InfoStream):

    def freeze(self):
	fileList = []
	for (fileId, (oldVersion, newVersion, csInfo)) in self.iteritems():
	    if oldVersion:
		oldStr = oldVersion.asString()
	    else:
		oldStr = ""

	    s = FileInfo(fileId, oldStr, newVersion.asString(), csInfo).freeze()
	    fileList.append(struct.pack("!I", len(s)) + s)

	return "".join(fileList)

    def thaw(self ,data):
	i = 0
	while i < len(data):
	    size = struct.unpack("!I", data[i:i+4])[0]
	    i += 4
	    info = FileInfo(data[i:i+size])
	    i += size
	    
	    oldVerStr = info.oldVersion()

	    if not oldVerStr:
		oldVersion = None
	    else:
		oldVersion = versions.VersionFromString(oldVerStr)

	    newVersion = versions.VersionFromString(info.newVersion())
	    self[info.fileId().value()] = (oldVersion, newVersion, 
					   info.csInfo())

    def __init__(self, data = None):
	if data:
	    self.thaw(data)

_STREAM_CS_PRIMARY  = 1
_STREAM_CS_PKGS     = 2
_STREAM_CS_OLD_PKGS = 3
_STREAM_CS_FILES    = 4

class ChangeSet(streams.LargeStreamSet):

    streamDict = { 
        _STREAM_CS_PRIMARY :(streams.ReferencedTroveList, "primaryTroveList" ),
        _STREAM_CS_PKGS    :(ChangeSetNewPackageList,     "newPackages"      ),
        _STREAM_CS_OLD_PKGS:(streams.ReferencedTroveList, "oldPackages"      ),
        _STREAM_CS_FILES   :(ChangeSetFileDict,		  "files"            ),
    }

    def isAbsolute(self):
	return self.absolute

    def isLocal(self):
	return self.local

    def addPrimaryPackage(self, name, version, flavor):
	self.primaryTroveList.append((name, version, flavor))

    def getPrimaryPackageList(self):
	return self.primaryTroveList

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

    def configFileIsDiff(self, fileId):
        (tag, cont) = self.earlyFileContents.get(fileId, (None, None))
        return tag == ChangedFileTypes.diff

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
	for (pkgName, version, flavor) in self.primaryTroveList:
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

    def writeContents(self, csf, contents, early):
	# these are kept sorted so we know which one comes next
	idList = contents.keys()
	idList.sort()

	if early:
	    tag = "1 "
	else:
	    tag = "0 "

        # diffs come first, followed by plain files

	for hash in idList:
	    (contType, f) = contents[hash]
            if contType == ChangedFileTypes.diff:
                csf.addFile(hash, f, tag + contType[4:])

	for hash in idList:
	    (contType, f) = contents[hash]
            if contType != ChangedFileTypes.diff:
                csf.addFile(hash, f, tag + contType[4:])

    def writeAllContents(self, csf):
	self.writeContents(csf, self.earlyFileContents, True)
	self.writeContents(csf, self.lateFileContents, False)

    def writeToFile(self, outFileName):
	try:
	    outFile = open(outFileName, "w+")
	    csf = filecontainer.FileContainer(outFile)
	    outFile.close()

	    str = self.freeze()
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

                    try:
                        fsFile = files.FileFromFilesystem(fullPath, fileId,
                                    possibleMatch = origFile)
                    except OSError, e:
                        if e.errno != errno.ENOENT:
                            raise
                        fsFile = None

		    if fsFile and fsFile == origFile:
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
                    if self.configFileIsDiff(newFile.id()):
                        (contType, cont) = self.getFileContents(newFile.id())
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

                    if (isinstance(fsFile, files.RegularFile) and
                        fsFile.contents.sha1() == origFile.contents.sha1()):
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
		repos.getTrove(name, newVer, pkgCs.getNewFlavor())
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
	    for (name, list) in pkgCs.iterChangedTroves():
		if not packageVersions.has_key(name): continue
		for (change, version) in list:
		    if change != '+': continue

		    for (oldVer, newVer) in packageVersions[name]:
			if oldVer == version:
			    pkgCs.updateChangedPackage(name, oldVer, newVer)

	# this has to be true, I think...
	self.local = 0

    def __init__(self, data = None):
	streams.LargeStreamSet.__init__(self, data)
	self.earlyFileContents = {}
	self.lateFileContents = {}
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

    def fileQueueCmp(a, b):
        if a[1][0] == "1" and b[1][0] == "0":
            return -1
        elif a[1][0] == "0" and b[1][0] == "1":
            return 1

        if a[0] < b[0]:
            return -1
        elif a[0] == b[0]:
            assert(0)
        else:
            return 1

    fileQueueCmp = staticmethod(fileQueueCmp)

    def configFileIsDiff(self, fileId):
        (tag, str) = self.configCache.get(fileId, (None, None))
        return tag == ChangedFileTypes.diff

    def _nextFile(self):
        if self.lastCsf:
            next = self.lastCsf.getNextFile()
            if next:
                util.tupleListBsearchInsert(self.fileQueue, 
                                            next + (self.lastCsf,),
                                            self.fileQueueCmp)
            self.lastCsf = None

        if not self.fileQueue:
            return None

        rc = self.fileQueue[0]
        self.lastCsf = rc[4]
        del self.fileQueue[0]
        return rc

    def getFileContents(self, fileId, withSize = False):
	if self.configCache.has_key(fileId):
            name = fileId
	    (tag, str) = self.configCache[fileId]
	    cont = filecontents.FromString(str)
	    size = len(str)
	else:
            self.filesRead = True

            rc = self._nextFile()
            while rc:
                name, tagInfo, f, size, csf = rc
                
                # if we found the fileId we're looking for, or the fileId
                # we got is a config file, cache or break out of the loop
                # accordingly
                if name == fileId or tagInfo[0] == '1':
                    tag = 'cft-' + tagInfo.split()[1]
                    cont = filecontents.FromFile(f)

                    # we found the one we're looking for, break out
                    if name == fileId:
                        self.lastCsf = csf
                        break

                rc = self._nextFile()

        if name != fileId:
            raise KeyError, 'fileId %s is not in the changeset' % \
                            sha1helper.sha1ToString(fileId)
        elif withSize:
            return (tag, cont, size)
        else:
            return (tag, cont)

    def rootChangeSet(self, db, keepExisting):
	assert(self.absolute)

	# this has an empty source path template, which is only used to
	# construct the eraseFiles list anyway
	
	# we don't use our localrep.ChangeSetJob here as it can't deal with
	# absolute change sets
	job = RootChangeSetJob(db, self)

	# absolute change sets cannot have eraseLists
	#assert(not eraseList)
	#assert(not eraseFiles)

        # these get rebuilt
        self.primaryTroveList = []
        self.files = {}
        self.oldPackages = []           # since we ignore eraseList below, this
                                        # is always empty
	items = []
	for newPkg in job.newPackageList():
	    items.append((newPkg.getName(), newPkg.getVersion(), 
			  newPkg.getFlavor()))

	outdated, eraseList = db.outdatedTroves(items)
        # this ignores eraseList, juts like doUpdate does

	for newPkg in job.newPackageList():
	    pkgName = newPkg.getName()
	    newVersion = newPkg.getVersion()
	    newFlavor = newPkg.getFlavor()

	    key = (pkgName, newVersion, newFlavor)
	    if not outdated.has_key(key):
		log.warning("package %s %s is already installed -- skipping",
			    pkgName, newVersion.asString())
		continue

            if keepExisting:
                oldVersion = None
                oldFlavor = None
            else:
                (oldVersion, oldFlavor) = outdated[key][1:3]

	    if not oldVersion:
		# new package; the Package.diff() right after this never
		# sets the absolute flag, so the right thing happens
		old = None
	    else:
		old = db.getTrove(pkgName, oldVersion, oldFlavor,
					     pristine = True)

	    # we ignore pkgsNeeded; it doesn't mean much in this case
	    (pkgChgSet, filesNeeded, pkgsNeeded) = newPkg.diff(old, 
                                                               absolute = 0)
	    self.newPackage(pkgChgSet)
            filesNeeded.sort()

	    for (fileId, oldVersion, newVersion, oldPath, newPath) in filesNeeded:
		(fileObj, fileVersion) = job.getFile(fileId)
		assert(newVersion == fileVersion)
		
		oldFile = None
		if oldVersion:
		    (oldFile, oldCont) = db.getFileVersion(fileId, 
					    oldVersion, withContents = 1)

		(filecs, hash) = fileChangeSet(fileId, oldFile, fileObj)

		self.addFile(fileId, oldVersion, newVersion, filecs)

		if hash and oldVersion and \
                        oldFile.flags.isConfig() and fileObj.flags.isConfig():
		    contType = ChangedFileTypes.file
		    cont = filecontents.FromChangeSet(self, fileId)
		    if oldVersion:
			(contType, cont) = fileContentsDiff(oldFile, oldCont, 
                                                            fileObj, cont)

                    if contType == ChangedFileTypes.diff:
                        self.configCache[fileId] = (contType, cont.get().read())

        self.absolute = False

    def writeAllContents(self, csf):
        # diffs go out, then we write out whatever contents are left
        assert(not self.filesRead)
        self.filesRead = True

        idList = self.configCache.keys()
        idList.sort()

	for hash in idList:
	    (tag, str) = self.configCache[hash]
            csf.addFile(hash, filecontents.FromString(str), "1 diff")

        next = self._nextFile()
        while next:
            name, tagInfo, f, size, otherCsf = next
            csf.addFile(name, filecontents.FromFile(f, size = size), tagInfo)
            next = self._nextFile()

    def merge(self, otherCs):
        assert(self.__class__ == otherCs.__class__)
        assert(not self.lastCsf)
        assert(not otherCs.lastCsf)

        self.configCache.update(otherCs.configCache)
        self.files.update(otherCs.files)
        self.primaryTroveList += otherCs.primaryTroveList
        self.newPackages.update(otherCs.newPackages)
        self.oldPackages += otherCs.oldPackages

        for entry in otherCs.fileQueue:
            util.tupleListBsearchInsert(self.fileQueue, entry, 
                                        self.fileQueueCmp)

    def __init__(self, file, skipValidate = 1):
	f = open(file, "r")
	csf = filecontainer.FileContainer(f)
	f.close()

	(name, tagInfo, control, size) = csf.getNextFile()
        assert(name == "CONARYCHANGESET")

	start = control.read()
	ChangeSet.__init__(self, data = start)

	for trvCs in self.newPackages.itervalues():
	    if trvCs.isAbsolute():
		self.absolute = 1

	    old = trvCs.getOldVersion()
	    new = trvCs.getNewVersion()

	    if (old and old.isLocal()) or new.isLocal():
		self.local = 1

	self.configCache = {}
        self.filesRead = False

        self.lastCsf = None
        self.fileQueue = []

        # load the diff cache
        nextFile = csf.getNextFile()
        while nextFile:
            name, tagInfo, f, size = nextFile

            tag = 'cft-' + tagInfo.split()[1]

            # cache all config file contents
            if tag != ChangedFileTypes.diff:
                break

            cont = filecontents.FromFile(f)
            str = cont.get().read()
            size = len(str)
            self.configCache[name] = (tag, str)
            cont = filecontents.FromString(str)

            nextFile = csf.getNextFile()

        if nextFile:
            self.fileQueue.append(nextFile + (csf,))

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

class RootChangeSetJob(repository.ChangeSetJob):

    storeOnlyConfigFiles = True

    def addPackage(self, pkg):
	self.packages.append(pkg)

    def newPackageList(self):
	return self.packages

    def oldPackage(self, pkg):
	self.oldPackages.append(pkg)

    def oldPackageList(self):
	return self.oldPackages

    def oldFile(self, fileId, fileVersion, fileObj):
	self.oldFiles.append((fileId, fileVersion, fileObj))

    def oldFileList(self):
	return self.oldFiles

    def addFile(self, troveID, fileId, fileObj, path, version):
	if fileObj:
	    self.files[fileId] = (fileObj.freeze(), version)

    def addFileContents(self, fileObj, newVer, fileContents, restoreContents,
			isConfig):
	pass

    def getFile(self, fileId):
	info = self.files[fileId]
        return (files.ThawFile(info[0], fileId), info[1])

    def newFileList(self):
	return self.files.keys()

    def __init__(self, repos, absCs):
	self.packages = []
	self.oldPackages = []
	self.oldFiles = []
	self.files = {}
	repository.ChangeSetJob.__init__(self, repos, absCs)

