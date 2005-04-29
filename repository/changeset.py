#
# Copyright (c) 2004-2005 Specifix, Inc.
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
from lib import enum
from lib import log
import errno
import filecontainer
import filecontents
import files
import gzip
import os
from lib import patch
import repository
from lib import sha1helper
import streams
import struct
import tempfile
import trove
from lib import util
import versions

from StringIO import StringIO

ChangedFileTypes = enum.EnumeratedType("cft", "file", "diff", "ptr")

_STREAM_CS_PRIMARY  = 1
_STREAM_CS_PKGS     = 2
_STREAM_CS_OLD_PKGS = 3
_STREAM_CS_FILES    = 4

class FileInfo(streams.TupleStream):

    __slots__ = []

    # pathId, oldVersion, newVersion, csInfo
    makeup = (("oldFileId", streams.StringStream, "B"),
	      ("newFileId", streams.StringStream, 20),
	      ("csInfo", streams.StringStream, "B"))

    def oldFileId(self):
        return self.items[0]()

    def newFileId(self):
        return self.items[1]()

    def csInfo(self):
        return self.items[2]()

    def __init__(self, first, newFileId = None, chg = None):
	if newFileId is None:
	    streams.TupleStream.__init__(self, first)
	else:
	    streams.TupleStream.__init__(self, first, newFileId, chg)

class RollbackRecordNewPackges(dict, streams.InfoStream):

    def freeze(self, skipSet = None):
	l = []
	for newInfo, oldInfo in self.iteritems():
            if oldInfo[0] is None:
                l.append('')
            else:
                l.append(oldInfo[0])
                l.append(oldInfo[1].asString())
                l.append(oldInfo[2].freeze())

            l.append(newInfo[0])
            l.append(newInfo[1].asString())
            l.append(newInfo[2].freeze())
	
	return "\0".join(l)

    def thaw(self, data):
        l = data.split('\0')
	i = 0
	while i < len(l):
            if l[i]:
                oldInfo = (l[i], versions.VersionFromString(l[i + 1]),
                           deps.ThawDependencySet(l[i + 2]))
                i += 3
            else:
                oldInfo = (None, None, None)
                i += 1

            newInfo = (l[i], versions.VersionFromString(l[i + 1]),
                       deps.ThawDependencySet(l[i + 2]))
            i += 3
            self[newInfo] = oldInfo

    def __init__(self, data = None):
	if data:
	    self.thaw(data)

class RollbackRecord(streams.LargeStreamSet):

    streamDict = { 
        _STREAM_CS_PKGS:    (RollbackRecordNewPackges,    "newPackages"      ),
        _STREAM_CS_OLD_PKGS:(streams.ReferencedTroveList, "oldPackages"      ),
    }

    """
    Subsitutes for a changeset when full rollback information is
    retrieved from a repository instead of being stored locally in
    a changeset.
    """

    ROLLBACK_RECORD_MAGIC = "\xEA\x3F\x81\xBC"

    def writeToFile(self, outFileName):
        f = open(outFileName, 'w')
        f.write(self.ROLLBACK_RECORD_MAGIC)
        f.write(self.freeze())

    def __init__(self, changeSet = None, fileName = None):
        assert(not(changeSet and fileName))

        if changeSet:
            streams.LargeStreamSet.__init__(self)
            for pkgCs in changeSet.iterNewPackageList():
                newInfo = (pkgCs.getName(), pkgCs.getNewVersion(),
                           pkgCs.getNewFlavor())

                if pkgCs.getOldVersion():
                    oldInfo = (pkgCs.getName(), pkgCs.getOldVersion(),
                               pkgCs.getOldFlavor())

                    self.newPackages[oldInfo] = newInfo
                else:
                    self.oldPackages.append(newInfo)

            for (name, version, flavor) in changeSet.getOldPackageList():
                self.newPackages[(name, version, flavor)] = (None, None, None)
        else:
            f = open(fileName, "r")
            magic = f.read(len(self.ROLLBACK_RECORD_MAGIC))
            if magic != self.ROLLBACK_RECORD_MAGIC:
                raise filecontainer.BadContainer

            data = f.read()
            streams.LargeStreamSet.__init__(self, data)

class ChangeSetNewPackageList(dict, streams.InfoStream):

    def freeze(self, skipSet = None):
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

    def freeze(self, skipSet = None):
	fileList = []
	for ((oldFileId, newFileId), (csInfo)) in self.iteritems():
	    if not oldFileId:
                oldFileId = ""

	    s = FileInfo(oldFileId, newFileId, csInfo).freeze()
	    fileList.append(struct.pack("!I", len(s)) + s)

	return "".join(fileList)

    def thaw(self ,data):
	i = 0
	while i < len(data):
	    size = struct.unpack("!I", data[i:i+4])[0]
	    i += 4
	    info = FileInfo(data[i:i+size])
	    i += size
	    
            oldFileId = info.oldFileId()
            if oldFileId == "":
                oldFileId = None

            newFileId = info.newFileId()
            self[(oldFileId, newFileId)] = info.csInfo()

    def __init__(self, data = None):
	if data:
	    self.thaw(data)

class ChangeSet(streams.LargeStreamSet):

    streamDict = { 
        _STREAM_CS_PRIMARY :(streams.ReferencedTroveList, "primaryTroveList" ),
        _STREAM_CS_PKGS    :(ChangeSetNewPackageList,     "newPackages"      ),
        _STREAM_CS_OLD_PKGS:(streams.ReferencedTroveList, "oldPackages"      ),
        _STREAM_CS_FILES   :(ChangeSetFileDict,		  "files"            ),
    }

    ignoreUnknown = True

    def isAbsolute(self):
	return self.absolute

    def isLocal(self):
	return self.local

    def addPrimaryTrove(self, name, version, flavor):
        assert(flavor is not None)
	self.primaryTroveList.append((name, version, flavor))

    def setPrimaryTroveList(self, l):
        del self.primaryTroveList[:]
        self.primaryTroveList.extend(l)

    def getPrimaryTroveList(self):
	return self.primaryTroveList

    def newPackage(self, csPkg):
	old = csPkg.getOldVersion()
	new = csPkg.getNewVersion()
	assert(not old or min(old.timeStamps()) > 0)
	assert(min(new.timeStamps()) > 0)

	self.newPackages[(csPkg.getName(), csPkg.getNewVersion(),
		          csPkg.getNewFlavor())] = csPkg

	if csPkg.isAbsolute():
	    self.absolute = True
	if (old and old.isLocal()) or new.isLocal():
	    self.local = 1

    def delNewPackage(self, name, version, flavor):
	del self.newPackages[(name, version, flavor)]
        if (name, version, flavor) in self.primaryTroveList:
            self.primaryTroveList.remove((name, version, flavor))

    def oldPackage(self, name, version, flavor):
	assert(min(version.timeStamps()) > 0)
	self.oldPackages.append((name, version, flavor))

    def hasOldPackage(self, name, version, flavor):
        return (name, version, flavor) in self.oldPackages

    def delOldPackage(self, name, version, flavor):
        self.oldPackages.remove((name, version, flavor))

    def iterNewPackageList(self):
	return self.newPackages.itervalues()

    def getNewPackageVersion(self, name, version, flavor):
	return self.newPackages[(name, version, flavor)]

    def hasNewPackage(self, name, version, flavor):
	return self.newPackages.has_key((name, version, flavor))

    def getOldPackageList(self):
	return self.oldPackages

    def configFileIsDiff(self, pathId):
        (tag, cont) = self.configCache.get(pathId, (None, None, None))
        return tag == ChangedFileTypes.diff

    def addFileContents(self, pathId, contType, contents, cfgFile,
                        compressed = False):
	if cfgFile:
            assert(not compressed)
	    self.configCache[pathId] = (contType, contents, compressed)
	else:
	    self.fileContents[pathId] = (contType, contents, compressed)

    def getFileContents(self, pathId, withSize = False, compressed = False):
        assert(not compressed)
	if self.fileContents.has_key(pathId):
	    cont = self.fileContents[pathId]
	else:
	    cont = self.configCache[pathId]

        # this shouldn't be done on precompressed contents
        assert(not cont[2])
        cont = cont[:2]

	if not withSize:
	    return cont

	return cont + (cont.size(), )

    def addFile(self, oldFileId, newFileId, csInfo):
        self.files[(oldFileId, newFileId)] = csInfo

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

    def getFileChange(self, oldFileId, newFileId):
	return self.files.get((oldFileId, newFileId), None)

    def _findFileChange(self, fileId):
        # XXX this is a linear search - do not use this method!
        # this only exists for AbstractTroveChangeSet.formatToFile()
        for oldFileId, newFileId in self.files.iterkeys():
            if newFileId == fileId:
                return oldFileId, self.files[(oldFileId, newFileId)]

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
	    (contType, f, compressed) = contents[hash]
            if contType == ChangedFileTypes.diff:
                csf.addFile(hash, f, tag + contType[4:],
                            precompressed = compressed)

	for hash in idList:
	    (contType, f, compressed) = contents[hash]
            if contType != ChangedFileTypes.diff:
                csf.addFile(hash, f, tag + contType[4:],
                            precompressed = compressed)

    def writeAllContents(self, csf):
	self.writeContents(csf, self.configCache, True)
	self.writeContents(csf, self.fileContents, False)

    def writeToFile(self, outFileName):
	try:
	    outFile = open(outFileName, "w+")
	    csf = filecontainer.FileContainer(outFile)

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

            newTroveInfo = trove.TroveInfo(pkg.getTroveInfo().freeze())
            newTroveInfo.twm(pkgCs.getTroveInfoDiff(), newTroveInfo)
            newTroveInfoDiff = pkg.getTroveInfo().diff(newTroveInfo)

	    # this is a modified package and needs to be inverted

	    invertedPkg = trove.TroveChangeSet(pkgCs.getName(), 
			       pkg.getChangeLog(),
			       pkgCs.getNewVersion(), pkgCs.getOldVersion(),
			       pkgCs.getNewFlavor(), pkgCs.getOldFlavor(),
                               troveInfoDiff = newTroveInfoDiff)

	    for (name, list) in pkgCs.iterChangedTroves():
		for (oper, version, flavor, byDef) in list:
		    if oper == '+':
			invertedPkg.oldTroveVersion(name, version, flavor)
		    elif oper == "-":
			invertedPkg.newTroveVersion(name, version, flavor, True)

	    for (pathId, path, fileId, version) in pkgCs.getNewFileList():
		invertedPkg.oldFile(pathId)

	    for pathId in pkgCs.getOldFileList():
                if not pkg.hasFile(pathId):
                    # this file was removed using 'conary remove /path'
                    # so it does not go in the rollback
                    continue
                
		(path, fileId, version) = pkg.getFile(pathId)
		invertedPkg.newFile(pathId, path, fileId, version)

		origFile = db.getFileVersion(pathId, fileId, version)
		rollback.addFile(None, fileId, origFile.freeze())

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
		    rollback.addFileContents(pathId,
					     ChangedFileTypes.file, cont, 1)
		else:
		    fullPath = db.root + path

                    try:
                        fsFile = files.FileFromFilesystem(fullPath, pathId,
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

		    rollback.addFileContents(pathId, ChangedFileTypes.file, 
					     cont, 0)

	    for (pathId, newPath, newFileId, newVersion) in pkgCs.getChangedFileList():
		if not pkg.hasFile(pathId):
		    # the file has been removed from the local system; we
		    # don't need to restore it on a rollback
		    continue
		(curPath, curFileId, curVersion) = pkg.getFile(pathId)

		if newPath:
		    invertedPkg.changedFile(pathId, curPath, curFileId, curVersion)
		else:
		    invertedPkg.changedFile(pathId, None, curFileId, curVersion)

                try:
                    csInfo = self.files[(curFileId, newFileId)]
                except KeyError:
                    log.error('File objects stored in your database do '
                              'not match the same version of those file '
                              'objects in the repository. The best thing '
                              'to do is erase the version on your system '
                              'by using "conary erase --just-db --no-deps" '
                              'and then run the update again by using '
                              '"conary update --replace-files"')
                    continue
                    
                origFile = db.getFileVersion(pathId, curFileId, curVersion)

                if csInfo[0] == "\x01":
                    # this is a diff, not an absolute change
                    newFile = origFile.copy()
                    newFile.twm(csInfo, origFile)
                else:
                    newFile = files.ThawFile(csInfo, pathId)

		rollback.addFile(newFileId, curFileId, origFile.diff(newFile))

		if not isinstance(origFile, files.RegularFile):
		    continue

		# If a config file has changed between versions, save
		# it; if it hasn't changed the unmodified version will
		# still be available from the database when the rollback
		# gets applied. We may be able to get away with just reversing
		# a diff rather then saving the full contents
		if origFile.flags.isConfig() and newFile.flags.isConfig() and \
                        (origFile.contents.sha1() != newFile.contents.sha1()):
                    if self.configFileIsDiff(newFile.pathId()):
                        (contType, cont) = self.getFileContents(newFile.pathId())
			f = cont.get()
			diff = "".join(patch.reverse(f.readlines()))
			f.seek(0)
			cont = filecontents.FromString(diff)
			rollback.addFileContents(pathId,
						 ChangedFileTypes.diff, cont, 1)
		    else:
			cont = filecontents.FromDataStore(db.contentsStore, 
				    origFile.contents.sha1(),
                                    origFile.contents.size())
			rollback.addFileContents(pathId,
						 ChangedFileTypes.file, cont,
						 newFile.flags.isConfig())
		elif origFile.hasContents and newFile.hasContents and \
                            origFile.contents.sha1() != newFile.contents.sha1():
		    # this file changed, so we need the contents
		    fullPath = db.root + curPath
		    fsFile = files.FileFromFilesystem(fullPath, pathId,
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

		    rollback.addFileContents(pathId,
					     ChangedFileTypes.file, cont,
					     origFile.flags.isConfig() or
					     newFile.flags.isConfig())

	    rollback.newPackage(invertedPkg)

	for (name, version, flavor) in self.getOldPackageList():
	    pkg = db.getTrove(name, version, flavor)
	    pkgDiff = pkg.diff(None)[0]
	    rollback.newPackage(pkgDiff)

            # everything in the rollback is considered primary
            rollback.addPrimaryTrove(name, version, flavor)

	    for (pathId, path, fileId, fileVersion) in pkg.iterFileList():
		fileObj = db.getFileVersion(pathId, fileId, fileVersion)
		rollback.addFile(None, fileId, fileObj.freeze())
		if fileObj.hasContents:
		    fullPath = db.root + path

		    if os.path.exists(fullPath):
			fsFile = files.FileFromFilesystem(fullPath, pathId,
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

		    rollback.addFileContents(pathId,
					     ChangedFileTypes.file, cont,
					     fileObj.flags.isConfig())

	return rollback

    def setTargetBranch(self, repos, targetBranchLabel):
	"""
	Retargets this changeset to create packages and files on
	branch targetLabel off of the source node.

	@param repos: repository which will be committed to
	@type repos: repository.Repository
	@param targetBranchLabel: label of the branch to commit to
	@type targetBranchLabel: versions.Label
	"""
	assert(not targetBranchLabel == versions.LocalLabel())

	packageVersions = {}

	for pkgCs in self.iterNewPackageList():
	    name = pkgCs.getName()
	    oldVer = pkgCs.getOldVersion()
	    ver = pkgCs.getNewVersion()
	    # what to do about versions for new packages?
	    assert(oldVer)

	    newBr = oldVer.createBranch(targetBranchLabel, withVerRel = 0)
	    newVer = newBr.createVersion(ver.trailingRevision())
            del newBr

	    # try and reuse the version number we created; if
	    # it's already in use we won't be able to though
	    try:
		repos.getTrove(name, newVer, pkgCs.getNewFlavor())
	    except repository.TroveMissing: 
		pass
	    else:
		branch = oldVer.createBranch(targetBranchLabel, withVerRel = 0)
		newVer = repos.getTroveLatestVersion(name, branch)

	    pkgCs.changeNewVersion(newVer)
	    if not packageVersions.has_key(name):
		packageVersions[name] = []
	    packageVersions[name].append((ver, newVer))

            # FILEID
            # this is just hosed. needs attention.

	    # files on the local branch get remapped; others don't
	    if self.isLocal(): 
		for (listMethod, addMethod) in [
			(pkgCs.getChangedFileList, pkgCs.changedFile),
			(pkgCs.getNewFileList, pkgCs.newFile) ]:
		    for (pathId, path, fileId, fileVersion) in listMethod():
			if fileVersion != "-" and fileVersion.isLocal():
			    addMethod(pathId, path, newVer)
			    oldVer = self.getFileOldVersion(pathId)
			    csInfo = self.getFileChange(pathId)
                            (otherPathId, oldVer, otherNewVer, csInfo) = \
                                self.getFile()
			    # this replaces the existing file 
			    self.addFile(pathId, oldVer, newVer, csInfo)

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
	self.configCache = {}
	self.fileContents = {}
	self.absolute = False
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

class PathIdsConflictError(Exception): pass

class ReadOnlyChangeSet(ChangeSet):

    def fileQueueCmp(a, b):
        if a[1][0] == "1" and b[1][0] == "0":
            return -1
        elif a[1][0] == "0" and b[1][0] == "1":
            return 1

        if a[0] < b[0]:
            return -1
        elif a[0] == b[0]:
            raise PathIdsConflictError
        else:
            return 1

    fileQueueCmp = staticmethod(fileQueueCmp)

    def configFileIsDiff(self, pathId):
        (tag, str, compressed) = self.configCache.get(pathId, 
                                                      (None, None, None))
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

    def getFileContents(self, pathId, withSize = False, compressed = False):
        name = None
	if self.configCache.has_key(pathId):
            name = pathId
	    (tag, contents, compressed) = self.configCache[pathId]

            if type(contents) == str:
                cont = filecontents.FromString(contents)
                size = len(contents)
            else:
                cont = contents
                size = contents.size()
	else:
            self.filesRead = True

            rc = self._nextFile()
            while rc:
                name, tagInfo, f, size, csf = rc
                if not compressed:
                    f = gzip.GzipFile(None, "r", fileobj = f)
                
                # if we found the pathId we're looking for, or the pathId
                # we got is a config file, cache or break out of the loop
                # accordingly
                if name == pathId or tagInfo[0] == '1':
                    tag = 'cft-' + tagInfo.split()[1]
                    cont = filecontents.FromFile(f)

                    # we found the one we're looking for, break out
                    if name == pathId:
                        self.lastCsf = csf
                        break

                rc = self._nextFile()

        if name != pathId:
            raise KeyError, 'pathId %s is not in the changeset' % \
                            sha1helper.md5ToString(pathId)
        elif withSize:
            return (tag, cont, size)
        else:
            return (tag, cont)

    def rootChangeSet(self, db, troveMap):
	"""
	Converts this (absolute) change set to a realative change
	set. The second parameter, troveMap, specifies the old trove
	for each trove listed in this change set. It is a dictionary
	mapping (troveName, newVersion, newFlavor) tuples to 
	(oldVersion, oldFlavor) pairs. If troveMap is None, then old
	versions are preserved (this is used to implement keepExisting),
	otherwise it is an error if no mapping exists (the mapping
	can specify (None, None) if their is no previous version available
	to compare against.
	"""
	assert(self.absolute)

	# this has an empty source path template, which is only used to
	# construct the eraseFiles list anyway
	
	# absolute change sets cannot have eraseLists
	#assert(not eraseFiles)

	newFiles = []
	newPackages = []

	for (key, troveCs) in self.newPackages.items():
	    troveName = troveCs.getName()
	    newVersion = troveCs.getNewVersion()
	    newFlavor = troveCs.getNewFlavor()
	    assert(not troveCs.getOldVersion())

	    if troveMap is not None and not troveMap.has_key(key):
		log.warning("package %s %s is already installed -- skipping",
			    troveName, newVersion.asString())
                del self.newPackages[key]
                if key in self.primaryTroveList:
                    self.primaryTroveList.remove(key)
		continue

            if troveMap is None:
                oldVersion = None
                oldFlavor = None
            else:
                (oldVersion, oldFlavor) = troveMap[key]

	    if not oldVersion:
		# new package; the Package.diff() right after this never
		# sets the absolute flag, so the right thing happens
		old = None
	    else:
		old = db.getTrove(troveName, oldVersion, oldFlavor,
					     pristine = True)
	    newPkg = trove.Trove(troveName, None, deps.DependencySet(), None)
	    newPkg.applyChangeSet(troveCs)

	    # we ignore pkgsNeeded; it doesn't mean much in this case
	    (pkgChgSet, filesNeeded, pkgsNeeded) = newPkg.diff(old, 
                                                               absolute = 0)
	    newPackages.append(pkgChgSet)
            filesNeeded.sort()

	    for (pathId, oldFileId, oldVersion, newFileId, newVersion) in filesNeeded:
                filecs = self.getFileChange(None, newFileId)
		fileObj = files.ThawFile(filecs, pathId)
		
		oldFile = None
		if oldVersion:
		    (oldFile, oldCont) = db.getFileVersion(pathId, 
                                    oldFileId, oldVersion, withContents = 1)

		(filecs, hash) = fileChangeSet(pathId, oldFile, fileObj)

		newFiles.append((oldFileId, newFileId, filecs))

		if hash and oldVersion and \
                        oldFile.flags.isConfig() and fileObj.flags.isConfig():
		    contType = ChangedFileTypes.file
		    cont = filecontents.FromChangeSet(self, pathId)
		    if oldVersion:
			(contType, cont) = fileContentsDiff(oldFile, oldCont, 
                                                            fileObj, cont)

                    if contType == ChangedFileTypes.diff:
                        self.configCache[pathId] = (contType, cont.get().read(),
                                                    False)

	self.files = {}
	for tup in newFiles:
	    self.addFile(*tup)

	self.packages = []
	for pkgCs in newPackages:
	    self.newPackage(pkgCs)

        self.absolute = False

    def writeAllContents(self, csf):
        # diffs go out, then we write out whatever contents are left
        assert(not self.filesRead)
        self.filesRead = True

        idList = self.configCache.keys()
        idList.sort()

	for hash in idList:
	    (tag, str, compressed) = self.configCache[hash]
            csf.addFile(hash, filecontents.FromString(str), "1 " + tag[4:])

        next = self._nextFile()
        while next:
            name, tagInfo, f, size, otherCsf = next
            csf.addFile(name, filecontents.FromFile(f, size = size), tagInfo,
                        precompressed = True)
            next = self._nextFile()

    def merge(self, otherCs):
        self.files.update(otherCs.files)
        self.primaryTroveList += otherCs.primaryTroveList
        self.newPackages.update(otherCs.newPackages)
        # keep the old package lists unique on merge.  we erase all the
        # entries and extend the existing oldPackages object because it
        # is a streams.ReferencedTroveList, not a regular list
        if otherCs.oldPackages:
            l = dict.fromkeys(self.oldPackages + otherCs.oldPackages).keys()
            del self.oldPackages[:]
            self.oldPackages.extend(l)

        if isinstance(otherCs, ReadOnlyChangeSet):
            assert(not self.lastCsf)
            assert(not otherCs.lastCsf)

            self.configCache.update(otherCs.configCache)

            for entry in otherCs.fileQueue:
                util.tupleListBsearchInsert(self.fileQueue, entry, 
                                            self.fileQueueCmp)
        else:
            assert(otherCs.__class__ ==  ChangeSet)

            # make a copy. the configCache should only store diffs
            configs = {}

            for (pathId, (contType, contents, compressed)) in \
                                    otherCs.configCache.iteritems():
                assert(not compressed)
                if contType == ChangedFileTypes.diff:
                    self.configCache[pathId] = (contType, contents, compressed)
                else:
                    configs[pathId] = (contType, contents, compressed)
                    
            wrapper = dictAsCsf(otherCs.fileContents)
            wrapper.addConfigs(configs)
            entry = wrapper.getNextFile()
            if entry:
                util.tupleListBsearchInsert(self.fileQueue,
                                            entry + (wrapper,), 
                                            self.fileQueueCmp)


    def __init__(self, data = None):
	ChangeSet.__init__(self, data = data)
	self.configCache = {}
        self.filesRead = False

        self.lastCsf = None
        self.fileQueue = []

class ChangeSetFromFile(ReadOnlyChangeSet):

    def __init__(self, fileName, skipValidate = 1):
        if type(fileName) is str:
            f = open(fileName, "r")
            csf = filecontainer.FileContainer(f)
        else:
            csf = filecontainer.FileContainer(fileName)

	(name, tagInfo, control, size) = csf.getNextFile()
        assert(name == "CONARYCHANGESET")

	start = gzip.GzipFile(None, "r", fileobj = control).read()
	ReadOnlyChangeSet.__init__(self, data = start)

	self.absolute = True
	empty = True

	for trvCs in self.newPackages.itervalues():
	    if not trvCs.isAbsolute():
		self.absolute = False
	    empty = False

	    old = trvCs.getOldVersion()
	    new = trvCs.getNewVersion()

	    if (old and old.isLocal()) or new.isLocal():
		self.local = 1

	if empty:
	    self.absolute = False

        # load the diff cache
        nextFile = csf.getNextFile()
        while nextFile:
            name, tagInfo, f, size = nextFile

            (isConfig, tag) = tagInfo.split()
            tag = 'cft-' + tag
            isConfig = isConfig == "1"

            # relative change sets only need diffs cached; absolute change
            # sets get all of their config files cached so we can turn
            # those into diffs. those cached values are replaced by the
            # diffs when this happens though, so this isn't a big loss
            if tag != ChangedFileTypes.diff and not(self.absolute and isConfig):
                break

            cont = filecontents.FromFile(gzip.GzipFile(None, "r", fileobj = f))
            s = cont.get().read()
            size = len(s)
            self.configCache[name] = (tag, s, False)
            cont = filecontents.FromString(s)

            nextFile = csf.getNextFile()

        if nextFile:
            self.fileQueue.append(nextFile + (csf,))

# old may be None
def fileChangeSet(pathId, old, new):
    contentsHash = None

    if old and old.__class__ == new.__class__:
	diff = new.diff(old)
	if isinstance(new, files.RegularFile) and      \
		  isinstance(old, files.RegularFile)   \
		  and new.contents.sha1() != old.contents.sha1():
	    contentsHash = new.contents.sha1()
    else:
	# different classes; these are always written as absolute changes
	old = None
	diff = new.freeze()
	if isinstance(new, files.RegularFile):
	    contentsHash = new.contents.sha1()

    return (diff, contentsHash)

def fileContentsUseDiff(oldFile, newFile):
    return oldFile and oldFile.flags.isConfig() and newFile.flags.isConfig()

def fileContentsDiff(oldFile, oldCont, newFile, newCont):
    if fileContentsUseDiff(oldFile, newFile):
	first = oldCont.get().readlines()
	second = newCont.get().readlines()

        # XXX difflib (and probably our patch as well) don't work properly
        # for files w/o trailing newlines.  But it can handle empty files.
        # Though we do need either the first or the second to do
        # a diff.  Diffing two empty files yields an empty file.
	if ((first or second) and
            (not first or first[-1][-1] == '\n') and
            (not second or second[-1][-1] == '\n')):
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
	(pkgChgSet, filesNeeded, pkgsNeeded) = pkg.diff(None, absolute = 1)
	cs.newPackage(pkgChgSet)

	for (pathId, oldFileId, oldVersion, newFileId, newVersion) in filesNeeded:
	    (file, realPath, filePath) = fileMap[pathId]
	    (filecs, hash) = fileChangeSet(pathId, None, file)
	    cs.addFile(oldFileId, newFileId, filecs)

	    if hash:
		cs.addFileContents(pathId, ChangedFileTypes.file,
			  filecontents.FromFilesystem(realPath),
			  file.flags.isConfig())

    return cs

class dictAsCsf:

    def getNextFile(self):
        if not self.items:
            return None

        (name, contType, contObj) = self.items[0]
        del self.items[0]

        # XXX there must be a better way, but I can't think of it
        f = contObj.get()
        (fd, path) = tempfile.mkstemp(suffix = '.cf-out')
        #os.unlink(path)
        gzf = gzip.GzipFile(path, "wb", fileobj = os.fdopen(os.dup(fd), "w"))
        util.copyfileobj(f, gzf)
        del f
        del gzf
        os.lseek(fd, 0, 0)
        f = os.fdopen(fd, "r")
        return (name, contType, f, contObj.size())

    def addConfigs(self, contents):
        # this is like __init__, but it knows things are config files so
        # it tags them with a "1" and puts them at the front
        l = [ (x[0], "1 " + x[1][0][4:], x[1][1]) 
                        for x in contents.iteritems() ]
        l.sort()
        self.items = l + self.items

    def __init__(self, contents):
        # convert the dict (which is a changeSet.fileContents object) to
        # a (name, contTag, contObj) list, where contTag is the same kind
        # of tag we use in csf files "[0|1] [file|diff]"
        self.items = [ (x[0], "0 " + x[1][0][4:], x[1][1]) for x in 
                            contents.iteritems() ]
        self.items.sort()
