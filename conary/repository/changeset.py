#
# Copyright (c) 2004-2005 rPath, Inc.
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

from StringIO import StringIO
import struct
import tempfile
import difflib
import errno
import gzip
import itertools
import os

from conary import files, streams, trove, versions
from conary.deps import deps
from conary.lib import enum, log, patch, sha1helper, util
from conary.repository import filecontainer, filecontents, errors

# "refr" being the same length as "file" matters
# "ptr" is for links
# "hldr" means there are no contents and the file should be skipped
#    (used for rollbacks)
ChangedFileTypes = enum.EnumeratedType("cft", "file", "diff", "ptr",
                                       "refr", "hldr")

_STREAM_CS_PRIMARY  = 1
_STREAM_CS_TROVES     = 2
_STREAM_CS_OLD_TROVES = 3
_STREAM_CS_FILES    = 4

_FILEINFO_OLDFILEID = 1
_FILEINFO_NEWFILEID = 2
_FILEINFO_CSINFO    = 3

SMALL = streams.SMALL
LARGE = streams.LARGE

class FileInfo(streams.StreamSet):

    streamDict = {
        _FILEINFO_OLDFILEID : (SMALL, streams.StringStream, "oldFileId"),
        _FILEINFO_NEWFILEID : (SMALL, streams.StringStream, "newFileId"),
        _FILEINFO_CSINFO    : (LARGE, streams.StringStream, "csInfo"   )
        }
    __slots__ = [ "oldFileId", "newFileId", "csInfo" ]

    def __init__(self, first, newFileId = None, csInfo = None):
	if newFileId is None:
	    streams.StreamSet.__init__(self, first)
	else:
            streams.StreamSet.__init__(self)
            self.oldFileId.set(first)
            self.newFileId.set(newFileId)
            self.csInfo.set(csInfo)

class ChangeSetNewTroveList(dict, streams.InfoStream):

    def freeze(self, skipSet = None):
	l = []
	for trv in self.itervalues():
	    s = trv.freeze()
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

class ChangeSet(streams.StreamSet):

    streamDict = {
        _STREAM_CS_PRIMARY:
        (LARGE, streams.ReferencedTroveList, "primaryTroveList"),
        _STREAM_CS_TROVES:
        (LARGE, ChangeSetNewTroveList,       "newTroves"       ),
        _STREAM_CS_OLD_TROVES:
        (LARGE, streams.ReferencedTroveList, "oldTroves"       ),
        _STREAM_CS_FILES:
           (LARGE, ChangeSetFileDict,        "files"           ),
    }
    ignoreUnknown = True

    def _resetTroveLists(self):
        # XXX hack
        self.newTroves = ChangeSetNewTroveList()
        self.oldTroves = streams.ReferencedTroveList()

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

    def getPrimaryPackageList(self):
        import warnings
        warnings.warn("getPrimaryPackage is deprecated, use "
                      "getPrimaryTroveList", DeprecationWarning)
        return self.primaryTroveList

    def newTrove(self, csTrove):
	old = csTrove.getOldVersion()
	new = csTrove.getNewVersion()
	assert(not old or min(old.timeStamps()) > 0)
	assert(min(new.timeStamps()) > 0)

	self.newTroves[(csTrove.getName(), new,
                        csTrove.getNewFlavor())] = csTrove

	if csTrove.isAbsolute():
	    self.absolute = True
	if (old and old.onLocalLabel()) or new.onLocalLabel():
	    self.local = 1

    def newPackage(self, csTrove):
        import warnings
        warnings.warn("newPackage is deprecated, use newTrove",
                      DeprecationWarning)
        return self.newTrove(csTrove)

    def delNewTrove(self, name, version, flavor):
	del self.newTroves[(name, version, flavor)]
        if (name, version, flavor) in self.primaryTroveList:
            self.primaryTroveList.remove((name, version, flavor))

    def oldTrove(self, name, version, flavor):
	assert(min(version.timeStamps()) > 0)
	self.oldTroves.append((name, version, flavor))

    def hasOldTrove(self, name, version, flavor):
        return (name, version, flavor) in self.oldTroves

    def delOldTrove(self, name, version, flavor):
        self.oldTroves.remove((name, version, flavor))

    def iterNewTroveList(self):
	return self.newTroves.itervalues()

    def iterNewPackageList(self):
        import warnings
        warnings.warn("iterNewPackageList is deprecated", DeprecationWarning)
        return self.newTroves.itervalues()

    def getNewTroveVersion(self, name, version, flavor):
	return self.newTroves[(name, version, flavor)]

    def hasNewTrove(self, name, version, flavor):
	return self.newTroves.has_key((name, version, flavor))

    def getOldTroveList(self):
	return self.oldTroves

    def configFileIsDiff(self, pathId):
        (tag, cont, compressed) = self.configCache.get(pathId, (None, None, None))
        return tag == ChangedFileTypes.diff

    def addFileContents(self, pathId, contType, contents, cfgFile,
                        compressed = False):
	if cfgFile:
            assert(not compressed)
	    self.configCache[pathId] = (contType, contents, compressed)
	else:
	    self.fileContents[pathId] = (contType, contents, compressed)

    def getFileContents(self, pathId, compressed = False):
        assert(not compressed)
	if self.fileContents.has_key(pathId):
	    cont = self.fileContents[pathId]
	else:
	    cont = self.configCache[pathId]

        # this shouldn't be done on precompressed contents
        assert(not cont[2])
        cont = cont[:2]

	return cont

    def addFile(self, oldFileId, newFileId, csInfo):
        self.files[(oldFileId, newFileId)] = csInfo

    def formatToFile(self, cfg, f):
	f.write("primary troves:\n")
	for (troveName, version, flavor) in self.primaryTroveList:
	    if flavor:
		f.write("\t%s %s %s\n" % (troveName, version.asString(), 
					  flavor.freeze()))
	    else:
		f.write("\t%s %s\n" % (troveName, version.asString()))
	f.write("\n")

	for trv in self.newTroves.itervalues():
	    trv.formatToFile(self, f)
	for (troveName, version, flavor) in self.oldTroves:
	    f.write("remove %s %s\n" % (troveName, version.asString()))

    def getFileChange(self, oldFileId, newFileId):
	return self.files.get((oldFileId, newFileId), None)

    def _findFileChange(self, fileId):
        # XXX this is a linear search - do not use this method!
        # this only exists for AbstractTroveChangeSet.formatToFile()
        for oldFileId, newFileId in self.files.iterkeys():
            if newFileId == fileId:
                return oldFileId, self.files[(oldFileId, newFileId)]

    def writeContents(self, csf, contents, early, withReferences):
	# these are kept sorted so we know which one comes next
	idList = contents.keys()
	idList.sort()

        sizeCorrection = 0

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
                if withReferences and \
                       (isinstance(f, filecontents.FromDataStore) or \
                        isinstance(f, filecontents.CompressedFromDataStore)):
                    path = f.path()
                    realSize = os.stat(path).st_size
                    sizeCorrection += (realSize - len(path))
                    csf.addFile(hash, 
                                filecontents.FromString(f.path()),
                                tag + ChangedFileTypes.refr[4:],
                                precompressed = True)
                else:
                    csf.addFile(hash, f, tag + contType[4:],
                                precompressed = compressed)

        return sizeCorrection

    def writeAllContents(self, csf, withReferences):
        one = self.writeContents(csf, self.configCache, True, withReferences)
	two = self.writeContents(csf, self.fileContents, False, withReferences)

        return one + two

    def writeToFile(self, outFileName, withReferences = False):
	try:
	    outFile = open(outFileName, "w+")
	    csf = filecontainer.FileContainer(outFile)

	    str = self.freeze()
	    csf.addFile("CONARYCHANGESET", filecontents.FromString(str), "")
	    correction = self.writeAllContents(csf, 
                                               withReferences = withReferences)
	    csf.close()

            return os.stat(outFileName).st_size + correction
	except:
	    os.unlink(outFileName)
	    raise

    # if availableFiles is set, this includes the contents that it can
    # find, but doesn't worry about files which it can't find
    def makeRollback(self, db, configFiles = False, 
                     redirectionRollbacks = True):
	assert(not self.absolute)

        rollback = ChangeSet()

	for troveCs in self.iterNewTroveList():
	    if not troveCs.getOldVersion():
		# this was a new trove, and the inverse of a new
		# trove is an old trove
		rollback.oldTrove(troveCs.getName(), troveCs.getNewVersion(), 
				    troveCs.getNewFlavor())
		continue

            # if redirectionRollbacks are requested, create one for troves
            # which are not on the local branch (ones which exist in the
            # repository)
            if not troveCs.getOldVersion().isOnLocalHost() and \
               not troveCs.getNewVersion().isOnLocalHost() and \
               redirectionRollbacks:
                newTrove = trove.Trove(troveCs.getName(), 
                                       troveCs.getNewVersion(),
                                       troveCs.getNewFlavor(), None)
                oldTrove = trove.Trove(troveCs.getName(), 
                                       troveCs.getOldVersion(),
                                       troveCs.getOldFlavor(), None,
                                       isRedirect = True)
                rollback.newTrove(oldTrove.diff(newTrove)[0])
                continue

	    trv = db.getTrove(troveCs.getName(), troveCs.getOldVersion(),
                                troveCs.getOldFlavor())

            newTroveInfo = trove.TroveInfo(trv.getTroveInfo().freeze())
            newTroveInfo.twm(troveCs.getTroveInfoDiff(), newTroveInfo)
            newTroveInfoDiff = trv.getTroveInfo().diff(newTroveInfo)

	    # this is a modified trove and needs to be inverted

	    invertedTrove = trove.TroveChangeSet(troveCs.getName(), 
                                                 trv.getChangeLog(),
                                                 troveCs.getNewVersion(),
                                                 troveCs.getOldVersion(),
                                                 troveCs.getNewFlavor(),
                                                 troveCs.getOldFlavor(),
                                                 troveCs.getNewSigs(),
                                                 troveCs.getOldSigs(),
                                                 troveInfoDiff = newTroveInfoDiff)

            invertedTrove.setRequires(trv.getRequires())
            invertedTrove.setProvides(trv.getProvides())

	    for (name, list) in troveCs.iterChangedTroves():
		for (oper, version, flavor, byDef) in list:
		    if oper == '+':
			invertedTrove.oldTroveVersion(name, version, flavor)
		    elif oper == "-":
			invertedTrove.newTroveVersion(name, version, flavor, True)

	    for (pathId, path, fileId, version) in troveCs.getNewFileList():
		invertedTrove.oldFile(pathId)

	    for pathId in troveCs.getOldFileList():
                if not trv.hasFile(pathId):
                    # this file was removed using 'conary remove /path'
                    # so it does not go in the rollback
                    continue
                
		(path, fileId, version) = trv.getFile(pathId)
		invertedTrove.newFile(pathId, path, fileId, version)

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
						      origFile.contents.sha1())
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
                        contType = ChangedFileTypes.file
		    else:
			# a file which was removed in this changeset is
			# missing from the files; we need to to put an
			# empty file in here so we can apply the rollback
			cont = filecontents.FromString("")
                        contType = ChangedFileTypes.hldr

		    rollback.addFileContents(pathId, contType, cont, 0)

	    for (pathId, newPath, newFileId, newVersion) in troveCs.getChangedFileList():
		if not trv.hasFile(pathId):
		    # the file has been removed from the local system; we
		    # don't need to restore it on a rollback
		    continue
		(curPath, curFileId, curVersion) = trv.getFile(pathId)

		if newPath:
		    invertedTrove.changedFile(pathId, curPath, curFileId, curVersion)
		else:
		    invertedTrove.changedFile(pathId, None, curFileId, curVersion)

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

                if files.fileStreamIsDiff(csInfo):
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
				    origFile.contents.sha1())
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

	    rollback.newTrove(invertedTrove)

	for (name, version, flavor) in self.getOldTroveList():
            if not version.isOnLocalHost() and redirectionRollbacks:
                oldTrove = trove.Trove(name, version, flavor, None, 
                                       isRedirect = True)
                rollback.newTrove(oldTrove.diff(None)[0])
                continue

	    trv = db.getTrove(name, version, flavor)
	    troveDiff = trv.diff(None)[0]
	    rollback.newTrove(troveDiff)

            # everything in the rollback is considered primary
            rollback.addPrimaryTrove(name, version, flavor)

	    for (pathId, path, fileId, fileVersion) in trv.iterFileList():
		fileObj = db.getFileVersion(pathId, fileId, fileVersion)
		rollback.addFile(None, fileId, fileObj.freeze())
		if fileObj.hasContents:
		    fullPath = db.root + path

		    if os.path.exists(fullPath):
			fsFile = files.FileFromFilesystem(fullPath, pathId,
				    possibleMatch = fileObj)
		    else:
			fsFile = None

		    if fsFile and fsFile.hasContents and \
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
	Retargets this changeset to create troves and files on
	branch targetLabel off of the parent of the source node. Version
        calculations aren't quite right for source troves 
        (s/incrementBuildCount).

	@param repos: repository which will be committed to
	@type repos: repository.Repository
	@param targetBranchLabel: label of the branch to commit to
	@type targetBranchLabel: versions.Label
	"""
	assert(not targetBranchLabel == versions.LocalLabel())
        # if it's local, Versoin.parentVersion() has to work everywhere
        assert(self.isLocal())
        assert(not self.isAbsolute())

	troveVersions = {}

	for troveCs in self.iterNewTroveList():
	    name = troveCs.getName()
            origVer = troveCs.getNewVersion()

	    oldVer = troveCs.getOldVersion()
	    newVer = oldVer.createBranch(targetBranchLabel, withVerRel = 1)

	    # try and reuse the version number we created; if
	    # it's already in use we won't be able to though
	    try:
		repos.getTrove(name, newVer, troveCs.getNewFlavor())
	    except errors.TroveMissing: 
		pass
	    else:
		branch = oldVer.createBranch(targetBranchLabel, withVerRel = 0)
		newVer = repos.getTroveLatestVersion(name, branch)
                newVer.incrementBuildCount()

	    troveCs.changeNewVersion(newVer)
            assert(not troveVersions.has_key(name))
	    troveVersions[(name, troveCs.getNewFlavor())] = \
                                [ (origVer, newVer) ]

            for (listMethod, addMethod, resetMethod) in [
                    (troveCs.getChangedFileList, troveCs.changedFile,
                     troveCs.resetChangedFileList),
                    (troveCs.getNewFileList, troveCs.newFile,
                     troveCs.resetNewFileList) ]:
                fileList = [ x for x in listMethod() ]
                resetMethod()
                for (pathId, path, fileId, fileVersion) in fileList:
                    if fileVersion != "-" and fileVersion.onLocalLabel():
                        addMethod(pathId, path, fileId, newVer)

	for troveCs in self.iterNewTroveList():
	    # the implementation of updateChangedTrove makes this whole thing
	    # O(n^2) (n is the number of troves changed in troveCs), which is
	    # just silly. if large groups are added like this the effect could
	    # become noticeable
	    for (name, list) in troveCs.iterChangedTroves():
                for (change, version, flavor, absolute) in list:
		    if change != '+': continue

                    if not troveVersions.has_key((name, flavor)): continue

		    for (oldVer, newVer) in troveVersions[(name, flavor)]:
			if oldVer == version:
			    troveCs.updateChangedTrove(name, flavor, oldVer, 
                                                     newVer)

	# this has to be true, I think...
	self.local = 0

    def getJobSet(self, primaries = False):
        """
        Regenerates the primary change set job (passed to change set creation)
        for this change set.
        """
        jobSet = set()

        for trvCs in self.newTroves.values():
            if trvCs.getOldVersion():
                job = (trvCs.getName(), 
                       (trvCs.getOldVersion(), trvCs.getOldFlavor()),
                       (trvCs.getNewVersion(), trvCs.getNewFlavor()),
                       trvCs.isAbsolute())
            else:
                job = (trvCs.getName(), (None, None),
                       (trvCs.getNewVersion(), trvCs.getNewFlavor()),
                       trvCs.isAbsolute())

            if not primaries or \
                    (job[0], job[2][0], job[2][1]) in self.primaryTroveList:
                jobSet.add(job)

        for item in self.oldTroves:
            if not primaries or item in self.primaryTroveList:
                jobSet.add((item[0], (item[1], item[2]), 
                                (None, None), False))

        return jobSet
            
    def __init__(self, data = None):
	streams.StreamSet.__init__(self, data)
	self.configCache = {}
	self.fileContents = {}
	self.absolute = False
	self.local = 0

class ChangeSetFromAbsoluteChangeSet(ChangeSet):

    #streamDict = ChangeSet.streamDict

    def __init__(self, absCS):
	self.absCS = absCS
	ChangeSet.__init__(self)

class PathIdsConflictError(Exception): 
    def __init__(self, pathId, trove1=None, file1=None, 
                               trove2=None, file2=None):
        self.pathId = pathId
        self.trove1 = trove1
        self.file1 = file1
        self.trove2 = trove2
        self.file2 = file2

    def getPathId(self):
        return self.pathId

    def getConflicts(self):
        return (self.trove1, self.file1), (self.trove2, self.file2)

    def getTroves(self):
        return self.trove1, self.trove2
    
    def getPaths(self):
        return self.file1[1], self.file2[1]

    def __str__(self):
        if self.trove1 is None:
            return 'PathIdsConflict: %s' % sha1helper.md5ToString(self.pathId)
        else:
            path1, path2 = self.getPaths()
            trove1, trove2 = self.getTroves()
            v1 = trove1.getNewVersion().trailingRevision()
            v2 = trove2.getNewVersion().trailingRevision()
            trove1Info = '(%s %s)' % (trove1.getName(), v1)
            trove2Info = '(%s %s)' % (trove2.getName(), v2)
            if path1:
                trove1Info = path1 + ' ' + trove1Info
            if path2:
                trove2Info = path2 + ' ' + trove2Info

            return (('PathIdConflictsError:\n'
                     '  %s\n'
                     '     conflicts with\n'
                     '  %s') % (trove1Info, trove2Info))

class ReadOnlyChangeSet(ChangeSet):

    def fileQueueCmp(a, b):
        if a[1][0] == "1" and b[1][0] == "0":
            return -1
        elif a[1][0] == "0" and b[1][0] == "1":
            return 1

        if a[0] < b[0]:
            return -1
        elif a[0] == b[0]:
            raise PathIdsConflictError(a[0])
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
        self.lastCsf = rc[3]
        del self.fileQueue[0]

        return rc

    def getFileContents(self, pathId, compressed = False):
        name = None
	if self.configCache.has_key(pathId):
            assert(not compressed)
            name = pathId
	    (tag, contents, compressed) = self.configCache[pathId]

            if type(contents) == str:
                cont = filecontents.FromString(contents)
            else:
                cont = contents
	else:
            self.filesRead = True

            rc = self._nextFile()
            while rc:
                name, tagInfo, f, csf = rc
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
        else:
            return (tag, cont)

    def makeAbsolute(self, repos):
	"""
        Converts this (relative) change set to an abstract change set.  File
        streams and contents are omitted unless the file changed. This is fine
        for changesets being committed, not so hot for changesets which are
        being applied directly to a system. The absolute changeset is returned
        as a new changeset; self is left unchanged.
	"""
	assert(not self.absolute)

        absCs = ChangeSet()
        absCs.setPrimaryTroveList(self.getPrimaryTroveList())
        neededFiles = []

        oldTroveList = [ (x.getName(), x.getOldVersion(),
                          x.getOldFlavor()) for x in self.newTroves.values() ]
        oldTroves = repos.getTroves(oldTroveList)

	# for each file find the old fileId for it so we can assemble the
	# proper stream and contents
	for trv, troveCs in itertools.izip(oldTroves,
                                           self.newTroves.itervalues()):
	    troveName = troveCs.getName()
	    newVersion = troveCs.getNewVersion()
	    newFlavor = troveCs.getNewFlavor()
	    assert(troveCs.getOldVersion() == trv.getVersion())
            assert(trv.getName() == troveName)

            # XXX this is broken.  makeAbsolute() is only used for
            # committing local changesets, and they can't have new
            # files, so we're OK at the moment.
	    for (pathId, path, fileId, version) in troveCs.getNewFileList():
		filecs = self.files[(None, fileId)]
		newFiles.append((None, fileId, filecs))

	    for (pathId, path, fileId, version) in troveCs.getChangedFileList():
		(oldPath, oldFileId, oldVersion) = trv.getFile(pathId)
		filecs = self.files[(oldFileId, fileId)]
		neededFiles.append((pathId, oldFileId, fileId, oldVersion,
                                    version, filecs))

            # we've mucked around with this troveCs, it won't pass
            # integrity checks
	    trv.applyChangeSet(troveCs, skipIntegrityChecks = True)
	    newCs = trv.diff(None, absolute = True)[0]
	    absCs.newTrove(newCs)

	fileList = [ (x[0], x[1], x[3]) for x in neededFiles ]
	fileObjs = repos.getFileVersions(fileList)

        # XXX this would be markedly more efficient if we batched up getting
        # file contents
	for ((pathId, oldFileId, newFileId, oldVersion, newVersion, filecs), 
                        fileObj) in itertools.izip(neededFiles, fileObjs):
	    fileObj.twm(filecs, fileObj)
	    (absFileCs, hash) = fileChangeSet(pathId, None, fileObj)
	    absCs.addFile(None, newFileId, absFileCs)

            if newVersion != oldVersion and fileObj.hasContents:
		# we need the contents as well
                if files.contentsChanged(filecs):
                    if fileObj.flags.isConfig():
                        # config files aren't available compressed
                        (contType, cont) = self.getFileContents(pathId)
                        if contType == ChangedFileTypes.diff:
                            origCont = repos.getFileContents([(oldFileId, 
                                                               oldVersion)])[0]
                            diff = cont.get().readlines()
                            oldLines = origCont.get().readlines()
                            (newLines, failures) = patch.patch(oldLines, diff)
                            assert(not failures)
                            fileContents = filecontents.FromString(
                                                            "".join(newLines))
                            absCs.addFileContents(pathId, 
                                                  ChangedFileTypes.file, 
                                                  fileContents, True)
                        else:
                            absCs.addFileContents(pathId, ChangedFileTypes.file,
                                                  cont, True)
                    else:
                        (contType, cont) = self.getFileContents(pathId,
                                                        compressed = True)
                        assert(contType == ChangedFileTypes.file)
                        absCs.addFileContents(pathId, ChangedFileTypes.file,
                                              cont, False, compressed = True)
                else:
                    # include the old contents; we might need them for
                    # a distributed branch
                    cont = repos.getFileContents([(oldFileId, oldVersion)])[0]
                    absCs.addFileContents(pathId, ChangedFileTypes.file, cont,
                                          fileObj.flags.isConfig())

        return absCs

    def rootChangeSet(self, db, troveMap):
	"""
	Converts this (absolute) change set to a relative change
	set. The second parameter, troveMap, specifies the old trove
	for each trove listed in this change set. It is a dictionary
	mapping (troveName, newVersion, newFlavor) tuples to 
	(oldVersion, oldFlavor) pairs. The troveMap may be (None, None)
	if a new install is desired (the trove is switched from absolute
        to relative to nothing in this case). If an entry is missing for
        a trove, that trove is left absolute.
	"""
	assert(self.absolute)

	# this has an empty source path template, which is only used to
	# construct the eraseFiles list anyway
	
	# absolute change sets cannot have eraseLists
	#assert(not eraseFiles)

	newFiles = []
	newTroves = []

	for (key, troveCs) in self.newTroves.items():
	    troveName = troveCs.getName()
	    newVersion = troveCs.getNewVersion()
	    newFlavor = troveCs.getNewFlavor()
	    assert(not troveCs.getOldVersion())

            if key not in troveMap:
                continue

            (oldVersion, oldFlavor) = troveMap[key]

	    if not oldVersion:
		# new trove; the Trove.diff() right after this never
		# sets the absolute flag, so the right thing happens
		old = None
	    else:
		old = db.getTrove(troveName, oldVersion, oldFlavor,
					     pristine = True)
	    newTrove = trove.Trove(troveName, None, deps.DependencySet(), None)
	    newTrove.applyChangeSet(troveCs)

	    # we ignore trovesNeeded; it doesn't mean much in this case
	    (troveChgSet, filesNeeded, trovesNeeded) = \
                          newTrove.diff(old, absolute = 0)
	    newTroves.append(troveChgSet)
            filesNeeded.sort()

	    for x in filesNeeded:
                (pathId, oldFileId, oldVersion, newFileId, newVersion) = x
                filecs = self.getFileChange(None, newFileId)

		if not oldVersion:
		    newFiles.append((oldFileId, newFileId, filecs))
		    continue
		
		fileObj = files.ThawFile(filecs, pathId)
		(oldFile, oldCont) = db.getFileVersion(pathId, 
				oldFileId, oldVersion, withContents = 1)
		(filecs, hash) = fileChangeSet(pathId, oldFile, fileObj)

		newFiles.append((oldFileId, newFileId, filecs))

		if (hash and oldVersion and oldFile.flags.isConfig()
                    and fileObj.flags.isConfig()):
		    contType = ChangedFileTypes.file
		    cont = filecontents.FromChangeSet(self, pathId)
		    if oldVersion:
			(contType, cont) = fileContentsDiff(oldFile, oldCont, 
                                                            fileObj, cont)

                    if contType == ChangedFileTypes.diff:
                        self.configCache[pathId] = (contType,
                                                    cont.get().read(), False)

	self.files = {}
	for tup in newFiles:
	    self.addFile(*tup)

	for troveCs in newTroves:
	    self.newTrove(troveCs)

        self.absolute = False

    def writeAllContents(self, csf, withReferences = False):
        # diffs go out, then we write out whatever contents are left
        assert(not self.filesRead)
        assert(not withReferences)
        self.filesRead = True

        idList = self.configCache.keys()
        idList.sort()

	for hash in idList:
	    (tag, str, compressed) = self.configCache[hash]
            csf.addFile(hash, filecontents.FromString(str), "1 " + tag[4:])

        next = self._nextFile()
        while next:
            name, tagInfo, f, otherCsf = next
            csf.addFile(name, filecontents.FromFile(f), tagInfo,
                        precompressed = True)
            next = self._nextFile()

        return 0

    def merge(self, otherCs):
        self.files.update(otherCs.files)

        self.primaryTroveList += otherCs.primaryTroveList
        self.newTroves.update(otherCs.newTroves)

        # keep the old trove lists unique on merge.  we erase all the
        # entries and extend the existing oldTroves object because it
        # is a streams.ReferencedTroveList, not a regular list
        if otherCs.oldTroves:
            l = dict.fromkeys(self.oldTroves + otherCs.oldTroves).keys()
            del self.oldTroves[:]
            self.oldTroves.extend(l)

        if isinstance(otherCs, ReadOnlyChangeSet):
            assert(not self.lastCsf)
            assert(not otherCs.lastCsf)

            self.configCache.update(otherCs.configCache)

            try:
                for entry in otherCs.fileQueue:
                    util.tupleListBsearchInsert(self.fileQueue, entry, 
                                                self.fileQueueCmp)
            except PathIdsConflictError, err:
                pathId = err.pathId
                # look up the trove and file that caused the pathId 
                # conflict.  
                for myTrove in self.iterNewTroveList():
                    files = (myTrove.getNewFileList() 
                             + myTrove.getChangedFileList())
                    conflict1 = [ x for x in files if x[0] == pathId]
                    if conflict1:
                        conflict1 = conflict1[0]
                        break
                for otherTrove in otherCs.iterNewTroveList():
                    files = (otherTrove.getNewFileList() 
                             + otherTrove.getChangedFileList())
                    conflict2 = [ x for x in files if x[0] == pathId]
                    if conflict2:
                        conflict2 = conflict2[0]
                        break
                assert(conflict1 and conflict2)
                raise PathIdsConflictError(pathId, myTrove, conflict1,
                                                   otherTrove, conflict2)

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

	(name, tagInfo, control) = csf.getNextFile()
        assert(name == "CONARYCHANGESET")

	start = gzip.GzipFile(None, 'r', fileobj = control).read()
	ReadOnlyChangeSet.__init__(self, data = start)

	self.absolute = True
	empty = True

	for trvCs in self.newTroves.itervalues():
	    if not trvCs.isAbsolute():
		self.absolute = False
	    empty = False

	    old = trvCs.getOldVersion()
	    new = trvCs.getNewVersion()

	    if (old and old.onLocalLabel()) or new.onLocalLabel():
		self.local = 1

	if empty:
	    self.absolute = False

        # load the diff cache
        nextFile = csf.getNextFile()
        while nextFile:
            name, tagInfo, f = nextFile

            (isConfig, tag) = tagInfo.split()
            tag = 'cft-' + tag
            isConfig = isConfig == "1"

            # relative change sets only need diffs cached; absolute change
            # sets get all of their config files cached so we can turn
            # those into diffs. those cached values are replaced by the
            # diffs when this happens though, so this isn't a big loss
            if tag != ChangedFileTypes.diff and not(self.absolute and isConfig):
                break

            cont = filecontents.FromFile(gzip.GzipFile(None, 'r', fileobj = f))
            s = cont.get().read()
            self.configCache[name] = (tag, s, False)
            cont = filecontents.FromString(s)

            nextFile = csf.getNextFile()

        if nextFile:
            self.fileQueue.append(nextFile + (csf,))

# old may be None
def fileChangeSet(pathId, old, new):
    contentsHash = None

    diff = new.diff(old)

    if old and old.__class__ == new.__class__:
	if isinstance(new, files.RegularFile) and      \
		  isinstance(old, files.RegularFile)   \
		  and ((new.contents.sha1() != old.contents.sha1()) or
                       (not old.flags.isConfig() and new.flags.isConfig())):
	    contentsHash = new.contents.sha1()
    elif isinstance(new, files.RegularFile):
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
# expects a list of (trove, fileMap) tuples
#
def CreateFromFilesystem(troveList):
    cs = ChangeSet()

    for (trv, fileMap) in troveList:
	(troveChgSet, filesNeeded, trovesNeeded) = trv.diff(None, absolute = 1)
	cs.newTrove(troveChgSet)

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
        os.unlink(path)
        gzf = gzip.GzipFile('', "wb", fileobj = os.fdopen(os.dup(fd), "w"))
        util.copyfileobj(f, gzf)
        gzf.close()
        f.close()
        os.lseek(fd, 0, 0)
        f = os.fdopen(fd, "r")
        return (name, contType, f)

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
