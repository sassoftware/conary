#
# Copyright (c) 2004-2009 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import struct
import tempfile
import errno
import gzip
import itertools
import os

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

from conary import files, streams, trove, versions
from conary.lib import enum, log, misc, patch, sha1helper, util, api
from conary.repository import filecontainer, filecontents, errors

# cft is a string used by the EnumeratedType class; it's not a type itself!
#
# "refr" being the same length as "file" matters. it means a path to a file's
#    contents are stored, not the file itself. it's used for repository
#    side changesets to avoid storing contents repeatedly
# "ptr" is for duplicate file contents in a changeset (including hardlinks)
# "hldr" means there are no contents and the file should be skipped
#    (used for locally stored rollbacks when the original file contents can't
#     be ascertained)
# "diff" means the file is stored as a unified diff, not absolute contents
# "file" means the file contents are stored normally
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

def makeKey(pathId, fileId):
    return pathId + fileId

def parseKey(key):
    return key[0:16], key[16:]

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
        l = [ x[1].freeze() for x in sorted(self.items()) ]
        return misc.pack("!" + "SI" * len(l), *l)

    def thaw(self, data):
        while self:
            self.clear()

	i = 0
	while i < len(data):
            i, (s,) = misc.unpack("!SI", i, data)
	    trvCs = trove.ThawTroveChangeSet(s)

	    self[(trvCs.getName(), trvCs.getNewVersion(),
					  trvCs.getNewFlavor())] = trvCs

    def __init__(self, data = None):
	if data:
	    self.thaw(data)

class ChangeSetFileDict(dict, streams.InfoStream):

    def freeze(self, skipSet = None):
	fileList = []
	for ((oldFileId, newFileId), (csInfo)) in sorted(self.iteritems()):
	    if not oldFileId:
                oldFileId = ""

	    s = FileInfo(oldFileId, newFileId, csInfo).freeze()
	    fileList.append(struct.pack("!I", len(s)) + s)

	return "".join(fileList)

    def thaw(self ,data):
	i = 0
	while i < len(data):
            i, ( frzFile, ) = misc.unpack("!SI", i, data)
            info = FileInfo(frzFile)

            oldFileId = info.oldFileId()
            if oldFileId == "":
                oldFileId = None

            newFileId = info.newFileId()
            self[(oldFileId, newFileId)] = info.csInfo()

    def __init__(self, data = None):
	if data:
	    self.thaw(data)

class ChangeSetFileContentsTuple(tuple):

    """
    Wrapper class for for the tuples stored in ChangeSet.configCache and
    ChangeSet.fileContents dicts which allow them to be sent through
    util.SendableFileSet.
    """

    _tag = 'ccs-tup'

    def _sendInfo(self):
        contType, contents, compressed = self

        if compressed:
            c = '1'
        else:
            c = '0'

        return ([ contents ], c + contType)

    @staticmethod
    def _fromInfo(fileObjList, s):
        if s[0] == '0':
            compressed = False
        else:
            compressed = True

        return ChangeSetFileContentsTuple((s[1:], fileObjList[0], compressed))
util.SendableFileSet._register(ChangeSetFileContentsTuple)

class ChangeSetFileContentsDict(dict):

    """
    Wrapper class for the ChangeSet.configCache and ChangeSet.fileContents
    dicts which can be sent through a util.SendableFileSet.
    """

    _tag = 'ccs-cd'

    def __hash__(self):
        return id(self)

    def _sendInfo(self):
        s = "".join([ "%s%s" % (struct.pack("B", len(x)), x)
                        for x in self.iterkeys() ])
        return (self.values(), s)

    @staticmethod
    def _fromInfo(fileObjList, s):
        d = ChangeSetFileContentsDict()
        if not fileObjList:
            return d

        i = 0
        for fileObj in fileObjList:
            keyLen = struct.unpack("B", s[i])[0]
            i += 1
            key = s[i:i + keyLen]
            i += keyLen
            d[key] = fileObj

        return d

util.SendableFileSet._register(ChangeSetFileContentsDict)

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
    _tag = 'ccs-rw'

    def _resetTroveLists(self):
        # XXX hack
        self.newTroves = ChangeSetNewTroveList()
        self.oldTroves = streams.ReferencedTroveList()

    def isEmpty(self):
        return not bool(self.newTroves) and not bool(self.oldTroves)

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

    @api.publicApi
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

    @api.publicApi
    def iterNewTroveList(self):
        """
        @return: dictionary-valueiterator object
        """
	return self.newTroves.itervalues()

    def iterNewPackageList(self):
        import warnings
        warnings.warn("iterNewPackageList is deprecated", DeprecationWarning)
        return self.newTroves.itervalues()

    @api.publicApi
    def getNewTroveVersion(self, name, version, flavor):
	return self.newTroves[(name, version, flavor)]

    def hasNewTrove(self, name, version, flavor):
	return self.newTroves.has_key((name, version, flavor))

    def getOldTroveList(self):
	return self.oldTroves

    def configFileIsDiff(self, pathId, fileId):
        key = makeKey(pathId, fileId)
        (tag, cont, compressed) = self.configCache.get(key, (None, None, None))
        if tag is None:
            (tag, cont, compressed) = self.configCache.get(pathId,
                                                           (None, None, None))
        return tag == ChangedFileTypes.diff

    def addFileContents(self, pathId, fileId, contType, contents, cfgFile,
                        compressed = False):
        key = makeKey(pathId, fileId)
        if key in self.configCache or key in self.fileContents:
            if key in self.configCache:
                otherContType = self.configCache[key]
            else:
                otherContType = self.fileContents[key]

            if (contType == ChangedFileTypes.diff or
                 otherContType == ChangedFileTypes.diff):
                raise ChangeSetKeyConflictError(key)

	if cfgFile:
            if compressed:
                s = util.decompressString(contents.get().read())
                contents = filecontents.FromString(s)
                compressed = False

            self.configCache[key] = ChangeSetFileContentsTuple((contType,
                                                                contents,
                                                                compressed))
        else:
            self.fileContents[key] = ChangeSetFileContentsTuple((contType,
                                                                 contents,
                                                                 compressed))

    def getFileContents(self, pathId, fileId, compressed = False):
        key = makeKey(pathId, fileId)
	if self.fileContents.has_key(key):
	    cont = self.fileContents[key]
	else:
	    cont = self.configCache[key]

        if compressed and cont[2]:
            # we have compressed contents, and we've been asked for compressed
            # contnets
            pass
        else:
            # ensure we have uncompressed contents, and we're being asked for
            # uncompressed contents
            assert(not compressed)
            assert(not cont[2])

        cont = cont[:2]

	return cont

    def addFile(self, oldFileId, newFileId, csInfo):
        self.files[(oldFileId, newFileId)] = csInfo

    def formatToFile(self, cfg, f):
	f.write("primary troves:\n")
	for (troveName, version, flavor) in self.primaryTroveList:
	    if flavor.isEmpty():
		f.write("\t%s %s\n" % (troveName, version.asString()))
	    else:
		f.write("\t%s %s %s\n" % (
                    troveName, version.asString(), flavor.freeze()))
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
                        isinstance(f, filecontents.CompressedFromDataStore):
                    path = f.path()
                    realSize = os.stat(path).st_size
                    sizeCorrection += (realSize - len(path))
                    if realSize >= 0x100000000:
                        # add 4 bytes to store a 64-bit size
                        sizeCorrection += 4
                    csf.addFile(hash, 
                                filecontents.FromString(path,
                                                        compressed = True),
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

    def appendToFile(self, outFile, withReferences = False,
                     versionOverride = None):
        start = outFile.tell()

        csf = filecontainer.FileContainer(outFile,
                                          version = versionOverride,
                                          append = True)

        str = self.freeze()
        csf.addFile("CONARYCHANGESET", filecontents.FromString(str), "")
        correction = self.writeAllContents(csf, 
                                           withReferences = withReferences)
        return (outFile.tell() - start) + correction

    def writeToFile(self, outFileName, withReferences = False, mode = 0666,
                    versionOverride = None):
        # 0666 is right for mode because of umask
	try:
            outFileFd = os.open(outFileName,
                                os.O_RDWR | os.O_CREAT | os.O_TRUNC, mode)

            outFile = os.fdopen(outFileFd, "w+")

            size = self.appendToFile(outFile, withReferences = withReferences,
                                     versionOverride = versionOverride)
            outFile.close()
            return size
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
                                       type = trove.TROVE_TYPE_REDIRECT)
                rollback.newTrove(oldTrove.diff(newTrove)[0])
                continue

	    trv = db.getTrove(troveCs.getName(), troveCs.getOldVersion(),
                                troveCs.getOldFlavor())
            newTroveInfo = troveCs.getTroveInfo()
            if newTroveInfo is None:
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
            invertedTrove.setTroveInfo(trv.troveInfo)

            for weak in (True, False):
                for (name, list) in troveCs.iterChangedTroves(
                                strongRefs = not weak, weakRefs = weak):
                    for (oper, version, flavor, byDef) in list:
                        if oper == '+':
                            invertedTrove.oldTroveVersion(name, version, flavor,
                                                          weakRef = weak)
                        elif oper == "-":
                            invertedTrove.newTroveVersion(name, version, flavor,
                               trv.includeTroveByDefault(name, version, flavor),
                               weakRef = weak)
                        elif oper == "~":
                            # invert byDefault flag
                            invertedTrove.changedTrove(name, version, flavor, not byDef,
                                                       weakRef = weak)

	    for (pathId, path, origFileId, version) in troveCs.getNewFileList():
		invertedTrove.oldFile(pathId)

	    for pathId in troveCs.getOldFileList():
                if not trv.hasFile(pathId):
                    # this file was removed using 'conary remove /path'
                    # so it does not go in the rollback
                    continue
                
		(path, origFileId, version) = trv.getFile(pathId)
		invertedTrove.newFile(pathId, path, origFileId, version)

		origFile = db.getFileVersion(pathId, origFileId, version)
		rollback.addFile(None, origFileId, origFile.freeze())

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
                    rollback.addFileContents(pathId, origFileId,
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

		    if fsFile and fsFile.contents == origFile.contents:
			cont = filecontents.FromFilesystem(fullPath)
                        contType = ChangedFileTypes.file
		    else:
			# a file which was removed in this changeset is
			# missing from the files; we need to to put an
			# empty file in here so we can apply the rollback
			cont = filecontents.FromString("")
                        contType = ChangedFileTypes.hldr

		    rollback.addFileContents(pathId, origFileId, contType,
                                             cont, 0)

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

                if curFileId == newFileId:
                    continue

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
                    if self.configFileIsDiff(newFile.pathId(), newFileId):
                        (contType, cont) = self.getFileContents(
                                    newFile.pathId(), newFileId)
			f = cont.get()
			diff = "".join(patch.reverse(f.readlines()))
			f.seek(0)
			cont = filecontents.FromString(diff)
                        rollback.addFileContents(pathId, curFileId,
						 ChangedFileTypes.diff, cont, 1)
		    else:
			cont = filecontents.FromDataStore(db.contentsStore, 
				    origFile.contents.sha1())
                        rollback.addFileContents(pathId, curFileId,
						 ChangedFileTypes.file, cont,
						 newFile.flags.isConfig())
		elif origFile.hasContents and newFile.hasContents and \
                            origFile.contents.sha1() != newFile.contents.sha1():
		    # this file changed, so we need the contents
		    fullPath = db.root + curPath
                    try:
                        fsFile = files.FileFromFilesystem(fullPath, pathId,
                                    possibleMatch = origFile)
                    except OSError, err:
                        if err.errno == errno.ENOENT:
                            # the file doesn't exist - the user removed
                            # it manually.  This will make us store
                            # just an empty string as contents
                            fsFile = None
                        else:
                            raise

                    if (isinstance(fsFile, files.RegularFile) and
                        fsFile.contents.sha1() == origFile.contents.sha1()):
			# the contents in the file system are right
			cont = filecontents.FromFilesystem(fullPath)
                        contType = ChangedFileTypes.file
		    else:
			# the contents in the file system are wrong; insert
			# a placeholder and let the local change set worry
			# about getting this right
			cont = filecontents.FromString("")
                        contType = ChangedFileTypes.hldr

                    rollback.addFileContents(pathId, curFileId, contType, cont,
					     origFile.flags.isConfig() or
					     newFile.flags.isConfig())

	    rollback.newTrove(invertedTrove)

	for (name, version, flavor) in self.getOldTroveList():
            if not version.isOnLocalHost() and redirectionRollbacks:
                oldTrove = trove.Trove(name, version, flavor, None, 
                                       type = trove.TROVE_TYPE_REDIRECT)
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

                    if fileObj.flags.isConfig():
                        cont = filecontents.FromDataStore(db.contentsStore,
                                    fileObj.contents.sha1())
                        # make a copy of the contents in memory in case
                        # the database gets changed
                        cont = filecontents.FromString(cont.get().read())
                        rollback.addFileContents(pathId, fileId,
                                                 ChangedFileTypes.file, cont,
                                                 fileObj.flags.isConfig())
                        continue

		    if os.path.exists(fullPath):
			fsFile = files.FileFromFilesystem(fullPath, pathId,
				    possibleMatch = fileObj)
		    else:
			fsFile = None

		    if fsFile and fsFile.hasContents and \
			    fsFile.contents.sha1() == fileObj.contents.sha1():
			# the contents in the file system are right
			cont = filecontents.FromFilesystem(fullPath)
                        contType = ChangedFileTypes.file
		    else:
			# the contents in the file system are wrong; insert
			# a placeholder and let the local change set worry
			# about getting this right
			cont = filecontents.FromString("")
                        contType = ChangedFileTypes.hldr

                    rollback.addFileContents(pathId, fileId, contType, cont,
					     fileObj.flags.isConfig())

	return rollback

    def setTargetShadow(self, repos, targetShadowLabel):
	"""
	Retargets this changeset to create troves and files on
	shadow targetLabel off of the parent of the source node. Version
        calculations aren't quite right for source troves 
        (s/incrementBuildCount).

	@param repos: repository which will be committed to
	@type repos: repository.Repository
	@param targetShadowLabel: label of the branch to commit to
	@type targetShadowLabel: versions.Label
	"""
	assert(not targetShadowLabel == versions.LocalLabel())
        # if it's local, Version.parentVersion() has to work everywhere
        assert(self.isLocal())
        assert(not self.isAbsolute())

	troveVersions = {}

        troveCsList = [ (x.getName(), x) for x in self.iterNewTroveList() ]
        troveCsList.sort()
        troveCsList.reverse()
        origTroveList = repos.getTroves([ (x[1].getName(), x[1].getOldVersion(),
                                           x[1].getOldFlavor()) 
                                          for x in troveCsList ])

        # this loop needs to handle components before packages; reverse
        # sorting by name ensures that
        #
        # XXX this is busted for groups 

	for (name, troveCs), oldTrv in \
                                itertools.izip(troveCsList, origTroveList):
            origVer = troveCs.getNewVersion()

	    oldVer = troveCs.getOldVersion()
            assert(oldVer is not None)
            newVer = oldVer.createShadow(targetShadowLabel)
            newVer.incrementBuildCount()

            if repos.hasTrove(name, newVer, troveCs.getNewFlavor()):
                newVer = repos.getTroveLatestVersion(name, newVer.branch()).copy()
                newVer.incrementBuildCount()

            newTrv = oldTrv.copy()
            newTrv.applyChangeSet(troveCs)

            newTrv.changeVersion(newVer)
            newTrv.invalidateDigests()
            newTrv.computeDigests()

            assert(not troveVersions.has_key(name))
            troveVersions[(name, troveCs.getNewFlavor())] = \
                                [ (origVer, newVer) ]

            fileList = [ x for x in newTrv.iterFileList() ]
            for (pathId, path, fileId, fileVersion) in fileList:
                if not fileVersion.onLocalLabel(): continue
                newTrv.updateFile(pathId, path, newVer, fileId)

            subTroves = [ x for x in newTrv.iterTroveListInfo() ]
            for (name, subVersion, flavor), byDefault, isStrong in subTroves:
                if not troveVersions.has_key((name, flavor)): continue

                newTrv.delTrove(name, subVersion, flavor, missingOkay = False)
                newTrv.addTrove(name, newVer, flavor, byDefault = byDefault,
                                weakRef = (not isStrong))

            # throw away sigs and recompute the hash
            newTrv.invalidateDigests()
            newTrv.computeDigests()

            self.delNewTrove(troveCs.getName(), troveCs.getNewVersion(),
                             troveCs.getNewFlavor())
            troveCs = newTrv.diff(oldTrv)[0]
            self.newTrove(troveCs)

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

    def clearTroves(self):
        """
        Reset the newTroves and oldTroves list for this changeset. File
        information is preserved.
        """
        self.primaryTroveList.thaw("")
        self.newTroves.thaw("")
        self.oldTroves.thaw("")

    def removeCommitted(self, repos):
        """
        Walk a changeset and remove and items which are already in the
        repositories. Returns a changeset which will commit without causing
        duplicate trove errors. If everything in the changeset has already
        been committed, return False. If there are items left for commit,
        return True.

        @param cs: Changeset to filter
        @type cs: repository.changeset.ChangeSet
        @rtype: repository.changeset.ChangeSet or None
        """
        newTroveInfoList = [ x.getNewNameVersionFlavor() for x in
                                self.iterNewTroveList() if x.getNewVersion()
                                is not None ]
        present = repos.hasTroves(newTroveInfoList)

        for (newTroveInfo, isPresent) in present.iteritems():
            if isPresent:
                self.delNewTrove(*newTroveInfo)

        if self.newTroves:
            return True

        return False

    def _sendInfo(self):
        new = self.newTroves.freeze()
        old = self.oldTroves.freeze()

        s = self.freeze()

        return ([ self.configCache, self.fileContents ], s)

    @staticmethod
    def _fromInfo(fileObjList, s):
        cs = ChangeSet(s)
        cs.configCache, cs.fileContents = fileObjList
        return cs

    def send(self, sock):
        """
        Sends this changeset over a unix-domain socket. This object remains
        a valid changeset (it is not affected by the send operation).

        @param sock: File descriptor for unix domain socket
        @type sock: int
        """
        fileSet = util.SendableFileSet()
        fileSet.add(self)
        fileSet.send(sock)

    def __init__(self, data = None):
	streams.StreamSet.__init__(self, data)
	self.configCache = ChangeSetFileContentsDict()
	self.fileContents = ChangeSetFileContentsDict()
	self.absolute = False
	self.local = 0
util.SendableFileSet._register(ChangeSet)

class ChangeSetFromAbsoluteChangeSet(ChangeSet):

    #streamDict = ChangeSet.streamDict

    def __init__(self, absCS):
	self.absCS = absCS
	ChangeSet.__init__(self)

class ChangeSetKeyConflictError(Exception):

    name = "ChangeSetKeyConflictError"

    def __init__(self, key, trove1=None, file1=None, trove2=None, file2=None):
        if len(key) == 16:
            self.pathId = key
            self.fileId = None
        else:
            self.pathId, self.fileId = parseKey(key)

        self.trove1 = trove1
        self.file1 = file1
        self.trove2 = trove2
        self.file2 = file2

    def getKey(self):
        if self.fileId:
            return self.pathId + self.fileId
        else:
            return self.pathId

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
            return '%s: %s,%s' % (self.name,
                                  sha1helper.md5ToString(self.pathId),
                                  sha1helper.sha1ToString(self.fileId))
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

            return (('%s:\n'
                     '  %s\n'
                     '     conflicts with\n'
                     '  %s') % (self.name, trove1Info, trove2Info))

class PathIdsConflictError(ChangeSetKeyConflictError):

    name = "PathIdsConflictError"

    def __str__(self):
        if self.trove1 is None:
            return '%s: %s' % (self.name, sha1helper.md5ToString(self.pathId))
        else:
            return ChangeSetKeyConflictError.__str__(self)

class ReadOnlyChangeSet(ChangeSet):

    def addFileContents(self, *args, **kw):
        raise NotImplementedError

    def fileQueueCmp(a, b):
        if a[1][0] == "1" and b[1][0] == "0":
            return -1
        elif a[1][0] == "0" and b[1][0] == "1":
            return 1

        if a[0] < b[0]:
            return -1
        elif a[0] == b[0]:
            if len(a[0]) == 16:
                raise PathIdsConflictError(a[0])
            else:
                # this is an actual conflict if one of the files is a diff
                # (other file types conflicts are okay; replacing contents
                # with a ptr is okay, as is the opposite)
                if (a[2:] == ChangedFileTypes.diff[4:] or
                    b[2:] == ChangedFileTypes.diff[4:]):
                    raise ChangeSetKeyConflictError(a[0])
        else:
            return 1

    fileQueueCmp = staticmethod(fileQueueCmp)

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

    def getFileContents(self, pathId, fileId, compressed = False):
        name = None
        key = makeKey(pathId, fileId)
	if self.configCache.has_key(pathId):
            assert(not compressed)
            name = pathId
	    (tag, contents, alreadyCompressed) = self.configCache[pathId]
            cont = contents
	elif self.configCache.has_key(key):
            name = key
	    (tag, contents, alreadyCompressed) = self.configCache[key]

            cont = contents

            if compressed:
                f = util.BoundedStringIO()
                compressor = gzip.GzipFile(None, "w", fileobj = f)
                util.copyfileobj(cont.get(), compressor)
                compressor.close()
                f.seek(0)
                cont = filecontents.FromFile(f, compressed = True)
	else:
            self.filesRead = True

            rc = self._nextFile()
            while rc:
                name, tagInfo, f, csf = rc
                if not compressed:
                    f = gzip.GzipFile(None, "r", fileobj = f)
                
                # if we found the key we're looking for, or the pathId
                # we got is a config file, cache or break out of the loop
                # accordingly
                #
                # we check for both the key and the pathId here for backwards
                # compatibility reading old change set formats
                if name == key or name == pathId or tagInfo[0] == '1':
                    tag = 'cft-' + tagInfo.split()[1]
                    cont = filecontents.FromFile(f, compressed = compressed)

                    # we found the one we're looking for, break out
                    if name == key or name == pathId:
                        self.lastCsf = csf
                        break

                rc = self._nextFile()

        if name != key and name != pathId:
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
            if trv.troveInfo.incomplete():
                raise errors.TroveError('''\
Cannot apply a relative changeset to an incomplete trove.  Please upgrade conary and/or reinstall %s=%s[%s].''' % (trv.getName(), trv.getVersion(),
                                   trv.getFlavor()))
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
                        (contType, cont) = self.getFileContents(
                                     pathId, newFileId)
                        if contType == ChangedFileTypes.diff:
                            origCont = repos.getFileContents([(oldFileId, 
                                                               oldVersion)])[0]
                            diff = cont.get().readlines()
                            oldLines = origCont.get().readlines()
                            (newLines, failures) = patch.patch(oldLines, diff)
                            assert(not failures)
                            fileContents = filecontents.FromString(
                                                            "".join(newLines))
                            absCs.addFileContents(pathId, newFileId,
                                                  ChangedFileTypes.file, 
                                                  fileContents, True)
                        else:
                            absCs.addFileContents(pathId, newFileId,
                                                  ChangedFileTypes.file,
                                                  cont, True)
                    else:
                        (contType, cont) = self.getFileContents(pathId,
                                                newFileId, compressed = True)
                        assert(contType == ChangedFileTypes.file)
                        absCs.addFileContents(pathId, newFileId,
                                              ChangedFileTypes.file,
                                              cont, False, compressed = True)
                else:
                    # include the old contents; we might need them for
                    # a distributed branch
                    cont = repos.getFileContents([(oldFileId, oldVersion)])[0]
                    absCs.addFileContents(pathId, newFileId,
                                          ChangedFileTypes.file, cont,
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

        Rooting can happen multiple times (only once per trove though). To
        allow this, the absolute file streams remain available from this
        changeset for all time; rooting does not remove them.
	"""
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

            if key not in troveMap:
                continue

            assert(not troveCs.getOldVersion())
            assert(troveCs.isAbsolute())

            (oldVersion, oldFlavor) = troveMap[key]

	    if not oldVersion:
		# new trove; the Trove.diff() right after this never
		# sets the absolute flag, so the right thing happens
		old = None
	    else:
		old = db.getTrove(troveName, oldVersion, oldFlavor,
					     pristine = True)
	    newTrove = trove.Trove(troveCs)

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

        # leave the old files in place; we my need those diffs for a
        # trvCs which hasn't been rooted yet
	for tup in newFiles:
	    self.addFile(*tup)

	for troveCs in newTroves:
	    self.newTrove(troveCs)

        self.absolute = False

    def writeAllContents(self, csf, withReferences = False):
        # diffs go out, then config files, then we whatever contents are left
        assert(not self.filesRead)
        assert(not withReferences)
        self.filesRead = True

        keyList = self.configCache.keys()
        keyList.sort()

        # write out the diffs. these are always in the cache
        for key in keyList:
            (tag, contents, compressed) = self.configCache[key]
            if isinstance(contents, str):
                contents = filecontents.FromString(contents)

            if tag == ChangedFileTypes.diff:
                csf.addFile(key, contents, "1 " + tag[4:])

        # Absolute change sets will have other contents which may or may
        # not be cached. For the ones which are cached, turn them into a
        # filecontainer-ish object (using DictAsCsf) which we will step
        # through along with the rest of the file contents. It beats sucking
        # all of this into RAM. We don't bother cleaning up the mess we
        # make in self.fileQueue since you can't write a changeset multiple
        # times anyway.
        allContents = {}
        for key in keyList:
            (tag, contents, compressed) = self.configCache[key]
            if tag != ChangedFileTypes.diff:
                allContents[key] = (tag, contents, False)

        wrapper = DictAsCsf({})
        wrapper.addConfigs(allContents)

        entry = wrapper.getNextFile()
        if entry:
            util.tupleListBsearchInsert(self.fileQueue,
                                        entry + (wrapper,), 
                                        self.fileQueueCmp)

        next = self._nextFile()
        correction = 0
        last = None
        while next:
            name, tagInfo, f, otherCsf = next
            if last == name:
                next = self._nextFile()
                continue
            last = name

            if tagInfo[2:] == ChangedFileTypes.refr[4:]:
                path = f.read()
                realSize = os.stat(path).st_size
                correction += realSize - len(path)
                if realSize >= 0x100000000:
                    # add 4 bytes to store a 64-bit size
                    correction += 4
                f.seek(0)
                contents = filecontents.FromString(path)
            else:
                contents = filecontents.FromFile(f)

            csf.addFile(name, contents, tagInfo, precompressed = True)
            next = self._nextFile()

        return correction

    def _mergeConfigs(self, otherCs):
        for key, f in otherCs.configCache.iteritems():
            if not self.configCache.has_key(key):
                self.configCache[key] = f
            elif len(key) == 16:
                raise PathIdsConflictError(key)
            elif (self.configCache[key][0] == ChangedFileTypes.diff or
                  f[0] == ChangedFileTypes.diff):
                raise ChangeSetKeyConflictError(key)

    def _mergeReadOnly(self, otherCs):
        assert(not self.lastCsf)
        assert(not otherCs.lastCsf)

        self._mergeConfigs(otherCs)
        self.fileContainers += otherCs.fileContainers
        self.csfWrappers += otherCs.csfWrappers
        for entry in otherCs.fileQueue:
            util.tupleListBsearchInsert(self.fileQueue, entry, 
                                        self.fileQueueCmp)

    def _mergeCs(self, otherCs):
        assert(otherCs.__class__ ==  ChangeSet)

        self._mergeConfigs(otherCs)
        wrapper = DictAsCsf(otherCs.fileContents)
        self.csfWrappers.append(wrapper)
        entry = wrapper.getNextFile()
        if entry:
            util.tupleListBsearchInsert(self.fileQueue,
                                        entry + (wrapper,), 
                                        self.fileQueueCmp)
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

        err = None
        try:
            if isinstance(otherCs, ReadOnlyChangeSet):
                self._mergeReadOnly(otherCs)
            else:
                self._mergeCs(otherCs)
        except ChangeSetKeyConflictError, err:
            pathId = err.pathId

            # look up the trove and file that caused the pathId
            # conflict.
            troves = set(itertools.chain(self.iterNewTroveList(),
                                         otherCs.iterNewTroveList()))
            conflicts = []
            for myTrove in sorted(troves):
                files = (myTrove.getNewFileList()
                         + myTrove.getChangedFileList())
                conflicts.extend((myTrove, x) for x in files if x[0] == pathId)

            if len(conflicts) >= 2:
                raise err.__class__(err.getKey(),
                                    conflicts[0][0], conflicts[0][1],
                                    conflicts[1][0], conflicts[1][1])
            else:
                raise

    def reset(self):
        for csf in self.fileContainers:
            csf.reset()
            # skip the CONARYCHANGESET
            (name, tagInfo, control) = csf.getNextFile()
            assert(name == "CONARYCHANGESET")

        for csf in self.csfWrappers:
            csf.reset()

        self.fileQueue = []
        for csf in itertools.chain(self.fileContainers, self.csfWrappers):
            # find the first non-config file
            entry = csf.getNextFile()
            while entry:
                if entry[1][0] == '0':
                    break

                entry = csf.getNextFile()

            if entry:
                util.tupleListBsearchInsert(self.fileQueue, entry + (csf,),
                                            self.fileQueueCmp)

        self.filesRead = False

    _tag = 'ccs-ro'

    def _sendInfo(self):
        (fileList, hdr) = ChangeSet._sendInfo(self)

        fileList = [ x.file for x in self.fileContainers ] + fileList

        s = struct.pack("!I", len(self.fileContainers)) + hdr
        return (fileList, s)

    @staticmethod
    def _fromInfo(fileObjList, s):
        containerCount = struct.unpack("!I", s[0:4])[0]
        fileContainers = fileObjList[0:containerCount]

        partialCs = ChangeSet._fromInfo(fileObjList[containerCount:], s[4:])
        fullCs = ReadOnlyChangeSet()
        fullCs.merge(partialCs)

        for fObj in fileContainers:
            csf = filecontainer.FileContainer(fObj)
            fullCs.fileContainers.append(csf)

        # this sets up fullCs.fileQueue based on the containers we just loaded
        fullCs.reset()

        return fullCs

    def __init__(self, data = None):
	ChangeSet.__init__(self, data = data)
        self.filesRead = False
        self.csfWrappers = []
        self.fileContainers = []

        self.lastCsf = None
        self.fileQueue = []
util.SendableFileSet._register(ReadOnlyChangeSet)

class ChangeSetFromFile(ReadOnlyChangeSet):
    @api.publicApi
    def __init__(self, fileName, skipValidate = 1):
        self.fileName = None
        try:
            if type(fileName) is str:
                try:
                    f = util.ExtendedFile(fileName, "r", buffering = False)
                except IOError, err:
                    raise errors.ConaryError(
                                "Error opening changeset '%s': %s" % 
                                    (fileName, err.strerror))
                try:
                    csf = filecontainer.FileContainer(f)
                except IOError, err:
                    raise filecontainer.BadContainer(
                                "File %s is not a valid conary changeset: %s" % (fileName, err))
                self.fileName = fileName
            else:
                csf = filecontainer.FileContainer(fileName)
                if hasattr(fileName, 'path'):
                    self.fileName = fileName.path

            (name, tagInfo, control) = csf.getNextFile()
            assert(name == "CONARYCHANGESET")
        except filecontainer.BadContainer:
            raise filecontainer.BadContainer(
                        "File %s is not a valid conary changeset." % fileName)

        control.file.seek(control.start, 0)
	start = gzip.GzipFile(None, 'r', fileobj = control).read()
	ReadOnlyChangeSet.__init__(self, data = start)

	self.absolute = True
	empty = True
        self.fileContainers = [ csf ]

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
            key, tagInfo, f = nextFile

            (isConfig, tag) = tagInfo.split()
            tag = 'cft-' + tag
            isConfig = isConfig == "1"

            # cache all config files because:
            #   1. diffs are needed both to precompute a job and to store
            #      the new config contents in the database
            #   2. full contents are needed if the config file moves components
            #      and we need to generate a diff and then store that config
            #      file in the database
            # (there are other cases as well)
            if not isConfig:
                break

            cont = filecontents.FromFile(gzip.GzipFile(None, 'r', fileobj = f))
            self.configCache[key] = (tag, cont, False)

            nextFile = csf.getNextFile()

        if nextFile:
            self.fileQueue.append(nextFile + (csf,))

def ChangeSetFromSocket(sock):

    fileObjs = util.SendableFileSet.recv(sock)
    assert(len(fileObjs) == 1)
    return fileObjs[0]

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

def fileContentsUseDiff(oldFile, newFile, mirrorMode = False):
    # Don't use diff's for config files when the autosource flag changes
    # because the client may not have anything around it can apply the diff 
    # to.
    return ((not mirrorMode) and
                oldFile and oldFile.flags.isConfig() and
                newFile.flags.isConfig() and
                (oldFile.flags.isAutoSource() == newFile.flags.isAutoSource()) )

def fileContentsDiff(oldFile, oldCont, newFile, newCont, mirrorMode = False):
    if fileContentsUseDiff(oldFile, newFile, mirrorMode = mirrorMode):
	first = oldCont.get().readlines()
	second = newCont.get().readlines()

	if first or second:
            diff = patch.unifiedDiff(first, second, "old", "new")
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

    for (oldTrv, trv, fileMap) in troveList:
	(troveChgSet, filesNeeded, trovesNeeded) = trv.diff(oldTrv,
                                                            absolute = 1)
	cs.newTrove(troveChgSet)

	for (pathId, oldFileId, oldVersion, newFileId, newVersion) in filesNeeded:
	    (file, realPath, filePath) = fileMap[pathId]
	    (filecs, hash) = fileChangeSet(pathId, None, file)
	    cs.addFile(oldFileId, newFileId, filecs)

            if hash and not file.flags.isPayload():
		cs.addFileContents(pathId, newFileId, ChangedFileTypes.file,
			  filecontents.FromFilesystem(realPath),
			  file.flags.isConfig())

    return cs

class DictAsCsf:
    maxMemSize = 16384

    def getNextFile(self):
        if self.next >= len(self.items):
            return None

        (name, contType, contObj, compressed) = self.items[self.next]
        self.next += 1

        if compressed:
            compressedFile = contObj.get()
        else:
            f = contObj.get()
            compressedFile = util.BoundedStringIO(maxMemorySize =
                                                            self.maxMemSize)
            bufSize = 16384

            gzf = gzip.GzipFile('', "wb", fileobj = compressedFile)
            while 1:
                buf = f.read(bufSize)
                if not buf:
                    break
                gzf.write(buf)
            gzf.close()

            compressedFile.seek(0)

        return (name, contType, compressedFile)

    def addConfigs(self, contents):
        # this is like __init__, but it knows things are config files so
        # it tags them with a "1" and puts them at the front
        l = [ (x[0], "1 " + x[1][0][4:], x[1][1], x[1][2])
                        for x in contents.iteritems() ]
        l.sort()
        self.items = l + self.items

    def reset(self):
        self.next = 0

    def __init__(self, contents):
        # convert the dict (which is a changeSet.fileContents object) to a
        # (name, contTag, contObj, compressed) list, where contTag is the same
        # kind of tag we use in csf files "[0|1] [file|diff]"
        self.items = [ (x[0], "0 " + x[1][0][4:], x[1][1], x[1][2]) for x in 
                            contents.iteritems() ]
        self.items.sort()
        self.next = 0

def _convertChangeSetV2V1(inPath, outPath):
    inFc = filecontainer.FileContainer(
                        util.ExtendedFile(inPath, "r", buffering = False))
    assert(inFc.version == filecontainer.FILE_CONTAINER_VERSION_FILEID_IDX)
    outFcObj = util.ExtendedFile(outPath, "w+", buffering = False)
    outFc = filecontainer.FileContainer(outFcObj,
            version = filecontainer.FILE_CONTAINER_VERSION_WITH_REMOVES)

    info = inFc.getNextFile()
    lastPathId = None
    size = 0
    while info:
        key, tag, f = info
        if len(key) == 36:
            # snip off the fileId
            key = key[0:16]

            if key == lastPathId:
                raise changeset.PathIdsConflictError(key)

            size -= 20

        if 'ptr' in tag:
            # I'm not worried about this pointing to the wrong file; that
            # can only happen if there are multiple files with the same
            # PathId, which would cause the conflict we test for above
            oldCompressed = f.read()
            old = gzip.GzipFile(None, "r", 
                                fileobj = StringIO(oldCompressed)).read()
            new = old[0:16]
            newCompressedF = StringIO()
            gzip.GzipFile(None, "w", fileobj = newCompressedF).write(new)
            newCompressed = newCompressedF.getvalue()
            fc = filecontents.FromString(newCompressed, compressed = True)
            size -= len(oldCompressed) - len(newCompressed)
        else:
            fc = filecontents.FromFile(f)

        outFc.addFile(key, fc, tag, precompressed = True)
        info = inFc.getNextFile()

    outFcObj.close()

    return size

def getNativeChangesetVersion(protocolVersion):
    """Return the native changeset version supported by a client speaking the
    supplied protocol version
    
    @param protocolVersion: Protocol version that the client negotiated with
    the server
    @type protocolVersion: int
    @rtype: int
    @return: native changeset version for a client speaking the protocol
    version
    """
    # Add more versions as necessary, but do remember to add them to
    # netclient's FILE_CONTAINER_* constants
    if protocolVersion < 38:
        return filecontainer.FILE_CONTAINER_VERSION_NO_REMOVES
    elif protocolVersion < 43:
        return filecontainer.FILE_CONTAINER_VERSION_WITH_REMOVES
    # Add more changeset versions here as the currently newest client is
    # replaced by a newer one
    return filecontainer.FILE_CONTAINER_VERSION_FILEID_IDX

class AbstractChangesetExploder:

    def __init__(self, cs):
        ptrMap = {}

        fileList = []
        linkGroups = {}
        linkGroupFirstPath = {}
        # sort the files by pathId,fileId
        for trvCs in cs.iterNewTroveList():
            trv = trove.Trove(trvCs)
            self.installingTrove(trv)
            for pathId, path, fileId, version in trv.iterFileList():
                fileList.append((pathId, fileId, path, trv))

        fileList.sort()

        restoreList = []

        for pathId, fileId, path, trv in fileList:
            fileCs = cs.getFileChange(None, fileId)
            if fileCs is None:
                self.fileMissing(trv, pathId, fileId, path)
                continue

            fileObj = files.ThawFile(fileCs, pathId)

            destDir = self.installFile(trv, path, fileObj)
            if not destDir:
                continue

            if fileObj.hasContents:
                restoreList.append((pathId, fileId, fileObj, destDir, path,
                                    trv))
            else:
                self.restoreFile(trv, fileObj, None, destDir, path)

        delayedRestores = {}
        for pathId, fileId, fileObj, destDir, destPath, trv in restoreList:
            (contentType, contents) = cs.getFileContents(pathId, fileId,
                                                         compressed = True)
            if contentType == ChangedFileTypes.ptr:
                targetPtrId = contents.get().read()
                targetPtrId = util.decompressString(targetPtrId)
                l = delayedRestores.setdefault(targetPtrId, [])
                l.append((fileObj, destDir, destPath))
                continue

            assert(contentType == ChangedFileTypes.file)

            ptrId = pathId + fileId
            if pathId in delayedRestores:
                ptrMap[pathId] = destPath
            elif ptrId in delayedRestores:
                ptrMap[ptrId] = destPath

            self.restoreFile(trv, fileObj, contents, destDir, destPath)

            linkGroup = fileObj.linkGroup()
            if linkGroup:
                linkGroups[linkGroup] = destPath

            for fileObj, targetDestDir, targetPath in \
                                            delayedRestores.get(ptrId, []):
                linkGroup = fileObj.linkGroup()
                if linkGroup in linkGroups:
                    self.restoreLink(trv, fileObj, targetDestDir,
                                     linkGroups[linkGroup], targetPath)
                else:
                    self.restoreFile(trv, fileObj, contents, targetDestDir,
                                     targetPath)

                    if linkGroup:
                        linkGroups[linkGroup] = targetPath

    def installingTrove(self, trv):
        pass

    def restoreFile(self, trv, fileObj, contents, destdir, path):
        fileObj.restore(contents, destdir, destdir + path)

    def restoreLink(self, trv, fileObj, destdir, sourcePath, targetPath):
        util.createLink(destdir + sourcePath, destdir + targetPath)

    def installFile(self, trv, path, fileObj):
        raise NotImplementedException

    def fileMissing(self, trv, pathId, fileId, path):
        raise KeyError, pathId + fileId

class ChangesetExploder(AbstractChangesetExploder):

    def __init__(self, cs, destDir):
        self.destDir = destDir
        AbstractChangesetExploder.__init__(self, cs)

    def installFile(self, trv, path, fileObj):
        return self.destDir
