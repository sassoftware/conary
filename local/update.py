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

"""
Handles all updates to the file system; files should never get changed
on the filesystem except by this module!

@var MERGE: Flag constant value.  If set, merge is attempted,
otherwise the changes from the changeset are used (this is for
rollbacks)
@var REPLACEFILES: Flag constant value.  If set, a file that is in
the way of a newly created file will be overwritten.  Otherwise an error
is produced.
"""
import errno
import sha
import stat
import sys
import tempfile
import os

from callbacks import UpdateCallback
from repository import changeset
from repository import filecontents
from deps import deps
import files
import itertools
from lib import log
from lib import patch
from lib import sha1helper
from lib import util
import trove
import versions

from build import tags

MERGE = 1 << 0
REPLACEFILES = 1 << 1
IGNOREUGIDS = 1 << 2
MISSINGFILESOKAY = 1 << 3
        
class FilesystemJob:
    """
    Represents a set of actions which need to be applied to the filesystem.
    This is kept very simple to minimize the chance of mistakes or errors.
    """

    def _rename(self, oldPath, newPath, msg):
	self.renames.append((oldPath, newPath, msg))

    def _registerLinkGroup(self, linkGroup, target):
        self.linkGroups[linkGroup] = target

    def _restore(self, fileObj, target, msg, contentsOverride = ""):
	self.restores.append((fileObj.pathId(), fileObj.freeze(), target, 
                              contentsOverride, msg))
        if fileObj and fileObj.hasContents:
            self.restoreSize += fileObj.contents.size()

	for tag in fileObj.tags:
            l = self.tagUpdates.setdefault(tag, [])
            l.append(target)

    def _remove(self, fileObj, target, msg):
	if isinstance(fileObj, files.Directory):
            self.directorySet.setdefault(target, 0)
	else:
	    self.removes[target] = (fileObj, msg)

            # track removals from each directory
	    dir = os.path.dirname(target)
            self.directorySet.setdefault(dir, 0)
            self.directorySet[dir] += 1

	for tag in fileObj.tags:
            l = self.tagRemoves.setdefault(tag, [])
            l.append(target)

    def userRemoval(self, troveName, troveVersion, troveFlavor, pathId):
        l = self.userRemovals.setdefault(
                    (troveName, troveVersion, troveFlavor), [])
        l.append(pathId)

    def iterUserRemovals(self):
	for ((troveName, troveVersion, troveFlavor), pathIdList) in \
					    self.userRemovals.iteritems():
	    yield (troveName, troveVersion, troveFlavor, pathIdList)

    def _createFile(self, target, str, msg):
	self.newFiles.append((target, str, msg))

    def preapply(self, tagSet = {}, tagScript = None):
	# this is run before the change make it to the database
	rootLen = len(self.root)
	tagCommands = TagCommand()

        for path in self.tagRemoves.get('taghandler', []):
            path = path[rootLen:]
            tagInfo = []
            for ti in tagSet.itervalues():
                if ti.file == path:
                    tagInfo.append(ti)

            if not tagInfo:
                continue

            for ti in tagInfo:
                # this prevents us from trying to run "files add" for this tag
                del tagSet[ti.tag]

                # we're running "handler preremove"; we don't need to run
                # "files preremove" as well, and we won't be able to run "files
                # remove" (since the taghandler would have disappeared)
                if self.tagRemoves.has_key(ti.tag):
                    del self.tagRemoves[ti.tag]

                if "handler preremove" in ti.implements:
                    tagCommands.addCommand(ti, 'handler', 'preremove',
                        [x for x in self.db.iterFilesWithTag(ti.tag)])

	for tag, l in self.tagRemoves.iteritems():
	    if tag == 'tagdescription':
                continue
	    if not tagSet.has_key(tag): continue
	    tagInfo = tagSet[tag]

	    if "files preremove" in tagInfo.implements:
                tagCommands.addCommand(tagInfo, 'files', 'preremove',
                    [x[rootLen:] for x in l ])
	    
        tagCommands.run(tagScript, self.root, preScript=True)

    def _createLink(self, linkGroup, target):
        # this is part of a hard link group, attempt making a
        # hardlink.
        linkPath = self.linkGroups[linkGroup]

        try:
            util.createLink(linkPath, target)
            # continue with the next file to restore
            return True
        except OSError, e:
            # ignore failure to create a cross-device symlink.
            # we'll restore the file as if it's not a hard link
            # below
            if e.errno != errno.EXDEV:
                raise

        return False

    def ptrCmp(a, b):
        if a[0] == b[0]:
            return 0
        elif a[0] < b[0]:
            return -1
        else:
            return 1

    ptrCmp = staticmethod(ptrCmp)

    def apply(self, tagSet = {}, tagScript = None, journal = None,
              callback = UpdateCallback()):

	def restoreFile(fileObj, contents, root, target, journal):
	    if fileObj.hasContents and contents and not \
				       fileObj.flags.isConfig():
		# config file sha1's are verified when they get inserted
		# into the config file cache
		d = sha.new()
		fileObj.restore(contents, root, target, journal=journal,
				digest = d)
		assert(d.digest() == fileObj.contents.sha1())
	    else:
		fileObj.restore(contents, root, target, journal=journal)

	# this is run after the changes are in the database (but before
	# they are committed
	tagCommands = TagCommand()
	runLdconfig = False
	rootLen = len(self.root)

	for (oldPath, newPath, msg) in self.renames:
	    os.rename(oldPath, newPath)
	    log.debug(msg)

	contents = None
	# restore in the same order files appear in the change set
        restores = self.restores[:]
        restores.sort()
        delayedRestores = []
        ptrTargets = {}

        extraContents = []

	paths = self.removes.keys()
	paths.sort()
	paths.reverse()
	for fileNum, target in enumerate(paths):
	    (fileObj, msg) = self.removes[target]
            callback.removeFiles(fileNum + 1, len(paths))

	    # don't worry about files which don't exist
	    try:
		os.lstat(target)
	    except OSError, e:
		if e.errno == errno.ENOENT:
		    log.warning("%s has already been removed" % 
				    target[len(self.root):])
		else:
		    log.error("%s could not be removed: %s" % 
				    (target, e.strerror))
		    raise
	    else:
		fileObj.remove(target)

	    log.debug(msg, target)

        restoreIndex = 0
        j = 0
        while restoreIndex < len(restores):
	    (pathId, fileObj, target, override, msg) = restores[restoreIndex]
            restoreIndex += 1

            if not fileObj:
                # this means we've reached some contents that are the
                # target of ptr's, but not a ptr itself. look through
                # the delayedRestore list for someplace to put this file
                match = None
                for j, item in enumerate(delayedRestores):
                    if pathId == item[4]:
                        match = j, item
                        break

                assert(match)

                (otherId, fileObj, target, msg, ptrId) = match[1]
                
                contType, contents = self.changeSet.getFileContents(pathId)
                assert(contType == changeset.ChangedFileTypes.file)
		restoreFile(fileObj, contents, self.root, target, journal)
                del delayedRestores[match[0]]

                if fileObj.hasContents and fileObj.linkGroup():
                    linkGroup = fileObj.linkGroup()
                    self.linkGroups[linkGroup] = target

                continue

	    # None means "don't restore contents"; "" means "take the
	    # contents from the change set or from the database". If we 
            # take the file contents from the change set, we look for the
            # opportunity to make a hard link instead of actually restoring it.
	    fileObj = files.ThawFile(fileObj, pathId)

            if fileObj.hasContents:
                callback.restoreFiles(fileObj.contents.size(), 
                                      self.restoreSize)
	    if override != "":
		contents = override
	    elif fileObj.hasContents:
                if fileObj.flags.isConfig() and not fileObj.flags.isSource():
                    # take the config file from the local database
                    contents = self.db.getFileContents(
                                    [ (None, None, fileObj) ])[0]
                elif fileObj.linkGroup() and \
                        self.linkGroups.has_key(fileObj.linkGroup()):
                    # this creates links whose target we already know
                    # (because it was already present or already restored)
                    if self._createLink(fileObj.linkGroup(), target):
                        continue
                else:
                    contType, contents = self.changeSet.getFileContents(pathId)
                    assert(contType != changeset.ChangedFileTypes.diff)
                    # PTR types are restored later
                    if contType == changeset.ChangedFileTypes.ptr:
                        ptrId = contents.get().read()
                        delayedRestores.append((pathId, fileObj, target, msg, 
                                                ptrId))
                        if not ptrTargets.has_key(ptrId):
                            ptrTargets[ptrId] = None
                            util.tupleListBsearchInsert(restores, 
                                (ptrId, None, None, None, None),
                                self.ptrCmp)

                        continue
                    elif contType == changeset.ChangedFileTypes.hldr:
                        # missing contents; skip it and hope someone else
                        # figures it out later (probably in the local part
                        # of the rollback)
                        
                        # XXX we need to create this or conary thinks it
                        # was removed by the user if it doesn't already
                        # exist, when that's not what we mean here
                        open(target, "w")
                        continue

	    restoreFile(fileObj, contents, self.root, target, journal)
            if ptrTargets.has_key(pathId):
                ptrTargets[pathId] = target
	    log.debug(msg, target)

            if fileObj.hasContents and fileObj.linkGroup():
                linkGroup = fileObj.linkGroup()
                self.linkGroups[linkGroup] = target

	for (pathId, fileObj, target, msg, ptrId) in delayedRestores:
            # we wouldn't be here if the fileObj didn't have contents and
            # no override

            # the source of the link group may not have been restored
            # yet (it could be in the delayedRestore list itself). that's
            # fine; we just restore the contents here and make the links
            # for everything else
            if fileObj.linkGroup():
                linkGroup = fileObj.linkGroup()
                if self.linkGroups.has_key(linkGroup):
                    if self._createLink(fileObj.linkGroup(), target):
                        continue
                else:
                    linkGroup = fileObj.linkGroup()
                    self.linkGroups[linkGroup] = target

	    restoreFile(fileObj, filecontents.FromFilesystem(ptrTargets[ptrId]),
			self.root, target, journal=journal)
            log.debug(msg, target)

        del delayedRestores

	for (target, str, msg) in self.newFiles:
            try:
                os.unlink(target)
            except OSError, e:
                if e.errno != errno.ENOENT:
                    raise
	    f = open(target, "w")
	    f.write(str)
	    f.close()
	    log.warning(msg)

        # FIXME: the next two operations need to be combined into one;
        # groups can depend on users, and vice-versa.  This ordering
        # merely happens to work for all cases we have at the moment.
        if ('group-info' in self.tagUpdates
            and not _checkHandler('group-info', self.root)):
            groupAction(self.root, self.tagUpdates['group-info'])
	    del self.tagUpdates['group-info']

        if ('user-info' in self.tagUpdates
            and not _checkHandler('user-info', self.root)):
            userAction(self.root, self.tagUpdates['user-info'])
	    del self.tagUpdates['user-info']


	if 'shlib' in self.tagUpdates:
	    shlibAction(self.root, self.tagUpdates['shlib'],
                        tagScript = tagScript)
	    del self.tagUpdates['shlib']
	elif runLdconfig:
	    # override to force ldconfig to run on shlib removal
	    shlibAction(self.root, [])

        # build a set of the new tag descriptions. we index them two ways
        # to make the rest of this a bit easier
        newTagSetByHandler = {}
        newTagSetByDescFile = {}

        for path in self.tagUpdates.get('tagdescription', []):
            tagInfo = tags.TagFile(path, {})
            newTagSetByHandler[tagInfo.file] = tagInfo
            newTagSetByDescFile[path] = tagInfo

        for path in self.tagUpdates.get('taghandler', []):
            # make these look like tagdescription changes, which then
            # get run with "handler update". 

            tagInfo = newTagSetByHandler.get(path, None)
            if tagInfo is None:
                path = path[rootLen:]
                tagInfo = None	
                for ti in tagSet.itervalues():
                    if ti.file == path: 
                        tagInfo = ti
                        break

            if not tagInfo:
                # one place this happens if is the taghandler was just
                # installed, but we catch that case by checking if
                # tagdescription has run
                continue

            l = self.tagUpdates.setdefault('tagdescription', [])
            if tagInfo.tagFile not in l:
                l.append(tagInfo.tagFile)
                newTagSetByDescFile[tagInfo.tagFile] = tagInfo

        for path in self.tagUpdates.get('tagdescription', []):
            # these are new tag action files which we need to run for
            # the first time. we run them against everything in the database
            # which has this tag, which includes the files we've just
            # installed

            tagInfo = newTagSetByDescFile[path]
            path = path[len(self.root):]
            
            # don't run these twice
            if self.tagUpdates.has_key(tagInfo.tag):
                del self.tagUpdates[tagInfo.tag]

            if "handler update" in tagInfo.implements:
                tagCommands.addCommand(tagInfo, 'handler', 'update',
                    [x for x in self.db.iterFilesWithTag(tagInfo.tag)])
            elif "files update" in tagInfo.implements:
                # if "handler update" isn't implemented, see if "files
                # update" is implemented. if so, we need to call this
                # for all of those items files (otherwise the handler will
                # never be called for files which are already installed)
                fileList = [x for x in 
                            self.db.iterFilesWithTag(tagInfo.tag) ] 
                if fileList:
                    tagCommands.addCommand(tagInfo, 'files', 'update', fileList)

            tagSet[tagInfo.tag] = tagInfo

	for (tag, l) in self.tagUpdates.iteritems():
            if tag == 'tagdescription':
                continue

	    tagInfo = tagSet.get(tag, None)
	    if tagInfo is None: continue

	    if "files update" in tagInfo.implements:
                tagCommands.addCommand(tagInfo, 'files', 'update',
		    [x[rootLen:] for x in l])

	for tag, l in self.tagRemoves.iteritems():
	    if not tagSet.has_key(tag): continue
	    tagInfo = tagSet[tag]

	    if "files remove" in tagInfo.implements:
                tagCommands.addCommand(tagInfo, 'files', 'remove',
                    [x[rootLen:] for x in l])
	    
	if tagCommands:
            callback.runningPostTagHandlers()
	    tagCommands.run(tagScript, self.root)

    def getErrorList(self):
	return self.errors

    def iterNewTroveList(self):
	return iter(self.newTroves)

    def getOldTroveList(self):
	return self.oldTroves

    def getDirectoryCountSet(self):
	return self.directorySet

    def _setupRemoves(self, repos, troveCs, changeSet, baseTrove, fsTrove,
                      root, flags):
        # Remove old files. if the files have already been removed, just
        # mention that fact and continue. Don't erase files which
        # have changed contents.
	cwd = os.getcwd()

        fileList = [ ((pathId,) + baseTrove.getFile(pathId)[1:])
                        for pathId in troveCs.getOldFileList() ]
        fileObjs = repos.getFileVersions(fileList)

	for pathId, oldFile in itertools.izip(troveCs.getOldFileList(), 
                                              fileObjs):
            if not baseTrove.hasFile(pathId):
                # this file was removed with 'conary remove /path', so
                # nothing more has to be done
		continue
                
	    (path, fileId, version) = baseTrove.getFile(pathId)

	    if not fsTrove.hasFile(pathId):
		log.debug("%s has already been removed" % path)
		continue

	    if path[0] == '/':
		realPath = root + path
	    else:
		realPath = cwd + "/" + path

	    if flags & MERGE:
		try:
		    # don't remove files if they've been changed locally
		    localFile = files.FileFromFilesystem(realPath, pathId,
                                                possibleMatch = oldFile)
		except OSError, exc:
		    # it's okay if the file is missing, it means we all agree
		    if exc.errno == errno.ENOENT:
			fsTrove.removeFile(pathId)
			continue
		    else:
			raise
	    else:
		localFile = None

	    # don't worry about metadata changes, just content changes
	    if oldFile.hasContents and localFile and localFile.hasContents and \
			oldFile.contents != localFile.contents and \
                        not oldFile.flags.isTransient():
		log.warning("%s has changed but has been removed "
				   "on head", path)

	    self._remove(oldFile, realPath, "removing %s")
	    fsTrove.removeFile(pathId)

    def _singleTrove(self, repos, troveCs, changeSet, baseTrove, fsTrove, root,
                     removalHints, flags):
	"""
	Build up the todo list for applying a single trove to the
	filesystem. 

	@param repos: the repository the files for baseTrove are stored in
	@type repos: repository.Repository
	@param troveCs: the trove changeset to apply to the filesystem
	@type troveCs: trove.TroveChangeSet
	@param changeSet: the changeset troveCs is part of
	@type changeSet: changeset.ChangeSet
	@param baseTrove: the trove the stuff in the filesystem came from
	@type baseTrove: trove.Trove
	@param fsTrove: the trove representing what's in the filesystem now.
        it is updated to represent what will be in the filesystem for this
        trove if apply() is used.
	@type fsTrove: trove.Trove
	@param root: root directory to apply changes to (this is ignored for
	source management, which uses the cwd)
	@type root: str
	@param flags: flags which modify update behavior.  See L{update}
        module variable summary for flags definitions.
	@type flags: int bitfield
	"""
	if baseTrove:
	    assert(troveCs.getOldVersion() == baseTrove.getVersion())
	cwd = os.getcwd()

        # fully updated tracks whether any errors have occured; if no
        # errors occur, fsTrove gets updated to the new version of the trove
        # this doesn't matter for binary stuff, just source management
	fullyUpdated = True

	if (flags & IGNOREUGIDS) or os.getuid():
	    noIds = True
            twmSkipList = { "contents" : True, "owner" : True,
                            "group" : True}
	else:
	    noIds = False
            twmSkipList = {  "contents" : True }

        # Create new files. If the files we are about to create already
        # exist, it's an error.
	for (pathId, headPath, headFileId, headFileVersion) in troveCs.getNewFileList():
	    if headPath[0] == '/':
                headRealPath = root + headPath
	    else:
		headRealPath = cwd + "/" + headPath

	    headFile = files.ThawFile(changeSet.getFileChange(None, headFileId), pathId)

            # these files are placed directly into the lookaside at build
            # time; we don't worry about them.  We still need to put them
            # in the fsTrove, though, since we update the CONARY state
            # from that and want to note these new files.
            if headFile.flags.isAutoSource():
                fsTrove.addFile(pathId, headPath, headFileVersion, headFileId)
                continue

            try:
                s = os.lstat(headRealPath)
                # if this file is a directory and the file on the file
                # system is a directory, we're OK
                if (isinstance(headFile, files.Directory)
                    and stat.S_ISDIR(s.st_mode)):
		    # if nobody else owns this directory, set the ownership
		    # and permissions from this trove. FIXME: if it is
		    # already owned, we just assume those permissions are
		    # right
		    if repos.pathIsOwned(headPath):
			continue
                elif (not isinstance(headFile, files.Directory)
                      and stat.S_ISDIR(s.st_mode)
                      and os.listdir(headRealPath)):
                    # this is a non-empty directory that's in the way of
                    # a new file.  Even --replace-files can't help here
                    self.errors.append("non-empty directory %s is in "
                                   "the way of a newly created "
                                   "file in %s=%s[%s]" % (
                                   util.normpath(headRealPath),
                                   troveCs.getName(), 
                                   troveCs.getNewVersion().asString(),
                                   deps.formatFlavor(troveCs.getNewFlavor())))
                elif (headFile.flags.isInitialContents()  and 
                      not self.removes.has_key(headRealPath)):
                    # don't replace InitialContents files if they already
                    # have contents on disk
                    fullyUpdated = False
                    continue
                elif not self.removes.has_key(headRealPath):
                    inWay = (flags & REPLACEFILES) == 0
                    for info in self.db.iterFindPathReferences(headPath):
                        if (flags & REPLACEFILES) or info[0:3] in removalHints:
                            self.userRemoval(*info)
                            inWay = False

                    if inWay:
                        self.errors.append("%s is in the way of a newly " 
                           "created file in %s=%s[%s]" % (  
                               util.normpath(headRealPath), 
                               troveCs.getName(), 
                               troveCs.getNewVersion().asString(),
                               deps.formatFlavor(troveCs.getNewFlavor())))
                        fullyUpdated = False
                        continue
            except OSError:
                # the path doesn't exist, carry on with the restore
                pass

	    self._restore(headFile, headRealPath, "creating %s")
	    fsTrove.addFile(pathId, headPath, headFileVersion, headFileId)

        # get the baseFile which was originally installed
        baseFileList = [ ((x[0],) + baseTrove.getFile(x[0])[1:]) 
                                        for x in troveCs.getChangedFileList() ]
        baseFileList = repos.getFileVersions(baseFileList)

        # Handle files which have changed betweeen versions. This is by
        # far the most complicated case.
	for (pathId, headPath, headFileId, headFileVersion), baseFile \
                in itertools.izip(troveCs.getChangedFileList(), baseFileList):
	    if not fsTrove.hasFile(pathId):
		# the file was removed from the local system; this change
		# wins
		self.userRemoval(troveCs.getName(), troveCs.getNewVersion(),
                                 troveCs.getNewFlavor(), pathId)
		continue

	    (fsPath, fsFileId, fsVersion) = fsTrove.getFile(pathId)
	    if fsPath[0] == "/":
		rootFixup = root
	    else:
		rootFixup = cwd + "/"

	    pathOkay = True             # do we have a valid, merged path?
	    contentsOkay = True         # do we have valid contents

	    finalPath = fsPath
	    # if headPath is none, the name hasn't changed in the repository
	    if headPath and headPath != fsPath:
		# the paths are different; if one of them matches the one
		# from the old trove, take the other one as it is the one
		# which changed
		if baseTrove.hasFile(pathId):
		    basePath = baseTrove.getFile(pathId)[0]
		else:
		    basePath = None

		if (not flags & MERGE) or fsPath == basePath :
		    # the path changed in the repository, propagate that change
		    self._rename(rootFixup + fsPath, rootFixup + headPath,
		                 "renaming %s to %s" % (fsPath, headPath))

                    # XXX is this correct?  all the other addFiles use
                    # the headFileId, not the fsFileId
		    fsTrove.addFile(pathId, headPath, fsVersion, fsFileId)
		    finalPath = headPath
		else:
		    pathOkay = False
		    finalPath = fsPath	# let updates work still
		    self.errors.append("path conflict for %s (%s on head)" % 
                                       (util.normpath(fsPath), headPath))

            # final path is the path to use w/o the root
            # real path is the path to use w/ the root
	    realPath = rootFixup + finalPath

	    # headFileVersion is None for renames, but in that case there
            # is nothing left to do for this file
            if not headFileVersion:
                continue

            # we know we are switching from one version of the file to
            # (we just checked headFileVersion, and if there isn't an
            # old version then this file would be new, not changed

            # get the baseFile which was originally installed
            (baseFilePath, baseFileId, baseFileVersion) = baseTrove.getFile(pathId)
            assert(baseFile.fileId() == baseFileId)

            # now assemble what the file is supposed to look like on head
            headChanges = changeSet.getFileChange(baseFileId, headFileId)
            if headChanges is None:
                log.error('File objects stored in your database do '
                          'not match the same version of those file '
                          'objects in the repository. The best thing '
                          'to do is erase the version on your system '
                          'by using "conary erase --just-db --no-deps" '
                          'and then run the update again by using '
                          '"conary update --replace-files"')
                raise AssertionError
                
            if headChanges[0] == '\x01':
                # the file was stored as a diff
                headFile = baseFile.copy()
                headFile.twm(headChanges, headFile)
                # verify that the merge yielded the correct fileId
                assert(headFile.fileId() == headFileId)
            else:
                # the file was stored frozen. this happens when the file
                # type changed between versions
                headFile = files.ThawFile(headChanges, pathId)
                
            if baseFile.flags.isAutoSource():
                fsTrove.addFile(pathId, headPath, headFileVersion, headFileId)
                continue
            
            # FIXME we should be able to inspect headChanges directly
            # to see if we need to go into the if statement which follows
            # this rather then having to look up the file from the old
            # trove for every file which has changed
            fsFile = files.FileFromFilesystem(realPath, pathId)
            
            # link groups come from the database; they aren't inferred from
            # the filesystem
            if fsFile.hasContents and baseFile.hasContents:
                fsFile.linkGroup.set(baseFile.linkGroup())

            fsFile.flags.isConfig(headFile.flags.isConfig())
            fsFile.flags.isSource(headFile.flags.isSource())
            fsFile.tags.thaw(headFile.tags.freeze())

            # this is changed to true when the file attributes have changed;
            # this helps us know if we need a restore event
	    attributesChanged = False

            # this forces the file to be restored, with contents
            forceUpdate = False

            # handle file types changing. this is dealt with as a bit
            # of an exception
            fileTypeError = False
            if baseFile.lsTag != headFile.lsTag:
                if isinstance(baseFile, files.Directory):
                    # a directory changed to some other type of file
                    if isinstance(fsFile, files.Directory):
                        # if the local filesystem still has a directory
                        # there, bail
                        if isinstance(headFile, files.SymbolicLink):
                            newLocation = os.path.abspath(os.path.join(
                                os.path.dirname(finalPath), headFile.target()))
                            self.errors.append(
                                '%s changed from a directory to '
                                'a symbolic link.  To apply this changeset, '
                                'first manually move %s to %s, then run '
                                '"ln -s %s %s".' %(finalPath, finalPath,
                                                   newLocation,
                                                   headFile.target(),
                                                   finalPath))
                        else:
                            self.errors.append("%s changed from a directory to "
                                               "a non-directory" %finalPath)
                        continue
                    else:
                        # someone changed the filesystem so we're replacing
                        # something else instead of a directory
                        forceUpdate = True
                        attributesChanged = True
                elif flags & REPLACEFILES or baseFile.lsTag == fsFile.lsTag:
                    # the file type changed between versions. Force an
                    # update because changes cannot be be merged
                    attributesChanged = True
                    fsFile = headFile
                    forceUpdate = True
                elif baseFile.lsTag != fsFile.lsTag:
                    fileTypeError = True
            elif baseFile.lsTag != fsFile.lsTag:
                # the user changed the file type. we could try and
                # merge things a bit more intelligently then we do
                # here, but it probably isn't worth the effort
                if flags & REPLACEFILES:
                    attributesChanged = True
                    fsFile = headFile
                    forceUpdate = True
                else:
                    fileTypeError = True

            if fileTypeError:
                self.errors.append("file type of %s changed" % finalPath)
                continue

            # if we're forcing an update, we don't need to merge this
            # stuff
	    if not forceUpdate and \
               not fsFile.eq(headFile, ignoreOwnerGroup = noIds):
		# some of the attributes have changed for this file; try
                # and merge
		if flags & MERGE:
		    if noIds:
			# we don't want to merge owner/group ids in
			# this case (something other than owner/group
			# changed, such as size).  simply take the
			# head values
			baseFile.inode.owner.set(headFile.inode.owner())
			baseFile.inode.group.set(headFile.inode.group())

		    conflicts = fsFile.twm(headChanges, baseFile, 
					   skip = twmSkipList)
		    if not conflicts:
			attributesChanged = True
		    else:
			contentsOkay = False
			self.errors.append("file attributes conflict for %s"
						% util.normpath(realPath))
		else:
		    # this forces the change to apply
		    fsFile.twm(headChanges, fsFile, 
                               skip = { "contents" : True })
		    attributesChanged = True

	    beenRestored = False

	    if forceUpdate or (
                   headFile.hasContents and \
                   fsFile.hasContents and \
                   fsFile.contents.sha1() != headFile.contents.sha1() and \
                   headFile.contents.sha1() != baseFile.contents.sha1()
                ):

                if not forceUpdate and headFile.flags.isInitialContents():
		    log.debug("skipping new contents of InitialContents file"
                              "%s" % finalPath)
		elif forceUpdate or (flags & REPLACEFILES) or \
                        (not flags & MERGE) or \
			headFile.flags.isTransient() or \
			fsFile.contents == baseFile.contents:

		    # the contents changed in just the repository, so take
		    # those changes
                    if headFile.flags.isConfig and \
                                changeSet.configFileIsDiff(pathId):
			(headFileContType,
			 headFileContents) = changeSet.getFileContents(pathId)

			baseLineF = repos.getFileContents([ (baseFileId,
					baseTrove.getFile(pathId)[2]) ])[0].get()

			baseLines = baseLineF.readlines()
			del baseLineF
			diff = headFileContents.get().readlines()
			(newLines, failedHunks) = patch.patch(baseLines, diff)
			assert(not failedHunks)
                        newContents = "".join(newLines)
			headFileContents = filecontents.FromString(newContents)

                        # now set the sha1 and size of the fsFile's
                        # contents to match what will be on the system
                        # once this is applied
                        fsFile.contents.sha1.set(sha1helper.sha1String(newContents))
                        fsFile.contents.size.set(len(newContents))
			self._restore(fsFile, realPath, 
				      "replacing %s with contents "
				      "from repository",
				      contentsOverride = headFileContents)
		    else:
                        # switch the fsFile to the sha1 for the new file
                        if fsFile.hasContents:
                            fsFile.contents.sha1.set(headFile.contents.sha1())
                            fsFile.contents.size.set(headFile.contents.size())
			self._restore(fsFile, realPath, 
				      "replacing %s with contents "
				      "from repository")

		    beenRestored = True
		elif headFile.contents == baseFile.contents:
		    # it changed in just the filesystem, so leave that change
		    log.debug("preserving new contents of %s" % finalPath)
		elif headFile.flags.isConfig() and \
					    not baseFile.flags.isConfig():
		    # it changed in the filesystem and the repository,
		    # but it wasn't always a config file. this means we
		    # don't have a patch available for it, and we just leave
		    # the old contents in place
		    if headFile.contents.sha1() != baseFile.contents.sha1():
			log.warning("preserving contents of %s (now a "
				    "config file)" % finalPath)
		elif headFile.flags.isConfig():
		    # it changed in both the filesystem and the repository; our
		    # only hope is to generate a patch for what changed in the
		    # repository and try and apply it here
                    if not changeSet.configFileIsDiff(pathId):
			self.errors.append("unexpected content type for %s" % 
						finalPath)
			contentsOkay = False
		    else:
                        (headFileContType,
                         headFileContents) = changeSet.getFileContents(pathId)

			cur = open(realPath, "r").readlines()
			diff = headFileContents.get().readlines()
			(newLines, failedHunks) = patch.patch(cur, diff)

			cont = filecontents.FromString("".join(newLines))
                        # XXX update fsFile.contents.{sha1,size}?
			self._restore(fsFile, realPath, 
			      "merging changes from repository into %s",
			      contentsOverride = cont)
			beenRestored = True

			if failedHunks:
			    self._createFile(
                                realPath + ".conflicts", 
                                "".join([x.asString() for x in failedHunks]),
                                "conflicts from merging changes from " 
                                "head into %s saved as %s.conflicts" % 
                            (util.normpath(realPath), util.normpath(realPath)))

			contentsOkay = True
		else:
		    self.errors.append(
                      "file contents conflict for %s" % util.normpath(realPath))
		    contentsOkay = False
            elif headFile.hasContents and headFile.linkGroup():
                # the contents haven't changed, but the link group has changed.
                # we want to let files in that link group hard link to this file
                # (if appropriate)
                self._registerLinkGroup(headFile.linkGroup(), realPath)

	    if attributesChanged and not beenRestored:
		self._restore(fsFile, realPath, 
		      "merging changes from repository into %s",
		      contentsOverride = None)

	    if pathOkay and contentsOkay:
		# XXX this doesn't even attempt to merge file permissions
		# and such; the good part of that is differing owners don't
		# break things
		fsTrove.addFile(pathId, finalPath, headFileVersion, headFileId)
	    else:
		fullyUpdated = False

	if fullyUpdated:
	    fsTrove.changeVersion(troveCs.getNewVersion())

	return fsTrove

    def __init__(self, db, changeSet, fsTroveDict, root, callback = None, 
		 flags = MERGE, removeHints = {}):
	"""
	Constructs the job for applying a change set to the filesystem.

	@param db: the db the current trove and file information 
	is in
	@type db: local.database.Database
	@param changeSet: the changeset to apply to the filesystem
	@type changeSet: changeset.ChangeSet
	@param fsTroveDict: dictionary mapping a trove name to the trove
	object representing what's currently stored in the filesystem
	@type fsTroveDict: dict of trove.Trove
	@param root: root directory to apply changes to (this is ignored for
	source management, which uses the cwd)
	@type root: str
	@param flags: flags which modify update behavior.  See L{update}
        module variable summary for flags definitions.
	@type flags: int bitfield
	"""
	self.renames = []
	self.restores = []
        self.restoreSize = 0
	self.removes = {}
	self.oldTroves = []
	self.errors = []
	self.newFiles = []
	self.root = root
	self.changeSet = changeSet
	self.directorySet = {}
	self.userRemovals = {}
	self.tagUpdates = {}
	self.tagRemoves = {}
        self.linkGroups = {}
	self.db = db

        for (name, oldVersion, oldFlavor) in changeSet.getOldTroveList():
            self.oldTroves.append((name, oldVersion, oldFlavor))
            oldTrove = db.getTrove(name, oldVersion, oldFlavor, 
                                   pristine = False)
            fileList = [ (x[0], x[2], x[3]) for x in oldTrove.iterFileList() ]
            fileObjs = db.getFileVersions(fileList)
            for (pathId, path, fileId, version), fileObj in \
                    itertools.izip(oldTrove.iterFileList(), fileObjs):
                self._remove(fileObj, root + path, "removing %s")

        pkgList = []

	for troveCs in changeSet.iterNewTroveList():
            old = troveCs.getOldVersion()
	    if old:
		localVer = old.createBranch(versions.LocalLabel(), 
                                            withVerRel = 1)
                newFsTrove = fsTroveDict[(troveCs.getName(), localVer)].copy()
            else:
                newFsTrove = trove.Trove(troveCs.getName(), versions.NewVersion(),
                                    troveCs.getNewFlavor(), troveCs.getChangeLog())

            pkgList.append((troveCs, newFsTrove))

	for (troveCs, newFsTrove) in pkgList:
	    old = troveCs.getOldVersion()

	    if old:
		baseTrove = db.getTrove(troveCs.getName(), old, 
                                         troveCs.getOldFlavor())
	    else:
                baseTrove = None

            self._setupRemoves(db, troveCs, changeSet, baseTrove,
                               newFsTrove, root, flags)

	for i, (troveCs, newFsTrove) in enumerate(pkgList):
	    if callback:
		callback.preparingUpdate(i + 1, len(pkgList))

	    old = troveCs.getOldVersion()

	    if old:
		baseTrove = db.getTrove(troveCs.getName(), old, 
                                         troveCs.getOldFlavor())
		self.oldTroves.append((baseTrove.getName(), 
					 baseTrove.getVersion(),
					 baseTrove.getFlavor()))
	    else:
                baseTrove = None

            self._singleTrove(db, troveCs, changeSet, baseTrove,
                                      newFsTrove, root, removeHints, flags)

            newFsTrove.mergeTroveListChanges(troveCs.iterChangedTroves(),
                                           redundantOkay = True)

        pkgList = [ x[1] for x in pkgList ]
        self.newTroves = pkgList

def _localChanges(repos, changeSet, curTrove, srcTrove, newVersion, root, flags,
                  withFileContents=True, forceSha1=False, 
                  ignoreTransient=False, ignoreAutoSource = False):
    """
    Populates a change set against the files in the filesystem and builds
    a trove object which describes the files installed.  The return
    is a tuple with a boolean saying if anything changed and a trove
    reflecting what's in the filesystem; the changeSet is updated as a
    side effect.

    @param repos: Repository this directory is against.
    @type repos: repository.Repository
    @param changeSet: Changeset to update with information for this trove
    @type changeSet: changeset.ChangeSet
    @param curTrove: Trove which is installed
    @type curTrove: trove.Trove
    @param srcTrove: Trove to generate the change set against
    @type srcTrove: trove.Trove
    @param newVersion: version to use for the newly created trove
    @type newVersion: versions.NewVersion
    @param root: root directory the files are in (ignored for sources, which
    are assumed to be in the current directory)
    @type root: str
    @param flags: (IGNOREUGIDS|MISSINGFILESOKAY) or zero
    @type flags: int
    @param forceSha1: disallows the use of inode information to avoid
                      checking the sha1 of the file if the inode information 
                      matches exactly.
    @type forceSha1: bool
    @param ignoreTransient: ignore transient files 
    @type ignoreTransient: bool
    @param ignoreAutoSource: ignore automatically added source files 
    @type ignoreAutoSource: bool
    """

    noIds = ((flags & IGNOREUGIDS) != 0)

    newTrove = curTrove.copy()
    newTrove.changeVersion(newVersion)

    pathIds = {}
    for (pathId, path, fileId, version) in newTrove.iterFileList():
	pathIds[pathId] = True

    # Iterating over the files in newTrove would be much more natural
    # then iterating over the ones in the old trove, and then going
    # through newTrove to find what we missed. However, doing it the
    # hard way lets us iterate right over the changeset we get from
    # the repository.
    if srcTrove:
	fileList = [ x for x in srcTrove.iterFileList() ]
	# need to walk changesets in order of fileid
	fileList.sort()
    else:
	fileList = []

    # Used in the loops to determine whether to mark files as config
    # would be nice to have a better list...
    nonCfgExt = ('ps', 'eps', 'gif', 'png', 'tiff', 'jpeg', 'jpg',
	'ico', 'rpm', 'ccs', 'gz', 'bz2', 'tgz', 'tbz', 'tbz2', 'zip')
    isSrcTrove = curTrove.getName().endswith(':source')

    srcFileObjs = repos.getFileVersions( [ (x[0], x[2], x[3]) for x in 
                                                    fileList ] )

    for (pathId, srcPath, srcFileId, srcFileVersion), srcFile in \
                    itertools.izip(fileList, srcFileObjs):
	# files which disappear don't need to make it into newTrove
	if not pathIds.has_key(pathId): continue
	del pathIds[pathId]

        # transient files never show up in in local changesets...
        if ignoreTransient and srcFile.flags.isTransient():
            continue
        elif ignoreAutoSource and srcFile.flags.isAutoSource():
            continue

	(path, fileId, version) = newTrove.getFile(pathId)

        if isSrcTrove:
            if path in curTrove.pathMap:
                realPath = curTrove.pathMap[path]
                isAutoSource = True
            else:
                realPath = os.getcwd() + "/" + path
                isAutoSource = False
        else:
	    realPath = root + path

        if forceSha1:
            possibleMatch = None
        else:
            possibleMatch = srcFile

	try:
            f = files.FileFromFilesystem(realPath, pathId,
                                         possibleMatch = possibleMatch)
	except OSError:
            if isSrcTrove:
		log.error("%s is missing (use remove if this is intentional)" 
		    % util.normpath(path))
                return None

	    if (flags & MISSINGFILESOKAY) == 0:
		log.warning("%s is missing (use remove if this is intentional)" 
		    % util.normpath(path))

            newTrove.removeFile(pathId)
            continue

	if isSrcTrove:
	    f.flags.isSource(set = True)
            f.flags.isAutoSource(set = isAutoSource)
            assert(srcFile.flags.isAutoSource() == f.flags.isAutoSource())

        # the link group doesn't change due to local mods
        if srcFile.hasContents and f.hasContents:
            f.linkGroup.set(srcFile.linkGroup())

        # these values are not picked up from the local system
        if hasattr(f, 'requires') and hasattr(srcFile, 'requires'):
            f.requires.set(srcFile.requires())
        if srcFile.hasContents and f.hasContents:
            f.provides.set(srcFile.provides())
        f.flags.set(srcFile.flags())
        if srcFile.hasContents and f.hasContents:
            f.flavor.set(srcFile.flavor())
        f.tags = srcFile.tags.copy()

	extension = path.split(".")[-1]
	if isSrcTrove and extension not in nonCfgExt:
	    f.flags.isConfig(set = True)
	    sb = os.stat(realPath)
	    if sb.st_size > 0 and stat.S_ISREG(sb.st_mode):
		fd = os.open(realPath, os.O_RDONLY)
		os.lseek(fd, -1, 2)
		term = os.read(fd, 1)
		if term != '\n':
		    log.warning("%s does not end with a trailing new line", 
			        srcPath)

		os.close(fd)

	if not f.eq(srcFile, ignoreOwnerGroup = noIds):
	    newTrove.addFile(pathId, path, newVersion, f.fileId())

	    (filecs, hash) = changeset.fileChangeSet(pathId, srcFile, f)
	    changeSet.addFile(srcFileId, f.fileId(), filecs)
	    if hash and withFileContents:
		newCont = filecontents.FromFilesystem(realPath)

		if srcFile.hasContents:
		    srcCont = repos.getFileContents(
                                        [ (srcFileId, srcFileVersion) ])[0]

                    (contType, cont) = changeset.fileContentsDiff(srcFile, srcCont,
                                                                  f, newCont)

                    changeSet.addFileContents(pathId, contType, cont, 
                                              f.flags.isConfig())

    # anything left in pathIds has been newly added
    for pathId in pathIds.iterkeys():
	(path, fileId, version) = newTrove.getFile(pathId)

        if isSrcTrove:
            if path in curTrove.pathMap:
                realPath = curTrove.pathMap[path]
                isAutoSource = True
            else:
                realPath = os.getcwd() + "/" + path
                isAutoSource = False

            if not isinstance(version, versions.NewVersion):
                srcFile = repos.getFileVersion(pathId, fileId, version)
                if ignoreAutoSource and srcFile.flags.isAutoSource():
                    # this is an autosource file which was newly added,
                    # probably by a merge (if it was added on the command
                    # line, it's version would be NewVersion)
                    changeSet.addFile(None, srcFile.fileId(), srcFile.freeze())
                    newTrove.addFile(pathId, path, version, srcFile.fileId())
                    continue
        else:
	    realPath = root + path

	# if we're committing against head, this better be a new file.
	# if we're generating a diff against someplace else, it might not 
	# be.
	assert(srcTrove or isinstance(version, versions.NewVersion))

	f = files.FileFromFilesystem(realPath, pathId)

	extension = path.split(".")[-1]
	if isSrcTrove:
            f.flags.isSource(set = True)
            f.flags.isAutoSource(set = isAutoSource)
            if extension not in nonCfgExt:
                f.flags.isConfig(set = True)

	# new file, so this part is easy
	changeSet.addFile(None, f.fileId(), f.freeze())
	newTrove.addFile(pathId, path, newVersion, f.fileId())

	if f.hasContents and withFileContents:
	    newCont = filecontents.FromFilesystem(realPath)
	    changeSet.addFileContents(pathId,
				      changeset.ChangedFileTypes.file,
				      newCont, f.flags.isConfig())

    (csTrove, filesNeeded, pkgsNeeded) = newTrove.diff(srcTrove)
    changeSet.newTrove(csTrove)

    if (csTrove.getOldFileList() or csTrove.getChangedFileList()
        or csTrove.getNewFileList()
        or [ x for x in csTrove.iterChangedTroves()]):
	foundDifference = True
    else:
	foundDifference = False

    return (foundDifference, newTrove)

def buildLocalChanges(repos, pkgList, root = "", withFileContents=True,
                      forceSha1 = False, ignoreTransient=False,
                      ignoreAutoSource = False, updateContainers = False):
    """
    Builds a change set against a set of files currently installed and
    builds a trove object which describes the files installed.  The
    return is a changeset and a list of tuples, each with a boolean
    saying if anything changed for a trove reflecting what's in the
    filesystem for that trove.

    @param repos: Repository this directory is against.
    @type repos: repository.Repository
    @param pkgList: Specifies which pacakage to work on, and is a list
    of (curTrove, srcTrove, newVersion, flags) tuples as defined in the parameter
    list for _localChanges()
    @param root: root directory the files are in (ignored for sources, which
    are assumed to be in the current directory)
    @type root: str
    @param forceSha1: disallows the use of inode information to avoid
                      checking the sha1 of the file if the inode information 
                      matches exactly.
    @type forceSha1: bool
    @param ignoreTransient: ignore transient files 
    @type ignoreTransient: bool
    @param ignoreAutoSource: ignore automatically added source files 
    @type ignoreAutoSource: bool
    @param updateContainers: Container troves are updated to point to the 
                             new versions of troves which have had files 
                             changed.
    """

    changeSet = changeset.ChangeSet()
    changedTroves = {}
    returnList = []
    for (curTrove, srcTrove, newVersion, flags) in pkgList: 
	result = _localChanges(repos, changeSet, curTrove, srcTrove, newVersion, 
			       root, flags, 
                               withFileContents = withFileContents,
                               forceSha1 = forceSha1, 
                               ignoreTransient = ignoreTransient,
                               ignoreAutoSource = ignoreAutoSource)
        if result is None:
            # an error occurred
            return None

        if result[0]:
            # something changed
            changedTroves[(curTrove.getName(), curTrove.getVersion(),
                                             curTrove.getFlavor())
                         ] = (curTrove.getName(), newVersion, curTrove.getFlavor())

	returnList.append(result)

    if not updateContainers:
        return (changeSet, returnList)

    for i, (curTrove, srcTrove, newVersion, flags) in enumerate(pkgList):
        inclusions = [ x for x in curTrove.iterTroveList() ]
        if not inclusions: continue
        assert(curTrove == srcTrove)
        assert(srcTrove.emptyFileList() and curTrove.emptyFileList())

        newTrove = curTrove.copy()
        changed = False

        for tuple in inclusions:
            # these are only different if files have been manually removd;
            # they should be the same for containers
            if tuple in changedTroves:
                newTrove.addTrove(*(changedTroves[tuple] + 
                                    (newTrove.includeTroveByDefault(*tuple),)))
                newTrove.delTrove(*(tuple + (False,)))
                changed = True
                
        if changed:
            newTrove.changeVersion(newVersion)
            trvCs = newTrove.diff(curTrove)[0]
            returnList[i] = (True, newTrove)
            changeSet.newTrove(trvCs)
            
    return (changeSet, returnList)

def shlibAction(root, shlibList, tagScript = None):
    p = "/sbin/ldconfig"

    # write any needed entries in ld.so.conf before running ldconfig
    sysetc = util.joinPaths(root, '/etc')
    if not os.path.isdir(sysetc):
	# normally happens only during testing, but why not be safe?
	util.mkdirChain(sysetc)
    ldsopath = util.joinPaths(root, '/etc/ld.so.conf')

    try:
	ldso = file(ldsopath, 'r+')
	ldsolines = ldso.readlines()
	ldso.close()
    except:
	# bootstrap
	ldsolines = []

    newlines = []
    rootlen = len(root)

    for path in shlibList:
	dirname = os.path.dirname(path)[rootlen:]
	dirline = dirname+'\n'
	if dirline not in ldsolines:
	    ldsolines.append(dirline)
	    newlines.append(dirname)

    if newlines:
	log.debug("adding ld.so.conf entries: %s",
		  " ".join(newlines))
	ldsofd, ldsotmpname = tempfile.mkstemp(
	    'ld.so.conf', '.ct', sysetc)
        if not os.getuid():
            # ld.so.conf should always be 0.0
            os.chown(ldsotmpname, 0, 0)
	try:
	    ldso = os.fdopen(ldsofd, 'w')
	    os.chmod(ldsotmpname, 0644)
	    ldso.writelines(ldsolines)
	    ldso.close()
	    os.rename(ldsotmpname, ldsopath)
	except:
	    os.unlink(ldsotmpname)
	    raise

    if tagScript is not None:
        f = open(tagScript, "a")
        f.write("/sbin/ldconfig\n")
    elif os.getuid():
	log.warning("ldconfig skipped (insufficient permissions)")
    elif os.access(util.joinPaths(root, p), os.X_OK) != True:
	log.error("/sbin/ldconfig is not available")
    else:
	log.debug("running ldconfig")
	pid = os.fork()
	if not pid:
	    os.chdir(root)
	    os.chroot(root)
	    try:
		# XXX add a test case for an invalid ldconfig binary
		os.execl(p, p)
	    except:
		pass
	    os._exit(1)
	(id, status) = os.waitpid(pid, 0)
	if not os.WIFEXITED(status) or os.WEXITSTATUS(status):
	    log.error("ldconfig failed")


def _checkHandler(root, tag):
    # the tag description and handler are installed together, but
    # the handler (at least in rpath linux) is multitag
    return os.access('/'.join((root, '/etc/conary/tags', tag)), os.R_OK)

class _InfoFile(dict):
    """
    Simple object for bootstrapping editing /etc/passwd and /etc/group
    This object is only used before the user-info and group-info tag
    handlers are installed, and before any shadowing information has
    been added to the filesystem.
    """
    def __init__(self, root, path, keyfield, idfield, listfield, defaultList):
        self._modified = False
        self._lines = []
        self._idmap = {}
        self._root = root
        self._path = path
        self._keyfield = keyfield
        self._idfield = idfield
        self._listfield = listfield
        try:
            f = file('/'.join((root, path)))
            self._lines = [ x.strip().split(':') for x in f.readlines() ]
            f.close()
        except:
            self._modified = True
        for line in self._lines:
            self[line[keyfield]] = line
            self._idmap[line[idfield]] = line
        if not self._lines:
            self._lines.append(defaultList)
            self._modified = True

    def addLine(self, lineItems):
        self._lines.append(lineItems)
        self[lineItems[self._keyfield]] = lineItems
        self._idmap[lineItems[self._idfield]] = lineItems
        self._modified = True

    def hasId(self, id):
        return id in self._idmap

    def newId(self):
        id = 1
        while self.hasId(str(id)):
            id += 1
        return str(id)

    def id(self, name):
        return self[name][self._idfield]

    def getList(self, name):
        assert(self._listfield)
        return self[name][self._listfield].split(',')

    def extendList(self, name, item):
        assert(self._listfield)
        l = self.getList(name)
        if item in l:
            return
        if l[0]:
            l.append(item)
        else:
            # don't leave an empty list item
            l = [item]
        self[name][self._listfield] = ','.join(l)

    def cmpLine(self, a, b):
        # sort numerically on id
        x = int(a[self._idfield])
        y = int(b[self._idfield])
        if x > y:
            return 1
        if x < y:
            return -1
        return 0

    def write(self):
        if self._modified:
            # this is only a bootstrap for when the taghandler isn't
            # there yet, so we don't need to worry about races for
            # security or anything else...
            fileName = '/'.join((self._root, self._path))
            f = file(fileName, 'w+')
            os.chmod(fileName, 0644)
            # sort lines based on id
            lines = sorted(self._lines, self.cmpLine)
            f.writelines(['%s\n' %(':'.join(x)) for x in lines])
            f.close()


class _KeyVal(dict):
    def __init__(self, path):
        f = file(path)
        for line in f.readlines():
            key, val = line.split('=', 1)
            self[key] = val.split('\n')[0]
        f.close()

def userAction(root, userFileList):
    passwd = _InfoFile(root, '/etc/passwd', 0, 2, None,
                       ['root', '*', '0', '0', 'root', '/root', '/bin/bash'])
    group = _InfoFile(root, '/etc/group', 0, 2, 3,
                      ['root', '*', '0', 'root' ])
    for path in userFileList:
        f = _KeyVal(path)
        f.setdefault('USER', os.path.basename(path))
        f.setdefault('PREFERRED_UID', '1')
        if passwd.hasId(f['PREFERRED_UID']):
            f['PREFERRED_UID'] = passwd.newId()
        f.setdefault('GROUP', f['USER'])
        if f['GROUP'] in group:
            f['GROUPID'] = group.id(f['GROUP'])
        else:
            f.setdefault('GROUPID', f['PREFERRED_UID'])
            if group.hasId(f['GROUPID']):
                f['GROUPID'] = group.newId()
            group.addLine([f['GROUP'], '*', f['GROUPID'], ''])
        f.setdefault('COMMENT', '')
        f.setdefault('HOMEDIR', '/')
        f.setdefault('SHELL', '/sbin/nologin')
        if f['USER'] not in passwd:
            passwd.addLine([
                f['USER'],
                '*',
                f['PREFERRED_UID'],
                f['GROUPID'],
                f['COMMENT'],
                f['HOMEDIR'],
                f['SHELL'],
            ])
        f.setdefault('SUPPLEMENTAL', '')
        for groupName in [ x for x in f['SUPPLEMENTAL'].split(',') if x ]:
            # dependencies ensure that groupName already exists
            group.extendList(groupName, f['USER'])
    passwd.write()
    group.write()


def groupAction(root, groupFileList):
    group = _InfoFile(root, '/etc/group', 0, 2, 3,
                      ['root', '*', '0', 'root' ])
    for path in groupFileList:
        f = _KeyVal(path)
        f.setdefault('GROUP', os.path.basename(path))
        if f['GROUP'] not in group:
            if group.hasId(f['PREFERRED_GID']):
                f['PREFERRED_GID'] = group.newId()
            group.addLine([f['GROUP'], '*', f['PREFERRED_GID'], ''])
        if 'USER' in f:
            # add user to group
            group.extendList(f['GROUP'], f['USER'])
    group.write()


class HandlerInfo:
    def __init__(self):
        self.tagToFile = {} # {tagInfo: fileList}
        self.fileToTag = {} # {fileName: tagInfoList}
    def update(self, tagInfo, fileList):
        l = self.tagToFile.setdefault(tagInfo, [])
        l.extend(fileList)
        for file in fileList:
            l = self.fileToTag.setdefault(file, [])
            l.append(tagInfo)

class TagCommand:
    def __init__(self):
        self.commandOrder = (
            ('handler', 'preremove'),
            ('files',   'preremove'),
            ('handler', 'update'),
            ('files',   'update'),
            ('files',   'remove'),
        )
        self.commands = {
            'handler': {
                'update':    {}, # {handler: HandlerInfo}
                'preremove': {},
            },
            'files': {
                'update':    {},
                'preremove': {},
                'remove':    {},
            },
        }

    def addCommand(self, tagInfo, updateType, updateClass, fileList):
        h = self.commands[updateType][updateClass].setdefault(
            tagInfo.file, HandlerInfo())
        h.update(tagInfo, fileList)

    def _badMultiTag(self, tagInfoList):
        if len([x for x in tagInfoList if x.datasource != 'multitag']):
            # multiple description without multitag protocol
            log.error('tag handler %s used by multiple tags'
                      ' without multitag protocol' % handler)
            return True
        return False

    def run(self, tagScript, root, preScript=False):
        if tagScript:
            if preScript:
                pre = "# "
            else:
                pre = ""

            # N.B. All changes in the logic for writing scripts need to
            # be paralleled by changes below in the non-tagScript branch,
            # where we're running programs instead.
            f = open(tagScript, "a")
            for (updateType, updateClass) in self.commandOrder:
                for handler in sorted(self.commands[updateType][updateClass]):
                    # stable sort order to be able to reproduce bugs,
                    # whether in conary or in the packaged software
                    hi = self.commands[updateType][updateClass][handler]
                    tagInfoList = hi.tagToFile.keys()
                    if (len(tagInfoList) > 1):
                        # multiple tags for one tag handler
                        if self._badMultiTag(tagInfoList):
                            break
                        datasource = 'multitag'
                    else:
                        tagInfo = tagInfoList.pop()
                        datasource = tagInfo.datasource

                    if datasource == 'args':
                        f.write("%s%s %s %s %s\n" % (pre, handler,
                            updateType, updateClass,
                            " ".join(sorted(hi.tagToFile[tagInfo]))))
                    elif datasource == 'stdin':
                        f.write("%s%s %s %s <<EOF\n" % (pre, handler,
                            updateType, updateClass))
                        for filename in sorted(hi.tagToFile[tagInfo]):
                            f.write("%s%s\n" % (pre, filename))
                        f.write("%sEOF\n" % pre)
                    elif datasource == 'multitag':
                        f.write("%s%s %s %s <<EOF\n" % (
                            pre, handler, updateType, updateClass))
                        for fileName in sorted(hi.fileToTag):
                            f.write("%s%s\n" % (pre, " ".join(
                                sorted([x.tag for x in
                                        hi.fileToTag[fileName]]))))
                            f.write("%s%s\n" % (pre, fileName))
                        f.write("%sEOF\n" % pre)
                    else:
                        log.error('unknown datasource %s' %datasource)

            f.close()
            return

        uid = os.getuid()
        # N.B. All changes in the logic for writing scripts need to
        # be paralleled by changes above in the tagScript branch,
        # where we're writing scripts instead.
        for (updateType, updateClass) in self.commandOrder:
            for handler in sorted(self.commands[updateType][updateClass]):
                # stable sort order
                hi = self.commands[updateType][updateClass][handler]
                tagInfoList = hi.tagToFile.keys()

                # start building the command line -- all the tag
                # handler protocols begin the same way
                command = [handler, updateType, updateClass]
                if (len(tagInfoList) > 1):
                    # multiple tags for one tag handler
                    if self._badMultiTag(tagInfoList):
                        break
                    datasource = 'multitag'
                else:
                    tagInfo = tagInfoList.pop()
                    datasource = tagInfo.datasource

                # if the handler uses the command line argument
                # protocol, add all the filenames to the command line
                if datasource == 'args':
                    command.extend(sorted(hi.tagToFile[tagInfo]))

                # double check that we're using a known protocol
                if datasource not in ('multitag', 'args', 'stdin'):
                    log.error('unknown datasource %s' %datasource)
                    break

                log.debug("running %s", " ".join(command))
                if root != '/' and uid:
                    continue

                p = os.pipe()
                pid = os.fork()
                if not pid:
                    os.close(p[1])
                    os.dup2(p[0], 0)
                    os.close(p[0])
                    os.environ['PATH'] = "/sbin:/bin:/usr/sbin:/usr/bin"
                    if root != '/':
                        os.chdir(root)
                        os.chroot(root)

                    try:
                        os.execv(command[0], command)
                    except Exception, e:
                        sys.stderr.write('%s\n' %e)
                    os._exit(1)

                os.close(p[0])
                if datasource == 'stdin':
                    for filename in sorted(hi.tagToFile[tagInfo]):
                        try:
                            os.write(p[1], filename + "\n")
                        except OSError, e:
                            if e.errno != errno.EPIPE:
                                raise
                            log.error(str(e))
                            break
                elif datasource == 'multitag':
                    for fileName in sorted(hi.fileToTag):
                        try:
                            os.write(p[1], "%s\n%s\n" %(" ".join(
                                sorted([x.tag for x in
                                        hi.fileToTag[fileName]])),
                                fileName))
                        except OSError, e:
                            if e.errno != errno.EPIPE:
                                raise
                            log.error(str(e))
                            break
                os.close(p[1])

                (id, status) = os.waitpid(pid, 0)
                if not os.WIFEXITED(status) or os.WEXITSTATUS(status):
                    log.error("%s failed", command[0])
