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

from repository import changeset
import errno
from repository import filecontents
import files
from lib import log
import os
from lib import patch
import stat
import sys
import tempfile
import trove
from lib import util
import versions

from build import tags

MERGE = 1 << 0
REPLACEFILES = 1 << 1
IGNOREUGIDS = 1 << 2
MISSINGFILESOKAY = 1 << 3
KEEPEXISTING = 1 << 4
        
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

	for tag in fileObj.tags:
	    if self.tagUpdates.has_key(tag):
		self.tagUpdates[tag].append(target)
	    else:
		self.tagUpdates[tag] = [ target ]

    def _remove(self, fileObj, target, msg):
	if isinstance(fileObj, files.Directory):
	    if not self.directorySet.has_key(target):
		self.directorySet[target] = 0
	else:
	    self.removes[target] = (fileObj, msg)
	    dir = os.path.dirname(target)
	    if self.directorySet.has_key(dir):
		self.directorySet[dir] += 1
	    else:
		self.directorySet[dir] = 1

	for tag in fileObj.tags:
	    if self.tagRemoves.has_key(tag):
		self.tagRemoves[tag].append(target)
	    else:
		self.tagRemoves[tag] = [ target ]

    def userRemoval(self, troveName, troveVersion, troveFlavor, pathId):
	if not self.userRemovals.has_key((troveName, troveVersion, troveFlavor)):
	    self.userRemovals[(troveName, troveVersion, troveFlavor)] = [ pathId ]
	else:
	    self.userRemovals[(troveName, troveVersion, troveFlavor)].append(pathId)

    def iterUserRemovals(self):
	for ((troveName, troveVersion, troveFlavor), pathIdList) in \
					    self.userRemovals.iteritems():
	    yield (troveName, troveVersion, troveFlavor, pathIdList)

    def _createFile(self, target, str, msg):
	self.newFiles.append((target, str, msg))

    def preapply(self, tagSet = {}, tagScript = None):
	# this is run before the change make it do the database
	rootLen = len(self.root)
	tagCommands = []

	if self.tagRemoves.has_key('tagdescription'):
	    for path in self.tagRemoves['tagdescription']:
		path = path[rootLen:]
		tagInfo = None	
		for ti in tagSet.itervalues():
		    if ti.tagFile[:rootLen] == self.root and \
		       ti.tagFile[rootLen:] == path: 
			tagInfo = ti
			break

		if tagInfo:
		    # this prevents us from trying to run "files add"
		    del tagSet[tagInfo.tag]

		    if self.tagRemoves.has_key(tagInfo.tag):
			# we're running "description preremove"; we don't need 
                        # to run "files preremove" as well, and we won't be
			# able to run "files remove"
			del self.tagRemoves[tagInfo.tag]

		    if "description preremove" in tagInfo.implements:
			tagCommands.append([ tagInfo, ("description", 
                                                       "preremove"), 
			   [x for x in 
				self.repos.iterFilesWithTag(tagInfo.tag) ] ] )

	    del self.tagRemoves['tagdescription']

	for tag, l in self.tagRemoves.iteritems():
	    if not tagSet.has_key(tag): continue
	    tagInfo = tagSet[tag]

	    if "files preremove" in tagInfo.implements:
		l.sort()
		cmd = [ tagInfo, ("files", "preremove"),
			    [ x[rootLen:] for x in l ] ]
		tagCommands.append(cmd)
	    
	if tagCommands:
	    runTagCommands(tagScript, self.root, tagCommands, preScript = True)

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

    def apply(self, tagSet = {}, tagScript = None):
	# this is run after the changes are in the database (but before
	# they are committed
	tagCommands = []
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
	for target in paths:
	    (fileObj, msg) = self.removes[target]

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
                # this means we've reached some contents that is the
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
                fileObj.restore(contents, self.root, target)
                del delayedRestores[match[0]]

                if fileObj.hasContents and fileObj.linkGroup.value():
                    linkGroup = fileObj.linkGroup.value()
                    self.linkGroups[linkGroup] = target

                continue

	    # None means "don't restore contents"; "" means "take the
	    # contents from the change set or from the database". If we 
            # take the file contents from the change set, we look for the
            # opportunity to make a hard link instead of actually restoring it.
	    fileObj = files.ThawFile(fileObj, pathId)

	    if override != "":
		contents = override
	    elif fileObj.hasContents:
                if fileObj.flags.isConfig() and not fileObj.flags.isSource():
                    # take the config file from the local database
                    contents = self.repos.getFileContents(
                                    [ (None, None, fileObj) ])[0]
                elif fileObj.linkGroup.value() and \
                        self.linkGroups.has_key(fileObj.linkGroup.value()):
                    # this creates links whose target we already know
                    # (because it was already present or already restored)
                    if self._createLink(fileObj.linkGroup.value(), target):
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

	    fileObj.restore(contents, self.root, target)
            if ptrTargets.has_key(pathId):
                ptrTargets[pathId] = target
	    log.debug(msg, target)

            if fileObj.hasContents and fileObj.linkGroup.value():
                linkGroup = fileObj.linkGroup.value()
                self.linkGroups[linkGroup] = target

	for (pathId, fileObj, target, msg, ptrId) in delayedRestores:
            # we wouldn't be here if the fileObj didn't have contents and
            # no override

            # the source of the link group may not have been restored
            # yet (it could be in the delayedRestore list itself). that's
            # fine; we just restore the contents here and make the links
            # for everything else
            if fileObj.linkGroup.value():
                linkGroup = fileObj.linkGroup.value()
                if self.linkGroups.has_key(linkGroup):
                    if self._createLink(fileObj.linkGroup.value(), target):
                        continue
                else:
                    linkGroup = fileObj.linkGroup.value()
                    self.linkGroups[linkGroup] = target

            fileObj.restore(filecontents.FromFilesystem(ptrTargets[ptrId]),
                            self.root, target)
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

	if self.tagUpdates.has_key('shlib'):
	    shlibAction(self.root, self.tagUpdates['shlib'],
                        tagScript = tagScript)
	    del self.tagUpdates['shlib']
	elif runLdconfig:
	    # override to force ldconfig to run on shlib removal
	    shlibAction(self.root, [])

	if self.tagUpdates.has_key('tagdescription'):
	    for path in self.tagUpdates['tagdescription']:
		# these are new tag action files which we need to run for
		# the first time. we run them against everything in the database
		# which has this tag, which includes the files we've just
		# installed

		tagInfo = tags.TagFile(path, {})
		path = path[len(self.root):]
		
		# don't run these twice
		if self.tagUpdates.has_key(tagInfo.tag):
		    del self.tagUpdates[tagInfo.tag]

		if "description update" in tagInfo.implements:
		    cmd = [ tagInfo, ("description", "update"),
			[x for x in self.repos.iterFilesWithTag(tagInfo.tag)] ]
		    tagCommands.append(cmd)
		elif "files update" in tagInfo.implements:
		    fileList = [x for x in 
				self.repos.iterFilesWithTag(tagInfo.tag) ] 
		    if fileList:
			cmd = [ tagInfo, ("files", "update"), fileList ]
			tagCommands.append(cmd)

		tagSet[tagInfo.tag] = tagInfo

	    del self.tagUpdates['tagdescription']

	for (tag, l) in self.tagUpdates.iteritems():
	    tagInfo = tagSet.get(tag, None)
	    if tagInfo is None: continue

	    if "files update" in tagInfo.implements:
		l.sort()
		cmd = [ tagInfo, ("files", "update"), 
		    [ x[rootLen:] for x in l ] ]
		tagCommands.append(cmd)

	for tag, l in self.tagRemoves.iteritems():
	    if not tagSet.has_key(tag): continue
	    tagInfo = tagSet[tag]

	    if "files remove" in tagInfo.implements:
		l.sort()
		cmd = [ tagInfo, ("files", "remove"),
			    [ x[rootLen:] for x in l ] ]
		tagCommands.append(cmd)
	    
	if tagCommands:
	    runTagCommands(tagScript, self.root, tagCommands)

    def getErrorList(self):
	return self.errors

    def iterNewPackageList(self):
	return iter(self.newPackages)

    def getOldPackageList(self):
	return self.oldPackages

    def getDirectoryCountSet(self):
	return self.directorySet

    def _handleRemoves(self, repos, pkgCs, changeSet, basePkg, fsPkg, root,
		       flags):
        # Remove old files. if the files have already been removed, just
        # mention that fact and continue. Don't erase files which
        # have changed contents.
	cwd = os.getcwd()

	for pathId in pkgCs.getOldFileList():
	    (path, fileId, version) = basePkg.getFile(pathId)

	    if not fsPkg.hasFile(pathId):
		log.debug("%s has already been removed" % path)
		continue

	    oldFile = repos.getFileVersion(pathId, fileId, version)
            # XXX mask out any flag that isn't the config flag.
            oldFile.flags.set(oldFile.flags.value() & files._FILE_FLAG_CONFIG)
            
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
			fsPkg.removeFile(pathId)
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
	    fsPkg.removeFile(pathId)

    def _singleTrove(self, repos, pkgCs, changeSet, basePkg, fsPkg, root,
		       flags):
	"""
	Build up the todo list for applying a single package to the
	filesystem. 

	@param repos: the repository the files for basePkg are stored in
	@type repos: repository.Repository
	@param pkgCs: the package changeset to apply to the filesystem
	@type pkgCs: trove.PackageChangeSet
	@param changeSet: the changeset pkgCs is part of
	@type changeSet: changeset.ChangeSet
	@param basePkg: the package the stuff in the filesystem came from
	@type basePkg: trove.Package
	@param fsPkg: the package representing what's in the filesystem now.
        it is updated to represent what will be in the filesystem for this
        trove if apply() is used.
	@type fsPkg: trove.Package
	@param root: root directory to apply changes to (this is ignored for
	source management, which uses the cwd)
	@type root: str
	@param flags: flags which modify update behavior.  See L{update}
        module variable summary for flags definitions.
	@type flags: int bitfield
	"""
	if basePkg:
	    assert(pkgCs.getOldVersion() == basePkg.getVersion())
	cwd = os.getcwd()

        # fully updated tracks whether any errors have occured; if no
        # errors occur, fsPkg gets updated to the new version of the trove
        # this doesn't matter for binary stuff, just source management
	fullyUpdated = True

	if (flags & IGNOREUGIDS) or os.getuid():
	    noIds = True
            # XXX this keeps attributes from being properly merged. we
            # need a better fix (twm needs to be made much more flexible)
            twmSkipList = [ "contents", "inode" ]
	else:
	    noIds = False
            twmSkipList = [ "contents" ]

        # Create new files. If the files we are about to create already
        # exist, it's an error.
	for (pathId, headPath, headFileId, headFileVersion) in pkgCs.getNewFileList():
	    if headPath[0] == '/':
		headRealPath = root + headPath
	    else:
		headRealPath = cwd + "/" + headPath

	    headFile = files.ThawFile(changeSet.getFileChange(None, headFileId), pathId)

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
                                       "file" % headRealPath)
                elif (not flags & REPLACEFILES and
                      not self.removes.has_key(headRealPath)):
                    self.errors.append("%s is in the way of a newly " 
                                       "created file" % headRealPath)
                    fullyUpdated = False
                    continue
            except OSError:
                # the path doesn't exist, carry on with the restore
                pass

	    self._restore(headFile, headRealPath, "creating %s")
	    fsPkg.addFile(pathId, headPath, headFileVersion, headFileId)

        # Handle files which have changed betweeen versions. This is by
        # far the most complicated case.
	for (pathId, headPath, headFileId, headFileVersion) in pkgCs.getChangedFileList():
	    if not fsPkg.hasFile(pathId):
		# the file was removed from the local system; this change
		# wins
		self.userRemoval(pkgCs.getName(), pkgCs.getNewVersion(),
                                 pkgCs.getNewFlavor(), pathId)
		continue

	    (fsPath, fsFileId, fsVersion) = fsPkg.getFile(pathId)
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
		# from the old package, take the other one as it is the one
		# which changed
		if basePkg.hasFile(pathId):
		    basePath = basePkg.getFile(pathId)[0]
		else:
		    basePath = None

		if (not flags & MERGE) or fsPath == basePath :
		    # the path changed in the repository, propagate that change
		    self._rename(rootFixup + fsPath, rootFixup + headPath,
		                 "renaming %s to %s" % (fsPath, headPath))

		    fsPkg.addFile(pathId, headPath, fsVersion, fsFileId)
		    finalPath = headPath
		else:
		    pathOkay = False
		    finalPath = fsPath	# let updates work still
		    self.errors.append("path conflict for %s (%s on head)" % 
                                       (fsPath, headPath))

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

            # FIXME we should be able to inspect headChanges directly
            # to see if we need to go into the if statement which follows
            # this rather then having to look up the file from the old
            # package for every file which has changed
            fsFile = files.FileFromFilesystem(realPath, pathId)
            
            # get the baseFile which was originally installed
            (baseFilePath, baseFileId, baseFileVersion) = basePkg.getFile(pathId)
            baseFile = repos.getFileVersion(pathId, baseFileId, baseFileVersion)
            
            # link groups come from the database; they aren't inferred from
            # the filesystem
            if fsFile.hasContents and baseFile.hasContents:
                fsFile.linkGroup.set(baseFile.linkGroup.value())

            # now assemble what the file is supposed to look like on head
            headChanges = changeSet.getFileChange(baseFileId, headFileId)
            if headChanges[0] == '\x01':
                # the file was stored as a diff
                headFile = baseFile.copy()
                headFile.twm(headChanges, headFile)
            else:
                # the file was stored frozen. this happens when the file
                # type changed between versions
                headFile = files.ThawFile(headChanges, pathId)
                
            fsFile.flags.isConfig(headFile.flags.isConfig())
            fsFile.flags.isSource(headFile.flags.isSource())

            # this is changed to true when the file attributes have changed;
            # this helps us know if we need a restore event
	    attributesChanged = False

            # this forces the file to be restored, with contents
            forceUpdate = False

            # handle file types changing. this is dealt with as a bit
            # of an exception
            fileTypeError = False
            if baseFile.lsTag != headFile.lsTag:
                # the file type changed between versions. 
                if flags & REPLACEFILES or baseFile.lsTag == fsFile.lsTag:
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
			baseFile.inode.setOwner(headFile.inode.owner())
			baseFile.inode.setGroup(headFile.inode.group())

		    conflicts = fsFile.twm(headChanges, baseFile, 
					   skip = twmSkipList)
		    if not conflicts:
			attributesChanged = True
		    else:
			contentsOkay = False
			self.errors.append("file attributes conflict for %s"
						% realPath)
		else:
		    # this forces the change to apply
		    fsFile.twm(headChanges, fsFile, skip = "contents")
		    attributesChanged = True

	    beenRestored = False

	    if forceUpdate or (
                   headFile.hasContents and \
                   fsFile.hasContents and \
                   fsFile.contents.sha1() != headFile.contents.sha1() and \
                   headFile.contents.sha1() != baseFile.contents.sha1()
                ):

		if forceUpdate or (flags & REPLACEFILES) or \
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
					basePkg.getFile(pathId)[2]) ])[0].get()

			baseLines = baseLineF.readlines()
			del baseLineF
			diff = headFileContents.get().readlines()
			(newLines, failedHunks) = patch.patch(baseLines, diff)
			assert(not failedHunks)
			headFileContents = \
			    filecontents.FromString("".join(newLines))

			self._restore(fsFile, realPath, 
				      "replacing %s with contents "
				      "from repository",
				      contentsOverride = headFileContents)
		    else:
                        # switch the fsFile to the sha1 for the new file
                        if fsFile.hasContents:
                            fsFile.contents.setSha1(headFile.contents.sha1())
			self._restore(fsFile, realPath, 
				      "replacing %s with contents "
				      "from repository")

		    beenRestored = True
		elif headFile.contents == baseFile.contents:
		    # it changed in just the filesystem, so leave that change
		    log.debug("preserving new contents of %s" % finalPath)
		elif headFile.flags.isConfig() and \
					    not baseFile.flags.isConfig():
		    # it changed in the filesystem and the repository, and
		    # but it wasn't always a config files. this means we
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
                                (realPath, realPath))

			contentsOkay = True
		else:
		    self.errors.append("file contents conflict for %s" % realPath)
		    contentsOkay = False
            elif headFile.hasContents and headFile.linkGroup.value():
                # the contents haven't changed, but the link group has changed.
                # we want to let files in that link group hard link to this file
                # (if appropriate)
                self._registerLinkGroup(headFile.linkGroup.value(), realPath)

	    if attributesChanged and not beenRestored:
		self._restore(fsFile, realPath, 
		      "merging changes from repository into %s",
		      contentsOverride = None)

	    if pathOkay and contentsOkay:
		# XXX this doesn't even attempt to merge file permissions
		# and such; the good part of that is differing owners don't
		# break things
		fsPkg.addFile(pathId, finalPath, headFileVersion, 
                              fsFile.fileId())
	    else:
		fullyUpdated = False

	if fullyUpdated:
	    fsPkg.changeVersion(pkgCs.getNewVersion())

	return fsPkg

    def __init__(self, repos, changeSet, fsPkgDict, root, flags = MERGE):
	"""
	Constructs the job for applying a change set to the filesystem.

	@param repos: the repository the current package and file information 
	is in
	@type repos: repository.Repository
	@param changeSet: the changeset to apply to the filesystem
	@type changeSet: changeset.ChangeSet
	@param fsPkgDict: dictionary mapping a package name to the package
	object representing what's currently stored in the filesystem
	@type fsPkgDict: dict of trove.Package
	@param root: root directory to apply changes to (this is ignored for
	source management, which uses the cwd)
	@type root: str
	@param flags: flags which modify update behavior.  See L{update}
        module variable summary for flags definitions.
	@type flags: int bitfield
	"""
	self.renames = []
	self.restores = []
	self.removes = {}
	self.oldPackages = []
	self.errors = []
	self.newFiles = []
	self.root = root
	self.changeSet = changeSet
	self.directorySet = {}
	self.userRemovals = {}
	self.tagUpdates = {}
	self.tagRemoves = {}
        self.linkGroups = {}
	self.repos = repos

        pkgList = []

	for pkgCs in changeSet.iterNewPackageList():
            old = pkgCs.getOldVersion()
	    if old:
		localVer = old.fork(versions.LocalBranch(), sameVerRel = 1)
                newFsPkg = fsPkgDict[(pkgCs.getName(), localVer)].copy()
            else:
                newFsPkg = trove.Trove(pkgCs.getName(), versions.NewVersion(),
                                    pkgCs.getNewFlavor(), pkgCs.getChangeLog())

            pkgList.append((pkgCs, newFsPkg))

	for (pkgCs, newFsPkg) in pkgList:
	    old = pkgCs.getOldVersion()

	    if old:
		basePkg = repos.getTrove(pkgCs.getName(), old, 
                                         pkgCs.getOldFlavor())
	    else:
                basePkg = None

            self._handleRemoves(repos, pkgCs, changeSet, basePkg,
                                      newFsPkg, root, flags)

	for (pkgCs, newFsPkg) in pkgList:
	    old = pkgCs.getOldVersion()

	    if old:
		basePkg = repos.getTrove(pkgCs.getName(), old, 
                                         pkgCs.getOldFlavor())
		self.oldPackages.append((basePkg.getName(), 
					 basePkg.getVersion(),
					 basePkg.getFlavor()))
	    else:
                basePkg = None

            self._singleTrove(repos, pkgCs, changeSet, basePkg,
                                      newFsPkg, root, flags)

            newFsPkg.mergeTroveListChanges(pkgCs.iterChangedTroves(),
                                           redundantOkay = True)

        pkgList = [ x[1] for x in pkgList ]
        self.newPackages = pkgList

	if flags & KEEPEXISTING:
	    return

	for (name, oldVersion, oldFlavor) in changeSet.getOldPackageList():
	    self.oldPackages.append((name, oldVersion, oldFlavor))
	    oldPkg = repos.getTrove(name, oldVersion, oldFlavor)
	    for (pathId, path, fileId, version) in oldPkg.iterFileList():
		fileObj = repos.getFileVersion(pathId, fileId, version)
		self._remove(fileObj, root + path, "removing %s")

def _localChanges(repos, changeSet, curPkg, srcPkg, newVersion, root, flags,
                  withFileContents=True, forceSha1=False, 
                  ignoreTransient=False):
    """
    Populates a change set against the files in the filesystem and builds
    a package object which describes the files installed.  The return
    is a tuple with a boolean saying if anything changed and a package
    reflecting what's in the filesystem; the changeSet is updated as a
    side effect.

    @param repos: Repository this directory is against.
    @type repos: repository.Repository
    @param changeSet: Changeset to update with information for this package
    @type changeSet: changeset.ChangeSet
    @param curPkg: Package which is installed
    @type curPkg: trove.Package
    @param srcPkg: Package to generate the change set against
    @type srcPkg: trove.Package
    @param newVersion: version to use for the newly created package
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
    """

    noIds = ((flags & IGNOREUGIDS) != 0)

    newPkg = curPkg.copy()
    newPkg.changeVersion(newVersion)

    pathIds = {}
    for (pathId, path, fileId, version) in newPkg.iterFileList():
	pathIds[pathId] = True

    # Iterating over the files in newPkg would be much more natural
    # then iterating over the ones in the old package, and then going
    # through newPkg to find what we missed. However, doing it the
    # hard way lets us iterate right over the changeset we get from
    # the repository.
    if srcPkg:
	fileList = [ x for x in srcPkg.iterFileList() ]
	# need to walk changesets in order of fileid
	fileList.sort()
    else:
	fileList = []

    # Used in the loops to determine whether to mark files as config
    # would be nice to have a better list...
    nonCfgExt = ('ps', 'eps', 'gif', 'png', 'tiff', 'jpeg', 'jpg',
	'ico', 'rpm', 'ccs', 'gz', 'bz2', 'tgz', 'tbz', 'tbz2')
    isSrcPkg = curPkg.getName().endswith(':source')

    for (pathId, srcPath, srcFileId, srcFileVersion) in fileList:
	# file disappeared
	if not pathIds.has_key(pathId): continue

	(path, fileId, version) = newPkg.getFile(pathId)
	del pathIds[pathId]

	if path[0] == '/':
	    realPath = root + path
	else:
	    realPath = os.getcwd() + "/" + path

	try:
	    os.lstat(realPath)
	except OSError:
            if isSrcPkg:
		log.error("%s is missing (use remove if this is intentional)" 
		    % path)
                return None

	    if (flags & MISSINGFILESOKAY) == 0:
		log.warning("%s is missing (use remove if this is intentional)" 
		    % path)

            newPkg.removeFile(pathId)
            continue

	srcFile = repos.getFileVersion(pathId, srcFileId, srcFileVersion)

        # transient files never show up in in local changesets...
        if ignoreTransient and srcFile.flags.isTransient():
            continue

        if forceSha1:
            possibleMatch = None
        else:
            possibleMatch = srcFile

	f = files.FileFromFilesystem(realPath, pathId,
				     possibleMatch = possibleMatch)

	if isSrcPkg:
	    f.flags.isSource(set = True)

        # the link group doesn't change due to local mods
        if srcFile.hasContents and f.hasContents:
            f.linkGroup.set(srcFile.linkGroup.value())

        # these values are not picked up from the local system
        if hasattr(f, 'requires') and hasattr(srcFile, 'requires'):
            f.requires.set(srcFile.requires.value())
        if srcFile.hasContents and f.hasContents:
            f.provides.set(srcFile.provides.value())
        f.flags.set(srcFile.flags.value())
        if srcFile.hasContents and f.hasContents:
            f.flavor.set(srcFile.flavor.value())
        f.tags = srcFile.tags.copy()

	extension = path.split(".")[-1]
	if isSrcPkg and extension not in nonCfgExt:
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
	    newPkg.addFile(pathId, path, newVersion, f.fileId())

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
            

    for pathId in pathIds.iterkeys():
	(path, fileId, version) = newPkg.getFile(pathId)

	if path[0] == '/':
	    realPath = root + path
	else:
	    realPath = os.getcwd() + "/" + path

	# if we're committing against head, this better be a new file.
	# if we're generating a diff against someplace else, it might not 
	# be.
	assert(srcPkg or isinstance(version, versions.NewVersion))

	f = files.FileFromFilesystem(realPath, pathId)

	extension = path.split(".")[-1]
	if isSrcPkg:
            f.flags.isSource(set = True)
            if extension not in nonCfgExt:
                f.flags.isConfig(set = True)

	# new file, so this part is easy
	changeSet.addFile(None, f.fileId(), f.freeze())
	newPkg.addFile(pathId, path, newVersion, f.fileId())

	if f.hasContents and withFileContents:
	    newCont = filecontents.FromFilesystem(realPath)
	    changeSet.addFileContents(pathId,
				      changeset.ChangedFileTypes.file,
				      newCont, f.flags.isConfig())

    (csPkg, filesNeeded, pkgsNeeded) = newPkg.diff(srcPkg)
    assert(not pkgsNeeded)
    changeSet.newPackage(csPkg)

    if (csPkg.getOldFileList() or csPkg.getChangedFileList()
        or csPkg.getNewFileList()):
	foundDifference = 1
    else:
	foundDifference = 0

    return (foundDifference, newPkg)

def buildLocalChanges(repos, pkgList, root = "", withFileContents=True,
                                                      forceSha1 = False):
    """
    Builds a change set against a set of files currently installed and
    builds a package object which describes the files installed.  The
    return is a changeset and a list of tuples, each with a boolean
    saying if anything changed for a package reflecting what's in the
    filesystem for that trove.

    @param repos: Repository this directory is against.
    @type repos: repository.Repository
    @param pkgList: Specifies which pacakage to work on, and is a list
    of (curPkg, srcPkg, newVersion, flags) tuples as defined in the parameter
    list for _localChanges()
    @param root: root directory the files are in (ignored for sources, which
    are assumed to be in the current directory)
    @type root: str
    @param forceSha1: disallows the use of inode information to avoid
                      checking the sha1 of the file if the inode information 
                      matches exactly.
    @type forceSha1: bool
    """

    changeSet = changeset.ChangeSet()
    returnList = []
    for (curPkg, srcPkg, newVersion, flags) in pkgList:
	result = _localChanges(repos, changeSet, curPkg, srcPkg, newVersion, 
			       root, flags, withFileContents=withFileContents,
                               forceSha1=forceSha1)
        if result is None:
            # an error occurred
            return None
	returnList.append(result)

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

def runTagCommands(tagScript, root, cmdList, preScript = False):
    if tagScript:
	if preScript:
	    pre = "# "
	else:
	    pre = ""

	f = open(tagScript, "a")
	for (tagInfo, cmd, args) in cmdList:
	    if tagInfo.datasource == 'args':
		f.write("%s%s %s %s\n" % (pre, tagInfo.file, " ".join(cmd), 
					" ".join(args)))
	    else:
		f.write("%s%s %s <<EOF\n" % (pre, tagInfo.file, " ".join(cmd)))
		for arg in args:
		    f.write("%s%s\n" % (pre, arg))
		f.write("%sEOF\n" % pre)
		
	f.close()
	return

    uid = os.getuid()

    for (tagInfo, cmd, args) in cmdList:
	log.debug("running %s %s", tagInfo.file, " ".join(cmd))
	if root != '/' and uid:
	    continue

	fullCmd = [ tagInfo.file ] + list(cmd)
	if tagInfo.datasource == 'args':
	    fullCmd += args

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
		os.execv(fullCmd[0], fullCmd)
	    except Exception, e:
		sys.stderr.write('%s\n' %e)
	    os._exit(1)

	os.close(p[0])
	if tagInfo.datasource == 'stdin':
	    for arg in args:
                try:
                    os.write(p[1], arg + "\n")
                except OSError, e:
                    if e.errno != errno.EPIPE:
                        raise
                    log.error(str(e))
                    break
        os.close(p[1])

	(id, status) = os.waitpid(pid, 0)
	if not os.WIFEXITED(status) or os.WEXITSTATUS(status):
	    log.error("%s failed", cmd[0])
