#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved

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
import log
import os
import package
import patch
import stat
import sys
import tempfile
import util
import versions

from build import tags

MERGE = 1 << 0
REPLACEFILES = 1 << 1
IGNOREUGIDS = 1 << 2
        
class FilesystemJob:
    """
    Represents a set of actions which need to be applied to the filesystem.
    This is kept very simple to minimize the chance of mistakes or errors.
    """

    def _rename(self, oldPath, newPath, msg):
	self.renames.append((oldPath, newPath, msg))

    def _restore(self, fileObj, target, msg, contentsOverride = ""):
	self.restores.append((fileObj.id(), fileObj, target, contentsOverride, 
			      msg))

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

    def userRemoval(self, troveName, troveVersion, troveFlavor, fileId):
	if not self.userRemovals.has_key((troveName, troveVersion, troveFlavor)):
	    self.userRemovals[(troveName, troveVersion, troveFlavor)] = [ fileId ]
	else:
	    self.userRemovals.append(fileId)

    def iterUserRemovals(self):
	for ((troveName, troveVersion, troveFlavor), fileIdList) in \
					    self.userRemovals.iteritems():
	    yield (troveName, troveVersion, troveFlavor, fileIdList)

    def _createFile(self, target, str, msg):
	self.newFiles.append((target, str, msg))

    def apply(self, tagSet = {}, tagScript = None):
	tagCommands = []
	runLdconfig = False
	rootLen = len(self.root)

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
		    del tagSet[tagInfo.tag]
		    if "self preremove" in tagInfo.implements:
			tagCommands.append([ path, "self", "preremove" ])

	    del self.tagRemoves['tagdescription']

	for tag, l in self.tagRemoves.iteritems():
	    if not tagSet.has_key(tag): continue
	    tagInfo = tagSet[tag]

	    if "files preremove" in tagInfo.implements:
		l.sort()
		cmd = [ tagInfo.file, "files", "preremove"] + \
			    [ x[rootLen:] for x in l ]
		tagCommands.append(cmd)
	    
	if tagCommands:
	    if tagScript:
		f = open(tagScript, "a")
		for cmd in tagCommands:
		    f.write("# %s\n" % " ".join(cmd))
		f.close()
	    else:
		runTagCommands(self.root, tagCommands)

	tagCommands = []

	for (oldPath, newPath, msg) in self.renames:
	    os.rename(oldPath, newPath)
	    log.debug(msg)

	contents = None
	# restore in the same order files appear in the change set
	self.restores.sort()
	for (fileId, fileObj, target, override, msg) in self.restores:
	    # None means "don't restore contents"; "" means "take the
	    # contents from the change set"
	    if override != "":
		contents = override
	    elif fileObj.hasContents:
		contents = self.changeSet.getFileContents(fileId)[1]
	    fileObj.restore(contents, self.root, target, contents != None)
	    log.debug(msg)

	paths = self.removes.keys()
	paths.sort()
	paths.reverse()
	for target in paths:
	    (fileObj, msg) = self.removes[target]

	    # don't worry about files which don't exist
	    try:
		os.lstat(target)
	    except OSError:
		pass	
	    else:
		fileObj.remove(target)

	    log.debug(msg)

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
	    shlibAction(self.root, self.tagUpdates['shlib'])
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

		if "self update" in tagInfo.implements:
		    cmd = [ path, "self", "update" ] + \
			[x for x in self.repos.iterFilesWithTag(tagInfo.tag)]
		    tagCommands.append(cmd)
		elif "files update" in tagInfo.implements:
		    cmd = [ path, "files", "update" ] + \
			[x for x in self.repos.iterFilesWithTag(tagInfo.tag)]
		    if len(cmd) > 3:
			tagCommands.append(cmd)

		tagSet[tagInfo.tag] = tagInfo

	    del self.tagUpdates['tagdescription']

	for (tag, l) in self.tagUpdates.iteritems():
	    tagInfo = tagSet.get(tag, None)
	    if tagInfo is None: continue

	    if "files update" in tagInfo.implements:
		l.sort()
		cmd = [ tagInfo.file, "files", "update" ] + \
		    [ x[rootLen:] for x in l ]
		tagCommands.append(cmd)

	for tag, l in self.tagRemoves.iteritems():
	    if not tagSet.has_key(tag): continue
	    tagInfo = tagSet[tag]

	    if "files remove" in tagInfo.implements:
		l.sort()
		cmd = [ tagInfo.file, "files", "remove"] + \
			    [ x[rootLen:] for x in l ]
		tagCommands.append(cmd)
	    
	if tagCommands:
	    if tagScript:
		f = open(tagScript, "a")
		f.write("\n".join([" ".join(x) for x in tagCommands]))
		f.write("\n")
		f.close()
	    else:
		runTagCommands(self.root, tagCommands)

    def getErrorList(self):
	return self.errors

    def iterNewPackageList(self):
	return iter(self.newPackages)

    def getOldPackageList(self):
	return self.oldPackages

    def getDirectoryCountSet(self):
	return self.directorySet

    def _singlePackage(self, repos, pkgCs, changeSet, basePkg, fsPkg, root,
		       flags):
	"""
	Build up the todo list for applying a single package to the
	filesystem. Returns a package object which represents what will
	end up in the filsystem after this object's apply() method is
	called.

	@param repos: the repository the files for basePkg are stored in
	@type repos: repository.Repository
	@param pkgCs: the package changeset to apply to the filesystem
	@type pkgCs: package.PackageChangeSet
	@param changeSet: the changeset pkgCs is part of
	@type changeSet: changeset.ChangeSet
	@param basePkg: the package the stuff in the filesystem came from
	@type basePkg: package.Package
	@param fsPkg: the package representing what's in the filesystem now
	@type fsPkg: package.Package
	@param root: root directory to apply changes to (this is ignored for
	source management, which uses the cwd)
	@type root: str
	@param flags: flags which modify update behavior.  See L{update}
        module variable summary for flags definitions.
	@type flags: int bitfield
	@rtype: package.Package
	"""
	if basePkg:
	    assert(pkgCs.getOldVersion() == basePkg.getVersion())
	fullyUpdated = 1
	cwd = os.getcwd()

	if (flags & IGNOREUGIDS) or os.getuid():
	    noIds = True
	else:
	    noIds = False

	if fsPkg:
	    fsPkg = fsPkg.copy()
	else:
	    fsPkg = package.Trove(pkgCs.getName(), versions.NewVersion(),
				    pkgCs.getFlavor(), pkgCs.getChangeLog())

	fsPkg.mergeTroveListChanges(pkgCs.iterChangedTroves(),
				    redundantOkay = True)

	for (fileId, headPath, headFileVersion) in pkgCs.getNewFileList():
	    if headPath[0] == '/':
		headRealPath = root + headPath
	    else:
		headRealPath = cwd + "/" + headPath

	    headFile = files.ThawFile(changeSet.getFileChange(fileId), fileId)

            try:
                s = os.lstat(headRealPath)
                # if this file is a directory and the file on the file
                # system is a directory, we're OK
                if (isinstance(headFile, files.Directory)
                    and stat.S_ISDIR(s.st_mode)):
                    # FIXME: this isn't the right directory handling
                    # we will want to set ownership/permissions if
                    # they don't conflict with any already-installed package
                    continue
                elif not flags & REPLACEFILES:
                    self.errors.append("%s is in the way of a newly " 
                                       "created file" % headRealPath)
                    fullyUpdated = 0
                    continue
            except OSError:
                # the path doesn't exist, carry on with the restore
                pass

	    self._restore(headFile, headRealPath, "creating %s" % headRealPath)

	    fsPkg.addFile(fileId, headPath, headFileVersion)

	for fileId in pkgCs.getOldFileList():
	    (path, version) = basePkg.getFile(fileId)

	    if not fsPkg.hasFile(fileId):
		log.debug("%s has already been removed" % path)
		continue

	    if path[0] == '/':
		realPath = root + path
	    else:
		realPath = cwd + "/" + path

	    if flags & MERGE:
		try:
		    # don't remove files if they've been changed locally
		    localFile = files.FileFromFilesystem(realPath, fileId)
		except OSError, exc:
		    # it's okay if the file is missing, it means we all agree
		    if exc.errno == errno.ENOENT:
			fsPkg.removeFile(fileId)
			continue
		    else:
			raise

	    oldFile = repos.getFileVersion(fileId, version)
            # XXX mask out any flag that isn't the config flag.
            # There are some flags that the localFile won't have
            # such as SHLIB or INITSCRIPT
            oldFile.flags.set(oldFile.flags.value() & files._FILE_FLAG_CONFIG)
            
	    # don't worry about metadata changes, just content changes
	    if oldFile.hasContents and localFile.hasContents and \
			oldFile.contents != localFile.contents:
		self.errors.append("%s has changed but has been removed "
				   "on head" % path)
		continue

	    self._remove(oldFile, realPath, "removing %s" % path)	
	    fsPkg.removeFile(fileId)

	for (fileId, headPath, headFileVersion) in pkgCs.getChangedFileList():
	    if not fsPkg.hasFile(fileId):
		# the file was removed from the local system; this change
		# wins
		self.userRemoval(pkgCs.getName(), pkgCs.getNewVersion(),
                                 pkgCs.getFlavor(), fileId)
		continue

	    (fsPath, fsVersion) = fsPkg.getFile(fileId)
	    if fsPath[0] == "/":
		rootFixup = root
	    else:
		rootFixup = cwd + "/"

	    pathOkay = 1
	    contentsOkay = 1
	    finalPath = fsPath
	    # if headPath is none, the name hasn't changed in the repository
	    if headPath and headPath != fsPath:
		# the paths are different; if one of them matches the one
		# from the old package, take the other one as it is the one
		# which changed
		if basePkg.hasFile(fileId):
		    basePath = basePkg.getFile(fileId)[0]
		else:
		    basePath = None

		if (not flags & MERGE) or fsPath == basePath :
		    # the path changed in the repository, propage that change
		    self._rename(rootFixup + fsPath, rootFixup + headPath,
		                 "renaming %s to %s" % (fsPath, headPath))

		    fsPkg.addFile(fileId, headPath, fsVersion)
		    finalPath = headPath
		else:
		    pathOkay = 0
		    finalPath = fsPath	# let updates work still
		    self.errors.append("path conflict for %s (%s on head)" % 
                                       (fsPath, headPath))

	    realPath = rootFixup + finalPath

	    # headFileVersion is None for renames
	    if headFileVersion:
		# FIXME we should be able to inspect headChanges directly
		# to see if we need to go into the if statement which follows
		# this rather then having to look up the file from the old
		# package for every file which has changed
		fsFile = files.FileFromFilesystem(realPath, fileId)
		
		if not basePkg.hasFile(fileId):
		    # a file which was not in the base package was created
		    # on both the head of the branch and in the filesystem;
		    # this can happen during source management
		    self.errors.append("new file %s conflicts with file on "
                                       "head of branch" % realPath)
		    contentsOkay = 0
		else:
		    (baseFilePath, baseFileVersion) = basePkg.getFile(fileId)
		    baseFile = repos.getFileVersion(fileId, baseFileVersion)
		
		headChanges = changeSet.getFileChange(fileId)
		headFile = baseFile.copy()
		headFile.twm(headChanges, headFile)
		fsFile.flags.isConfig(headFile.flags.isConfig())
		fsChanges = fsFile.diff(baseFile)

	    attributesChanged = False

	    if basePkg and headFileVersion and \
	         not fsFile.metadataEqual(headFile, ignoreOwnerGroup = noIds):
		# something has changed for the file
		if flags & MERGE:
		    if noIds:
			# we don't want to merge owner/group ids in
			# this case (something other than owner/group
			# # changed, such as size).  simply take the
			# head values
			baseFile.inode.setOwner(headFile.inode.owner())
			baseFile.inode.setGroup(headFile.inode.group())

		    conflicts = fsFile.twm(headChanges, baseFile, 
					   skip = "contents")
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

	    else:
		conflicts = True
		mergedChanges = None

	    beenRestored = False

	    if headFileVersion and headFile.hasContents and \
	       fsFile.hasContents and \
	       fsFile.contents.sha1() != headFile.contents.sha1():
		# the contents have changed... let's see what to do

		# get the contents if the version on head has contents, and
		# either
		#	1. the version from the base package doesn't have 
		#	   contents, or
		#	2. the file changed between head and base
		# (if both are false, no contents would have been saved for
		# this file)
		if (headFile.hasContents
                    and (not baseFile.hasContents
                         or headFile.contents.sha1() != 
			    baseFile.contents.sha1())):
		    headFileContType = changeSet.getFileContentsType(fileId)
		else:
		    headFileContType = None

		if (flags & REPLACEFILES) or (not flags & MERGE) or \
				fsFile.contents == baseFile.contents:
		    # the contents changed in just the repository, so take
		    # those changes
		    if headFileContType == changeset.ChangedFileTypes.diff:
			sha1 = baseFile.contents.sha1()
			baseLineF = repos.getFileContents(pkgCs.getName(),
					pkgCs.getOldVersion(), pkgCs.getFlavor(),
					basePkg.getFile(fileId)[0])

			baseLines = baseLineF.readlines()
			del baseLineF
			headFileContents = changeSet.getFileContents(fileId)[1]
			diff = headFileContents.get().readlines()
			(newLines, failedHunks) = patch.patch(baseLines, diff)
			assert(not failedHunks)
			headFileContents = \
			    filecontents.FromString("".join(newLines))

			self._restore(fsFile, realPath, 
				      "replacing %s with contents "
				      "from repository" % realPath,
				      contentsOverride = headFileContents)
		    else:
			self._restore(fsFile, realPath, 
				      "replacing %s with contents "
				      "from repository" % realPath)

		    beenRestored = True
		elif headFile.contents == baseFile.contents:
		    # it changed in just the filesystem, so leave that change
		    log.debug("preserving new contents of %s" % realPath)
		elif fsFile.flags.isConfig() or headFile.flags.isConfig():
		    # it changed in both the filesystem and the repository; our
		    # only hope is to generate a patch for what changed in the
		    # repository and try and apply it here
		    if headFileContType != changeset.ChangedFileTypes.diff:
			self.errors.append("unexpected content type for %s" % 
						realPath)
			contentsOkay = 0
		    else:
			cur = open(realPath, "r").readlines()
			headFileContents = changeSet.getFileContents(fileId)[1]
			diff = headFileContents.get().readlines()
			(newLines, failedHunks) = patch.patch(cur, diff)

			cont = filecontents.FromString("".join(newLines))
			self._restore(fsFile, realPath, 
			      "merging changes from repository into %s" % 
			      realPath,
			      contentsOverride = cont)
			beenRestored = True

			if failedHunks:
			    self._createFile(
                                realPath + ".conflicts", 
                                "".join([x.asString() for x in failedHunks]),
                                "conflicts from merging changes from " 
                                "head into %s saved as %s.conflicts" % 
                                (realPath, realPath))

			contentsOkay = 1
		else:
		    self.errors.append("file contents conflict for %s" % realPath)
		    contentsOkay = 0

	    if attributesChanged and not beenRestored:
		self._restore(fsFile, realPath, 
		      "merging changes from repository into %s" % realPath,
		      contentsOverride = None)

	    if pathOkay and contentsOkay:
		# XXX this doesn't even attempt to merge file permissions
		# and such; the good part of that is differing owners don't
		# break things
		if not headFileVersion:
		    headFileVersion = fsPkg.getFile(fileId)[1]
		fsPkg.addFile(fileId, finalPath, headFileVersion)
	    else:
		fullyUpdated = 0

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
	@type fsPkgDict: dict of package.Package
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
	self.newPackages = []
	self.oldPackages = []
	self.errors = []
	self.newFiles = []
	self.root = root
	self.changeSet = changeSet
	self.directorySet = {}
	self.userRemovals = {}
	self.tagUpdates = {}
	self.tagRemoves = {}
	self.repos = repos

	for pkgCs in changeSet.iterNewPackageList():
	    name = pkgCs.getName()
	    old = pkgCs.getOldVersion()
	    if old:
		localVer = old.fork(versions.LocalBranch(), sameVerRel = 1)
		basePkg = repos.getTrove(name, old, pkgCs.getFlavor())
		pkg = self._singlePackage(repos, pkgCs, changeSet, basePkg, 
				      fsPkgDict[(name, localVer)], root, flags)
		self.oldPackages.append((basePkg.getName(), 
					 basePkg.getVersion(),
					 basePkg.getFlavor()))
	    else:
		pkg = self._singlePackage(repos, pkgCs, changeSet, None, 
					  None, root, flags)

	    self.newPackages.append(pkg)

	for (name, oldVersion, oldFlavor) in changeSet.getOldPackageList():
	    self.oldPackages.append((name, oldVersion, oldFlavor))
	    oldPkg = repos.getTrove(name, oldVersion, oldFlavor)
	    for (fileId, path, version) in oldPkg.iterFileList():
		fileObj = repos.getFileVersion(fileId, version)
		self._remove(fileObj, root + path,
			     "removing %s" % root + path)

def _localChanges(repos, changeSet, curPkg, srcPkg, newVersion, root, flags):
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
    @type curPkg: package.Package
    @param srcPkg: Package to generate the change set against
    @type srcPkg: package.Package
    @param newVersion: version to use for the newly created package
    @type newVersion: versions.NewVersion
    @param root: root directory the files are in (ignored for sources, which
    are assumed to be in the current directory)
    @type root: str
    @param flags: IGNOREUGIDS or zero
    @type flags: int
    """

    noIds = ((flags & IGNOREUGIDS) != 0)

    newPkg = curPkg.copy()
    newPkg.changeVersion(newVersion)

    fileIds = {}
    for (fileId, path, version) in newPkg.iterFileList():
	fileIds[fileId] = True

    """
    Iterating over the files in newPkg would be much more natural then
    iterating over the ones in the old package, and then going through
    newPkg to find what we missed. However, doing it the hard way lets
    us iterate right over the changeset we get from the repository.
    """
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

    for (fileId, srcPath, srcFileVersion) in fileList:
	# file disappeared
	if not fileIds.has_key(fileId): continue

	(path, version) = newPkg.getFile(fileId)
	del fileIds[fileId]

	if path[0] == '/':
	    realPath = root + path
	else:
	    realPath = os.getcwd() + "/" + path

	try:
	    os.lstat(realPath)
	except OSError:
	    log.error("%s is missing (use remove if this is intentional)" 
		% path)
	    return None

	srcFile = repos.getFileVersion(fileId, srcFileVersion)

	f = files.FileFromFilesystem(realPath, fileId,
				     possibleMatch = srcFile)
	
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

	if not f.metadataEqual(srcFile, ignoreOwnerGroup = noIds):
	    newPkg.addFile(fileId, path, newVersion)

	    (filecs, hash) = changeset.fileChangeSet(fileId, srcFile, f)
	    changeSet.addFile(fileId, srcFileVersion, newVersion, filecs)
	    if hash:
		newCont = filecontents.FromFilesystem(realPath)

		if srcFile.hasContents:
		    theFile = repos.getFileContents(srcPkg.getName(),
				srcPkg.getVersion(), srcPkg.getFlavor(), srcPath)
		    srcCont = filecontents.FromFile(theFile)

		(contType, cont) = changeset.fileContentsDiff(srcFile, srcCont,
                                                              f, newCont)
						
		changeSet.addFileContents(fileId, contType, cont, 
					  f.flags.isConfig())

    for fileId in fileIds.iterkeys():
	(path, version) = newPkg.getFile(fileId)

	if path[0] == '/':
	    realPath = root + path
	else:
	    realPath = os.getcwd() + "/" + path

	# if we're committing against head, this better be a new file.
	# if we're generating a diff against someplace else, it might not 
	# be.
	assert(srcPkg or isinstance(version, versions.NewVersion))

	f = files.FileFromFilesystem(realPath, fileId)

	extension = path.split(".")[-1]
	if isSrcPkg and extension not in nonCfgExt:
	    f.flags.isConfig(set = True)

	# new file, so this part is easy
	changeSet.addFile(fileId, None, newVersion, f.freeze())
	newPkg.addFile(fileId, path, newVersion)

	if f.hasContents:
	    newCont = filecontents.FromFilesystem(realPath)
	    changeSet.addFileContents(fileId,
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

def buildLocalChanges(repos, pkgList, root = "", flags = 0):
    """
    Builds a change set against a set of files currently installed and
    builds a package object which describes the files installed.  The
    return is a changeset and a list of tuples, each with a boolean
    saying if anything changed for a package reflecting what's in the
    filesystem for that package.

    @param repos: Repository this directory is against.
    @type repos: repository.Repository
    @param pkgList: Specifies which pacakage to work on, and is a list
    of (curPkg, srcPkg, newVersion) tuples as defined in the parameter
    list for _localChanges()
    @param root: root directory the files are in (ignored for sources, which
    are assumed to be in the current directory)
    @type root: str
    @param flags: IGNOREUGIDS or zero
    @type flags: int
    """

    changeSet = changeset.ChangeSet()
    returnList = []
    for (curPkg, srcPkg, newVersion) in pkgList:
	result = _localChanges(repos, changeSet, curPkg, srcPkg, newVersion, 
			       root, flags)
        if result is None:
            # an error occurred
            return None
	returnList.append(result)

    return (changeSet, returnList)

def shlibAction(root, shlibList):
    p = "/sbin/ldconfig"

    if os.getuid():
	log.warning("ldconfig skipped (insufficient permissions)")

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
	try:
	    ldso = os.fdopen(ldsofd, 'w')
	    os.chmod(ldsotmpname, 0644)
	    ldso.writelines(ldsolines)
	    ldso.close()
	    os.rename(ldsotmpname, ldsopath)
	except:
	    os.unlink(ldsotmpname)
	    raise

    if os.getuid():
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

def runTagCommands(root, cmdList):
    uid = os.getuid()

    for cmd in cmdList:
	log.debug("running %s", " ".join(cmd))
	if root != '/' and uid:
	    continue

	pid = os.fork()
	if not pid:
	    os.environ['PATH'] = "/sbin:/bin:/usr/sbin:/usr/bin"
	    if root != '/':
		os.chdir(root)
		os.chroot(root)

	    try:
		os.execv(cmd[0], cmd)
	    except Exception, e:
		sys.stderr.write('%s\n' %e)
	    os._exit(1)

	(id, status) = os.waitpid(pid, 0)
	if not os.WIFEXITED(status) or os.WEXITSTATUS(status):
	    log.error("%s failed", cmd[0])
