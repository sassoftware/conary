#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import changeset
import cook
import errno
import filecontents
import files
import helper
import log
import os
import package
import patch
import recipe
import repository
import sys
import util
import versioned
import versions

class SourceState:
    """
    Representation of the SRS file used to keep track of files in source
    directories.
    """

    def expandVersionStr(self, versionStr):
	if versionStr[0] == "@":
	    # get the name of the repository from the current branch
	    repName = self.getTroveBranch().branchNickname().getHost()
	    return repName + versionStr
	elif versionStr[0] != "/" and versionStr.find("@") == -1:
	    # non fully-qualified version; make it relative to the current
	    # branch
	    return self.getTroveBranch().asString() + "/" + versionStr

    def addFile(self, fileId, path, version):
	self.files[fileId] = (path, version)

    def removeFile(self, fileId):
	del self.files[fileId]

    def removeFilePath(self, file):
	for (fileId, (path, version)) in self.getFileList():
	    if path == file: 
		del self.files[fileId]
		return True

	return False

    def setTroveName(self, name):
	self.troveName = name

    def setTroveVersion(self, version):
	self.troveVersion = version

    def setTroveBranch(self, branch):
	self.troveBranch = branch

    def getTroveName(self):
	return self.troveName

    def getTroveVersion(self):
	return self.troveVersion

    def getTroveBranch(self):
	return self.troveBranch

    def getFileList(self):
	return self.files.iteritems()

    def getFile(self, fileId):
	return self.files[fileId]

    def hasFile(self, fileId):
	return self.files.has_key(fileId)

    def getRecipeFileNames(self):
	list = []
	for (fileId, (path, version)) in self.files.iteritems():
	    if path.endswith(".recipe"): list.append(os.getcwd() + '/' + path)

	return list
	
    def parseFile(self, filename):
	f = open(filename)
	for line in f.readlines():
	    fields = line.split()
	    if fields[0] == "name":
		self.setTroveName(fields[1])
	    elif fields[0] == "version":
		if fields[1] != "@NEW@":
		    self.setTroveVersion(versions.VersionFromString(fields[1]))
	    elif fields[0] == "branch":
		self.setTroveBranch(versions.VersionFromString(fields[1]))
	    elif fields[0] == "file":
		if fields[3] == "@NEW@":
		    version = None
		else:
		    version = versions.VersionFromString(fields[3])

		self.addFile(fields[1], fields[2], version)

    def write(self, filename):
	f = open(filename, "w")
	f.write("name %s\n" % self.troveName)

	if self.troveVersion:
	    f.write("version %s\n" % self.troveVersion.asString())
	else:
	    f.write("version %s\n" % "@NEW@")

	f.write("branch %s\n" % self.troveBranch.asString())

	for (fileId, (path, version)) in self.files.iteritems():
	    if version:
		f.write("file %s %s %s\n" % (fileId, path, version.asString()))
	    else:
		f.write("file %s %s %s\n" % (fileId, path, "@NEW@"))

    def __init__(self, filename = None):
	self.files = {}
	self.troveVersion = None
	if filename: self.parseFile(filename)

def checkout(repos, cfg, dir, name, versionStr = None):
    # We have to be careful with branch nicknames.  First, we could get
    # multiple matches for a single package. Two, when a nickname uniquely
    # identifies a package we still need to make sure the state has the name of
    # the actual branch since empty branches yield objects whose versions are
    # on the parent branch.
    name += ":sources"
    trvList = helper.findPackage(repos, cfg.packagenamespace, 
				 cfg.installbranch, name, 
				 versionStr = versionStr)
    if len(trvList) > 1:
	log.error("branch %s matches more then one version", versionStr)
	return
    trv = trvList[0]
	
    if not dir:
	dir = trv.getName().split(":")[-2]

    if not os.path.isdir(dir):
	try:
	    os.mkdir(dir)
	except OSError, err:
	    log.error("cannot create directory %s/%s: %s", os.getcwd(), dir,
                      str(err))
	    return

    state = SourceState()
    state.setTroveName(trv.getName())
    version = trv.getVersion()
    state.setTroveVersion(version)

    branch = helper.fullBranchName(cfg.packagenamespace[1:], cfg.installbranch,
				   version, versionStr)
    state.setTroveBranch(branch)

    for (fileId, path, version) in trv.fileList():
	fullPath = dir + "/" + path
	fileObj = repos.getFileVersion(fileId, version)
	contents = filecontents.FromRepository(repos, fileObj.sha1())
	fileObj.restore(contents, fullPath, 1)

	state.addFile(fileId, path, version)

    state.write(dir + "/SRS")

def buildChangeSet(repos, state, srcVersion = None, needsHead = False):
    """
    Builds a change set against the sources in the current directory and
    builds an in-core state object as if these changes were committed. If
    no version is passed, the changeset is against the head of the
    working branch. The return is a tuple with a boolean saying if
    anything changes, the new state, the changeset, and the package which
    was diff'd against.

    @param repos: Repository this directory is against.
    @type repos: repository.Repository
    @param state: Current state object
    @type state: SourceState
    @param srcVersion: Version in the repository to generate the change ste
    against
    @type srcVersion: versions.Version
    @param needsHead: If true, this operation fails if it's done against
    something other then head
    @type needsHead: Boolean
    @rtype: (boolean, SourceState, changeset.ChangeSet, package.Package)
    """

    if not state.getTroveVersion():
	# new package, so it shouldn't exist yet
	if needsHead:
	    if repos.hasPackage(state.getTroveName()):
		log.error("%s is marked as a new package but it " 
			  "already exists" % state.getTroveName())
		return
	srcVersion = None
	srcPkg = None
    else:
	if not srcVersion:
	    srcVersion = state.getTroveVersion()

	srcPkg = repos.getPackageVersion(state.getTroveName(), srcVersion)
	srcPkg.changeVersion(repos.pkgGetFullVersion(state.getTroveName(), 
			     srcVersion))

	if needsHead:
	    # existing package
	    headVersion = repos.pkgLatestVersion(state.getTroveName(), 
						 state.getTroveBranch())
	    if not headVersion.equal(state.getTroveVersion()):
		log.error("working version (%s) is different from the head " +
			  "of the branch (%s); use update", 
			  state.getTroveVersion().asString(), 
			  srcVersion.asString())
		return

	    # make sure the files in this directory are based on the same
	    # versions as those in the package at head
	    bail = 0
	    for (fileId, (path, version)) in state.getFileList():
		if not version:
		    assert(not srcPkg.hasFile(fileId))
		    # new file, it shouldn't be in the old package at all
		else:
		    srcFileVersion = srcPkg.getFile(fileId)[1]
		    if not version.equal(srcFileVersion):
			log.error("%s is not at head; use update" % path)
			bail = 1
	
	    if bail: return

    # load the recipe; we need this to figure out what version we're building
    try:
	recipeFiles = state.getRecipeFileNames()
	classes = {}
	for filename in recipeFiles:
	    newClasses = recipe.RecipeLoader(filename)
	    classes.update(newClasses)
    except recipe.RecipeFileError, msg:
	raise cook.CookError(str(msg))

    if not classes:
	log.error("no recipe files were found")
	return

    recipeVersionStr = None
    for className in classes.iterkeys():
	if not recipeVersionStr:
	    recipeVersionStr = classes[className].version
	elif recipeVersionStr != classes[className].version:
	    log.error("all recipes must have the same version")
	    return

    troveBranch = state.getTroveBranch()

    if not srcVersion:
	# new package
	newVersion = troveBranch.copy()
	newVersion.appendVersionRelease(recipeVersionStr, 1)
    elif srcVersion.trailingVersion().getVersion() == recipeVersionStr and \
         troveBranch.equal(srcVersion.branch()):
	newVersion = srcVersion.copy()
	newVersion.incrementRelease()
    else:
	newVersion = troveBranch.copy()
	newVersion.appendVersionRelease(recipeVersionStr, 1)

    state.setTroveVersion(newVersion)

    pkg = package.Package(state.getTroveName(), newVersion)
    changeSet = changeset.ChangeSet()

    foundDifference = 0

    for (fileId, (path, version)) in state.getFileList():
	realPath = os.getcwd() + "/" + path

	f = files.FileFromFilesystem(realPath, fileId, type = "src")

	if path.endswith(".recipe"):
	    f.isConfig(set = True)

	if not srcPkg or not srcPkg.hasFile(fileId):
	    # if we're committing against head, this better be a new file.
	    # if we're generating a diff against someplace else, it might not 
	    # be.
	    assert(needsHead and not version)
	    # new file, so this is easy
	    changeSet.addFile(fileId, None, newVersion, f.infoLine())
	    state.addFile(fileId, path, newVersion)
	    newCont = filecontents.FromFilesystem(realPath)
	    changeSet.addFileContents(fileId,
				      changeset.ChangedFileTypes.file,
				      newCont)
	    pkg.addFile(fileId, path, newVersion)
	    foundDifference = 1
	    continue

	oldVersion = srcPkg.getFile(fileId)[1]	
	(oldFile, oldCont) = repos.getFileVersion(fileId, oldVersion,
						  withContents = 1)
        if not f.same(oldFile):
	    foundDifference = 1
	    pkg.addFile(fileId, path, newVersion)
	    state.addFile(fileId, path, newVersion)

	    (filecs, hash) = changeset.fileChangeSet(fileId, oldFile, f)
	    changeSet.addFile(fileId, oldVersion, newVersion, filecs)
	    if hash:
		newCont = filecontents.FromFilesystem(realPath)
		(contType, cont) = changeset.fileContentsDiff(oldFile, oldCont,
					f, newCont)
						
		changeSet.addFileContents(fileId, contType, cont)
				   
	else:
	    pkg.addFile(f.id(), path, oldVersion)

    (csPkg, filesNeeded, pkgsNeeded) = pkg.diff(srcPkg)
    assert(not pkgsNeeded)
    changeSet.newPackage(csPkg)

    if csPkg.getOldFileList() or csPkg.getChangedFileList():
	foundDifference = 1

    return (foundDifference, state, changeSet, srcPkg)

def commit(repos):
    if not os.path.isfile("SRS"):
	log.error("SRS file must exist in the current directory for source commands")
	return

    state = SourceState("SRS")

    # we need to commit based on changes to the head of a branch
    result = buildChangeSet(repos, state, needsHead = True)
    if not result: return

    (isDifferent, state, changeSet, oldPackage) = result

    if not isDifferent:
	log.info("no changes have been made to commit")
    else:
	repos.commitChangeSet(changeSet)
	state.write("SRS")

def diff(repos, versionStr = None):
    if not os.path.isfile("SRS"):
	log.error("SRS file must exist in the current directory for source commands")
	return

    state = SourceState("SRS")

    if versionStr:
	versionStr = state.expandVersionStr(versionStr)

	pkgList = helper.findPackage(repos, None, None, state.getTroveName(), 
				     versionStr)
	if len(pkgList) > 1:
	    log.error("%s specifies multiple versions" % versionStr)
	    return

	srcVersion = pkgList[0].getVersion()
    else:
	srcVersion = None

    result = buildChangeSet(repos, state, srcVersion = srcVersion)
    if not result: return

    (changed, state, changeSet, oldPackage) = result
    if not changed: return

    packageChanges = changeSet.getNewPackageList()
    assert(len(packageChanges) == 1)
    pkgCs = packageChanges[0]

    for (fileId, path, newVersion) in pkgCs.getNewFileList():
	print "%s: new" % path

    for (fileId, path, newVersion) in pkgCs.getChangedFileList():
	if not path:
	    path = oldPackage.getFile(fileId)[0]
	    sys.stdout.write("%s" % path)
	else:
	    oldPath = oldPackage.getFile(fileId)[0]
	    sys.stdout.write("%s (aka %s)" % (path, oldPath))

	if not newVersion: 
	    print
	    continue
	
	sys.stdout.write(": changed\n")

	csInfo = changeSet.getFileChange(fileId)
	print "    %s" % csInfo

	sha1 = csInfo.split()[1]
	if sha1 != "-":
	    (contType, contents) = changeSet.getFileContents(fileId)
	    if contType == changeset.ChangedFileTypes.diff:
		lines = contents.get().readlines()
		str = "    " + "    ".join(lines)
		print
		print str
		print

    for fileId in pkgCs.getOldFileList():
	path = oldPackage.getFile(fileId)[0]
	print "%s: removed" % path
	
def update(repos, versionStr = None):
    if not os.path.isfile("SRS"):
	log.error("SRS file must exist in the current directory for source commands")
	return

    state = SourceState("SRS")
    pkgName = state.getTroveName()
    baseVersion = state.getTroveVersion()
    
    if not versionStr:
	head = repos.getLatestPackage(pkgName, state.getTroveBranch())
	newBranch = None
	headVersion = head.getVersion()
	if headVersion.equal(baseVersion):
	    log.info("working directory is already based on head of branch")
	    return
    else:
	versionStr = state.expandVersionStr(versionStr)

	pkgList = helper.findPackage(repos, None, None, pkgName, versionStr)
	if len(pkgList) > 1:
	    log.error("%s specifies multiple versions" % versionStr)
	    return

	head = pkgList[0]
	headVersion = head.getVersion()
	newBranch = helper.fullBranchName(None, None, headVersion, versionStr)

    changeSet = repos.createChangeSet([(pkgName, baseVersion, headVersion, 0)])

    packageChanges = changeSet.getNewPackageList()
    assert(len(packageChanges) == 1)
    pkgCs = packageChanges[0]
    basePkg = repos.getPackageVersion(state.getTroveName(), 
				      state.getTroveVersion())

    fullyUpdated = 1

    for (fileId, headPath, headVersion) in pkgCs.getNewFileList():
	# this gets broken links right
	try:
	    os.lstat(headPath)
	    log.error("%s is in the way of a newly created file" % headPath)
	    fullyUpdated = 0
	    continue
	except:
	    pass

	log.info("creating %s" % headPath)
	(headFile, headFileContents) = \
		repos.getFileVersion(fileId, headVersion, withContents = 1)
	src = repos.pullFileContentsObject(headFile.sha1())
	dest = open(headPath, "w")
	util.copyfileobj(src, dest)
	state.addFile(fileId, headPath, headVersion)
	del src
	del dest

    for fileId in pkgCs.getOldFileList():
	(path, version) = basePkg.getFile(fileId)
	if not state.hasFile(fileId):
	    log.info("%s has already been removed" % path)
	    continue

	# don't remove files if they've been changed locally
	try:
	    localFile = files.FileFromFilesystem(path, fileId, type = "src")
	except OSError, exc:
	    # it's okay if the file is missing, it just means we all agree
	    if exc.errno == errno.ENOENT:
		state.removeFile(fileId)
		continue
	    else:
		raise

	oldFile = repos.getFileVersion(fileId, version)


	if not oldFile.same(localFile):
	    log.error("%s has changed but has been removed on head" % path)
	    continue

	log.info("removing %s" % path)	

	os.unlink(path)
	state.removeFile(fileId)

    for (fileId, headPath, headFileVersion) in pkgCs.getChangedFileList():
	(fsPath, fsVersion) = state.getFile(fileId)
	pathOkay = 1
	contentsOkay = 1
	realPath = fsPath
	# if headPath is none, the name hasn't changed in the repository
	if headPath and headPath != fsPath:
	    # the paths are different; if one of them matches the one
	    # from the old package, take the other one as it is the one
	    # which changed
	    if basePkg.hasFile(fileId):
		basePath = basePkg.getFile(fileId)[0]
	    else:
		basePath = None

	    if fsPath == basePath:
		# the path changed in the repository, propage that change
		log.info("renaming %s to %s" % (fsPath, headPath))
		os.rename(fsPath, headPath)
		state.addFile(fileId, headPath, fsVersion)
		realPath = headPath
	    else:
		pathOkay = 0
		realPath = fsPath	# let updates work still
		log.error("path conflict for %s (%s on head)" % 
			  (fsPath, headPath))
	
	# headFileVersion is None for renames
	if headFileVersion:
	    fsFile = files.FileFromFilesystem(realPath, fileId, type = "src")
	    (headFile, headFileContents) = \
		    repos.getFileVersion(fileId, headFileVersion, 
					 withContents = 1)

	if headFileVersion and fsFile.sha1() != headFile.sha1():
	    # the contents have changed... let's see what to do
	    if basePkg.hasFile(fileId):
		baseFileVersion = basePkg.getFile(fileId)[1]
		(baseFile, baseFileContents) = repos.getFileVersion(fileId, 
				    baseFileVersion, withContents = 1)
	    else:
		baseFile = None

	    if not baseFile:
		log.error("new file %s conflicts with file on head of branch"
				% realPath)
		contentsOkay = 0
	    elif headFile.sha1() == baseFile.sha1():
		# it changed in just the filesystem, so leave that change
		log.info("preserving new contents of %s" % realPath)
	    elif fsFile.sha1() == baseFile.sha1():
		# the contents changed in just the repository, so take
		# those changes
		log.info("replacing %s with contents from head" % realPath)
		src = repos.pullFileContentsObject(headFile.sha1())
		dest = open(realPath, "w")
		util.copyfileobj(src, dest)
		del src
		del dest
	    elif fsFile.isConfig() or headFile.isConfig():
		# it changed in both the filesystem and the repository; our
		# only hope is to generate a patch for what changed in the
		# repository and try and apply it here
		(contType, cont) = changeset.fileContentsDiff(
			baseFile, baseFileContents,
			headFile, headFileContents)
		if contType != changeset.ChangedFileTypes.diff:
		    log.error("contents conflict for %s" % realPath)
		    contentsOkay = 0
		else:
		    log.info("merging changes from head into %s" % realPath)
		    diff = cont.get().readlines()
		    cur = open(realPath, "r").readlines()
		    (newLines, failedHunks) = patch.patch(cur, diff)

		    f = open(realPath, "w")
		    f.write("".join(newLines))

		    if failedHunks:
			log.warning("conflicts from merging changes from " +
			    "head into %s saved as %s.conflicts" % 
			    (realPath, realPath))
			failedHunks.write(realPath + ".conflicts", 
					  "current", "head")

		    contentsOkay = 1
	    else:
		log.error("contents conflict for %s" % realPath)
		contentsOkay = 0

	if pathOkay and contentsOkay:
	    # XXX this doesn't even attempt to merge file permissions
	    # and such; the good part of that is differing owners don't
	    # break things
	    if not headFileVersion:
		headFileVersion = state.getFile(fileId)[1]
	    state.addFile(fileId, realPath, headFileVersion)
	else:
	    fullyUpdated = 0

    if fullyUpdated:
	state.setTroveVersion(headVersion)
	if newBranch: state.setTroveBranch(newBranch)

    state.write("SRS")

def addFile(file):
    if not os.path.isfile("SRS"):
	log.error("SRS file must exist in the current directory for source commands")
	return

    state = SourceState("SRS")

    if not os.path.exists(file):
	log.error("files must be created before they can be added")
	return
    elif not os.path.isfile(file):
	log.error("only normal files can be part of source packages")

    for (fileId, (path, version)) in state.getFileList():
	if path == file:
	    log.error("file %s is already part of this source package" % path)
	    return

    fileId = cook.makeFileId(os.getcwd(), file)

    state.addFile(fileId, file, None)
    state.write("SRS")

def removeFile(file):
    if not os.path.isfile("SRS"):
	log.error("SRS file must exist in the current directory for source commands")
	return

    state = SourceState("SRS")

    if os.path.exists(file):
	log.error("files must be removed from the filesystem first")
	return

    if not state.removeFilePath(file):
	log.error("file %s is not under management" % file)

    state.write("SRS")

def newPackage(repos, cfg, name):
    state = SourceState()
    if name[0] != ":":
	name = cfg.packagenamespace + ":" + name
    name += ":sources"

    if repos and repos.hasPackage(name):
	log.error("package %s already exists" % name)
	return

    state.setTroveName(name)
    state.setTroveBranch(cfg.defaultbranch)

    dir = name.split(":")[-2]
    if not os.path.isdir(dir):
	try:
	    os.mkdir(dir)
	except:
	    log.error("cannot create directory %s/%s", os.getcwd(), dir)
	    return

    state.write(dir + "/" + "SRS")

def renameFile(oldName, newName):
    if not os.path.isfile("SRS"):
	log.error("SRS file must exist in the current directory for source commands")
	return

    state = SourceState("SRS")

    if not os.path.isfile(oldName):
	log.error("%s does not exist or is not a regular file" % oldName)
	return

    try:
	os.lstat(newName)
    except:
	pass
    else:
	log.error("%s already exists" % newName)
	return

    for (fileId, (path, version)) in state.getFileList():
	if path == oldName:
	    log.info("renaming %s to %s", oldName, newName)
	    os.rename(oldName, newName)
	    state.addFile(fileId, newName, version)
	    state.write("SRS")
	    return
    
    log.error("file %s is not under management" % oldName)
