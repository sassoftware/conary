#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import changeset
import cook
import filecontents
import files
import helper
import log
import os
import package
import recipe
import repository
import util
import versioned
import versions

class SourceState:
    """
    Representation of the SRS file used to keep track of files in source
    directories.
    """

    def addFile(self, fileId, path, version):
	self.files[path] = (fileId, version)

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

    def getRecipeFileNames(self):
	list = []
	for path in self.files.iterkeys():
	    if path.endswith(".recipe"): list.append(os.getcwd() + '/' + path)

	return list
	
    def parseFile(self, filename):
	f = open(filename)
	for line in f.readlines():
	    fields = line.split()
	    if fields[0] == "name":
		self.setTroveName(fields[1])
	    elif fields[0] == "version":
		self.setTroveVersion(versions.VersionFromString(fields[1]))
	    elif fields[0] == "branch":
		self.setTroveBranch(versions.VersionFromString(fields[1]))
	    elif fields[0] == "file":
		self.addFile(fields[1], fields[2], 
			     versions.VersionFromString(fields[3]))

    def write(self, filename):
	f = open(filename, "w")
	f.write("name %s\n" % self.troveName)
	f.write("version %s\n" % self.troveVersion.asString())
	f.write("branch %s\n" % self.troveBranch.asString())

	for (path, (fileId, version)) in self.files.iteritems():
	    f.write("file %s %s %s\n" % (fileId, path, version.asString()))

    def __init__(self, filename = None):
	self.files = {}
	if filename: self.parseFile(filename)

def checkin(repos, cfg, file):
    f = open(file, "r")

    try:
	grp = package.GroupFromTextFile(f, cfg.packagenamespace, repos)
    except package.ParseError:
	return

    simpleVer = grp.getSimpleVersion()

    ver = repos.pkgLatestVersion(grp.getName(), cfg.defaultbranch)
    if not ver:
	ver = cfg.defaultbranch.copy()
	ver.appendVersionRelease(simpleVer, 1)
    elif ver.trailingVersion().getVersion() == simpleVer:
	ver.incrementVersionRelease()
    else:
	ver = ver.branch()
	ver.appendVersionRelease(simpleVer, 1)

    grp.changeVersion(ver)
    changeSet = changeset.CreateFromFilesystem( [ (grp, {}) ] )
    repos.commitChangeSet(changeSet)

def checkout(repos, cfg, dir, name, versionStr = None):
    # This doesn't use helper.findPackage as it doesn't want to allow
    # branches nicknames. Doing so would cause two problems. First, we could
    # get multiple matches for a single pacakge. Two, even if we got
    # a single match we wouldn't know where to check in changes. A nickname
    # branch doesn't work for checkins as it could refer to multiple
    # branches, even if it doesn't right now.
    if name[0] != ":":
	name = cfg.packagenamespace + ":" + name
    name = name + ":sources"

    if not versionStr:
	version = cfg.defaultbranch
    else:
	if versionStr != "/":
	    versionStr = cfg.defaultbranch.asString() + "/" + versionStr

	try:
	    version = versions.VersionFromString(versionStr)
	except versions.ParseError, e:
	    log.error(str(e))
	    return

    try:
	if version.isBranch():
	    trv = repos.getLatestPackage(name, version)
	else:
	    trv = repos.getPackageVersion(name, version)
    except versioned.MissingBranchError, e:
	log.error(str(e))
	return
    except repository.PackageMissing, e:
	log.error(str(e))
	return
	
    if not dir:
	dir = trv.getName().split(":")[-2]

    if not os.path.isdir(dir):
	try:
	    os.mkdir(dir)
	except:
	    log.error("cannot create directory %s/%s", os.getcwd(), dir)
	    return

    state = SourceState()
    state.setTroveName(trv.getName())
    state.setTroveVersion(trv.getVersion())

    if version.isBranch():
	state.setTroveBranch(version)
    else:
	state.setTroveBranch(version.branch())

    for (fileId, path, version) in trv.fileList():
	fullPath = dir + "/" + path
	fileObj = repos.getFileVersion(fileId, version)
	src = repos.pullFileContentsObject(fileObj.sha1())
	dest = open(fullPath, "w")
	contents = filecontents.FromRepository(repos, fileObj.sha1())
	fileObj.restore(contents, fullPath, 1)

	state.addFile(fileId, path, version)

    state.write(dir + "/SRS")

def buildChangeSet(repos, srcVersion = None, needsHead = False):
    """
    Builds a change set against the sources in the current directory and
    builds an in-core state object as if these changes were committed. If
    no version is passed, the changeset is against the head of the
    working branch. The return is a tuple with a boolean saying if
    anything changes, the new state, the changeset, and the package which
    was diff'd against.

    @param repos: Repository this directory is against.
    @type repos: repository.Repository
    @param srcVersion: Version in the repository to generate the change ste
    against
    @type srcVersion: versions.Version
    @param needsHead: If true, this operation fails if it's done against
    something other then head
    @type needsHead: Boolean
    @rtype: (boolean, SourceState, changeset.ChangeSet, package.Package)
    """

    if not os.path.isfile("SRS"):
	log.error("SRS file must exist in the current directory for source commands")
	return

    state = SourceState("SRS")

    if not srcVersion:
	srcVersion = repos.pkgLatestVersion(state.getTroveName(), 
					    state.getTroveBranch())

    srcPkg = repos.getPackageVersion(state.getTroveName(), srcVersion)

    if needsHead and not srcVersion.equal(state.getTroveVersion()):
	log.error("working version (%s) is different from the head of the " +
		  "branch (%s); use update", 
		  state.getTroveVersion().asString(), 
		  oldPackage.getVersion().asString())
	return


    # load the recipe; we need this to figure out what version we're building
    try:
	recipeFiles = state.getRecipeFileNames()
	classes = {}
	for filename in recipeFiles:
	    newClasses = recipe.RecipeLoader(filename)
	    classes.update(newClasses)
    except recipe.RecipeFileError, msg:
	raise CookError(str(msg))

    recipeVersionStr = None
    for className in classes.iterkeys():
	if not recipeVersionStr:
	    recipeVersionStr = classes[className].version
	elif recipeVersionStr != classes[className].version:
	    log.error("all recipes must have the same version")
	    return

    if srcVersion.trailingVersion().getVersion() == recipeVersionStr:
	newVersion = srcVersion.copy()
	newVersion.incrementVersionRelease()
    else:
	newVersion = state.getTroveBranch().copy()
	newVersion.appendVersionRelease(recipeVersionStr, 1)

    state.setTroveVersion(newVersion)

    pkg = package.Package(state.getTroveName(), newVersion)
    fileMap = {}
    changeSet = changeset.ChangeSet()

    foundDifference = 0

    for (path, (fileId, version)) in state.getFileList():
	realPath = os.getcwd() + "/" + path

	f = files.FileFromFilesystem(realPath, fileId, type = "src")

	if path.endswith(".recipe"):
	    f.isConfig(set = True)

	duplicateVersion = cook.checkBranchForDuplicate(repos, 
						    state.getTroveBranch(), f)
        if not duplicateVersion:
	    foundDifference = 1
	    pkg.addFile(fileId, path, newVersion)
	    state.addFile(fileId, path, newVersion)

	    oldVersion = srcPkg.getFile(fileId)[1]
	    (oldFile, oldCont) = repos.getFileVersion(fileId, oldVersion,
						      withContents = 1)
	    (filecs, hash) = changeset.fileChangeSet(fileId, oldFile, f)
	    changeSet.addFile(fileId, oldVersion, newVersion, filecs)
	    if hash:
		newCont = filecontents.FromFilesystem(realPath)
		(contType, cont) = changeset.fileContentsDiff(oldFile, oldCont,
					f, newCont)
						
		changeSet.addFileContents(hash, contType, cont)
				   
	else:
	    pkg.addFile(f.id(), path, duplicateVersion)

        fileMap[f.id()] = (f, realPath, path)

    (csPkg, filesNeeded, pkgsNeeded) = pkg.diff(srcPkg)
    assert(not pkgsNeeded)
    changeSet.newPackage(csPkg)

    return (foundDifference, state, changeSet, srcPkg)

def commit(repos, cfg):
    # we need to commit based on changes to the head of a branch

    result = buildChangeSet(repos, needsHead = True)
    if not result: return

    (isDifferent, state, changeSet, oldPackage) = result

    if not isDifferent:
	log.info("no changes have been made to commit")
    else:
	repos.commitChangeSet(changeSet)
	state.write("SRS")

def diff(repos, cfg):

    result = buildChangeSet(repos, needsHead = True)
    if not result: return

    (changed, state, changeSet, oldPackage) = result
    if not changed: return

    packageChanges = changeSet.getNewPackageList()
    assert(len(packageChanges) == 1)
    pkgCs = packageChanges[0]

    for (fileId, path, newVersion) in pkgCs.getChangedFileList():
	if not path:
	    path = oldPackage.getFile(fileId)[0]
	print "%s:" % path

	csInfo = changeSet.getFileChange(fileId)
	print "    %s" % csInfo

	sha1 = csInfo.split()[1]
	if sha1 != "-":
	    (contType, contents) = changeSet.getFileContents(sha1)
	    if contType == changeset.ChangedFileTypes.diff:
		lines = contents.get().readlines()
		str = "    " + "    ".join(lines)
		print
		print str
		print
	
