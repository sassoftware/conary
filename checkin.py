#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import changeset
import cook
import helper
import log
import os
import package
import recipe
import sys
import update
import versioned
import versions

class SourceState(package.Package):

    def removeFilePath(self, file):
	for (fileId, (path, version)) in self.iterFileList():
	    if path == file: 
		self.removeFile(fileId)
		return True

	return False

    def write(self, filename):
	f = open(filename, "w")
	f.write("name %s\n" % self.name)
	if self.version:
	    f.write("version %s\n" % self.version.asString())
	f.write("branch %s\n" % self.branch.asString())
	f.write(self.formatString())

    def getBranch(self):
	return self.branch

    def changeBranch(self, branch):
	self.branch = branch

    def getRecipeFileName(self):
        # XXX this is not the correct way to solve this problem
        # assumes a fully qualified trove name
        fields = self.getName().split(':')
        name = fields[-2]
        return os.path.join(os.getcwd(), name + '.recipe')

    def expandVersionStr(self, versionStr):
	if versionStr[0] == "@":
	    # get the name of the repository from the current branch
	    repName = self.getBranch().branchNickname().getHost()
	    return repName + versionStr
	elif versionStr[0] != "/" and versionStr.find("@") == -1:
	    # non fully-qualified version; make it relative to the current
	    # branch
	    return self.getBranch().asString() + "/" + versionStr

    def __init__(self, name, version, branch):
	package.Package.__init__(self, name, version)
	self.branch = branch

class SourceStateFromFile(SourceState):

    def parseFile(self, filename):
	f = open(filename)
	rc = [self]
	for (what, isBranch) in [ ('name', 0), ('version', 1), ('branch', 1) ]:
	    line = f.readline()
	    fields = line.split()
	    assert(len(fields) == 2)
	    assert(fields[0] == what)
	    if isBranch:
		rc.append(versions.VersionFromString(fields[1]))
	    else:
		rc.append(fields[1])

	SourceState.__init__(*rc)

	self.read(f)

    def __init__(self, file):
	if not os.path.isfile(file):
	    log.error("SRS file must exist in the current directory for source commands")
	    raise OSError  # XXX

	self.parseFile(file)

def _verifyAtHead(repos, headPkg, state):
    headVersion = repos.pkgLatestVersion(state.getName(), 
					 state.getBranch())
    if not headVersion.equal(state.getVersion()):
	return False

    # make sure the files in this directory are based on the same
    # versions as those in the package at head
    for (fileId, (path, version)) in state.iterFileList():
	if isinstance(version, versions.NewVersion):
	    assert(not headPkg.hasFile(fileId))
	    # new file, it shouldn't be in the old package at all
	else:
	    srcFileVersion = headPkg.getFile(fileId)[1]
	    if not version.equal(srcFileVersion):
		return False

    return True

def _getRecipeVersion(recipeFile):
    # load the recipe; we need this to figure out what version we're building
    try:
        loader = recipe.RecipeLoader(recipeFile)
    except recipe.RecipeFileError, e:
	log.error("unable to load recipe file %s: %s", recipeFile, str(e))
        return None
    except IOError, e:
	log.error("unable to load recipe file %s: %s", recipeFile, e.strerror)
        return None
    
    if not loader:
	log.error("unable to load a valid recipe class from %s", recipeFile)
	return None

    recipeClass = loader.getRecipe()
    return recipeClass.version


def checkout(repos, cfg, dir, name, versionStr = None):
    # We have to be careful with branch nicknames.  First, we could get
    # multiple matches for a single package. Two, when a nickname uniquely
    # identifies a package we still need to make sure the state has the name of
    # the actual branch since empty branches yield objects whose versions are
    # on the parent branch.
    name += ":sources"
    try:
        trvList = helper.findPackage(repos, cfg.packagenamespace, 
                                     cfg.installbranch, name, 
                                     versionStr = versionStr)
    except helper.PackageNotFound, e:
        log.error(str(e))
        return
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

    branch = helper.fullBranchName(cfg.packagenamespace[1:], cfg.installbranch,
				   trv.getVersion(), versionStr)
    state = SourceState(trv.getName(), trv.getVersion(), branch)

    for (fileId, (path, version)) in trv.iterFileList():
	fullPath = dir + "/" + path
	(fileObj, contents) = repos.getFileVersion(fileId, version,
						   withContents = True)

	fileObj.restore(contents, fullPath, 1)

	state.addFile(fileId, path, version)

    state.write(dir + "/SRS")

def commit(repos):
    state = SourceStateFromFile("SRS")

    if isinstance(state.getVersion(), versions.NewVersion):
	# new package, so it shouldn't exist yet
	if repos.hasPackage(state.getName()):
	    log.error("%s is marked as a new package but it " 
		      "already exists" % state.getName())
	    return
	srcPkg = None
    else:
	srcPkg = repos.getPackageVersion(state.getName(), state.getVersion())
	# update the version to one w/ a timestamp
	srcPkg.changeVersion(repos.pkgGetFullVersion(state.getName(), 
			     srcPkg.getVersion()))

	if not _verifyAtHead(repos, srcPkg, state):
	    log.error("contents of working directory are not all "
		      "from the head of the branch; use update")
	    return

    recipeVersionStr = _getRecipeVersion(state.getRecipeFileName())
    if not recipeVersionStr: return

    if srcPkg:
	newVersion = helper.nextVersion(recipeVersionStr, srcPkg.getVersion(),
					state.getBranch(), binary = False)
    else:
	newVersion = helper.nextVersion(recipeVersionStr, None,
					state.getBranch(), binary = False)

    result = update.buildLocalChanges(repos, [(state, srcPkg, newVersion)])
    if not result: return

    (changeSet, ((isDifferent, newState),)) = result

    if not isDifferent:
	log.info("no changes have been made to commit")
    else:
	repos.commitChangeSet(changeSet)
	newState.write("SRS")

def diff(repos, versionStr = None):
    state = SourceStateFromFile("SRS")
    if state.getVersion().equal(versions.NewVersion()):
	log.error("no versions have been committed")
	return

    if versionStr:
	versionStr = state.expandVersionStr(versionStr)

	pkgList = helper.findPackage(repos, None, None, state.getName(), 
				     versionStr)
	if len(pkgList) > 1:
	    log.error("%s specifies multiple versions" % versionStr)
	    return

	oldPackage = pkgList[0]
    else:
	oldPackage = repos.getPackageVersion(state.getName(), 
					     state.getVersion())

    # update the version to one w/ a timestamp
    oldPackage.changeVersion(repos.pkgGetFullVersion(state.getName(), 
			     oldPackage.getVersion()))

    result = update.buildLocalChanges(repos, [(state, oldPackage, 
					       versions.NewVersion())])
    if not result: return

    (changeSet, ((isDifferent, newState),)) = result
    if not isDifferent: return

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

	fileType = csInfo.split()[0]
	sha1 = csInfo.split()[1]
	if fileType == "f" and sha1 != "-":
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
	
def updateSrc(repos, versionStr = None):
    state = SourceStateFromFile("SRS")
    pkgName = state.getName()
    baseVersion = state.getVersion()
    
    if not versionStr:
	head = repos.getLatestPackage(pkgName, state.getBranch())
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

    fsJob = update.FilesystemJob(repos, changeSet, 
				 { state.getName() : state }, "" )
    errList = fsJob.getErrorList()
    if errList:
	for err in errList: log.error(err)
    fsJob.apply()
    newPkgs = fsJob.getNewPackageList()
    assert(len(newPkgs) == 1)
    newState = newPkgs[0]

    if newState.getVersion().equal(pkgCs.getNewVersion()) and newBranch:
	newState.changeBranch(newBranch)

    newState.write("SRS")

def addFile(file):
    state = SourceStateFromFile("SRS")

    try:
	os.lstat(file)
    except OSError:
	log.error("files must be created before they can be added")
	return

    for (fileId, (path, version)) in state.iterFileList():
	if path == file:
	    log.error("file %s is already part of this source package" % path)
	    return

    fileId = cook.makeFileId(os.getcwd(), file)

    state.addFile(fileId, file, versions.NewVersion())
    state.write("SRS")

def removeFile(file):
    state = SourceStateFromFile("SRS")

    if not state.removeFilePath(file):
	log.error("file %s is not under management" % file)

    if os.path.exists(file):
	os.unlink(file)

    state.write("SRS")

def newPackage(repos, cfg, name):
    if name[0] != ":":
	name = cfg.packagenamespace + ":" + name
    name += ":sources"

    state = SourceState(name, versions.NewVersion(), cfg.defaultbranch)

    if repos and repos.hasPackage(name):
	log.error("package %s already exists" % name)
	return

    dir = name.split(":")[-2]
    if not os.path.isdir(dir):
	try:
	    os.mkdir(dir)
	except:
	    log.error("cannot create directory %s/%s", os.getcwd(), dir)
	    return

    state.write(dir + "/" + "SRS")

def renameFile(oldName, newName):
    state = SourceStateFromFile("SRS")

    if not os.path.exists(oldName):
	log.error("%s does not exist or is not a regular file" % oldName)
	return

    try:
	os.lstat(newName)
    except:
	pass
    else:
	log.error("%s already exists" % newName)
	return

    for (fileId, (path, version)) in state.iterFileList():
	if path == oldName:
	    os.rename(oldName, newName)
	    state.addFile(fileId, newName, version)
	    state.write("SRS")
	    return
    
    log.error("file %s is not under management" % oldName)
