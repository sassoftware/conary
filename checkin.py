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

from build import recipe, lookaside
from local import update
from repository import changeset
import changelog
import cook
import files
import helper
import log
import magic
import os
import repository
import sys
import time
import trove
import util
import versions

class SourceState(trove.Trove):

    def removeFilePath(self, file):
	for (fileId, path, version) in self.iterFileList():
	    if path == file: 
		self.removeFile(fileId)
		return True

	return False

    def write(self, filename):
	f = open(filename, "w")
	f.write("name %s\n" % self.name)
	if self.version:
	    f.write("version %s\n" % self.version.freeze())
	f.write(self.freezeFileList())

    def changeBranch(self, branch):
	self.branch = branch

    def getRecipeFileName(self):
        # XXX this is not the correct way to solve this problem
        # assumes a fully qualified trove name
        name = self.getName().split(':')[0]
        return os.path.join(os.getcwd(), name + '.recipe')

    def expandVersionStr(self, versionStr):
	if versionStr[0] == "@":
	    # get the name of the repository from the current branch
	    repName = self.getVersion().branch().label().getHost()
	    return repName + versionStr
	elif versionStr[0] != "/" and versionStr.find("@") == -1:
	    # non fully-qualified version; make it relative to the current
	    # branch
	    return self.getVersion().branch().asString() + "/" + versionStr

	return versionStr

    def __init__(self, name, version):
	trove.Trove.__init__(self, name, version, None, None)

class SourceStateFromFile(SourceState):

    def parseFile(self, filename):
	f = open(filename)
	rc = [self]
	for (what, isBranch) in [ ('name', 0), ('version', 1) ]:
	    line = f.readline()
	    fields = line.split()
	    assert(len(fields) == 2)
	    assert(fields[0] == what)
	    if isBranch:
		rc.append(versions.ThawVersion(fields[1]))
	    else:
		rc.append(fields[1])

	SourceState.__init__(*rc)

	self.readFileList(f)

    def __init__(self, file):
	if not os.path.isfile(file):
	    log.error("CONARY file must exist in the current directory for source commands")
	    raise OSError  # XXX

	self.parseFile(file)

def _verifyAtHead(repos, headPkg, state):
    headVersion = repos.getTroveLatestVersion(state.getName(), 
					 state.getVersion().branch())
    if not headVersion == state.getVersion():
	return False

    # make sure the files in this directory are based on the same
    # versions as those in the package at head
    for (fileId, path, version) in state.iterFileList():
	if isinstance(version, versions.NewVersion):
	    assert(not headPkg.hasFile(fileId))
	    # new file, it shouldn't be in the old package at all
	else:
	    srcFileVersion = headPkg.getFile(fileId)[1]
	    if not version == srcFileVersion:
		return False

    return True

def _getRecipeLoader(cfg, repos, recipeFile):
    # load the recipe; we need this to figure out what version we're building
    try:
        loader = recipe.RecipeLoader(recipeFile, cfg=cfg, repos=repos)
    except recipe.RecipeFileError, e:
	log.error("unable to load recipe file %s: %s", recipeFile, str(e))
        return None
    except IOError, e:
	log.error("unable to load recipe file %s: %s", recipeFile, e.strerror)
        return None
    
    if not loader:
	log.error("unable to load a valid recipe class from %s", recipeFile)
	return None

    return loader


def checkout(repos, cfg, workDir, name, versionStr = None):
    # We have to be careful with labels
    name += ":source"
    try:
        trvList = repos.findTrove(cfg.buildLabel, name, None,
				  versionStr = versionStr)
    except repository.repository.PackageNotFound, e:
        log.error(str(e))
        return
    if len(trvList) > 1:
	log.error("branch %s matches more then one version", versionStr)
	return
    trv = trvList[0]
	
    if not workDir:
	workDir = trv.getName().split(":")[0]

    if not os.path.isdir(workDir):
	try:
	    os.mkdir(workDir)
	except OSError, err:
	    log.error("cannot create directory %s/%s: %s", os.getcwd(),
                      workDir, str(err))
	    return

    branch = helper.fullBranchName(cfg.buildLabel, trv.getVersion(), 
				   versionStr)
    state = SourceState(trv.getName(), trv.getVersion())

    # it's a shame that findTrove already sent us the trove since we're
    # just going to request it again
    cs = repos.createChangeSet([(trv.getName(), None, None, trv.getVersion(),
			        True)])

    pkgCs = cs.iterNewPackageList().next()

    fileList = pkgCs.getNewFileList()
    fileList.sort()

    for (fileId, path, version) in fileList:
	fullPath = workDir + "/" + path
	fileObj = files.ThawFile(cs.getFileChange(fileId), fileId)
	if fileObj.hasContents:
	    contents = cs.getFileContents(fileId)[1]
	else:
	    contents = None

	fileObj.restore(contents, '/', fullPath, 1)

	state.addFile(fileId, path, version)

    state.write(workDir + "/CONARY")

def commit(repos, cfg, message):
    if cfg.name is None or cfg.contact is None:
	log.error("name and contact information must be set for commits")
	return

    try:
        state = SourceStateFromFile("CONARY")
    except OSError:
        return

    if isinstance(state.getVersion(), versions.NewVersion):
	# new package, so it shouldn't exist yet
	if repos.hasPackage(cfg.buildLabel.getHost(), state.getName()):
	    log.error("%s is marked as a new package but it " 
		      "already exists" % state.getName())
	    return
	srcPkg = None
    else:
	srcPkg = repos.getTrove(state.getName(), state.getVersion(), None)

	if not _verifyAtHead(repos, srcPkg, state):
	    log.error("contents of working directory are not all "
		      "from the head of the branch; use update")
	    return

    loader = _getRecipeLoader(cfg, repos, state.getRecipeFileName())
    if loader is None: return

    # fetch all the sources
    recipeClass = loader.getRecipe()
    if issubclass(recipeClass, recipe.PackageRecipe):
        lcache = lookaside.RepositoryCache(repos)
        srcdirs = [ os.path.dirname(recipeClass.filename),
                    cfg.sourcePath % {'pkgname': recipeClass.name} ]
        recipeObj = recipeClass(cfg, lcache, srcdirs)
        recipeObj.setup()
        files = recipeObj.fetchAllSources()
    
    recipeVersionStr = recipeClass.version

    if isinstance(state.getVersion(), versions.NewVersion):
	branch = versions.Version([cfg.buildLabel])
    else:
	branch = state.getVersion().branch()

    newVersion = helper.nextVersion(repos, state.getName(), recipeVersionStr, 
				    None, branch, binary = False)

    result = update.buildLocalChanges(repos, 
		    [(state, srcPkg, newVersion, update.IGNOREUGIDS)] )
    if not result: return

    (changeSet, ((isDifferent, newState),)) = result

    if not isDifferent:
	log.info("no changes have been made to commit")
	return

    if message and message[-1] != '\n':
	message += '\n'

    cl = changelog.ChangeLog(cfg.name, cfg.contact, message)
    if message is None and not cl.getMessage():
	log.error("no change log message was given")
	return

    pkgCs = changeSet.iterNewPackageList().next()
    pkgCs.changeChangeLog(cl)

    repos.commitChangeSet(changeSet)
    newState.write("CONARY")

def rdiff(repos, buildLabel, troveName, oldVersion, newVersion):
    if not troveName.endswith(":source"):
	troveName += ":source"

    new = repos.findTrove(buildLabel, troveName, None, versionStr = newVersion)
    if len(new) > 1:
	log.error("%s matches multiple versions" % newVersion)
	return
    new = new[0]
    newV = new.getVersion()

    try:
	count = -int(oldVersion)
	vers = repos.getTroveVersionsByLabel([troveName],
					     newV.branch().label())
	vers = vers[troveName]
	# erase everything later then us
	i = vers.index(newV)
	del vers[i:]

	branchList = []
	for v in vers:
	    if v.branch() == newV.branch():
		branchList.append(v)

	if len(branchList) < count:
	    oldV = None
	    old = None
	else:
	    oldV = branchList[-count]
	    old = repos.getTrove(troveName, oldV, None)
    except ValueError:
	old = repos.findTrove(buildLabel, troveName, None, 
			      versionStr = oldVersion)
	if len(old) > 1:
	    log.error("%s matches multiple versions" % oldVersion)
	    return
	old = old[0]
	oldV = old.getVersion()

    cs = repos.createChangeSet([(troveName, None, oldV, newV, False)])

    _showChangeSet(repos, cs, old, new)

def diff(repos, versionStr = None):
    try:
        state = SourceStateFromFile("CONARY")
    except OSError:
        return

    if state.getVersion() == versions.NewVersion():
	log.error("no versions have been committed")
	return

    if versionStr:
	versionStr = state.expandVersionStr(versionStr)

	pkgList = repos.findTrove(None, state.getName(), None, None, 
				  versionStr = versionStr)
	if len(pkgList) > 1:
	    log.error("%s specifies multiple versions" % versionStr)
	    return

	oldPackage = pkgList[0]
    else:
	oldPackage = repos.getTrove(state.getName(), state.getVersion(), None)

    result = update.buildLocalChanges(repos, 
	    [(state, oldPackage, versions.NewVersion(), update.IGNOREUGIDS)])
    if not result: return

    (changeSet, ((isDifferent, newState),)) = result
    if not isDifferent: return
    _showChangeSet(repos, changeSet, oldPackage, state)

def _showChangeSet(repos, changeSet, oldPackage, newPackage):
    packageChanges = changeSet.iterNewPackageList()
    pkgCs = packageChanges.next()
    assert(util.assertIteratorAtEnd(packageChanges))

    showOneLog(pkgCs.getNewVersion(), pkgCs.getChangeLog())

    for (fileId, path, newVersion) in pkgCs.getNewFileList():
	print "%s: new" % path
	chg = changeSet.getFileChange(fileId)
	f = files.ThawFile(chg, fileId)

	if f.hasContents and f.flags.isConfig():
	    (contType, contents) = changeSet.getFileContents(fileId)
	    print contents.get().read()

    for (fileId, path, newVersion) in pkgCs.getChangedFileList():
	if path:
	    dispStr = path
	    if oldPackage:
		oldPath = oldPackage.getFile(fileId)[0]
		dispStr += " (aka %s)" % oldPath
	else:
	    path = oldPackage.getFile(fileId)[0]
	    dispStr = path
	
	if not newVersion:
	    sys.stdout.write(dispStr + '\n')
	    continue
	    
	sys.stdout.write(dispStr + ": changed\n")
        
	sys.stdout.write("Index: %s\n%s\n" %(path, '=' * 68))

	csInfo = changeSet.getFileChange(fileId)
	print '\n'.join(files.fieldsChanged(csInfo))

	if files.contentsChanged(csInfo):
	    contType = changeSet.getFileContentsType(fileId)
	    if contType == changeset.ChangedFileTypes.diff:
                sys.stdout.write('--- %s %s\n+++ %s %s\n'
                                 %(path, newPackage.getVersion().asString(),
                                   path, newVersion.asString()))

	        contents = changeSet.getFileContents(fileId)[1]
		lines = contents.get().readlines()
		str = "".join(lines)
		print str
		print

    for fileId in pkgCs.getOldFileList():
	path = oldPackage.getFile(fileId)[0]
	print "%s: removed" % path
	
def updateSrc(repos, versionStr = None):
    try:
        state = SourceStateFromFile("CONARY")
    except OSError:
        return
    pkgName = state.getName()
    baseVersion = state.getVersion()
    
    if not versionStr:
	headVersion = repos.getTroveLatestVersion(pkgName, 
						  state.getVersion().branch())
	head = repos.getTrove(pkgName, headVersion, None)
	newBranch = None
	headVersion = head.getVersion()
	if headVersion == baseVersion:
	    log.info("working directory is already based on head of branch")
	    return
    else:
	versionStr = state.expandVersionStr(versionStr)

        try:
            pkgList = repos.findTrove(None, pkgName, None,
                                      versionStr = versionStr)
        except repository.repository.PackageNotFound:
	    log.error("Unable to find source component %s with version %s"
                      % (pkgName, versionStr))
	    return
            
	if len(pkgList) > 1:
	    log.error("%s specifies multiple versions" % versionStr)
	    return

	head = pkgList[0]
	headVersion = head.getVersion()
	newBranch = helper.fullBranchName(None, headVersion, versionStr)

    changeSet = repos.createChangeSet([(pkgName, None, baseVersion, 
					headVersion, 0)])

    packageChanges = changeSet.iterNewPackageList()
    pkgCs = packageChanges.next()
    assert(util.assertIteratorAtEnd(packageChanges))

    localVer = state.getVersion().fork(versions.LocalBranch(), sameVerRel = 1)
    fsJob = update.FilesystemJob(repos, changeSet, 
				 { (state.getName(), localVer) : state }, "",
				 flags = update.IGNOREUGIDS | update.MERGE)
    errList = fsJob.getErrorList()
    if errList:
	for err in errList: log.error(err)
    fsJob.apply()
    newPkgs = fsJob.iterNewPackageList()
    newState = newPkgs.next()
    assert(util.assertIteratorAtEnd(newPkgs))

    if newState.getVersion() == pkgCs.getNewVersion() and newBranch:
	newState.changeBranch(newBranch)

    newState.write("CONARY")

def addFiles(fileList):
    try:
        state = SourceStateFromFile("CONARY")
    except OSError:
        return

    for file in fileList:
	try:
	    os.lstat(file)
	except OSError:
	    log.error("file %s does not exist", file)
	    continue

	found = False
	for (fileId, path, version) in state.iterFileList():
	    if path == file:
		log.error("file %s is already part of this source component" % path)
		found = True

	if found: 
	    continue

	fileMagic = magic.magic(file)
	if fileMagic and fileMagic.name == "changeset":
	    log.error("do not add changesets to source components")
	    continue
	elif file == "CONARY":
	    log.error("refusing to add CONARY to the list of managed sources")
	    continue

	fileId = cook.makeFileId(os.getcwd(), file)

	state.addFile(fileId, file, versions.NewVersion())

    state.write("CONARY")

def removeFile(file):
    try:
        state = SourceStateFromFile("CONARY")
    except OSError:
        return

    if not state.removeFilePath(file):
	log.error("file %s is not under management" % file)

    if os.path.exists(file):
	os.unlink(file)

    state.write("CONARY")

def newPackage(repos, cfg, name):
    name += ":source"

    state = SourceState(name, versions.NewVersion())

    if repos and repos.hasPackage(cfg.buildLabel.getHost(), name):
	log.error("package %s already exists" % name)
	return

    dir = name.split(":")[0]
    if not os.path.isdir(dir):
	try:
	    os.mkdir(dir)
	except:
	    log.error("cannot create directory %s/%s", os.getcwd(), dir)
	    return

    state.write(dir + "/" + "CONARY")

def renameFile(oldName, newName):
    try:
        state = SourceStateFromFile("CONARY")
    except OSError:
        return

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

    for (fileId, path, version) in state.iterFileList():
	if path == oldName:
	    os.rename(oldName, newName)
	    state.addFile(fileId, newName, version)
	    state.write("CONARY")
	    return
    
    log.error("file %s is not under management" % oldName)

def showLog(repos, branch = None):
    try:
        state = SourceStateFromFile("CONARY")
    except OSError:
        return

    if not branch:
	branch = state.getVersion().branch()
    else:
	if branch[0] != '/':
	    log.error("branch name expected instead of %s" % branch)
	    return
	branch = versions.VersionFromString(branch)

    troveName = state.getName()

    verList = repos.getTroveVersionsByLabel([troveName], branch.label())
    verList = verList[troveName]
    verList.reverse()
    l = []
    for version in verList:
	if version.branch() != branch: return
	l.append((troveName, version, None))

    print "Name  :", troveName
    print "Branch:", branch.asString()
    print

    troves = repos.getTroves(l)

    for trove in troves:
	v = trove.getVersion()
	cl = trove.getChangeLog()
	showOneLog(v, cl)

def showOneLog(version, changeLog=''):
    when = time.strftime("%c", time.localtime(version.timeStamps()[-1]))

    if version == versions.NewVersion():
	versionStr = "(working version)"
    else:
	versionStr = version.trailingVersion().asString()

    if changeLog:
	print "%s %s (%s) %s" % \
	    (versionStr, changeLog.name, changeLog.contact, when)
	lines = changeLog.message.split("\n")
	for l in lines:
	    print "    %s" % l
    else:
	print "%s %s (no log message)\n" \
	      %(versionStr, when)
