#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Contains the functions which builds a recipe and commits the
resulting packages to the repository.
"""

import buildpackage
import changeset
import files
import log
import lookaside
import os
import package
import recipe
import repository
import sha1helper
import signal
import sys
import tempfile
import time
import types
import util
import tempfile

# -------------------- private below this line -------------------------

# type could be "src"
#
# returns a (pkg, fileMap) tuple
def _createPackage(repos, branch, bldPkg, ident):
    fileMap = {}
    p = package.Package(bldPkg.getName(), bldPkg.getVersion())

    for (path, buildFile) in bldPkg.items():
        realPath = buildFile.getRealPath()
        if isinstance(buildFile, buildpackage.BuildDeviceFile):
            f = files.FileFromInfoLine(buildFile.infoLine(), ident(path))
        elif realPath:
            f = files.FileFromFilesystem(realPath, ident(path), 
                                         type = buildFile.getType(),
                                         requireSymbolicOwnership=True)
	    # setuid or setgid must be set explicitly in buildFile
	    f.thePerms &= 01777
        else:
            raise CookError("unable to create file object for package")

        # set ownership, flags, etc
        f.merge(buildFile)
        
	duplicateVersion = checkBranchForDuplicate(repos, branch, f)
        if not duplicateVersion:
	    p.addFile(f.id(), path, bldPkg.getVersion())
	else:
	    p.addFile(f.id(), path, duplicateVersion)

        fileMap[f.id()] = (f, realPath, path)

    return (p, fileMap)

class _IdGen:
    def __call__(self, path):
	if self.map.has_key(path):
	    return self.map[path]

	hash = sha1helper.hashString("%s %f %s" % (path, time.time(), 
							self.noise))
	self.map[path] = hash
	return hash

    def __init__(self, map=None):
	# file ids need to be unique. we include the time and path when
	# we generate them; any data put here is also used
	uname = os.uname()
	self.noise = "%s %s" % (uname[1], uname[2])
        if map is None:
            self.map = {}
        else:
            self.map = map

    def populate(self, repos, lcache, pkg):
	# Find the files and ids which were owned by the last version of
	# this package on the branch. We also construct an object which
	# lets us look for source files this build needs inside of the
	# repository
	for (fileId, path, version) in pkg.fileList():
	    self.map[path] = fileId
	    if path[0] != "/":
		# we might need to retrieve this source file
		# to enable a build, so we need to find the
		# sha1 hash of it since that's how it's indexed
		# in the file store
		f = repos.getFileVersion(fileId, version)
		lcache.addFileHash(path, f.sha1())

def cookObject(repos, cfg, recipeClass, buildBranch, changeSetFile = None, 
	       prep=True, macros=()):
    """
    Turns a recipe object into a change set, and sometimes commits the
    result.

    @param repos: Repository to both look for source files and file id's in.
    @type repos: repository.Repository
    @param cfg: srs configuration
    @type cfg: srscfg.SrsConfiguration
    @param recipeClass: class which will be instantiated into a recipe
    @type recipeClass: class descended from recipe.Recipe
    @param buildBranch: the branch the new build will be committed to
    @type buildBranch: versions.Version
    @param changeSetFile: if set, the changeset is stored in this file
    instead of committed to a repository
    @type changeSetFile: str
    @param prep: If true, the build stops after the package is unpacked
    and None is returned instead of a changeset.
    @type prep: boolean
    @param macros: set of macros for the build
    @type macros: sequence
    @rtype: list of strings
    """

    if issubclass(recipeClass, recipe.PackageRecipe):
	return cookPackageObject(repos, cfg, recipeClass, buildBranch,
				 changeSetFile = changeSetFile,
				 prep = prep, macros = macros)
    
    assert(0)

def cookGroupObject(repos, cfg, recipeClass, buildBranch, 
		      changeSetFile = None, prep=True, macros=()):
    """
    Just like cookObject(), but only works for objects descended
    from recipe.GroupRecipe.

    The parameters and return type are identical to those for
    cookObject()
    """

    built = []
    log.info("Building %s", recipeClass.name)

    fullName = cfg.packagenamespace + ":" + recipeClass.name

    recipeObj = recipeClass(repos, cfg)

def cookPackageObject(repos, cfg, recipeClass, buildBranch, 
		      changeSetFile = None, prep=True, macros=()):
    """
    Just like cookObject(), but only works for objects descended
    from recipe.PackageRecipe.

    The parameters and return type are identical to those for
    cookObject()
    """

    repos.open("r")

    built = []

    log.info("Building %s", recipeClass.name)
    fullName = cfg.packagenamespace + ":" + recipeClass.name
    srcName = fullName + ":sources"

    lcache = lookaside.RepositoryCache(repos)

    srcdirs = [ os.path.dirname(recipeClass.filename),
		cfg.sourcepath % {'pkgname': recipeClass.name} ]
    recipeObj = recipeClass(cfg, lcache, srcdirs, macros)

    newVersion = None
    if repos.hasPackage(fullName):
	# if this package/version exists already, increment the
	# existing revision
	newVersion = repos.pkgLatestVersion(fullName, buildBranch)
	if newVersion and (
	  recipeObj.version == newVersion.trailingVersion().getVersion()):
	    newVersion = newVersion.copy()
	    newVersion.incrementBuildCount()
	else:
	    newVersion = None

    # this package/version doesn't exist yet
    if not newVersion:
	newVersion = buildBranch.copy()
	newVersion.appendVersionRelease(recipeObj.version, 1)

    # build up the name->fileid mapping so we reuse fileids wherever
    # possible; we do this by looking in the database for a pacakge
    # with the same name as the recipe and recursing through it's
    # subpackages; this mechanism continues to work as subpackages
    # come and go. this has to happen early as we build up the entries
    # for the source lookaside cache simultaneously

    ident = _IdGen()
    if repos.hasPackage(fullName):
	pkgList = [ (fullName, 
		    [repos.pkgLatestVersion(fullName, buildBranch)]) ]
	while pkgList:
	    (name, versionList) = pkgList[0]
	    del pkgList[0]
	    for version in versionList:
		pkg = repos.getPackageVersion(name, version)
		pkgList += pkg.getPackageList()

    if repos.hasPackage(srcName):
	pkg = repos.getLatestPackage(srcName, buildBranch)
	ident.populate(repos, lcache, pkg)

    builddir = cfg.buildpath + "/" + recipeObj.name

    recipeObj.setup()
    recipeObj.unpackSources(builddir)

    # if we're only extracting, continue to the next recipe class.
    if prep:
	return
    
    cwd = os.getcwd()
    util.mkdirChain(builddir + '/' + recipeObj.mainDir())
    os.chdir(builddir + '/' + recipeObj.mainDir())
    repos.close()

    util.mkdirChain(cfg.tmpdir)
    destdir = tempfile.mkdtemp("", "srs-%s-" % recipeObj.name, cfg.tmpdir)
    recipeObj.doBuild(builddir, destdir)
    log.info('Processing %s', recipeClass.name)
    recipeObj.doDestdirProcess() # includes policy

    repos.open("w")
    
    os.chdir(cwd)
    
    # build up the name->fileid mapping so we reuse fileids wherever
    # build up the name->fileid mapping so we reuse fileids wherever
    # possible; we do this by looking in the database for a pacakge
    # with the same name as the recipe and recursing through it's
    # subpackages; this mechanism continues to work as subpackages
    # come and go

    ident = _IdGen()
    if repos.hasPackage(fullName):
	pkgList = [ (fullName, 
		    [repos.pkgLatestVersion(fullName, buildBranch)]) ]
	while pkgList:
	    (name, versionList) = pkgList[0]
	    del pkgList[0]
	    for version in versionList:
		pkg = repos.getPackageVersion(name, version)
		pkgList += pkg.getPackageList()

    srcName = fullName + ":sources"
    if repos.hasPackage(srcName):
	pkg = repos.getLatestPackage(srcName, buildBranch)
	ident.populate(repos, lcache, pkg)

    # possible; we do this by looking in the database for a pacakge
    # with the same name as the recipe and recursing through it's
    # subpackages; this mechanism continues to work as subpackages
    # come and go

    ident = _IdGen()
    if repos.hasPackage(fullName):
	pkgList = [ (fullName, 
		    [repos.pkgLatestVersion(fullName, buildBranch)]) ]
	while pkgList:
	    (name, versionList) = pkgList[0]
	    del pkgList[0]
	    for version in versionList:
		pkg = repos.getPackageVersion(name, version)
		pkgList += pkg.getPackageList()

    srcName = fullName + ":sources"
    if repos.hasPackage(srcName):
	pkg = repos.getLatestPackage(srcName, buildBranch)
	ident.populate(repos, lcache, pkg)

    packageList = []

    for buildPkg in recipeObj.getPackages(cfg.packagenamespace, newVersion):
	(p, fileMap) = _createPackage(repos, buildBranch, buildPkg, ident)
	built.append((p.getName(), p.getVersion().asString()))
	packageList.append((p, fileMap))

    # build the group before the source package is added to the 
    # packageList; the package's group doesn't include sources
    grpName = cfg.packagenamespace + ":" + recipeClass.name
    grp = package.Package(grpName, newVersion)
    for (pkg, map) in packageList:
	grp.addPackage(pkg.getName(), [ pkg.getVersion() ])

    changeSet = changeset.CreateFromFilesystem(packageList)
    grpDiff = grp.diff(None, abstract = 1)[0]

    changeSet.newPackage(grpDiff)

    if changeSetFile:
	changeSet.writeToFile(changeSetFile)
    else:
	repos.commitChangeSet(changeSet)

    repos.open("r")

    recipeObj.cleanup(builddir, destdir)

    return built

# -------------------- public below this line -------------------------

def cookItem(repos, cfg, item, prep=0, macros=()):
    """
    Cooks an item specified on the command line. If the item is a file
    which can be loaded as a recipe, it's cooked and a change set with
    the result is saved. If that's not the case, the item is taken to
    be the name of a package, and the recipe is pulled from the :sources
    component, built, and committed to the repository.

    @param repos: Repository to use for building
    @type repos: repository.Repository
    @param cfg: srs configuration
    @type cfg: srscfg.SrsConfiguration
    @param item: the item to cook
    @type item: str
    @param prep: If true, the build stops after the package is unpacked
    and None is returned instead of a changeset.
    @type prep: boolean
    @param macros: set of macros for the build
    @type macros: sequence
    """

    buildList = []
    changeSetFile = None
    if os.path.isfile(item):
	recipeFile = item

	if recipeFile[0] != '/':
	    recipeFile = "%s/%s" % (os.getcwd(), recipeFile)

	try:
	    classList = recipe.RecipeLoader(recipeFile)
	except recipe.RecipeFileError, msg:
	    raise CookError(str(msg))

	for (className, classObject) in classList.items():
	    buildList.append((classObject, cfg.defaultbranch,
			      classObject.name + ".srs"))
    else:
	name = item
	if name[0] != ":":
	    name = cfg.packagenamespace + ":" + item
	name += ":sources"

	try:
	    sourceComponent = repos.getLatestPackage(name, cfg.defaultbranch)
	except repository.PackageMissing:
	    raise CookError, "cannot find anything to build for %s" % item

	srcFileInfo = None
	for (fileId, path, version) in sourceComponent.fileList():
	    if path == item + ".recipe":
		srcFileInfo = (fileId, version)
		break
	
	if not srcFileInfo:
	    raise CookError, "%s does not contain %s.recipe" % (name, item)
	
	fileObj = repos.getFileVersion(fileId, version)
	theFile = repos.pullFileContentsObject(fileObj.sha1())
	(fd, recipeFile) = tempfile.mkstemp("", "recipe-")

	os.write(fd, theFile.read())
	os.close(fd)

	try:
	    classList = recipe.RecipeLoader(recipeFile)
	except recipe.RecipeFileError, msg:
	    raise CookError(str(msg))

	for (className, classObject) in classList.items():
	    buildList.append((classObject, cfg.defaultbranch, None))

	os.unlink(recipeFile)

    built = []
    for (classObject, branch, csFile) in buildList:
	try:
	    built += cookObject(repos, cfg, classObject, branch,
				changeSetFile = csFile,
				prep = prep, macros = macros)
	except repository.RepositoryError, e:
	    raise CookError(str(e))

    return built

class CookError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
	return self.msg

    def __str__(self):
	return repr(self)

def cookCommand(cfg, args, prep, macros):
    # this ensures the repository exists
    repos = repository.LocalRepository(cfg.reppath, "c")
    repos.close()

    for item in args:
        # we want to fork here to isolate changes the recipe might make
        # in the environment (such as environment variables)
        signal.signal(signal.SIGTTOU, signal.SIG_IGN)
        pid = os.fork()
        if not pid:
            # child, set ourself to be the foreground process
            os.setpgrp()
            os.tcsetpgrp(0, os.getpgrp())
	    repos = repository.LocalRepository(cfg.reppath, "r")
            try:
                built = cookItem(repos, cfg, item, prep=prep, macros=macros)
            except CookError, msg:
		log.error(str(msg))
                sys.exit(1)
            for (pkg, version) in built:
                print "Committed", pkg, version, "to the repository"
            sys.exit(0)
        else:
            while 1:
                try:
                    # XXX replace 2 with os.WUNTRACED in python 2.3
                    (id, status) = os.waitpid(pid, 2)
                    if os.WIFSTOPPED(status):
                        # if our child has been stopped (Ctrl+Z or similar)
                        # stop ourself
                        os.kill(os.getpid(), os.WSTOPSIG(status))
                        # when we continue, place our child back
                        # in the foreground process group
                        os.tcsetpgrp(0, pid)
                        # tell the child to continue
                        os.kill(-pid, signal.SIGCONT)
                    else:
                        # if our child exited with a non-0 status, exit
                        # with that status
                        if os.WEXITSTATUS(status):
                            sys.exit(os.WEXITSTATUS(status))
                        break
                except KeyboardInterrupt:
                    os.kill(-pid, signal.SIGINT)
        # make sure that we are the foreground process again
        os.tcsetpgrp(0, os.getpgrp())

#
# see if the head of the specified branch is a duplicate
# of the file object passed; it so return the version object
# for that duplicate
def checkBranchForDuplicate(repos, branch, file):
    version = repos.fileLatestVersion(file.id(), branch)
    if not version:
	return None

    lastFile = repos.getFileVersion(file.id(), version)

    if file.same(lastFile):
	return version

    return None

def makeFileId(*args):
    assert(args)
    str = "".join(args)
    return _IdGen()(str)
