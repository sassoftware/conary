#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Contains the functions which builds a recipe and commits the
resulting packages to the repository.
"""

from build import *

from repository import changeset
from repository import filecontents
import files
from repository import fsrepos
import helper
import log
from build import lookaside
import os
import package
import repository
import sha1helper
import signal
import sys
import tempfile
import time
import types
import util

# -------------------- private below this line -------------------------

# type could be "src"
#
# returns a (pkg, fileMap) tuple
def _createPackage(repos, branch, bldPkg, ident):
    fileMap = {}
    p = package.Package(bldPkg.getName(), bldPkg.getVersion())

    for (path, buildFile) in bldPkg.items():
        realPath = buildFile.getRealPath()
	(fileId, fileVersion) = ident(path)
        if isinstance(buildFile, buildpackage._BuildDeviceFile):
            f = files.ThawFile(buildFile.freeze(), fileId)
        elif realPath:
            f = files.FileFromFilesystem(realPath, fileId,
                                         requireSymbolicOwnership=True)
	    # setuid or setgid must be set explicitly in buildFile
	    # XXX there must be a better way
	    f.inode.setPerms(f.inode.perms() & 01777)
        else:
            raise CookError("unable to create file object for package")

        # set ownership, flags, etc
        f.inode.merge(buildFile.inode)
	f.flags.merge(buildFile.flags)
        
        if not fileVersion:
	    p.addFile(f.id(), path, bldPkg.getVersion())
	else:
	    oldFile = repos.getFileVersion(f.id(), fileVersion)
	    if oldFile == f:
		p.addFile(f.id(), path, fileVersion)
	    else:
		p.addFile(f.id(), path, bldPkg.getVersion())

        fileMap[f.id()] = (f, realPath, path)

    return (p, fileMap)

class _IdGen:
    def __call__(self, path):
	if self.map.has_key(path):
	    return self.map[path]

	hash = sha1helper.hashString("%s %f %s" % (path, time.time(), 
							self.noise))
	self.map[path] = (hash, None)
	return (hash, None)

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
	for (fileId, path, version) in pkg.iterFileList():
	    self.map[path] = (fileId, version)
	    if path[0] != "/":
		# we might need to retrieve this source file
		# to enable a build, so we need to find the
		# sha1 hash of it since that's how it's indexed
		# in the file store
		f = repos.getFileVersion(fileId, version)
                # it only makes sense to fetch regular files, skip
                # anything that isn't
                if isinstance(f, files.RegularFile):
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

    if not (hasattr(recipeClass, 'name') and hasattr(recipeClass, 'version')):
        raise CookError('recipe class must have name and version defined')

    log.info("Building %s", recipeClass.name)
    fullName = recipeClass.name

    currentVersion = None
    if repos.hasPackage(fullName):
	currentVersion = repos.pkgLatestVersion(fullName, buildBranch)

    newVersion = helper.nextVersion(recipeClass.version, currentVersion, 
				    buildBranch, binary = True)

    if issubclass(recipeClass, recipe.PackageRecipe):
	ret = cookPackageObject(repos, cfg, recipeClass, 
                                newVersion, buildBranch,
                                prep = prep, macros = macros)
    elif issubclass(recipeClass, recipe.GroupRecipe):
	ret = cookGroupObject(repos, cfg, recipeClass, 
                              newVersion, buildBranch, macros = macros)
    elif issubclass(recipeClass, recipe.FilesetRecipe):
	ret = cookFilesetObject(repos, cfg, recipeClass, 
                                newVersion, buildBranch, macros = macros)
    else:
        raise AssertionError

    # cook*Object returns None if using prep
    if ret is None:
        return []
    
    (cs, built, cleanup) = ret
    if changeSetFile:
	cs.writeToFile(changeSetFile)
    else:
	repos.open("w")
	repos.commitChangeSet(cs)
	repos.open("r")

    if cleanup:
	(fn, args) = cleanup
	fn(*args)

    return built

def cookGroupObject(repos, cfg, recipeClass, newVersion, buildBranch, 
		      macros=()):
    """
    Turns a group recipe object into a change set. Returns the absolute
    changeset created, a list of the names of the packages built, and
    and None (for compatibility with cookPackageObject).

    @param repos: Repository to both look for source files and file id's in.
    @type repos: repository.Repository
    @param cfg: srs configuration
    @type cfg: srscfg.SrsConfiguration
    @param recipeClass: class which will be instantiated into a recipe
    @type recipeClass: class descended from recipe.Recipe
    @param newVersion: version to assign the newly built objects
    @param buildBranch: the branch the new build will be committed to
    @type buildBranch: versions.Version
    @param macros: set of macros for the build
    @type macros: sequence
    @rtype: tuple
    """

    fullName = recipeClass.name

    recipeObj = recipeClass(repos, cfg, buildBranch)
    recipeObj.setup()

    includedSet = recipeObj.getTroveList()
    grp = package.Package(fullName, newVersion)
    for (name, versionList) in includedSet.iteritems():
	grp.addPackage(name, versionList)

    grpDiff = grp.diff(None, absolute = 1)[0]
    changeSet = changeset.ChangeSet()
    changeSet.newPackage(grpDiff)

    built = [ (grp.getName(), grp.getVersion().asString()) ]
    return (changeSet, built, None)

def cookFilesetObject(repos, cfg, recipeClass, newVersion, buildBranch, 
		      macros=()):
    """
    Turns a fileset recipe object into a change set. Returns the absolute
    changeset created, a list of the names of the packages built, and
    and None (for compatibility with cookPackageObject).

    @param repos: Repository to both look for source files and file id's in.
    @type repos: repository.Repository
    @param cfg: srs configuration
    @type cfg: srscfg.SrsConfiguration
    @param recipeClass: class which will be instantiated into a recipe
    @type recipeClass: class descended from recipe.Recipe
    @param newVersion: version to assign the newly built objects
    @param buildBranch: the branch the new build will be committed to
    @type buildBranch: versions.Version
    @param macros: set of macros for the build
    @type macros: sequence
    @rtype: tuple
    """

    fullName = recipeClass.name

    recipeObj = recipeClass(repos, cfg, buildBranch)
    recipeObj.setup()

    changeSet = changeset.ChangeSet()
    fileset = package.Package(fullName, newVersion)

    for (fileId, path, version) in recipeObj.iterFileList():
	fileset.addFile(fileId, path, version)
	fileObj = repos.getFileVersion(fileId, version)
	changeSet.addFile(fileId, None, version, fileObj.freeze())
	if fileObj.hasContents:
	    changeSet.addFileContents(fileId, changeset.ChangedFileTypes.file,
			filecontents.FromRepository(repos, 
				    fileObj.contents.sha1(), 
				    fileObj.contents.size()),
			fileObj.flags.isConfig())

    filesetDiff = fileset.diff(None, absolute = 1)[0]
    changeSet.newPackage(filesetDiff)

    built = [ (fileset.getName(), fileset.getVersion().asString()) ]
    return (changeSet, built, None)

def cookPackageObject(repos, cfg, recipeClass, newVersion, buildBranch, 
		      prep=True, macros=()):
    """
    Turns a package recipe object into a change set. Returns the absolute
    changeset created, a list of the names of the packages built, and
    and a tuple with a function to call and its arguments, which should
    be called when the build root for the package can be safely removed
    (the changeset returned refers to files in that build root, so those
    files can't be removed until the changeset has been comitted or saved)

    @param repos: Repository to both look for source files and file id's in.
    @type repos: repository.Repository
    @param cfg: srs configuration
    @type cfg: srscfg.SrsConfiguration
    @param recipeClass: class which will be instantiated into a recipe
    @type recipeClass: class descended from recipe.Recipe
    @param newVersion: version to assign the newly built objects
    @param buildBranch: the branch the new build will be committed to
    @type buildBranch: versions.Version
    @param prep: If true, the build stops after the package is unpacked
    and None is returned instead of a changeset.
    @type prep: boolean
    @param macros: set of macros for the build
    @type macros: sequence
    @rtype: tuple
    """

    repos.open("r")

    built = []
    fullName = recipeClass.name
    srcName = fullName + ":sources"

    lcache = lookaside.RepositoryCache(repos)

    srcdirs = [ os.path.dirname(recipeClass.filename),
		cfg.sourcepath % {'pkgname': recipeClass.name} ]
    recipeObj = recipeClass(cfg, lcache, srcdirs, macros)

    # build up the name->fileid mapping so we reuse fileids wherever
    # possible; we do this by looking in the database for a pacakge
    # with the same name as the recipe and recursing through it's
    # subpackages; this mechanism continues to work as subpackages
    # come and go. this has to happen early as we build up the entries
    # for the source lookaside cache simultaneously

    ident = _IdGen()
    if repos.hasPackage(fullName):
	pkgList = [ (fullName, 
		    repos.pkgLatestVersion(fullName, buildBranch)) ]
	while pkgList:
	    (name, version) = pkgList[0]
	    del pkgList[0]

	    pkg = repos.getPackageVersion(name, version)
	    pkgList += [ x for x in pkg.iterPackageList() ]
	    ident.populate(repos, lcache, pkg)

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
    
    packageList = []

    for buildPkg in recipeObj.getPackages(newVersion):
	(p, fileMap) = _createPackage(repos, buildBranch, buildPkg, ident)
	built.append((p.getName(), p.getVersion().asString()))
	packageList.append((p, fileMap))

    # build the group before the source package is added to the 
    # packageList; the package's group doesn't include sources
    grpName = recipeClass.name
    grp = package.Package(grpName, newVersion)
    for (pkg, map) in packageList:
	grp.addPackageVersion(pkg.getName(), pkg.getVersion())

    changeSet = changeset.CreateFromFilesystem(packageList)
    changeSet.addPrimaryPackage(grpName, newVersion)

    grpDiff = grp.diff(None, absolute = 1)[0]
    changeSet.newPackage(grpDiff)

    return (changeSet, built, (recipeObj.cleanup, (builddir, destdir)))

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
	    loader = recipe.RecipeLoader(recipeFile, cfg=cfg, repos=repos)
	except recipe.RecipeFileError, msg:
	    raise CookError(str(msg))

	recipeClass = loader.getRecipe()
        changeSetFile = "%s-%s.srs" % (recipeClass.name, recipeClass.version)
    else:
        try:
            loader = recipe.recipeLoaderFromSourceComponent(item,
                                                            item + '.recipe',
                                                            cfg, repos)
        except recipe.RecipeFileError, msg:
            raise CookError(str(msg))

        recipeClass = loader.getRecipe()

    built = None
    try:
        troves = cookObject(repos, cfg, recipeClass, cfg.defaultbranch,
                            changeSetFile = changeSetFile,
                            prep = prep, macros = macros)
        if troves:
            built = (tuple(troves), changeSetFile)
    except repository.repository.RepositoryError, e:
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
    repos = fsrepos.FilesystemRepository(cfg.reppath, "c")
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
	    repos = fsrepos.FilesystemRepository(cfg.reppath, "r")
            try:
                built = cookItem(repos, cfg, item, prep=prep, macros=macros)
            except CookError, msg:
		log.error(str(msg))
                sys.exit(1)
            if built is None:
                # --prep
                sys.exit(0)
            components, csFile = built
            for component, version in components:
                print "Created component:", component, version
            if csFile is None:
                print 'Changeset committed to the repository.'
            else:
                print 'Changeset written to:', csFile
            sys.exit(0)
        else:
            while 1:
                try:
                    (id, status) = os.waitpid(pid, os.WUNTRACED)
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

def makeFileId(*args):
    assert(args)
    str = "".join(args)
    return _IdGen()(str)[0]
