#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Contains the functions which builds a recipe and commits the
resulting packages to the repository.
"""

from build import *

import deps.deps
from repository import changeset
from repository import filecontents
from repository import repository
import files
import helper
import log
from build import lookaside, use
import os
import package
import resource
import sha1helper
import signal
import sys
import tempfile
import time
import types
import util
import versions

# -------------------- private below this line -------------------------

# type could be "src"
#
# returns a (pkg, fileMap) tuple
def _createComponent(repos, branch, bldPkg, newVersion, ident):
    fileMap = {}
    p = package.Trove(bldPkg.getName(), newVersion, bldPkg.flavor, None)
    p.setRequires(bldPkg.requires)
    p.setProvides(bldPkg.provides)

    for (path, (realPath, f)) in bldPkg.iteritems():
        if isinstance(f, files.RegularFile):
            flavor = f.flavor.deps
        else:
            flavor = None
        (fileId, fileVersion) = ident(path, flavor)
	f.id(fileId)

        if not fileVersion:
            # no existing versions for this path
	    p.addFile(f.id(), path, newVersion)
	else:
	    oldFile = repos.getFileVersion(f.id(), fileVersion)
            # check to see if the file we have now is the same as the
            # file in the previous version of the file (modes, contents, etc)
	    if oldFile == f:
                # if it's the same, use old version
		p.addFile(f.id(), path, fileVersion)
	    else:
                # otherwise use the new version
		p.addFile(f.id(), path, newVersion)

        fileMap[f.id()] = (f, realPath, path)

    return (p, fileMap)

class _IdGen:
    def __call__(self, path, flavor):
	if self.map.has_key((path, flavor)):
	    return self.map[(path, flavor)]

	fileid = sha1helper.hashString("%s %f %s %s" % (path, time.time(), 
                                                     self.noise,
                                                     flavor))
	self.map[(path, flavor)] = (fileid, None)
	return (fileid, None)

    def __init__(self, map=None):
	# file ids need to be unique. we include the time and path when
	# we generate them; any data put here is also used
	uname = os.uname()
	self.noise = "%s %s" % (uname[1], uname[2])
        if map is None:
            self.map = {}
        else:
            self.map = map

    def populate(self, repos, pkg):
	# Find the files and ids which were owned by the last version of
	# this package on the branch.
        for f in repos.iterFilesInTrove(pkg.getName(), pkg.getVersion(),
                                        pkg.getFlavor(), withFiles=True):
            fileId, path, version, fileObj = f
            if isinstance(fileObj, files.RegularFile):
                flavor = fileObj.flavor.deps
            else:
                flavor = None
            self.map[(path, flavor)] = (fileId, version)
# -------------------- public below this line -------------------------

def cookObject(repos, cfg, recipeClass, buildLabel, changeSetFile = None, 
	       prep=True, macros={}, buildBranch = None):
    """
    Turns a recipe object into a change set, and sometimes commits the
    result.

    @param repos: Repository to both look for source files and file id's in.
    @type repos: repository.Repository
    @param cfg: conary configuration
    @type cfg: conarycfg.ConaryConfiguration
    @param recipeClass: class which will be instantiated into a recipe
    @type recipeClass: class descended from recipe.Recipe
    @param buildLabel: label to use to to find the branch to build on
    @type buildBranch: versions.BranchName
    @param changeSetFile: if set, the changeset is stored in this file
    instead of committed to a repository
    @type changeSetFile: str
    @param prep: If true, the build stops after the package is unpacked
    and None is returned instead of a changeset.
    @type prep: boolean
    @param macros: set of macros for the build
    @type macros: dict
    @param buildBranch: branch to build on; if present buildLabel is ignored.
    this branch does not need to contain timestamps; they'll be looked up if
    they are missing.
    
    @type buildBranch: versions.Version
    @rtype: list of strings
    """

    if not (hasattr(recipeClass, 'name') and hasattr(recipeClass, 'version')):
        raise CookError('recipe class must have name and version defined')

    log.info("Building %s", recipeClass.name)
    fullName = recipeClass.name

    if not buildBranch:
	if repos.hasPackage(buildLabel.getHost(), fullName):
	    vers = repos.getTroveLeavesByLabel([fullName], buildLabel)[fullName]
	    if not len(vers):
		raise CookError('No branches labeled %s exist for trove %s'
				% (buildLabel.asString(), fullName))
	    elif len(vers) > 1:
		raise CookError('Multiple branches labeled %s exist for '
				'trove %s' % (fullName, buildLabel.asString))
		
	    buildBranch = vers[0].branch()
	else:
	    # for the first build, we're willing to create the branch for
	    # them (though it has to be a trunk!)
	    buildBranch = versions.Version([buildLabel])
    elif not buildBranch.timeStamps():
	# trunk branch, go ahead and create if
	pass
    elif max(buildBranch.timeStamps()) == 0:
	# need to get the timestamps (and the branch has to exist)
	try:
	    ver = repos.getTroveLatestVersion(fullName, buildBranch)
	except repository.PackageMissing:
	    raise CookError('Branch %s does not exist for trove %s'
			    % (buildBranch.asString(), fullName))

	buildBranch = ver.branch()
	del ver

    if issubclass(recipeClass, recipe.PackageRecipe):
	ret = cookPackageObject(repos, cfg, recipeClass, buildBranch,
                                prep = prep, macros = macros)
    elif issubclass(recipeClass, recipe.GroupRecipe):
	ret = cookGroupObject(repos, cfg, recipeClass, buildBranch, 
			      macros = macros)
    elif issubclass(recipeClass, recipe.FilesetRecipe):
	ret = cookFilesetObject(repos, cfg, recipeClass, buildBranch, 
				macros = macros)
    else:
        raise AssertionError

    # cook*Object returns None if using prep
    if ret is None:
        return []
    
    (cs, built, cleanup) = ret
    if changeSetFile:
	cs.writeToFile(changeSetFile)
    else:
	repos.commitChangeSet(cs)

    if cleanup:
	(fn, args) = cleanup
	fn(*args)

    return built

def cookGroupObject(repos, cfg, recipeClass, buildBranch, macros={}):
    """
    Turns a group recipe object into a change set. Returns the absolute
    changeset created, a list of the names of the packages built, and
    and None (for compatibility with cookPackageObject).

    @param repos: Repository to both look for source files and file id's in.
    @type repos: repository.Repository
    @param cfg: conary configuration
    @type cfg: conarycfg.ConaryConfiguration
    @param recipeClass: class which will be instantiated into a recipe
    @type recipeClass: class descended from recipe.Recipe
    @param newVersion: version to assign the newly built objects
    @param buildBranch: the branch the new build will be committed to
    @type buildBranch: versions.Version
    @param macros: set of macros for the build
    @type macros: dict
    @rtype: tuple
    """

    fullName = recipeClass.name

    recipeObj = recipeClass(repos, cfg, buildBranch, cfg.flavor)
    recipeObj.setup()

    nextVersion = helper.nextVersion(repos, fullName, recipeClass.version, 
				     None, buildBranch, binary = True)

    grpFlavor = deps.deps.DependencySet()
    grp = package.Trove(fullName, nextVersion, grpFlavor, None)

    d = {}
    for (name, versionList) in recipeObj.getTroveList().iteritems():
	d[name] = versionList

    d = repos.getTroveVersionFlavors(d)

    for (name, subd) in d.iteritems():
	for (v, flavorList) in subd.iteritems():
	    for flavor in flavorList:
		if not flavor or cfg.flavor.satisfies(flavor):
		    grp.addTrove(name, v, flavor)
		    if flavor:
			grpFlavor.union(flavor)

    grpDiff = grp.diff(None, absolute = 1)[0]
    changeSet = changeset.ChangeSet()
    changeSet.newPackage(grpDiff)

    built = [ (grp.getName(), grp.getVersion().asString()) ]
    return (changeSet, built, None)

def cookFilesetObject(repos, cfg, recipeClass, buildBranch, macros={}):
    """
    Turns a fileset recipe object into a change set. Returns the absolute
    changeset created, a list of the names of the packages built, and
    and None (for compatibility with cookPackageObject).

    @param repos: Repository to both look for source files and file id's in.
    @type repos: repository.Repository
    @param cfg: conary configuration
    @type cfg: conarycfg.ConaryConfiguration
    @param recipeClass: class which will be instantiated into a recipe
    @type recipeClass: class descended from recipe.Recipe
    @param buildBranch: the branch the new build will be committed to
    @type buildBranch: versions.Version
    @param macros: set of macros for the build
    @type macros: dict
    @rtype: tuple
    """

    fullName = recipeClass.name

    recipeObj = recipeClass(repos, cfg, buildBranch, cfg.flavor)
    recipeObj.setup()

    changeSet = changeset.ChangeSet()

    l = []
    flavor = deps.deps.DependencySet()
    for (fileId, path, version) in recipeObj.iterFileList():
	fileObj = repos.getFileVersion(fileId, version)
	l.append((fileId, path, version))
	if fileObj.hasContents:
	    flavor.union(fileObj.flavor.value())
	changeSet.addFile(fileId, None, version, fileObj.freeze())
	
	# since the file is already in the repository (we just committed
	# it there, so it must be there!) leave the contents out. this
	# means that the change set we generate can't be used as the 
	# source of an update, but it saves sending files across the
	# network for no reason

    nextVersion = helper.nextVersion(repos, fullName, recipeClass.version, 
				     flavor, buildBranch, binary = True)

    fileset = package.Trove(fullName, nextVersion, flavor, None)
    for (fileId, path, version) in l:
	fileset.addFile(fileId, path, version)

    filesetDiff = fileset.diff(None, absolute = 1)[0]
    changeSet.newPackage(filesetDiff)

    built = [ (fileset.getName(), fileset.getVersion().asString()) ]
    return (changeSet, built, None)

def cookPackageObject(repos, cfg, recipeClass, buildBranch, 
		      prep=True, macros={}):
    """
    Turns a package recipe object into a change set. Returns the absolute
    changeset created, a list of the names of the packages built, and
    and a tuple with a function to call and its arguments, which should
    be called when the build root for the package can be safely removed
    (the changeset returned refers to files in that build root, so those
    files can't be removed until the changeset has been comitted or saved)

    @param repos: Repository to both look for source files and file id's in.
    @type repos: repository.Repository
    @param cfg: conary configuration
    @type cfg: conarycfg.ConaryConfiguration
    @param recipeClass: class which will be instantiated into a recipe
    @type recipeClass: class descended from recipe.Recipe
    @param buildBranch: the branch the new build will be committed to
    @type buildBranch: versions.Version
    @param prep: If true, the build stops after the package is unpacked
    and None is returned instead of a changeset.
    @type prep: boolean
    @param macros: set of macros for the build
    @type macros: dict
    @rtype: tuple
    """

    built = []
    fullName = recipeClass.name

    lcache = lookaside.RepositoryCache(repos)

    srcdirs = [ os.path.dirname(recipeClass.filename),
		cfg.sourcePath % {'pkgname': recipeClass.name} ]
    recipeObj = recipeClass(cfg, lcache, srcdirs, macros)

    # populate the repository source lookaside cache from the :source component
    # XXXXXXX Ack, this will get the latest version of file that's in a
    #         :source component -- not the version of the file that comes
    #         from the same version as the recipe we're currently cooking!
    srcName = fullName + ':source'
    try:
        srcVersion = repos.getTroveLatestVersion(srcName, buildBranch)
    except repository.PackageMissing:
        srcVersion = None
    if srcVersion:
        for f in repos.iterFilesInTrove(srcName, srcVersion, None,
                                        withFiles=True):
            fileId, path, version, fileObj = f
            assert(path[0] != "/")
            # we might need to retrieve this source file
            # to enable a build, so we need to find the
            # sha1 hash of it since that's how it's indexed
            # in the file store
            if isinstance(fileObj, files.RegularFile):
                # it only makes sense to fetch regular files, skip
                # anything that isn't
                lcache.addFileHash(path, srcName, srcVersion, 
				   fileObj.contents.sha1())

    builddir = cfg.buildPath + "/" + recipeObj.name
    use.track(True)

    try:
	recipeObj.setup()
	recipeObj.unpackSources(builddir)
	# if we're only extracting, continue to the next recipe class.
	if prep:
	    return
	
	cwd = os.getcwd()
	util.mkdirChain(builddir + '/' + recipeObj.mainDir())

	try:
	    os.chdir(builddir + '/' + recipeObj.mainDir())
	    util.mkdirChain(cfg.tmpDir)
	    destdir = tempfile.mkdtemp("", "conary-%s-" % recipeObj.name, cfg.tmpDir)
	    recipeObj.doBuild(builddir, destdir)
	    log.info('Processing %s', recipeClass.name)
	    recipeObj.doDestdirProcess() # includes policy
	    use.track(False)
	finally:
	    os.chdir(cwd)
    except Exception, e:
	if cfg.debugRecipeExceptions:
	    raise 
	else: 
	    print e 
	    sys.exit(1)
    
    grpName = recipeClass.name

    # build up the name->fileid mapping so we reuse fileids wherever
    # possible; we do this by looking in the database for the latest
    # group for each flavor avalable on the branch and recursing
    # through its subpackages; this mechanism continues to work as
    # subpackages come and go.
    ident = _IdGen()
    try:
        versionList = repos.getTroveFlavorsLatestVersion(grpName, buildBranch)
    except repository.PackageNotFound:
        versionList = []
    troveList = [ (grpName, x[0], x[1]) for x in versionList ]
    while troveList:
        troves = repos.getTroves(troveList)
        troveList = []
        for trove in troves:
            ident.populate(repos, trove)
            troveList += [ x for x in trove.iterTroveList() ]

    requires = deps.deps.DependencySet()
    provides = deps.deps.DependencySet()
    flavor = deps.deps.DependencySet()

    bldList = recipeObj.getPackages()

    for buildPkg in bldList:
	flavor.union(buildPkg.flavor)

    nextVersion = helper.nextVersion(repos, grpName, recipeClass.version, 
				     flavor, buildBranch, binary = True)

    grp = package.Trove(grpName, nextVersion, flavor, None)

    packageList = []
    for buildPkg in bldList:
	(p, fileMap) = _createComponent(repos, buildBranch, buildPkg, 
					nextVersion, ident)

	requires.union(p.getRequires())
	provides.union(p.getProvides())

	built.append((p.getName(), p.getVersion().asString()))
	packageList.append((p, fileMap))
	grp.addTrove(p.getName(), p.getVersion(), p.getFlavor())

    grp.setRequires(requires)
    grp.setProvides(provides)

    changeSet = changeset.CreateFromFilesystem(packageList)
    changeSet.addPrimaryPackage(grpName, nextVersion, None)

    grpDiff = grp.diff(None, absolute = 1)[0]
    changeSet.newPackage(grpDiff)

    return (changeSet, built, (recipeObj.cleanup, (builddir, destdir)))

def cookItem(repos, cfg, item, prep=0, macros={}, buildBranch = None):
    """
    Cooks an item specified on the command line. If the item is a file
    which can be loaded as a recipe, it's cooked and a change set with
    the result is saved. If that's not the case, the item is taken to
    be the name of a package, and the recipe is pulled from the :source
    component, built, and committed to the repository.

    @param repos: Repository to use for building
    @type repos: repository.Repository
    @param cfg: conary configuration
    @type cfg: conarycfg.ConaryConfiguration
    @param item: the item to cook
    @type item: str
    @param prep: If true, the build stops after the package is unpacked
    and None is returned instead of a changeset.
    @type prep: boolean
    @param macros: set of macros for the build
    @type macros: dict
    """

    buildList = []
    changeSetFile = None
    if item.endswith('.recipe') and os.path.isfile(item):
	recipeFile = item

	if recipeFile[0] != '/':
	    recipeFile = "%s/%s" % (os.getcwd(), recipeFile)

	try:
	    loader = recipe.RecipeLoader(recipeFile, cfg=cfg, repos=repos)
	except recipe.RecipeFileError, msg:
	    raise CookError(str(msg))

	recipeClass = loader.getRecipe()
        changeSetFile = "%s-%s.ccs" % (recipeClass.name, recipeClass.version)
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
        troves = cookObject(repos, cfg, recipeClass, cfg.buildLabel,
                            changeSetFile = changeSetFile,
                            prep = prep, macros = macros,
			    buildBranch = buildBranch)
        if troves:
            built = (tuple(troves), changeSetFile)
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

def cookCommand(cfg, args, prep, macros, buildBranch = None):
    # this ensures the repository exists
    repos = helper.openRepository(cfg.repositoryMap, cfg.repPath)

    for item in args:
        # we want to fork here to isolate changes the recipe might make
        # in the environment (such as environment variables)
        signal.signal(signal.SIGTTOU, signal.SIG_IGN)
        pid = os.fork()
        if not pid:
            # child, set ourself to be the foreground process
            os.setpgrp()
            os.tcsetpgrp(0, os.getpgrp())
	    # make sure we do not accidentally make files group-writeable
	    os.umask(0022)
	    # and if we do not create core files we will not package them
	    resource.setrlimit(resource.RLIMIT_CORE, (0,0))
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
    return _IdGen()(str, None)[0]
