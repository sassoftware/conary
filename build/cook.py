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
Contains the functions which builds a recipe and commits the
resulting packages to the repository.
"""

import deps.deps
from repository import changeset
from repository import filecontents
from repository import repository
from repository.netclient import NetworkRepositoryClient
import files
from lib import log
import buildinfo, buildpackage, lookaside, use, recipe
import os
import resource
from lib import sha1helper
import shutil
import signal
import sys
import tempfile
import time
import trove
import types
import conaryclient
from lib import util
import versions

# -------------------- private below this line -------------------------

# type could be "src"
#
# returns a (pkg, fileMap) tuple
def _createComponent(repos, branch, bldPkg, newVersion, ident):
    fileMap = {}
    p = trove.Trove(bldPkg.getName(), newVersion, bldPkg.flavor, None)
    p.setRequires(bldPkg.requires)
    p.setProvides(bldPkg.provides)

    linkGroups = {}
    for pathList in bldPkg.linkGroups.itervalues():
        linkGroupId = sha1helper.sha1String("\n".join(pathList))
        linkGroups.update({}.fromkeys(pathList, linkGroupId))

    for (path, (realPath, f)) in bldPkg.iteritems():
        if isinstance(f, files.RegularFile):
            flavor = f.flavor.deps
        else:
            flavor = None
        (pathId, fileVersion, oldFile) = ident(path, flavor)
	f.pathId(pathId)
        
        linkGroupId = linkGroups.get(path, None)
        if linkGroupId:
            f.linkGroup.set(linkGroupId)

        if not fileVersion:
            # no existing versions for this path
	    p.addFile(f.pathId(), path, newVersion, f.fileId())
	else:
            # check to see if the file we have now is the same as the
            # file in the previous version of the file (modes, contents, etc)
	    if oldFile == f:
                # if it's the same, use old version
		p.addFile(f.pathId(), path, fileVersion, f.fileId())
	    else:
                # otherwise use the new version
		p.addFile(f.pathId(), path, newVersion, f.fileId())

        fileMap[f.pathId()] = (f, realPath, path)

    return (p, fileMap)

class _IdGen:
    def __call__(self, path, flavor):
	if self.map.has_key(path):
	    return self.map[path]

	fileid = sha1helper.md5String("%s %f %s" % (path, time.time(), 
                                                     self.noise))
	self.map[(path, flavor)] = (fileid, None, None)
	return (fileid, None, None)

    def __init__(self, map=None):
	# path ids need to be unique. we include the time and path when
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
            pathId, path, fileId, version, fileObj = f
            if isinstance(fileObj, files.RegularFile):
                flavor = fileObj.flavor.deps
            else:
                flavor = None
            if self.map.has_key(path):
                assert(self.map[path][0] == pathId)
            self.map[path] = (pathId, version, fileObj)
# -------------------- public below this line -------------------------

def cookObject(repos, cfg, recipeClass, buildLabel, changeSetFile = None, 
	       prep=True, macros={}, buildBranch = None, targetLabel = None, 
               sourceVersion = None, 
	       resume = None, alwaysBumpCount = False):
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
    @type buildLabel: versions.Label
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
    @param targetLabel: label to use for the cooked troves; it is used
    as a new branch from whatever version was previously built
    default), the buildBranch is used
    @type targetLabel: versions.Label
    @param resume: indicates whether to resume the previous build.  If True,
    resume at the line of last breakage.  If an integer, resume at that line.
    If 'policy', rerun the policy only.  Note that resume is only valid when
    cooking a recipe from a file, not from the repository.  
    @type resume: bool or str
    @param alwaysBumpCount: if True, the cooked troves will not share a 
    full version with any other existing troves with the same name, 
    even if their flavors would differentiate them.  
    @type alwaysBumpCount: bool
    
    @rtype: list of strings
    """

    use.overrideFlags(cfg, recipeClass.name)
    if not (hasattr(recipeClass, 'name') and hasattr(recipeClass, 'version')):
        raise CookError('recipe class must have name and version defined')
    if '-' in recipeClass.version:
        raise recipe.RecipeFileError(
            "Version string %s has illegal '-' character" %recipeClass.version)

    log.info("Building %s", recipeClass.name)
    fullName = recipeClass.name

    if not buildBranch:
	vers = repos.getTroveLeavesByLabel([fullName], buildLabel)[fullName]

	if not vers:
	    # try looking for :source
	    srcName = fullName + ":source"
	    vers = repos.getTroveLeavesByLabel([srcName], buildLabel)[srcName]

	if len(vers) > 1:
	    raise CookError('Multiple branches labeled %s exist for '
			    'trove %s' % (fullName, buildLabel.asString))
	elif not len(vers):
	    if not repos.hasPackage(buildLabel.getHost(), fullName):
		# for the first build, we're willing to create the branch for
		# them 
		buildBranch = versions.Version([buildLabel])
	    else:
		raise CookError('No branches labeled %s exist for trove %s'
				% (buildLabel.asString(), fullName))
	else:
	    buildBranch = vers[0].branch()

	# hack
	for version in buildBranch.versions:
	    if isinstance(version, versions.VersionRelease) and \
			version.buildCount is None:
		version.buildCount = 0


    elif not buildBranch.timeStamps():
	# trunk branch, go ahead and create if
	pass
    elif max(buildBranch.timeStamps()) == 0:
	# need to get the timestamps (and the branch has to exist)
	try:
	    ver = repos.getTroveLatestVersion(fullName, buildBranch)
	except repository.TroveMissing:
	    raise CookError('Branch %s does not exist for trove %s'
			    % (buildBranch.asString(), fullName))

	buildBranch = ver.branch()
	del ver

    if buildBranch:
        macros['buildbranch'] = buildBranch.asString()
        macros['buildlabel'] = buildBranch.asString().split('/')[-1]
    else:
        macros['buildbranch'] = buildLabel.asString()
        macros['buildlabel'] = buildLabel.asString()

    if issubclass(recipeClass, recipe.PackageRecipe):
	ret = cookPackageObject(repos, cfg, recipeClass, buildBranch,
                                prep = prep, macros = macros,
				targetLabel = targetLabel,
                                sourceVersion = sourceVersion,
				resume = resume, 
                                alwaysBumpCount = alwaysBumpCount)
    elif issubclass(recipeClass, recipe.GroupRecipe):
	ret = cookGroupObject(repos, cfg, recipeClass, buildBranch, 
			      macros = macros, targetLabel = targetLabel,
                              sourceVersion = sourceVersion,
                              alwaysBumpCount = alwaysBumpCount)
    elif issubclass(recipeClass, recipe.FilesetRecipe):
	ret = cookFilesetObject(repos, cfg, recipeClass, buildBranch, 
				macros = macros, targetLabel = targetLabel,
                                sourceVersion = sourceVersion,
                                alwaysBumpCount = alwaysBumpCount)
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

def cookGroupObject(repos, cfg, recipeClass, buildBranch, macros={},
		    targetLabel = None, sourceVersion=None,
                    alwaysBumpCount=False):
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
    @param targetLabel: label to use for the cooked troves; it is used
    as a new branch from whatever version was previously built
    default), the buildBranch is used
    @type targetLabel: versions.Label
    @param alwaysBumpCount: if True, the cooked troves will not share a 
    full version with any other existing troves with the same name, 
    even if their flavors would differentiate them.  
    @type alwaysBumpCount: bool
    """

    fullName = recipeClass.name

    recipeObj = recipeClass(repos, cfg, buildBranch, cfg.flavor)

    try:
        use.track(True)
        recipeObj.Flags._freeze()
	recipeObj.setup()
        recipeObj.findTroves()
	use.track(False)
    except recipe.RecipeFileError, msg:
	raise CookError(str(msg))

    grpFlavor = deps.deps.DependencySet()
    grpFlavor.union(buildpackage._getUseDependencySet(recipeObj))

    for (name, versionFlavorList) in recipeObj.getTroveList().iteritems():
        for (version, flavor) in versionFlavorList:
            grpFlavor.union(flavor)

    if not grpFlavor:
        grpFlavor = None

    grp = trove.Trove(fullName, versions.NewVersion(), grpFlavor, None)

    for (name, versionFlavorList) in recipeObj.getTroveList().iteritems():
        for (version, flavor) in versionFlavorList:
            grp.addTrove(name, version, flavor)

    targetVersion = repos.nextVersion(fullName, recipeClass.version, grpFlavor, 
				      buildBranch, binary = True, 
                                      sourceVersion=sourceVersion,
                                      alwaysBumpCount=alwaysBumpCount)

    if targetLabel:
	targetVersion = targetVersion.fork(targetLabel)
	targetVersion.trailingVersion().incrementBuildCount()

    grp.changeVersion(targetVersion)

    grpDiff = grp.diff(None, absolute = 1)[0]
    changeSet = changeset.ChangeSet()
    changeSet.newPackage(grpDiff)

    built = [ (grp.getName(), grp.getVersion().asString(), grp.getFlavor()) ]
    return (changeSet, built, None)

def cookFilesetObject(repos, cfg, recipeClass, buildBranch, macros={},
		      targetLabel = None, sourceVersion=None,
                      alwaysBumpCount=False):
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
    @param targetLabel: label to use for the cooked troves; it is used
    as a new branch from whatever version was previously built
    default), the buildBranch is used
    @type targetLabel: versions.Label
    @param alwaysBumpCount: if True, the cooked troves will not share a 
    full version with any other existing troves with the same name, 
    even if their flavors would differentiate them.  
    @type alwaysBumpCount: bool
    @rtype: tuple
    """

    fullName = recipeClass.name

    recipeObj = recipeClass(repos, cfg, buildBranch, cfg.flavor)
    recipeObj.setup()

    changeSet = changeset.ChangeSet()

    l = []
    flavor = deps.deps.DependencySet()
    for (pathId, path, fileId, version) in recipeObj.iterFileList():
	fileObj = repos.getFileVersion(pathId, fileId, version)
	l.append((pathId, path, version, fileId))
	if fileObj.hasContents:
	    flavor.union(fileObj.flavor.value())
	changeSet.addFile(None, fileId, fileObj.freeze())
    if not flavor:
        flavor = None

	# since the file is already in the repository (we just committed
	# it there, so it must be there!) leave the contents out. this
	# means that the change set we generate can't be used as the 
	# source of an update, but it saves sending files across the
	# network for no reason

    targetVersion = repos.nextVersion(fullName, recipeClass.version, flavor, 
				      buildBranch, binary = True, 
                                      sourceVersion=sourceVersion,
                                      alwaysBumpCount=False)

    if targetLabel:
	targetVersion = targetVersion.fork(targetLabel)
	targetVersion.trailingVersion().incrementBuildCount()

    fileset = trove.Trove(fullName, targetVersion, flavor, None)
    for (pathId, path, version, fileId) in l:
	fileset.addFile(pathId, path, version, fileId)

    filesetDiff = fileset.diff(None, absolute = 1)[0]
    changeSet.newPackage(filesetDiff)

    built = [ (fileset.getName(), fileset.getVersion().asString(), 
                                                fileset.getFlavor()) ]
    return (changeSet, built, None)

def cookPackageObject(repos, cfg, recipeClass, buildBranch, prep=True, 
		      macros={}, targetLabel = None, sourceVersion=None,
                      resume = None, alwaysBumpCount=False):
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
    @param targetLabel: label to use for the cooked troves; if None (the
    default), the version used is the next version on the buildBranch 
    @param alwaysBumpCount: if True, the cooked troves will not share a 
    full version with any other existing troves with the same name, 
    even if their flavors would differentiate them.  
    @type alwaysBumpCount: bool
    @rtype: tuple
    """

    built = []
    fullName = recipeClass.name

    lcache = lookaside.RepositoryCache(repos)

    srcdirs = [ os.path.dirname(recipeClass.filename),
		cfg.sourceSearchDir % {'pkgname': recipeClass.name} ]
    recipeObj = recipeClass(cfg, lcache, srcdirs, macros)
    recipeObj.populateLcache()
    
    builddir = cfg.buildPath + "/" + recipeObj.name
    use.track(True)
    recipeObj.Flags._freeze()

    recipeObj.setup()
    bldInfo = buildinfo.BuildInfo(builddir)
    recipeObj.buildinfo = bldInfo

    # don't bother with prep the dirs if we are resuming
    if not resume:
	if os.path.exists(builddir):
	    shutil.rmtree(builddir)
	util.mkdirChain(builddir)
    else:
        try:
            bldInfo.read()
        except:
            pass

    bldInfo.begin()
    if resume is True:
        resume = bldInfo.lastline
    recipeObj.unpackSources(builddir, resume)
    # if we're only extracting, continue to the next recipe class.
    if prep:
	return
	
    cwd = os.getcwd()
    try:
	if resume and 'destdir' in bldInfo:
	    destdir = bldInfo.destdir
            # this writes destdir back out to the file
            # in case we need it again
            bldInfo.destdir = destdir
	else:
	    util.mkdirChain(builddir + '/' + recipeObj.mainDir())
	    util.mkdirChain(cfg.tmpDir)
	    destdir = tempfile.mkdtemp("", "conary-%s-" % recipeObj.name, cfg.tmpDir)
	    bldInfo.destdir = destdir
	os.chdir(builddir + '/' + recipeObj.mainDir())
	recipeObj.doBuild(builddir, destdir, resume=resume)
	if resume and resume != "policy" and recipeObj.resumeList[-1][1] != False:
	    log.info('Finished Building %s Lines %s, Not Running Policy', recipeClass.name, resume)
	    return
	log.info('Processing %s', recipeClass.name)
	recipeObj.doDestdirProcess() # includes policy
	bldInfo.stop()
	use.track(False)
    finally:
	os.chdir(cwd)
    
    grpName = recipeClass.name

    bldList = recipeObj.getPackages()
    if not bldList:
	# no components in packages
	log.warning('Cowardlily refusing to create empty package %s'
		    %recipeClass.name)
	return

    flavorMap = {}
    for buildPkg in bldList:
        compName = buildPkg.getName()
        main, comp = compName.split(':')
        if main not in flavorMap:
            flavorMap[main] = deps.deps.DependencySet()
        flavorMap[main].union(buildPkg.flavor)
    for pkg, flavor in flavorMap.iteritems():
        if not flavor:
            flavorMap[pkg] = None

    targetVersion = repos.nextVersion(grpName, recipeClass.version, 
				      flavorMap.values(), buildBranch, 
                                      binary = True, 
                                      sourceVersion=sourceVersion, 
                                      alwaysBumpCount=alwaysBumpCount)

    if targetLabel:
	targetVersion = targetVersion.fork(targetLabel)
	targetVersion.trailingVersion().incrementBuildCount()

    # build up the name->fileid mapping so we reuse fileids wherever
    # possible; we do this by looking in the database for the latest
    # packages for each flavor available on the branch and recursing
    # through their subpackages; this mechanism continues to work as
    # packages and subpackages come and go.
    packageList = []
    grpMap = {}
    ident = _IdGen()
    for buildPkg in bldList:
        compName = buildPkg.getName()
        main, comp = compName.split(':')
        if main not in grpMap:
            grpMap[main] = trove.Trove(main, targetVersion, flavorMap[main], 
                                                                        None)

        searchBranch = buildBranch
        versionList = []
        while not versionList and searchBranch:
            try:
                versionList = repos.getTroveFlavorsLatestVersion(main, 
                                                                 searchBranch)
            except repository.PackageNotFound:
                pass

            if not versionList:
                if searchBranch.hasParent():
                    searchBranch = searchBranch.parentNode().branch()
                else:
                    searchBranch = None

        troveList = [ (main, x[0], x[1]) for x in versionList ]
        while troveList:
            troves = repos.getTroves(troveList)
            troveList = []
            for trv in troves:
                ident.populate(repos, trv)
                troveList += [ x for x in trv.iterTroveList() ]

    for buildPkg in bldList:
        compName = buildPkg.getName()
        main, comp = compName.split(':')
        grp = grpMap[main]

	(p, fileMap) = _createComponent(repos, buildBranch, buildPkg, 
					targetVersion, ident)

	built.append((compName, p.getVersion().asString(), 
                                                    p.getFlavor() or None))
	packageList.append((p, fileMap))
	
	# don't install :test component when you are installing
	# the package
	if not comp in recipeObj.getUnpackagedComponentNames():
	    grp.addTrove(compName, p.getVersion(), p.getFlavor() or None)

    changeSet = changeset.CreateFromFilesystem(packageList)
    for packageName in grpMap:
        changeSet.addPrimaryPackage(packageName, targetVersion, 
                                                        flavorMap[packageName])

    for grp in grpMap.values():
        grpDiff = grp.diff(None, absolute = 1)[0]
        changeSet.newPackage(grpDiff)

    return (changeSet, built, (recipeObj.cleanup, (builddir, destdir)))

def cookItem(repos, cfg, item, prep=0, macros={}, buildBranch = None,
	     emerge = False, resume = None):
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
    targetLabel = None

    if item.endswith('.recipe') and os.path.isfile(item):
	if emerge:
	    raise CookError, \
		("troves must be emerged from directly from a repository")

	recipeFile = item

	if recipeFile[0] != '/':
	    recipeFile = "%s/%s" % (os.getcwd(), recipeFile)

	pkgname = recipeFile.split('/')[-1].split('.')[0]

	try:
	    loader = recipe.RecipeLoader(recipeFile, cfg=cfg, repos=repos)
            version = None
	except recipe.RecipeFileError, msg:
	    raise CookError(str(msg))

	recipeClass = loader.getRecipe()
        changeSetFile = "%s-%s.ccs" % (recipeClass.name, recipeClass.version)

	srcName = recipeClass.name + ":source"
	versionDict = repos.getTroveLeavesByLabel([srcName], cfg.buildLabel)
	versionList = versionDict[srcName]
        sourceVersion = None
        if versionList:
            maxVersion = versionList[0]
            for version in versionList[1:]:
                if version.isAfter(maxVersion):
                    maxVersion = version
            sourceVersion = maxVersion
	targetLabel = versions.CookBranch()
    else:
	if resume:
	    raise CookError('Cannot use --resume argument when cooking in repository')

        if emerge:
            label = cfg.installLabelPath
        else:
            label = None

        try:
            (loader, sourceVersion) = recipe.recipeLoaderFromSourceComponent(
                                        item, item + '.recipe', cfg, repos,
                                        label = label)[0:2]
        except recipe.RecipeFileError, msg:
            raise CookError(str(msg))

        recipeClass = loader.getRecipe()

	if emerge:
	    (fd, changeSetFile) = tempfile.mkstemp('.ccs', "emerge-%s-" % item)
	    os.close(fd)
	    targetLabel = versions.EmergeBranch()

    built = None
    try:
        troves = cookObject(repos, cfg, recipeClass, cfg.buildLabel,
                            changeSetFile = changeSetFile,
                            prep = prep, macros = macros,
			    buildBranch = buildBranch, 
			    targetLabel = targetLabel,
                            sourceVersion = sourceVersion,
			    resume = resume)
        if troves:
            built = (tuple(troves), changeSetFile)
    except repository.RepositoryError, e:
	if emerge:
	    os.unlink(changeSetFile)
        raise CookError(str(e))

    if emerge:
        client = conaryclient.ConaryClient(cfg)
        try:
            changeSet = changeset.ChangeSetFromFile(changeSetFile)
            client.applyChangeSet(changeSet)
        except (conaryclient.UpdateError, repository.CommitError), e:
            log.error(e)
            log.error("Not committing changeset: please apply %s by hand" % changeSetFile)
        else: 
            os.unlink(changeSetFile)
            built = (built[0], None)
    return built

class CookError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
	return self.msg

    def __str__(self):
	return repr(self)

def cookCommand(cfg, args, prep, macros, buildBranch = None, emerge = False, resume = None):
    # this ensures the repository exists
    repos = NetworkRepositoryClient(cfg.repositoryMap)

    # do not cook as root!
    # XXX fix emerge to build as non-root user, either build as current
    # non-root user and use consolehelper to install the changeset, or
    # have an "emergeUser" config item and change uid after the fork.
    if not emerge and not os.getuid:
        raise CookError('Do not cook as root')

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
                built = cookItem(repos, cfg, item, prep=prep, macros=macros,
				 emerge = emerge, resume = resume)
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
                if emerge == True:
                    print 'Changeset committed to local system.'
                else:
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
