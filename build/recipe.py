#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Contains the base Recipe class, default macros, and miscellaneous
components used by srs .recipe files
"""

import build
import buildpackage
import destdirpolicy
import errno
import helper
import imp
import inspect
import log
import macros
import os
import package
import packagepolicy
from repository import repository
import shutil
import source
import sys
import tempfile
import types
import util

from fnmatch import fnmatchcase

baseMacros = {
    # paths
    'prefix'		: '/usr',
    'sysconfdir'	: '/etc',
    'initdir'		: '%(sysconfdir)s/init.d',
    'lib'               : 'lib',  # may be overridden with 'lib64'
    'exec_prefix'	: '%(prefix)s',
    'bindir'		: '%(exec_prefix)s/bin',
    'essentialbindir'	: '/bin',
    'sbindir'		: '%(exec_prefix)s/sbin',
    'essentialsbindir'	: '/sbin',
    'libdir'		: '%(exec_prefix)s/%(lib)s',
    'essentiallibdir'	: '/%(lib)s',
    'libexecdir'	: '%(exec_prefix)s/libexec',
    'localstatedir'	: '/var',
    'cachedir'		: '%(localstatedir)s/cache',
    'sharedstatedir'	: '%(prefix)s/com',
    'includedir'	: '%(prefix)s/include',
    'datadir'		: '%(prefix)s/share',
    'mandir'		: '%(datadir)s/man',
    'infodir'		: '%(datadir)s/info',
    'docdir'		: '%(datadir)s/doc',
    'thisdocdir'        : '%(docdir)s/%(name)s-%(version)s',
    # special component prefixes that the whole system needs to share
    'krbprefix'		: '%(exec_prefix)s/kerberos',
    'x11prefix'		: '%(exec_prefix)s/X11R6',
    # arguments/flags (empty ones are for documentation; non-existant = empty)
    'cc'		: 'gcc',
    'cflags'            : '-O2', # -g when we have debuginfo
    'cppflags'		: '', # just for providing in recipes
    'ldflags'		: '', # -g when we have debuginfo
    'mflags'		: '',
    'parallelmflags'    : '',
    'sysroot'		: '',
    'march'		: 'i386', # "machine arch"
    'os'		: 'linux',
    'target'		: 'i386-unknown-linux',
    'strip'		: 'strip',
}

crossMacros = {
    # set crossdir from cook, directly or indirectly, before adding the rest
    #'crossdir'		: 'cross-target',
    'prefix'		: '/opt/%(crossdir)s',
    'sysroot'		: '%(prefix)s/sys-root',
    'headerpath'	: '%(sysroot)s/usr/include',
}

def setupRecipeDict(d, filename):
    exec 'from build import build' in d
    exec 'from build.recipe import PackageRecipe' in d
    exec 'from build.recipe import GroupRecipe' in d
    exec 'from build.recipe import FilesetRecipe' in d
    exec 'from build.recipe import loadRecipe' in d
    exec 'import os, package, re, sys, stat, util' in d
    exec 'from build.use import Use, Arch' in d
    if sys.excepthook == util.excepthook:
	exec 'sys.excepthook = util.excepthook' in d
    d['filename'] = filename

class RecipeLoader:
    def __init__(self, filename, cfg=None, repos=None, component=None):
        self.recipes = {}
        
        if filename[0] != "/":
            raise IOError, "recipe file names must be absolute paths"

        basename = os.path.basename(filename)
        self.file = basename.replace('.', '-')
        self.module = imp.new_module(self.file)
        sys.modules[self.file] = self.module
        f = open(filename)

	setupRecipeDict(self.module.__dict__, filename)

        # store cfg and repos, so that the recipe can load
        # recipes out of the repository
        self.module.__dict__['cfg'] = cfg
        self.module.__dict__['repos'] = repos
        self.module.__dict__['component'] = component

        # create the recipe class by executing the code in the recipe
        try:
            code = compile(f.read(), filename, 'exec')
        except SyntaxError, err:
            msg = ('Error in recipe file "%s": %s\n' %(basename, err))
            if err.offset is not None:
                msg += '%s%s^\n' %(err.text, ' ' * (err.offset-1))
            else:
                msg += err.text
            raise RecipeFileError(msg)
        exec code in self.module.__dict__

        # all recipes that could be loaded by loadRecipe are loaded;
        # get rid of our references to cfg and repos
        del self.module.__dict__['cfg']
        del self.module.__dict__['repos']
        del self.module.__dict__['component']

        found = False
        for (name, obj) in self.module.__dict__.items():
            if type(obj) is not types.ClassType:
                continue
            # if a recipe has been marked to be ignored (for example, if
            # it was loaded from another recipe by loadRecipe()
            # (don't use hasattr here, we want to check only the recipe
            # class itself, not any parent class
            if 'ignore' in obj.__dict__:
                continue
            recipename = getattr(obj, 'name', '')
            # make sure the class is derived from Recipe
            if issubclass(obj, PackageRecipe):
                if recipename.startswith('group-'):
                    raise RecipeFileError(
                        'Error in recipe file "%s": package name cannot '
                        'begin with "group-"' %basename)
                if recipename.startswith('fileset-'):
                    raise RecipeFileError(
                        'Error in recipe file "%s": package name cannot '
                        'begin with "fileset-"' %basename)
	    elif issubclass(obj, GroupRecipe):
                if recipename and not recipename.startswith('group-'):
                    raise RecipeFileError(
                        'Error in recipe file "%s": group name must '
                        'begin with "group-"' %basename)
	    elif issubclass(obj, FilesetRecipe):
                if recipename and not recipename.startswith('fileset-'):
                    raise RecipeFileError(
                        'Error in recipe file "%s": fileset name must '
                        'begin with "fileset-"' %basename)
            else:
                continue
            self.recipes[name] = obj
            obj.filename = filename
            if hasattr(obj, 'name') and hasattr(obj, 'version'):
                if found:
                    raise RecipeFileError(
                        'Error in recipe file "%s": multiple recipe classes '
                        'with both name and version exist' %basename)
                self.recipe = obj
                found = True

    def allRecipes(self):
        return self.recipes

    def getRecipe(self):
        return self.recipe

    def __del__(self):
        try:
            del sys.modules[self.file]
        except:
            pass

def recipeLoaderFromSourceComponent(component, filename, cfg, repos):
    if not component.endswith(':source'):
        component += ":source"
    name = filename[:-len('.recipe')]

    try:
	version = repos.getTroveLatestVersion(component, cfg.defaultbranch)
        sourceComponent = repos.getTrove(component, cfg.defaultbranch, None)
    except repository.PackageMissing:
        raise RecipeFileError, 'cannot find source component %s' % component

    srcFileInfo = None
    for (fileId, path, version) in repos.iterFilesInTrove(sourceComponent.getName(),
                                                          sourceComponent.getVersion(),
                                                          sourceComponent.getFlavor()):
        if path == filename:
            srcFileInfo = (fileId, version)
            break

    if not srcFileInfo:
        raise RecipeFileError, '%s does not contain %s' % (component, filename)

    fileObj = repos.getFileVersion(fileId, version)
    theFile = repos.pullFileContentsObject(fileObj.contents.sha1())
    (fd, recipeFile) = tempfile.mkstemp(".recipe", 'temp-%s-' %name)

    os.write(fd, theFile.read())
    os.close(fd)

    try:
        loader = RecipeLoader(recipeFile, cfg, repos, component)
    finally:
        os.unlink(recipeFile)
    
    return loader

def loadRecipe(file):
    callerGlobals = inspect.stack()[1][0].f_globals
    cfg = callerGlobals['cfg']
    repos = callerGlobals['repos']
    if file[0] != '/':
        recipepath = os.path.dirname(callerGlobals['filename'])
        localfile = recipepath + '/' + file
    try:
        loader = RecipeLoader(localfile)
    except IOError, err:
        if err.errno == errno.ENOENT:
            loader = recipeLoaderFromSourceComponent(callerGlobals['component'],
                                                     file,
                                                     cfg,
                                                     repos)
    for name, recipe in loader.allRecipes().items():
        # hide all recipes from RecipeLoader - we don't want to return
        # a recipe that has been loaded by loadRecipe
        recipe.ignore = 1
        callerGlobals[name] = recipe
    # stash a reference to the module in the namespace
    # of the recipe that loaded it, or else it will be destroyed
    callerGlobals[os.path.basename(file).replace('.', '-')] = loader

class _sourceHelper:
    def __init__(self, theclass, recipe):
        self.theclass = theclass
	self.recipe = recipe
    def __call__(self, *args, **keywords):
        self.recipe._sources.append(self.theclass(self.recipe, *args, **keywords))

class _recipeHelper:
    def __init__(self, list, theclass):
        self.list = list
        self.theclass = theclass
    def __call__(self, *args, **keywords):
        self.list.append(self.theclass(*args, **keywords))

class _policyUpdater:
    def __init__(self, theobject):
        self.theobject = theobject
    def __call__(self, *args, **keywords):
	self.theobject.updateArgs(*args, **keywords)

class Recipe:
    """Virtual base class for all Recipes"""
    def __init__(self):
        assert(self.__class__ is not Recipe)

class PackageRecipe(Recipe):
    buildRequires = []
    runRequires = []

    def mainDir(self, new = None):
	if new:
	    self.theMainDir = new % self.macros
	return self.theMainDir

    def nameVer(self):
	return os.sep.join((self.name, self.version))

    def cleanup(self, builddir, destdir):
	util.rmtree(builddir)
	util.rmtree(destdir)

    def fetchAllSources(self):
	"""
	returns a list of file locations for all the sources in
	the package recipe
	"""
	files = []
	for src in self._sources:
	    f = src.fetch()
	    if f:
		files.append(f)
	return files

    def unpackSources(self, builddir):
	self.macros.maindir = self.theMainDir
	if os.path.exists(builddir):
	    shutil.rmtree(builddir)
	util.mkdirChain(builddir)
	for source in self._sources:
	    source.unpack(builddir)

    def extraBuild(self, action):
        self._build.append(action)

    def doBuild(self, buildpath, root):
        builddir = os.sep.join((buildpath, self.mainDir()))
	self.macros.update({'builddir': builddir,
			    'destdir': root})
	for bld in self._build:
	    bld.doBuild(self)

    def doDestdirProcess(self):
	for post in self.destdirPolicy:
            post.doProcess(self)

    def getPackages(self, fullVersion):
	# policies look at the recipe instance for all information
	self.fullVersion = fullVersion
	for policy in self.packagePolicy:
	    policy.doProcess(self)
        return self.autopkg.getPackages()


    def disableParallelMake(self):
        self.macros.parallelmflags = ''

    def __getattr__(self, name):
	"""
	Allows us to dynamically suck in namespace of other modules
	with modifications.
	 - The public namespace of the build module is accessible,
	   and build objects are created and put on the build list
	   automatically when they are referenced.
	 - The public namespaces of the policy modules are accessible;
	   policy objects already on their respective lists are returned,
	   policy objects not on their respective lists are added to
	   the end of their respective lists like build objects are
	   added to the build list.
	"""
        if not name.startswith('_'):
	    if name.startswith('add'):
		return _sourceHelper(source.__dict__[name[3:]], self)
	    if name in build.__dict__:
		return _recipeHelper(self._build, build.__dict__[name])
	    for (policy, list) in (
		(destdirpolicy, self.destdirPolicy),
		(packagepolicy, self.packagePolicy)):
		if name in policy.__dict__:
		    policyClass = policy.__dict__[name]
		    for policyObj in list:
			if isinstance(policyObj, policyClass):
			    return _policyUpdater(policyObj)
		    return _recipeHelper(list, policyClass)
        return self.__dict__[name]

    def __delattr__(self, name):
	"""
	Allows us to delete policy items from their respective lists
	by deleting a name in the recipe self namespace.  For example,
	to remove the EtcConfig package policy from the package policy
	list, one could do::
	 del self.EtcConfig
	This would prevent the EtcConfig package policy from being
	executed.  The policy objects are carefully ordered in the
	default policy lists; deleting a policy object and then
	referencing it again will cause it to show up at the end of
	the list.  Don't do that.

	In general, delete policy only as a last resort; you can
	usually disable policy entirely with the keyword argument::
	 exceptions='.*'
	"""
	for (policy, list) in (
	    (destdirpolicy, self.destdirPolicy),
	    (packagepolicy, self.packagePolicy)):
	    if name in policy.__dict__:
		policyClass = policy.__dict__[name]
		for index in range(len(list)):
		    policyObj = list[index]
		    if isinstance(policyObj, policyClass):
			del list[index]
			return
	del self.__dict__[name]
    
    def __init__(self, cfg, laReposCache, srcdirs, extraMacros={}):
        assert(self.__class__ is not Recipe)
	self._sources = []
	self._build = []
        self.destdirPolicy = destdirpolicy.DefaultPolicy()
        self.packagePolicy = packagepolicy.DefaultPolicy()
        self.cfg = cfg
	self.laReposCache = laReposCache
	self.srcdirs = srcdirs
	self.theMainDir = self.nameVer()
	self.macros = macros.Macros()
	self.macros.update(baseMacros)
	self.macros.name = self.name
	self.macros.version = self.version
	if extraMacros:
	    self.macros.update(extraMacros)

class GroupRecipe(Recipe):

    def addTrove(self, name, versionStr = None):
	try:
	    pkgList = self.repos.findTrove(self.label, name, versionStr)
	except repository.PackageNotFound, e:
	    raise RecipeFileError, str(e)

	versionList = [ x.getVersion() for x in pkgList ]
	self.troveVersions[pkgList[0].getName()] = versionList

    def getTroveList(self):
	return self.troveVersions

    def __init__(self, repos, cfg, branch):
	self.repos = repos
	self.cfg = cfg
	self.troveVersions = {}
	self.label = branch.label()

class FilesetRecipe(Recipe):

    def addFileFromPackage(self, pattern, pkg, recurse, remapList):
	pathMap = {}
	for (fileId, pkgPath, version) in pkg.iterFileList():
	    pathMap[pkgPath] = (fileId, version)

	patternList = util.braceExpand(pattern)
	matches = {}
	for pattern in patternList:
	    if not recurse:
		matchList = [ n for n in pathMap.keys() if 
				    fnmatchcase(n, pattern)]
	    else:
		matchList = []	
		dirCount = pattern.count("/")
		for n in pathMap.iterkeys():
		    i = n.count("/")
		    if i > dirCount:
			dirName = os.sep.join(n.split(os.sep)[:dirCount + 1])
			match = fnmatchcase(dirName, pattern)
		    elif i == dirCount:
			match = fnmatchcase(n, pattern)
		    else:
			match = False

		    if match: matchList.append(n)
			
	    for path in matchList:
		matches[path] = pathMap[path]

	if not matches:
	    return False

	for path in matches.keys():
	    (fileId, version) = matches[path]

	    for (old, new) in remapList:
		if path == old:
		    path = new
		    break
		elif len(path) > len(old) and path.startswith(old) and \
					      path[len(old)] == "/":
		    path = new + path[len(old):]
		    break

	    if self.paths.has_key(path):
		raise RecipeFileError, "%s has been included multiple times" \
			% path

	    self.files[fileId] = (path, version)
	    self.paths[path] = 1

	return True

    def addFile(self, pattern, component, versionStr = None, recurse = True,
		remap = []):
	"""
	Adds files which match pattern from version versionStr of component.
	Pattern is glob-style, with brace expansion. If recurse is set,
	anything below a directory which matches pattern is also included,
	and the directory itself does not have to be part of the package.
	Remap is a list of (oldPath, newPath) tuples. The first oldPath
	which matches the start of a matched pattern is rewritten as
	newPath.
	"""

	if type(remap) == tuple:
	    remap = [ remap ]

	try:
	    pkgList = self.repos.findTrove(self.label, component, versionStr)
	except repository.PackageNotFound, e:
	    raise RecipeFileError, str(e)

	if len(pkgList) == 0:
	    raise RecipeFileError, "no packages match %s" % component
	elif len(pkgList) > 1:
	    raise RecipeFileError, "too many packages match %s" % component

	foundIt = False
	pkg = pkgList[0]
	for sub in package.walkPackageSet(self.repos, pkg):
	    foundIt = foundIt or self.addFileFromPackage(pattern, sub, recurse,
							 remap)

	if not foundIt:
	    raise RecipeFileError, "%s does not exist in version %s of %s" % \
		(pattern, pkg.getVersion().asString(), pkg.getName())
	    
    def iterFileList(self):
	for (fileId, (path, version)) in self.files.iteritems():
	    yield (fileId, path, version)
	    
    def __init__(self, repos, cfg, branch):
	self.repos = repos
	self.cfg = cfg
	self.files = {}
	self.paths = {}
	self.label = branch.label()
	
class RecipeFileError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
	return self.msg

    def __str__(self):
	return repr(self)
