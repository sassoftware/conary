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
Contains the base Recipe class, default macros, and miscellaneous
components used by conary .recipe files
"""

import build
import buildpackage
import destdirpolicy
import errno
import files
import helper
import imp
import inspect
import log
import macros
import magic
import os
import packagepolicy
from repository import repository
import shutil
import source
import sys
import tempfile
import trove
import types
import use
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
    'tagdescriptiondir' : '%(sysconfdir)s/conary/tags',
    'taghandlerdir'     : '%(libexecdir)s/conary/tags',
    'testdir'	        : '%(localstatedir)s/conary/tests',
    'thistestdir'	: '%(testdir)s/%(name)s-%(version)s',
    # special component prefixes that the whole system needs to share
    'krbprefix'		: '%(exec_prefix)s/kerberos',
    'x11prefix'		: '%(exec_prefix)s/X11R6',
    # arguments/flags (empty ones are for documentation; non-existant = empty)
    'cc'		: 'gcc',
    'cxx'		: 'g++',
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
    exec 'from build import action' in d
    exec 'from build.recipe import PackageRecipe' in d
    exec 'from build.recipe import GroupRecipe' in d
    exec 'from build.recipe import FilesetRecipe' in d
    exec 'from build.recipe import loadRecipe' in d
    exec 'import os, re, sys, stat, util' in d
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

def recipeLoaderFromSourceComponent(component, filename, cfg, repos,
                                    versionStr = None):
    if not component.endswith(':source'):
        component += ":source"
    name = filename[:-len('.recipe')]

    try:
	pkgs = repos.findTrove(cfg.buildLabel, component, None, versionStr)
	if len(pkgs) > 1:
	    raise RecipeFileError("source component %s has multiple versions "
				  "with label %s", component,
				  cfg.buildLabel.asString())
	sourceComponent = pkgs[0]
    except repository.TroveMissing:
        raise RecipeFileError, 'cannot find source component %s' % component

    (fd, recipeFile) = tempfile.mkstemp(".recipe", 'temp-%s-' %name)
    outF = os.fdopen(fd, "w")

    inF = None
    for (fileId, filePath, fileVersion) in sourceComponent.iterFileList():
	if filePath == filename:
	    inF = repos.getFileContents(sourceComponent.getName(),
					sourceComponent.getVersion(),
					sourceComponent.getFlavor(), filename,
					fileVersion).get()
	    break
    
    if not inF:
	raise RecipeFileError("version %s of %s does not contain %s" %
		  (sourceComponent.getName(), sourceComponent.getVersion(),
	 	  filename))

    util.copyfileobj(inF, outF)

    del inF
    del outF

    try:
        loader = RecipeLoader(recipeFile, cfg, repos, component)
    finally:
        os.unlink(recipeFile)
    recipe = loader.getRecipe()
    recipe._trove = sourceComponent.copy()
    return (loader, sourceComponent.getVersion())

def loadRecipe(file, sourcecomponent=None):
    callerGlobals = inspect.stack()[1][0].f_globals
    cfg = callerGlobals['cfg']
    repos = callerGlobals['repos']
    if sourcecomponent and not sourcecomponent.endswith(':source'):
	sourcecomponent = sourcecomponent + ':source'
	# XXX the sourcecomponent argument should go away
	# and always pull by file name
    else:
	sourcecomponent = file.split('.')[0] + ':source'
    if file[0] != '/':
        recipepath = os.path.dirname(callerGlobals['filename'])
        localfile = recipepath + '/' + file
    try:
        loader = RecipeLoader(localfile)
    except IOError, err:
        if err.errno == errno.ENOENT:
            loader = recipeLoaderFromSourceComponent(sourcecomponent, file, 
						     cfg, repos)[0]

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
    def __init__(self, list, recipe, theclass):
        self.list = list
        self.theclass = theclass
	self.recipe = recipe
    def __call__(self, *args, **keywords):
        self.list.append(self.theclass(self.recipe, *args, **keywords))

class _policyUpdater:
    def __init__(self, theobject):
        self.theobject = theobject
    def __call__(self, *args, **keywords):
	self.theobject.updateArgs(*args, **keywords)

class Recipe:
    """Virtual base class for all Recipes"""
    _trove = None

    def __init__(self):
        assert(self.__class__ is not Recipe)

class PackageRecipe(Recipe):
    buildRequires = []
    Flags = use.LocalFlags
    
    def mainDir(self, new = None):
	if new:
	    self.theMainDir = new % self.macros
	    self.macros.maindir = self.theMainDir
	return self.theMainDir

    def nameVer(self):
	return '-'.join((self.name, self.version))

    def cleanup(self, builddir, destdir):
	if 'noClean' in self.cfg.__dict__ and self.cfg.noClean:
	    pass
	else:
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

    def extraSource(self, action):
        self._sources.append(action)


    def unpackSources(self, builddir):
	self.macros.builddir = builddir
	for source in self._sources:
	    source.doAction()

    def extraBuild(self, action):
        self._build.append(action)

    def doBuild(self, buildPath, root, resume=None):
        builddir = os.sep.join((buildPath, self.mainDir()))
	self.macros.update({'builddir': builddir,
			    'destdir': root})
	self.magic = magic.magicCache(root)
	if resume == 'policy':
	    return
	if resume:
	    resume = int(resume)
	    log.debug("Resuming on line %d" % resume)
	    for bld in self._build:
		if bld.linenum >= resume:
		    bld.doAction()
	else:
	    for bld in self._build:
		bld.doAction()

    def doDestdirProcess(self):
	for post in self.destdirPolicy:
            post.doProcess(self)

    def getPackages(self):
	# policies look at the recipe instance for all information
	for policy in self.packagePolicy:
	    policy.doProcess(self)
        return self.autopkg.getPackages()


    def disableParallelMake(self):
        self.macros.parallelmflags = ''

    def populateLcache(self):
        """
        Populate a repositoy lookaside cache
        """
        recipeClass = self.__class__ 
        repos = self.laReposCache.repos
        
        # build a list containing this recipe class and any ancestor class
        # from which it descends
        classes = [ recipeClass ]
        bases = list(recipeClass.__bases__)
        while bases:
            parent = bases.pop()
            bases.extend(list(parent.__bases__))
            if issubclass(parent, PackageRecipe):
                classes.append(parent)

        # reverse the class list, this way the files will be found in the
        # youngest descendant first
        classes.reverse()

        # populate the repository source lookaside cache from the :source
        # components
        for rclass in classes:
            if not rclass._trove:
                continue
            srcName = rclass._trove.getName()
            srcVersion = rclass._trove.getVersion()
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
                    self.laReposCache.addFileHash(srcName, srcVersion,
                                                  None, path, version)

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
		return _recipeHelper(self._build, self, build.__dict__[name])
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
	self.macros = macros.Macros()
	self.macros.update(baseMacros)
	self.macros.name = self.name
	self.macros.version = self.version
	if extraMacros:
	    self.macros.update(extraMacros)
	self.mainDir(self.nameVer())

class GroupRecipe(Recipe):

    def addTrove(self, name, versionStr = None):
	try:
	    pkgList = self.repos.findTrove(self.label, name, self.flavor, 
					   versionStr = versionStr)
	except repository.PackageNotFound, e:
	    raise RecipeFileError, str(e)

        l = self.troveVersions.get(name, [])
        l.extend([ x.getVersion() for x in pkgList ])
        self.troveVersions[name] = l

    def getTroveList(self):
	return self.troveVersions

    def __init__(self, repos, cfg, branch, flavor):
	self.repos = repos
	self.cfg = cfg
	self.troveVersions = {}
	self.label = branch.label()
	self.flavor = flavor

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
	and the directory itself does not have to be part of the trove.
	Remap is a list of (oldPath, newPath) tuples. The first oldPath
	which matches the start of a matched pattern is rewritten as
	newPath.
	"""

	if type(remap) == tuple:
	    remap = [ remap ]

	try:
	    pkgList = self.repos.findTrove(self.label, component, self.flavor,
					   versionStr = versionStr)
	except repository.PackageNotFound, e:
	    raise RecipeFileError, str(e)

	if len(pkgList) == 0:
	    raise RecipeFileError, "no packages match %s" % component
	elif len(pkgList) > 1:
	    raise RecipeFileError, "too many packages match %s" % component

	foundIt = False
	pkg = pkgList[0]
	for sub in self.repos.walkTroveSet(pkg):
	    foundIt = foundIt or self.addFileFromPackage(pattern, sub, recurse,
							 remap)

	if not foundIt:
	    raise RecipeFileError, "%s does not exist in version %s of %s" % \
		(pattern, pkg.getVersion().asString(), pkg.getName())
	    
    def iterFileList(self):
	for (fileId, (path, version)) in self.files.iteritems():
	    yield (fileId, path, version)
	    
    def __init__(self, repos, cfg, branch, flavor):
	self.repos = repos
	self.cfg = cfg
	self.files = {}
	self.paths = {}
	self.label = branch.label()
	self.flavor = flavor
	
class RecipeFileError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
	return self.msg

    def __str__(self):
	return repr(self)
