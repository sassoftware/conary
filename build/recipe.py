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
import imp
import inspect
from lib import log
import macros
from lib import magic
import os
import packagepolicy
from repository import repository
import source
import sys
import tempfile
import types
import use
from lib import util
import versions
from deps import deps

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
    'servicedir'        : '/var',
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
    'tagdatadir'        : '%(datadir)s/conary/tags',
    'testdir'	        : '%(localstatedir)s/conary/tests',
    'thistestdir'	: '%(testdir)s/%(name)s-%(version)s',
    # special component prefixes that the whole system needs to share
    'krbprefix'		: '%(exec_prefix)s/kerberos',
    'x11prefix'		: '%(exec_prefix)s/X11R6',
    # arguments/flags (empty ones are for documentation; non-existant = empty)
    'cc'		: 'gcc',
    'cxx'		: 'g++',
    'cxxflags'          : '',    # cxx specific flags
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
    'buildbranch'       : '',
    'buildlabel'        : '',
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
    exec 'from lib import util' in d
    exec 'import os, re, sys, stat' in d
    exec 'from build.use import Use, Arch' in d
    exec 'from build.use import LocalFlags as Flags' in d
    d['filename'] = filename

class RecipeLoader:
    def __init__(self, filename, cfg=None, repos=None, component=None):
        self.recipes = {}
        
        if filename[0] != "/":
            raise IOError, "recipe file names must be absolute paths"

        if component:
            pkgname = component.split(':')[0]
        else:
            pkgname = filename.split('/')[-1]
            pkgname = pkgname[:-len('.recipe')]
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
        # We need to track Use flags that might be mentioned only
        # outside of the setup() function.  
        if cfg is not None:
            use.overrideFlags(cfg, pkgname)

        # LocalFlags must be thawed when loading a recipe -- the recipe
        # may try to set the value
        use.LocalFlags._thaw()

        use.resetUsed()
        exec code in self.module.__dict__
        if cfg is not None:
            use.clearOverrides(cfg, pkgname)

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
            if issubclass(obj, PackageRecipe) and obj is not PackageRecipe:
                if recipename.startswith('group-'):
                    raise RecipeFileError(
                        'Error in recipe file "%s": package name cannot '
                        'begin with "group-"' %basename)
                if recipename.startswith('fileset-'):
                    raise RecipeFileError(
                        'Error in recipe file "%s": package name cannot '
                        'begin with "fileset-"' %basename)
	    elif issubclass(obj, GroupRecipe) and obj is not GroupRecipe:
                if recipename and not recipename.startswith('group-'):
                    raise RecipeFileError(
                        'Error in recipe file "%s": group name must '
                        'begin with "group-"' %basename)
	    elif issubclass(obj, FilesetRecipe) and obj is not FilesetRecipe:
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
                if '-' in obj.version:
                    raise RecipeFileError(
                        "Version string %s has illegal '-' character"
                        %obj.version)
                if obj.name != pkgname:
                    raise RecipeFileError(
                        "Recipe object name '%s' does not match "
                        "file/component name '%s'"
                        % (obj.name, pkgname))
                found = True
            else:
                raise RecipeFileError(
                    "Recipe in file/component '%s' did not contain both a name"
                    " and a version attribute." % pkgname)
        # inherit any tracked flags that we found while loading parent
        # classes
        if found:
            if self.recipe._trackedFlags is not None:
                use.setUsed(self.recipe._trackedFlags)
            
            # add in the tracked flags that we found while loading this
            # class
            self.recipe._trackedFlags = use.getUsed()
        else:
            # we'll get this if the recipe file is empty 
            raise RecipeFileError(
                "file/component '%s' did not contain a valid recipe" % pkgname)

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
                                    versionStr=None, label=None):
    if not component.endswith(':source'):
        component += ":source"
    name = filename[:-len('.recipe')]
    if not label:
	label = cfg.buildLabel
    else:
	if type(label) == str:
            if '@' not in label:
                # copy namespace and branchname
                l = cfg.buildLabel
                label = versions.Label('%s@%s:%s' %(label, l.getNamespace(), l.getLabel()))
            else:
                label = versions.Label(label)

    try:
	pkgs = repos.findTrove(label, component, deps.DependencySet(), 
                               versionStr)
	if len(pkgs) > 1:
	    raise RecipeFileError("source component %s has multiple versions "
				  "with label %s" %(component,
                                                    cfg.buildLabel.asString()))
	sourceComponent = pkgs[0]
    except repository.TroveMissing:
        raise RecipeFileError, 'cannot find source component %s' % component

    (fd, recipeFile) = tempfile.mkstemp(".recipe", 'temp-%s-' %name)
    outF = os.fdopen(fd, "w")

    inF = None
    for (pathId, filePath, fileId, fileVersion) in sourceComponent.iterFileList():
	if filePath == filename:
	    inF = repos.getFileContents([ (fileId, fileVersion) ])[0].get()
	    break
    
    if not inF:
	raise RecipeFileError("version %s of %s does not contain %s" %
		  (sourceComponent.getName(), 
                   sourceComponent.getVersion().asString(),
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

def loadRecipe(file, sourcecomponent=None, label=None):
    oldUsed = use.getUsed()

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
        loader = RecipeLoader(localfile, cfg)
    except IOError, err:
        if err.errno == errno.ENOENT:
            loader = recipeLoaderFromSourceComponent(sourcecomponent, file, 
						     cfg, repos, label=label)[0]

    for name, recipe in loader.allRecipes().items():
        # hide all recipes from RecipeLoader - we don't want to return
        # a recipe that has been loaded by loadRecipe
        recipe.ignore = 1
        callerGlobals[name] = recipe
    # stash a reference to the module in the namespace
    # of the recipe that loaded it, or else it will be destroyed
    callerGlobals[os.path.basename(file).replace('.', '-')] = loader

    # return the tracked flags to their state before loading this recipe
    use.resetUsed()
    use.setUsed(oldUsed)


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
    _trackedFlags = None

    def __init__(self):
        assert(self.__class__ is not Recipe)

    def __repr__(self):
        return "<%s Object>" % self.__class__

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

    def fetchAllSources(self):
	"""
	returns a list of file locations for all the sources in
	the package recipe
	"""
	self.prepSources()
	files = []
	for src in self._sources:
	    f = src.fetch()
	    if f:
		if type(f) in (tuple, list):
		    files.extend(f)
		else:
		    files.append(f)
	return files

    def extraSource(self, action):
	"""
	extraSource allows you to append a source list item that is
	not a part of source.py.  Be aware when writing these source
	list items that you are writing conary internals!  In particular,
	anything that needs to add a source file to the repository will
	need to implement fetch(), and all source files will have to be
	sought using the lookaside cache.
	"""
        self._sources.append(action)


    def prepSources(self):
	for source in self._sources:
	    source.doPrep()

    def processResumeList(self, resume):
	resumelist = []
	if resume:
	    lines = resume.split(',')
	    for lines in resume.split(','):
		if ':' in lines:
		    begin, end = lines.split(':')
		    if begin:
			begin = int(begin)
		    if end:
			end = int(end)
		    resumelist.append([begin, end])
		else:
		    resumelist.append([int(lines), int(lines)])
	    if len(resumelist) == 1 and resumelist[0][0] == resumelist[0][1]:
		resumelist[0][1] = False
	self.resumeList = resumelist

    def iterResumeList(self, actions):
	resume = self.resumeList
	resumeBegin = resume[0][0]
	resumeEnd = resume[0][1]
	for action in actions:
	    if not resumeBegin or action.linenum >= resumeBegin:
		if not resumeEnd or action.linenum <= resumeEnd:
		    yield action
		elif resumeEnd:
		    resume = resume[1:]
		    if not resume:
			return
		    resumeBegin = resume[0][0]
		    resumeEnd = resume[0][1]
		    if action.linenum == resumeBegin:
			yield action

    def unpackSources(self, builddir, destdir, resume=None):
	self.macros.builddir = builddir
	self.macros.destdir = destdir

	if resume == 'policy':
	    return
	elif resume:
	    log.debug("Resuming on line(s) %s" % resume)
	    # note resume lines must be in order
	    self.processResumeList(resume)
	    for source in self.iterResumeList(self._sources):
		source.doPrep()
		source.doAction()
	else:
	    for source in self._sources:
		source.doPrep()
		source.doAction()

    def extraBuild(self, action):
	"""
	extraBuild allows you to append a build list item that is
	not a part of build.py.  Be aware when writing these build
	list items that you are writing conary internals!
	"""
        self._build.append(action)

    def doBuild(self, buildPath, resume=None):
        builddir = os.sep.join((buildPath, self.mainDir()))
        self.macros.builddir = builddir
        self.magic = magic.magicCache(self.macros.destdir)
        if resume == 'policy':
            return
        if resume:
            for bld in self.iterResumeList(self._build):
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

    def getUnpackagedComponentNames(self):
        # someday, this will probably be per-branch policy
        return ('test', 'debuginfo')


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
            for f in repos.iterFilesInTrove(srcName, srcVersion, 
                                            deps.DependencySet(),
                                            withFiles=True):
                pathId, path, fileId, version, fileObj = f
                assert(path[0] != "/")
                # we might need to retrieve this source file
                # to enable a build, so we need to find the
                # sha1 hash of it since that's how it's indexed
                # in the file store
                if isinstance(fileObj, files.RegularFile):
                    # it only makes sense to fetch regular files, skip
                    # anything that isn't
                    self.laReposCache.addFileHash(srcName, srcVersion,
                                                  None, pathId, path, 
                                                  fileId, version)

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
		    return _recipeHelper(list, self, policyClass)
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
        self.destdirPolicy = destdirpolicy.DefaultPolicy(self)
        self.packagePolicy = packagepolicy.DefaultPolicy(self)
        self.cfg = cfg
	self.laReposCache = laReposCache
	self.srcdirs = srcdirs
	self.macros = macros.Macros()
	self.macros.update(baseMacros)
	for key in cfg.macroKeys():
	    self.macros._override(key, cfg['macros.' + key])
	self.macros.name = self.name
	self.macros.version = self.version
        self.packages = { self.name : True }
	if extraMacros:
	    self.macros.update(extraMacros)
	self.mainDir(self.nameVer())

class GroupRecipe(Recipe):
    Flags = use.LocalFlags

    def addTrove(self, name, versionStr = None, flavor = None, source = None):
        # XXX we likely should not accept multiple types
        # of flavors, it's not a good API
        if isinstance(flavor, deps.DependencySet) or flavor is None:
            # nothing needs to be done
            pass
        elif isinstance(flavor, use.Flag):
            flavor = flavor.asSet()
        else:
            raise ValueError, 'invalid flavor'
        self.addTroveList.append((name, versionStr, flavor, source))

    def findTroves(self):
        for (name, versionStr, flavor, source) in self.addTroveList:
            try:
                desFlavor = self.cfg.buildFlavor.copy()
                if flavor is not None:
                    # specified flavor overrides the default build flavor
                    if isinstance(flavor, use.Flag):
                        flavor = flavor.toDependency()
                    # XXX we likely should not accept multiple types
                    # of flavors, it's not a good API
                    elif (isinstance(flavor, deps.DependencySet) or
                          flavor is None):
                        # nothing needs to be done
                        pass
                    else:
                        raise AssertionError
                    desFlavor.union(flavor, deps.DEP_MERGE_TYPE_OVERRIDE)
                sys.stderr.write('findtrove: %s %s %s\n' %(name, desFlavor, versionStr))
                sys.stderr.flush()
                pkgList = self.repos.findTrove(self.label, name, desFlavor,
                                               versionStr = versionStr)
            except repository.TroveNotFound, e:
                raise RecipeFileError, str(e)
            for trove in pkgList:
                v = trove.getVersion()
                f = trove.getFlavor()
                l = self.troveVersionFlavors.get(name, [])
                if (v, f) not in l:
                    l.append((v,f))
                self.troveVersionFlavors[name] = l

    def getTroveList(self):
	return self.troveVersionFlavors

    def __init__(self, repos, cfg, branch, flavor):
	self.repos = repos
	self.cfg = cfg
	self.troveVersionFlavors = {}
	self.label = branch.label()
	self.flavor = flavor
        self.addTroveList = []

class FilesetRecipe(Recipe):
    # XXX need to work on adding files from different flavors of troves
    def addFileFromPackage(self, pattern, pkg, recurse, remapList):
	pathMap = {}
	for (pathId, pkgPath, fileId, version) in pkg.iterFileList():
	    pathMap[pkgPath] = (pathId, fileId, version)

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
	    (pathId, fileId, version) = matches[path]

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

	    self.files[pathId] = (path, fileId, version)
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
	except repository.TroveNotFound, e:
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
	for (pathId, (path, fileId, version)) in self.files.iteritems():
	    yield (pathId, path, fileId, version)
	    
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
