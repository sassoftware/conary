#
# Copyright (c) 2004-2005 Specifix, Inc.
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
#stdlib
import errno
from fnmatch import fnmatchcase
import imp
import inspect
from itertools import izip
import os
import sys
import tempfile
import types

#conary
import build
import buildpackage
from deps import deps
import destdirpolicy
import files
from lib import log
from lib import magic
from lib import util
from local import database
import macros
import packagepolicy
from repository import repository
import source
import use
import updatecmd
import versions



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
    'servicedir'        : '/srv',
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
    'debuglibdir'       : '/usr/lib/debug', # no %(prefix)s or %(lib)s!
    'debugsrcdir'       : '/usr/src/debug', # no %(prefix)s!
    # special component prefixes that the whole system needs to share
    'krbprefix'		: '%(exec_prefix)s/kerberos',
    'x11prefix'		: '%(exec_prefix)s/X11R6',
    # arguments/flags (empty ones are for documentation; non-existant = empty)
    'cc'		: 'gcc',
    'cxx'		: 'g++',
    'cxxflags'          : '',    # cxx specific flags
    'optflags'          : '-O2',
    'dbgflags'          : '-g', # for debuginfo
    'cflags'            : '%(optflags)s %(dbgflags)s', 
    'cppflags'		: '', # just for providing in recipes
    'ldflags'		: '%(dbgflags)s',
    'mflags'		: '', # make flags
    'parallelmflags'    : '',
    'sysroot'		: '',
    'os'		: 'linux',
    'target'		: '%(targetarch)s-unknown-linux',
    'debugedit'         : 'debugedit',
    'strip'             : 'eu-strip', # eu-strip for debuginfo
    'strip-archive'     : 'strip', # eu-strip segfaults on ar
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

def localImport(d, package, modules=()):
    """
    import a package into a non-global context.

    @param d: the context to import the module
    @type d: dict
    @param package: the name of the module to import
    @type package: str
    @param modules: a sequence of modules to import from the package.
    If a 2-tuple is in the sequence, rename the imported module to
    the second value in the tuple.
    @type modules: sequence of strings or tuples, or empty tuple

    Examples of translated import statements::
      from foo import bar as baz:
          localImport(d, "foo", (("bar", "baz"))
      from bar import fred, george:
          localImport(d, "bar", ("fred", "george"))
      import os
          localImport(d, "os")
    """
    m = __import__(package, d, {}, modules)
    if modules:
        for name in modules:
            if type(name) is tuple:
                mod = name[0]
                name = name[1]
            else:
                mod = name
            d[name] = getattr(m, mod)
    else:
        d[package] = m
    # save a reference to the module inside this context, so it won't
    # be garbage collected until the context is deleted.
    l = d.setdefault('__localImportModules', [])
    l.append(m)

def setupRecipeDict(d, filename):
    localImport(d, 'build', ('build', 'action'))
    localImport(d, 'build.recipe', ('PackageRecipe', 'GroupRecipe',
                                    'RedirectRecipe', 'FilesetRecipe',
                                    'loadRecipe'))
    localImport(d, 'lib', ('util',))
    for x in ('os', 're', 'sys', 'stat'):
        localImport(d, x)
    localImport(d, 'build.use', ('Arch', 'Use', ('LocalFlags', 'Flags')))
    d['filename'] = filename

class RecipeLoader:
    def __init__(self, filename, cfg=None, repos=None, component=None,
                 branch=None, ignoreInstalled=False):
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
        self.module.__dict__['branch'] = branch
        self.module.__dict__['name'] = pkgname
        self.module.__dict__['ignoreInstalled'] = ignoreInstalled

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

        use.resetUsed()
        exec code in self.module.__dict__

        # all recipes that could be loaded by loadRecipe are loaded;
        # get rid of our references to cfg and repos
        del self.module.__dict__['cfg']
        del self.module.__dict__['repos']
        del self.module.__dict__['component']
        del self.module.__dict__['branch']
        del self.module.__dict__['name']
        del self.module.__dict__['ignoreInstalled']

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
            if (issubclass(obj, PackageRecipe) 
                        and obj is not PackageRecipe) or \
               (issubclass(obj, RedirectRecipe) 
                        and obj is not RedirectRecipe):
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

def recipeLoaderFromSourceComponent(name, cfg, repos,
                                    versionStr=None, labelPath=None,
                                    ignoreInstalled=False):
    name = name.split(':')[0]
    component = name + ":source"
    filename = name + '.recipe'
    if not labelPath:
	labelPath = cfg.buildLabel
    try:
	pkgs = repos.findTrove(labelPath, 
                               (component, versionStr, deps.DependencySet()))
	if len(pkgs) > 1:
	    raise RecipeFileError("source component %s has multiple versions "
				  "with label %s" %(component,
                                                    cfg.buildLabel.asString()))
        sourceComponent = repos.getTrove(*pkgs[0])
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
        loader = RecipeLoader(recipeFile, cfg, repos, component, 
                              sourceComponent.getVersion().branch(),
                              ignoreInstalled=ignoreInstalled)
    finally:
        os.unlink(recipeFile)
    recipe = loader.getRecipe()
    recipe._trove = sourceComponent.copy()
    return (loader, sourceComponent.getVersion())




def loadRecipe(troveSpec, label=None):
    """
    Load a recipe so that its class/data can be used in another recipe.

    If a complete version is not specified in the trovespec, the version of 
    the recipe to load will be based on what is installed on the system.  
    For example, if C{loadRecipe('foo')} is called, and package C{foo} with
    version C{/bar.org@bar:devel/4.1-1-1} is installed on the system, then
    C{foo:source} with version C{/bar.org@bar:devel/4.1-1} will be loaded.
    The recipe will also be loaded with the installed package's flavor.

    If the package is not installed anywhere on the system, the C{labelPath}
    will be searched without reference to the installed system.  

    @param troveSpec: C{name}I{[}C{=I{version}}I{][}C{[I{flavor}]}I{]}
    specification of the trove to load.  The flavor given will be used
    to find the given recipe and also to set the flavor of the loaded recipe.
    @param label: label string to search for the given recipe in place of 
    using the default C{labelPath}.  
    If not specified, the labels listed in the version in the including 
    recipe will be used as the c{labelPath} to search.
    For example, if called from recipe with version
    C{/conary.specifix.com@spx:devel//shadow/1.0-1-1},
    the default C{labelPath} that would be constructed would be:
    C{[conary.specifix.com@spx:shadow, conary.specifix.com@spx:devel]}
    """

    def _findInstalledVersion(db, labelPath, name, versionStr, flavor):
        """ Specialized search of the installed system along a labelPath, 
            defaulting to searching the whole system if the trove is not
            found along the label path.

            The version and flavor of the first found installed trove is 
            returned, or C{None} if no trove is found.
        """
        # first search on the labelPath.  
        try:
            troves = db.findTrove(labelPath, name, flavor, versionStr)
            if len(troves) > 1:
                raise RuntimeError, (
                                'Multiple troves could match loadRecipe' 
                                ' request %s' % troveSpec)
            if troves:
                return troves[0][1].getSourceVersion(), troves[0][2]
        except repository.TroveNotFound:
            pass
        if labelPath is None:
            return None
        try:
            troves = db.findTrove(None, name, flavor, versionStr)
            if len(troves) > 1:
                raise RuntimeError, (
                                'Multiple troves could match loadRecipe' 
                                ' request for %s' % name)
            if troves:
                return troves[0][1].getSourceVersion(), troves[0][2]
        except repository.TroveNotFound:
            pass
        return None

    callerGlobals = inspect.stack()[1][0].f_globals
    cfg = callerGlobals['cfg']
    repos = callerGlobals['repos']
    branch = callerGlobals['branch']
    ignoreInstalled = callerGlobals['ignoreInstalled']
    parentPackageName = callerGlobals['name']

    oldUsed = use.getUsed()
    name, versionStr, flavor = updatecmd.parseTroveSpec(troveSpec, None)

    if name.endswith('.recipe'):
        file = name
        name = name[:-len('.recipe')]
    else:
        file = name + '.recipe'


    #first check to see if a filename was specified, and if that 
    #recipe actually exists.   
    loader = None
    if not (label or versionStr or flavor):
        if name[0] != '/':
            recipepath = os.path.dirname(callerGlobals['filename'])
            localfile = recipepath + '/' + file
        else:
            localfile = name + '.recipe'

        if os.path.exists(localfile):
            if flavor:
                oldBuildFlavor = cfg.buildFlavor
                cfg.buildFlavor = deps.overrideFlavor(oldBuildFlavor, flavor)
                use.setBuildFlagsFromFlavor(name, cfg.buildFlavor)
            loader = RecipeLoader(localfile, cfg, 
                                  ignoreInstalled=ignoreInstalled)

    if not loader:
        if label:
            labelPath = [versions.Label(label)]
        elif branch:
            # if no labelPath was specified, search backwards through the 
            # labels on the current branch.
            labelPath = [branch.label()]
            while branch.hasParentBranch():
                branch = branch.parentBranch()
                labelPath.append(branch.label())
        else:
            labelPath = None
        if not ignoreInstalled:
            # look on the local system to find a trove that is installed that
            # matches this loadrecipe request.  Use that trove's version
            # and flavor information to grab the source out of the repository
            db = database.Database(cfg.root, cfg.dbPath)
            parts = _findInstalledVersion(db, labelPath, name, 
                                          versionStr, flavor)
            if parts:
                version, flavor = parts
                if (version.isLocalCook() or version.isEmerge() 
                    or version.isLocal()):
                    version = version.getSourceVersion().parentVersion()
                versionStr = version.getSourceVersion().asString()
        if flavor:
            # override the current flavor with the flavor found in the 
            # installed trove (or the troveSpec flavor, if no installed 
            # trove was found.  
            oldBuildFlavor = cfg.buildFlavor
            cfg.buildFlavor = deps.overrideFlavor(oldBuildFlavor, flavor)
            use.setBuildFlagsFromFlavor(name, cfg.buildFlavor)
        loader = recipeLoaderFromSourceComponent(name, cfg, repos, 
                                                 labelPath=labelPath, 
                                                 versionStr=versionStr,
                                             ignoreInstalled=ignoreInstalled)[0]
    if flavor:
        cfg.buildFlavor = oldBuildFlavor
        use.setBuildFlagsFromFlavor(parentPackageName, cfg.buildFlavor)


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

    def sourceMap(self, path):
        basepath = os.path.basename(path)
        if basepath in self.sourcePathMap:
            if basepath == path:
                # we only care about truly different source locations with the
                # same basename
                return
            if basepath in self.pathConflicts:
                self.pathConflicts[basepath].append(path)
            else:
                self.pathConflicts[basepath] = [
                    # previous (first) instance
                    self.sourcePathMap[basepath],
                    # this instance
                    path
                ]
        else:
            self.sourcePathMap[basepath] = path

    def fetchAllSources(self):
	"""
	returns a list of file locations for all the sources in
	the package recipe
	"""
        # first make sure we had no path conflicts:
        if self.pathConflicts:
            errlist = []
            for basepath in self.pathConflicts.keys():
                errlist.extend([x for x in self.pathConflicts[basepath]])
            raise RecipeFileError, '\n'.join(errlist)
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

    def checkBuildRequirements(self, cfg, sourceVersion, ignoreDeps=False):
        """ Checks to see if the build requirements for the recipe 
            are installed
        """
	db = database.Database(cfg.root, cfg.dbPath)
        time = sourceVersion.timeStamps()[-1]
        reqMap = {}
        missingReqs = []
        for buildReq in self.buildRequires:
            (name, versionStr, flavor) = updatecmd.parseTroveSpec(buildReq, 
                                                                     None)
            # XXX move this to use more of db.findTrove's features, instead
            # of hand parsing
            try:
                troves = db.findTrove(None, name)
                troves = db.getTroves(troves)
            except repository.TroveNotFound:
                missingReqs.append(buildReq)
                continue
                break
            versionMatches = []
            for trove in troves:
                if versionStr is None:
                    versionMatches.append(trove) 
                    continue
                if versionStr.find('@') == -1:
                    label = trove.getVersion().branch().label()
                    if versionStr[0] == ':' or versionStr.find(':') == -1:
                        if versionStr[0] == ':':
                            versionStr = versionStr[1:]
                        else:
                            log.warning('Deprecated buildreq format.  Use '
                                        ' foo=:label, not foo=label')
                        if label.getLabel() == versionStr:
                            versionMatches.append(trove)
                        continue
                    if ("%s:%s" % (label.getNamespace(), label.getLabel())\
                                                              == versionStr):
                        versionMatches.append(trove)
                        break
                    continue
                else:
                    raise RecipeFileError("Unsupported buildReq format")
            if not versionMatches:
                missingReqs.append(buildReq)
                continue
            versionMatches.sort(lambda a, b: a.getVersion().__cmp__(b.getVersion()))
            if not flavor:
                reqMap[buildReq] = versionMatches[-1]
                continue
            for trove in reversed(versionMatches):
                troveFlavor = trove.getFlavor()
                if troveFlavor.stronglySatisfies(flavor):
                    reqMap[buildReq] = trove
                    break
            if buildReq not in reqMap:
                missingReqs.append(buildReq)
        if missingReqs:
            if not ignoreDeps:
                raise RuntimeError, ("Could not find the following troves "
                                     "needed to cook this recipe:\n"  
                                     "%s" % '\n'.join(missingReqs))
        self.buildReqMap = reqMap

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
	    for line in lines:
		if ':' in line:
		    begin, end = line.split(':')
		    if begin:
			begin = int(begin)
		    if end:
			end = int(end)
		    resumelist.append([begin, end])
		else:
                    if len(lines) == 1:
                        resumelist.append([int(line), False])
                    else:
                        resumelist.append([int(line), int(line)])
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
        Populate a repository lookaside cache
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
                    self.laReposCache.addFileHash(srcName, srcVersion, pathId,
                        path, fileId, version, fileObj.contents.sha1())

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
	self.macros.update(use.Arch._getMacros())
        # allow for architecture not to be set -- this could happen 
        # when storing the recipe e.g. 
	for key in cfg.macroKeys():
	    self.macros._override(key, cfg['macros.' + key])
	self.macros.name = self.name
	self.macros.version = self.version
        self.packages = { self.name : True }
	if extraMacros:
	    self.macros.update(extraMacros)
	self.mainDir(self.nameVer())
        self.sourcePathMap = {}
        self.pathConflicts = {}

class SingleGroup:

    def addTrove(self, name, versionStr = None, flavor = None, source = None,
                 byDefault = True):
        assert(flavor is None or isinstance(flavor, str))

        if flavor is not None:
            flavor = deps.parseFlavor(flavor)
            if flavor is None:
                raise ValueError, 'invalid flavor'

        self.addTroveList.append((name, versionStr, flavor, source, byDefault))

    def findTroves(self, cfg, repos, labelPath):
        self.size = 0

        validSize = True
        troveList = []
        flavorMap = {}
        findTroveList = []
        for (name, versionStr, flavor, source, byDefault) in self.addTroveList:
            desFlavor = cfg.buildFlavor.copy()
            if flavor is not None:
                desFlavor = deps.overrideFlavor(desFlavor, flavor)
            findTroveList.append((name, versionStr, desFlavor))
            flavorMap[flavor] = desFlavor
        try:
            results = repos.findTroves(labelPath, findTroveList)
        except repository.TroveNotFound, e:
            raise RecipeFileError, str(e)
        for (name, versionStr, flavor, source, byDefault) in self.addTroveList:
            desFlavor = flavorMap[flavor]
            pkgList = results[name, versionStr, desFlavor]
            assert(len(pkgList) == 1)
            troveList.append((pkgList[0], byDefault))
            assert(desFlavor.score(pkgList[0][2]) is not False)

        troves = repos.getTroves([ x[0] for x in troveList ], 
                                      withFiles = False)
        for (((name, v, f), byDefault), trove) in izip(troveList, troves):
            if trove.isRedirect():
                raise RecipeFileError, \
                        "%s is a redirect, which are not allowed in groups" \
                        % name

            l = self.troveVersionFlavors.get(name, [])
            if (v, f) not in l:
                l.append((v,f, byDefault))
            self.troveVersionFlavors[name] = l
            # XXX this code is to deal with troves that existed 
            # before troveInfo was added
            if validSize:
                size = trove.getSize()
                # allow older changesets that are missing size
                # info to be added ( this will make the size
                # invalid, so don't store it)
                if size is not None:
                    self.size += trove.getSize()
                else:
                    validSize = False
        if not validSize:
            self.size = None

    def getRequires(self):
        return self.requires

    def getTroveList(self):
	return self.troveVersionFlavors

    def __init__(self):
        self.addTroveList = []
        self.requires = deps.DependencySet()
	self.troveVersionFlavors = {}

    def Requires(self, requirement):
        self.requires.addDep(deps.TroveDependencies, 
                             deps.Dependency(requirement))

class GroupRecipe(Recipe):
    Flags = use.LocalFlags

    def Requires(self, requirement, groupName = None):
        if requirement[0] == '/':
            raise RecipeFileError, 'file requirements not allowed in groups'
        if groupName is None: groupName = self.name

        self.groups[groupName].Requires(requirement)

    def addTrove(self, name, versionStr = None, flavor = None, source = None,
                 byDefault = True, groupName = None):
        if groupName is None: groupName = self.name
        self.groups[groupName].addTrove(name, versionStr = versionStr,
                                        flavor = flavor, source = source,
                                        byDefault = byDefault)

    def findTroves(self, groupName = None):
        if groupName is None: groupName = self.name
        self.groups[groupName].findTroves(self.cfg, self.repos, 
                                          self.labelPath)

    def getRequires(self, groupName = None):
        if groupName is None: groupName = self.name
        return self.groups[groupName].getRequires()

    def getTroveList(self, groupName = None):
        if groupName is None: groupName = self.name
	return self.groups[groupName].getTroveList()

    def getSize(self, groupName = None):
        if groupName is None: groupName = self.name
        return self.groups[groupName].size

    def setLabelPath(self, *path):
        self.labelPath = [ versions.Label(x) for x in path ]

    def createGroup(self, groupName):
        if self.groups.has_key(groupName):
            raise RecipeFileError, 'group %s was already created' % groupName
        if not groupName.startswith('group-'):
            raise RecipeFileError, 'group names must start with "group-"'
        self.groups[groupName] = SingleGroup()

    def getGroupNames(self):
        return self.groups.keys()

    def __init__(self, repos, cfg, label, flavor, extraMacros={}):
	self.repos = repos
	self.cfg = cfg
	self.labelPath = [ label ]
	self.flavor = flavor
        self.macros = macros.Macros()
        self.macros.update(extraMacros)
        self.groups = {}
        self.groups[self.name] = SingleGroup()

class RedirectRecipe(Recipe):
    Flags = use.LocalFlags

    def addRedirect(self, name, versionStr = None, flavorStr = None,
                    fromTrove = None):
        if flavorStr is not None:
            flavor = deps.parseFlavor(flavorStr)
            if flavor is None:
                raise ValueError, 'invalid flavor %s' % flavorStr
        else:
            flavor = None

        if fromTrove is None:
            fromTrove = self.name
        elif fromTrove.find(":") != -1:
            raise ValueError, 'components cannot be individually redirected'

        self.addTroveList.append((name, versionStr, flavor, fromTrove))

    def findTroves(self):
        self.size = 0

        validSize = True
        troveList = []

        packageSet = {}

        for (name, versionStr, flavor, fromName) in self.addTroveList:
            try:
                desFlavor = self.cfg.buildFlavor.copy()
                if flavor is not None:
                    desFlavor.union(flavor, deps.DEP_MERGE_TYPE_OVERRIDE)
                pkgList = self.repos.findTrove(self.label, 
                                               (name, versionStr, desFlavor))
            except repository.TroveNotFound, e:
                raise RecipeFileError, str(e)

            assert(len(pkgList) == 1)
            packageSet[pkgList[0]] = fromName
            troveList.append(pkgList[0])

        troves = self.repos.getTroves(troveList, withFiles = False)
        redirections = {}
        for topLevelTrove in troves:
            topName = topLevelTrove.getName()
            topVersion = topLevelTrove.getVersion()
            topFlavor = topLevelTrove.getFlavor()
            fromName = packageSet[(topName, topVersion, topFlavor)]

            d = self.redirections.setdefault(fromName, {})

            # this redirects from oldPackage -> newPackage
            d[(topName, topVersion, topFlavor)] = True

            for (name, version, flavor) in topLevelTrove.iterTroveList():
                # redirect from oldPackage -> referencedPackage
                d[(name, version, flavor)] = True

                if name.find(":") != -1:
                    compName = fromName + ":" + name.split(":")[1]
                    # redirect from oldPackage -> oldPackage:component. we
                    # leave version/flavor alone; they get filled in later
                    d[(compName, None, None)] = True

                    # redirect from oldPackage:component -> newPackage:component
                    d2 = self.redirections.setdefault(compName, {})
                    d2[(name, version, flavor)] = True

        for name,d  in redirections.iteritems():
            self.redirections[name] = [ (x[0], x[1], x[2]) for x in d ]

    def getRedirections(self):
	return self.redirections

    def __init__(self, repos, cfg, label, flavor, extraMacros={}):
	self.repos = repos
	self.cfg = cfg
        self.redirections = {}
	self.label = label
	self.flavor = flavor
        self.addTroveList = []
        self.macros = macros.Macros()
        self.macros.update(extraMacros)


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
	    pkgList = self.repos.findTrove(self.label, 
                                           (component, versionStr, self.flavor))
	except repository.TroveNotFound, e:
	    raise RecipeFileError, str(e)

	if len(pkgList) == 0:
	    raise RecipeFileError, "no packages match %s" % component
	elif len(pkgList) > 1:
	    raise RecipeFileError, "too many packages match %s" % component

	foundIt = False
	pkg = self.repos.getTrove(*pkgList[0])
	for sub in self.repos.walkTroveSet(pkg):
	    foundIt = foundIt or self.addFileFromPackage(pattern, sub, recurse,
							 remap)

	if not foundIt:
	    raise RecipeFileError, "%s does not exist in version %s of %s" % \
		(pattern, pkg.getVersion().asString(), pkg.getName())
	    
    def iterFileList(self):
	for (pathId, (path, fileId, version)) in self.files.iteritems():
	    yield (pathId, path, fileId, version)
	    
    def __init__(self, repos, cfg, label, flavor, extraMacros={}):
	self.repos = repos
	self.cfg = cfg
	self.files = {}
	self.paths = {}
	self.label = label
	self.flavor = flavor
        self.macros = macros.Macros()
        self.macros.update(extraMacros)
	
class RecipeFileError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
	return self.msg

    def __str__(self):
	return repr(self)
