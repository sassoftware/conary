#
# Copyright (c) 2004-2005 rPath, Inc.
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
from itertools import chain,izip
import new
import os
import string
import sys
import tempfile
import types

#conary
import build
import buildpackage
import usergroup
import conaryclient
import cook
from deps import deps
import destdirpolicy
import files
from lib import log
from lib import magic
from lib import util
from local import database
import macros
import packagepolicy
from repository import repository,trovesource
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
    'userinfodir'       : '%(sysconfdir)s/conary/userinfo',
    'groupinfodir'      : '%(sysconfdir)s/conary/groupinfo',
    'buildlogpath'      : '%(debugsrcdir)s/buildlogs/%(name)s-%(version)s-log.bz2',
    # special component prefixes that the whole system needs to share
    'krbprefix'		: '%(exec_prefix)s/kerberos',
    'x11prefix'		: '%(exec_prefix)s/X11R6',
    # programs/options (empty ones are for documentation)
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
    'strip'             : 'eu-strip', # eu-strip for debuginfo, "strip -g" else
    'strip-archive'     : 'strip -g', # eu-strip segfaults on ar
    'monodis'           : '%(bindir)s/monodis',
    # filled in at cook time
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
                                    'DistroPackageRecipe',
                                    'BuildPackageRecipe',
                                    'CPackageRecipe',
                                    'AutoPackageRecipe',
                                    'UserInfoRecipe',
                                    'GroupInfoRecipe',
                                    'loadSuperClass', 'loadInstalled',
                                    'clearBuildReqs',
                                    # XXX when all recipes have been migrated
                                    # we can get rid of loadRecipe
                                    ('loadSuperClass', 'loadRecipe')))
    localImport(d, 'lib', ('util',))
    for x in ('os', 're', 'sys', 'stat'):
        localImport(d, x)
    localImport(d, 'build.use', ('Arch', 'Use', ('LocalFlags', 'Flags')))
    d['filename'] = filename

class RecipeLoader:
    _recipesToCopy = []

    @classmethod
    def addRecipeToCopy(class_, recipeClass):
        class_._recipesToCopy.append(recipeClass)

    def _copyReusedRecipes(self, moduleDict):
        # XXX HACK - get rid of this when we move the
        # recipe classes to the repository.
        # makes copies of some of the superclass recipes that are 
        # created in this module.  (specifically, the ones with buildreqs)
        for recipeClass in self._recipesToCopy:
            name = recipeClass.__name__
            # when we create a new class object, it needs its superclasses.
            # get the original superclass list and substitute in any 
            # copies
            mro = list(inspect.getmro(recipeClass)[1:])
            newMro = []
            for superClass in mro:
                superName = superClass.__name__
                newMro.append(moduleDict.get(superName, superClass))

            recipeCopy = new.classobj(name, tuple(newMro),
                                     recipeClass.__dict__.copy())
            recipeCopy.buildRequires = recipeCopy.buildRequires[:]
            moduleDict[name] = recipeCopy



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
        self.module.__dict__['loadedTroves'] = []
        self.module.__dict__['loadedSpecs'] = {}

        self._copyReusedRecipes(self.module.__dict__)

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
            if ((issubclass(obj, PackageRecipe)
                 and obj is not PackageRecipe
                 and not issubclass(obj, UserGroupInfoRecipe)) or
                (issubclass(obj, RedirectRecipe) 
                 and obj is not RedirectRecipe)):
                if recipename[0] not in string.ascii_letters + string.digits:
                    raise RecipeFileError(
                        'Error in recipe file "%s": package name must start '
                        'with an ascii letter or digit.' %basename)
                if recipename.startswith('group-'):
                    raise RecipeFileError(
                        'Error in recipe file "%s": package name cannot '
                        'begin with "group-"' %basename)
                if recipename.startswith('fileset-'):
                    raise RecipeFileError(
                        'Error in recipe file "%s": package name cannot '
                        'begin with "fileset-"' %basename)
                if recipename.startswith('info-'):
                    raise RecipeFileError(
                        'Error in recipe file "%s": package name cannot '
                        'begin with "info-"' %basename)
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
	    elif issubclass(obj, UserGroupInfoRecipe) and obj is not UserGroupInfoRecipe:
                if recipename and not recipename.startswith('info-'):
                    raise RecipeFileError(
                        'Error in recipe file "%s": info name must '
                        'begin with "info-"' %basename)
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
        if found:
            # inherit any tracked flags that we found while loading parent
            # classes.  Also inherit the list of recipes classes needed to load
            # this recipe.
            self.recipe._loadedTroves = self.module.__dict__['loadedTroves']
            self.recipe._loadedSpecs = self.module.__dict__['loadedSpecs']

            if self.recipe._trackedFlags is not None:
                use.setUsed(self.recipe._trackedFlags)
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

def _scoreLoadRecipeChoice(labelPath, version):
    """ These labels all match the given labelPath.
        We score them based on the number of matching labels in 
        the label path, and return the one that's "best".

        The following rules should apply:
        * if the labelPath is [bar, foo] and you are choosing between
          /foo/bar/ and /foo/blah/bar, choose /foo/bar.  Assumption
          is that any other shadow/branch in the path may be from a 
          maintenance branch.
        * if the labelPath is [bar] and you are choosing between
          /foo/bar/ and /foo/blah/bar, choose /foo/bar.
    """
    # FIXME I'm quite sure this heuristic will get replaced with
    # something smarter/more sane as time progresses
    score = 0
    labelPath = [ x for x in reversed(labelPath)]
    branch = version.branch()
    while True:
        label = branch.label()
        try:
            index = labelPath.index(label)
        except ValueError:
            index = -1
        score += index
        if not branch.hasParentBranch():
            break
        branch = branch.parentBranch()
    return score

def getBestLoadRecipeChoices(labelPath, troveTups):
    scores = [ (_scoreLoadRecipeChoice(labelPath, x[1]), x) for x in troveTups ]
    maxScore = max(scores)[0]
    return [x[1] for x in scores if x[0] == maxScore ]



def recipeLoaderFromSourceComponent(name, cfg, repos,
                                    versionStr=None, labelPath=None,
                                    ignoreInstalled=False, 
                                    filterVersions=False):
    name = name.split(':')[0]
    component = name + ":source"
    filename = name + '.recipe'
    if not labelPath:
	labelPath = [cfg.buildLabel]
    try:
	pkgs = repos.findTrove(labelPath, 
                               (component, versionStr, deps.DependencySet()))
    except repository.TroveMissing:
        raise RecipeFileError, 'cannot find source component %s' % component
    if filterVersions:
        pkgs = getBestLoadRecipeChoices(labelPath, pkgs)
    if len(pkgs) > 1:
        raise RecipeFileError("source component %s has multiple versions "
                              "on labelPath %s: %s" %(component,
                            ', '.join(x.asString() for x in labelPath),
                            pkgs))
    sourceComponent = repos.getTrove(*pkgs[0])

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


def loadSuperClass(troveSpec, label=None):
    """
    Load a recipe so that its class/data can be used as a super class for
    this recipe.

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
    C{/conary.rpath.com@rpl:devel//shadow/1.0-1-1},
    the default C{labelPath} that would be constructed would be:
    C{[conary.rpath.com@rpl:shadow, conary.rpath.com@rpl:devel]}
    """
    callerGlobals = inspect.stack()[1][0].f_globals
    ignoreInstalled = True
    _loadRecipe(troveSpec, label, callerGlobals, False)

def loadInstalled(troveSpec, label=None):
    """
    Load a recipe so that its data about the installed system can be used 
    in this recipe.

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
    C{/conary.rpath.com@rpl:devel//shadow/1.0-1-1},
    the default C{labelPath} that would be constructed would be:
    C{[conary.rpath.com@rpl:shadow, conary.rpath.com@rpl:devel]}
    """

    callerGlobals = inspect.stack()[1][0].f_globals
    _loadRecipe(troveSpec, label, callerGlobals, True)


def _loadRecipe(troveSpec, label, callerGlobals, findInstalled):
    """ See docs for loadInstalledPackage and loadSuperClass.  """

    def _findInstalledVersion(db, labelPath, name, versionStr, flavor):
        """ Specialized search of the installed system along a labelPath, 
            defaulting to searching the whole system if the trove is not
            found along the label path.

            The version and flavor of the first found installed trove is 
            returned, or C{None} if no trove is found.
        """
        # first search on the labelPath.  
        try:
            troves = db.findTrove(labelPath, (name, versionStr, flavor))
            if len(troves) > 1:
                raise RuntimeError, (
                                'Multiple troves could match loadInstalled' 
                                ' request %s' % troveSpec)
            if troves:
                return troves[0][1].getSourceVersion(), troves[0][2]
        except repository.TroveNotFound:
            pass
        if labelPath is None:
            return None
        try:
            troves = db.findTrove(None, (name, versionStr, flavor))
            if len(troves) > 1:
                raise RuntimeError, (
                                'Multiple troves could match loadRecipe' 
                                ' request for %s' % name)
            if troves:
                return troves[0][1].getSourceVersion(), troves[0][2]
        except repository.TroveNotFound:
            pass
        return None


    cfg = callerGlobals['cfg']
    repos = callerGlobals['repos']
    branch = callerGlobals['branch']
    parentPackageName = callerGlobals['name']
    if 'ignoreInstalled' in callerGlobals:
        alwaysIgnoreInstalled = callerGlobals['ignoreInstalled']
    else:
        alwaysIgnoreInstalled = False

    oldUsed = use.getUsed()
    name, versionStr, flavor = updatecmd.parseTroveSpec(troveSpec)

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
            parentFilePath = callerGlobals['filename']
            localfile = os.path.dirname(parentFilePath) + '/' + file
        else:
            localfile = name + '.recipe'

        if os.path.exists(localfile):
            if flavor:
                oldBuildFlavor = cfg.buildFlavor
                cfg.buildFlavor = deps.overrideFlavor(oldBuildFlavor, flavor)
                use.setBuildFlagsFromFlavor(name, cfg.buildFlavor)
            loader = RecipeLoader(localfile, cfg, 
                                  ignoreInstalled=alwaysIgnoreInstalled)

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
        if findInstalled and not alwaysIgnoreInstalled:
            # look on the local system to find a trove that is installed that
            # matches this loadrecipe request.  Use that trove's version
            # and flavor information to grab the source out of the repository
            db = database.Database(cfg.root, cfg.dbPath)
            parts = _findInstalledVersion(db, labelPath, name, 
                                          versionStr, flavor)
            if parts:
                version, flavor = parts
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
                                     ignoreInstalled=alwaysIgnoreInstalled,
                                     filterVersions=True)[0]


    for name, recipe in loader.allRecipes().items():
        # hide all recipes from RecipeLoader - we don't want to return
        # a recipe that has been loaded by loadRecipe
        recipe.ignore = 1
        callerGlobals[name] = recipe
        if recipe._trove:
            # create a tuple with the version and flavor information needed to 
            # load this trove again.   You might be able to rely on the
            # flavor that the trove was built with, but when you load a
            # recipe that is not a superclass of the current recipe, 
            # its flavor is not assumed to be relevant to the resulting 
            # package (otherwise you might have completely irrelevant flavors
            # showing up for any package that loads the python recipe, e.g.)
            usedFlavor = use.createFlavor(name, recipe._trackedFlags)
            troveTuple = (recipe._trove.getName(), recipe._trove.getVersion(),
                          usedFlavor)
            callerGlobals['loadedTroves'].extend(recipe._loadedTroves)
            callerGlobals['loadedTroves'].append(troveTuple)
            callerGlobals['loadedSpecs'][troveSpec] = (troveTuple, recipe)
    if flavor:
        cfg.buildFlavor = oldBuildFlavor
        # must set this flavor back after the above use.createFlavor()
        use.setBuildFlagsFromFlavor(parentPackageName, cfg.buildFlavor)

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

def clearBuildReqs(*buildReqs):
    """ Clears inherited build requirement lists of a given set of packages,
        or all packages if none listed. 
    """
    def _removePackages(class_, pkgs):
        if not pkgs:
            class_.buildRequires = []
        else:
            for pkg in pkgs:
                if pkg in class_.buildRequires:
                    class_.buildRequires.remove(pkg)

    callerGlobals = inspect.stack()[1][0].f_globals
    classes = []
    for value in callerGlobals.itervalues():
        if inspect.isclass(value) and issubclass(value, PackageRecipe):
            classes.append(value)

    for class_ in classes:
        _removePackages(class_, buildReqs)

        for base in inspect.getmro(class_):
            if issubclass(base, PackageRecipe):
                _removePackages(base, buildReqs)

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
    _loadedTroves = []
    _loadedSpecs = {}

    def __init__(self):
        assert(self.__class__ is not Recipe)

    @classmethod
    def getLoadedTroves(class_):
        return class_._loadedTroves

    @classmethod
    def getLoadedSpecs(class_):
        return class_._loadedSpecs

    def __repr__(self):
        return "<%s Object>" % self.__class__

class PackageRecipe(Recipe):
    buildRequires = []
    Flags = use.LocalFlags
    explicitMainDir = False
    
    def mainDir(self, new=None, explicit=True):
	if new:
	    self.theMainDir = new % self.macros
	    self.macros.maindir = self.theMainDir
            self.explicitMainDir |= explicit
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

        def _filterBuildReqsByVersionStr(versionStr, troves):
            if not versionStr:
                return troves

            versionMatches = []
            if versionStr.find('@') == -1:
                if versionStr.find(':') == -1:
                    log.warning('Deprecated buildreq format.  Use '
                                ' foo=:tag, not foo=tag')
                    versionStr = ':' + versionStr

            # we don't allow full version strings or just releases
            if versionStr[0] not in ':@':
                raise RecipeFileError("Unsupported buildReq format")


            for trove in troves:
                labels = trove.getVersion().iterLabels()
                if versionStr[0] == ':':
                    branchTag = versionStr[1:]
                    branchTags = [ x.getLabel() for x in labels ] 
                    if branchTag in branchTags:
                        versionMatches.append(trove)
                else:
                    # versionStr must begin with an @
                    branchNames = []
                    for label in labels:
                        branchNames.append('@%s:%s' % (label.getNamespace(),
                                                       label.getLabel()))
                    if versionStr in branchNames:
                        versionMatches.append(trove)
            return versionMatches

        def _filterBuildReqsByFlavor(flavor, troves):
            troves.sort(lambda a, b: a.getVersion().__cmp__(b.getVersion()))
            if not flavor:
                return troves[-1]
            for trove in reversed(versionMatches):
                troveFlavor = trove.getFlavor()
                if troveFlavor.stronglySatisfies(flavor):
                    return trove

	db = database.Database(cfg.root, cfg.dbPath)
        time = sourceVersion.timeStamps()[-1]
        reqMap = {}
        missingReqs = []
        for buildReq in self.buildRequires:
            (name, versionStr, flavor) = updatecmd.parseTroveSpec(buildReq)
            # XXX move this to use more of db.findTrove's features, instead
            # of hand parsing
            try:
                troves = db.trovesByName(name)
                troves = db.getTroves(troves)
            except repository.TroveNotFound:
                missingReqs.append(buildReq)
                continue
                break

            versionMatches =  _filterBuildReqsByVersionStr(versionStr, troves)
                
            if not versionMatches:
                missingReqs.append(buildReq)
                continue
            match = _filterBuildReqsByFlavor(flavor, versionMatches)
            if match:
                reqMap[buildReq] = match
            else:
                missingReqs.append(buildReq)
            
            
        
        if missingReqs:
            if not ignoreDeps:
                log.error("Could not find the following troves "
                          "needed to cook this recipe:\n"  
                          "%s" % '\n'.join(sorted(missingReqs)))
                raise cook.CookError, 'unresolved build dependencies'
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
        return self.autopkg.getComponents()

    def setByDefaultOn(self, includeSet):
        self.byDefaultIncludeSet = includeSet

    def setByDefaultOff(self, excludeSet):
        self.byDefaultExcludeSet = excludeSet

    def byDefault(self, compName):
        c = compName[compName.index(':'):]
        if compName in self.byDefaultIncludeSet:
            # intended for foo:bar overrides :bar in excludelist
            return True
        if compName in self.byDefaultExcludeSet:
            # explicitly excluded
            return False
        if c in self.byDefaultIncludeSet:
            return True
        if c in self.byDefaultExcludeSet:
            return False
        return True

    def disableParallelMake(self):
        self.macros._override('parallelmflags', '')

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

    def isatty(self, value=None):
        if value is not None:
            self._tty = value
        return self._tty

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
        if name in self.__dict__:
            return self.__dict__[name]
        raise AttributeError, name

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

    def _includeSuperClassBuildReqs(self):
        """ Include build requirements from super classes by searching
            up the class hierarchy for buildRequires.  You can only
            override this currenly by calling 
            <superclass>.buildRequires.remove()
        """
        buildReqs = set()
        for base in inspect.getmro(self.__class__):
            buildReqs.update(getattr(base, 'buildRequires', []))
        self.buildRequires = list(buildReqs)
    
    def __init__(self, cfg, laReposCache, srcdirs, extraMacros={}):
        Recipe.__init__(self)
	self._sources = []
	self._build = []

        self._includeSuperClassBuildReqs()
        self.destdirPolicy = destdirpolicy.DefaultPolicy(self)
        self.packagePolicy = packagepolicy.DefaultPolicy(self)
        self.byDefaultIncludeSet = frozenset()
        self.byDefaultExcludeSet = frozenset()
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
	self.mainDir(self.nameVer(), explicit=False)
        self.sourcePathMap = {}
        self.pathConflicts = {}


class UserGroupInfoRecipe(PackageRecipe):
    # abstract base class
    ignore = 1

    def __init__(self, cfg, laReposCache, srcdirs, extraMacros={}):
        PackageRecipe.__init__(self, cfg, laReposCache, srcdirs, extraMacros)
        self.destdirPolicy = []
        self.packagePolicy = []
        self.requires = []
        self.infofilename = None
        self.realfilename = None

    def getPackages(self):
        comp = buildpackage.BuildComponent(
            'info-%s:%s' %(self.infoname, self.type), self)
        f = comp.addFile(self.infofilename, self.realfilename)
        f.tags.set("%s-info" %self.type)
        self.addProvides(f)
        self.addRequires(f)
        comp.provides.union(f.provides())
        comp.requires.union(f.requires())
        return [comp]

    def addProvides(self, f):
        pass

    def addRequires(self, f):
        if not self.requires:
            return
        depSet = deps.DependencySet()
        for info, type in self.requires:
            if type == 'user':
                depClass = deps.UserInfoDependencies
            else:
                depClass = deps.GroupInfoDependencies
            depSet.addDep(depClass, deps.Dependency(info, []))
        f.requires.set(depSet)

    def requiresUser(self, user):
        self.requires.append((user, 'user'))

    def requiresGroup(self, group):
        self.requires.append((group, 'group'))

    def __getattr__(self, name):
        if not name.startswith('_'):
	    if name in usergroup.__dict__:
		return _recipeHelper(self._build, self,
                                     usergroup.__dict__[name])
        if name in self.__dict__:
            return self.__dict__[name]
        raise AttributeError, name

class UserInfoRecipe(UserGroupInfoRecipe):
    type = 'user'
    # abstract base class
    ignore = 1

    def addProvides(self, f):
        depSet = deps.DependencySet()
        depSet.addDep(deps.UserInfoDependencies,
                      deps.Dependency(self.infoname, []))
        depSet.addDep(deps.GroupInfoDependencies,
                      deps.Dependency(self.groupname, []))
        f.provides.set(depSet)

class GroupInfoRecipe(UserGroupInfoRecipe):
    type = 'group'
    # abstract base class
    ignore = 1

    def addProvides(self, f):
        depSet = deps.DependencySet()
        depSet.addDep(deps.GroupInfoDependencies,
                      deps.Dependency(self.infoname, []))
        f.provides.set(depSet)


# XXX the next four classes will probably migrate to the repository
# somehow, but not until we have figured out how to do this without
# requiring that every recipe have a loadSuperClass line in it.

class DistroPackageRecipe(PackageRecipe):
    """
    Most packages in the distribution should descend from this class,
    directly or indirectly, except for direct build requirements of
    this class.  This package differs from the C{PackageRecipe}
    class only by providing explicit build requirements.
    """
    # :lib in here is only for runtime, not to link against.
    # Any package that needs to link should still specify the :devellib
    buildRequires = [
        'filesystem:runtime',
        'setup:runtime',
        'python:runtime',
        'python:lib',
        'conary:runtime',
        'conary:lib',
        'conary:python',
        'sqlite:lib',
        'bzip2:runtime',
        'gzip:runtime',
        'tar:runtime',
        'cpio:runtime',
        'patch:runtime',
    ]
    Flags = use.LocalFlags
    # abstract base class
    ignore = 1

RecipeLoader.addRecipeToCopy(DistroPackageRecipe)

class BuildPackageRecipe(DistroPackageRecipe):
    """
    Packages that need to be built with the make utility and basic standard
    shell tools should descend from this recipe in order to automatically
    have a reasonable set of build requirements.  This package differs
    from the C{PackageRecipe} class only by providing explicit build
    requirements.
    """
    # Again, no :devellib here
    buildRequires = [
        'coreutils:runtime',
        'make:runtime',
        'mktemp:runtime',
        # all the rest of these are for configure
        'findutils:runtime',
        'gawk:runtime',
        'grep:runtime',
        'sed:runtime',
        'diffutils:runtime',
    ]
    buildRequires.extend(DistroPackageRecipe.buildRequires)
    Flags = use.LocalFlags
    # abstract base class
    ignore = 1
RecipeLoader.addRecipeToCopy(BuildPackageRecipe)

class CPackageRecipe(BuildPackageRecipe):
    """
    Most packages should descend from this recipe in order to automatically
    have a reasonable set of build requirements for a package that builds
    C source code to binaries.  This package differs from the
    C{PackageRecipe} class only by providing explicit build requirements.
    """
    buildRequires = [
        'binutils:runtime',
        'binutils:lib',
        'binutils:devellib',
        'gcc:runtime',
        'gcc:lib',
        'gcc:devellib',
        'glibc:runtime',
        'glibc:lib',
        'glibc:devellib',
        'glibc:devel',
        'debugedit:runtime',
    ]
    buildRequires.extend(BuildPackageRecipe.buildRequires)
    Flags = use.LocalFlags
    # abstract base class
    ignore = 1
RecipeLoader.addRecipeToCopy(CPackageRecipe)

class AutoPackageRecipe(CPackageRecipe):
    """
    Recipe class for simple packages built with auto* tools.  Child
    classes should provide the C{unpack()} method for populating the
    source list.  To call policy, implement the C{policy()} method and
    put any necessary policy invocations there.  Next mostly likely is
    to provide a C{makeinstall()} method if C{MakeInstall()} is
    insufficient for the package.  Least likely to need overriding
    are C{configure()} if C{Configure()} is insufficient, and
    C{make()} if C{Make()} is insufficient.
    """
    Flags = use.LocalFlags
    # abstract base class
    ignore = 1

    def setup(r):
        r.unpack()
        r.configure()
        r.make()
        r.makeinstall()
        r.policy()

    def unpack(r):
        pass
    def configure(r):
        r.Configure()
    def make(r):
        r.Make()
    def makeinstall(r):
        r.MakeInstall()
    def policy(r):
        pass
RecipeLoader.addRecipeToCopy(AutoPackageRecipe)


class SingleGroup:

    def addTrove(self, name, versionStr = None, flavor = None, source = None,
                 byDefault = None, ref = None):
        self.addTroveList.append((name, versionStr, flavor, source, 
				  byDefault, ref)) 

    def removeTrove(self, name, versionStr = None, flavor = None):
        self.removeTroveList.append((name, versionStr, flavor))

    def addAllTroves(self, reference, byDefault = None):
        self.addReferenceList.append((reference, byDefault))

    def addNewGroup(self, name, byDefault = None):
	self.newGroupList.append([ name, byDefault ])

    def setByDefault(self, byDefault):
        assert(isinstance(byDefault, bool))
	self.byDefault = byDefault

    def _foundTrove(self, troveTup, size, byDefault, isRedirect):
        self.troves[troveTup] = (size, byDefault)
        if isRedirect:
            # we check later to ensure that all redirects added 
            # by addTrove lines (or other means) are removed
            # by removeTrove lines later.
            self.redirects.add(troveTup)

    def findTroves(self, troveMap, repos):
        self._findTroves(troveMap)
        self._removeTroves(repos)
        self._checkForRedirects()

    def autoResolveDeps(self, cfg, repos, labelPath, includedTroves):
        if self.autoResolve:
            self._resolveDependencies(cfg, repos, labelPath, includedTroves)

    def checkDependencies(self, cfg, includedTroves):
        if self.depCheck:
            failedDeps = self._checkDependencies(cfg, includedTroves)
            if failedDeps:
                return failedDeps

    def calcSize(self):
        self.size = 0
        validSize = True
        for (n,v,f), (size, byDefault) in self.troves.iteritems():
            if size is None:
                validSize = False
                self.size = None
            if validSize:
                self.size += size
            l = self.troveVersionFlavors.setdefault(n,[])
            l.append((v,f,byDefault))

    def _findTroves(self, troveMap):
        """ given a trove map which already contains a dict for all queries
            needed for all groups cooked, pick out those troves that 
            are relevant to this group.
        """
        validSize = True
        self.troves = {}

        for (name, versionStr, flavor, source, byDefault, refSource) \
                                                    in self.addTroveList:
            troveList = troveMap[refSource][name, versionStr, flavor]

            if byDefault is None:
                byDefault = self.byDefault
            
            for (troveTup, size, isRedirect) in troveList:
                self._foundTrove(troveTup, size, byDefault, isRedirect)

        # these are references which were used in addAllTroves() commands
        for refSource, byDefault in self.addReferenceList:
            troveList = refSource.getSourceTroves()
            troveTups = [ x for x in chain(
                                *[x.iterTroveList() for x in troveList])]
            troveList = refSource.getTroves(troveTups, withFiles=False)

            if byDefault is None:
                byDefault = self.byDefault

            for (troveTup, trv) in izip(troveTups, troveList):
                self._foundTrove(troveTup, trv.getSize(), byDefault, 
                                 trv.isRedirect())

    def getDefaultTroves(self):
        return [ x[0] for x in self.troves.iteritems() if x[1][1] ]

    def _resolveDependencies(self, cfg, repos, labelPath, includedTroves):
        """ adds the troves needed to to resolve all open dependencies 
            in this group.  Will raise an error if not all dependencies
            can be resolved.  
        """
        #FIXME: this should probably be able to resolve against
        # other trove source than the repository.

        # set up configuration
        oldDbPath = cfg.dbPath
        cfg.setValue('dbPath', ':memory:')
        oldRoot = cfg.root
        cfg.setValue('root', ':memory:')
        oldInstallLabelPath = cfg.installLabelPath
        resolveLabelPath = labelPath
        cfg.installLabelPath = labelPath
        oldAutoResolve = cfg.autoResolve
        cfg.autoResolve = True
        # set up a conaryclient to do the dep solving
        client = conaryclient.ConaryClient(cfg)

        if self.checkOnlyByDefaultDeps:
            troveList = self.getDefaultTroves() + includedTroves
        else:
            troveList = list(self.troves) + includedTroves
        
        # build a list of the troves that we're checking so far
        troves = [ (n, (None, None), (v, f), True) for (n,v,f) in troveList]

        updJob, suggMap = client.updateChangeSet(troves, recurse = True,
                                                 resolveDeps = True,
                                                 test = True)
        # restore config
        cfg.setValue('dbPath', oldDbPath)
        cfg.setValue('root', oldRoot)
        cfg.installLabelPath = oldInstallLabelPath
        cfg.autoResolve = oldAutoResolve
        neededTups = set(chain(*suggMap.itervalues()))
        troves = repos.getTroves(neededTups, withFiles=False)
        for troveTup, trv in izip(neededTups, troves):
            self._foundTrove(troveTup, trv.getSize(), self.byDefault,
                             trv.isRedirect())

    def _checkDependencies(self, cfg, includedTroves):
        if self.checkOnlyByDefaultDeps:
            troveList = self.getDefaultTroves()
        else:
            troveList = list(self.troves)

        troveList += includedTroves

        troves = [ (n, (None, None), (v, f), True) for (n,v,f) in troveList]

        oldDbPath = cfg.dbPath
        cfg.setValue('dbPath', ':memory:')
        oldRoot = cfg.root
        cfg.setValue('root', ':memory:')

        client = conaryclient.ConaryClient(cfg)
        if self.checkOnlyByDefaultDeps:
            depCs = client.updateChangeSet(troves, recurse = True,
                                            resolveDeps=False, split=False)[0]
            cs = depCs.csList[0]
        else:
            cs = client.repos.createChangeSet(troves, 
                                              recurse = True, withFiles=False)

        failedDeps = client.db.depCheck(cs)[0]
        cfg.setValue('dbPath', oldDbPath)
        cfg.setValue('root', oldRoot)
        return failedDeps

    def _removeTroves(self, source):
        groupSource = trovesource.GroupRecipeSource(source, self)
        groupSource.searchAsDatabase()
        results = groupSource.findTroves(None, self.removeTroveList)
        troveTups = chain(*results.itervalues())
        for troveTup in troveTups:
            del self.troves[troveTup]
            self.redirects.discard(troveTup)

    def _checkForRedirects(self):
        if self.redirects:
            redirects = [('%s=%s[%s]' % (n,v.asString(),deps.formatFlavor(f))) \
                                        for (n,v,f) in sorted(self.redirects)]
            raise RecipeFileError, \
                "found redirects, which are not allowed in groups: \n%s" \
                    % '\n'.join(redirects)

    def getRequires(self):
        return self.requires

    def getTroveList(self):
	return self.troveVersionFlavors

    def getNewGroupList(self):
	return self.newGroupList

    def hasTroves(self):
        return bool(self.newGroupList or self.getTroveList())

    def __init__(self, depCheck, autoResolve, checkOnlyByDefaultDeps,
                 byDefault = True):

        self.redirects = set()
        self.addTroveList = []
        self.addReferenceList = []
        self.removeTroveList = []
        self.newGroupList = []
        self.requires = deps.DependencySet()
	self.troveVersionFlavors = {}

        self.depCheck = depCheck
        self.autoResolve = autoResolve
        self.checkOnlyByDefaultDeps = checkOnlyByDefaultDeps
        self.byDefault = byDefault

    def Requires(self, requirement):
        self.requires.addDep(deps.TroveDependencies, 
                             deps.Dependency(requirement))

class _GroupReference:
    """ A reference to a set of troves, created by a trove spec, that 
        can be searched like a repository using findTrove.  Hashable
        by the trove spec(s) given.  Note the references can be 
        recursive -- This reference could be relative to another 
        reference, passed in as the upstreamSource.
    """
    def __init__(self, troveSpecs, upstreamSource=None):
        self.troveSpecs = troveSpecs
        self.upstreamSource = upstreamSource

    def __hash__(self):
        return hash((self.troveSpecs, self.upstreamSource))

    def findSources(self, repos, labelPath, flavorPath):
        """ Find the troves that make up this trove reference """
        if self.upstreamSource is None:
            source = repos
        else:
            source = self.upstreamSource

        results = source.findTroves(labelPath, self.troveSpecs, flavorPath)
        troveTups = [ x for x in chain(*results.itervalues())]
        self.sourceTups = troveTups
        self.source = trovesource.TroveListTroveSource(source, troveTups)
        self.source.searchAsRepository()

    def findTroves(self, *args, **kw):
        return self.source.findTroves(*args, **kw)

    def getTroves(self, *args, **kw):
        return self.source.getTroves(*args, **kw)

    def getSourceTroves(self):
        """ Returns the list of troves that form this reference 
            (without their children).
        """
        return self.getTroves(self.sourceTups, withFiles=False)


class GroupRecipe(Recipe):
    Flags = use.LocalFlags
    depCheck = False
    autoResolve = False
    checkOnlyByDefaultDeps = True

    def Requires(self, requirement, groupName = None):
        if requirement[0] == '/':
            raise RecipeFileError, 'file requirements not allowed in groups'
        if groupName is None: groupName = self.name

        self.groups[groupName].Requires(requirement)

    def _parseFlavor(self, flavor):
        assert(flavor is None or isinstance(flavor, str))
        if flavor is None:
            return None
        flavorObj = deps.parseFlavor(flavor)
        if flavorObj is None:
            raise ValueError, 'invalid flavor: %s' % flavor
        return flavorObj

    def _parseGroupNames(self, groupName):
        if groupName is None:
            return [self.name]
        elif not isinstance(groupName, (list, tuple)):
            return [groupName]
        else:
            return groupName

    def addTrove(self, name, versionStr = None, flavor = None, source = None,
                 byDefault = None, groupName = None, ref=None):
        groupNames = self._parseGroupNames(groupName)
        flavor = self._parseFlavor(flavor)
        # track this trove in the GroupRecipe so that it can be found
        # as a group with the rest of the troves.
        self.toFind.setdefault(ref, set()).add((name, versionStr, flavor))
        if ref is not None:
            self.sources.add(ref)

        for groupName in groupNames:
            self.groups[groupName].addTrove(name, versionStr = versionStr,
                                                flavor = flavor,
                                                source = source,
                                                byDefault = byDefault, 
                                                ref = ref)

    def setByDefault(self, byDefault=True, groupName=None):
        """ Set whether troves added to this group are installed by default 
            or not.  (This default value can be overridden by the byDefault
            parameter to individual addTrove commands).  If you set the 
            byDefault value for the main group, you set it for any 
            future groups created.
        """
        groupNames = self._parseGroupNames()
        for groupName in groupNames:
            self.groups[groupName].setByDefault(byDefault)

    def addAllTroves(self, reference, groupName=None):
        """ Add all of the troves directly contained in the given 
            reference to groupName.  For example, if the cooked group-foo 
            contains references to the troves 
            foo1=<version>[flavor] and foo2=<version>[flavor],
            the lines 
            ref = r.addReference('group-foo')
            followed by
            r.addAllTroves(ref)
            would be equivalent to you having added the addTrove lines
            r.addTrove('foo1', <version>) 
            r.addTrove('foo2', <version>) 
        """
        assert(reference is not None)
        self.sources.add(reference)

        groupNames = self._parseGroupNames(groupName)
        for groupName in groupNames:
            self.groups[groupName].addAllTroves(reference)

    def removeTrove(self, name, versionStr=None, flavor=None, 
                    groupName=None):
        """ Remove a trove added to this group, either by an addAllTroves
            line or by an addTrove line. 
        """
        groupNames = self._parseGroupNames(groupName)
        flavor = self._parseFlavor(flavor)
        for groupName in groupNames:
            self.groups[groupName].removeTrove(name, versionStr, flavor)

    def addReference(self, name, versionStr=None, flavor=None, ref=None):
        flavor = self._parseFlavor(flavor)
        return _GroupReference(((name, versionStr, flavor),), ref)

    def addNewGroup(self, name, groupName = None, byDefault = True):
	if groupName is None:
	    groupName = self.name

	if not self.groups.has_key(name):
	    raise RecipeFileError, 'group %s has not been created' % name

	self.groups[groupName].addNewGroup(name, byDefault)

    def getRequires(self, groupName = None):
        if groupName is None: groupName = self.name
        return self.groups[groupName].getRequires()

    def getTroveList(self, groupName = None):
        if groupName is None: groupName = self.name
	return self.groups[groupName].getTroveList()

    def getNewGroupList(self, groupName = None):
        if groupName is None: groupName = self.name
	return self.groups[groupName].getNewGroupList()

    def getSize(self, groupName = None):
        if groupName is None: groupName = self.name
        return self.groups[groupName].size

    def setLabelPath(self, *path):
        self.labelPath = [ versions.Label(x) for x in path ]

    def createGroup(self, groupName, depCheck = False, autoResolve = False,
                    byDefault = None, checkOnlyByDefaultDeps = None):
        if self.groups.has_key(groupName):
            raise RecipeFileError, 'group %s was already created' % groupName
        if not groupName.startswith('group-'):
            raise RecipeFileError, 'group names must start with "group-"'
        if byDefault is None:
            byDefault = self.groups[self.name].byDefault
        if checkOnlyByDefaultDeps is None:
            checkOnlyByDefaultDeps  = self.groups[self.name].checkOnlyByDefaultDeps

        self.groups[groupName] = SingleGroup(depCheck, autoResolve, 
                                             checkOnlyByDefaultDeps, byDefault)

    def getGroupNames(self):
        return self.groups.keys()

    def _orderGroups(self):
        """ Order the groups so that each group is after any group it 
            contains.  Raises an error if a cycle is found.
        """
        # boy using a DFS for such a small graph seems like overkill.
        # but its handy since we're also trying to find a cycle at the same
        # time.
        children = {}
        groupNames = self.getGroupNames()
        for groupName in groupNames:
            children[groupName] = \
                    set([x[0] for x in self.getNewGroupList(groupName)])

        timeStamp = 0

        # the different items in the seen dict
        START = 0   # time at which the node was first visited
        FINISH = 1  # time at which all the nodes child nodes were finished
                    # with
        PATH = 2    # path to get to this node from wherever it was 
                    # started.
        seen = dict((x, [None, None, []]) for x in groupNames)

        for groupName in groupNames:
            if seen[groupName][START]: continue
            stack = [groupName]

            while stack:
                timeStamp += 1
                node = stack[-1]

                if seen[node][FINISH]:
                    # we already visited this node through 
                    # another path that was longer.  
                    stack = stack[:-1]
                    continue
                childList = []
                if not seen[node][START]:
                    seen[node][START] = timeStamp

                    if children[node]:
                        path = seen[node][PATH] + [node]
                        for child in children[node]:
                            if child in path:
                                cycle = path[path.index(child):] + [child]
                                raise RecipeFileError('cycle in groups: %s' % cycle)

                            if not seen[child][START]:
                                childList.append(child)

                if not childList:
                    # we've finished with all this nodes children 
                    # mark it as done
                    seen[node][FINISH] = timeStamp
                    stack = stack[:-1]
                else:
                    path = seen[node][PATH] + [node]
                    for child in childList:
                        seen[child] = [None, None, path]
                        stack.append(child)

        groupsByLastSeen = ( (seen[x][FINISH], x) for x in groupNames)
        return [x[1] for x in sorted(groupsByLastSeen)]

    def _getIncludedTroves(self, groupName, checkOnlyByDefaultDeps):
        """ 
            Returns the troves in all subGroups included by this trove.
            If checkOnlyByDefaultDeps is False, exclude troves that are 
            not included by default.
        """
        allTroves = []
        childGroups = []
        for childGroup, byDefault in self.groups[groupName].getNewGroupList(): 
            if byDefault or not checkOnlyByDefaultDeps:
                childGroups.append(childGroup)

        while childGroups:
            childGroup = childGroups.pop()
            groupObj = self.groups[childGroup]

            if checkOnlyByDefaultDeps:
                allTroves.extend(groupObj.getDefaultTroves())
            else:
                allTroves.extend(groupObj.troves)

            for childGroup, byDft in self.groups[childGroup].getNewGroupList(): 
                if byDft or not checkOnlyByDefaultDeps:
                    childGroups.append(childGroup)
        return allTroves

    def findAllTroves(self):
        if self.toFind is not None:
            # find all troves needed by all included groups together, at 
            # once.  We then pass that information into the individual
            # groups.
            self._findSources()
            self._findTroves()
            self.toFind = None

        groupNames = self._orderGroups()

        for groupName in groupNames:
            groupObj = self.groups[groupName]

            # assign troves to this group
            groupObj.findTroves(self.troveSpecMap, self.repos)

            # if ordering is right, we now should be able to recurse through
            # the groups included by this group and get all recursively
            # included troves
            includedTroves = self._getIncludedTroves(groupName, 
                                             groupObj.checkOnlyByDefaultDeps)

            # include those troves when doing dependency resolution/checking
            groupObj.autoResolveDeps(self.cfg, self.repos, self.labelPath, 
                                                           includedTroves)

            failedDeps = groupObj.checkDependencies(self.cfg, includedTroves)
            if failedDeps:
                return groupName, failedDeps

            groupObj.calcSize()

            if not groupObj.hasTroves():
                raise RecipeFileError('%s has no troves in it' % groupName)


    def _findSources(self):
        for troveSource in self.sources:
            if troveSource is None:
                continue
            troveSource.findSources(self.repos, self.labelPath, self.flavor)

    def _findTroves(self):
        """ Finds all the troves needed by all groups, and then 
            stores the information for retrieval by the individual 
            groups (stored in troveSpecMap).
        """
        repos = self.repos
        cfg = self.cfg

        troveTups = set()

        results = {}
        for troveSource, toFind in self.toFind.iteritems():
            try:
                if troveSource is None:
                    source = repos
                else:
                    source = troveSource

                results[troveSource] = source.findTroves(self.labelPath, 
                                                         toFind, 
                                                         cfg.buildFlavor)
            except repository.TroveNotFound, e:
                raise RecipeFileError, str(e)
            for result in results.itervalues():
                troveTups.update(chain(*result.itervalues()))

        troveTups = list(troveTups)
        troves = repos.getTroves(troveTups, withFiles=False)

        foundTroves = dict(izip(troveTups, troves))

        troveSpecMap = {}
        # store the pertinent information in troveSpecMap
        # keyed off of source, then troveSpec
        # note - redirect troves are not allowed in group recipes.
        # we track whether a trove is a redirect because it's possible
        # it could be added at one point (say, by an overly general
        # addTrove line) and then removed afterwards by a removeTrove.
        for troveSource, toFind in self.toFind.iteritems():
            d = {}
            for troveSpec in toFind:
                d[troveSpec] = [ (x,
                                  foundTroves[x].getSize(), 
                                  foundTroves[x].isRedirect()) 
                                    for x in results[troveSource][troveSpec] ]
            troveSpecMap[troveSource] = d
        self.troveSpecMap = troveSpecMap

    def __init__(self, repos, cfg, label, flavor, extraMacros={}):
	self.repos = repos
	self.cfg = cfg
	self.labelPath = [ label ]
	self.flavor = flavor
        self.macros = macros.Macros()
        self.macros.update(extraMacros)

        self.toFind = {}
        self.troveSpecMap = {}
        self.foundTroves = {}
        self.sources = set()

        self.groups = {}
        self.groups[self.name] = SingleGroup(self.depCheck, self.autoResolve,   
                                             self.checkOnlyByDefaultDeps)

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

            # this redirects from oldTrove -> newTrove
            d[(topName, topVersion, topFlavor)] = True

            for (name, version, flavor) in topLevelTrove.iterTroveList():
                # redirect from oldTrove -> referencedPackage
                d[(name, version, flavor)] = True

                if name.find(":") != -1:
                    compName = fromName + ":" + name.split(":")[1]
                    # redirect from oldTrove -> oldTrove:component. we
                    # leave version/flavor alone; they get filled in later
                    d[(compName, None, None)] = True

                    # redirect from oldTrove:component -> newTrove:component
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
                                           (component, versionStr, None),
                                           self.flavor)
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
