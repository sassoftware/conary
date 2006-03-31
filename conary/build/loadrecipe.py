#
# Copyright (c) 2004-2006 rPath, Inc.
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

import imp
import inspect
import new
import os
import string
import sys
import tempfile
import traceback

from conary.repository import errors
from conary.build import recipe,use
from conary.build import errors as builderrors
from conary.build.errors import RecipeFileError
from conary.conaryclient import cmdline
from conary.deps import deps
from conary.lib import util
from conary.local import database
from conary import versions


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
        if isinstance(modules, str):
            modules = (modules,)
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

def setupRecipeDict(d, filename, directory=None):
    localImport(d, 'conary.build', ('build', 'action'))
    localImport(d, 'conary.build.loadrecipe', 
                                   ('loadSuperClass', 'loadInstalled',
                                    # XXX when all recipes have been migrated
                                    # we can get rid of loadRecipe
                                    ('loadSuperClass', 'loadRecipe')))
    localImport(d, 'conary.build.grouprecipe', 'GroupRecipe')
    localImport(d, 'conary.build.filesetrecipe', 'FilesetRecipe')
    localImport(d, 'conary.build.redirectrecipe', 'RedirectRecipe')
    localImport(d, 'conary.build.packagerecipe', 
                                  ('clearBuildReqs',
                                   'PackageRecipe', 
                                   'BuildPackageRecipe',
                                   'CPackageRecipe',
                                   'AutoPackageRecipe'))
    localImport(d, 'conary.build.inforecipe',  ('UserInfoRecipe', 
                                                'GroupInfoRecipe'))
    localImport(d, 'conary.lib', ('util',))
    for x in ('os', 're', 'sys', 'stat'):
        localImport(d, x)
    localImport(d, 'conary.build.use', ('Arch', 'Use', ('LocalFlags', 'Flags')))
    d['filename'] = filename
    if not directory:
        directory = os.path.dirname(filename)
    d['directory'] = directory
    _copyReusedRecipes(d)

_recipesToCopy = []
def _addRecipeToCopy(recipeClass):
    global recipesToCopy
    _recipesToCopy.append(recipeClass)

def _copyReusedRecipes(moduleDict):
    # XXX HACK - get rid of this when we move the
    # recipe classes to the repository.
    # makes copies of some of the superclass recipes that are 
    # created in this module.  (specifically, the ones with buildreqs)
    global _recipesToCopy
    for recipeClass in _recipesToCopy:
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

class RecipeLoader:

    def __init__(self, filename, cfg=None, repos=None, component=None,
                 branch=None, ignoreInstalled=False, directory=None):
        try:
            self._load(filename, cfg, repos, component,
                       branch, ignoreInstalled, directory)
        except Exception, err:
            raise builderrors.LoadRecipeError('unable to load recipe file %s: %s'\
                                              % (filename, err))

    def _load(self, filename, cfg=None, repos=None, component=None,
              branch=None, ignoreInstalled=False, directory=None):
        self.recipes = {}
        
        if filename[0] != "/":
            raise builderrors.LoadRecipeError("recipe file names must be absolute paths")

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

	setupRecipeDict(self.module.__dict__, filename, directory)

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


        # create the recipe class by executing the code in the recipe
        try:
            code = compile(f.read(), filename, 'exec')
        except SyntaxError, err:
            msg = ('Error in recipe file "%s": %s\n' %(basename, err))
            if err.offset is not None:
                msg += '%s%s^\n' %(err.text, ' ' * (err.offset-1))
            else:
                msg += err.text
            raise builderrors.RecipeFileError(msg)

        use.resetUsed()
        try:
            exec code in self.module.__dict__
        except Exception, err:
            tb = sys.exc_info()[2]
            while tb and tb.tb_frame.f_code.co_filename != filename:
                tb = tb.tb_next

            if not tb:
                raise

            err = ''.join(traceback.format_exception(err.__class__, err, tb))
            del tb
            msg = ('Error in recipe file "%s":\n %s' %(basename, err))
            raise builderrors.RecipeFileError(msg)
            

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
            if not inspect.isclass(obj) or not issubclass(obj, recipe.Recipe):
                continue
            # if a recipe has been marked to be ignored (for example, if
            # it was loaded from another recipe by loadRecipe()
            # (don't use hasattr here, we want to check only the recipe
            # class itself, not any parent class
            if 'ignore' in obj.__dict__:
                continue
            # make sure the class is derived from Recipe
            if not issubclass(obj, recipe.Recipe):
                continue

            self.recipes[name] = obj
            obj.filename = filename
            if hasattr(obj, 'name') and hasattr(obj, 'version'):
                validateRecipe(obj, pkgname, basename)

                if found:
                    raise builderrors.RecipeFileError(
                        'Error in recipe file "%s": multiple recipe classes '
                        'with both name and version exist' % basename)
                self.recipe = obj
                
                found = True
            else:
                raise builderrors.RecipeFileError(
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
            raise builderrors.RecipeFileError(
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
    # FIXME I'm quite sure this heuristic will get replaced with
    # something smarter/more sane as time progresses
    if not labelPath:
        return 0
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
    """ These labels all match the given labelPath.
        We score them based on the number of matching labels in 
        the label path, and return the one that's "best".

        The following rules should apply:
            - If the labelPath is [bar, foo] and you are choosing between
              /foo/bar/ and /foo/blah/bar, choose /foo/bar.  Assumption
              is that any other shadow/branch in the path may be from a 
              maintenance branch.
            - If the labelPath is [bar] and you are choosing between
              /foo/bar/ and /foo/blah/bar, choose /foo/bar.
            - If two troves are on the same branch, prefer the later trove.
    """
    scores = [ (_scoreLoadRecipeChoice(labelPath, x[1]), x) for x in troveTups ]
    maxScore = max(scores)[0]
    troveTups = [x[1] for x in scores if x[0] == maxScore ]

    if len(troveTups) <= 1:
        return troveTups
    else:
        byBranch = {}
        for troveTup in troveTups:
            branch = troveTup[1].branch()
            if branch in byBranch:
                byBranch[branch] = max(byBranch[branch], troveTup)
        return byBranch.values()

def recipeLoaderFromSourceComponent(name, cfg, repos,
                                    versionStr=None, labelPath=None,
                                    ignoreInstalled=False, 
                                    filterVersions=False, 
                                    parentDir=None):
    # FIXME parentDir specifies the directory to look for 
    # local copies of recipes called with loadRecipe.  If 
    # empty, we'll look in the tmp directory where we create the recipe
    # file for this source component - probably not intended behavior.

    name = name.split(':')[0]
    component = name + ":source"
    filename = name + '.recipe'
    if not labelPath:
        if not cfg.buildLabel:
             raise builderrors.LoadRecipeError(
            'no build label set -  cannot find source component %s' % component)
            
	labelPath = [cfg.buildLabel]
    try:
	pkgs = repos.findTrove(labelPath,
                               (component, versionStr, deps.DependencySet()))
    except errors.TroveNotFound:
        raise builderrors.LoadRecipeError(
                                'cannot find source component %s' % component)
    if filterVersions:
        pkgs = getBestLoadRecipeChoices(labelPath, pkgs)
    if len(pkgs) > 1:
        raise builderrors.LoadRecipeError(
                              "source component %s has multiple versions "
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
	raise builderrors.RecipeFileError("version %s of %s does not contain %s" %
		  (sourceComponent.getName(), 
                   sourceComponent.getVersion().asString(),
	 	   filename))

    util.copyfileobj(inF, outF)

    del inF
    outF.close()
    del outF

    try:
        loader = RecipeLoader(recipeFile, cfg, repos, component, 
                              sourceComponent.getVersion().branch(),
                              ignoreInstalled=ignoreInstalled,
                              directory=parentDir)
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
                troves = getBestLoadRecipeChoices(labelPath, troves)
                if len(troves) > 1:
                    raise builderrors.RecipeFileError(
                                    'Multiple troves could match loadInstalled'
                                    ' request %s: %s' %(troveSpec, troves))
            if troves:
                return troves[0][1].getSourceVersion(), troves[0][2]
        except errors.TroveNotFound:
            pass
        if labelPath is None:
            return None
        try:
            troves = db.findTrove(None, (name, versionStr, flavor))
            troves = getBestLoadRecipeChoices(None, troves)
            if len(troves) > 1:
                raise RuntimeError, (
                                'Multiple troves could match loadRecipe' 
                                ' request for %s' % name)
            if troves:
                return troves[0][1].getSourceVersion(), troves[0][2]
        except errors.TroveNotFound:
            pass
        return None


    cfg = callerGlobals['cfg']
    repos = callerGlobals['repos']
    branch = callerGlobals['branch']
    parentPackageName = callerGlobals['name']
    parentDir = callerGlobals['directory']
    if 'ignoreInstalled' in callerGlobals:
        alwaysIgnoreInstalled = callerGlobals['ignoreInstalled']
    else:
        alwaysIgnoreInstalled = False

    oldUsed = use.getUsed()
    name, versionStr, flavor = cmdline.parseTroveSpec(troveSpec)
    versionSpec, flavorSpec = versionStr, flavor

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
            localfile = parentDir + '/' + file
        else:
            localfile = name + '.recipe'

        if os.path.exists(localfile):
            if flavor:
                oldBuildFlavor = cfg.buildFlavor
                cfg.buildFlavor = deps.overrideFlavor(oldBuildFlavor, flavor)
                use.setBuildFlagsFromFlavor(name, cfg.buildFlavor)
            loader = RecipeLoader(localfile, cfg, repos=repos,
                                  ignoreInstalled=alwaysIgnoreInstalled)

    if not loader:
        if label:
            labelPath = [versions.Label(label)]
        elif branch:
            # if no labelPath was specified, search backwards through the 
            # labels on the current branch.
            labelPath = list(branch.iterLabels())
            labelPath.reverse()
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
                while version.isOnLocalHost():
                    version = version.parentVersion()
                versionStr = str(version)

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
                                     filterVersions=True,
                                     parentDir=parentDir)[0]

    if label and not versionSpec:
        # If they used the old-style specification of label, we should 
        # convert to new style for purposes of storing in troveInfo
        troveSpec = '%s=%s' % (name, label)
        if flavorSpec:
            troveSpec = '%s[%s]' % (troveSpec, flavorSpec)

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

def validateRecipe(recipeClass, packageName, fileName):
    if recipeClass.name[0] not in string.ascii_letters + string.digits:
        raise RecipeFileError(
            'Error in recipe file "%s": package name must start '
            'with an ascii letter or digit.' % fileName)

    if '-' in recipeClass.version:
        raise RecipeFileError(
            "Version string %s has illegal '-' character" % recipeClass.version)

    if recipeClass.name != packageName:
        raise RecipeFileError(
                    "Recipe object name '%s' does not match "
                    "file/component name '%s'"
                    % (recipeClass.name, packageName))

    packageType = recipeClass.getType()

    prefixes = {recipe.RECIPE_TYPE_INFO: 'info-',
                recipe.RECIPE_TYPE_GROUP: 'group-',
                recipe.RECIPE_TYPE_FILESET: 'fileset-'}

    if packageType in prefixes:
        if not recipeClass.name.startswith(prefixes[packageType]):
            raise builderrors.BadRecipeNameError(
                    'recipe name must start with "%s"' % prefixes[packageType])
    else:   
        for prefix in prefixes.itervalues():
            if recipeClass.name.startswith(prefix):
                raise builderrors.BadRecipeNameError(
                                'recipe name cannot start with "%s"' % prefix)
                
    recipeClass.validateClass()

