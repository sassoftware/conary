#
# Copyright (c) 2004-2008 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import copy
import imp
import inspect
import itertools
import new
import os
import string
import sys
import types
import tempfile
import traceback

from conary.repository import errors
from conary.build import recipe, use
from conary.build import errors as builderrors
from conary.build.errors import RecipeFileError
from conary.build.factory import Factory as FactoryRecipe
from conary.conaryclient import cmdline
from conary.deps import deps
from conary.lib import log, util
from conary.local import database
from conary import versions



class RecipeLoaderFromString:

    _defaultsLoaded = False

    # This is the module dictionary which is used as the starting point
    # for all recipe imports. Derivatives of RecipeLoader should use this
    # version, not the one stored in the derivative class.
    baseModuleDict = {}

    # we don't set these up right away because of import ordering issues
    baseModuleImports = [
        ('conary.build', ('build', 'action')),
        ('conary.build.loadrecipe', 
                           ('loadSuperClass', 'loadInstalled',
                            # XXX when all recipes have been migrated
                            # we can get rid of loadRecipe
                            ('loadSuperClass', 'loadRecipe'))),
        ('conary.build.grouprecipe', 'GroupRecipe'),
        ('conary.build.filesetrecipe', 'FilesetRecipe'),
        ('conary.build.redirectrecipe', 'RedirectRecipe'),
        ('conary.build.derivedrecipe', 'DerivedPackageRecipe'),
        ('conary.build.packagerecipe', 
                          ('clearBuildReqs', 'clearBuildRequires',
                           'clearCrossReqs', 'clearCrossRequires',
                           'PackageRecipe', 'BuildPackageRecipe',
                           'CPackageRecipe', 'AutoPackageRecipe')),
        ('conary.build.inforecipe',  ('UserInfoRecipe', 'GroupInfoRecipe',
                                      'UserGroupInfoRecipe' )),
        ('conary.lib', ('util',)),
        ('os',),
        ('re',),
        ('sys',),
        ('stat',),
        ('conary.build.use', ('Arch', 'Use', ('LocalFlags', 'Flags'),
                                            'PackageFlags')) ]

    def __init__(self, codeString, filename, cfg=None, repos=None,
                 component=None, branch=None, ignoreInstalled=False,
                 directory=None, buildFlavor=None, db=None, overrides = None,
                 factory = False, objDict = {}):
        try:
            self._load(codeString, filename, cfg, repos, component,
                       branch, ignoreInstalled, directory, 
                       buildFlavor=buildFlavor, db=db,
                       overrides=overrides, factory = factory,
                       objDict = objDict)
        except Exception, err:
            raise builderrors.LoadRecipeError('unable to load recipe file %s:\n%s'\
                                              % (filename, err))
    @classmethod
    def _initClass(theKlass):
        theKlass.baseModuleDict = {}
        theKlass._defaultsLoaded = False
        for args in theKlass.baseModuleImports:
            theKlass._localImport(theKlass.baseModuleDict, *args)

    @staticmethod
    def _localImport(d, package, modules=()):
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
              _localImport(d, "foo", (("bar", "baz"))
          from bar import fred, george:
              _localImport(d, "bar", ("fred", "george"))
          import os
              _localImport(d, "os")
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

    @staticmethod
    def baseImport(package, modules=()):
        RecipeLoaderFromString._localImport(RecipeLoaderFromString.baseModuleDict, package, modules)

    @staticmethod
    def _copyReusedRecipes(moduleDict):
        # XXX HACK - get rid of this when we move the
        # recipe classes to the repository.
        # makes copies of some of the superclass recipes that are 
        # created in this module.  (specifically, the ones with buildreqs)
        recipeClassDict = {}
        for recipeClass in moduleDict.values():
            if (type(recipeClass) != type or
                    not issubclass(recipeClass, recipe.Recipe)):
                continue
            numParents = len(inspect.getmro(recipeClass))
            recipeClassDict[recipeClass.__name__] = (numParents, recipeClass)
        # create copies of recipes by the number of parents they have
        # a class always has more parents than its parent does,
        # if you copy the superClasses first, the copies will.
        recipeClasses = [ x[1]  for x in sorted(recipeClassDict.values(),
                                                key=lambda x: x[0]) ]
        for recipeClass in recipeClasses:
            className = recipeClass.__name__
            # when we create a new class object, it needs its superclasses.
            # get the original superclass list and substitute in any 
            # copies
            mro = list(inspect.getmro(recipeClass)[1:])
            newMro = []
            for superClass in mro:
                superName = superClass.__name__
                newMro.append(moduleDict.get(superName, superClass))

            newDict = {}
            for name, attr in recipeClass.__dict__.iteritems():
                if type(attr) in [ types.ModuleType, types.MethodType,
                                   types.UnboundMethodType,
                                   types.FunctionType,
                                   staticmethod,
                                   # don't copy in flags, as they
                                   # need to have their data copied out
                                   use.LocalFlagCollection]:
                    newDict[name] = attr
                else:
                    newDict[name] = copy.deepcopy(attr)

            moduleDict[className] = new.classobj(className,
                                                 tuple(newMro), newDict)

    @staticmethod
    def _getTrovesFromRepos(repos, labelPath, troveSpecs):
        """
        Gets trove objects from a repository based on a list of trove specs.
        If any aren't found, None is returned. If a connectivity error
        occurs, None is returned for all troves.
        """
        nvfDict = repos.findTroves(labelPath, troveSpecs)

        trovesNeeded = []
        for i, spec in enumerate(troveSpecs):
            nvf = nvfDict.get(spec, None)
            if not nvf:
                continue

            if len(nvf) > 1:
                raise builderrors.RecipeFileError('too many matches for '
                                'autoLoadRecipe entry %s' % spec)

            trovesNeeded.append((i, nvf[0]))

        troves = repos.getTroves([ x[1] for x in trovesNeeded],
                                 withFiles = False)

        result = [ None ] * len(troveSpecs)
        for ((i, nvf), trv) in itertools.izip(trovesNeeded, troves):
            result[i] = trv

        return result

    @staticmethod
    def _loadDefaultPackages(d, cfg, repos, db = None, buildFlavor = None):
        if RecipeLoaderFromString._defaultsLoaded:
            # we don't need to load these twice
            return

        RecipeLoaderFromString._defaultsLoaded = True

        if db is None:
            db = database.Database(cfg.root, cfg.dbPath)

        troveSpecs = [ cmdline.parseTroveSpec(x) for x in cfg.autoLoadRecipes ]

        groupTroves = RecipeLoaderFromString._getTrovesFromRepos(repos,
                                                cfg.installLabelPath,
                                                troveSpecs)

        # we look for recipes in reverse order to allow the troves at the
        # front of the list to override those at the end
        recipeTroves = {}

        for trv in reversed(groupTroves):
            for x in itertools.chain([ trv.getNameVersionFlavor() ],
                                      trv.iterTroveList(weakRefs = True,
                                                        strongRefs = True) ):
                if x[0].endswith(':recipe'):
                    recipeTroves[x[0]] = x

        # We have a list of the troves to autoload recipes from now. Go get
        # those troves so we can get the file information we need. The
        # sort here is to keep this order repeatable
        troveList = repos.getTroves(sorted(recipeTroves.values()),
                                    withFiles = True)
        filesNeeded = []
        for trv in troveList:
            filesNeeded += [ x for x in trv.iterFileList() if
                                    x[1].endswith('.recipe') ]

        recipes = repos.getFileContents([ (x[2], x[3]) for x in filesNeeded ])

        for (fileContents, fileInfo) in itertools.izip(recipes, filesNeeded):
            loader = RecipeLoaderFromString(fileContents.get().read(),
                                  fileInfo[1], cfg, repos = repos,
                                  ignoreInstalled = True,
                                  buildFlavor = buildFlavor, db = db)

            recipe = loader.getRecipe()
            recipe.internalAbstractBaseClass = True

            d.update(loader.recipes)

    def _findRecipeClass(self, pkgname, basename, objDict, factory = False):
        result = None
        for (name, obj) in objDict.items():
            if not inspect.isclass(obj):
                continue
            if name == 'FactoryRecipeClass':
                continue

            # if a recipe has been marked to be ignored (for example, if
            # it was loaded from another recipe by loadRecipe()
            # (don't use hasattr here, we want to check only the recipe
            # class itself, not any parent class
            if 'internalAbstractBaseClass' in obj.__dict__:
                continue
            # make sure the class is derived from either Recipe or Factory
            if ((    factory and not issubclass(obj, FactoryRecipe)) or
                (not factory and not issubclass(obj, recipe.Recipe  ))):
                continue

            if hasattr(obj, 'name') and hasattr(obj, 'version'):
                self._validateRecipe(obj, pkgname, basename)

                if result:
                    raise builderrors.RecipeFileError(
                        'Error in recipe file "%s": multiple recipe classes '
                        'with both name and version exist' % basename)

                result = (name, obj)
            else:
                raise builderrors.RecipeFileError(
                    "Recipe in file/component '%s' did not contain both a name"
                    " and a version attribute." % pkgname)

        if not result:
            raise builderrors.RecipeFileError(
                "file/component '%s' did not contain a valid recipe" % pkgname)

        return result

    @staticmethod
    def _validateRecipe(recipeClass, packageName, fileName):
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
        elif packageType == recipe.RECIPE_TYPE_REDIRECT:
            # redirects are allowed to have any format
            pass
        else:
            for prefix in prefixes.itervalues():
                if recipeClass.name.startswith(prefix):
                    raise builderrors.BadRecipeNameError(
                                    'recipe name cannot start with "%s"' % prefix)
        recipeClass.validateClass()

    def _load(self, codeString, filename, cfg=None, repos=None, component=None,
              branch=None, ignoreInstalled=False, directory=None,
              buildFlavor=None, db=None, overrides=None, factory=False,
              objDict = None):
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

        # store cfg and repos, so that the recipe can load
        # recipes out of the repository

        self.module.__dict__.update(self.baseModuleDict)

        self.module.__dict__['cfg'] = cfg
        self.module.__dict__['repos'] = repos
        self.module.__dict__['db'] = db
        self.module.__dict__['buildFlavor'] = buildFlavor
        self.module.__dict__['filename'] = filename
        
        if not directory:
            directory = os.path.dirname(filename)
        self.module.__dict__['directory'] = directory

        self._loadDefaultPackages(self.module.__dict__, cfg, repos, db,
                                  buildFlavor = buildFlavor)
        self._copyReusedRecipes(self.module.__dict__)

        if factory:
            self._localImport(self.module.__dict__, 'conary.build.factory',
                              ('Factory', 'FactoryException' ))

        self.module.__dict__['component'] = component
        self.module.__dict__['branch'] = branch
        self.module.__dict__['name'] = pkgname
        self.module.__dict__['ignoreInstalled'] = ignoreInstalled
        self.module.__dict__['loadedTroves'] = []
        self.module.__dict__['loadedSpecs'] = {}
        self.module.__dict__['overrides'] = overrides
        if objDict:
            self.module.__dict__.update(objDict)

        # create the recipe class by executing the code in the recipe
        try:
            code = compile(codeString, filename, 'exec')
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
        except (errors.ConaryError, builderrors.CvcError), err:
            # don't show the exception for conary and cvc errors -
            # we assume their exception message already contains the 
            # required information

            tb = sys.exc_info()[2]
            while tb and tb.tb_frame.f_code.co_filename != filename:
                tb = tb.tb_next
            linenum = tb.tb_frame.f_lineno

            msg = ('Error in recipe file "%s", line %s:\n %s' % (basename, linenum, err))


            raise builderrors.RecipeFileError(msg)
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
        del self.module.__dict__['db']
        del self.module.__dict__['cfg']
        del self.module.__dict__['repos']
        del self.module.__dict__['component']
        del self.module.__dict__['branch']
        del self.module.__dict__['name']
        del self.module.__dict__['ignoreInstalled']
        del self.module.__dict__['buildFlavor']
        del self.module.__dict__['overrides']

        (name, obj) = self._findRecipeClass(pkgname, basename,
                                            self.module.__dict__,
                                            factory = factory)
        self.recipes[name] = obj
        obj.filename = filename
        self.recipe = obj
        # create a reference to this module inside of the recipe to prevent
        # the module from getting unloaded
        obj.__moduleObj__ = self.module

        # inherit any tracked flags that we found while loading parent
        # classes.  Also inherit the list of recipes classes needed to load
        # this recipe.
        self.recipe.addLoadedTroves(self.module.__dict__['loadedTroves'])
        self.recipe.addLoadedSpecs(self.module.__dict__['loadedSpecs'])

        if self.recipe._trackedFlags is not None:
            use.setUsed(self.recipe._trackedFlags)
        self.recipe._trackedFlags = use.getUsed()
        if buildFlavor is not None:
            self.recipe._buildFlavor = buildFlavor
        self.recipe._localFlavor = use.localFlagsToFlavor(self.recipe.name)

        # _usedFlavor here is a complete hack. Unfortuantely _trackedFlags
        # can change because it contains global flags, and if we make a copy
        # of it those copies can't be passed to use.setUsed() somewhere
        # else because of those same globals. Sweet.
        self.recipe._usedFlavor = use.createFlavor(self.recipe.name,
                                                   self.recipe._trackedFlags)
        self.recipe._sourcePath = directory

    def allRecipes(self):
        return self.recipes

    def getRecipe(self):
        return self.recipe

class RecipeLoader(RecipeLoaderFromString):

    def __init__(self, filename, cfg=None, repos=None, component=None,
                 branch=None, ignoreInstalled=False, directory=None,
                 buildFlavor=None, db=None, overrides = None,
                 factory = False, objDict = {}):
        try:
            f = open(filename)
            codeString = f.read()
            f.close()
        except Exception, err:
            raise builderrors.LoadRecipeError(
                                    'unable to load recipe file %s:\n%s'\
                                   % (filename, err))

        RecipeLoaderFromString.__init__(self, codeString, filename,
                cfg = cfg, repos = repos, component = component,
                branch = branch, ignoreInstalled = ignoreInstalled,
                directory = directory, buildFlavor = buildFlavor,
                db = db, overrides = overrides, factory = factory,
                objDict = objDict)

class RecipeLoaderFromSourceTrove(RecipeLoader):

    @staticmethod
    def findFileByPath(sourceTrove, path):
        for (pathId, filePath, fileId, fileVersion) in sourceTrove.iterFileList():
            if filePath == path:
                return (fileId, fileVersion)

        return None

    def __init__(self, sourceTrove, repos, cfg, versionStr=None, labelPath=None,
                 ignoreInstalled=False, filterVersions=False,
                 parentDir=None, defaultToLatest = False,
                 buildFlavor = None, db = None, overrides = None,
                 getFileFunction = None, branch = None):
        self.recipes = {}

        if getFileFunction is None:
            getFileFunction = lambda repos, fileId, fileVersion, path: \
                    repos.getFileContents([ (fileId, fileVersion) ])[0].get()

        name = sourceTrove.getName().split(':')[0]

        if (sourceTrove.getFactory() and
            sourceTrove.getFactory() != 'factory'):
            if not versionStr:
                if branch:
                    versionStr = str(branch)
                else:
                    versionStr = sourceTrove.getVersion().branch()

            factoryName = 'factory-' + sourceTrove.getFactory()

            loader = ChainedRecipeLoader(factoryName, None, True, cfg,
                                         repos, branch, name, parentDir,
                                         buildFlavor, ignoreInstalled,
                                         overrides, db)

            # XXX name + '.recipe' sucks, but there isn't a filename that
            # actually exists
            factoryCreatedRecipe = self.recipeFromFactory(sourceTrove,
                                                          loader.getRecipe(),
                                                          name,
                                                          name + '.recipe',
                                                          repos,
                                                          getFileFunction)
            factoryCreatedRecipe._trove = sourceTrove.copy()
            factoryCreatedRecipe._sourcePath = parentDir

            self.recipes.update(loader.recipes)
            self.recipes[factoryCreatedRecipe.name] = factoryCreatedRecipe
        else:
            factoryCreatedRecipe = None

        recipePath = name + '.recipe'
        match = self.findFileByPath(sourceTrove, recipePath)

        if not match and factoryCreatedRecipe:
            # this is a recipeless factory; use the recipe class created
            # by the factory for this build
            self.recipe = factoryCreatedRecipe
            # this validates the class is well-formed as a recipe
            self._findRecipeClass(name, name + '.recipe',
                                  { self.recipe.name : self.recipe })
            return
        elif not match:
            # this is just missing the recipe; we need it
            raise builderrors.RecipeFileError("version %s of %s does not "
                                              "contain %s" %
                      (sourceTrove.getName(),
                       sourceTrove.getVersion().asString(),
                       filename))

        (fd, recipeFile) = tempfile.mkstemp(".recipe", 'temp-%s-' %name, 
                                            dir=cfg.tmpDir)
        outF = os.fdopen(fd, "w")

        inF = getFileFunction(repos, match[0], match[1], recipePath)

        util.copyfileobj(inF, outF)

        del inF
        outF.close()
        del outF

        if branch is None:
            branch = sourceTrove.getVersion().branch()

        if factoryCreatedRecipe:
            objDict = { 'FactoryRecipeClass' : factoryCreatedRecipe }
        else:
            objDict = {}

        try:
            RecipeLoader.__init__(self, recipeFile, cfg, repos,
                      sourceTrove.getName(),
                      branch = branch,
                      ignoreInstalled=ignoreInstalled,
                      directory=parentDir, buildFlavor=buildFlavor,
                      db=db, overrides=overrides,
                      factory = (sourceTrove.getFactory() == 'factory'),
                      objDict = objDict)
        finally:
            os.unlink(recipeFile)

        self.recipe._trove = sourceTrove.copy()

    def recipeFromFactory(self, sourceTrv, factoryClass, pkgname,
                          recipeFileName, repos, getFileFunction):
        # (fileId, fileVersion) by path
        pathDict = dict( (x[1], (x[2], x[3])) for x in
                                                sourceTrv.iterFileList() )
        def openSourceFile(path):
            if path not in pathDict:
                raise builderrors.LoadRecipeError(
                        'Path %s not found in %s=%s', path,
                        sourceTrv.getName(), sourceTrv.getVersion())

            fileId, fileVersion = pathDict[path]

            return getFileFunction(repos, fileId, fileVersion, path)

        files = sorted([ x[1] for x in sourceTrv.iterFileList() ])
        factory = factoryClass(pkgname, sourceFiles = files,
                               openSourceFileFn = openSourceFile)
        recipe = factory.getRecipeClass()

        recipe.addLoadedTroves(factoryClass._loadedTroves)
        recipe.addLoadedSpecs(factoryClass._loadedSpecs)

        recipe.addLoadedTroves(factoryClass._loadedTroves)

        if factoryClass._trove:
            # this doesn't happen if you load from the local directory
            recipe.addLoadedTroves(
                            [ factoryClass._trove.getNameVersionFlavor() ])
            recipe.addLoadedSpecs(
                            { factoryClass.name :
                                (factoryClass._trove.getNameVersionFlavor(),
                                 factoryClass) } )

        return recipe

    def getSourceComponentVersion(self):
        return self.recipe._trove.getVersion()

class RecipeLoaderFromRepository(RecipeLoaderFromSourceTrove):

    def __init__(self, name, cfg, repos, versionStr=None, labelPath=None,
                 ignoreInstalled=False, filterVersions=False,
                 parentDir=None, defaultToLatest = False,
                 buildFlavor = None, db = None, overrides = None):
        # FIXME parentDir specifies the directory to look for 
        # local copies of recipes called with loadRecipe.  If 
        # empty, we'll look in the tmp directory where we create the recipe
        # file for this source component - probably not intended behavior.

        name = name.split(':')[0]
        component = name + ":source"
        if not labelPath:
            if not cfg.buildLabel:
                 raise builderrors.LoadRecipeError(
                'no build label set -  cannot find source component %s' % component)
            labelPath = [cfg.buildLabel]
        if repos is None:
            raise builderrors.LoadRecipeError(
                                    'cannot find source component %s: No repository access' % (component, ))
        try:
            pkgs = repos.findTrove(labelPath,
                                   (component, versionStr, deps.Flavor()))
        except (errors.TroveNotFound, errors.OpenError), err:
            raise builderrors.LoadRecipeError(
                                    'cannot find source component %s: %s' %
                                    (component, err))
        if filterVersions:
            pkgs = getBestLoadRecipeChoices(labelPath, pkgs)
        if len(pkgs) > 1:
            pkgs = sorted(pkgs, reverse=True)
            if defaultToLatest:
                log.warning("source component %s has multiple versions "
                             "on labelPath %s\n\n"
                             "Picking latest: \n       %s\n\n"
                             "Not using:\n      %s"
                              %(component,
                               ', '.join(x.asString() for x in labelPath),
                                '%s=%s' % pkgs[0][:2],
                                '\n       '.join('%s=%s' % x[:2] for x in pkgs[1:])))
            else:
                raise builderrors.LoadRecipeError(
                    "source component %s has multiple versions "
                    "on labelPath %s: %s"
                     %(component,
                       ', '.join(x.asString() for x in labelPath),
                       ', '.join('%s=%s' % x[:2] for x in pkgs)))

        sourceComponent = repos.getTrove(*pkgs[0])

        RecipeLoaderFromSourceTrove.__init__(self, sourceComponent, repos, cfg,
                 versionStr=versionStr, labelPath=labelPath,
                 ignoreInstalled=ignoreInstalled, filterVersions=filterVersions,
                 parentDir=parentDir, defaultToLatest = defaultToLatest,
                 buildFlavor = buildFlavor, db = db, overrides = overrides)

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
    troveTups = [x for x in scores if x[0] == maxScore ]

    if len(troveTups) <= 1:
        return [x[1] for x in troveTups]
    else:
        byBranch = {}
        for score, troveTup in troveTups:
            branch = troveTup[1].branch()
            if branch in byBranch:
                byBranch[branch] = max(byBranch[branch], (score, troveTup))
            else:
                byBranch[branch] = (score, troveTup)
        return [x[1] for x in byBranch.itervalues()]

def recipeLoaderFromSourceComponent(name, cfg, repos,
                                    versionStr=None, labelPath=None,
                                    ignoreInstalled=False,
                                    filterVersions=False,
                                    parentDir=None,
                                    defaultToLatest = False,
                                    buildFlavor = None,
                                    db = None, overrides = None):
    l = RecipeLoaderFromRepository(name, cfg, repos, versionStr=versionStr,
                                   labelPath=labelPath,
                                   ignoreInstalled=ignoreInstalled,
                                   filterVersions=filterVersions,
                                   parentDir=parentDir,
                                   defaultToLatest=defaultToLatest,
                                   buildFlavor=buildFlavor, db=db,
                                   overrides=overrides)
    return l, l.getSourceComponentVersion()

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
    callerGlobals = sys._getframe(1).f_globals
    _loadRecipe(troveSpec, label, callerGlobals, True)

def _pickLatest(component, troves, labelPath=None):
    troves.sort(reverse=True)
    err = "source component %s has multiple versions" % component
    if labelPath:
        err += " on labelPath %s:" % ', '.join(x.asString() for x in labelPath)
    else:
        err += ':'
    err += ("\nPicking latest:\n      %s\n\n"
            "Not using:\n      %s\n"
              %('%s=%s' % troves[0][:2],
                '\n       '.join('%s=%s' % x[:2] for x in troves[1:])))
    log.warning(err)
    return troves[0]

def ChainedRecipeLoader(troveSpec, label, findInstalled, cfg,
                        repos, branch, parentPackageName,
                        parentDir, buildFlavor,
                        alwaysIgnoreInstalled, overrides, db):

    # This loads a recipe from another recipe. It's used to load factory
    # recipes as well as superclasses. It returns a child of RecipeLoader

    def _findInstalledVersion(db, labelPath, name, versionStr, flavor, repos):
        """ Specialized search of the installed system along a labelPath, 
            defaulting to searching the whole system if the trove is not
            found along the label path.

            The version and flavor of the first found installed trove is 
            returned, or C{None} if no trove is found.
        """
        # first search on the labelPath.  
        troves = []
        try:
            troves = db.findTrove(labelPath, (name, versionStr, flavor))
            if len(troves) > 1:
                troves = getBestLoadRecipeChoices(labelPath, troves)
                if len(troves) > 1:
                    # sort by timeStamp even though they're across
                    # branches.  This will give us _some_ result to move
                    # forward with, which is better than blowing up.
                    troves = [_pickLatest(name, troves, labelPath)]
        except errors.TroveNotFound:
            pass
        if not troves:
            if labelPath is None:
                return None
            try:
                troves = db.findTrove(None, (name, versionStr, flavor))
                troves = getBestLoadRecipeChoices(None, troves)
            except errors.TroveNotFound:
                pass
        if not troves:
            return None

        if len(troves) > 1:
            troves = [_pickLatest(name, troves)]
        if troves:
            sourceVersion =  troves[0][1].getSourceVersion(False)
            flavor = troves[0][2]
            sourceName = name.split(':')[0] + ':source'
            noFlavor = deps.parseFlavor('')
            if not repos.hasTrove(sourceName, sourceVersion, noFlavor):
                while sourceVersion.hasParentVersion():
                    sourceVersion = sourceVersion.parentVersion()
                    if repos.hasTrove(sourceName, sourceVersion, noFlavor):
                        break
            return sourceVersion, flavor
        return None

    # def ChainedRecipeLoader begins here
    oldUsed = use.getUsed()
    name, versionStr, flavor = cmdline.parseTroveSpec(troveSpec)
    versionSpec, flavorSpec = versionStr, flavor

    if db is None:
        db = database.Database(cfg.root, cfg.dbPath)

    if name.endswith('.recipe'):
        file = name
        name = name[:-len('.recipe')]
    else:
        file = name + '.recipe'

    if label and not versionSpec:
        # If they used the old-style specification of label, we should 
        # convert to new style for purposes of storing in troveInfo
        troveSpec = '%s=%s' % (name, label)
        if flavorSpec is not None and not troveSpec.isEmpty():
            troveSpec = '%s[%s]' % (troveSpec, flavorSpec)

    if overrides and troveSpec in overrides:
        recipeToLoad, newOverrideDict = overrides[troveSpec]
        if hasattr(newOverrideDict, '_loadedSpecs'):
            # handle case where loadSpec is passed directly back in
            newOverrideDict = newOverrideDict._loadedSpecs
    else:
        recipeToLoad = newOverrideDict = None

    #first check to see if a filename was specified, and if that 
    #recipe actually exists.   
    loader = None
    if parentDir and not (recipeToLoad or label or versionStr or
                            (flavor is not None)):
        if name[0] != '/':
            localfile = parentDir + '/' + file
        else:
            localfile = name + '.recipe'

        if os.path.exists(localfile):
            # XXX: FIXME: this next test is unreachable
            if flavor is not None and not flavor.isEmpty():
                if buildFlavor is None:
                    oldBuildFlavor = cfg.buildFlavor
                    use.setBuildFlagsFromFlavor()
                else:
                    oldBuildFlavor = buildFlavor
                    buildFlavor = deps.overrideFlavor(oldBuildFlavor, flavor)
                use.setBuildFlagsFromFlavor(name, buildFlavor, error=False)
            log.info('Loading %s from %s' % (name, localfile))
            # ick
            factory = name.startswith('factory-')
            loader = RecipeLoader(localfile, cfg, repos=repos,
                                  ignoreInstalled=alwaysIgnoreInstalled,
                                  buildFlavor=buildFlavor,
                                  db=db, factory=factory)
            loader.recipe._trove = None

    if not loader and not findInstalled:
        # optimization: look on filesystem and local database to determine if
        # we have a local copy of the recipe already.
        loader, oldBuildFlavor = _getLoaderForInstalledRecipe(
                os.path.basename(name), versionStr, flavor, cfg, repos, db,
                buildFlavor)

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

        if cfg.installLabelPath:
            if labelPath:
                for label in cfg.installLabelPath:
                    if label not in labelPath:
                        labelPath.append(label)
            else:
                labelPath = cfg.installLabelPath

        if not recipeToLoad and findInstalled and not alwaysIgnoreInstalled:
            # look on the local system to find a trove that is installed that
            # matches this loadrecipe request.  Use that trove's version
            # and flavor information to grab the source out of the repository
            parts = _findInstalledVersion(db, labelPath, name, versionStr, 
                                          flavor, repos)
            if parts:
                version, flavor = parts
                while version.isOnLocalHost():
                    version = version.parentVersion()
                versionStr = str(version)

        if recipeToLoad:
            name, versionStr, flavor = recipeToLoad

        if flavor is not None:
            # override the current flavor with the flavor found in the 
            # installed trove (or the troveSpec flavor, if no installed 
            # trove was found.
            if buildFlavor is None:
                oldBuildFlavor = cfg.buildFlavor
                cfg.buildFlavor = deps.overrideFlavor(oldBuildFlavor, flavor)
                use.setBuildFlagsFromFlavor(name, cfg.buildFlavor, error=False)
            else:
                oldBuildFlavor = buildFlavor
                buildFlavor = deps.overrideFlavor(oldBuildFlavor, flavor)
                use.setBuildFlagsFromFlavor(name, buildFlavor, error=False)

        loader = RecipeLoaderFromRepository(name, cfg, repos,
                                     labelPath=labelPath,
                                     versionStr=versionStr,
                                     ignoreInstalled=alwaysIgnoreInstalled,
                                     filterVersions=True,
                                     parentDir=parentDir,
                                     defaultToLatest=True,
                                     db=db, overrides=newOverrideDict)

    if flavor is not None:
        if buildFlavor is None:
            buildFlavor = cfg.buildFlavor = oldBuildFlavor
        else:
            buildFlavor = oldBuildFlavor
        # must set this flavor back after the above use.createFlavor()
        use.setBuildFlagsFromFlavor(parentPackageName, buildFlavor, error=False)

    # return the tracked flags to their state before loading this recipe
    use.resetUsed()
    use.setUsed(oldUsed)

    return loader

def _loadRecipe(troveSpec, label, callerGlobals, findInstalled):
    """ See docs for loadInstalledPackage and loadSuperClass.  """

    cfg = callerGlobals['cfg']
    repos = callerGlobals['repos']
    branch = callerGlobals['branch']
    parentPackageName = callerGlobals['name']
    parentDir = callerGlobals['directory']
    buildFlavor = callerGlobals.get('buildFlavor', None)
    alwaysIgnoreInstalled = callerGlobals.get('ignoreInstalled', False)
    db = callerGlobals.get('db', None)

    # overrides could be None and we want to set it
    overrides = callerGlobals.get('overrides', None)
    if overrides is None:
        overrides = {}

    loader = ChainedRecipeLoader(troveSpec, label, findInstalled, cfg,
                                 repos, branch, parentPackageName,
                                 parentDir, buildFlavor,
                                 alwaysIgnoreInstalled, overrides, db)

    for name, recipe in loader.allRecipes().items():
        # hide all recipes from RecipeLoader - we don't want to return
        # a recipe that has been loaded by loadRecipe, so we treat them
        # for these purposes as if they are abstract base classes
        recipe.internalAbstractBaseClass = 1
        callerGlobals[name] = recipe
        if recipe._trove:
            # create a tuple with the version and flavor information needed to 
            # load this trove again.   You might be able to rely on the
            # flavor that the trove was built with, but when you load a
            # recipe that is not a superclass of the current recipe, 
            # its flavor is not assumed to be relevant to the resulting 
            # package (otherwise you might have completely irrelevant flavors
            # showing up for any package that loads the python recipe, e.g.)
            troveTuple = (recipe._trove.getName(), recipe._trove.getVersion(),
                          recipe._usedFlavor)
            log.info('Loaded %s from %s=%s[%s]' % ((name,) + troveTuple))
            callerGlobals['loadedTroves'].extend(recipe._loadedTroves)
            callerGlobals['loadedTroves'].append(troveTuple)
            callerGlobals['loadedSpecs'][troveSpec] = (troveTuple, recipe)

    # stash a reference to the module in the namespace
    # of the recipe that loaded it, or else it will be destroyed
    callerGlobals[loader.recipe.__module__] = loader

def _getLoaderForInstalledRecipe(troveName, versionStr, flavor, cfg, repos, db,
        buildFlavor):
    loader = oldBuildFlavor = None
    recipeFile = os.path.join(cfg.baseClassDir, troveName + '.recipe')
    if os.path.exists(recipeFile):
        # existence of recipe path is not enough, verify the trovespec
        recipeTrvs = db.iterTrovesByPath(recipeFile)
        match = False
        if recipeTrvs and recipeTrvs[0]:
            try:
                # the only reason to use findTrove is to take advantage
                # of existing version matching logic
                nvf = db.findTrove(None, \
                        (recipeTrvs[0].getName(), versionStr, None))
                match = bool([x for x in nvf \
                            if x == recipeTrvs[0].getNameVersionFlavor()])
            except errors.TroveNotFound:
                pass

        if match:
            if flavor is not None and not flavor.isEmpty():
                if buildFlavor is None:
                    oldBuildFlavor = cfg.buildFlavor
                    use.setBuildFlagsFromFlavor()
                else:
                    oldBuildFlavor = buildFlavor
                    buildFlavor = deps.overrideFlavor(oldBuildFlavor, flavor)
                use.setBuildFlagsFromFlavor(troveName, buildFlavor, error=False)

            loader = RecipeLoader(recipeFile, cfg, repos = repos,
                                  ignoreInstalled = True,
                                  buildFlavor = buildFlavor, db = db)

            for recipeClassName in loader.recipes.keys():
                flvSuffix = str(nvf[0][2]) and "[%s]" % nvf[0][2] or ""
                log.info('Loaded %s from %s=%s%s' % (recipeClassName,
                                                     nvf[0][0], nvf[0][1],
                                                     flvSuffix))

    return loader, oldBuildFlavor

class RecipeLoaderFromSourceDirectory(RecipeLoaderFromSourceTrove):

    def __init__(self, trv, branch = None, cfg = None, repos = None,
                 ignoreInstalled = None, sourceFiles = None,
                 buildFlavor = None, labelPath = None):
        def getFile(repos, fileId, fileVersion, path):
            return open(path)

        if branch:
            versionStr = str(branch)
        else:
            versionStr = None

        RecipeLoaderFromSourceTrove.__init__(self, trv, repos, cfg,
                                             versionStr = versionStr,
                                             ignoreInstalled=ignoreInstalled,
                                             getFileFunction = getFile,
                                             branch = branch,
                                             buildFlavor = buildFlavor,
                                             parentDir = os.getcwd())

RecipeLoaderFromString._initClass()
