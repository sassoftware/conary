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
import inspect

from conary import files, versions
from conary.errors import ParseError
from conary.build import action, lookaside, source, policy
from conary.build.errors import RecipeFileError
from conary.lib import log, util

import glob
import imp
import os
import sys

"""
Contains the base Recipe class
"""
RECIPE_TYPE_UNKNOWN   = 0
RECIPE_TYPE_PACKAGE   = 1
RECIPE_TYPE_FILESET   = 2
RECIPE_TYPE_GROUP     = 3
RECIPE_TYPE_INFO      = 4
RECIPE_TYPE_REDIRECT  = 5
RECIPE_TYPE_FACTORY   = 6

class _policyUpdater:
    def __init__(self, theobject):
        self.theobject = theobject
    def __call__(self, *args, **keywords):
        self.theobject.updateArgs(*args, **keywords)

def _ignoreCall(*args, **kw):
    pass

def isPackageRecipe(recipeClass):
    return recipeClass.getType() == RECIPE_TYPE_PACKAGE

def isFileSetRecipe(recipeClass):
    return recipeClass.getType() == RECIPE_TYPE_FILESET

def isGroupRecipe(recipeClass):
    return recipeClass.getType() == RECIPE_TYPE_GROUP

def isInfoRecipe(recipeClass):
    return recipeClass.getType() == RECIPE_TYPE_INFO

def isRedirectRecipe(recipeClass):
    return recipeClass.getType() == RECIPE_TYPE_REDIRECT

def isFactoryRecipe(recipeClass):
    return recipeClass.getType() == RECIPE_TYPE_FACTORY

def loadMacros(paths):
    '''
    Load default macros from a series of I{paths}.

    @rtype: dict
    @return: A dictionary of default macros
    '''

    baseMacros = {}
    loadPaths = []
    for path in paths:
        globPaths = sorted(list(glob.glob(path)))
        loadPaths.extend(globPaths)

    for path in loadPaths:
        compiledPath = path+'c'
        deleteCompiled = not util.exists(compiledPath)
        macroModule = imp.load_source('tmpmodule', path)
        if deleteCompiled:
            util.removeIfExists(compiledPath)
        baseMacros.update(x for x in macroModule.__dict__.iteritems()
                          if not x[0].startswith('__'))

    return baseMacros

class _sourceHelper:
    def __init__(self, theclass, recipe):
        self.theclass = theclass
	self.recipe = recipe
    def __call__(self, *args, **keywords):
        self.recipe._sources.append(self.theclass(self.recipe, *args, **keywords))

class Recipe(object):
    """Virtual base class for all Recipes"""
    _trove = None
    _trackedFlags = None
    _recipeType = RECIPE_TYPE_UNKNOWN
    _isDerived = False
    _sourceModule = None

    def __init__(self, lightInstance = False, laReposCache = None,
                 srcdirs = None):
        assert(self.__class__ is not Recipe)
        self.validate()
        self.externalMethods = {}
        # lightInstance for only instantiating, not running (such as checkin)
        self._lightInstance = lightInstance
        self._sources = []
        self.loadSourceActions()
        self.buildinfo = None
        self.metadataSkipSet = []
        self.laReposCache = laReposCache
        self.srcdirs = srcdirs
        self.sourcePathMap = {}
        self.pathConflicts = {}
        self._recordMethodCalls = False
        self.methodsCalled = []
        self.unusedMethods = set()
        self.methodDepth = 0
        self._pathTranslations = []

        superClasses = self.__class__.__mro__

        for itemName in dir(self):
            if itemName[0] == '_':
                continue
            item = getattr(self, itemName)
            if inspect.ismethod(item):
                if item.im_class == type:
                    # classmethod
                    continue
                className = self.__class__.__name__
                for class_ in superClasses:
                    classItem = getattr(class_, itemName, None)
                    if classItem is None:
                        continue
                    if classItem.im_func == item.im_func:
                        className = class_.__name__
                if className in ['Recipe', 'AbstractPackageRecipe',
                                 'GroupRecipe', 'RedirectRecipe', 
                                 'DerivedPackageRecipe', 'FilesetRecipe',
                                 '_BaseGroupRecipe']:
                    continue
                setattr(self, itemName, self._wrapMethod(className, item))
                self.unusedMethods.add((className, item.__name__))

    @classmethod
    def getType(class_):
        return class_._recipeType

    def _wrapMethod(self, className, method):
        def _callWrapper(*args, **kw):
            return self._recordMethod(className, method, *args, **kw)
        return _callWrapper

    def _recordMethod(self, className, method, *args, **kw):
        if self._recordMethodCalls:
            self.methodDepth += 1
            self.methodsCalled.append((self.methodDepth, className,
                                       method.__name__))
        rv = method(*args, **kw)
        if self._recordMethodCalls:
            self.unusedMethods.discard((className, method.__name__))
            self.methodDepth -= 1
        return rv

    def recordCalls(self, method, *args, **kw):
        self._recordMethodCalls = True
        try:
            return method(*args, **kw)
        finally:
            self._recordMethodCalls = False

    def __repr__(self):
        return "<%s Object>" % self.__class__

    @classmethod
    def validateClass(class_):
        if class_.version == '':
            raise ParseError("empty release string")

    def validate(self):
        pass

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
            externalMethod = self.externalMethods.get(name, None)
            if externalMethod is not None:
                return externalMethod

            if self._lightInstance:
                return _ignoreCall

        return object.__getattribute__(self, name)

    def _addSourceAction(self, name, item):
        self.externalMethods[name] = _sourceHelper(item, self)

    def _loadSourceActions(self, test):
        for name, item in source.__dict__.items():
            if (name[0:3] == 'add' and issubclass(item, action.Action)
                    and test(item)):
                self._addSourceAction(name, item)

    def loadSourceActions(self):
        pass

    def fetchLocalSources(self):
        """
            Return locations for all files that are not autosourced.
            Useful for determining where used in the recipe are located.
        """
        files = []
        for src in self.getSourcePathList():
            f = src.fetchLocal()
            if f:
                if type(f) in (tuple, list):
                    files.extend(f)
                else:
                    files.append(f)
        return files

    def fetchAllSources(self, refreshFilter=None, skipFilter=None):
        """
        returns a list of file locations for all the sources in
        the package recipe
        """
        # first make sure we had no path conflicts:
        if self.pathConflicts:
            errlist = []
            for basepath in self.pathConflicts.keys():
                errlist.extend([x for x in self.pathConflicts[basepath]])
            raise RecipeFileError("The following file names conflict "
                                  "(cvc does not currently support multiple"
                                  " files with the same name from different"
                                  " locations):\n   " + '\n   '.join(errlist))
        self.prepSources()
        files = []
        for src in self.getSourcePathList():
            if skipFilter and skipFilter(os.path.basename(src.getPath())):
                continue

            f = src.fetch(refreshFilter)
            if f:
                if type(f) in (tuple, list):
                    files.extend(f)
                else:
                    files.append(f)
        return files

    def getSourcePathList(self):
        return [ x for x in self._sources if isinstance(x, source._AnySource)
                and x.__dict__.get('sourceDir') is None]

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

    def unpackSources(self, resume=None, downloadOnly=False):
        if resume == 'policy':
            return
        elif resume:
            log.info("Resuming on line(s) %s" % resume)
            # note resume lines must be in order
            self.processResumeList(resume)
            for source in self.iterResumeList(self._sources):
                source.doPrep()
                source.doAction()
        elif downloadOnly:
            for source in self._sources:
                source.doPrep()
                source.doDownload()
        else:
            for source in self._sources:
                source.doPrep()
                source.doAction()

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
            if issubclass(parent, Recipe):
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
            # CNY-31: walk over the files in the trove we found upstream
            # (which we may have modified to remove the non-autosourced files
            # Also, if an autosource file is marked as needing to be refreshed
            # in the Conary state file, the lookaside cache has to win, so
            # don't populate it with the repository file)
            for pathId, path, fileId, version in rclass._trove.iterFileList():
                assert(path[0] != "/")
                # we might need to retrieve this source file
                # to enable a build, so we need to find the
                # sha1 hash of it since that's how it's indexed
                # in the file store
                if isinstance(version, versions.NewVersion):
                    # don't try and look up things on the NewVersion label!
                    continue

                fileObj = repos.getFileVersion(pathId, fileId, version)
                if isinstance(fileObj, files.RegularFile):
                    # it only makes sense to fetch regular files, skip
                    # anything that isn't
                    self.laReposCache.addFileHash(srcName, srcVersion, pathId,
                        path, fileId, version, fileObj.contents.sha1())

    def sourceMap(self, path):
        if os.path.exists(path):
            basepath = path
        else:
            basepath = os.path.basename(path)
        if basepath in self.sourcePathMap:
            if self.sourcePathMap[basepath] == path:
                # we only care about truly different source locations with the
                # same basename
                return
            if basepath in self.pathConflicts:
                self.pathConflicts[basepath].add(path)
            else:
                self.pathConflicts[basepath] = set([
                    # previous (first) instance
                    self.sourcePathMap[basepath],
                    # this instance
                    path
                ])
        else:
            self.sourcePathMap[basepath] = path

    def isCrossCompileTool(self):
        return False

    def recordMove(self, src, dest):
        destdir = util.normpath(self.macros.destdir)
        def _removeDestDir(p):
            p = util.normpath(p)
            if p[:len(destdir)] == destdir:
                return p[len(destdir):]
            else:
                return p
        if os.path.isdir(src):
            # assume move is about to happen
            baseDir = src
            postRename = False
        elif os.path.isdir(dest):
            # assume move just happened
            baseDir = dest
            postRename = True
        else:
            # don't walk directories
            baseDir = None
        src = _removeDestDir(src)
        dest = _removeDestDir(dest)
        self._pathTranslations.append((src, dest))
        if baseDir:
            for base, dirs, files in os.walk(baseDir):
                for path in dirs + files:
                    if not postRename:
                        fSrc = os.path.join(base, path)
                        fSrc = fSrc.replace(self.macros.destdir, '')
                        fDest = fSrc.replace(src, dest)
                    else:
                        fDest = os.path.join(base, path)
                        fDest = fDest.replace(self.macros.destdir, '')
                        fSrc = fDest.replace(dest, src)
                    self._pathTranslations.append((fSrc, fDest))

    def move(self, src, dest):
        self.recordMove(src, dest)
        util.move(src, dest)

    def loadPolicy(self, policySet = None, internalPolicyModules = None):
        if internalPolicyModules is None:
            internalPolicyModules = self.internalPolicyModules
        (self._policyPathMap, self._policies) = \
                policy.loadPolicy(self, policySet = policySet,
                              internalPolicyModules = internalPolicyModules,
                              basePolicy = self.basePolicyClass)
        # create bucketless name->policy map for getattr
        policyList = []
        for bucket in self._policies.keys():
            policyList.extend(self._policies[bucket])
        self._policyMap = dict((x.__class__.__name__, x) for x in policyList)
        # Some policy needs to pass arguments to other policy at init
        # time, but that can't happen until after all policy has been
        # initialized
        for name, policyObj in self._policyMap.iteritems():
            self.externalMethods[name] = _policyUpdater(policyObj)
        # must be a second loop so that arbitrary policy cross-reference
        # works; otherwise it is dependent on sort order whether or
        # not it works
        for name, policyObj in self._policyMap.iteritems():
            policyObj.postInit()

        # returns list of policy files loaded
        return self._policyPathMap.keys()

    def doProcess(self, bucketName, logFile = sys.stdout):
        policyBucket = policy.__dict__[bucketName]
        formattedLog = False
        if hasattr(logFile, 'pushDescriptor'):
            formattedLog = True
            logFile.pushDescriptor(bucketName)
        try:
            for post in self._policies[policyBucket]:
                if formattedLog:
                    logFile.pushDescriptor(post.__class__.__name__)
                try:
                    logFile.write('Running policy: %s\r' % \
                            post.__class__.__name__)
                    logFile.flush()
                    post.doProcess(self)
                    post.postPolicy()
                finally:
                    if formattedLog:
                        logFile.popDescriptor(post.__class__.__name__)
        finally:
            if formattedLog:
                logFile.popDescriptor(bucketName)

    def _fetchFile(self, sourceName, refreshFilter = None, localOnly = False):
        f = lookaside.findAll(self.cfg, self.laReposCache,
            sourceName, self.name, self.srcdirs,
            refreshFilter = refreshFilter, localOnly = localOnly,
            allowNone = True)
        return f

