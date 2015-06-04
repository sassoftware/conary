#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import inspect
import itertools

from conary import files as files_mod, trove, versions
from conary.build import action, lookaside, source, policy
from conary.build import macros
from conary.build.errors import RecipeFileError, RecipeDependencyError
from conary.deps import deps
from conary.errors import ParseError
from conary.lib import log, util
from conary.lib.compat import any
from conary.local import database
from conary.conaryclient import cmdline

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
RECIPE_TYPE_CAPSULE   = 7

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

def isCapsuleRecipe(recipeClass):
    return recipeClass.getType() == RECIPE_TYPE_CAPSULE

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
        self.recipe.populateLcache()

class Recipe(object):
    """Virtual base class for all Recipes"""
    _trove = None
    _trackedFlags = None
    _recipeType = RECIPE_TYPE_UNKNOWN
    _isDerived = False
    _sourceModule = None
    COOK_TYPE_LOCAL = 0
    COOK_TYPE_REPOSITORY = 1

    buildRequires = []
    crossRequires = []
    buildRequirementsOverride = None
    crossRequirementsOverride = None


    def __init__(self, cfg, lightInstance=False, laReposCache=None,
                 srcdirs = None):
        if laReposCache is None:
            laReposCache = lookaside.RepositoryCache(None)
        assert(self.__class__ is not Recipe)
        self.validate()
        self.cfg = cfg
        self.externalMethods = {}
        # lightInstance for only instantiating, not running (such as checkin)
        self._lightInstance = lightInstance
        self._sources = []
        self.loadSourceActions()
        self.buildinfo = None
        self.metadataSkipSet = [
            'keyValue',
        ]
        self.laReposCache = laReposCache
        self.srcdirs = srcdirs
        self.sourcePathMap = {}
        self.pathConflicts = {}
        self._recordMethodCalls = False
        self.methodsCalled = []
        self.unusedMethods = set()
        self.methodDepth = 0
        self._pathTranslations = []
        self._repos = None
        self._capsulePathMap = {}
        self._capsulePackageMap = {}
        self._capsuleDataMap = {}
        self._capsules = {}
        self._lcstate = None
        self._propertyMap = {}

        baseMacros = loadMacros(cfg.defaultMacros)
        self.macros = macros.Macros(ignoreUnknown=lightInstance)
        self.macros.update(baseMacros)

        # Metadata is a hash keyed on a trove name and with a list of
        # per-trove-name MetadataItem like objects (well, dictionaries)
        self._metadataItemsMap = {}
        # Old metadata, keyed on trove name, with ((n, v, f), metadata, log)
        # as value
        self._oldMetadataMap = {}
        self._filteredKeyValueMetadata = set()
        # Multi-URL map, used for multiple URL support in addArchive et al
        self.multiurlMap = {}
        # Search method for sources
        self.cookType = self.COOK_TYPE_LOCAL

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
                                 'SourcePackageRecipe',
                                 'BaseRequiresRecipe',
                                 'GroupRecipe', '_GroupRecipe',
                                 'GroupSetRecipe', '_GroupSetRecipe',
                                 'RedirectRecipe',
                                 'AbstractDerivedPackageRecipe',
                                 'DerivedPackageRecipe',
                                 'AbstractDerivedCapsuleRecipe',
                                 'DerivedCapsuleRecipe',
                                 'FilesetRecipe', '_BaseGroupRecipe']:
                    continue
                setattr(self, itemName, self._wrapMethod(className, item))
                self.unusedMethods.add((className, item.__name__))

        # Inspected only when it is important to know for reporting
        # purposes what was specified in the recipe per se, and not
        # in superclasses or in defaultBuildRequires
        self._recipeRequirements = {
            'buildRequires': list(self.buildRequires),
            'crossRequires': list(self.crossRequires)
        }

        self._includeSuperClassBuildReqs()
        self._includeSuperClassCrossReqs()
        self.transitiveBuildRequiresNames = None
        self._subscribeLogPath = None
        self._subscribedPatterns = []
        self._logFile = None
        self._isCrossCompileTool = False
        self._isCrossCompiling = False

    def _getParentClass(self, className):
        klass = self.__class__
        while klass.__name__ != className:
            if klass is None:
                # None's base class is object. object's base class is None
                return None
            klass = klass.__base__
        return klass

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
        # wait to check build requires until the object is instantiated
        # so that we can include all of the parent classes' buildreqs
        # in the check

        for buildRequires in self.buildRequires:
            (n, vS, f) = cmdline.parseTroveSpec(buildRequires)
            if n.count(':') > 1:
                raise RecipeFileError("Build requirement '%s' cannot have two colons in its name" % (buildRequires))

            # we don't allow full version strings or just releases
            if vS and vS[0] not in ':@':
                raise RecipeFileError("Unsupported buildReq format %s" % buildRequires)


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

    def fetchAllSources(self, refreshFilter=None, skipFilter=None,
            withEphemeral=True):
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
            if not f:
                continue
            if src.ephemeral and not withEphemeral:
                continue

            if type(f) in (tuple, list):
                files.extend(f)
            else:
                files.append(f)
        return files

    def getSourcePathList(self, withEphemeral=True):
        return [ x for x in self._sources if isinstance(x, source._AnySource)
                and x.sourceDir is None
                and (withEphemeral or not x.ephemeral)
                ]

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
        class lcachePopulationState:
            """Used to track the state of the lcache to enable it to be
            efficiently populated on-demand"""
            classes=None
            sourcePaths={}
            completedActions = set()
            pathMap = {}

        if not self.laReposCache.repos:
            return

        if not self._lcstate:
            repos = self.laReposCache.repos
            self._lcstate = lcachePopulationState()

            # build a list containing this recipe class and any ancestor class
            # from which it descends
            recipeClass = self.__class__
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
            self._lcstate.classes = classes

            for rclass in self._lcstate.classes:
                if not rclass._trove:
                    continue
                srcName = rclass._trove.getName()
                srcVersion = rclass._trove.getVersion()
                # CNY-31: walk over the files in the trove we found upstream
                # (which we may have modified to remove the non-autosourced
                # files.
                # Also, if an autosource file is marked as needing to be
                # refreshed in the Conary state file, the lookaside cache has
                # to win, so don't populate it with the repository file)
                fileList = []
                for pathId, path, fileId, version in \
                        rclass._trove.iterFileList():

                    assert(path[0] != "/")
                    # we might need to retrieve this source file
                    # to enable a build, so we need to find the
                    # sha1 hash of it since that's how it's indexed
                    # in the file store
                    if isinstance(version, versions.NewVersion):
                        # don't try and look up things on the NewVersion label!
                        continue
                    fileList.append((pathId, path, fileId, version))

                fileObjs = repos.getFileVersions([ (x[0],x[2],x[3])
                                              for x in fileList])
                for i in range(len(fileList)):
                    fileObj = fileObjs[i]
                    if isinstance(fileObj, files_mod.RegularFile):
                        (pathId, path, fileId, version) = fileList[i]
                        self._lcstate.pathMap[path] = (srcName, srcVersion,
                            pathId, path, fileId, version, fileObj)

        # populate the repository source lookaside cache from the :source
        # components
        sourcePaths = self._lcstate.sourcePaths
        actions = set(self.getSourcePathList())-self._lcstate.completedActions
        for a in actions:
            self._lcstate.completedActions.add(a)
            ps = a.getPathAndSuffix()

            # check if we have an autosourced file
            if any(x in ps[0] for x in ['://', ':pserver:', ':ext:']):
                # use guess name if it is provided
                k = os.path.basename(ps[0]) or ps[1]
                assert(k)
                sourcePaths[ k ] = ps
            else:
                sourcePaths[ ps[0] ] = ps
                if a.ephemeral:
                    raise RecipeFileError("File '%s' is marked as ephemeral "
                            "but is not autosourced" % (ps[0],))

        pathMap = self._lcstate.pathMap
        delList = []
        for path in pathMap:
            fullPath = None
            if path in sourcePaths:
                fullPath = lookaside.laUrl(sourcePaths[path][0]).filePath()
            elif path.find("/") == -1:  # we might have a guessed name
                for k in sourcePaths:
                    if k and path.startswith(k) and sourcePaths[k][2]:
                        for sk in sourcePaths[k][2]:
                            if path.endswith(sk) and \
                                    len(k) + len(sk) == len(path)-1:
                                fullUrl = sourcePaths[k][0]+k+'.'+sk
                                fullPath = \
                                    lookaside.laUrl(fullUrl).filePath()

            (srcName, srcVersion, pathId, path, fileId, version, fileObj) = \
                pathMap[path]
            if not fullPath:
                if fileObj.flags.isAutoSource():
                    continue
                else:
                    fullPath = path
            self.laReposCache.addFileHash(fullPath, srcName,
                srcVersion, pathId, path, fileId, version,
                fileObj.contents.sha1(), fileObj.inode.perms())
            delList.append(path)

        for path in delList:
            if path in sourcePaths:
                del sourcePaths[path]
            del pathMap[path]

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
        if localOnly:
            kw = dict(searchMethod=self.fileFinder.SEARCH_LOCAL_ONLY)
        else:
            kw = {}

        inRepos, f = self.fileFinder.fetch(sourceName,
                                           refreshFilter=refreshFilter,
                                           allowNone=True, **kw)
        return f

    def _addMetadataItem(self, troveNames, metadataItemDict):
        assert isinstance(metadataItemDict, dict)
        for troveName in troveNames:
            self._metadataItemsMap.setdefault(troveName,
                                              []).append(metadataItemDict)

    def _setOldMetadata(self, metadataMap):
        self._oldMetadataMap = metadataMap

    def _getOldMetadata(self):
        return self._oldMetadataMap

    def _addProperty(self, ptype, main, comp, xml):
        l = self._propertyMap.setdefault((main, comp), [])
        l.append((ptype, xml))

    def _setProperties(self, trv, main, comp):
        key = (main, comp)
        for ptype, xml in self._propertyMap.get(key, []):
            trv.troveInfo.properties.add(ptype, xml)

    def needsCrossFlags(self):
        return self._isCrossCompileTool or self._isCrossCompiling

    def checkBuildRequirements(self, cfg, sourceVersion, raiseError=True):
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
            troves.sort(key = lambda x: x.getVersion())
            if flavor is None:
                # get latest
                return troves[-1]
            for trove in troves:
                troveFlavor = trove.getFlavor()
                if troveFlavor.stronglySatisfies(flavor):
                    return trove

        def _matchReqs(reqList, db):
            reqMap = {}
            missingReqs = []
            for buildReq in reqList:
                (name, versionStr, flavor) = cmdline.parseTroveSpec(buildReq)
                # XXX move this to use more of db.findTrove's features, instead
                # of hand parsing
                troves = db.trovesByName(name)
                troves = db.getTroves(troves)

                versionMatches =  _filterBuildReqsByVersionStr(versionStr, troves)

                if not versionMatches:
                    missingReqs.append(buildReq)
                    continue
                match = _filterBuildReqsByFlavor(flavor, versionMatches)
                if match:
                    reqMap[buildReq] = match
                else:
                    missingReqs.append(buildReq)
            return reqMap, missingReqs


        db = database.Database(cfg.root, cfg.dbPath)


        if self.needsCrossFlags() and self.crossRequires:
            if not self.macros.sysroot:
                err = ("cross requirements needed but %(sysroot)s undefined")
                if raiseError:
                    log.error(err)
                    raise RecipeDependencyError(err)
                else:
                    log.warning(err)
                    self.buildReqMap = {}
                    self.ignoreDeps = True
                    return

            if self.cfg.root != '/':
                sysroot = self.cfg.root + self.macros.sysroot
            else:
                sysroot = self.macros.sysroot
            if not os.path.exists(sysroot):
                err = ("cross requirements needed but sysroot (%s) does not exist" % (sysroot))
                if raiseError:
                    raise RecipeDependencyError(err)
                else:
                    log.warning(err)
                    self.buildReqMap = {}
                    self.ignoreDeps = True
                    return

            else:
                crossDb = database.Database(sysroot, cfg.dbPath)

        reqMap, missingReqs = _matchReqs(self.buildRequires, db)
        if self.needsCrossFlags() and self.crossRequires:
            crossReqMap, missingCrossReqs = _matchReqs(self.crossRequires,
                                                       crossDb)
        else:
            missingCrossReqs = []
            crossReqMap = {}

        if missingReqs or missingCrossReqs:
            if missingReqs:
                err = ("Could not find the following troves "
                       "needed to cook this recipe:\n"
                       "%s" % '\n'.join(sorted(missingReqs)))
                if missingCrossReqs:
                    err += '\n'
            else:
                err = ''
            if missingCrossReqs:
                err += ("Could not find the following cross requirements"
                        " (that must be installed in %s) needed to cook this"
                        " recipe:\n"
                        "%s" % (sysroot, '\n'.join(sorted(missingCrossReqs))))
            if raiseError:
                log.error(err)
                raise RecipeDependencyError('unresolved build dependencies')
            else:
                log.warning(err)
        self.buildReqMap = reqMap
        self.crossReqMap = crossReqMap
        self.ignoreDeps = not raiseError

    def _getTransitiveDepClosure(self, targets=None):
        def isTroveTarget(trove):
            if targets is None:
                return True
            return trove.getName() in targets

        db = database.Database(self.cfg.root, self.cfg.dbPath)

        reqList =  [ req for req in self.getBuildRequirementTroves(db)
                     if isTroveTarget(req) ]
        reqNames = set(req.getName() for req in reqList)
        depSetList = [ req.getRequires() for req in reqList ]
        d = db.getTransitiveProvidesClosure(depSetList)
        for depSet in d:
            reqNames.update(
                set(troveTup[0] for troveTup in d[depSet]))

        return reqNames

    def _getTransitiveBuildRequiresNames(self):
        if self.transitiveBuildRequiresNames is not None:
            return self.transitiveBuildRequiresNames

        self.transitiveBuildRequiresNames = self._getTransitiveDepClosure()
        return self.transitiveBuildRequiresNames

    def getBuildRequirementTroves(self, db):
        if self.buildRequirementsOverride is not None:
            return db.getTroves(self.buildRequirementsOverride,
                                withFiles=False)
        return self.buildReqMap.values()

    def getCrossRequirementTroves(self):
        if self.crossRequirementsOverride:
            db = database.Database(self.cfg.root, self.cfg.dbPath)
            return db.getTroves(self.crossRequirementsOverride,
                                     withFiles=False)
        return self.crossRequires.values()

    def getRecursiveBuildRequirements(self, db, cfg):
        if self.buildRequirementsOverride is not None:
            return self.buildRequirementsOverride
        buildReqs = self.getBuildRequirementTroves(db)
        buildReqs = set((x.getName(), x.getVersion(), x.getFlavor())
                        for x in buildReqs)
        packageReqs = [ x for x in self.buildReqMap.itervalues()
                        if trove.troveIsCollection(x.getName()) ]
        for package in packageReqs:
            childPackages = [ x for x in package.iterTroveList(strongRefs=True,
                                                               weakRefs=True) ]
            hasTroves = db.hasTroves(childPackages)
            buildReqs.update(x[0] for x in itertools.izip(childPackages,
                                                          hasTroves) if x[1])
        buildReqs = self._getRecursiveRequirements(db, buildReqs, cfg.flavor)
        return buildReqs

    def _getRecursiveRequirements(self, db, troveList, flavorPath):
        # gets the recursive requirements for the listed packages
        seen = set()
        while troveList:
            depSetList = []
            for trv in db.getTroves(list(troveList), withFiles=False):
                required = deps.DependencySet()
                oldRequired = trv.getRequires()
                [ required.addDep(*x) for x in oldRequired.iterDeps()
                  if x[0] != deps.AbiDependency ]
                depSetList.append(required)
            seen.update(troveList)
            sols = db.getTrovesWithProvides(depSetList, splitByDep=True)
            troveList = set()
            for depSetSols in sols.itervalues():
                for depSols in depSetSols:
                    bestChoices = []
                    # if any solution for a dep is satisfied by the installFlavor
                    # path, then choose the solutions that are satisfied as
                    # early as possible on the flavor path.  Otherwise return
                    # all solutions.
                    for flavor in flavorPath:
                        bestChoices = [ x for x in depSols if flavor.satisfies(x[2])]
                        if bestChoices:
                            break
                    if bestChoices:
                        depSols = set(bestChoices)
                    else:
                        depSols = set(depSols)
                    depSols.difference_update(seen)
                    troveList.update(depSols)
        return seen

    def setRepos(self, repos):
        self._repos = repos

    def getRepos(self):
        return self._repos

    def isatty(self, value=None):
        if value is not None:
            self._tty = value
        return self._tty

    def _setSubscribeLogPath(self, path):
        self._subscribeLogPath = path

    def getSubscribeLogPath(self):
        return self._subscribeLogPath

    def _setLogFile(self, logFile):
        self._logFile = logFile
        for pattern in self._subscribedPatterns:
            logFile.subscribe(pattern)
        self._subscribedPatterns = None

    def subscribeLogs(self, pattern):
        if self._logFile:
            self._logFile.subscribe(pattern)
        else:
            self._subscribedPatterns.append(pattern)

    def synchronizeLogs(self):
        if self._logFile:
            self._logFile.synchronize()

    def _includeSuperClassBuildReqs(self):
        self._includeSuperClassItemsForAttr('buildRequires')

    def _includeSuperClassCrossReqs(self):
        self._includeSuperClassItemsForAttr('crossRequires')

    def _includeSuperClassItemsForAttr(self, attr):
        """ Include build requirements from super classes by searching
            up the class hierarchy for buildRequires.  You can
            override this currently only by calling
            <superclass>.buildRequires.remove()
        """
        buildReqs = set()
        superBuildReqs = set()
        immediateSuper = True
        for base in inspect.getmro(self.__class__):
            thisClassReqs = getattr(base, attr, [])
            buildReqs.update(thisClassReqs)
            if base != self.__class__:
                if immediateSuper:
                    if (set(self._recipeRequirements[attr]) ==
                        set(getattr(base, attr, []))):
                        # requirements in recipe were inherited,
                        # not explicitly specified, so report
                        # them as if recipe explicitly contained
                        # an empty list
                        self._recipeRequirements[attr] = []
                    # We have now inspected the immediate superclass
                    immediateSuper = False
                superBuildReqs.update(thisClassReqs)
        setattr(self, attr, list(buildReqs))
        self._recipeRequirements['%sSuper' %attr] = superBuildReqs


    def _validatePathInfoForCapsule(self, fileData, ignorePaths=set()):
        for fileName, fileDatum in [(x[0], x[1:]) for x in fileData
                                    if x[0] not in ignorePaths and
                                       x[0] in self._capsuleDataMap]:
            oldDatum, _ = self._capsuleDataMap[fileName][-1]
            # Ignore mtime
            oldDatum = oldDatum[:4]
            fileDatum = fileDatum[:4]
            if oldDatum != fileDatum:
                self.reportErrors(
                    'file %s added with conflicting contents:'
                    ' %s:%s 0%0o %s != %s:%s 0%0o %s',
                    *((fileName,)+oldDatum+fileDatum))

    def _setPathInfoForCapsule(self, capsulePath, fileData, packageName):
        '''creates a map of contained filePaths to the capsule'''
        for fileDatum in fileData:
            fileName = fileDatum[0]
            fileInfo = fileDatum[1:6]
            l = self._capsuleDataMap.setdefault(fileName, [])
            l.append((fileInfo, packageName))
        for path in [x[0] for x in fileData]:
            l = self._capsulePathMap.setdefault(path, [])
            l.append(capsulePath)

    def _getCapsulePathsForFile(self, path):
        '''returns list of paths to capsule files from which this path came'''
        return self._capsulePathMap.get(path)

    def _addCapsule(self, capsulePath, capsuleType, capsulePackage):
        '''records a capsule associated with recipe'''
        self._capsules[capsulePackage] = (capsuleType, capsulePath)
        self._capsulePackageMap[capsulePath] = capsulePackage

    def _getCapsulePackage(self, capsulePath):
        '''returns the capsule package:component associated with a capsule
        path'''
        return self._capsulePackageMap.get(capsulePath)

    def _getCapsule(self, capsulePackage):
        '''returns the type and path to file for a capsule'''
        return self._capsules.get(capsulePackage)

    def _hasCapsulePackage(self, capsulePackage):
        return capsulePackage in self._capsules

    def _iterCapsulePaths(self):
        '''
        yields a (filePath, capsulePath, 'package:component') tuple
        for each file in each capsule that has been added
        '''
        for filePath, capsuleList in self._capsulePathMap.iteritems():
            for capsulePath in capsuleList:
                yield filePath, capsulePath, \
                    self._getCapsulePackage(capsulePath)

    def _iterCapsulePathData(self):
        '''
        yields a (filePath, package, user, group, mode, mtime) tuple
        for each file in each capsule that has been added
        '''
        for fileName, fileData in self._capsuleDataMap.iteritems():
            for fileDatum in fileData:
                fileInfo, package = fileDatum
                yield fileName, package, fileInfo[0], fileInfo[1], fileInfo[2], fileInfo[4]

    def getProductDefinitionVersion(self):
        key = 'productDefinitionVersion'
        if key in self.macros:
            return self.macros[key]
        return None

