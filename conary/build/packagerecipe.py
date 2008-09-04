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

import os
import inspect
import itertools

from conary.build.recipe import Recipe, RECIPE_TYPE_PACKAGE, loadMacros
from conary.build import defaultrecipes
from conary.build.errors import RecipeFileError
from conary import trove

from conary.build import action
from conary.build import build
from conary.build import errors
from conary.build import macros
from conary.build import policy
from conary.build import use
from conary.conaryclient import cmdline
from conary.deps import deps
from conary.lib import log, magic, util
from conary.local import database

from conary.repository import errors as repoerrors



crossMacros = {
    'crossdir'          : 'cross-target-%(target)s',
    'crossprefix'	: '/opt/%(crossdir)s',
    'sysroot'		: '%(crossprefix)s/sys-root',
    'headerpath'	: '%(sysroot)s%(includedir)s'
}


class _recipeHelper:
    def __init__(self, list, recipe, theclass):
        self.list = list
        self.theclass = theclass
	self.recipe = recipe
    def __call__(self, *args, **keywords):
        self.list.append(self.theclass(self.recipe, *args, **keywords))

def clearBuildRequires(*buildReqs):
    """ Clears inherited build requirement lists of a given set of packages,
        or all packages if none listed.
    """
    _clearReqs('buildRequires', buildReqs)

def clearBuildReqs(*buildReqs):
    #log.warning('clearBuildReqs() is deprecated.  Use clearBuildRequires()')
    clearBuildRequires(*buildReqs)

def clearCrossRequires(*crossReqs):
    """ Clears inherited build requirement lists of a given set of packages,
        or all packages if none listed.
    """
    _clearReqs('crossRequires', crossReqs)

def clearCrossReqs(*crossReqs):
    #log.warning('clearCrossReqs() is deprecated.  Use clearCrossRequires()')
    clearCrossRequires(*crossReqs)

def _clearReqs(attrName, reqs):
    # walk the stack backwards until we find the frame
    # that looks like a recipe frame.  loadrecipe sets up
    # a __localImportModules dictionary in the global space
    # of the module that is created for the recipe.  PackageRecipe
    # should also be a global in the frame.
    # First get the stack.  Specify 0 lines of context to avoid tripping
    # up if source files are not available.
    stack = inspect.stack(0)
    # now get the innermost frame, which is the first element of
    # the stack list.
    frame = stack.pop(0)[0]
    while stack:
        callerGlobals = frame.f_globals
        if ('PackageRecipe' in callerGlobals
            and '__localImportModules' in callerGlobals):
            # if we have PackageRecipe and __localImportModules, we
            # found the most likely candidate for the recipe frame
            break
        # try the next frame up
        frame = stack.pop(0)[0]
    if not stack:
        raise RuntimeError('unable to determine the frame that is '
                           'creating the recipe class')
    # get a list of all classes that are derived from AbstractPackageRecipe
    classes = []
    for value in callerGlobals.itervalues():
        if inspect.isclass(value) and issubclass(value, Recipe):
            if 'AbstractPackageRecipe' in [x.__name__ for x in value.mro()]:
                classes.append(value)

    # define a convenience function for removing buildReqs from a list
    # or clearing them.
    def _removePackages(class_, pkgs):
        # if no specific buildReqs were mentioned to remove, remove them all
        if not pkgs:
            setattr(class_, attrName, [])
            return
        # get the set of packages to remove
        buildReqs = set(getattr(class_, attrName))
        remove = set(pkgs)
        buildReqs = buildReqs - remove
        setattr(class_, attrName, list(buildReqs))

    for class_ in classes:
        _removePackages(class_, reqs)

        for base in inspect.getmro(class_):
            if issubclass(base, Recipe) and base not in classes:
                if 'AbstractPackageRecipe' in [x.__name__ for x in base.mro()]:
                    _removePackages(base, reqs)

crossFlavor = deps.parseFlavor('cross')
def getCrossCompileSettings(flavor):
    flavorTargetSet = flavor.getDepClasses().get(deps.DEP_CLASS_TARGET_IS, None)
    if flavorTargetSet is None:
        return None

    targetFlavor = deps.Flavor()
    for insSet in flavorTargetSet.getDeps():
        targetFlavor.addDep(deps.InstructionSetDependency, insSet)
    isCrossTool = flavor.stronglySatisfies(crossFlavor)
    return None, targetFlavor, isCrossTool

class AbstractPackageRecipe(Recipe):
    buildRequires = []
    crossRequires = []
    buildRequirementsOverride = None
    crossRequirementsOverride = None
    _derivedFrom = []

    Flags = use.LocalFlags
    explicitMainDir = False

    internalAbstractBaseClass = 1
    _recipeType = RECIPE_TYPE_PACKAGE
    internalPolicyModules = ( 'destdirpolicy', 'packagepolicy')
    basePolicyClass = policy.Policy

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

    def mainDir(self, new=None, explicit=True):
	if new:
	    self.theMainDir = new % self.macros
	    self.macros.maindir = self.theMainDir
            self.explicitMainDir |= explicit
            if explicit:
                if self.buildinfo:
                    self.buildinfo.maindir = self.theMainDir
	return self.theMainDir

    def nameVer(self):
	return '-'.join((self.name, self.version))

    def cleanup(self, builddir, destdir):
	if self.cfg.cleanAfterCook:
	    util.rmtree(builddir)

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
                    raise errors.RecipeDependencyError(err)
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
                    raise errors.RecipeDependencyError(err)
                else:
                    log.warning(err)
                    self.buildReqMap = {}
                    self.ignoreDeps = True
                    return

            else:
                crossDb = database.Database(sysroot, cfg.dbPath)
        time = sourceVersion.timeStamps()[-1]

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
                raise errors.RecipeDependencyError(
                                            'unresolved build dependencies')
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

    def loadSourceActions(self):
        self._loadSourceActions(lambda item: item._packageAction is True)

    def _addBuildAction(self, name, item):
        self.externalMethods[name] = _recipeHelper(self._build, self, item)

    def getPackages(self):
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

    def __delattr__(self, name):
	"""
	Allows us to delete policy items from their respective lists
	by deleting a name in the recipe self namespace.  For example,
	to remove the AutoDoc package policy from the package policy
	list, one could do::
         del r.AutoDoc
	This would prevent the AutoDoc package policy from being
	executed.

	In general, delete policy only as a last resort; you can
	usually disable policy entirely with the keyword argument::
	 exceptions='.*'
	"""
        if name in self._policyMap:
            policyObj = self._policyMap[name]
            bucket = policyObj.bucket
            if bucket in (policy.TESTSUITE,
                          policy.DESTDIR_PREPARATION,
                          policy.PACKAGE_CREATION,
                          policy.ERROR_REPORTING):
                # cannot delete conary internal policy
                return
            self._policies[bucket] = [x for x in self._policies[bucket]
                                      if x is not policyObj]
            del self._policyMap[policyObj.__class__.__name__]
            del self.externalMethods[name]
            return
	del self.__dict__[name]

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

    def setCrossCompile(self, (crossHost, crossTarget, isCrossTool)):
        """ Tell conary it should cross-compile, or build a part of a
            cross-compiler toolchain.

            Example: setCrossCompile(('x86-foo-linux', 'x86_64', False))

            @param crossHost: the architecture of the machine the built binary
                 should run on.  Can be either <arch> or <arch>-<vendor>-<os>.
                 If None, determine crossHost based on isCrossTool value.
            @param crossTarget: the architecture of the machine the built
                 binary should be targeted for.
                 Can be either <arch> or <arch>-<vendor>-<os>.
            @param isCrossTool: If true, we are building a cross-compiler for
                 use on this system.  We set values so that the resulting
                 binaries from this build should be runnable on the build
                 architecture.
        """
        def _parseArch(archSpec, target=False):
            if isinstance(archSpec, deps.Flavor):
                return archSpec, None, None

            if '-' in archSpec:
                arch, vendor, hostOs = archSpec.split('-')
            else:
                arch  = archSpec
                vendor = hostOs = None

            try:
                if target:
                    flavor = deps.parseFlavor('target: ' + arch)
                else:
                    flavor = deps.parseFlavor('is: ' + arch)
            except deps.ParseError, msg:
                raise errors.CookError('Invalid architecture specification %s'
                                       %archSpec)

            if flavor is None:
                raise errors.CookError('Invalid architecture specification %s'
                                       %archSpec)
            return flavor, vendor, hostOs

        def _setArchFlags(flavor):
            # given an flavor, make use.Arch match that flavor.
            for flag in use.Arch._iterAll():
                flag._set(False)
            use.setBuildFlagsFromFlavor(self.name, flavor, error=False)

        def _setTargetMacros(crossTarget, macros):
            targetFlavor, vendor, targetOs = _parseArch(crossTarget)
            if vendor:
                macros['targetvendor'] = vendor
            if targetOs:
                macros['targetos'] = targetOs
            _setArchFlags(targetFlavor)
            self.targetFlavor = deps.Flavor()
            targetDeps = targetFlavor.iterDepsByClass(
                                            deps.InstructionSetDependency)
            self.targetFlavor.addDeps(deps.TargetInstructionSetDependency,
                                      targetDeps)
            macros['targetarch'] = use.Arch._getMacro('targetarch')
            archMacros = use.Arch._getMacros()
            # don't override values we've set for crosscompiling
            archMacros.pop('targetarch', False)
            macros.update(archMacros)

        def _setHostMacros(crossHost, macros):
            hostFlavor, vendor, hostOs = _parseArch(crossHost)
            if vendor:
                macros['hostvendor'] = vendor
            if hostOs:
                macros['hostos'] = hostOs

            _setArchFlags(hostFlavor)
            macros['hostarch'] = use.Arch._getMacro('targetarch')
            macros['hostmajorarch'] = use.Arch.getCurrentArch()._name
            self.hostmacros = _createMacros('%(host)s', hostOs)

        def _setBuildMacros(macros):
            # get the necessary information about the build system
            # the only information we can grab is the arch.
            macros['buildarch'] = use.Arch._getMacro('targetarch')
            self.buildmacros = _createMacros('%(build)s')


        def _createMacros(compileTarget, osName=None):
            theMacros = self.macros.copy(False)

            archMacros = use.Arch._getMacros()
            theMacros.majorarch = use.Arch.getCurrentArch()._name
            theMacros.update(archMacros)
            # locate the correct config.site files
            theMacros.env_path = os.environ['PATH']
            _setSiteConfig(theMacros, theMacros.majorarch, osName)
            theMacros['cc'] = '%s-gcc' % compileTarget
            theMacros['cxx'] = '%s-g++' % compileTarget
            theMacros['strip'] = '%s-strip' % compileTarget
            theMacros['strip_archive'] = '%s-strip -g' % compileTarget
            return theMacros

        def _setSiteConfig(macros, arch, osName, setEnviron=False):
            if osName is None:
                osName = self.macros.os
            archConfig = None
            osConfig = None
            for siteDir in self.cfg.siteConfigPath:
                ac = '/'.join((siteDir, arch))
                if util.exists(ac):
                    archConfig = ac
                if osName:
                    oc = '/'.join((siteDir, osName))
                    if util.exists(oc):
                        osConfig = oc
            if not archConfig and not osConfig:
                macros.env_siteconfig = ''
                return

            siteConfig = None
            if setEnviron and 'CONFIG_SITE' in os.environ:
                siteConfig = os.environ['CONFIG_SITE']
            siteConfig = ' '.join((x for x in [siteConfig, archConfig, osConfig]
                                   if x is not None))
            macros.env_siteconfig = siteConfig
            if setEnviron:
                os.environ['CONFIG_SITE'] = siteConfig

        self.macros.update(dict(x for x in crossMacros.iteritems() 
                                 if x[0] not in self.macros))

        tmpArch = use.Arch.copy()

        _setBuildMacros(self.macros)

        if isCrossTool:
            targetFlavor, vendor, targetOs = _parseArch(crossTarget, True)
            self._isCrossCompileTool = True
        else:
            self._isCrossCompiling = True

        if crossHost is None:
            if isCrossTool:
                _setHostMacros(self._buildFlavor, self.macros)
                _setTargetMacros(crossTarget, self.macros)
                # leave things set up for the target
            else:
                # we want the resulting binaries to run
                # on the target machine.
                _setTargetMacros(crossTarget, self.macros)
                _setHostMacros(crossTarget, self.macros)
        else:
            _setTargetMacros(crossTarget, self.macros)
            _setHostMacros(crossHost, self.macros)

        # make sure that host != build, so that we are always
        # doing a real cross compile.  To make this work, we add
        # _build to the buildvendor. However, this little munging of
        # of the build system should not affect where the expected
        # gcc and g++ for local builds are located, so set those local
        # values first.

        origBuild = self.macros['build'] % self.macros
        self.macros['buildcc'] = '%s-gcc' % (origBuild)
        self.macros['buildcxx'] = '%s-g++' % (origBuild)

        if (self.macros['host'] % self.macros) == (self.macros['build'] % self.macros):
            self.macros['buildvendor'] += '_build'

        if isCrossTool:
            # we want the resulting binaries to run on our machine
            # but be targeted for %(target)s
            compileTarget = origBuild
        else:
            # we're expecting the resulting binaries to run on
            # target
            compileTarget = '%(target)s'

        self.macros['cc'] = '%s-gcc' % compileTarget
        self.macros['cxx'] = '%s-g++' % compileTarget
        self.macros['strip'] = '%s-strip' % compileTarget
        self.macros['strip_archive'] = '%s-strip -g' % compileTarget


        newPath = '%(crossprefix)s/bin:' % self.macros
        os.environ['PATH'] = newPath + os.environ['PATH']

        if not isCrossTool and self.macros.cc == self.macros.buildcc:
            # if necessary, specify the path for the system
            # compiler.  Otherwise, if target == build,  attempts to compile
            # for the build system may use the target compiler.
            self.macros.buildcc = '%(bindir)s/' + self.macros.buildcc
            self.macros.buildcxx = '%(bindir)s/' + self.macros.buildcxx
        
        # locate the correct config.site files
        _setSiteConfig(self.macros, self.macros.hostmajorarch,
                       self.macros.hostos, setEnviron=True)

    def needsCrossFlags(self):
        return self._isCrossCompileTool or self._isCrossCompiling

    def isCrossCompiling(self):
        return self._isCrossCompiling

    def isCrossCompileTool(self):
        return self._isCrossCompileTool

    def glob(self, expression):
        return action.Glob(self, expression)

    def regexp(self, expression):
        return action.Regexp(expression)

    def setupAbstractBaseClass(r):
        r.addSource(r.name + '.recipe', dest = str(r.cfg.baseClassDir) + '/')

    def setDerivedFrom(self, troveInfoList):
        self._derivedFrom = troveInfoList

    def getDerivedFrom(self):
        return self._derivedFrom

    def _addTroveScript(self, troveNames, scriptContents, scriptType,
                        fromClass = None):
        scriptTypeMap = dict((y[2], x) for (x, y) in
                             trove.TroveScripts.streamDict.items())
        assert(scriptType in scriptTypeMap)
        for troveName in troveNames:
            self._scriptsMap.setdefault(troveName, {})[scriptType] = \
                (scriptContents, fromClass)

    def __init__(self, cfg, laReposCache, srcdirs, extraMacros={},
                 crossCompile=None, lightInstance=False):
        Recipe.__init__(self, lightInstance = lightInstance,
                        laReposCache = laReposCache, srcdirs = srcdirs)
	self._build = []

        # lightInstance for only instantiating, not running (such as checkin)
        self._lightInstance = lightInstance
        if not hasattr(self,'_buildFlavor'):
            self._buildFlavor = cfg.buildFlavor

        self._policyPathMap = {}
        self._policies = {}
        self._policyMap = {}
        self._componentReqs = {}
        self._componentProvs = {}
        self._derivedFiles = {} # used only for derived packages
        # Inspected only when it is important to know for reporting
        # purposes what was specified in the recipe per se, and not
        # in superclasses or in defaultBuildRequires
        self._recipeRequirements = {
            'buildRequires': list(self.buildRequires),
            'crossRequires': list(self.crossRequires)
        }
        self._includeSuperClassBuildReqs()
        self._includeSuperClassCrossReqs()
        self.byDefaultIncludeSet = frozenset()
        self.byDefaultExcludeSet = frozenset()
        self.cfg = cfg
        self._repos = None
	self.macros = macros.Macros(ignoreUnknown=lightInstance)
        baseMacros = loadMacros(cfg.defaultMacros)
	self.macros.update(baseMacros)
        self.hostmacros = self.macros.copy()
        self.targetmacros = self.macros.copy()
        self.transitiveBuildRequiresNames = None
        # Mapping from trove name to scripts
        self._scriptsMap = {}
        self._subscribeLogPath = None
        self._subscribedPatterns = []
        self._logFile = None

        self._provideGroup = {} # used by User build action to indicate if
        # group should also be provided

        # allow for architecture not to be set -- this could happen
        # when storing the recipe e.g.
 	for key in cfg.macros:
 	    self.macros._override(key, cfg['macros'][key])

	self.macros.name = self.name
	self.macros.version = self.version
        if '.' in self.version:
            self.macros.major_version = '.'.join(self.version.split('.')[0:2])
        else:
            self.macros.major_version = self.version
        self.packages = { self.name : True }
        self.manifests = set()
	if extraMacros:
	    self.macros.update(extraMacros)

        self._isCrossCompileTool = False
        self._isCrossCompiling = False
        if crossCompile is None:
            crossCompile = getCrossCompileSettings(self._buildFlavor)

        if crossCompile:
            self.setCrossCompile(crossCompile)
        else:
            self.macros.update(use.Arch._getMacros())
            self.macros.setdefault('hostarch', self.macros['targetarch'])
            self.macros.setdefault('buildarch', self.macros['targetarch'])
        if not hasattr(self, 'keepBuildReqs'):
            self.keepBuildReqs = []

        if self.needsCrossFlags() and self.keepBuildReqs is not True:
            crossSuffixes = ['devel', 'devellib']
            crossTools = ['gcc', 'libgcc', 'binutils']
            if (not hasattr(self, 'keepBuildReqs') 
                or not hasattr(self.keepBuildReqs, '__iter__')):
                # if we're in the "lightReference" mode, this might 
                # return some bogus object...
                self.keepBuildReqs = set()
            newCrossRequires = \
                [ x for x in self.buildRequires 
                   if (':' in x and x.split(':')[-1] in crossSuffixes
                       and x.split(':')[0] not in crossTools
                       and x not in self.keepBuildReqs) ]
            self.buildRequires = [ x for x in self.buildRequires
                                   if x not in newCrossRequires ]
            self.crossRequires.extend(newCrossRequires)

        self.mainDir(self.nameVer(), explicit=False)
        self._autoCreatedFileCount = 0

# For compatibility with older modules. epydoc doesn't document classes
# starting with _, see CNY-1848
_AbstractPackageRecipe = AbstractPackageRecipe

class SourcePackageRecipe(AbstractPackageRecipe):
    internalAbstractBaseClass = 1
    def __init__(self, *args, **kwargs):
        klass = self._getParentClass('AbstractPackageRecipe')
        klass.__init__(self, *args, **kwargs)
        for name, item in build.__dict__.items():
            if inspect.isclass(item) and issubclass(item, action.Action):
                self._addBuildAction(name, item)
_SourcePackageRecipe = SourcePackageRecipe

exec defaultrecipes.BaseRequiresRecipe
exec defaultrecipes.PackageRecipe
exec defaultrecipes.BuildPackageRecipe
exec defaultrecipes.CPackageRecipe
exec defaultrecipes.AutoPackageRecipe
