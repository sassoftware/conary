#
# Copyright (c) 2004-2006 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import copy
import os
import imp
import inspect
import sys

from conary.build.recipe import Recipe, RECIPE_TYPE_PACKAGE
from conary.build.loadrecipe import _addRecipeToCopy
from conary.build.errors import RecipeFileError

from conary.build import build
from conary.build import errors
from conary.build import macros
from conary.build import policy
from conary.build import source
from conary.build import use
from conary.conaryclient import cmdline
from conary.deps import deps
from conary import files
from conary.lib import log, magic, util
from conary.local import database



crossMacros = {
    'crossdir'          : 'cross-target-%(target)s',
    'crossprefix'	: '/opt/%(crossdir)s',
    'sysroot'		: '%(crossprefix)s/sys-root',
    'headerpath'	: '%(sysroot)s%(includedir)s'
}

def loadMacros(paths):
    baseMacros = {}
    for path in paths:
        if not os.path.exists(path):
            continue
        macroModule = imp.load_source('tmpmodule', path)
        baseMacros.update(x for x in macroModule.__dict__.iteritems()
                          if not x[0].startswith('__'))
    return baseMacros

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
        if inspect.isclass(value) and issubclass(value, _AbstractPackageRecipe):
            classes.append(value)

    for class_ in classes:
        _removePackages(class_, buildReqs)

        for base in inspect.getmro(class_):
            if issubclass(base, _AbstractPackageRecipe):
                _removePackages(base, buildReqs)

def _ignoreCall(*args, **kw):
    pass

class _AbstractPackageRecipe(Recipe):
    buildRequires = []
    Flags = use.LocalFlags
    explicitMainDir = False

    _recipeType = RECIPE_TYPE_PACKAGE

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
            (name, versionStr, flavor) = cmdline.parseTroveSpec(buildReq)
            # XXX move this to use more of db.findTrove's features, instead
            # of hand parsing
            try:
                troves = db.trovesByName(name)
                troves = db.getTroves(troves)
            except errors.TroveNotFound:
                missingReqs.append(buildReq)
                continue

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
                raise errors.RecipeDependencyError, \
                                            'unresolved build dependencies'
        self.buildReqMap = reqMap
        self.ignoreDeps = ignoreDeps

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
	    log.info("Resuming on line(s) %s" % resume)
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

    def loadPolicy(self):
        (self._policyPathMap, self._policies) = policy.loadPolicy(self)
        # create bucketless name->policy map for getattr
        policyList = []
        for bucket in self._policies.keys():
            policyList.extend(self._policies[bucket])
        self._policyMap = dict((x.__class__.__name__, x) for x in policyList)
        # Some policy needs to pass arguments to other policy at init
        # time, but that can't happen until after all policy has been
        # initialized
        for policyObj in self._policyMap.values():
            policyObj.postInit()

        # returns list of policy files loaded
        return self._policyPathMap.keys()

    def doProcess(self, policyBucket):
	for post in self._policies[policyBucket]:
            sys.stdout.write('Running policy: %s\r' %post.__class__.__name__)
            sys.stdout.flush()
            post.doProcess(self)

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
	    if name in self._policyMap:
	        policyObj = self._policyMap[name]
                return _policyUpdater(policyObj)
            if self._lightInstance:
                return _ignoreCall
        if name in self.__dict__:
            return self.__dict__[name]
        raise AttributeError, name

    def __delattr__(self, name):
	"""
	Allows us to delete policy items from their respective lists
	by deleting a name in the recipe self namespace.  For example,
	to remove the AutoDoc package policy from the package policy
	list, one could do::
	 del self.AutoDoc
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
        def _parseArch(archSpec):
            if isinstance(archSpec, deps.DependencySet):
                return archSpec, None, None

            if '-' in archSpec:
                arch, vendor, hostOs = archSpec.split('-')
            else:
                arch  = archSpec
                vendor = hostOs = None

            try:
                flavor = deps.parseFlavor('is: ' + arch)
            except deps.ParseError, msg:
                raise errors.CookError('Invalid architecture specification %s'
                                       %archSpec)

            if not flavor:
                raise errors.CookError('Invalid architecture specification %s'
                                       %archSpec)
            return flavor, vendor, hostOs

        def _setArchFlags(flavor):
            # given an flavor, make use.Arch match that flavor.
            for flag in use.Arch._iterAll():
                flag._set(False)
            use.setBuildFlagsFromFlavor(self.name, flavor)

        def _setBuildMacros(macros):
            # get the necessary information about the build system
            # the only information we can grab is the arch.
            macros['buildarch'] = use.Arch._getMacro('targetarch')

        def _setTargetMacros(crossTarget, macros):
            targetFlavor, vendor, targetOs = _parseArch(crossTarget)
            if vendor:
                macros['targetvendor'] = vendor
            if targetOs:
                macros['targetos'] = targetOs
            _setArchFlags(targetFlavor)
            macros['targetarch'] = use.Arch._getMacro('targetarch')

        def _setHostMacros(crossHost, macros):
            hostFlavor, vendor, hostOs = _parseArch(crossHost)
            if vendor:
                macros['hostvendor'] = vendor
            if hostOs:
                macros['hostos'] = hostOs

            tmpArch = copy.deepcopy(use.Arch)
            _setArchFlags(hostFlavor)
            macros['hostarch'] = use.Arch._getMacro('targetarch')
            macros['hostmajorarch'] = use.Arch.getCurrentArch()._name
                
            use.Arch = tmpArch

        def _setSiteConfig(macros):
            archConfig = None
            osConfig = None
            for siteDir in self.cfg.siteConfigPath:
                ac = '/'.join((siteDir, macros.hostmajorarch))
                if util.exists(ac):
                    archConfig = ac
                oc = '/'.join((siteDir, macros.hostos))
                if util.exists(oc):
                    osConfig = oc
            if not archConfig and not osConfig:
                return

            siteConfig = None
            if 'CONFIG_SITE' in os.environ:
                siteConfig = os.environ['CONFIG_SITE']
            siteConfig = ' '.join((x for x in [siteConfig, archConfig, osConfig]
                                   if x is not None))
            os.environ['CONFIG_SITE'] = siteConfig

        macros = self.macros
        macros.update(dict(x for x in crossMacros.iteritems() if x[0] not in macros))

        tmpArch = use.Arch.copy()

        _setBuildMacros(macros)
        _setTargetMacros(crossTarget, macros)

        if crossHost is None:
            if isCrossTool:
                # we want the resulting binaries to run on
                # this machine.
                macros['hostarch'] = macros['buildarch']
                macros['hostmajorarch'] = use.Arch.getCurrentArch()._name
            else:
                # we want the resulting binaries to run
                # on the target machine.
                macros['hostarch'] = macros['targetarch']
                _setHostMacros(crossTarget, macros)
        else:
            _setHostMacros(crossHost, macros)

        # make sure that host != build, so that we are always
        # doing a real cross compile.  To make this work, we add
        # _build to the buildvendor. However, this little munging of
        # of the build system should not affect where the expected
        # gcc and g++ for local builds are located, so set those local
        # values first.

        origBuild = macros['build'] % macros
        macros['buildcc'] = '%s-gcc' % (origBuild)
        macros['buildcxx'] = '%s-g++' % (origBuild)

        if (macros['host'] % macros) == (macros['build'] % macros):
            macros['buildvendor'] += '_build'

        if isCrossTool:
            # we want the resulting binaries to run on our machine
            # but be targeted for %(target)s
            compileTarget = origBuild
        else:
            # we're expecting the resulting binaries to run on
            # target
            compileTarget = '%(target)s'

        macros['cc'] = '%s-gcc' % compileTarget
        macros['cxx'] = '%s-g++' % compileTarget
        macros['strip'] = '%s-strip' % compileTarget
        macros['strip_archive'] = '%s-strip -g' % compileTarget


	archMacros = use.Arch._getMacros()
        # don't override values we've set for crosscompiling
        archMacros = dict(x for x in archMacros.iteritems() if x[0] != 'targetarch')
        macros.update(archMacros)
        newPath = '%(crossprefix)s/bin:' % self.macros
        os.environ['PATH'] = newPath + os.environ['PATH']

        if not isCrossTool and self.macros.cc == self.macros.buildcc:
            # if necessary, specify the path for the system
            # compiler.  Otherwise, if target == build,  attempts to compile
            # for the build system may use the target compiler.
            self.macros.buildcc = '%(bindir)s/' + self.macros.buildcc
            self.macros.buildcxx = '%(bindir)s/' + self.macros.buildcxx
        
        # locate the correct config.site files
        _setSiteConfig(macros)

        # set the bootstrap flag
        # FIXME: this should probably be a cross flag instead.
        use.Use.bootstrap._set()

    def __init__(self, cfg, laReposCache, srcdirs, extraMacros={},
                 crossCompile=None, lightInstance=False):
        Recipe.__init__(self)
	self._sources = []
	self._build = []
        self.buildinfo = False
        # lightInstance for only instantiating, not running (such as checkin)
        self._lightInstance = lightInstance

        self._policyPathMap = {}
        self._policies = {}
        self._policyMap = {}
        self._includeSuperClassBuildReqs()
        self.byDefaultIncludeSet = frozenset()
        self.byDefaultExcludeSet = frozenset()
        self.cfg = cfg
	self.laReposCache = laReposCache
	self.srcdirs = srcdirs
	self.macros = macros.Macros()
        baseMacros = loadMacros(cfg.defaultMacros)
	self.macros.update(baseMacros)
        if crossCompile:
            self.setCrossCompile(crossCompile)
        else:
            self.macros.update(use.Arch._getMacros())
            self.macros['hostarch'] = self.macros['targetarch']
            self.macros['buildarch'] = self.macros['targetarch']

        # allow for architecture not to be set -- this could happen
        # when storing the recipe e.g.
 	for key in cfg.macros:
 	    self.macros._override(key, cfg['macros'][key])
	self.macros.name = self.name
	self.macros.version = self.version
        self.packages = { self.name : True }
	if extraMacros:
	    self.macros.update(extraMacros)
	self.mainDir(self.nameVer(), explicit=False)
        self.sourcePathMap = {}
        self.pathConflicts = {}
        self._autoCreatedFileCount = 0


class PackageRecipe(_AbstractPackageRecipe):
    """
    NAME
    ====
    B{C{PackageRecipe}} - Base class which provides Conary functionality

    SYNOPSIS
    ========

    C{PackageRecipe} is inherited by the other *PackageRecipe super classes

    DESCRIPTION
    ===========

    The C{PackageRecipe} class provides Conary recipes with references to
    the essential troves which offer Conary's packaging requirements. 
    (python, sqlite, gzip, bzip2, tar, cpio, and patch)

    Other PackageRecipe classes such as C{AutoPackageRecipe} inherit the
    functionality offered by C{PackageRecipe}.

    EXAMPLE
    =======

    FIXME example
    """
    internalAbstractBaseClass = 1
    # these initial buildRequires need to be cleared where they would
    # otherwise create a requirement loop.  Also, note that each instance
    # of :lib in here is only for runtime, not to link against.
    # Any package that needs to link should still specify the :devel
    # component
    buildRequires = [
        'filesystem:runtime',
        'setup:runtime',
        'python:runtime',
        'python:lib',
        'conary:runtime',
        'conary:python',
        'conary-build:runtime',
        'conary-build:lib',
        'conary-build:python',
        'sqlite:lib',
        'bzip2:runtime',
        'gzip:runtime',
        'tar:runtime',
        'cpio:runtime',
        'patch:runtime',
    ]
# need this because we have non-empty buildRequires in PackageRecipe
_addRecipeToCopy(PackageRecipe)



# FIXME the next three classes will probably migrate to the repository
# somehow, but not until we have figured out how to do this without
# requiring that every recipe have a loadSuperClass line in it.

class BuildPackageRecipe(PackageRecipe):
    """
    NAME
    ====

    B{C{BuildPackageRecipe}} - Build packages requiring Make and shell
    utilities

    SYNOPSIS
    ========

    C{class I{className(BuildPackageRecipe):}}

    DESCRIPTION
    ===========

    The C{BuildPackageRecipe} class provides recipes with capabilities for
    building packages which require the C{make} utility, and additional,
    standard shell tools, (coreutils) and the programs needed to run
    C{configure}. (findutils, C{gawk}, C{grep}, C{sed}, and diffutils)
    
    C{BuildPackageRecipe} inherits from C{PackageRecipe}, and therefore
    includes all the build requirements of  C{PackageRecipe}. 

    EXAMPLE
    =======

    C{class DocbookDtds(BuidlPackageRecipe):}

    Uses C{BuildPackageRecipe} to define the class for a Docbook Document Type
    Definition collection recipe.
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
    Flags = use.LocalFlags
    internalAbstractBaseClass = 1
_addRecipeToCopy(BuildPackageRecipe)


class CPackageRecipe(BuildPackageRecipe):
    """
    NAME
    ====

    B{C{CPackageRecipe}} - Build packages consisting of binaries built from C
    source code

    SYNOPSIS
    ========

    C{class I{className(CPackageRecipe):}}

    DESCRIPTION
    ===========
    The C{CPackageRecipe} class provides the essential build requirements
    needed for packages consisting of binaries built from C source code, such
    as the linker and C library. C{CPacakgeRecipe} inherits from
    C{BuildPackageRecipe}, and therefore includes all the build requirements of
    C{BuildPackageRecipe}.

    Most package recipes which are too complex for C{AutoPackageRecipe}, and
    consist of applications derived from C source code which do not require
    additional shell utilities as build requirements use the
    C{CPackageRecipe} class.

    EXAMPLE
    =======

    C{class Bzip2(CPackageRecipe):}

    Defines the class for a C{bzip2} recipe using C{AutoPackageRecipe}.
    """
    buildRequires = [
        'binutils:runtime',
        'binutils:lib',
        'binutils:devellib',
        'gcc:runtime',
        'gcc:lib',
        'gcc:devel',
        'gcc:devellib',
        'glibc:runtime',
        'glibc:lib',
        'glibc:devellib',
        'glibc:devel',
        'libgcc:lib',
        'libgcc:devellib',
        'debugedit:runtime',
        'elfutils:runtime',
    ]
    Flags = use.LocalFlags
    internalAbstractBaseClass = 1
_addRecipeToCopy(CPackageRecipe)

class AutoPackageRecipe(CPackageRecipe):
    """
    NAME
    ====

    B{C{AutoPackageRecipe}} - Build simple packages with auto* tools

    SYNOPSIS
    ========

    C{class I{className(AutoPackageRecipe):}}

    DESCRIPTION
    ===========

    The  C{AutoPackageRecipe} class provides a simple means for the
    creation of packages from minimal recipes, which are built from source
    code using the auto* tools, such as C{automake}, and C{autoconf}.

    Processing in the C{AutoPackageRecipe} class is a simple workflow modeled
    after building software from source code, and is essentially comprised of
    these steps:

        1. Unpack source archive
        2. C{configure}
        3. C{make}
        4. C{make install}
        5. Applying Conary policy (optional)

    With C{AutoPackageRecipe} the recipe writer does not necessarily need to
    define the C{Configure}, C{Make}, or C{MakeInstall} methods, which allows
    for very compact, and simple recipes.

    The recipe's child classes should define the C{unpack()} method in order
    to populate the source list.

    Invoke the C{policy} method, with necessary policy parameters, and
    keywords in your recipe to enforce Conary policy in the package.

    If the standard C{Configure()}, C{Make()}, and C{MakeInstall()} methods
    are insufficient for your package requirements, you should define your own
    methods to override them.

    Of the three methods, C{Configure}, and C{Make} are least likely to be
    insufficient, and require overriding for the majority of recipes using
    C{AutoPackageRecipe}.

    EXAMPLE
    =======

    C{class Gimp(AutoPackageRecipe):}

    Defines the class for a GNU Image Manipulation Program (Gimp) recipe using
    C{AutoPackageRecipe}.
    """
    Flags = use.LocalFlags
    internalAbstractBaseClass = 1

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
_addRecipeToCopy(AutoPackageRecipe)
