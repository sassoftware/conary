#
# Copyright (c) 2005 rPath, Inc.
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

import copy
import os
import inspect

from conary.build.recipe import Recipe, RECIPE_TYPE_PACKAGE
from conary.build.loadrecipe import _addRecipeToCopy
from conary.build.errors import RecipeFileError

from conary.build import build
from conary.build import destdirpolicy
from conary.build import errors
from conary.build import macros
from conary.build import packagepolicy
from conary.build import source
from conary.build import use
from conary.conaryclient import cmdline
from conary.deps import deps
from conary import files
from conary.lib import log, magic, util
from conary.local import database

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
    'crossprefix'	: '/opt/%(crossdir)s',
    'sysroot'		: '%(crossprefix)s/sys-root',

    # cross compiling tools often need critical headers for 
    # 
    'headerpath'	: '%(sysroot)s/usr/include',



    # the target platform for the created binaries

    'targetvendor'      : 'unknown',
    'targetos'          : 'linux',
    'target'		: '%(targetarch)s-%(targetvendor)s-%(targetos)s',
    # the platform on which the created binaries should be run
    # (different from host only when the resulting binary is a cross-compiler)
    'hostvendor'        : 'unknown',
    'hostos'            : 'linux',
    'host'		: '%(hostarch)s-%(hostvendor)s-%(hostos)s',

    # build is the system on which the binaries are being run
    'buildvendor'       : 'unknown',
    'buildos'           : 'linux',
    'build'		: '%(buildarch)s-%(buildvendor)s-%(buildos)s',

}


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

    def setCrossCompile(self, (crossHost, crossTarget, crossTool)):
        """ Tell conary it should cross-compile, or build a part of a
            cross-compiler toolchain.

            Example: setCrossCompile(('x86-foo-linux', 'x86_64', False))

            @param crossHost: the architecture of the machine the built binary 
                 should run on.  Can be either <arch> or <arch>-<vendor>-<os>.
                 If None, determine crossHost based on crossTool value.
            @param crossTarget: the architecture of the machine the built
                 binary should be targeted for.
                 Can be either <arch> or <arch>-<vendor>-<os>.
            @param crossTool: If true, we are building a cross-compiler for
                 use on this system.  We set values so that the resulting 
                 binaries from this build should be runnable on the build 
                 architecture.
        """
        def _parseArch(archSpec):
            if '-' in archSpec:
                arch, vendor, hostOs = archSpec.split('-')
            else:
                arch  = archSpec
                vendor = hostOs = None

            try:
                flavor = deps.parseFlavor('is: ' + arch)
            except deps.ParseError, msg:
                raise CookError, 'Invalid architecture specification %s'

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
            if targetOs:
                macros['hostos'] = targetOs

            tmpArch = copy.deepcopy(use.Arch)
            _setArchFlags(hostFlavor)
            use.Arch = tmpArch

            macros['hostarch'] = use.Arch._getMacro('targetarch')

             
        macros = crossMacros.copy()
        tmpArch = use.Arch.copy()

        _setBuildMacros(macros)
        _setTargetMacros(crossTarget, macros)

        if crossHost is None:
            if crossTool:
                # we want the resulting binaries to run on 
                # this machine.
                macros['hostarch'] = macros['buildarch']
            else:
                # we want the resulting binaries to run 
                # on the target machine.
                macros['hostarch'] = macros['targetarch']
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
                
        if crossTool:
            # we want the resulting binaries to run on our machine
            # but be targeted for %(target)s
            macros['compile'] = origBuild
        else:
            # we're expecting the resulting binaries to run on 
            # target
            macros['compile'] = '%(target)s'

        macros['cc'] = '%(compile)s-gcc'
        macros['cxx'] = '%(compile)s-g++'
        macros['strip'] = '%(compile)s-strip'
        macros['strip-archive'] = '%(compile)s-strip -g'

        macros['crossdir'] = 'cross-target-%(target)s'
            
	self.macros.update(use.Arch._getMacros())
        self.macros.update(macros)
        newPath = '%(crossprefix)s/bin:' % self.macros
        os.environ['PATH'] = newPath + os.environ['PATH']

        # set the bootstrap flag
        # FIXME: this should probably be a cross flag instead.
        use.Use.bootstrap._set()
    
    def __init__(self, cfg, laReposCache, srcdirs, extraMacros={}, 
                 crossCompile=None):
        Recipe.__init__(self)
	self._sources = []
	self._build = []
        self.buildinfo = False

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
        if crossCompile:
            self.setCrossCompile(crossCompile)
        else:
            self.macros.update(use.Arch._getMacros())

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
    # abstract base class
    ignore = 1
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
        'conary:lib',
        'conary:python',
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
    Packages that need to be built with the make utility and basic standard
    shell tools should descend from this recipe in order to automatically
    have a reasonable set of build requirements.  This package differs
    from the C{PackageRecipe} class only by providing additional explicit
    build requirements.
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
    # abstract base class
    ignore = 1
_addRecipeToCopy(BuildPackageRecipe)


class CPackageRecipe(BuildPackageRecipe):
    """
    Most packages should descend from this recipe in order to automatically
    have a reasonable set of build requirements for a package that builds
    C source code to binaries.  This package differs from the
    C{BuildPackageRecipe} class only by providing additional explicit build
    requirements.
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
    # abstract base class
    ignore = 1
_addRecipeToCopy(CPackageRecipe)

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
_addRecipeToCopy(AutoPackageRecipe)
