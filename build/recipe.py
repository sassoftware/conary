#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Contains the base Recipe class, default macros, and miscellaneous
components used by srs .recipe files
"""

import build
import buildpackage
import destdirpolicy
import gzip
import helper
import imp
import inspect
import log
import lookaside
import os
import packagepolicy
import rpmhelper
import shutil
import sys
import types
import util

baseMacros = (
    # Note that these macros cannot be represented as a dictionary,
    # because the items need to be added in order so that they will
    # be properly interpolated.
    #
    # paths
    ('prefix'		, '/usr'),
    ('sysconfdir'	, '/etc'),
    ('initdir'		, '%(sysconfdir)s/rc.d/init.d'), # XXX fixme?
    ('lib'              , 'lib'),  # may be overridden with 'lib64'
    ('exec_prefix'	, '%(prefix)s'),
    ('bindir'		, '%(exec_prefix)s/bin'),
    ('essentialbindir'	, '/bin'),
    ('sbindir'		, '%(exec_prefix)s/sbin'),
    ('essentialsbindir'	, '/sbin'),
    ('libdir'		, '%(exec_prefix)s/%(lib)s'),
    ('essentiallibdir'	, '/%(lib)s'),
    ('libexecdir'	, '%(exec_prefix)s/libexec'),
    ('localstatedir'	, '/var'),
    ('sharedstatedir'	, '%(prefix)s/com'),
    ('includedir'	, '%(prefix)s/include'),
    ('datadir'		, '%(prefix)s/share'),
    ('mandir'		, '%(datadir)s/man'),
    ('infodir'		, '%(datadir)s/info'),
    ('docdir'		, '%(datadir)s/doc'),
    ('thisdocdir'       , '%(docdir)s/%(name)s-%(version)s'),
    # arguments/flags (empty ones are for documentation; non-existant = empty)
    ('cc'		, 'gcc'),
    ('cflags'           , '-O2'),
    ('mflags'		, ''),
    ('parallelmflags'   , ''),
    ('sysroot'		, ''),
    ('march'		, 'i386'), # "machine arch"
    ('os'		, 'linux'),
    ('target'		, 'i386-unknown-linux'),
)

crossMacros = (
    # set crossdir from cook, directly or indirectly, before adding the rest
    #('crossdir'	, 'cross-target'),
    ('prefix'		, '/opt/%(crossdir)s'),
    ('sysroot'		, '%(prefix)s/sys-root'),
    ('headerpath'	, '%(sysroot)s/usr/include')
)

class Macros(dict):
    def __setitem__(self, name, value):
        # only expand references to ourself
        d = {name: self.get(name)}
        # escape any macros in the new value
        value = value.replace('%', '%%')
        # unescape refrences to ourself
        value = value.replace('%%%%(%s)s' %name, '%%(%s)s'%name)
        # expand our old value when defining the new value
 	dict.__setitem__(self, name, value % d)
        
    # we want keys that don't exist to default to empty strings
    def __getitem__(self, name):
	if name in self:
	    return dict.__getitem__(self, name) %self
	return ''
    
    def addMacros(self, *macroSet):
	# must be in order; later macros in the set can depend on
	# earlier ones
	# for ease of use, we allow passing in a tuple of tuples, or
	# a simple set of tuples
	if len(macroSet) == 1 and type(macroSet[0]) is tuple:
	    # we were passed a tuple of tuples (like baseMacros)
	    macroSet = macroSet[0]
        if len(macroSet) > 0 and type(macroSet[0]) is not tuple:
            # we were passed something like ('foo', 'bar')
            macroSet = (macroSet,)
	for key, value in macroSet:
	    self[key] = value
    
    def copy(self):
	new = Macros()
	new.update(self)
	return new


def _extractSourceFromRPM(rpm, targetfile):
    filename = os.path.basename(targetfile)
    directory = os.path.dirname(targetfile)
    r = file(rpm, 'r')
    rpmhelper.seekToData(r)
    gz = gzip.GzipFile(fileobj=r)
    (rpipe, wpipe) = os.pipe()
    pid = os.fork()
    if not pid:
	os.dup2(rpipe, 0)
	os.chdir(directory)
	os.execl('/bin/cpio', 'cpio', '-ium', filename)
	sys.exit(1)
    while 1:
	buf = gz.read(4096)
	if not buf:
	    break
	os.write(wpipe, buf)
    os.close(wpipe)
    (pid, status) = os.waitpid(pid, 0)
    if not os.WIFEXITED(status):
	raise IOError, 'cpio died extracting %s from RPM %s' \
	               %(filename, os.path.basename(rpm))
    if os.WEXITSTATUS(status):
	raise IOError, 'cpio returned failure %d extracting %s from RPM %s' \
	               %(os.WEXITSTATUS(status), filename, os.path.basename(rpm))
    if not os.path.exists(targetfile):
	raise IOError, 'failed to extract source %s from RPM %s' \
	               %(filename, os.path.basename(rpm))

class RecipeLoader(types.DictionaryType):
    def __init__(self, filename):
        if filename[0] != "/":
            raise IOError, "recipe file names must be absolute paths"

        self.file = os.path.basename(filename).replace('.', '-')
        self.module = imp.new_module(self.file)
        sys.modules[self.file] = self.module
        f = open(filename)

        exec 'from recipe import PackageRecipe' in self.module.__dict__
        exec 'from recipe import GroupRecipe' in self.module.__dict__
        exec 'from recipe import loadRecipe' in self.module.__dict__
        exec 'import build, os, package, sys, util' in self.module.__dict__
        exec 'from use import Use, Arch' in self.module.__dict__
        if sys.excepthook == util.excepthook:
            exec 'sys.excepthook = util.excepthook' in self.module.__dict__
        exec 'filename = "%s"' %(filename) in self.module.__dict__
        try:
            code = compile(f.read(), filename, 'exec')
        except SyntaxError, err:
            msg = ('Error in recipe file "%s": '
                   '%s\n' %(os.path.basename(filename), err))
            if err.offset is not None:
                msg += '%s%s^\n' %(err.text, ' ' * (err.offset-1))
            else:
                msg += err.text
            raise RecipeFileError(msg)
        exec code in self.module.__dict__
        for (name, obj) in  self.module.__dict__.items():
            if type(obj) is not types.ClassType:
                continue
            # if a recipe has been marked to be ignored (for example, if
            # it was loaded from another recipe by loadRecipe()
            # (don't use hasattr here, we want to check only the recipe
            # class itself, not any parent class
            if 'ignore' in obj.__dict__:
                continue
            # make sure the class is derived from Recipe
            # and has a name
            if issubclass(obj, PackageRecipe) and hasattr(obj, 'name'):
                if obj.name.startswith('group-'):
                    raise RecipeFileError(
                        'Error in recipe file "%s": package name cannot '
                        'begin with "group-"' %os.path.basename(filename))
                obj.filename = filename
                self[name] = obj
	    elif issubclass(obj, GroupRecipe) and hasattr(obj, 'name'):
                if not obj.name.startswith('group-'):
                    raise RecipeFileError(
                        'Error in recipe file "%s": group name must '
                        'begin with "group-"' %os.path.basename(filename))
                obj.filename = filename
                self[name] = obj

    def __del__(self):
        try:
            del sys.modules[self.file]
        except:
            pass

# XXX this should be extended to load a recipe from srs
def loadRecipe(file):
    callerGlobals = inspect.stack()[1][0].f_globals
    if file[0] != '/':
        recipepath = os.path.dirname(callerGlobals['filename'])
        file = recipepath + '/' + file
    recipes = RecipeLoader(file)
    for name, recipe in recipes.items():
        # XXX hack to hide parent recipes
        recipe.ignore = 1
        callerGlobals[name] = recipe
        # stash a reference to the module in the namespace
        # of the recipe that loaded it, or else it will be destroyed
        callerGlobals[os.path.basename(file).replace('.', '-')] = recipes

## def bootstrapRecipe(recipeClass, buildRequires, file=None):
##     if file:
##         loadRecipe(file)

##     buildRequires = [ 'cross-gcc', 'bootstrap-glibc' ]
##     name = 'bootstrap-' + recipeClass.name
##     extraConfig = '--target=%(target)s --host=%(target)s'
##     def setup(self):
##         self.mainDir('diffutils-%s' % self.version)
##         Diffutils.setup(self)
        
##     bootstrap = type('Bootstrap' + recipeClass.__name__,
##                      (recipeCLass,),
##                      {'buildRequires': buildRequires,
##                       'name'         : name,
##                       'extraConfig'  : extraConfig,
##                       'setup'        : setup})

class _recipeHelper:
    def __init__(self, list, theclass):
        self.list = list
        self.theclass = theclass
    def __call__(self, *args, **keywords):
        self.list.append(self.theclass(*args, **keywords))

class _policyUpdater:
    def __init__(self, theobject):
        self.theobject = theobject
    def __call__(self, *args, **keywords):
	self.theobject.updateArgs(*args, **keywords)

class Recipe:

    pass

class PackageRecipe(Recipe):
    buildRequires = []
    runRequires = []

    def _addSignature(self, filename, keyid):
	# do not search unless a gpg keyid is specified
	if not keyid or not filename:
	    return
        for suffix in ('sig', 'sign', 'asc'):
            gpg = '%s.%s' %(filename, suffix)
            c = lookaside.searchAll(self.cfg, self.laReposCache, gpg, 
                                    self.name, self.srcdirs)
            if c:
                if filename not in self.signatures:
                    self.signatures[filename] = []
                self.signatures[filename].append((gpg, c, keyid))
                break

    def _appendSource(self, filename, keyid, type, extractDir, use, args):
	filename = filename % self.macros
	self.sources.append((filename, type, extractDir, use, args))
	self._addSignature(filename, keyid)

    def addArchive(self, filename, extractDir='', keyid=None, use=None):
	self._appendSource(filename, keyid, 'tarball', extractDir, use, ())

    def addPatch(self, filename, level='1', backup='', extractDir='',
                 keyid=None, use=None, macros=False, extraArgs=''):
	self._appendSource(filename, keyid, 'patch', extractDir, use,
                           (level, backup, macros, extraArgs))

    def addSource(self, filename, keyid=None, extractDir='',
                  apply=None, use=None, macros=False):
	self._appendSource(filename, keyid, 'source', extractDir, use,
                           (apply, macros))

    def _extractFromRPM(self, rpm, filename):
        """
        Extracts filename from rpm file and creates an entry in the
        source lookaside cache for the extracted file
        """
	# check signature in RPM package?
	rpm = rpm % self.macros
	filename = filename % self.macros
	f = lookaside.searchAll(self.cfg, self.laReposCache, 
                                os.path.basename(filename), self.name,
                                self.srcdirs)
	if not f:
	    r = lookaside.findAll(self.cfg, self.laReposCache, rpm, 
				  self.name, self.srcdirs)
	    c = lookaside.createCacheName(self.cfg, filename, self.name)
	    _extractSourceFromRPM(r, c)
	    f = lookaside.findAll(self.cfg, self.laReposCache, filename, 
				  self.name, self.srcdirs)

    def addArchiveFromRPM(self, rpm, filename, **keywords):
        self._extractFromRPM(rpm, filename)
        self.addArchive(filename, **keywords)

    def addPatchFromRPM(self, rpm, filename, **keywords):
        self._extractFromRPM(rpm, filename)
        self.addPatch(filename, **keywords)

    def addSourceFromRPM(self, rpm, filename, **keywords):
        self._extractFromRPM(rpm, filename)
        self.addSource(filename, **keywords)

    def addAction(self, action, targetdir='', use=None):
	self._appendSource('', '', 'action', targetdir, use, (action))

    def allSources(self):
        sources = []
        for (filename, filetype, extractDir, use, args) in self.sources:
	    if filename: # no file for an action
		sources.append(filename)
	for signaturelist in self.signatures.values():
            for (gpg, cached, keyid) in signaturelist:
                sources.append(gpg)
	return sources

    def mainDir(self, new = None):
	if new:
	    self.theMainDir = new % self.macros

	return self.theMainDir

    def nameVer(self):
	return self.name + "-" + self.version

    def cleanup(self, builddir, destdir):
	shutil.rmtree(builddir)
	shutil.rmtree(destdir)

    def checkSignatures(self, filepath, filename):
        if filename not in self.signatures:
            return
        if not util.checkPath("gpg"):
            return
	for (gpg, signature, keyid) in self.signatures[filename]:
	    # FIXME: our own keyring
	    if os.system("gpg --no-secmem-warning --verify %s %s"
			  %(signature, filepath)):
		# FIXME: only do this if key missing, this is cheap for now
		os.system("gpg --keyserver pgp.mit.edu --recv-keys 0x %s"
		          %(keyid))
		if os.system("gpg --no-secmem-warning --verify %s %s"
			      %(signature, filepath)):
		    raise RuntimeError, "GPG signature %s failed" %(signature)

    def unpackSources(self, builddir):
        self.addMacros('maindir', self.theMainDir)
        if os.path.exists(builddir):
	    shutil.rmtree(builddir)
	util.mkdirChain(builddir)

	for (filename, filetype, targetdir, use, args) in self.sources:

	    if use != None:
		if type(use) is not tuple:
		    use=(use,)
		for usevar in use:
		    if not usevar:
			# put this in the repository, but do not apply it
			filetype = None
			continue
		if filetype == None:
		    continue

	    if filetype == 'tarball':
		f = lookaside.findAll(self.cfg, self.laReposCache, filename, 
                                      self.name, self.srcdirs)
		self.checkSignatures(f, filename)
		if f.endswith(".bz2") or f.endswith(".tbz2"):
		    tarflags = "-jxf"
		elif f.endswith(".gz") or f.endswith(".tgz"):
		    tarflags = "-zxf"
		else:
		    raise RuntimeError, "unknown archive compression"
		if targetdir:
                    targetdir = targetdir % self.macros
		    destdir = '%s/%s' % (builddir, targetdir)
		    util.mkdirChain(destdir)
		else:
		    destdir = builddir
		util.execute("tar -C %s %s %s" % (destdir, tarflags, f))
		continue

	    # Not a tarball, so different assumption about where to operate
	    destDir = builddir + "/" + self.theMainDir
	    util.mkdirChain(destDir)

	    if filetype == 'patch':
		(level, backup, macros, extraArgs) = args
		f = lookaside.findAll(self.cfg, self.laReposCache, filename, 
			      self.name, self.srcdirs)
		provides = "cat"
		if filename.endswith(".gz"):
		    provides = "zcat"
		elif filename.endswith(".bz2"):
		    provides = "bzcat"
		if backup:
		    backup = '-b -z %s' % backup
		if targetdir:
		    destDir = "/".join((destDir, targetdir))
		if macros:
		    log.debug('applying macros to patch %s' %f)
		    pin = os.popen("%s '%s'" %(provides, f))
		    log.debug('patch -d %s -p%s %s %s' %(destDir, level, backup, extraArgs))
		    pout = os.popen('patch -d %s -p%s %s %s'
		                    %(destDir, level, backup, extraArgs), 'w')
		    pout.write(pin.read()%self.macros)
		    pin.close()
		    pout.close()
		else:
		    util.execute("%s '%s' | patch -d %s -p%s %s %s"
				 %(provides, f, destDir, level, backup, extraArgs))
		continue

	    if filetype == 'source':
		(apply, macros) = args
		f = lookaside.findAll(self.cfg, self.laReposCache, filename, 
				      self.name, self.srcdirs)
		if targetdir:
		    destDir = "/".join((destDir, targetdir))
		    util.mkdirChain(destDir)
		if macros:
		    log.debug('applying macros to source %s' %f)
		    pin = file(f)
		    pout = file(destDir + os.sep + os.path.basename(filename), "w")
		    pout.write(pin.read()%self.macros)
		    pin.close()
		    pout.close()
		else:
		    util.copyfile(f, destDir + os.sep + os.path.basename(filename))
		if apply:
		    util.execute(apply, destDir)
		continue

	    if filetype == 'action':
		(action) = args
		util.execute(action, destDir)

    def doBuild(self, buildpath, root):
        builddir = buildpath + "/" + self.mainDir()
	self.addMacros(('builddir', builddir),
                       ('destdir', root))
        
        if self.build is None:
            pass
        elif isinstance(self.build, str):
            util.execute(self.build %self.macros)
        elif isinstance(self.build, (tuple, list)):
	    for bld in self.build:
                if type(bld) is str:
                    util.execute(bld %self.macros)
                else:
                    bld.doBuild(self)
	else:
	    self.build.doBuild(self)

    def addProcess(self, post):
	self.process[:0] = [post] # prepend so that policy is done last

    def doDestdirProcess(self):
        for post in self.process:
            post.doProcess(self)
	for post in self.destdirPolicy:
            post.doProcess(self)

    def getPackages(self, namePrefix, fullVersion):
	# policies look at the recipe instance for all information
	self.namePrefix = namePrefix
	self.fullVersion = fullVersion
	for policy in self.packagePolicy:
	    policy.doProcess(self)
        return self.autopkg.getPackages()


    def disableParallelMake(self):
        self.macros['parallelmflags'] = ''

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
	    if name in build.__dict__:
		return _recipeHelper(self.build, build.__dict__[name])
	    for (policy, list) in (
		(destdirpolicy, self.destdirPolicy),
		(packagepolicy, self.packagePolicy)):
		if name in policy.__dict__:
		    policyClass = policy.__dict__[name]
		    for policyObj in list:
			if isinstance(policyObj, policyClass):
			    return _policyUpdater(policyObj)
		    return _recipeHelper(list, policyClass)
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
    
    def __init__(self, cfg, laReposCache, srcdirs, extraMacros=()):
        assert(self.__class__ is not Recipe)
	self.sources = []
	# XXX fixme: convert to proper documentation string
	# sources is list of (file, filetype, targetdir, use, (args)) tuples,
	# where:
	# - file is the name of the file
	# - filetype is in ('tarball', 'patch', 'source', 'action')
	# - targetdir is subdirectory to work in
	# - use is a use flag or tuple of use flags; if it is a tuple,
	#   they all have to be True in order to apply it
	# - args is filetype-specific:
	#   patch: (level, backup, macros, extraArgs)
	#     - level is -p<level> (1)
	#     - backup is .backupname suffix (none)
	#     - macros is boolean: apply self.macros to patch? (False)
	#     - extraArgs is string of additional patch args ('')
	#   source: (apply, macros)
	#     - apply is None or command to util.execute(apply)
	#     - macros is boolean: apply self.macros to patch? (False)
	#   action: (action)
	#     - action is the command to execute, in builddir
	self.signatures = {}
        self.cfg = cfg
	self.laReposCache = laReposCache
	self.srcdirs = srcdirs
	self.theMainDir = self.name + "-" + self.version
	self.build = []
	# what needs to be done to massage the installed tree
        self.process = []
        self.destdirPolicy = destdirpolicy.DefaultPolicy()
        self.packagePolicy = packagepolicy.DefaultPolicy()
	self.macros = Macros()
	self.addMacros = self.macros.addMacros
	self.addMacros(baseMacros)
	self.macros['name'] = self.name
	self.macros['version'] = self.version
	if extraMacros:
	    self.addMacros(extraMacros)

class GroupRecipe(Recipe):

    def addTrove(self, name, versionStr):
	try:
	    pkgList = helper.findPackage(self.repos, self.cfg.packagenamespace,
				     None, name, versionStr)
	except helper.PackageNotFound, e:
	    raise RecipeFileError, str(e)

	versionList = [ x.getVersion() for x in pkgList ]
	self.troveVersions[pkgList[0].getName()] = versionList

    def getTroveList(self):
	return self.troveVersions

    def __init__(self, repos, cfg):
	self.repos = repos
	self.cfg = cfg
	self.troveVersions = {}

class RecipeFileError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
	return self.msg

    def __str__(self):
	return repr(self)
