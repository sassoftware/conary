#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Contains the base Recipe class, default macros, and miscellaneous
components used by srs .recipe files
"""

import imp, sys
import os
import util
import build
import destdirpolicy
import packagepolicy
import shutil
import types
import inspect
import lookaside
import rpmhelper
import gzip
import buildpackage

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
    ('sbindir'		, '%(exec_prefix)s/sbin'),
    ('libdir'		, '%(exec_prefix)s/%(lib)s'),
    ('libexecdir'	, '%(exec_prefix)s/libexec'),
    ('localstatedir'	, '%(prefix)s/var'),
    ('sharedstatedir'	, '%(prefix)s/com'),
    ('includedir'	, '%(prefix)s/include'),
    ('datadir'		, '%(prefix)s/share'),
    ('mandir'		, '%(datadir)s/man'),
    ('infodir'		, '%(datadir)s/info'),
    ('docdir'		, '%(datadir)s/doc'),
    ('develdocdir'	, '%(datadir)s/develdoc'),
    # arguments/flags (empty ones are for documentation; non-existant = empty)
    ('cflags'           , '-O2'),
    ('mflags'		, ''),
    ('parallelmflags'   , ''),
    ('sysroot'		, ''),
)

crossMacros = (
    # set crossdir from cook, directly or indirectly, before adding the rest
    #('crossdir'	, 'cross-target'),
    ('prefix'		, '/opt/%(crossdir)s'),
    ('sysroot'		, '%(prefix)s/sys-root'),
    ('headerpath'	, '%(sysroot)s/usr/include')
)

# XXX TEMPORARY - remove directories such as /usr/include from this
# list when filesystem package is in place.
baseSubFilters = (
    # automatic subpackage names and sets of regexps that define them
    # cannot be a dictionary because it is ordered; first match wins
    ('devel', ('\.a',
               '\.so',
               '.*/include/.*\.h',
               '/usr/include/',
               '/usr/include',
               '/usr/share/man/man(2|3)/',
               '/usr/share/man/man(2|3)',
               '/usr/share/develdoc/',
               '/usr/share/develdoc',
               '/usr/share/aclocal/',
               '/usr/share/aclocal')),
    ('lib', ('.*/lib/.*\.so\..*')),
    ('doc', ('/usr/share/(doc|man|info)/',
             '/usr/share/(doc|man|info)')),
    ('locale', ('/usr/share/locale/',
                '/usr/share/locale')),
    ('runtime', ('.*',)),
)

class Macros(dict):
    def __setitem__(self, name, value):
	dict.__setitem__(self, name, value % self)

    # we want keys that don't exist to default to empty strings
    def __getitem__(self, name):
	if self.has_key(name):
	    return dict.__getitem__(self, name)
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


def extractSourceFromRPM(rpm, targetfile):
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

        exec 'from recipe import Recipe' in self.module.__dict__
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
            msg += '%s%s^\n' %(err.text, ' ' * (err.offset-1))
            raise RecipeFileError(msg)
        exec code in self.module.__dict__
        for (name, obj) in  self.module.__dict__.items():
            if type(obj) == types.ClassType:
                # make sure the class is derived from Recipe
                # and has a name
                if obj.__dict__.has_key('ignore'):
                    continue
                if issubclass(obj, Recipe) and obj.__dict__.has_key('name'):
                    obj.__dict__['filename'] = filename
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
    def __init__(self, list, index, theobject, theclass):
        self.list = list
	self.index = index
        self.theobject = theobject
        self.theclass = theclass
    def __call__(self, *args, **keywords):
	if not args:
	    # we can just update the existing object
	    for key in keywords.keys():
		if not self.theobject.__dict__.has_key(key):
		    raise TypeError, (
			'no such key %s in %s'
			    %(key, self.theobject.__class__.__name__))
	    self.theobject.__dict__.update(keywords)
	else:
	    # we have to replace the old object with a new one
	    # we don't yet use args for anything, but if we do
	    # it will be evaluated at init time and so cannot be
	    # re-evaluated...
	    self.list[index] = self.theclass(*args, **keywords)

class Recipe:
    buildRequires = []
    runRequires = []

    def _addSignature(self, file, keyid):
	# do not search unless a gpg keyid is specified
	if not keyid:
	    return
	gpg = '%s.sig' %(file)
	c = lookaside.searchAll(self.cfg, self.laReposCache, gpg, 
				self.name, self.srcdirs)
	if not c:
	    gpg = '%s.sign' %(file)
	    c = lookaside.searchAll(self.cfg, self.laReposCache,
				    gpg, self.name, self.srcdirs)
	if c:
	    if not self.signatures.has_key(file):
		self.signatures[file] = []
	    self.signatures[file].append((gpg, c, keyid))

    def _appendSource(self, file, keyid, type, extractDir, use, args):
	file = file % self.macros
	extractDir = extractDir % self.macros
	self.sources.append((file, type, extractDir, use, args))
	self._addSignature(file, keyid)

    def addArchive(self, file, extractDir='', keyid=None, use=None):
	self._appendSource(file, keyid, 'tarball', extractDir, use, ())

    def addArchiveFromRPM(self, rpm, file, extractDir='', use=None):
	# no keyid -- what would it apply to?
	# may choose to check key in RPM package instead?
	rpm = rpm % self.macros
	file = file % self.macros
	f = lookaside.searchAll(self.cfg, self.laReposCache, 
			     os.path.basename(file), self.name, self.srcdirs)
	if not f:
	    r = lookaside.findAll(self.cfg, self.laReposCache, rpm, 
				  self.name, self.srcdirs)
	    c = lookaside.createCacheName(self.cfg, file, self.name)
	    extractSourceFromRPM(r, c)
	    f = lookaside.findAll(self.cfg, self.laReposCache, file, 
				  self.name, self.srcdirs)
	# file already expanded, and no key can be supplied
	extractDir = extractDir % self.macros
	self.sources.append((file, 'tarball', extractDir, use, ()))

    def addPatch(self, file, level='1', backup='', extractDir='', keyid=None, use=None):
	self._appendSource(file, keyid, 'patch', extractDir, use, (level, backup))

    def addSource(self, file, keyid=None, extractDir='', apply=None, use=None):
	self._appendSource(file, keyid, 'source', extractDir, use, (apply))

    def allSources(self):
        sources = []
        for (file, filetype, extractDir, use, args) in self.sources:
            sources.append(file)
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

    def checkSignatures(self, filepath, file):
        if not self.signatures.has_key(file):
            return
	for (gpg, signature, keyid) in self.signatures[file]:
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
	if os.path.exists(builddir):
	    shutil.rmtree(builddir)
	util.mkdirChain(builddir)

	for (file, filetype, targetdir, use, args) in self.sources:

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
		f = lookaside.findAll(self.cfg, self.laReposCache, file, 
			      self.name, self.srcdirs)
		self.checkSignatures(f, file)
		if f.endswith(".bz2"):
		    tarflags = "-jxf"
		elif f.endswith(".gz") or f.endswith(".tgz"):
		    tarflags = "-zxf"
		else:
		    raise RuntimeError, "unknown archive compression"
		if targetdir:
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
		(level, backup) = args
		f = lookaside.findAll(self.cfg, self.laReposCache, file, 
			      self.name, self.srcdirs)
		provides = "cat"
		if file.endswith(".gz"):
		    provides = "zcat"
		elif file.endswith(".bz2"):
		    provides = "bzcat"
		if backup:
		    backup = '-b -z %s' % backup
		if targetdir:
		    destDir = "/".join((destDir, targetdir))
		util.execute('%s %s | patch -d %s -p%s %s'
		             %(provides, f, destDir, level, backup))
		continue

	    if filetype == 'source':
		(apply) = args
		f = lookaside.findAll(self.cfg, self.laReposCache, file, 
				      self.name, self.srcdirs)
		util.copyfile(f, destDir + "/" + os.path.basename(file))
		if apply:
		    util.execute(apply, destDir)
		continue

    def doBuild(self, buildpath, root):
        builddir = buildpath + "/" + self.mainDir()
	self.addMacros(('builddir', builddir),
	               ('destdir', root))
        if self.build is None:
            pass
	if self.build == []:
	    print '\n\n\n'
	    print ' +++ default build rules DEPRECATED, please set build rules explicitly! +++'
	    print '\n\n\n'
	    self.build = self.defaultbuild
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

    def addDevice(self, target, devtype, major, minor, owner, group, perms):
        self._devices.append((target, devtype, major, minor, owner, group, perms))

    def packages(self, namePrefix, version, root):
        # by default, everything that hasn't matched a pattern in the
        # main package filter goes in the package named self.name
        self.mainFilters.append(buildpackage.Filter(self.name, '.*'))
	self.autopkg = buildpackage.AutoBuildPackage(namePrefix, version,
                                                     self.mainFilters,
                                                     self.subFilters)
        self.autopkg.walk(root)
	for policy in self.packagePolicy:
	    policy.doProcess(self)
	# XXX next two should get wrapped up in policy, I think
        for device in self._devices:
            self.autopkg.addDevice(*device)
        self.packages = self.autopkg.getPackages()

    def package(self):
        pass

    def getPackages(self):
        return self.packages

    def __getattr__(self, name):
	"""
	Allows us to dynamically suck in namespace of other modules
	with modifications.
	 - The public namespace of the build module is accessible,
	   and build objects are created and put on the build list
	   automatically when they are referenced.
	 - The public namespaces of the policy modules are accessible;
	   policy objects already on their respective lists are returned,
	   policy objects not on ther respective lists are added to
	   them like build objects are added to the build list.
	"""
        if not name.startswith('_'):
	    if build.__dict__.has_key(name):
		return _recipeHelper(self.build, build.__dict__[name])
	    if destdirpolicy.__dict__.has_key(name):
		policyClass = destdirpolicy.__dict__[name]
		for index in range(len(self.destdirPolicy)):
		    policyObj = self.destdirPolicy[index]
		    if isinstance(policyObj, policyClass):
			return _policyUpdater(self.destdirPolicy, index,
			                      policyObj, policyClass)
		return _recipeHelper(self.destdirPolicy, policyClass)
	    if packagepolicy.__dict__.has_key(name):
		policyClass = packagepolicy.__dict__[name]
		for index in range(len(self.packagePolicy)):
		    policyObj = self.packagePolicy[index]
		    if isinstance(policyObj, policyClass):
			return _policyUpdater(self.packagePolicy, index,
			                      policyObj, policyClass)
		return _recipeHelper(self.packagePolicy, policyClass)
        return self.__dict__[name]
    
    def __init__(self, cfg, laReposCache, srcdirs, extraMacros=()):
        assert(self.__class__ is not Recipe)
	self.sources = []
	# XXX fixme: convert to proper documentation string
	# sources is list of (file, filetype, targetdir, use, (args)) tuples,
	# where:
	# - file is the name of the file
	# - filetype is 'tarball', 'patch', 'source'
	# - targetdir is subdirectory to work in
	# - use is a use flag or tuple of use flags; if it is a tuple,
	#   they all have to be True in order to apply it
	# - args is filetype-specific:
	#   patch: (level, backup)
	#     - level is -p<level>
	#     - backup is .backupname suffix
	#   source: (apply)
	#     - apply is None or command to util.execute(apply)
	self.signatures = {}
        self._devices = []
        self.cfg = cfg
	self.laReposCache = laReposCache
	self.srcdirs = srcdirs
	self.theMainDir = self.name + "-" + self.version
	# what needs to be done to get from sources to an installed tree
	self.defaultbuild = [build.Make(), build.MakeInstall()]
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
            
	self.subFilters = []
	for pattern in baseSubFilters:
	    self.subFilters.append(buildpackage.Filter(*pattern))
	self.mainFilters = []

class RecipeFileError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
	return self.msg

    def __str__(self):
	return repr(self)
