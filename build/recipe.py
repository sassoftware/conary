#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
"""
The recipe module contains the base Recipe class, default macros,
and miscellaneous components used by srs .recipe files
"""

import imp, sys
import os
import util
import build
import package
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
baseAutoSpec = (
    # automatic subpackage names and sets of regexps that define them
    # cannot be a dictionary because it is ordered; first match wins
    ('devel',
	('\.a',
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
    def __init__(self, file):
        if file[0] != "/":
            raise IOError, "recipe file names must be absolute paths"

        self.file = os.path.basename(file).replace('.', '-')
        self.module = imp.new_module(self.file)
        sys.modules[self.file] = self.module
        f = open(file)

        exec 'from recipe import Recipe' in self.module.__dict__
        exec 'from recipe import loadRecipe' in self.module.__dict__
        exec 'import build, os, package, sys, util' in self.module.__dict__
        if sys.excepthook == util.excepthook:
            exec 'sys.excepthook = util.excepthook' in self.module.__dict__
        exec 'filename = "%s"' %(file) in self.module.__dict__
        code = compile(f.read(), file, 'exec')
        exec code in self.module.__dict__
        for (name, obj) in  self.module.__dict__.items():
            if type(obj) == types.ClassType:
                # make sure the class is derived from Recipe
                # and has a name
                if obj.__dict__.has_key('ignore'):
                    continue
                if issubclass(obj, Recipe) and obj.__dict__.has_key('name'):
                    obj.__dict__['filename'] = file
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

class Recipe:
    buildRequires = []
    runRequires = []

    def addSignature(self, file, keyid):
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

    def addArchive(self, file, extractDir='', keyid=None):
	self.sources.append((file, 'tarball', extractDir, ()))
	self.addSignature(file, keyid)

    def addArchiveFromRPM(self, rpm, file, extractDir='', keyid=None):
	f = lookaside.searchAll(self.cfg, self.laReposCache, 
			     os.path.basename(file), self.name, self.srcdirs)
	if not f:
	    r = lookaside.findAll(self.cfg, self.laReposCache, rpm, 
				  self.name, self.srcdirs)
	    c = lookaside.createCacheName(self.cfg, file, self.name)
	    extractSourceFromRPM(r, c)
	    f = lookaside.findAll(self.cfg, self.laReposCache, file, 
				  self.name, self.srcdirs)
	self.sources.append((file, 'tarball', extractDir, ()))
	self.addSignature(f, keyid)

    def addPatch(self, file, level='1', backup='', keyid=None):
	self.sources.append((file, 'patch', '', (level, backup)))
	self.addSignature(file, keyid)

    def addSource(self, file, keyid=None, extractDir='', apply=None):
	self.sources.append((file, 'source', extractDir, (apply)))
	self.addSignature(file, keyid)

    def allSources(self):
        sources = []
        for (file, filetype, extractDir, args) in self.sources:
            sources.append(file)
	for signaturelist in self.signatures.values():
            for (gpg, cached, keyid) in signaturelist:
                sources.append(gpg)
	return sources

    def mainDir(self, new = None):
	if new:
	    self.theMainDir = new

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

	for (file, filetype, targetdir, args) in self.sources:
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
		f = util.findFile(file, self.srcdirs)
		provides = "cat"
		if file.endswith(".gz"):
		    provides = "zcat"
		elif file.endswith(".bz2"):
		    provides = "bzcat"
		if backup:
		    backup = '-b -z %s' % backup
		util.execute('%s %s | patch -d %s -p%s %s' %(provides, f, destDir, level, backup))
		continue

	    if filetype == 'source':
		(apply) = args
		f = lookaside.findAll(self.cfg, self.laReposCache, file, 
				      self.name, self.srcdirs)
		shutil.copyfile(f, destDir + "/" + os.path.basename(file))
		if apply:
		    util.execute(apply, destDir)
		continue

    def doBuild(self, buildpath, root):
        builddir = buildpath + "/" + self.mainDir()
	self.addMacros(('builddir', builddir),
	               ('destdir', root))
        if self.build is None:
            pass
        elif type(self.build) is str:
            util.execute(self.build %self.macros)
        elif type(self.build) is tuple:
	    for bld in self.build:
                if type(bld) is str:
                    util.execute(bld %self.macros)
                else:
                    bld.doBuild(self.macros)
	else:
	    self.build.doBuild(self.macros)

    def doInstall(self, buildpath, root):
        builddir = buildpath + "/" + self.mainDir()
	self.addMacros(('builddir', builddir),
	               ('destdir', root))
        if self.install is None:
            pass
        elif type(self.install) is str:
            util.execute(self.install %self.macros)
	elif type(self.install) is tuple:
	    for inst in self.install:
                if type(inst) is str:
                    util.execute(inst %self.macros)
                else:
                    inst.doInstall(self.macros)
	else:
	    self.install.doInstall(self.macros)

    def packages(self, namePrefix, version, root):
	# "None" will be replaced by explicit subpackage list
	self.packageSpecSet = buildpackage.PackageSpecSet(
					namePrefix + ":" + self.name,
					version,
					self.autoSpecList, None)
        self.packageSet = buildpackage.Auto(self.name, root,
                                            self.packageSpecSet)

    def getPackageSet(self):
        return self.packageSet

    def __init__(self, cfg, laReposCache, srcdirs, extraMacros=()):
        assert(self.__class__ is not Recipe)
	self.sources = []
	# XXX fixme: convert to proper documentation string
	# sources is list of (file, filetype, targetdir, (args)) tuples, where
	# - file is the name of the file
	# - filetype is 'tarball', 'patch', 'source'
	# - targetdir is subdirectory to work in
	# - args is filetype-specific:
	#   patch: (level, backup)
	#     - level is -p<level>
	#     - backup is .backupname suffix
	#   source: (apply)
	#     - apply is None or command to util.execute(apply)
	self.signatures = {}
        self.cfg = cfg
	self.laReposCache = laReposCache
	self.srcdirs = srcdirs
	self.theMainDir = self.name + "-" + self.version
	self.build = build.Make()
        self.install = build.MakeInstall()
	self.macros = Macros()
	self.addMacros = self.macros.addMacros
	self.addMacros(baseMacros)
	self.macros['name'] = self.name
	self.macros['version'] = self.version
	if extraMacros:
	    self.addMacros(extraMacros)
	self.autoSpecList = []
	for spec in baseAutoSpec:
	    self.autoSpecList.append(buildpackage.PackageSpec(*spec))
