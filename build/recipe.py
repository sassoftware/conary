#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
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

baseMacros = (
    # paths
    ('prefix'		, '/usr'),
    ('sysconfdir'	, '/etc'),
    ('lib'              , 'lib'),  # may be overridden with 'lib64'
    ('exec_prefix'	, '%(prefix)s'),
    ('bindir'		, '%(exec_prefix)s/bin'),
    ('sbindir'		, '%(exec_prefix)s/sbin'),
    ('libdir'		, '%(exec_prefix)s/%(lib)s'),
    ('libexecdir'	, '%(exec_prefix)s/libexec'),
    ('localstatedir'	, '%(prefix)s/var'),
    ('sharedstatedir'	, '%(prefix)s/com'),
    ('includedir'	, '%(prefix)s/include'),
    ('datadir'		, '/usr/share'),
    ('mandir'		, '%(datadir)s/man'),
    ('infodir'		, '%(datadir)s/info'),
    ('docdir'		, '%(datadir)s/doc'),
    ('develdocdir'	, '%(datadir)s/develdoc'),
    # arguments/flags
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


def flatten(list):
    if type(list) != types.ListType: return [list]
    if list == []: return list
    return flatten(list[0]) + flatten(list[1:])


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

#def bootstrapRecipe(file, class, buildRequires):
#    loadRecipe(file) # XXX not necessary if we put boostraps in main files
#    exec """class Bootstrap%s(%s):
#	buildRequires = %s
#	name = "bootstrap-%s"
#	def setup(self):
#	    FIXMEcrossmacros(self.recipeCfg)
#	    FIXMEcrossenv
#	    FIXMEself.mainDir(class, self.version)
#	    %s.setup(self)
#    """ %(class, class, buildRequires.repr(), class, class)
        

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

    def addTarball(self, file, extractDir='', keyid=None):
	self.tarballs.append((file, extractDir))
	self.addSignature(file, keyid)

    def addTarballFromRPM(self, rpm, file, extractDir='', keyid=None):
	f = lookaside.searchAll(self.cfg, self.laReposCache, 
			     os.path.basename(file), self.name, self.srcdirs)
	if not f:
	    r = lookaside.findAll(self.cfg, self.laReposCache, rpm, 
				  self.name, self.srcdirs)
	    c = lookaside.createCacheName(self.cfg, file, self.name)
	    extractSourceFromRPM(r, c)
	    f = lookaside.findAll(self.cfg, self.laReposCache, file, 
				  self.name, self.srcdirs)
	self.tarballs.append((file, extractDir))
	self.addSignature(f, keyid)

    def addPatch(self, file, level='1', backup='', keyid=None):
	self.patches.append((file, level, backup))
	self.addSignature(file, keyid)

    def addSource(self, file, keyid=None):
	self.sources.append(file)
	self.addSignature(file, keyid)

    def allSources(self):
        sources = []
        for (tarball, extractdir) in self.tarballs:
            sources.append(tarball)
        for (patch, level, backup) in self.patches:
            sources.append(patch)
	for signaturelist in self.signatures.values():
            for (gpg, cached, keyid) in signaturelist:
                sources.append(gpg)
	return sources + self.sources

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
	for (file, extractdir) in self.tarballs:
            f = lookaside.findAll(self.cfg, self.laReposCache, file, 
				  self.name, self.srcdirs)
	    self.checkSignatures(f, file)
            if f.endswith(".bz2"):
                tarflags = "-jxf"
            elif f.endswith(".gz") or f.endswith(".tgz"):
                tarflags = "-zxf"
            else:
                raise RuntimeError, "unknown archive compression"
            if extractdir:
                destdir = '%s/%s' % (builddir, extractdir)
                util.execute("mkdir -p %s" % destdir)
            else:
                destdir = builddir
            util.execute("tar -C %s %s %s" % (destdir, tarflags, f))
	
	for file in self.sources:
            f = lookaside.findAll(self.cfg, self.laReposCache, file, 
				  self.name, self.srcdirs)
	    destDir = builddir + "/" + self.theMainDir
	    util.mkdirChain(destDir)
	    shutil.copyfile(f, destDir + "/" + file)

	for (file, level, backup) in self.patches:
            # XXX handle .gz/.bz2 patch files
            f = util.findFile(file, self.srcdirs)
	    destDir = builddir + "/" + self.theMainDir
            if backup:
                backup = '-b -z %s' % backup
            util.execute('patch -d %s -p%s %s < %s' %(destDir, level, backup, f))

    def doBuild(self, buildpath):
        builddir = buildpath + "/" + self.mainDir()
	self.macros['builddir'] = builddir
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

    def packages(self, root):
        self.packageSet = package.Auto(self.name, root)

    def getPackageSet(self):
        return self.packageSet

    def __init__(self, cfg, laReposCache, srcdirs, extraMacros=()):
	self.tarballs = []
	self.patches = []
	self.sources = []
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
