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

def flatten(list):
    if type(list) != types.ListType: return [list]
    if list == []: return list
    return flatten(list[0]) + flatten(list[1:])

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
	exec 'sys.excepthook = util.excepthook' in self.module.__dict__
        exec 'filename = "%s"' %(file) in self.module.__dict__
        code = compile(f.read(), file, 'exec')
        exec code in self.module.__dict__
        for (key, value) in  self.module.__dict__.items():
            if type(value) == types.ClassType:
                # make sure the class is derived from something
                # and has a name
                # XXX better test?
                if value.__dict__.has_key('ignore'):
                    continue
                if len(value.__bases__) > 0 and 'name' in dir(value):
                    value.__dict__['filename'] = file
                    self[key] = value

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
        
class Recipe:
    buildRequires = []
    runRequires = []

    def addSignature(self, file, keyid):
	# do not search unless a gpg keyid is specified
	if not keyid:
	    return
	gpg = '%s.sig' %(file)
	c = lookaside.searchAll(self.cfg, gpg, self.name, self.srcdirs)
	if not c:
	    gpg = '%s.sign' %(file)
	    c = lookaside.searchAll(self.cfg, gpg, self.name, self.srcdirs)
	if c:
	    if not self.signatures.has_key(file):
		self.signatures[file] = []
	    self.signatures[file].append((gpg, c))

    def addTarball(self, file, extractDir='', keyid=None):
	self.tarballs.append((file, extractDir))
	self.addSignature(file, keyid)

    def addTarballFromRPM(self, rpm, file, extractDir='', keyid=None):
	f = lookaside.searchAll(self.cfg, os.path.basename(file), self.name, self.srcdirs)
	if not f:
	    r = lookaside.findAll(self.cfg, rpm, self.name, self.srcdirs)
	    c = lookaside.createCacheName(self.cfg, file, self.name)
	    os.system("cd %s; rpm2cpio %s | cpio -ium %s" %(os.path.dirname(c), r, file))
	    f = lookaside.findAll(self.cfg, file, self.name, self.srcdirs)
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
            for (gpg, cached) in signaturelist:
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
	for (gpg, signature) in self.signatures[file]:
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
            f = lookaside.findAll(self.cfg, file, self.name, self.srcdirs)
	    self.checkSignatures(f, file)
            if f.endswith(".bz2"):
                tarflags = "-jxf"
            elif f.endswith(".gz") or f.endswith(".tgz"):
                tarflags = "-zxf"
            else:
                raise RuntimeError, "unknown archive compression"
            if extractdir:
                destdir = '%s/%s' % (builddir, extractdir)
                os.system("mkdir -p %s" % destdir)
            else:
                destdir = builddir
	    os.system("tar -C %s %s %s" % (destdir, tarflags, f))
	
	for file in self.sources:
            f = lookaside.findAll(self.cfg, file, self.name, self.srcdirs)
	    destDir = builddir + "/" + self.theMainDir
	    util.mkdirChain(destDir)
	    shutil.copyfile(f, destDir + "/" + file)

	for (file, level, backup) in self.patches:
            # XXX handle .gz/.bz2 patch files
            f = util.findFile(file, self.srcdirs)
	    destDir = builddir + "/" + self.theMainDir
            if backup:
                backup = '-b -z %s' % backup
            os.system('patch -d %s -p%s %s < %s' %(destDir, level, backup, f))

    def doBuild(self, buildpath):
        builddir = buildpath + "/" + self.mainDir()
        if self.build is None:
            pass
        elif type(self.build) is str:
            os.system(self.build % {'builddir':builddir})
        elif type(self.build) == types.TupleType:
	    for bld in self.build:
                if type(bld) is str:
                    os.system(bld % {'builddir':builddir})
                else:
                    bld.doBuild(builddir)
	else:
	    self.build.doBuild(builddir)

    def doInstall(self, buildpath, root):
        builddir = buildpath + "/" + self.mainDir()
        if self.install is None:
            pass
        elif type(self.install) is str:
            os.system(self.install % {'builddir':builddir, 'destdir':root})
	elif type(self.install) == types.TupleType:
	    for inst in self.install:
                if type(inst) is str:
                    os.system(inst % {'builddir':builddir, 'destdir':root})
                else:
                    inst.doInstall(builddir, root)
	else:
	    self.install.doInstall(builddir, root)

    def packages(self, root):
        self.packageSet = package.Auto(self.name, root)

    def getPackageSet(self):
        return self.packageSet

    def __init__(self, cfg, srcdirs):
	self.tarballs = []
	self.patches = []
	self.sources = []
	self.signatures = {}
        self.cfg = cfg
	self.srcdirs = srcdirs
	self.theMainDir = self.name + "-" + self.version
	self.build = build.Make()
        self.install = build.MakeInstall()
