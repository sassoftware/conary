#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import imp, sys, types
import os
import util
import build
import package
import shutil
import types
import inspect

def flatten(list):
    if type(list) != type([]): return [list]
    if list == []: return list
    return flatten(list[0]) + flatten(list[1:])

class RecipeLoader(types.DictionaryType):
    def __init__(self, file):
        self.module = imp.new_module(file)
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
                    self[key] = value

# XXX this should be extended to load a recipe from srs
def loadRecipe(file):
    callerGlobals = inspect.stack()[1][0].f_globals
    if file[0] != '/':
        recipepath = os.path.dirname(callerGlobals['filename'])
        file = recipepath + '/' + file
    recipes = RecipeLoader(file)
    for name, recipe in recipes.items():
        # XXX hack to hide parent recipies
        recipe.ignore = 1
        callerGlobals[name] = recipe
        # stash a reference to the module in the namespace
        # of the recipe that loaded it, or else it will be destroyed
        callerGlobals[file] = recipes
        
class Recipe:

    def addSignature(self, file):
	md5 = util.searchFile('%s.md5sum' %(file), self.srcdirs)
	if md5:
	    if not self.signatures.has_key(file):
		self.signatures[file] = []
	    self.signatures[file].append(md5)

	gpg = util.searchFile('%s.sign' %(file), self.srcdirs)
	if not gpg:
	    gpg = util.searchFile('%s.sig' %(file), self.srcdirs)
	if gpg:
	    if not self.signatures.has_key(file):
		self.signatures[file] = []
	    self.signatures[file].append(gpg)


    def addTarball(self, file):
	self.tarballs.append(file)
	self.addSignature(file)

    def addPatch(self, file):
	self.patches.append(file)
	self.addSignature(file)

    def addSource(self, file):
	self.sources.append(file)
	self.addSignature(file)

    def allSources(self):
	return self.sources + self.tarballs + self.patches + \
               flatten(self.signatures)

    def mainDir(self, new = None):
	if new:
	    self.theMainDir = new

	return self.theMainDir

    def nameVer(self):
	return self.name + "-" + self.version

    def cleanup(self, builddir, rootDir):
	shutil.rmtree(builddir)
	shutil.rmtree(rootDir)

    def checkSignatures(self, filepath, file):
        if not self.signatures.has_key(file):
            return
	for signature in self.signatures[file]:
	    if signature.endswith(".md5sum"):
		if os.system("cat %s | md5sum --check %s"
			      %(signature, filepath)):
		    raise RuntimeError, "md5 signature %s failed" %(signature)
	    elif signature.endswith(".sign") or signature.endswith(".sig"):
		if os.system("gpg --no-secmem-warning --verify %s %s"
			      %(signature, filepath)):
		    raise RuntimeError, "GPG signature %s failed" %(signature)

    def unpackSources(self, builddir):
	if os.path.exists(builddir):
	    shutil.rmtree(builddir)
	util.mkdirChain(builddir)
	for file in self.tarballs:
            f = util.findFile(file, self.srcdirs)
	    self.checkSignatures(f, file)
            if f.endswith(".bz2"):
                tarflags = "-jxf"
            elif f.endswith(".gz") or f.endswith(".tgz"):
                tarflags = "-zxf"
            else:
                raise RuntimeError, "unknown archive compression"
	    os.system("tar -C %s %s %s" % (builddir, tarflags, f))
	
	for file in self.sources:
            f = util.findFile(file, self.srcdirs)
	    destDir = builddir + "/" + self.theMainDir
	    util.mkdirChain(destDir)
	    shutil.copyfile(f, destDir + "/" + file)

    def doBuild(self, builddir):
        if self.build is None:
            pass
        elif type(self.build) == types.TupleType:
	    for bld in self.build:
		bld.doBuild(builddir + "/" + self.mainDir())
	else:
	    self.build.doBuild(builddir + "/" + self.mainDir())

    def doInstall(self, builddir, root):
        if self.install is None:
            pass
	elif type(self.install) == types.TupleType:
	    for inst in self.install:
		inst.doInstall(builddir + "/" + self.mainDir(), root)
	else:
	    self.install.doInstall(builddir + "/" + self.mainDir(), root)

    def packages(self, root):
        self.packageSet = package.Auto(self.name, root)

    def getPackageSet(self):
        return self.packageSet

    def __init__(self, srcdirs):
	self.tarballs = []
	self.patches = []
	self.sources = []
	self.signatures = {}
	self.srcdirs = srcdirs
	self.theMainDir = self.name + "-" + self.version
	self.build = build.Make()
        self.install = build.MakeInstall()
