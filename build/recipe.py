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

class RecipeLoader(types.DictionaryType):
    def __init__(self, file):
        self.module = imp.new_module(file)
        f = open(file)
        
        exec 'from recipe import Recipe' in self.module.__dict__
        exec 'import build, os, package, sys, util' in self.module.__dict__
        exec 'sys.excepthook = util.excepthook' in self.module.__dict__ 
        code = compile(f.read(), file, 'exec')
        exec code in self.module.__dict__
        for (key, value) in  self.module.__dict__.items():
            if type(value) == types.ClassType:
                # make sure the class is derived from something
                # and has a name
                # XXX better test?
                if len(value.__bases__) > 0 and 'name' in dir(value):
                    self[key] = value

class Recipe:

    def addTarball(self, file):
	self.tarballs.append(file)

    def addPatch(self, file):
	self.patches.append(file)

    def addSource(self, file):
	self.sources.append(file)

    def allSources(self):
	return self.sources + self.tarballs + self.patches

    def mainDir(self, new = None):
	if new:
	    self.theMainDir = new

	return self.theMainDir

    def nameVer(self):
	return self.name + "-" + self.version

    def cleanup(self, builddir, rootDir):
	shutil.rmtree(builddir)
	shutil.rmtree(rootDir)

    def unpackSources(self, srcdirs, builddir):
	if os.path.exists(builddir):
	    shutil.rmtree(builddir)
	util.mkdirChain(builddir)
	for file in self.tarballs:
            f = util.findFile(file, srcdirs)
            if f.endswith(".bz2"):
                tarflags = "-jxf"
            elif f.endswith(".gz") or f.endswith(".tgz"):
                tarflags = "-zxf"
            else:
                raise RuntimeError, "unknown archive compression"
	    os.system("tar -C %s %s %s" % (builddir, tarflags, f))
	
	for file in self.sources:
            f = util.findFile(file, srcdirs)
	    destDir = builddir + "/" + self.theMainDir
	    util.mkdirChain(destDir)
	    shutil.copyfile(f, destDir + "/" + file)

    def doBuild(self, builddir):
        if self.build is None:
            pass
        elif type(self.build) == types.TupleType:
	    for bld in self.build:
		bld(builddir + "/" + self.mainDir())
	else:
	    self.build.doBuild(builddir + "/" + self.mainDir())

    def doInstall(self, builddir, root):
	if type(self.install) == types.TupleType:
	    for inst in self.install:
		inst.doInstall(builddir + "/" + self.mainDir(), root)
	else:
	    self.install.doInstall(builddir + "/" + self.mainDir(), root)

    def packages(self, root):
        self.packageSet = package.Auto(self.name, root)

    def getPackageSet(self):
        return self.packageSet

    def __init__(self):
	self.tarballs = []
	self.patches = []
	self.sources = []
	self.theMainDir = self.name + "-" + self.version
	self.build = build.Make()
