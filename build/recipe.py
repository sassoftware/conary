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
        exec 'import recipe' in self.module.__dict__
        exec 'import build' in self.module.__dict__
        exec 'import os' in self.module.__dict__
        exec 'import package' in self.module.__dict__
        code = compile(f.read(), file, 'exec')
        exec code in self.module.__dict__
        for (key, value) in  self.module.__dict__.items():
            if type(value) == types.ClassType:
                # XXX better test?
                if 'nameVer' in dir(value):
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

    def unpackSources(self, srcdir, builddir):
	util.mkdirChain(builddir)
	for file in self.tarballs:
            if file.endswith(".bz2"):
                tarflags = "-jxvf"
            elif file.endswith(".gz") or file.endswit(".tgz"):
                tarflags = "-zxvf"
            else:
                raise RuntimeError, "unknown archive compression"
	    os.system("tar -C %s %s %s" % (builddir, tarflags,
                                           srcdir + "/" + file))
	
	for file in self.sources:
	    destDir = builddir + "/" + self.theMainDir
	    util.mkdirChain(destDir)
	    shutil.copyfile(srcdir + "/" + file, destDir + "/" + file)

    def doBuild(self, builddir):
	if type(self.build) == types.TupleType:
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
