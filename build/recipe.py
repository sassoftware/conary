#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import imp, sys, types
import os
import util
import build
import package

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

    def allSources(self):
	return self.tarballs + self.patches

    def mainDir(self, new = None):
	if new:
	    self.theMainDir = new

	return self.theMainDir

    def nameVer(self):
	return self.name + "-" + self.version

    def unpackSources(self, srcdir, builddir):
	util.mkdirChain(builddir)
	for file in self.tarballs:
	    os.system("tar -C %s -xvzf %s" % (builddir, srcdir + "/" + file))

    def doBuild(self, builddir):
	self.build.doBuild(builddir + "/" + self.mainDir())

    def doInstall(self, builddir, root):
	self.install.doInstall(builddir + "/" + self.mainDir(), root)

    def packages(self, root):
        self.packageSet = package.Auto(self.name, root)

    def getPackageSet(self):
        return self.packageSet

    def __init__(self):
	self.tarballs = []
	self.patches = []
	self.theMainDir = self.name + "-" + self.version
	self.build = build.Make()
