#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import os
import versioned
import string
import types
import util
import versions

# this is the repository's idea of a package
class Package:

    def addFile(self, id, path, version):
	self.files[path] = (id, path, version)

    def fileList(self):
	l = []
        paths = self.files.keys()
        paths.sort()
        for path in paths:
	    l.append(self.files[path])

	return l

    def formatString(self):
	str = ""
	for (fileId, path, version) in self.files.values():
	    str = str + ("%s %s %s\n" % (fileId, path, version.asString()))
	return str

    def __init__(self, name):
	self.files = {}
	self.name = name

class PackageFromFile(Package):

    def read(self, dataFile):
	for line in dataFile.readLines():
	    (fileId, path, version) = string.split(line)
	    version = versions.VersionFromString(version)
	    self.addFile(fileId, path, version)

    def __init__(self, name, dataFile):
	Package.__init__(self, name)
	self.read(dataFile)

def stripNamespace(namespace, str):
    if str[:len(namespace) + 1] == namespace + "/":
	return str[len(namespace) + 1:]
    return str

# this is a set of all of the versions of a single packages 
class PackageSet:
    def getVersion(self, version):
	f1 = self.f.getVersion(version)
	p = PackageFromFile(self.name, f1)
	f1.close()
	return p

    def hasVersion(self, version):
	return self.f.hasVersion(version)

    def addVersion(self, version, package):
	self.f.addVersion(version, package.formatString())

    def versionList(self):
	return self.f.versionList()

    def getLatestPackage(self, branch):
	return self.getVersion(self.f.findLatestVersion(branch))

    def getLatestVersion(self, branch):
	return self.f.findLatestVersion(branch)
	
    def close(self):
	self.f.close()
	self.f = None

    def __del__(self):
	if self.f: self.close()

    def __init__(self, dbpath, name, mode = "r"):
	self.name = name
	self.pkgPath = dbpath + self.name
	self.packages = {}

	util.mkdirChain(os.path.dirname(self.pkgPath))

	if mode == "r":
	    self.f = versioned.open(self.pkgPath, "r")
	elif os.path.exists(self.pkgPath):
	    self.f = versioned.open(self.pkgPath, "r+")
	else:
	    self.f = versioned.open(self.pkgPath, "w+")

#----------------------------------------------------------------------------

# this is the build system's idea of a package. maybe they'll merge. someday.

class BuildFile:

    def configFile(self):
	self.isConfigFile = 1

    def __init__(self):
	self.isConfigFile = 0

class BuildPackage(types.DictionaryType):

    def addFile(self, path):
	self[path] = BuildFile()

    def addDirectory(self, path):
	self[path] = BuildFile()

    def __init__(self, name):
	self.name = name
	types.DictionaryType.__init__(self)

class BuildPackageSet:

    def addPackage(self, pkg):
	self.__dict__[pkg.name] = pkg
	self.pkgs[pkg.name] = pkg

    def packageSet(self):
	return self.pkgs.items()

    def __init__(self, name):
	self.name = name
	self.pkgs = {}

def Auto(name, root):
    runtime = BuildPackage("runtime")
    mans = BuildPackage("man")
    os.path.walk(root, autoVisit, (root, runtime, mans))

    set = BuildPackageSet(name)
    set.addPackage(runtime)
    if mans.keys():
	set.addPackage(mans)
    
    return set

def autoVisit(arg, dir, files):
    (root, buildPkg, manPkg) = arg
    dir = dir[len(root):]

    for file in files:
        if dir:
            path = dir + '/' + file
        else:
            path = '/' + file
        if path.startswith('/usr/share/man/'):
            manPkg.addFile(path)
        else:
            buildPkg.addFile(path)
