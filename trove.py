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

    def addFile(self, fileId, path, version):
	self.files[path] = (fileId, path, version)

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

    def idmap(self):
	map = {}
	for (fileId, path, version) in self.files.values():
	    map[fileId] = (path, version)

	return map

    def diff(self, them):
	# find all of the file ids which have been added, removed, and
	# stayed the same
	selfMap = self.idmap()

	if them:
	    themMap = them.idmap()
	else:
	    themMap = {}

	rc = ""

	removedIds = []
	addedIds = []
	sameIds = {}
	filesNeeded = []

	allIds = selfMap.keys() + themMap.keys()
	for id in allIds:
	    inSelf = selfMap.has_key(id)
	    inThem = themMap.has_key(id)
	    if inSelf and inThem:
		sameIds[id] = None
	    elif inSelf:
		addedIds.append(id)
	    else:
		removedIds.append(id)

	for id in removedIds:
	    rc = rc + "-%s\n" % id

	for id in addedIds:
	    (selfPath, selfVersion) = selfMap[id]
	    rc = rc + "+%s %s %s\n" % (id, selfPath, selfVersion.asString())
	    filesNeeded.append((id, None, selfVersion))

	for id in sameIds.keys():
	    
	    (selfPath, selfVersion) = selfMap[id]
	    (themPath, themVersion) = themMap[id]

	    if selfPath != themPath:
		rc = rc + "~%s path %s %s\n" % (id, themPath, selfPath)

	    if not selfVersion.equal(themVersion):
		rc = rc + "~%s version %s %s\n" % \
		    (id, themVersion.asString(), selfVersion.asString())
		filesNeeded.append((id, themVersion, selfVersion))

	return (rc, filesNeeded)

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

    def changeSet(self, oldVersion, newVersion):
	if oldVersion:
	    old = self.getVersion(oldVersion)
	else:
	    old = None

	new = self.getVersion(newVersion)
	newStr = newVersion.asString()

	(rc, filesNeeded) = new.diff(old)

	if oldVersion:
	    oldStr = oldVersion.asString()
	else:
	    oldStr = "(none)"

	rc = "SRS PKG CHANGESET %s %s %s %d\n" % (self.name, oldStr, newStr, 
						  rc.count("\n")) + rc
	return rc
	
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
