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
import re
import files
import sha1helper
import time

# this is the repository's idea of a package
class Package:
    def getName(self):
        return self.name
    
    def getVersion(self):
        return self.version
    
    def setVersion(self, version):
        self.version = version
    
    def getFileMap(self):
        return self.fileMap
    
    def setFileMap(self, map):
        self.fileMap = map

    def addFile(self, fileId, path, version):
	self.files[path] = (fileId, path, version)
	self.idMap[fileId] = (path, version)

    # fileId is the only thing that must be here; the other fields could
    # be "-"
    def updateFile(self, fileId, path, version):
	(origPath, origVersion) = self.idMap[fileId]

	if not path:
	    path = origPath
	else:
	    del self.files[path]

	if not version:
	    version = origVersion
	    
	self.files[path] = (fileId, path, version)
	self.idMap[fileId] = (path, version)

    def removeFile(self, fileId):   
	path = self.idMap[fileId][0]
	del self.files[path]
	del self.idMap[fileId]

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

    # returns a dictionary mapping a fileId to a (path, version, pkgName) tuple
    def applyChangeSet(self, repos, pkgCS):
	fileMap = {}

	for (fileId, path, fileVersion) in pkgCS.getNewFileList():
	    self.addFile(fileId, path, fileVersion)
	    fileMap[fileId] = self.idMap[fileId] + (self.name, )
	    (path, fileVersion, self.name)

	for (fileId, path, fileVersion) in pkgCS.getChangedFileList():
	    self.updateFile(fileId, path, fileVersion)
	    # look up the path/version in self.idMap as the ones here
	    # could be None
	    fileMap[fileId] = self.idMap[fileId] + (self.name, )

	for fileId in pkgCS.getOldFileList():
	    self.removeFile(fileId)

	return fileMap

    def diff(self, them, themVersion, ourVersion):
	# find all of the file ids which have been added, removed, and
	# stayed the same
	if them:
	    themMap = them.idMap
	    chgSet = PackageChangeSet(self.name, themVersion, ourVersion)
	else:
	    themMap = {}
	    chgSet = PackageChangeSet(self.name, None, ourVersion)

	removedIds = []
	addedIds = []
	sameIds = {}
	filesNeeded = []

	allIds = self.idMap.keys() + themMap.keys()
	for id in allIds:
	    inSelf = self.idMap.has_key(id)
	    inThem = themMap.has_key(id)
	    if inSelf and inThem:
		sameIds[id] = None
	    elif inSelf:
		addedIds.append(id)
	    else:
		removedIds.append(id)

	for id in removedIds:
	    chgSet.oldFile(id)

	for id in addedIds:
	    (selfPath, selfVersion) = self.idMap[id]
	    filesNeeded.append((id, None, selfVersion))
	    chgSet.newFile(id, selfPath, selfVersion)

	for id in sameIds.keys():
	    (selfPath, selfVersion) = self.idMap[id]
	    (themPath, themVersion) = themMap[id]

	    newPath = None
	    newVersion = None

	    if selfPath != themPath:
		newPath = selfPath

	    if not selfVersion.equal(themVersion):
		newVersion = selfVersion
		filesNeeded.append((id, themVersion, selfVersion))

	    if newPath or newVersion:
		chgSet.changedFile(id, newPath, newVersion)

	return (chgSet, filesNeeded)

    def __init__(self, name):
	self.files = {}
	self.idMap = {}
	self.name = name

class PackageChangeSet:

    def newFile(self, fileId, path, version):
	self.newFiles.append((fileId, path, version))

    def getNewFileList(self):
	return self.newFiles

    def oldFile(self, fileId):
	self.oldFiles.append(fileId)

    def getOldFileList(self):
	return self.oldFiles

    def getName(self):
	return self.name

    def getOldVersion(self):
	return self.oldVersion

    def getNewVersion(self):
	return self.newVersion

    # path and/or version can be None
    def changedFile(self, fileId, path, version):
	self.changedFiles.append((fileId, path, version))

    def getChangedFileList(self):
	return self.changedFiles

    def parse(self, line):
	action = line[0]

	if action == "+" or action == "~":
	    (fileId, path, version) = string.split(line[1:])

	    if version == "-":
		version = None
	    else:
		version = versions.VersionFromString(version)

	    if path == "-":
		path = None

	    if action == "+":
		self.newFile(fileId, path, version)
	    else:
		self.changedFile(fileId, path, version)
	elif action == "-":
	    self.oldFile(line[1:])

    def formatToFile(self, changeSet, cfg, f):
	f.write("%s " % self.name)
	if self.oldVersion:
	    f.write("from %s to " % self.oldVersion.asString(cfg.defaultbranch))
	else:
	    f.write("abstract ")
	f.write("%s\n" % self.newVersion.asString(cfg.defaultbranch))

	for (fileId, path, version) in self.newFiles:
	    f.write("\tadded %s\n" % path)
	for (fileId, path, version) in self.changedFiles:
	    f.write("\tchanged %s\n" % path)
	    change = changeSet.getFileChange(fileId)
	    print "\t\t%s" % change
	for path in self.oldFiles:
	    f.write("\tremoved %s(.*)%s\n" % (path[:8], path[-8:]))

    def asString(self):
	rc = ""

	for id in self.getOldFileList():
	    rc += "-%s\n" % id

	for (id, path, version) in self.getNewFileList():
	    rc += "+%s %s %s\n" % (id, path, version.asString())

	for (id, path, version) in self.getChangedFileList():
	    rc += "~%s " % id
	    if path:
		rc += path
	    else:
		rc += "-"

	    if version:
		rc += " " + version.asString() + "\n"
	    else:
		rc += " -\n"

	if self.oldVersion:
	    oldVerStr = self.oldVersion.asString()
	else:
	    oldVerStr = "(none)"

	hdr = "SRS PKG CHANGESET %s %s %s %d\n" % \
		  (self.name, oldVerStr, self.newVersion.asString(), 
		   rc.count("\n"))
	return hdr + rc	
    
    def __init__(self, name, oldVersion, newVersion):
	self.name = name
	self.oldVersion = oldVersion
	self.newVersion = newVersion
	self.newFiles = []
	self.oldFiles = []
	self.changedFiles = []

class PackageFromFile(Package):

    def read(self, dataFile):
	for line in dataFile.readLines():
	    (fileId, path, version) = string.split(line)
	    version = versions.VersionFromString(version)
	    self.addFile(fileId, path, version)

    def __init__(self, name, dataFile):
	Package.__init__(self, name)
	self.read(dataFile)

def stripNamespace(namespace, pkgName):
    if pkgName.startswith(namespace + ":"):
	return pkgName[len(namespace) + 1:]
    return pkgName

# this is a set of all of the versions of a single packages 
class PackageSet:
    def getVersion(self, version):
	f1 = self.f.getVersion(version)
	p = PackageFromFile(self.name, f1)
	f1.close()
	return p

    def hasVersion(self, version):
	return self.f.hasVersion(version)

    def eraseVersion(self, version):
	self.f.eraseVersion(version)

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
	self.f = None

    def __init__(self, db, name):
	self.name = name
	self.f = db.openFile(name)

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

class PackageSpec:
    
    def __init__(self, name, relist):
	self.name = name
	tmplist = []
	if type(relist) is str:
	    regexp = relist
	else:
	    for subre in relist:
		if subre[:1] == '/':
		    subre = '^' + subre
		if subre[-1:] != '/':
		    subre = subre + '$'
		tmplist.append('(' + subre + ')')
	    regexp = string.join(tmplist, '|')
	self.regexp = re.compile(regexp)

    def match(self, string):
	# search instead of match in order to not automatically
	# front-anchor searches
	return self.regexp.search(string)

class PackageSpecInstance:
    """An instance of a spec formed by the conjugation of an explicitspec and
    an autospec"""
    def __init__(self, instance, explicitspec, autospec):
	self.instance = instance
	self.explicitspec  = explicitspec
	self.autospec = autospec

class PackageSpecSet(dict):
    """An "ordered dictionary" containing PackageSpecInstances"""
    def __init__(self, auto, explicit):
	"""Storage area for (sub)package definitions; keeps
	automatic subpackage definitions (like runtime, doc,
	etc) and explicit subpackage definitions (higher-level
	subpackages; each automatic subpackage applies to each
	explicit subpackage.
	
	@param auto: automatic subpackage list
	@type auto: tuple of (name, regex) or (name, (tuple, of
	regex)) tuples
	@param explicit: explicit subpackage list
	@type explicit: tuple of (name, regex) or (name, (tuple, of
	regex)) tuples
	"""
	self.auto = auto
	if explicit:
	    self.explicit = explicit
	else:
	    self.explicit = (PackageSpec('', '.*'), )
	self.packageList = []
	self.packageMap = {}
	for explicitspec in self.explicit:
	    for autospec in self.auto:
		name = self._getname(explicitspec.name, autospec.name)
		self[name] = PackageSpecInstance(BuildPackage(name),
                                                 explicitspec, autospec)
		self.packageList.append(name)
		if not self.packageMap.has_key(explicitspec.name):
		    self.packageMap[explicitspec.name] = {}
		self.packageMap[explicitspec.name][autospec.name] = self[name]

    def _getname(self, subname, autoname):
	"""Cheap way of saying "if subname, then subname/autoname,
	otherwise just autoname"."""
	return string.lstrip(string.join((subname, autoname), '/'), '/')
    
    def add(self, path, autospec, explicitspec):
	self.packageMap[explicitspec.name][autospec.name].instance.addFile(path)


def Auto(name, root, specSet):
    os.path.walk(root, autoVisit, (root, specSet))

    set = BuildPackageSet(name)
    for name in specSet.packageList:
	if specSet[name].instance.keys():
	    set.addPackage(specSet[name].instance)
    return set

def autoVisit(arg, dir, files):
    (root, specSet) = arg
    dir = dir[len(root):]

    for file in files:
        if dir:
            path = dir + '/' + file
        else:
            path = '/' + file
	
	for explicitspec in specSet.explicit:
	    if explicitspec.match(path):
		for autospec in specSet.auto:
		    if autospec.match(path):
			specSet.add(path, autospec, explicitspec)
			break
		break

def packageSetExists(db, pkg):
    return db.hasFile(pkg)

# type could be "src"
def createPackage(repos, cfg, destdir, fileList, name, version, ident, 
		  pkgtype = "auto"):
    fileMap = {}
    p = Package(name)

    for filePath in fileList:
	if pkgtype == "auto":
	    realPath = destdir + filePath
	    targetPath = filePath
	else:
	    realPath = filePath
	    targetPath = os.path.basename(filePath)

	file = files.FileFromFilesystem(realPath, ident(targetPath), 
					type = pkgtype)

	fileDB = repos.getFileDB(file.id())

        duplicateVersion = fileDB.checkBranchForDuplicate(cfg.defaultbranch,
                                                          file)
        if not duplicateVersion:
	    p.addFile(file.id(), targetPath, version)
	else:
	    p.addFile(file.id(), targetPath, duplicateVersion)

        fileMap[file.id()] = (file, realPath, targetPath)

    p.setFileMap(fileMap)
    p.setVersion(version)
    return p

class IdGen:
    def __call__(self, path):
	if self.map.has_key(path):
	    return self.map[path]

	hash = sha1helper.hashString("%s %f %s" % (path, time.time(), 
							self.noise))
	self.map[path] = hash
	return hash

    def __init__(self, map=None):
	# file ids need to be unique. we include the time and path when
	# we generate them; any data put here is also used
	uname = os.uname()
	self.noise = "%s %s" % (uname[1], uname[2])
        if map is None:
            self.map = {}
        else:
            self.map = map

    def populate(self, cfg, repos, lcache, name):
	# Find the files and ids which were owned by the last version of
	# this package on the branch. We also construct an object which
	# lets us look for source files this build needs inside of the
	# repository
	fileIdMap = {}
	fullName = cfg.packagenamespace + ":" + name
	pkg = None
	for pkgName in repos.getPackageList(fullName):
	    pkgSet = repos.getPackageSet(pkgName)
	    pkg = pkgSet.getLatestPackage(cfg.defaultbranch)
	    for (fileId, path, version) in pkg.fileList():
		fileIdMap[path] = fileId
		if path[0] != "/":
		    # we might need to retrieve this source file
		    # to enable a build, so we need to find the
		    # sha1 hash of it since that's how it's indexed
		    # in the file store
		    filedb = repos.getFileDB(fileId)
		    file = filedb.getVersion(version)
		    lcache.addFileHash(path, file.sha1())

        self.map.update(fileIdMap)
