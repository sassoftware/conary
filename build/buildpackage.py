#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

#----------------------------------------------------------------------------
# this is the build system's idea of a package. maybe they'll merge. someday.

import types
import string
import re
import os
import sha1helper
import time

class BuildFile:

    def configFile(self):
	self.isConfigFile = 1

    def __init__(self):
	self.isConfigFile = 0

class BuildPackage(types.DictionaryType):

    def addFile(self, path):
	self[path] = BuildFile()

    def getName(self):
	return self.name

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
    def __init__(self, namePrefix, auto, explicit):
	"""Storage area for (sub)package definitions; keeps
	automatic subpackage definitions (like runtime, doc,
	etc) and explicit subpackage definitions (higher-level
	subpackages; each automatic subpackage applies to each
	explicit subpackage.

	@param namePrefix: the prefix to use to build a full name from the
	subpackage name, such as ":srs.specifixinc.com:tmpwatch"
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
		name = self._getname(namePrefix, explicitspec.name, 
				     autospec.name)
		self[name] = PackageSpecInstance(BuildPackage(name),
                                                 explicitspec, autospec)
		self.packageList.append(name)
		if not self.packageMap.has_key(explicitspec.name):
		    self.packageMap[explicitspec.name] = {}
		self.packageMap[explicitspec.name][autospec.name] = self[name]

    def _getname(self, prefix, subname, autoname):
        """Returns the full name of the package when subname could be None"""
	if subname:
	    return prefix + ":" + subname + ":" + autoname
	else:
	    return prefix + ":" + autoname
    
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
