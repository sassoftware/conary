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
import files

class BuildFile(files.FileMode):
    def getRealPath(self):
        return self.realPath

    def getType(self):
        return self.type

    def __init__(self, realPath, type):
        files.FileMode.__init__(self)
        self.realPath = realPath
        self.type = type

class BuildDeviceFile(files.DeviceFile, BuildFile):
    def __init__(self, devtype, major, minor, owner, group, perms):
        BuildFile.__init__(self, None, "auto")

        self.infoTag = devtype
        self.major = major
        self.minor = minor
        self.theOwner = owner
        self.theGroup = group
        self.thePerms = perms
        self.theSize = 0
        self.theMtime = 0
        
class BuildPackage(types.DictionaryType):

    def addFile(self, path, realPath, type="auto"):
        """
        Add a file to the build package

        @param path: the destination of the file in the package
        @param realPath: the location of the actual file on the filesystem,
        used to obtain the contents of the file when creating a changeset
        to commit to the repository
        @param type: type of file.  Use "src" for source files.
        """
	self[path] = BuildFile(realPath, type)

    def addDevice(self, path, devtype, major, minor,
                  user='root', group='root', perms=0660):
        """
        Add a device node to the build package

        @param path: the destination of the device node in the package
        """
	self[path] = BuildDeviceFile(devtype, major, minor, user, group, perms)

    def getName(self):
	return self.name

    def getVersion(self):
	return self.version

    def __init__(self, name, version):
	self.name = name
	self.version = version
	types.DictionaryType.__init__(self)

class BuildPackageSet:

    def addPackage(self, pkg):
	self.__dict__[pkg.name] = pkg
	self.pkgs[pkg.name] = pkg

    def packageSet(self):
	return self.pkgs.items()

    def __init__(self):
	self.pkgs = {}

class Filter:
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

class AutoBuildPackage:
    """
    AutoBuildPackage creates a set of BuildPackage instances and
    provides facilities for automatically populating them with files
    according to Filters.
    """
    def __init__(self, namePrefix, version, mainFilters, subFilters):
        """
	@param namePrefix: the fully qualified name of the main package
	such as ":srs.specifixinc.com"
	@param version: a versionObject specifying the version of the
	package, which is used as the version of each subpackage
	@param mainFilters: Filters used to add files to main packages
	@type mainFilters: sequence of Filter instances
	@param subFilters: Filters used to add files to sub packages
	@type subFilters: sequence of Filter instances
	"""
	self.mainFilters = mainFilters
        self.subFilters = subFilters
        # dictionary of all the build packages
        self.packages = {}
        # reverse map from the main-package:sub-package combination to
        # the correct build package
	self.packageMap = {}
	for main in self.mainFilters:
	    for sub in self.subFilters:
		name = self._getname(namePrefix, main.name, sub.name)
                package = BuildPackage(name, version)
		self.packages[name] = package
		if not self.packageMap.has_key(main):
		    self.packageMap[main] = {}
		self.packageMap[main][sub] = package

    def _getname(self, prefix, pkgname, subname):
        return string.join((prefix, pkgname, subname), ':')

    def findPackage(self, path):
        """Return the BuildPackage that matches the path"""
	for main in self.mainFilters:
	    if main.match(path):
		for sub in self.subFilters:
		    if sub.match(path):
			return self.packageMap[main][sub]
        return None
    
    def addFile(self, path, realPath):
        """
        Add a path to the correct BuildPackage instance by matching
        the file name against the main-package and sub-package filters

        @param path: path to add to the BuildPackage
        @type path: str
        @rtype: None
        """
        pkg = self.findPackage(path)
        pkg.addFile(path, realPath)

    def addDevice(self, path, devtype, major, minor,
                  user='root', group='root', perms=0660):
        """
        Add a device to the correct BuildPackage instance by matching
        the file name against the main package and sub-package filters
        """
        pkg = self.findPackage(path)
        pkg.addDevice(path, devtype, major, minor, user, group, perms)

    def packageSet(self):
        """
        Examine the BuildPackage instances that have been created and
        return a new BuildPackageSet instance that includes only those
        which have files
        
        @return: list of BuildPackages instances
        @rtype: list
        """
        set = BuildPackageSet()
        for name in self.packages.keys():
            if self.packages[name].keys():
                set.addPackage(self.packages[name])
        return set
            
    def walk(self, root):
        """
        Traverse the directory tree specified by @C{root}, adding entries
        to the BuildPackages

        @param root: root of path to walk
        @type root: str
        @rtype: None
        """
        os.path.walk(root, _autoVisit, (root, self))

def _autoVisit(arg, dir, files):
    """
    Helper function called by os.path.walk() when AutoBuildPackage.walk()
    is called
    """
    (root, autopkg) = arg
    dir = dir[len(root):]

    for file in files:
        if dir:
            path = dir + '/' + file
        else:
            path = '/' + file

        autopkg.addFile(path, root + path)
