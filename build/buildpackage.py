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

class BuildFile:
    def getRealPath(self):
        return self.realPath

    def getType(self):
        return self.type

    def __init__(self, realPath, type):
        self.realPath = realPath
        self.type = type

class BuildDeviceFile(BuildFile):
    def getRealPath(self):
        return self.realPath

    def getType(self):
        return self.type

    def infoLine(self):
        # type major minor perms owner group size mtime
        return "%c %d %d 0%o %s %s 0 0" % (self.devtype, self.major,
                                           self.minor, self.perms,
                                           self.owner, self.group)

    def __init__(self, devtype, major, minor, owner, group, perms):
        self.type = "auto"
        self.realPath = None

        self.devtype = devtype
        self.major = major
        self.minor = minor
        self.owner = owner
        self.group = group
        self.perms = perms

class BuildPackage(types.DictionaryType):

    def addFile(self, path, realPath, type="auto"):
        """add a file to the build package
        @param path: the destination of the file in the package
        @param realPath: the location of the actual file on the filesystem,
        used to obtain the contents of the file when creating a changeset
        to commit to the repository
        @param type: type of file.  Use "src" for source files.
        """
	self[path] = BuildFile(realPath, type)

    def addDevice(self, path, devtype, major, minor,
                  user='root', group='root', perms=0660):
        """add a device node to the build package
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

class BuildPackageFactory:
    """BuildPackageFactory creates a set of BuildPackage instances
    and provides facilities for populating them with files according
    to PackageSpecs.
    """
    def __init__(self, namePrefix, version, auto, explicit):
        """
	@param namePrefix: the fully qualified name of the main package
	such as ":srs.specifixinc.com:tmpwatch"
	@param version: a versionObject specifying the version of the
	package, which is used as the version of each subpackage
	@param auto: automatic subpackage list
	@type auto: sequence of PackageSpec instances
	@param explicit: explicit subpackage list
	@type explicit: sequence of PackageSpec instances
	"""
	self.auto = auto
	if explicit:
	    self.explicit = explicit
	else:
	    self.explicit = (PackageSpec('', '.*'), )
        # dictionary of all the build packages
        self.packages = {}
        # reverse map from the explicitspec/autospec combination to
        # the correct build package
	self.packageMap = {}
	for explicitspec in self.explicit:
	    for autospec in self.auto:
		name = self._getname(namePrefix, explicitspec.name,
                                     autospec.name)
                package = BuildPackage(name, version)
		self.packages[name] = package
		if not self.packageMap.has_key(explicitspec):
		    self.packageMap[explicitspec] = {}
		self.packageMap[explicitspec][autospec] = package

    def _getname(self, prefix, subname, autoname):
        """Return the full name of the package when subname could be None"""
	if subname:
	    return string.join((prefix, subname, autoname), ':')
	else:
	    return string.join((prefix, autoname), ':')

    def findPackage(self, path):
        """Return the BuildPackage that matches the path"""
	for explicitspec in self.explicit:
	    if explicitspec.match(path):
		for autospec in self.auto:
		    if autospec.match(path):
			return self.packageMap[explicitspec][autospec]
        return None
    
    def addFile(self, path, realPath):
        """Add a path to the correct BuildPackage instance by matching
        the file name against the the explicit and auto specs

        @param path: path to add to the BuildPackage
        @type path: str
        @rtype: None
        """
        pkg = self.findPackage(path)
        pkg.addFile(path, realPath)

    def addDevice(self, path, devtype, major, minor,
                  user='root', group='root', perms=0660):
        """Add a device to the correct BuildPackage instance by matching
        the file name against the the explicit and auto specs
        """
        pkg = self.findPackage(path)
        pkg.addDevice(path, devtype, major, minor, user, group, perms)

    def packageSet(self):
        """Examine the BuildPackage instances created by the factory
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
        """Traverse the directory tree specified by @C{root}, adding entries
        to the BuildPackages
        @param root: root of path to walk
        @type root: str
        @rtype: None
        """
        os.path.walk(root, _autoVisit, (root, self))

def _autoVisit(arg, dir, files):
    """Helper function called by os.path.walk() when
    BuildPackageFactory.walk() is called"""
    (root, factory) = arg
    dir = dir[len(root):]

    for file in files:
        if dir:
            path = dir + '/' + file
        else:
            path = '/' + file

        factory.addFile(path, root + path)
