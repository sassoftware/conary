#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Contains classes used during the build process to collect files
into BuildPackages.  These BuildPackages are used to create Packages
and create changesets from the files created during the build process
"""

import stat
import string
import re
import os
import files

class BuildFile(files.File):
    def getRealPath(self):
        return self.realPath

    def __init__(self, realPath):
        files.File.__init__(self, None)
        self.realPath = realPath

class _BuildDeviceFile(files.DeviceFile, BuildFile):
    def __init__(self, major, minor, owner, group, perms):
        BuildFile.__init__(self, None)

	self.devt.setMajor(major)
	self.devt.setMinor(minor)
	self.inode.setOwner(owner)
	self.inode.setGroup(group)
	self.inode.setPerms(perms)
	self.inode.setMtime(0)
	self.flags.set(0)

class BuildBlockDeviceFile(_BuildDeviceFile):

    lsTag = "b"
        
class BuildCharacterDeviceFile(_BuildDeviceFile):

    lsTag = "c"

def BuildDeviceFile(devtype, major, minor, owner, group, perms):
    if devtype == "b":
	return BuildBlockDeviceFile(major, minor, owner, group, perms)
    elif devtype == "c":
	return BuildCharacterDeviceFile(major, minor, owner, group, perms)
    raise AssertionError

class BuildPackage(dict):

    def addFile(self, path, realPath):
        """
        Add a file to the build package

        @param path: the destination of the file in the package
        @param realPath: the location of the actual file on the filesystem,
        used to obtain the contents of the file when creating a changeset
        to commit to the repository
        """
	self[path] = BuildFile(realPath)

    def addDevice(self, path, devtype, major, minor,
                  owner='root', group='root', perms=0660):
        """
        Add a device node to the build package

        @param path: the destination of the device node in the package
        """
	self[path] = BuildDeviceFile(devtype, major, minor, owner, group, perms)

    def getName(self):
        """
        Return the name of the BuildPackage

        @returns: name of the BuildPackag
        @rtype: str
        """
	return self.name

    def getVersion(self):
        """
        Return the version of the BuildPackage

        @returns: name of the BuildPackag
        @rtype: versions.Version instance
        """
	return self.version

    def __init__(self, name, version):
	self.name = name
	self.version = version
	dict.__init__(self)


class AutoBuildPackage:
    """
    AutoBuildPackage creates a set of BuildPackage instances and
    provides facilities for automatically populating them with files
    according to Filters.
    """
    def __init__(self, version, pkgFilters, compFilters):
        """
	@param version: the version of each package
        @type version: versions.Version instance
	@param pkgFilters: Filters used to add files to main packages
	@type pkgFilters: sequence of Filter instances
	@param compFilters: Filters used to add files to components
	@type compFilters: sequence of Filter instances
	"""
	self.pkgFilters = pkgFilters
        self.compFilters = compFilters
        # dictionary of all the build packages
        self.packages = {}
        # reverse map from the package:component combination to
        # the correct build package
	self.packageMap = {}
	for main in self.pkgFilters:
	    for comp in self.compFilters:
		name = self._getname(main.name, comp.name)
		if name not in self.packages:
		    package = BuildPackage(name, version)
		    self.packages[name] = package
		if main not in self.packageMap:
		    self.packageMap[main] = {}
		self.packageMap[main][comp] = self.packages[name]
	# dictionary from pathnames to fileobjects
	self.pathMap = {}
	# dictionary from pathnames to packages
	self.pkgMap = {}

    def _getname(self, pkgname, compname):
        return string.join((pkgname, compname), ':')

    def findPackage(self, path):
        """Return the BuildPackage that matches the path"""
	for main in self.pkgFilters:
	    if main.match(path):
		for comp in self.compFilters:
		    if comp.match(path):
			return self.packageMap[main][comp]
        return None
    
    def addFile(self, path, realPath):
        """
        Add a path to the correct BuildPackage instance by matching
        the file name against the package and component filters

        @param path: path to add to the BuildPackage
        @type path: str
        @rtype: None
        """
        pkg = self.findPackage(path)
        pkg.addFile(path, realPath)
	self.pathMap[path] = pkg[path]
	self.pkgMap[path] = pkg

    def addDevice(self, path, devtype, major, minor,
                  owner='root', group='root', perms=0660):
        """
        Add a device to the correct BuildPackage instance by matching
        the file name against the package and component filters
        """
        pkg = self.findPackage(path)
        pkg.addDevice(path, devtype, major, minor, owner, group, perms)
	self.pathMap[path] = pkg[path]
	self.pkgMap[path] = pkg

    def getPackages(self):
        """
        Examine the BuildPackage instances that have been created and
        return a list that includes only those which have files
        
        @return: list of BuildPackages instances
        @rtype: list
        """
        l = []
        for name in self.packages.keys():
            if self.packages[name].keys():
                l.append(self.packages[name])
        return l
            
    def walk(self, root):
        """
        Traverse the directory tree specified by @C{root}, adding file
        entries to the BuildPackages

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
