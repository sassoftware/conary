#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed with the whole that it will be usefull, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

"""
Contains classes used during the build process to collect files
into BuildPackages.  These BuildPackages are used to create Packages
and create changesets from the files created during the build process
"""

import string
import os
import files
import time
import use

from deps import deps

def BuildDeviceFile(devtype, major, minor, owner, group, perms):
    if devtype == "b":
	f = files.BlockDevice(None)
    elif devtype == "c":
	f = files.CharacterDevice(None)
    else:
	raise AssertionError

    f.devt.setMajor(major)
    f.devt.setMinor(minor)
    f.inode.setOwner(owner)
    f.inode.setGroup(group)
    f.inode.setPerms(perms)
    f.inode.setMtime(time.time())
    f.flags.set(0)

    return f

def _getUseDependencySet():
    """
    Returns a deps.DependencySet instance that represents the Use flags
    that have been used.
    """
    set = deps.DependencySet()
    flags = use.Use.getUsed()
    depFlags = []
    names = flags.keys()
    if names:
        names.sort()
        for name in names:
            val = flags[name]
            if val:
                depFlags.append(name)
            else:
                depFlags.append('!' + name)
        dep = deps.Dependency('use', depFlags)
        set.addDep(deps.UseDependency, dep)
    return set
    
class BuildPackage(dict):

    def addFile(self, path, realPath):
        """
        Add a file to the build package

        @param path: the destination of the file in the package
        @param realPath: the location of the actual file on the filesystem,
        used to obtain the contents of the file when creating a changeset
        to commit to the repository
        """
	f = files.FileFromFilesystem(realPath, None, buildDeps = True)
        if f.hasContents:
            self.requires.union(f.requires.value())
            self.provides.union(f.provides.value())
            self.flavor.union(f.flavor.value())
        
	f.inode.setPerms(f.inode.perms() & 01777)
	self[path] = (realPath, f)

    def addDevice(self, path, devtype, major, minor,
                  owner='root', group='root', perms=0660):
        """
        Add a device node to the build package

        @param path: the destination of the device node in the package
        """
        f = BuildDeviceFile(devtype, major, minor, owner, group, perms)
	self[path] = (None, f)

    def getFile(self, path):
        return self[path][1]

    def getRealPath(self, path):
        return self[path][0]

    def getName(self):
        """
        Return the name of the BuildPackage

        @returns: name of the BuildPackag
        @rtype: str
        """
	return self.name

    def __init__(self, name):
	self.name = name
        self.requires = deps.DependencySet()
        self.provides = deps.DependencySet()
        self.flavor = _getUseDependencySet()
	dict.__init__(self)


class AutoBuildPackage:
    """
    AutoBuildPackage creates a set of BuildPackage instances and
    provides facilities for automatically populating them with files
    according to Filters.
    """
    def __init__(self, pkgFilters, compFilters):
        """
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
		    self.packages[name] = BuildPackage(name)
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
	self.pathMap[path] = pkg.getFile(path)
	self.pkgMap[path] = pkg

    def addDevice(self, path, devtype, major, minor,
                  owner='root', group='root', perms=0660):
        """
        Add a device to the correct BuildPackage instance by matching
        the file name against the package and component filters
        """
        pkg = self.findPackage(path)
        pkg.addDevice(path, devtype, major, minor, owner, group, perms)
	self.pathMap[path] = pkg.getFile(path)
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
        os.path.walk(root, self._autoVisit, root)

    def _autoVisit(self, root, dir, files):
        """
        Helper function called by os.path.walk() when AutoBuildPackage.walk()
        is called
        """
        dir = dir[len(root):]

        for file in files:
            if dir:
                path = dir + '/' + file
            else:
                path = '/' + file

            self.addFile(path, root + path)
