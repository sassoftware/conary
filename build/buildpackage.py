#
# Copyright (c) 2004-2005 Specifix, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

"""
Contains classes used during the build process to collect files
into BuildPackages.  These BuildPackages are used to create Packages
and create changesets from the files created during the build process
"""

import files
import lib.elf
from lib import log
import os
import string
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

def _getUseDependencySet(recipe):
    """
    Returns a deps.DependencySet instance that represents the Use flags
    that have been used.
    """
    return use.createFlavor(recipe.name, use.Use._iterUsed(), 
                                         recipe.Flags._iterUsed(), 
                                         use.Arch._iterUsed())
    
class BuildPackage(dict):

    def addFile(self, path, realPath):
        """
        Add a file to the build package

        @param path: the destination of the file in the package
        @param realPath: the location of the actual file on the filesystem,
        used to obtain the contents of the file when creating a changeset
        to commit to the repository
        """
	(f, linkCount, inode) = files.FileFromFilesystem(realPath, None, 
                                        inodeInfo = True)
	f.inode.setPerms(f.inode.perms() & 01777)
	self[path] = (realPath, f)

        if f.hasContents and isinstance(f, files.RegularFile):
            results = lib.elf.inspect(realPath)
            if results != None:
                requires, provides = results
                abi = None
                for depClass, main, flags in requires:
                    if depClass == 'abi':
                        abi = (main, flags)
                        self.isnsetMap[path] = flags[1]
                        break

                self.requiresMap[path] = self.getDepsFromElf(requires, abi)
                self.providesMap[path] = self.getDepsFromElf(provides, abi)

        if linkCount > 1:
            if f.hasContents:
                l = self.linkGroups.get(inode, [])
                l.append(path)
                self.linkGroups[inode] = l
                # add to list to check for config files later
                self.hardlinks.append(path)
            else:
                if not isinstance(f, files.Directory):
                    # no hardlinks allowed for special files other than dirs
                    self.badhardlinks.append(path)

    def getDepsFromElf(self, elfinfo, abi):
        """
        Add dependencies from ELF information.

        @param elfinfo: List provided by C{lib.elf.inspect()}
        @param abi: tuple of abi information to blend into soname dependencies
        """
	set = deps.DependencySet()
	for (depClass, main, flags) in elfinfo:
            flags = [ (x, deps.FLAG_SENSE_REQUIRED) for x in flags ]
	    if depClass == 'soname':
                if '/' in main:
                    base = os.path.basename(main)
                    log.error(
                        'soname %s contains "/" character, truncating to %s',
                        main, base
                    )
                    main = base
                assert(abi)
                curClass = deps.SonameDependencies
                flags.extend((x, deps.FLAG_SENSE_REQUIRED) for x in abi[1])
                dep = deps.Dependency(abi[0] + "/" + main, flags)
	    elif depClass == 'abi':
		curClass = deps.AbiDependency
                dep = deps.Dependency(main, flags)
	    else:
		assert(0)

	    set.addDep(curClass, dep)
        return set

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

        @returns: name of the BuildPackage
        @rtype: str
        """
	return self.name

    def getUserMap(self):
        """
        Dict mapping user names to tuples of C{(preferred_uid, groupname,
        preferred_groupid, homedir, comment, shell)}
        """
        return self.recipe.usermap

    def getUserGroupMap(self):
        """
        Reverse map from group name to user name for groups created as part
        of a user definition.
        """
        return self.recipe.usergrpmap

    def getGroupMap(self):
        """
        Dict mapping group names to preferred_groupid
        """
        return self.recipe.groupmap

    def getSuppGroupMap(self):
        """
        Dict mapping user names to C{(group, preferred_groupid)} tuples
        """
        return self.recipe.suppmap

    def __init__(self, name, recipe):
	self.name = name
        self.requires = deps.DependencySet()
        self.provides = deps.DependencySet()
        self.flavor = _getUseDependencySet(recipe)
        self.linkGroups = {}
        self.requiresMap = {}
        self.providesMap = {}
        self.isnsetMap = {}
        self.hardlinks = []
        self.badhardlinks = []
        self.recipe = recipe
	dict.__init__(self)


class AutoBuildPackage:
    """
    AutoBuildPackage creates a set of BuildPackage instances and
    provides facilities for automatically populating them with files
    according to Filters.
    """
    def __init__(self, pkgFilters, compFilters, recipe):
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
		    self.packages[name] = BuildPackage(name, recipe)
		if main not in self.packageMap:
		    self.packageMap[main] = {}
		self.packageMap[main][comp] = self.packages[name]
	# dictionary from pathnames to fileobjects
	self.pathMap = {}
	# dictionary from pathnames to packages
	self.pkgMap = {}

    def _getname(self, pkgname, compname):
        return string.join((pkgname, compname), ':')

    def _findPackage(self, path):
        """
	Return the BuildPackage that matches the path.
	Should be called only once per path to add an entry to self.pkgMap
	"""
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
        pkg = self._findPackage(path)
        pkg.addFile(path, realPath)
	self.pathMap[path] = pkg.getFile(path)
	self.pkgMap[path] = pkg

    def delFile(self, path):
        """
	Remove a file from the package and from the caches.

        @param path: path to remove from the BuildPackage
        @type path: str
        @rtype: None
        """
	del self.pkgMap[path][path]
	del self.pkgMap[path]
	del self.pathMap[path]

    def addDevice(self, path, devtype, major, minor,
                  owner='root', group='root', perms=0660):
        """
        Add a device to the correct BuildPackage instance by matching
        the file name against the package and component filters
        """
        pkg = self._findPackage(path)
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
