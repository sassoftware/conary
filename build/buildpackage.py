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
        
class BuildPackage(dict):

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

class Filter:
    """
    Determine whether a path meets a set of constraints.  FileFilter
    acts like a regular expression, except that besides matching
    the name, it can also test against file metadata.
    """
    def __init__(self, name, regex, macros, setmode=None, unsetmode=None):
	"""
	Provide information to match against.
	@param name: name of component
	@param regex: regular expression(s) to match against pathnames
	@type regex: string, list of strings, or compiled regular expression
	@param macros: current recipe macros
	@param setmode: bitmask containing bits that must be set
	for a match
	@type setmode: integer
	@param unsetmode: bitmask containing bits that must be unset
	for a match
	@type unsetmode: integer

	The setmode and unsetmode masks should be constructed from
	C{stat.S_IFDIR}, C{stat.S_IFCHR}, C{stat.S_IFBLK}, C{stat.S_IFREG},
	C{stat.S_IFIFO}, C{stat.S_IFLNK}, and C{stat.S_IFSOCK}
	Note that these are not simple bitfields.  To specify
	``no symlinks'' in unsetmask you need to provide
	C{stat.S_IFLNK^stat.S_IFREG}.
	To specify only character devices in setmask, you need
	C{stat.S_IFCHR^stat.SBLK}.
	Here are the binary bitmasks for the flags::
	    S_IFDIR  = 0100000000000000
	    S_IFCHR  = 0010000000000000
	    S_IFBLK  = 0110000000000000
	    S_IFREG  = 1000000000000000
	    S_IFIFO  = 0001000000000000
	    S_IFLNK  = 1010000000000000
	    S_IFSOCK = 1100000000000000
	"""
	self.name = name
	self.destdir = macros['destdir']
	self.setmode = setmode
	self.unsetmode = unsetmode
	tmplist = []
	if type(regex) is str:
	    regexp = regex
	    self.regexp = re.compile(regexp %macros)
	elif type(regex) in (tuple, list):
	    for subre in regex:
		if subre[:1] == '/':
		    subre = '^' + subre
		if subre[-1:] != '/':
		    subre = subre + '$'
		tmplist.append('(' + subre + ')')
	    regexp = string.join(tmplist, '|')
	    self.regexp = re.compile(regexp %macros)
	else:
	    self.regexp = regex

    def match(self, path):
	"""
	Compare a path to the constraints
	@param path: The string that should match the regex
	"""
	# search instead of match in order to not automatically
	# front-anchor searches
	match = self.regexp.search(path)
	if match:
	    if self.setmode or self.unsetmode:
		mode = os.lstat(self.destdir + os.sep + path)[stat.ST_MODE]
		if self.setmode is not None:
		    # if some bit in setmode is not set in mode, no match
		    if (self.setmode & mode) != self.setmode:
			return 0
		if self.unsetmode is not None:
		    # if some bit in unsetmode is set in mode, no match
		    if self.unsetmode & mode:
			return 0
	    return 1

	return 0

class AutoBuildPackage:
    """
    AutoBuildPackage creates a set of BuildPackage instances and
    provides facilities for automatically populating them with files
    according to Filters.
    """
    def __init__(self, namePrefix, version, pkgFilters, compFilters):
        """
	@param namePrefix: the package prefix, such as ":srs.specifixinc.com"
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
		name = self._getname(namePrefix, main.name, comp.name)
		if name not in self.packages:
		    package = BuildPackage(name, version)
		    self.packages[name] = package
		if main not in self.packageMap:
		    self.packageMap[main] = {}
		self.packageMap[main][comp] = self.packages[name]

    def _getname(self, prefix, pkgname, compname):
        return string.join((prefix, pkgname, compname), ':')

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

    def addDevice(self, path, devtype, major, minor,
                  owner='root', group='root', perms=0660):
        """
        Add a device to the correct BuildPackage instance by matching
        the file name against the package and component filters
        """
        pkg = self.findPackage(path)
        pkg.addDevice(path, devtype, major, minor, owner, group, perms)

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
