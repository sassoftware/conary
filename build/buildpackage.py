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

    def configFile(self):
	self.isConfigFile = 1

    def __init__(self):
	self.isConfigFile = 0

class BuildPackage(types.DictionaryType):

    def addFile(self, path):
	self[path] = BuildFile()

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

class BuildPackageGenerator:
    """BuildPackageGenerator takes a set of PackageSpec lists
    and provides facilities for populating new BuildPackage instances
    with files according to the PackageSpecs.
    """    
    def __init__(self, namePrefix, version, auto, explicit):
        """
	@param namePrefix: the fully qualified name of the main package
	such as ":srs.specifixinc.com:tmpwatch"
	@param version: a versionObject specifying the version of the
	package, which is used as the version of each subpackage
	@param auto: automatic subpackage list
	@type auto: sequence of PackageSpec instances
	regex)) tuples
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
		if not self.packageMap.has_key(explicitspec.name):
		    self.packageMap[explicitspec.name] = {}
		self.packageMap[explicitspec.name][autospec.name] = package

    def _getname(self, prefix, subname, autoname):
        """Returns the full name of the package when subname could be None"""
	if subname:
	    return string.join((prefix, subname, autoname), ':')
	else:
	    return string.join((prefix, autoname), ':')
    
    def addPath(self, path):
        """addPath takes a pathname and adds it to the correct BuildPackage
        instance given the explicit/auto spec matches
        @param path: path to add to the BuildPackage
        @type path: str
        @rtype: None
        """
	for explicitspec in self.explicit:
	    if explicitspec.match(path):
		for autospec in self.auto:
		    if autospec.match(path):
			pkg = self.packageMap[explicitspec.name][autospec.name]
                        pkg.addFile(path)
			break
		break

    def packageSet(self):
        """packageSet examines the packages created by the generator and
        only returns those which have files in them
        @return: list of BuildPackages instances
        @rtype: list
        """
        set = BuildPackageSet()
        for name in self.packages.keys():
            if self.packages[name].keys():
                set.addPackage(self.packages[name])
        return set
            
    def walk(self, root):
        """traverse the directory tree specified by @C{root}, adding entries
        to the BuildPackages as we go
        @param root: root of path to walk
        @type root: str
        @rtype: None
        """
        os.path.walk(root, _autoVisit, (root, self))

def _autoVisit(arg, dir, files):
    """Helper function called by os.path.walk() when
    BuildPackageGenerator.walk() is called"""
    (root, generator) = arg
    dir = dir[len(root):]

    for file in files:
        if dir:
            path = dir + '/' + file
        else:
            path = '/' + file

        generator.addPath(path)
