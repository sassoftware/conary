#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import deps
import deps.arch
import deps.deps
import os
import versions

class ConaryConfiguration:

    def read(self, file):
	if os.path.exists(file):
	    f = open(file, "r")
	    for line in f:
		self.configLine(line)
	    f.close()

    def configLine(self, line):
	line = line.strip()
	if not line or line[0] == '#':
	    return
	(key, val) = line.split(None, 1)
        key = key.lower()
	if not self.lowerCaseMap.has_key(key):
	    raise ParseError, ("configuration value %s unknown" % key)

	self.__dict__[self.lowerCaseMap[key]] = val

	try:
	    if key.lower() == "installlabel":
		self.installLabel = versions.BranchName(self.installLabel)
	    elif key.lower() == "buildlabel":
		self.buildLabel = versions.BranchName(self.buildLabel)
	except versions.ParseError, e:
	    raise ParseError, str(e)

    def display(self):
	keys = self.__dict__.keys()
	keys.sort()
	for item in keys:
	    if type(self.__dict__[item]) is str:
		print "%-20s %s" % (item, self.__dict__[item])
	    elif isinstance(self.__dict__[item], versions.Version):
		print "%-20s %s" % (item, self.__dict__[item].asString())
	    elif isinstance(self.__dict__[item], versions.BranchName):
		print "%-20s %s" % (item, self.__dict__[item].asString())
	    elif isinstance(self.__dict__[item], deps.deps.Dependency):
		print "%-20s %s" % (item, self.__dict__[item])
	    elif item == "flavor":
		pass
	    else:
		print "%-20s (unknown type)" % (item)

    def __init__(self):
	self.repPath = "/var/lib/conary-rep"
	self.root = "/"
	self.sourcePath = "/usr/src/conary/sources"
	self.buildPath = "/usr/src/conary/builds"
	self.installLabel = None
	self.buildLabel = None
	self.lookaside = "/var/cache/conary"
	self.dbPath = "/var/lib/conarydb"
        self.tmpDir = "/var/tmp/"
	self.name = None
	self.contact = None
	self.instructionSet = deps.arch.current()

        self.lowerCaseMap = {}
        for (key, value) in self.__dict__.items():
            self.lowerCaseMap[key.lower()] = key

	self.flavor = deps.deps.DependencySet()
	self.flavor.addDep(deps.deps.InstructionSetDependency, 
			   self.instructionSet)

	self.read("/etc/conaryrc")
	if os.environ.has_key("HOME"):
	    self.read(os.environ["HOME"] + "/" + ".conaryrc")

class ConaryCfgError(Exception):

    """
    Ancestor for all exceptions raised by the conarycfg module.
    """

    pass

class ParseError(ConaryCfgError):

    """
    Indicates that an error occured parsing the config file.
    """

    def __str__(self):
	return self.str

    def __init__(self, str):
	self.str = str
