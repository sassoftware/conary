#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import deps
import deps.arch
import deps.deps
import os
import versions
import sys

class SrsConfiguration:

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
	(key, val) = line.split()
	if not self.__dict__.has_key(key):
	    raise ParseError, ("configuration value %s unknown" % key)

	self.__dict__[key] = val

	try:
	    if key == "installlabel":
		self.installlabel = versions.BranchName(self.installlabel)
	    elif key == "buildlabel":
		self.buildlabel = versions.BranchName(self.buildlabel)
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
	self.reppath = "/var/lib/srsrep"
	self.root = "/"
	self.sourcepath = "/usr/src/srs/sources"
	self.buildpath = "/usr/src/srs/builds"
	self.installlabel = None
	self.buildlabel = None
	self.lookaside = "/var/cache/srs"
	self.dbpath = "/var/lib/srsdb"
        self.tmpdir = "/var/tmp/"
	self.instructionSet = deps.arch.current()

	self.flavor = deps.deps.DependencySet()
	self.flavor.addDep(deps.deps.InstructionSetDependency, 
			   self.instructionSet)

	self.read("/etc/srsrc")
	self.read(os.environ["HOME"] + "/" + ".srsrc")

class SrsCfgError(Exception):

    """
    Ancestor for all exceptions raised by the srscfg module.
    """

    pass

class ParseError(SrsCfgError):

    """
    Indicates that an error occured parsing the config file.
    """

    def __str__(self):
	return self.str

    def __init__(self, str):
	self.str = str
