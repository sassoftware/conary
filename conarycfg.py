#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
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
	    raise KeyError, ("configuration value %s unknown" % key)

	self.__dict__[key] = val

	if key == "defaultbranch":
	    self.defaultbranch = versions.VersionFromString(self.defaultbranch)

	    if self.defaultbranch.isVersion():
		sys.stderr.write("The configured default branch %s specifies " +
		     "version, not a branch.\n" % self.defaultbranch.asString())
	elif key == "installbranch":
	    self.installbranch = versions.BranchName(self.installbranch)

    def display(self):
	keys = self.__dict__.keys()
	keys.sort()
	for item in keys:
	    if type(self.__dict__[item]) is str:
		print "%-20s %s" % (item, self.__dict__[item])
	    elif isinstance(self.__dict__[item], versions.Version):
		print "%-20s %s" % (item, self.__dict__[item].asString())
	    elif isinstance(self.__dict__[item], versions.BranchName):
		print "%-20s %s" % (item, self.__dict__[item])
	    else:
		print "%-20s (unknown type)" % (item)

    def __init__(self):
	self.reppath = "/var/lib/srsrep"
	self.root = "/"
	self.sourcepath = "/usr/src/srs/sources"
	self.buildpath = "/usr/src/srs/builds"
	self.packagenamespace = ":localhost"
	self.defaultbranch = None
	self.installbranch = None
	self.lookaside = "/var/cache/srs"
	self.dbpath = "/var/lib/srsdb"
        self.tmpdir = "/var/tmp/"
	self.defaultbranch = versions.VersionFromString("/localhost@local")

	self.read("/etc/srsrc")
	self.read(os.environ["HOME"] + "/" + ".srsrc")
