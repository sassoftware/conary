#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import os
import string
import types
import versions
import sys

class SrsConfiguration:

    def read(self, file):
	if os.path.exists(file):
	    f = open(file, "r")
	    for line in f.readlines():
		self.configLine(line)
	    f.close()

    def configLine(self, line):
	(key, val) = string.split(line)
	if not self.__dict__.has_key(key):
	    raise KeyError, ("configuration value %s unknown" % key)

	self.__dict__[key] = val

    def display(self):
	keys = self.__dict__.keys()
	keys.sort()
	for item in keys:
	    if type(self.__dict__[item]) == types.StringType:
		print "%-20s %s" % (item, self.__dict__[item])
	    elif self.__dict__[item].__class__ == versions.Version:
		print "%-20s %s" % (item, self.__dict__[item].asString())

    def __init__(self):
	self.reppath = "/var/lib/srsrep"
	self.root = "/"
	self.sourcepath = "/usr/src/srs/sources"
	self.buildpath = "/usr/src/srs/builds"
	self.packagenamespace = ":localhost"
	self.defaultbranch = None
	self.lookaside = "/var/cache/srs"
	self.dbpath = "/var/lib/srsdb"

	self.read("/etc/srsrc")
	self.read(os.environ["HOME"] + "/" + ".srsrc")

	if self.defaultbranch:
	    self.defaultbranch = versions.VersionFromString(self.defaultbranch)
	else:
	    self.defaultbranch = versions.VersionFromString("/localhost@local")

	if self.defaultbranch.isVersion():
	    sys.stderr.write("The configured default branch %s specifies " +
		 "version, not a branch.\n" % self.defaultbranch.asString())
