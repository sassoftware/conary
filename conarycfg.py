#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import os
import string

class SrsConfiguration:

    def read(self, file):
	if os.path.exists(file):
	    f = open(file, "r")
	    for line in f.readlines():
		(key, val) = string.split(line)
		if not self.__dict__.has_key(key):
		    raise KeyError, "configuration value %s unknown" % key

		self.__dict__[key] = val
	    f.close()

    def display(self):
	keys = self.__dict__.keys()
	keys.sort()
	for item in keys:
	    if type(self.__dict__[item]) == type ("a"):
		print "%-15s %s" % (item, self.__dict__[item])

    def __init__(self):
	self.reppath = "/var/lib/srsrep"
	self.root = "/"
	self.sourcepath = "/usr/src/srs/sources"
	self.buildpath = "/usr/src/srs/builds"

	self.read("/etc/srsrc")
	self.read(os.environ["HOME"] + "/" + ".srsrc")
	
