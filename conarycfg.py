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

    def __init__(self):
	self.reppath = "/var/lib/srsrep"
	self.root = "/"
	self.sourcepath = "/usr/src/srs"

	self.read(os.environ["HOME"] + "/" + ".srsrc")
	self.read("/etc/srsrc")
	
