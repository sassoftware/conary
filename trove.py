import os.path
import versioned
import string

class Package:
    
    def addFile(self, path, version):
	self.files[path] = version

    def write(self):
	f = versioned.open(self.pkgPath, "r+")
	if f.hasVersion(self.version):
	    f.setVersion(self.version)
	else:
	    f.createVersion(self.version)

	for (file, version) in self.files.items():
	    f.write("%s %s\n" % (file, version))

	f.close()

    def read(self):
	if os.path.exists(self.pkgPath):
	    # this creates the file if it doesn't exist so write()
	    # knows it does exist
	    f = versioned.open(self.pkgPath, "r+")
	    if f.hasVersion(self.version):
		for line in f.readLines():
		    (path, version) = string.split(line)
		    self.addFile(path, version)

	    f.close()

    def __init__(self, dbpath, name, version):
	self.files = {}
	self.name = name
	self.version = version
	self.dbpath = dbpath
	self.pkgPath = self.dbpath + "/pkgs/" + self.name
	self.read()
