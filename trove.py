import os.path
import util

class Package:
    
    def addFile(self, file):
	self.files[file.path()] = file

    def write(self):
	pkgPath = self.dbpath + "/pkgs/" + self.name + self.version
	(dir, name) = os.path.split(pkgPath)
	util.mkdirChain(dir)

	f = open(pkgPath, "w")
	for file in self.files.values():
	    f.write("%s %s\n" % (file.path(), file.version()))
	f.close()

    def __init__(self, dbpath, name, version):
	self.files = {}
	self.name = name
	self.version = version
	self.dbpath = dbpath
