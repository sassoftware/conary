import os.path
import versioned
import string

# this is a single version of a single package
class Package:

    def addFile(self, path, version):
	self.files["/files" + path] = version

    def fileList(self):
	l = []
	# rip off the /files prefix
	for (path, file) in self.files.items():
	    l.append((path[6:], file))

	return l

    def write(self, dataFile):
	for (file, version) in self.files.items():
	    dataFile.write("%s %s\n" % (file, version))

    def __init__(self, name):
	self.files = {}
	self.name = name

class PackageFromFile(Package):

    def read(self, dataFile):
	for line in dataFile.readLines():
	    (path, version) = string.split(line)
	    # remove the "files/" bit
	    self.addFile(path[6:], version)

    def __init__(self, name, dataFile):
	Package.__init__(self, name)
	self.read(dataFile)

# this is a set of all of the versions of a single packages 
class PackageSet:
    def write(self):
	f = versioned.open(self.pkgPath, "w")
	for (version, package) in self.packages.items():
	    f.createVersion(version)
	    package.write(f)
	f.close()

    def getVersion(self, version):
	return self.packages[version]

    def hasVersion(self, version):
	return self.packages.has_key(version)

    def createVersion(self, version):
	self.packages[version] = Package(version)
	return self.packages[version]

    def versionList(self):
	return self.packages.keys()

    def getLatest(self):
	v = versioned.latest(self.packages.keys())
	return (v, self.packages[v])
	
    def __init__(self, reppath, name):
	self.name = name
	self.pkgPath = reppath + "/pkgs/" + self.name

	f = versioned.open(self.pkgPath, "r+")
	versions = f.versionList()
	self.packages = {}
	for version in versions:
	    f.setVersion(version)
	    self.packages[version] = PackageFromFile(name, f)

	f.close()
