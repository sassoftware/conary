import os
import versioned
import string
import types
import util

# this is the repositories idea of a package
class Package:

    def addFile(self, path, version):
	self.files["/files" + path] = version

    def addSource(self, path, version):
	self.files["/sources" + path] = version

    def fileList(self):
	l = []
	# rip off the /files prefix
	for (path, file) in self.files.items():
	    if path[:6] == "/files":
		l.append((path[6:], file))

	return l

    def sourceList(self):
	l = []
	# rip off the /files prefix
	for (path, file) in self.files.items():
	    if path[:8] == "/sources":
		l.append((path[8:], file))

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
	    if path[:8] == "/sources":
		self.addSource(path[8:], version)
	    else:
		self.addFile(path[6:], version)

    def __init__(self, name, dataFile):
	Package.__init__(self, name)
	self.read(dataFile)

# this is a set of all of the versions of a single packages 
class PackageSet:
    def write(self):
	util.mkdirChain(os.path.split(self.pkgPath)[0])
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

	f = versioned.open(self.pkgPath, "r")
	versions = f.versionList()
	self.packages = {}
	for version in versions:
	    f.setVersion(version)
	    self.packages[version] = PackageFromFile(name, f)

	f.close()

#----------------------------------------------------------------------------

# this is the build system's idea of a package. maybe they'll merge. someday.

class BuildFile:

    def configFile(self):
	self.isConfigFile = 1

    def __init__(self):
	self.isConfigFile = 0

class BuildPackage(types.DictionaryType):

    def addFile(self, path):
	self[path] = BuildFile()

    def addDirectory(self, path):
	self[path] = BuildFile()

    def __init__(self, name):
	self.name = name
	types.DictionaryType.__init__(self)

class BuildPackageSet:

    def addPackage(self, pkg):
	self.__dict__[pkg.name] = pkg
	self.pkgs[pkg.name] = pkg

    def packageSet(self):
	return self.pkgs.items()

    def __init__(self, name):
	self.name = name
	self.pkgs = {}

def Auto(name, root):
    runtime = BuildPackage("runtime")
    mans = BuildPackage("man")
    os.path.walk(root, autoVisit, (root, runtime, mans))

    set = BuildPackageSet(name)
    set.addPackage(runtime)
    if mans.keys():
	set.addPackage(mans)
    
    return set

def autoVisit(arg, dir, files):
    (root, pkg, man) = arg

    for file in files:
	path = dir[len(root):] + "/" + file
	if not os.path.isdir(path):
	    if path[:15] == "/usr/share/man/":
		man.addFile(path)
	    else:
		pkg.addFile(path)
