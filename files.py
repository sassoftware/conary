import string
import os
import versioned
import md5sum
import pwd
import grp
import shutil
import util
import stat

class FileMode:

    # parses "rwx" style string
    def parseTriplet(self, str, shleft, setval):
	i = 0
	add = 0
	if str[0] == "r":
	    i = 4
	if str[1] == "w":
	    i = i + 2
	if str[2] == "x":
	    i = i + 1
	if str[2] == "s" or str[2] == "t":
	    i = i + 1
	    add = setval
	if str[2] == "S" or str[2] == "T":
	    add = setval

	return add + (i << shleft)

    # new can be an integer file mode or a string (ls style) listing
    def perms(self, new = None):
	if (new != None):
	    if type(new) == type(1):
		self.thePerms = new
	    elif type(new) == type("a"):
		a = self.parseTriplet(new[1:4], 6, 04000)
		b = self.parseTriplet(new[4:7], 3, 02000)
		c = self.parseTriplet(new[7:10], 0, 01000)
		self.thePerms = a + b + c

	return self.thePerms

    def owner(self, new = None):
	if (new != None):
	    self.theOwner = new

	return self.theOwner

    def group(self, new = None):
	if (new != None):
	    self.theGroup = new

	return self.theGroup

    def mtime(self, new = None):
	if (new != None):
	    self.theMtime = new

	return self.theMtime

    # parses ls style output, returns the filename
    def parsels(self, line):
	info = string.split(line)

	self.perms(info[0])
	self.owner(info[2])
	self.group(info[3])

	date = info[5:7]

	return info[8]

    def infoLine(self):
	return "%o %s %s %s" % (self.thePerms, self.theOwner, self.theGroup,
				self.theMtime)

    def compare(self, other):
	if self.thePerms == other.thePerms and \
	   self.theOwner == other.theOwner and \
	   self.theGroup == other.theGroup:
	    return 1

	return 0

    def __init__(self, info = None):
	if info:
	    (self.thePerms, self.theOwner, self.theGroup, self.theMtime) = \
		string.split(info)
	    self.thePerms = int(self.thePerms, 8)
	else:
	    self.thePerms = None
	    self.theOwner = None
	    self.theGroup = None
	    self.theMtime = None
	
class File(FileMode):
    def path(self, new = None):
	if (new != None):
	    l = os.path.split(new)
	    self.dir(l[0])
	    self.name(l[1])
	
	return self.dir() + '/' + self.name()

    def name(self, new = None):
	if (new != None):
	    self.theName = new

	return self.theName

    def dir(self, new = None):
	if (new != None):
	    self.theDir = new

	return self.theDir

    def version(self, new = None):
	if (new != None):
	    self.theVersion = new

	return self.theVersion

    def uniqueName(self):
	return md5sum.md5str(self.theVersion)

    def infoLine(self):
	return FileMode.infoLine(self)

    def copy(self):
	raise "method should be provided by derivative classes"

    def chmod(self, path):
	os.chmod(path, self.thePerms)

    def __init__(self, path, newVersion = None, info = None):
	self.path(path)
	self.theVersion = newVersion
	FileMode.__init__(self, info)

class SymbolicLink(File):

    def linkTarget(self, newLinkTarget = None):
	if (newLinkTarget):
	    self.theLinkTarget = newLinkTarget

	return self.theLinkTarget

    def infoLine(self):
	return "l %s %s" % (self.theLinkTarget, File.infoLine(self))

    def compare(self, other):
	if self.theLinkTarget == other.theLinkTarget:
	    # recursing does a permission check, which doens't apply 
	    # to symlinks under Linux
	    return 1

	return 0

    def chmod(self, path):
	# chmod() on a symlink follows the symlink
	pass

    def copy(self, source, target):
	os.symlink(self.theLinkTarget, target)

    def __init__(self, path, version = None, info = None):
	if (info):
	    (self.theLinkTarget, info) = string.split(info, None, 1)
	else:
	    self.theLinkTarget = None

	File.__init__(self, path, version, info)

class Directory(File):

    def infoLine(self):
	return "d %s" % (File.infoLine(self))

    def compare(self, other):
	return File.compare(self, other)

    def copy(self, source, target):
	if not os.path.exists(target):
	    os.mkdir(target)

    def __init__(self, path, version = None, info = None):
	File.__init__(self, path, version, info)

class RegularFile(File):

    def md5(self, md5 = None):
	if (md5 != None):
	    self.themd5 = md5

	return self.themd5

    def infoLine(self):
	return "f %s %s" % (self.themd5, File.infoLine(self))

    def compare(self, other):
	if self.themd5 == other.themd5:
	    return File.compare(self, other)

	return 0

    def copy(self, source, target):
	shutil.copyfile(source, target)

    def __init__(self, path, version = None, info = None):
	if (info):
	    (self.themd5, info) = string.split(info, None, 1)
	else:
	    self.themd5 = None

	File.__init__(self, path, version, info)

class FileDB:

    def read(self):
	f = versioned.open(self.dbfile, "r")
	self.versions = {}

	for version in f.versionList():
	    f.setVersion(version)
	    line = f.read()
	    self.versions[version] = FileFromInfoLine(self.path, version, line)

	f.close()

    def findVersion(self, file):
	for (v, f) in self.versions.items():
	    if type(f) == type(file) and f.compare(file):
		return (v, f)

	return None

    def addVersion(self, version, file):
	if self.versions.has_key(version):
	    raise KeyError, "duplicate version for database"
	else:
	    if file.path() != self.path:
		raise KeyError, "path mismatch for file database"
	
	self.versions[version] = file

    def getVersion(self, version):
	return self.versions[version]

    def write(self):
	dir = os.path.split(self.dbfile)[0]
	util.mkdirChain(dir)

	f = versioned.open(self.dbfile, "w")
	for (version, file) in self.versions.items():
	    f.createVersion(version)
	    f.write("%s\n" % file.infoLine())

	f.close()

    def __init__(self, dbpath, path):
	self.dbpath = dbpath
	self.path = path
	self.dbfile = dbpath + '/files' + path + '.info'
	self.read()

def FileFromFilesystem(root, path):
    (perms, inode, dev, links, uid, gid, size, atime, mtime, ctime) = \
	os.lstat(root + path)

    if (stat.S_ISREG(perms)):
	f = RegularFile(path)
	f.perms(perms)
	f.md5(md5sum.md5sum(root + path))
    elif (stat.S_ISLNK(perms)):
	f = SymbolicLink(path)
	f.perms(0777)
	f.linkTarget(os.readlink(root + path))
    elif (stat.S_ISDIR(perms)):
	f = Directory(path)
	f.perms(perms & 07777)
    else:
	raise TypeError, "unsupported file type for %s" % path

    f.owner(pwd.getpwuid(uid)[0])
    f.group(grp.getgrgid(gid)[0])
    f.mtime(mtime)

    return f

def FileFromInfoLine(path, version, infoLine):
    (type, infoLine) = string.split(infoLine, None, 1)
    if type == "f":
	return RegularFile(path, version, infoLine)
    elif type == "l":
	return SymbolicLink(path, version, infoLine)
    elif type == "d":
	return Directory(path, version, infoLine)
    else:
	raise KeyError, "bad infoLine %s" % infoLine
