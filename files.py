import string
import os

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

    def infoLine(self):
	return FileMode.infoLine(self)

    def __init__(self, path, info = None):
	self.path(path)
	FileMode.__init__(self, info)

class RegularFile(File):

    def md5(self, md5 = None):
	if (md5 != None):
	    self.themd5 = md5

	return self.themd5

    def infoLine(self):
	return "f %s %s" % (self.themd5, File.infoLine(self))

    def __init__(self, path, info = None):
	if (info):
	    (type, self.themd5, info) = string.split(info, None, 2)
	else:
	    self.themd5 = None

	File.__init__(self, path, info)

class FileDB:

    def read(self):
	if (not os.path.exists(self.dbfile)):
	    return

	f = open(self.dbfile, "r")
	for line in f.readlines():
	    (version, rest) = string.split(line, None, 1)
	    self.versions[version] = RegularFile(self.path, rest)
	f.close()

    def add(self, version, file):
	if self.versions.has_key(version):
	    raise KeyError, "duplicate version for database"
	else:
	    if file.path() != self.path:
		raise KeyError, "path mismatch for file database"
	
	self.versions[version] = file

    def write(self):
	f = open(self.dbfile, "w")
	for (version, file) in self.versions.items():
	    f.write("%s %s\n" % (version, file.infoLine()))
	f.close()

    def __init__(self, dbpath, path):
	self.versions = {}
	self.dbpath = dbpath
	self.path = path
	self.dbfile = dbpath + '/files' + path + '.info'
	self.read()
