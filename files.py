#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import string
import os
import versioned
import sha1helper
import pwd
import grp
import shutil
import stat
import pwd
import grp
import util
import types
import zipfile
import datastore

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
	    if type(new) == types.IntType:
		self.thePerms = new
	    elif type(new) == types.StringType:
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

    def same(self, other):
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

    # path to the file in the repository
    def pathInRep(self, reppath):
	return reppath + "/files" + self.path()

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
	return sha1helper.hashString(self.path)

    def infoLine(self):
	return FileMode.infoLine(self)

    def restore(self, reppath, root):
	self.chmod(root)

    def chmod(self, root):
	os.chmod(root + self.path(), self.thePerms)

    def setOwnerGroup(self, root):
	if os.getuid(): return

	# root should set the file ownerships properly
	uid = pwd.getpwnam(f.owner())[2]
	gid = grp.getgrnam(f.group())[2]

	# FIXME: this needs to use lchown, which is in 2.3, and
	# this should happen unconditionally
	os.chown(root + self.path(), uid, gid)

    # copies a files contents into the repository, if necessary
    def archive(self, reppath, root):
	# most file types don't need to do this
	pass

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

    def same(self, other):
	if self.theLinkTarget == other.theLinkTarget:
	    # recursing does a permission check, which doens't apply 
	    # to symlinks under Linux
	    return 1

	return 0

    def chmod(self, root, path):
	# chmod() on a symlink follows the symlink
	pass

    def setOwnerGroup(self, root, path):
	# chmod() on a symlink follows the symlink
	pass

    def restore(self, reppath, srcpath, root):
	target = root + self.path()
	os.symlink(self.theLinkTarget, target)

	# this doesn't actually do anything for a symlink
	File.restore(self, reppath, root)

    def __init__(self, path, version = None, info = None):
	if (info):
	    (self.theLinkTarget, info) = string.split(info, None, 1)
	else:
	    self.theLinkTarget = None

	File.__init__(self, path, version, info)

class Socket(File):

    def infoLine(self):
	return "s %s" % (File.infoLine(self))

    def same(self, other):
	return File.same(self, other)

    def copy(self, source, target):
	pass

    def __init__(self, path, version = None, info = None):
	File.__init__(self, path, version, info)

class NamedPipe(File):

    def infoLine(self):
	return "p %s" % (File.infoLine(self))

    def same(self, other):
	return File.same(self, other)

    def restore(self, reppath, srcpath, root):
	target = root + self.path()
	if not os.path.exists(target):
	    os.mkfifo(target)

	File.restore(self, reppath, root)

    def __init__(self, path, version = None, info = None):
	File.__init__(self, path, version, info)

class Directory(File):

    def infoLine(self):
	return "d %s" % (File.infoLine(self))

    def same(self, other):
	return File.same(self, other)

    def restore(self, reppath, srcpath, root):
	target = root + self.path()
	if not os.path.exists(target):
	    os.mkdir(target)

	File.restore(self, reppath, root)

    def __init__(self, path, version = None, info = None):
	File.__init__(self, path, version, info)

class DeviceFile(File):

    def infoLine(self):
	return "v %c %d %d %s" % (self.type, self.major, self.minor,
				  File.infoLine(self))

    def same(self, other):
	if (self.type == other.type and self.major == other.major and
			self.minor == other.minor):
	    return File.same(self, other)
	
	return 0

    def restore(self, reppath, srcpath, root):
	target = root + self.path()
	if not os.path.exists(target):
	    # FIXME os.mknod is in 2.3
	    os.system("mknod %s %c %d %d" % (target, self.type, self.major,
					    self.minor))

	File.restore(self, reppath, root)

    def majorMinor(self, type = None, major = None, minor = None):
	if type:
	    self.type = type
	    self.major = major
	    self.minor = minor
	
	return (self.type, self.major, self.minor)

    def __init__(self, path, version = None, info = None):
	if (info):
	    (self.type, self.major, self.minor, info) = \
		    string.split(info, None, 3)
	    self.major = int(self.major)
	    self.minor = int(self.minor)

	File.__init__(self, path, version, info)

class RegularFile(File):

    def sha1(self, sha1 = None):
	if (sha1 != None):
	    self.thesha1 = sha1

	return self.thesha1

    def uniqueName(self):
	return self.thesha1

    def infoLine(self):
	return "f %s %s" % (self.thesha1, File.infoLine(self))

    def same(self, other):
	if self.thesha1 == other.thesha1:
	    return File.same(self, other)

	return 0

    def restore(self, reppath, srcpath, root):
	target = root + self.path()
	return self.doRestore(reppath, root, target)
	
    def doRestore(self, reppath, root, target):
	store = datastore.DataStore(reppath + "/contents")
	path = os.path.dirname(target)
	util.mkdirChain(path)
	f = open(target, "w")
	srcFile = store.openFile(self.uniqueName())
	f.write(srcFile.read())
	f.close()
	srcFile.close()
	File.restore(self, reppath, root)

    def archive(self, reppath, root):
	# no need to store the same contents twice; this happens regularly
	# for source and packaged files (config files for example), as well
	# as when the same file exists on multiple branches. we don't allow
	# removing from the archive, so we don't need to ref count or anything
	store = datastore.DataStore(reppath + "/contents")
	if store.hasFile(self.uniqueName()): 
	    return

	file = open(root + "/" +  self.path(), "r")
	store.addFile(file, self.uniqueName())
	file.close()

    def __init__(self, path, version = None, info = None):
	if (info):
	    (self.thesha1, info) = string.split(info, None, 1)
	else:
	    self.thesha1 = None

	File.__init__(self, path, version, info)

class SourceFile(RegularFile):

    def restore(self, reppath, srcpath, root):
	target = root + "/" + srcpath + "/" + os.path.basename(self.path())
	return self.doRestore(reppath, root + "/" + srcpath, target)

    def fileName(self):
	return self.pkgName + "/" + os.path.basename(self.path())

    def pathInRep(self, reppath):
	return reppath + "/sources/" + self.fileName()

    def infoLine(self):
	return "src %s %s" % (self.thesha1, File.infoLine(self))

    def __init__(self, pkgName, path, version = None, info = None):
	self.pkgName = pkgName
	RegularFile.__init__(self, path, version, info)

class FileDB:

    # see if the head of the specified branch is a duplicate
    # of the file object passed; it so return the version object
    # for that duplicate
    def checkBranchForDuplicate(self, branch, file):
	version = self.f.findLatestVersion(branch)
	if not version:
	    return None

	f1 = self.f.getVersion(version)
	lastFile = FileFromInfoLine(self.path, version, f1.read())
	f1.close()

	if file.same(lastFile):
	    return version

	return None

    def findVersion(self, file):
	for (v, f) in self.versions.items():
	    if type(f) == type(file) and f.same(file):
		return (v, f)

	return None

    def addVersion(self, version, file):
	if self.f.hasVersion(version):
	    raise KeyError, "duplicate version for database"
	#else:
	    #if file.pathInRep(self.reppath) + ".info" != self.dbfile:
		#raise KeyError, "path mismatch for file database"
	
	self.f.addVersion(version, "%s\n" % file.infoLine())

    def getVersion(self, version):
	f1 = self.f.getVersion(version)
	file = FileFromInfoLine(self.path, version, f1.read())
	f1.close()
	return file

    def close(self):
	if self.f:
	    self.f.close()
	    self.f = None

    def __del__(self):
	self.close()

    # path is the *full* *absolute* path to the file in the repository
    def __init__(self, reppath, path):
	self.reppath = reppath

	# strip off the leading /reppath/files or /reppath/sources
	parts = string.split(path[len(reppath):], "/")
	self.path = "/" + string.join(parts[2:], "/")

	dbfile = path + ".info"
	util.mkdirChain(os.path.dirname(dbfile))
	if os.path.exists(dbfile):
	    self.f = versioned.open(dbfile, "r+")
	else:
	    self.f = versioned.open(dbfile, "w+")

def FileFromFilesystem(pkgName, root, path, type = "auto"):
    s = os.lstat(root + path)

    if (type == "src"):
	f = SourceFile(pkgName, path)
	f.sha1(sha1helper.hashFile(root + path))
    elif (stat.S_ISREG(s.st_mode)):
	f = RegularFile(path)
	f.sha1(sha1helper.hashFile(root + path))
    elif (stat.S_ISLNK(s.st_mode)):
	f = SymbolicLink(path)
	f.linkTarget(os.readlink(root + path))
    elif (stat.S_ISDIR(s.st_mode)):
	f = Directory(path)
    elif (stat.S_ISSOCK(s.st_mode)):
	f = Socket(path)
    elif (stat.S_ISFIFO(s.st_mode)):
	f = NamedPipe(path)
    elif (stat.S_ISBLK(s.st_mode)):
	f = DeviceFile(path)
	f.majorMinor("b", s.st_rdev >> 8, s.st_rdev & 0xff)
    elif (stat.S_ISCHR(s.st_mode)):
	f = DeviceFile(path)
	f.majorMinor("c", s.st_rdev >> 8, s.st_rdev & 0xff)
    else:
	raise TypeError, "unsupported file type for %s" % path

    f.perms(s.st_mode & 07777)
    f.owner(pwd.getpwuid(s.st_uid)[0])
    f.group(grp.getgrgid(s.st_gid)[0])
    f.mtime(s.st_mtime)

    return f

def FileFromInfoLine(path, version, infoLine):
    (type, infoLine) = string.split(infoLine, None, 1)
    if type == "f":
	return RegularFile(path, version, infoLine)
    elif type == "l":
	return SymbolicLink(path, version, infoLine)
    elif type == "d":
	return Directory(path, version, infoLine)
    elif type == "p":
	return NamedPipe(path, version, infoLine)
    elif type == "v":
	return DeviceFile(path, version, infoLine)
    elif type == "s":
	return Socket(path, version, infoLine)
    elif type == "src":
	# just use the basename here; we don't need the /sources/pkgname bit
	# of things; the base filename is all we need
	#
	# the pkgname "foo" isn't actually used; it will go away from
	# here entirely when we make more progress on the repository
	# format
	return SourceFile("foo", os.path.basename(path), version, infoLine)
    else:
	raise KeyError, "bad infoLine %s" % infoLine
