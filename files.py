#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import string
import os
import versioned
import sha1helper
import stat
import pwd
import grp
import util
import types
from datastore import DataStore

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

	#date = info[5:7]

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

    def infoLine(self):
	return FileMode.infoLine(self)

    def id(self, new = None):
	if new:
	    self.theId = new

	return self.theId

    def restore(self, target):
	self.chmod(target)
	self.setOwnerGroup(target)

    def chmod(self, target):
	os.chmod(target, self.thePerms)

    def setOwnerGroup(self, target):
	if os.getuid(): return

	uid = pwd.getpwnam(self.owner())[2]
	gid = grp.getgrnam(self.group())[2]

	# FIXME: this needs to use lchown, which is in 2.3, and
	# this should happen unconditionally
	os.chown(target, uid, gid)

    # copies a files contents into the repository, if necessary
    def archive(self, repos, source):
	# most file types don't need to do this
	pass

    def __init__(self, fileId, info = None):
	self.theId = fileId
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

    def chmod(self, target):
	# chmod() on a symlink follows the symlink
	pass

    def setOwnerGroup(self, target):
	# chmod() on a symlink follows the symlink
	pass

    def restore(self, repos, target):
	if os.path.exists(target):
	    os.unlink(target)
	os.symlink(self.theLinkTarget, target)
	File.restore(self, target)

    def __init__(self, fileId, info = None):
	if (info):
	    (self.theLinkTarget, info) = string.split(info, None, 1)
	else:
	    self.theLinkTarget = None

	File.__init__(self, fileId, info)

class Socket(File):

    def infoLine(self):
	return "s %s" % (File.infoLine(self))

    def same(self, other):
	return File.same(self, other)

    def copy(self, source, target):
	pass

    def __init__(self, fileId, info = None):
	File.__init__(self, fileId, info)

class NamedPipe(File):

    def infoLine(self):
	return "p %s" % (File.infoLine(self))

    def same(self, other):
	return File.same(self, other)

    def restore(self, repos, target):
	if os.path.exists(target):
	    os.unlink(target)
	os.mkfifo(target)
	File.restore(self, target)

    def __init__(self, fileId, info = None):
	File.__init__(self, fileId, info)

class Directory(File):

    def infoLine(self):
	return "d %s" % (File.infoLine(self))

    def same(self, other):
	return File.same(self, other)

    def restore(self, repos, target):
	if not os.path.isdir(target):
	    os.mkdir(target)

	File.restore(self, target)

    def __init__(self, fileId, info = None):
	File.__init__(self, fileId, info)

class DeviceFile(File):

    def infoLine(self):
	return "v %c %d %d %s" % (self.type, self.major, self.minor,
				  File.infoLine(self))

    def same(self, other):
	if (self.type == other.type and self.major == other.major and
			self.minor == other.minor):
	    return File.same(self, other)
	
	return 0

    def restore(self, repos, target):
	if os.path.exists(target):
	    os.unlink(target)

	# FIXME os.mknod is in 2.3
	os.system("mknod %s %c %d %d" % (target, self.type, self.major,
					self.minor))

	File.restore(self, target)

    def majorMinor(self, type = None, major = None, minor = None):
	if type:
	    self.type = type
	    self.major = major
	    self.minor = minor
	
	return (self.type, self.major, self.minor)

    def __init__(self, fileId, info = None):
	if (info):
	    (self.type, self.major, self.minor, info) = \
		    string.split(info, None, 3)
	    self.major = int(self.major)
	    self.minor = int(self.minor)

	File.__init__(self, fileId, info)

class RegularFile(File):

    def sha1(self, sha1 = None):
	if (sha1 != None):
	    self.thesha1 = sha1

	return self.thesha1

    def infoLine(self):
	return "f %s %s" % (self.thesha1, File.infoLine(self))

    def same(self, other):
	if self.thesha1 == other.thesha1:
	    return File.same(self, other)

	return 0

    def restore(self, repos, target):
	if os.path.exists(target):
	    os.unlink(target)
	else:
	    path = os.path.dirname(target)
	    util.mkdirChain(path)

	f = open(target, "w")
	repos.pullFileContents(self.sha1(), f)
	f.close()
	File.restore(self, target)

    def archive(self, repos, source):
	if repos.hasFileContents(self.sha1()):
	    return

	file = open(source, "r")
	repos.newFileContents(self.sha1(), file)
	file.close()

    def __init__(self, fileId, info = None):
	if (info):
	    (self.thesha1, info) = string.split(info, None, 1)
	else:
	    self.thesha1 = None

	File.__init__(self, fileId, info)

class SourceFile(RegularFile):

    def infoLine(self):
	return "src %s %s" % (self.thesha1, File.infoLine(self))

    def __init__(self, fileId, info = None):
	RegularFile.__init__(self, fileId, info)

class FileDB:

    # see if the head of the specified branch is a duplicate
    # of the file object passed; it so return the version object
    # for that duplicate
    def checkBranchForDuplicate(self, branch, file):
	version = self.f.findLatestVersion(branch)
	if not version:
	    return None

	f1 = self.f.getVersion(version)
	lastFile = FileFromInfoLine(f1.read(), self.fileId)
	f1.close()

	if file.same(lastFile):
	    return version

	return None

    def addVersion(self, version, file):
	if self.f.hasVersion(version):
	    raise KeyError, "duplicate version for database"
	else:
	    if file.id() != self.fileId:
		raise KeyError, "file id mismatch for file database"
	
	self.f.addVersion(version, "%s\n" % file.infoLine())

    def getVersion(self, version):
	f1 = self.f.getVersion(version)
	file = FileFromInfoLine(f1.read(), self.fileId)
	f1.close()
	return file

    def close(self):
	if self.f:
	    self.f.close()
	    self.f = None

    def __del__(self):
	self.close()

    def __init__(self, dbpath, fileId):
	self.fileId = fileId
	store = DataStore(dbpath)
	if store.hasFile(fileId):
	    f = store.openFile(fileId, "r+")
	else:
	    f = store.newFile(fileId)

	self.f = versioned.open(f)

def FileFromFilesystem(path, fileId, type = None):
    s = os.lstat(path)

    if type == "src":
	f = SourceFile(fileId)
	f.sha1(sha1helper.hashFile(path))
    elif (stat.S_ISREG(s.st_mode)):
	f = RegularFile(fileId)
	f.sha1(sha1helper.hashFile(path))
    elif (stat.S_ISLNK(s.st_mode)):
	f = SymbolicLink(fileId)
	f.linkTarget(os.readlink(path))
    elif (stat.S_ISDIR(s.st_mode)):
	f = Directory(fileId)
    elif (stat.S_ISSOCK(s.st_mode)):
	f = Socket(fileId)
    elif (stat.S_ISFIFO(s.st_mode)):
	f = NamedPipe(fileId)
    elif (stat.S_ISBLK(s.st_mode)):
	f = DeviceFile(fileId)
	f.majorMinor("b", s.st_rdev >> 8, s.st_rdev & 0xff)
    elif (stat.S_ISCHR(s.st_mode)):
	f = DeviceFile(fileId)
	f.majorMinor("c", s.st_rdev >> 8, s.st_rdev & 0xff)
    else:
	raise TypeError, "unsupported file type for %s" % path

    f.perms(s.st_mode & 07777)
    f.owner(pwd.getpwuid(s.st_uid)[0])
    f.group(grp.getgrgid(s.st_gid)[0])
    f.mtime(s.st_mtime)

    return f

def FileFromInfoLine(infoLine, fileId):
    (type, infoLine) = string.split(infoLine, None, 1)
    if type == "f":
	return RegularFile(fileId, infoLine)
    elif type == "l":
	return SymbolicLink(fileId, infoLine)
    elif type == "d":
	return Directory(fileId, infoLine)
    elif type == "p":
	return NamedPipe(fileId, infoLine)
    elif type == "v":
	return DeviceFile(fileId, infoLine)
    elif type == "s":
	return Socket(fileId, infoLine)
    elif type == "src":
	return SourceFile(fileId, infoLine)
    else:
	raise KeyError, "bad infoLine %s" % infoLine
