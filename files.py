#
# Copyright (c) 2004 Specifix, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
import copy
import grp
import log
import os
import pwd
import sha1helper
import socket
import stat
import streams
import string
import struct
import sys
import tempfile
import time
import types
import util

from deps import filedeps, deps

_FILE_FLAG_CONFIG = 1 << 0
# the following three are a legacy from before tag handlers, kept because they
# describe entries in the main repository...  If we run out, we can
# touch up our main repository (the only one that has any references to
# these flag values) to use the corresponding tags and then reuse these
# values.
_FILE_FLAG_INITSCRIPT = 1 << 1
_FILE_FLAG_SHLIB = 1 << 2
_FILE_FLAG_GCONFSCHEMA = 1 << 3
# transient contents that may have modified contents overwritten
_FILE_FLAG_TRANSIENT = 1 << 4
_FILE_FLAG_SOURCEFILE = 1 << 5
# files which were added to source components by conary rather then by
# the user. this isn't used yet, just reserved.
_FILE_FLAG_AUTOSOURCE = 1 << 6	

class DeviceStream(streams.TupleStream):

    __slots__ = []

    makeup = (("major", streams.IntStream, 4), ("minor", streams.IntStream, 4))

    def major(self):
        return self.items[0].value()

    def setMajor(self, value):
        return self.items[0].set(value)

    def minor(self):
        return self.items[1].value()

    def setMinor(self, value):
        return self.items[1].set(value)

class RegularFileStream(streams.TupleStream):

    __slots__ = []
    makeup = (("size", streams.LongLongStream, 8), 
	      ("sha1", streams.Sha1Stream, 20))

    def size(self):
        return self.items[0].value()

    def setSize(self, value):
        return self.items[0].set(value)

    def sha1(self):
        return self.items[1].value()

    def setSha1(self, value):
        return self.items[1].set(value)

class OldRegularFileStream(streams.TupleStream):

    __slots__ = []
    makeup = (("size", streams.LongLongStream, 8), 
	      ("sha1", streams.StringStream, 40))
    
class InodeStream(streams.TupleStream):

    __slots__ = []

    """
    Stores basic inode information on a file: perms, owner, group.
    """

    # this is permissions, mtime, owner, group
    makeup = (("perms", streams.ShortStream, 2), 
	      ("mtime", streams.MtimeStream, 4), 
              ("owner", streams.StringStream, "B"), 
	      ("group", streams.StringStream, "B"))

    def perms(self):
        return self.items[0].value()

    def setPerms(self, value):
        return self.items[0].set(value)

    def mtime(self):
        return self.items[1].value()

    def setMtime(self, value):
        return self.items[1].set(value)

    def owner(self):
        return self.items[2].value()

    def setOwner(self, value):
        return self.items[2].set(value)

    def group(self):
        return self.items[3].value()

    def setGroup(self, value):
        self.items[3].set(value)
        
    def triplet(self, code, setbit = 0):
	l = [ "-", "-", "-" ]
	if code & 4:
	    l[0] = "r"
	    
	if code & 2:
	    l[1] = "w"

	if setbit:
	    if code & 1:
		l[2] = "s"
	    else:
		l[2] = "S"
	elif code & 1:
	    l[2] = "x"
	    
	return l

    def permsString(self):
	perms = self.perms()

	l = self.triplet(perms >> 6, perms & 04000) + \
	    self.triplet(perms >> 3, perms & 02000) + \
	    self.triplet(perms >> 0)
	
	if perms & 01000:
	    if l[8] == "x":
		l[8] = "t"
	    else:
		l[8] = "T"

	return "".join(l)

    def timeString(self, now = None):
	if not now:
	    now = time.time()
	timeSet = time.localtime(self.mtime())
	nowSet = time.localtime(now)

	# if this file is more then 6 months old, use the year
	monthDelta = nowSet[1] - timeSet[1]
	yearDelta = nowSet[0] - timeSet[0]

	if monthDelta < 0:
	    yearDelta = yearDelta - 1
	    monthDelta = monthDelta + 12

	monthDelta = monthDelta + 12 * yearDelta

	if nowSet[2] < timeSet[2]:
	    monthDelta = monthDelta - 1

	if monthDelta < 6:
	    return time.strftime("%b %e %H:%M", timeSet)
	else:
	    return time.strftime("%b %e  %Y", timeSet)

    def metadataEqual(self, other):
	return self.__class__ == other.__class__ and \
	       self.perms() == other.perms() 

    def __eq__(self, other):
	return self.__class__ == other.__class__ and \
	       self.perms() == other.perms() and\
	       self.owner() == other.owner() and \
	       self.group() == other.group()

class FlagsStream(streams.IntStream):

    __slots__ = "val"

    def isConfig(self, set = None):
	return self._isFlag(_FILE_FLAG_CONFIG, set)

    def isSource(self, set = None):
	return self._isFlag(_FILE_FLAG_SOURCEFILE, set)

    def isTransient(self, set = None):
	return self._isFlag(_FILE_FLAG_TRANSIENT, set)

    def isInitScript(self, set = None):
	'legacy, not used for new packages'
	assert(0)
	return self._isFlag(_FILE_FLAG_INITSCRIPT, set)

    def isShLib(self, set = None):
	'legacy, not used for new packages'
	assert(0)
	return self._isFlag(_FILE_FLAG_SHLIB, set)

    def isGconfSchema(self, set = None):
	'legacy, not used for new packages'
	assert(0)
	return self._isFlag(_FILE_FLAG_GCONFSCHEMA, set)

    def _isFlag(self, flag, set):
	if set != None:
            if self.val is None:
                self.val = 0x0
	    if set:
		self.val |= flag
	    else:
		self.val &= ~(flag)

	return (self.val and self.val & flag)

class File(streams.StreamSet):

    lsTag = None
    hasContents = 0
    streamDict = { streams._STREAM_INODE : (InodeStream, "inode"),
                   streams._STREAM_FLAGS : (FlagsStream, "flags"),
		   streams._STREAM_TAGS :  (streams.StringsStream, "tags") }
    __slots__ = [ "theId", "inode", "flags", "tags" ]

    def modeString(self):
	l = self.inode.permsString()
	return self.lsTag + string.join(l, "")

    def timeString(self):
	return self.inode.timeString()

    def sizeString(self):
	return "       0"

    def id(self, new = None):
	if new:
	    self.theId = new

	return self.theId

    def remove(self, target):
	os.unlink(target)

    def restore(self, root, target, restoreContents, skipMtime = 0):
	self.setOwnerGroup(root, target)
	self.chmod(target)

	if not skipMtime:
	    self.setMtime(target)

    def setMtime(self, target):
	os.utime(target, (self.inode.mtime(), self.inode.mtime()))

    def chmod(self, target):
	os.chmod(target, self.inode.perms())

    def setOwnerGroup(self, root, target):
	if os.getuid(): return

	global userCache, groupCache

	uid = userCache.lookup(root, self.inode.owner())
	gid = groupCache.lookup(root, self.inode.group())

	os.lchown(target, uid, gid)

    def twm(self, diff, base, skip = None):
	sameType = struct.unpack("B", diff[0])
	if not sameType: 
	    # XXX file type changed -- we don't support this yet
	    raise AssertionError
	assert(self.lsTag == base.lsTag)
	assert(self.lsTag == diff[1])
	
	return streams.StreamSet.twm(self, diff[2:], base, skip = skip)

    def __eq__(self, other):
	if other.lsTag != self.lsTag: return False
	return streams.StreamSet.__eq__(self, other)

    def metadataEqual(self, other, ignoreOwnerGroup):
	if not ignoreOwnerGroup:
	    return self == other

	for streamType, name in self.streamDict.itervalues():
	    if name == 'inode':
		if not self.__getattribute__(name).metadataEqual(
		       other.__getattribute__(name)):
		    return False
	    elif not self.__getattribute__(name) == other.__getattribute__(name):
		return False

	return True

    def freeze(self):
	return self.lsTag + streams.StreamSet.freeze(self)

    def __init__(self, fileId, streamData = None):
        assert(self.__class__ is not File)
	self.theId = fileId
	if streamData is not None:
	    streams.StreamSet.__init__(self, streamData[1:])
	else:
	    streams.StreamSet.__init__(self, None)

class SymbolicLink(File):

    lsTag = "l"
    streamDict = { streams._STREAM_TARGET : (streams.StringStream, "target") }
    streamDict.update(File.streamDict)
    __slots__ = "target"

    def sizeString(self):
	return "%8d" % len(self.target.value())

    def chmod(self, target):
	# chmod() on a symlink follows the symlink
	pass

    def setOwnerGroup(self, root, target):
	# chmod() on a symlink follows the symlink
	pass

    def restore(self, fileContents, root, target, restoreContents):
	if os.path.exists(target) or os.path.islink(target):
	    os.unlink(target)
        util.mkdirChain(os.path.dirname(target))
	os.symlink(self.target.value(), target)
	File.restore(self, root, target, restoreContents, skipMtime = 1)

class Socket(File):

    lsTag = "s"
    __slots__ = []

    def restore(self, fileContents, root, target, restoreContents):
	if os.path.exists(target) or os.path.islink(target):
	    os.unlink(target)
        util.mkdirChain(os.path.dirname(target))
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0);
        sock.bind(target)
        sock.close()
	File.restore(self, root, target, restoreContents)

class NamedPipe(File):

    lsTag = "p"
    __slots__ = []

    def restore(self, fileContents, root, target, restoreContents):
	if os.path.exists(target) or os.path.islink(target):
	    os.unlink(target)
        util.mkdirChain(os.path.dirname(target))
	os.mkfifo(target)
	File.restore(self, root, target, restoreContents)

class Directory(File):

    lsTag = "d"
    __slots__ = []

    def restore(self, fileContents, root, target, restoreContents):
	if not os.path.isdir(target):
	    util.mkdirChain(target)

	File.restore(self, root, target, restoreContents)

    def remove(self, target):
	raise NotImplementedError

class DeviceFile(File):

    streamDict = { streams._STREAM_DEVICE : (DeviceStream, "devt") }
    streamDict.update(File.streamDict)
    __slots__ = [ 'devt' ]

    def sizeString(self):
	return "%3d, %3d" % (self.devt.major(), self.devt.minor())

    def restore(self, fileContents, root, target, restoreContents):
	if os.path.exists(target) or os.path.islink(target):
	    os.unlink(target)

	if os.getuid(): return

	if self.lsTag == 'c':
	    flags = stat.S_IFCHR
	else:
	    flags = stat.S_IFBLK
        util.mkdirChain(os.path.dirname(target))
	os.mknod(target, flags, os.makedev(self.devt.major(), 
		 self.devt.minor()))
            
	File.restore(self, root, target, restoreContents)

class BlockDevice(DeviceFile):

    lsTag = "b"
    __slots__ = []

class CharacterDevice(DeviceFile):

    lsTag = "c"
    __slots__ = []
    
class RegularFile(File):

    streamDict = { 
	streams._STREAM_CONTENTS : (RegularFileStream , 'contents' ), 
        streams._STREAM_PROVIDES : (streams.DependenciesStream, 'provides' ), 
        streams._STREAM_REQUIRES : (streams.DependenciesStream, 'requires' ), 
        streams._STREAM_FLAVOR   : (streams.DependenciesStream, 'flavor' ) }

    streamDict.update(File.streamDict)
    __slots__ = ('contents', 'provides', 'requires', 'flavor')

    lsTag = "-"
    hasContents = 1

    def sizeString(self):
	return "%8d" % self.contents.size()

    def restore(self, fileContents, root, target, restoreContents):
	if restoreContents:
	    # this is first to let us copy the contents of a file
	    # onto itself; the unlink helps that to work
	    src = fileContents.get()

	    path = os.path.dirname(target)
	    name = os.path.basename(target)
	    if not os.path.isdir(path):
		util.mkdirChain(path)

	    tmpfd, tmpname = tempfile.mkstemp(name, '.ct', path)
	    try:
		File.restore(self, root, tmpname, restoreContents)
		f = os.fdopen(tmpfd, 'w')
		util.copyfileobj(src, f)
		f.close()
		os.rename(tmpname, target)
		self.setMtime(target)
	    except:
		os.unlink(tmpname)
		raise

	else:
	    File.restore(self, root, target, restoreContents)

    def __init__(self, *args, **kargs):
	File.__init__(self, *args, **kargs)

def FileFromFilesystem(path, fileId, possibleMatch = None, buildDeps = False):
    s = os.lstat(path)

    try:
        owner = pwd.getpwuid(s.st_uid)[0]
    except KeyError, msg:
	raise FilesError(
	    "Error mapping uid %d to user name: %s" %(s.st_uid, msg))

    try:
        group = grp.getgrgid(s.st_gid)[0]
    except KeyError, msg:
	raise FilesError(
	    "Error mapping gid %d to group name: %s" %(s.st_gid, msg))

    needsSha1 = 0
    inode = InodeStream(s.st_mode & 07777, s.st_mtime, owner, group)

    if (stat.S_ISREG(s.st_mode)):
	f = RegularFile(fileId)
	needsSha1 = 1
    elif (stat.S_ISLNK(s.st_mode)):
	f = SymbolicLink(fileId)
	f.target.set(os.readlink(path))
    elif (stat.S_ISDIR(s.st_mode)):
	f = Directory(fileId)
    elif (stat.S_ISSOCK(s.st_mode)):
	f = Socket(fileId)
    elif (stat.S_ISFIFO(s.st_mode)):
	f = NamedPipe(fileId)
    elif (stat.S_ISBLK(s.st_mode)):
	f = BlockDevice(fileId)
	f.devt.setMajor(s.st_rdev >> 8)
	f.devt.setMinor(s.st_rdev & 0xff)
    elif (stat.S_ISCHR(s.st_mode)):
	f = CharacterDevice(fileId)
	f.devt.setMajor(s.st_rdev >> 8)
	f.devt.setMinor(s.st_rdev & 0xff)
    else:
        raise FilesError("unsupported file type for %s" % path)

    f.inode = inode
    f.flags = FlagsStream(0)
    
    # assume we have a match if the FileMode and object type match
    if possibleMatch and (possibleMatch.__class__ == f.__class__) \
		     and f.inode == possibleMatch.inode \
		     and f.inode.mtime() == possibleMatch.inode.mtime() \
		     and (not s.st_size or
			  (possibleMatch.hasContents and
			   s.st_size == possibleMatch.contents.size())):
        f.flags.set(possibleMatch.flags.value())
        return possibleMatch

    if needsSha1:
	sha1 = sha1helper.hashFileBin(path)
	f.contents = RegularFileStream()
	f.contents.setSize(s.st_size)
	f.contents.setSha1(sha1)

    if buildDeps and f.hasContents and isinstance(f, RegularFile):
	result = filedeps.findFileDependencies(path)
	if result != None:
	    f.requires.set(result[0])
	    f.provides.set(result[1])

        f.flavor.set(filedeps.findFileFlavor(path))

    return f

def ThawFile(frz, fileId):
    if frz[0] == "-":
	return RegularFile(fileId, streamData = frz)
    elif frz[0] == "d":
	return Directory(fileId, streamData = frz)
    elif frz[0] == "p":
	return NamedPipe(fileId, streamData = frz)
    elif frz[0] == "s":
	return Socket(fileId, streamData = frz)
    elif frz[0] == "l":
	return SymbolicLink(fileId, streamData = frz)
    elif frz[0] == "b":
	return BlockDevice(fileId, streamData = frz)
    elif frz[0] == "c":
	return CharacterDevice(fileId, streamData = frz)

    raise AssertionError

class FilesError(Exception):
    def __init__(self, msg):
        Exception.__init__(self)
        self.msg = msg

    def __repr__(self):
	return self.msg

    def __str__(self):
	return repr(self)

def contentsChanged(diff):
    if diff[0] == 0:
	return False

    type = diff[1]
    if type != "-": return False
    i = 2

    while i < len(diff):
	streamId, size = struct.unpack("!BH", diff[i:i+3])
	i += 3

	name = RegularFile.streamDict[streamId][1]
	
	if name == "contents":
	    return size != 0

	i += size

    assert(i == len(diff))

    return False

def fieldsChanged(diff):
    sameType = struct.unpack("B", diff[0])
    if not sameType:
	return [ "type" ]
    type = diff[1]
    i = 2

    if type == "-":
	cl = RegularFile
    elif type == "d":
	cl = Directory
    elif type == "b":
	cl = BlockDevice
    elif type == "c":
	cl = CharacterDevice
    elif type == "s":
	cl = Socket
    elif type == "l":
	cl = SymbolicLink
    elif type == "p":
	cl = NamedPipe
    else:
	raise AssertionError

    rc = []

    while i < len(diff):
	streamId, size = struct.unpack("!BH", diff[i:i+3])
	i += 3
	if not size: continue

	name = cl.streamDict[streamId][1]
	
	if name == "inode":
	    l = tupleChanged(InodeStream, diff[i:i+size])
	    if l:
		s = " ".join(l)
		rc.append("inode(%s)" % s)
	elif name == "contents":
	    l = tupleChanged(RegularFileStream, diff[i:i+size])
	    if l:
		s = " ".join(l)
		rc.append("contents(%s)" % s)
	else:
	    rc.append(name)

	i += size

    assert(i == len(diff))

    return rc

def tupleChanged(cl, diff):
    what = struct.unpack("B", diff[0])[0]

    rc = []
    for (i, (name, itemType, size)) in enumerate(cl.makeup):
	if what & (1 << i):
	    rc.append(name)

    return rc

class UserGroupIdCache:

    def lookup(self, root, name):
	theId = self.cache.get(name, None)
	if theId is not None:
	    return theId

	if root and root != '/':
	    curDir = os.open(".", os.O_RDONLY)
	    os.chdir("/")
	    os.chroot(root)
	
	try:
	    theId = self.lookupFn(name)[2]
	except KeyError:
	    log.warning('%s %s does not exist - using root', self.name, name)
	    theId = 0

	if root and root != '/':
	    os.chroot(".")
	    os.fchdir(curDir)

	self.cache[name] = theId
	return theId

    def __init__(self, name, lookupFn):
	self.lookupFn = lookupFn
	self.name = name
	self.cache = { 'root' : 0 }
	
userCache = UserGroupIdCache('user', pwd.getpwnam)
groupCache = UserGroupIdCache('group', grp.getgrnam)
