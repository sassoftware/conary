# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import string
import copy
import os
import sha1helper
import stat
import pwd
import grp
import util
import types
import time
import socket
import struct
import log

_FILE_FLAG_CONFIG = 1 << 0
_FILE_FLAG_INITSCRIPT = 1 << 1
_FILE_FLAG_SHLIB = 1 << 2

_STREAM_INFO	    = 0
_STREAM_SHORT	    = 1
_STREAM_INT	    = 2
_STREAM_LONGLONG    = 3
_STREAM_STRING	    = 4
_STREAM_DEVICE	    = 5
_STREAM_SIZESHA1    = 6
_STREAM_INODE	    = 7
_STREAM_FLAGS	    = 7
_STREAM_MTIME	    = 7

class InfoStream:

    streamId = _STREAM_INFO

    def freeze(self):
	raise NotImplementedError

    def diff(self, them):
	raise NotImplementedError

    def set(self, val):
	raise NotImplementedError

    def merge(self, val):
	raise NotImplementedError

    def twm(self, diff, base):
	"""
	Performs a three way merge. Base is the original information,
	diff is one of the changes, and self is the (already changed)
	object. Returns a boolean saying whether or not the merge was
	successful.
	"""
	raise NotImplementedError
	
class NumericStream(InfoStream):

    def value(self):
	return self.val

    def set(self, val):
	self.val = val

    def merge(self, other):
	if other.val != None:
	    self.val = other.val

    def freeze(self):
	return struct.pack(self.format, self.val)

    def diff(self, them):
	if self.val != them.val:
	    return struct.pack(self.format, self.val)

	return ""

    def thaw(self, frz):
	self.val = struct.unpack(self.format, frz)[0]

    def twm(self, diff, base):
	if not diff: return False

	newSize = struct.unpack(self.format, diff)[0]
	if self.val == base.val:
	    self.val = newSize
	    return False
	elif base.val != newSize:
	    return True

    def __eq__(self, other):
	return other.__class__ == self.__class__ and \
	       self.val == other.val

    def __init__(self, val = None):
	if type(val) == str:
	    self.thaw(val)
	else:
	    self.val = val

class ShortStream(NumericStream):

    streamId = _STREAM_SHORT

    format = "!H"

class IntStream(NumericStream):

    streamId = _STREAM_INT

    format = "!I"

class MtimeStream(NumericStream):

    streamId = _STREAM_MTIME

    format = "!I"

    def __eq__(self, other):
	# don't ever compare mtimes
	return True

    def twm(self, diff, base):
	# and don't let merges fail
	NumericStream.twm(self, diff, base)
	return False

class LongLongStream(NumericStream):

    streamId = _STREAM_LONGLONG

    format = "!Q"

class StringStream(InfoStream):
    """
    Stores a simple string; used for the target of symbolic links
    """

    streamId = _STREAM_STRING

    def value(self):
	return self.s

    def set(self, val):
	self.s = val

    def merge(self, other):
	if other.s != None:
	    self.s = other.s

    def freeze(self):
	return self.s

    def diff(self, them):
	if self.s != them.s:
	    return self.s

	return ""

    def thaw(self, frz):
	self.s = frz

    def twm(self, diff, base):
	if not diff: return False

	if self.s == base.s:
	    self.s = diff
	    return False
	elif base.s != diff:
	    return True

    def __eq__(self, other):
	return other.__class__ == self.__class__ and \
	       self.s == other.s

    def __init__(self, s = None):
	self.s = s

class TupleStream(InfoStream):

    def __eq__(self, other):
	return other.__class__ == self.__class__ and other.items == self.items

    def freeze(self):
	rc = []
	for (i, (name, itemType, size)) in enumerate(self.makeup):
	    if type(size) == int or (i + 1 == len(self.makeup)):
		rc.append(self.items[i].freeze())
	    else:
		s = self.items[i].freeze()
		rc.append(struct.pack(size, len(s)) + s)

	return "".join(rc)

    def merge(self, other):
	for i in xrange(len(self.makeup)):
	    self.items[i].merge(other.items[i])

    def diff(self, them):
	code = 0
	rc = []
	for (i, (name, itemType, size)) in enumerate(self.makeup):
	    d = self.items[i].diff(them.items[i])
	    if d:
		if type(size) == int or (i + 1) == len(self.makeup):
		    rc.append(d)
		else:
		    rc.append(struct.pack(size, len(d)) + d)
		code |= (1 << i)
		
	return struct.pack("B", code) + "".join(rc)

    def twm(self, diff, base):
	what = struct.unpack("B", diff[0])[0]
	idx = 1
	conflicts = False

	for (i, (name, itemType, size)) in enumerate(self.makeup):
	    if what & (1 << i):
		if type(size) == int:
		    pass
		elif (i + 1) == len(self.makeup):
		    size = len(diff) - idx
		else:
		    if size == "B":
			size = struct.unpack("B", diff[idx])[0]
			idx += 1
		    elif size == "!H":
			size = struct.unpack("!H", diff[idx:idx + 2])[0]
			idx += 2
		    else:
			assert(0)

		d = diff[idx:size]

		conflicts = conflicts or \
		    self.items[i].twm(diff[idx:idx + size], base.items[i])
		idx += size

	return conflicts

    def thaw(self, s):
	self.items = []
	idx = 0
	for (i, (name, itemType, size)) in enumerate(self.makeup):
	    if type(size) == int:
		self.items.append(itemType(s[idx:idx + size]))
	    elif (i + 1) == len(self.makeup):
		self.items.append(itemType(s[idx:]))
		size = 0
	    else:
		if size == "B":
		    size = struct.unpack("B", s[idx])[0]
		    idx += 1
		elif size == "!H":
		    size = struct.unpack("!H", s[idx:idx + 2])[0]
		    idx += 2
		else:
		    assert(0)

		self.items.append(itemType(s[idx:idx + size]))

	    idx += size

    def __deepcopy__(self, memo):
	# trying to copy the lambda this uses causes problems; this
	# avoids them
	return self.__class__(self.freeze())

    def __init__(self, first = None, *rest):
	if first == None:
	    self.items = []
	    for (i, (name, itemType, size)) in enumerate(self.makeup):
		self.items.append(itemType())
	elif type(first) == str and not rest:
	    self.thaw(first)
	else:
	    all = (first, ) + rest
	    self.items = []
	    for (i, (name, itemType, size)) in enumerate(self.makeup):
		self.items.append(itemType(all[i]))

	for (i, (name, itemType, size)) in enumerate(self.makeup):
	    self.__dict__[name] = lambda num = i: self.items[num].value()
	    setName = "set" + name[0].capitalize() + name[1:]
	    self.__dict__[setName] = \
		lambda val, num = i: self.items[num].set(val)

class DeviceStream(TupleStream):

    makeup = (("major", IntStream, 4), ("minor", IntStream, 4))
    streamId = _STREAM_DEVICE

class RegularFileStream(TupleStream):

    makeup = (("size", LongLongStream, 8), ("sha1", StringStream, 40))
    streamId = _STREAM_SIZESHA1

class InodeStream(TupleStream):

    """
    Stores basic inode information on a file: perms, owner, group.
    """

    # this is permissions, mtime, owner, group
    makeup = (("perms", ShortStream, 2), ("mtime", MtimeStream, 4), 
              ("owner", StringStream, "B"), ("group", StringStream, "B"))
    streamId = _STREAM_INODE

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

    def timeString(self):
	timeSet = time.localtime(self.mtime())
	nowSet = time.localtime(time.time())

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

class FlagsStream(IntStream):

    streamId = _STREAM_FLAGS

    def isConfig(self, set = None):
	return self._isFlag(_FILE_FLAG_CONFIG, set)

    def isInitScript(self, set = None):
	return self._isFlag(_FILE_FLAG_INITSCRIPT, set)

    def isShLib(self, set = None):
	return self._isFlag(_FILE_FLAG_SHLIB, set)

    def _isFlag(self, flag, set):
	if set != None:
            if self.val is None:
                self.val = 0x0
	    if set:
		self.val |= flag
	    else:
		self.val &= ~(flag)

	return (self.val and self.val & flag)

class File:

    lsTag = None
    hasContents = 0
    streamList = ( ("inode", InodeStream), ("flags", FlagsStream) )

    def modeString(self):
	l = self.inode.permsString()
	return self.lsTag + string.join(l, "")

    def timeString(self):
	return self.inode.timeString()

    def sizeString(self):
	return "       0"

    def copy(self):
	return copy.deepcopy(self)

    def id(self, new = None):
	if new:
	    self.theId = new

	return self.theId

    def remove(self, target):
	os.unlink(target)

    def restore(self, target, restoreContents, skipMtime = 0):
	self.setOwnerGroup(target)
	self.chmod(target)

	if not skipMtime:
	    os.utime(target, (self.inode.mtime(), self.inode.mtime()))

    def chmod(self, target):
	os.chmod(target, self.inode.perms())

    def setOwnerGroup(self, target):
	if os.getuid(): return

        try:
            uid = pwd.getpwnam(self.inode.owner())[2]
        except KeyError:
            log.warning('user %s does not exist - using root', self.owner())
            uid = 0
        try:
            gid = grp.getgrnam(self.inode.group())[2]
        except KeyError:
            log.warning('group %s does not exist - using root', self.group())
            gid = 0

	os.lchown(target, uid, gid)

    def initializeStreams(self, data):
	if not data: 
	    for (name, streamType) in self.streamList:
		self.__dict__[name] = streamType()
	else:
	    # skip over the file type for now
	    i = 1
	    for (name, streamType) in self.streamList:
		(streamId, size) = struct.unpack("!BH", data[i:i+3])
		assert(streamId == streamType.streamId)
		i += 3
		self.__dict__[name] = streamType(data[i:i + size])
		i += size

	    # FIXME
	    #assert(i == len(data))

    def diff(self, other):
	if self.lsTag != other.lsTag:
	    d = self.freeze()
	    return struct.pack("!BH", 0, len(d)) + d

	rc = [ "\x01", self.lsTag ]
	for (name, streamType) in self.streamList:
	    d = self.__dict__[name].diff(other.__dict__[name])
	    rc.append(struct.pack("!H", len(d)) + d)

	return "".join(rc)

    def twm(self, diff, base, skip = None):
	sameType = struct.unpack("B", diff[0])
	if not sameType: 
	    # XXX file type changed -- we don't support this yet
	    assert(0)
	assert(self.lsTag == base.lsTag)
	assert(self.lsTag == diff[1])
	i = 2
	conflicts = False
	
	for (name, streamType) in self.streamList:
	    size = struct.unpack("!H", diff[i:i+2])[0]
	    i += 2
	    if name != skip:
		w = self.__dict__[name].twm(diff[i:i+size], base.__dict__[name])
	    i += size
	    conflicts = conflicts or w

	assert(i == len(diff))

	return conflicts

    def __eq__(self, other):
	if other.lsTag != self.lsTag: return False

	for (name, streamType) in self.streamList:
	    if not self.__dict__[name] == other.__dict__[name]:
		return False

	return True

    def metadataEqual(self, other, ignoreOwnerGroup):
	if not ignoreOwnerGroup:
	    return self == other

	for (name, streamType) in self.streamList:
	    if name == 'inode':
		if not self.__dict__[name].metadataEqual(
		       other.__dict__[name]):
		    return False
	    elif not self.__dict__[name] == other.__dict__[name]:
		return False

	return True

    def freeze(self):
	rc = [ self.lsTag ]
	for (name, streamType) in self.streamList:
	    s = self.__dict__[name].freeze()
	    rc.append(struct.pack("!BH", streamType.streamId, len(s)) + s)
	return "".join(rc)

    def __init__(self, fileId, streamData = None):
        assert(self.__class__ is not File)
	self.theId = fileId
	self.initializeStreams(streamData)

class SymbolicLink(File):

    lsTag = "l"
    streamList = File.streamList + (("target", StringStream ),)

    def sizeString(self):
	return "%8d" % len(self.target.value())

    def chmod(self, target):
	# chmod() on a symlink follows the symlink
	pass

    def setOwnerGroup(self, target):
	# chmod() on a symlink follows the symlink
	pass

    def restore(self, fileContents, target, restoreContents):
	if os.path.exists(target) or os.path.islink(target):
	    os.unlink(target)
        util.mkdirChain(os.path.dirname(target))
	os.symlink(self.theLinkTarget, target)
	File.restore(self, target, restoreContents, skipMtime = 1)

class Socket(File):

    lsTag = "s"

    def restore(self, fileContents, target, restoreContents):
	if os.path.exists(target) or os.path.islink(target):
	    os.unlink(target)
        util.mkdirChain(os.path.dirname(target))
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0);
        sock.bind(target)
        sock.close()
	File.restore(self, target, restoreContents)

class NamedPipe(File):

    lsTag = "p"

    def restore(self, fileContents, target, restoreContents):
	if os.path.exists(target) or os.path.islink(target):
	    os.unlink(target)
        util.mkdirChain(os.path.dirname(target))
	os.mkfifo(target)
	File.restore(self, target, restoreContents)

class Directory(File):

    lsTag = "d"

    def restore(self, fileContents, target, restoreContents):
	if not os.path.isdir(target):
	    util.mkdirChain(target)

	File.restore(self, target, restoreContents)

    def remove(self, target):
        try:
            os.rmdir(target)
        except OSError, err:
            # XXX
            log.warning('rmdir %s failed: %s', target, str(err))

class DeviceFile(File):

    streamList = File.streamList + (("devt", DeviceStream ),)

    def sizeString(self):
	return "%3d, %3d" % (self.major, self.minor)

    def restore(self, fileContents, target, restoreContents):
	if os.path.exists(target) or os.path.islink(target):
	    os.unlink(target)

	if os.getuid(): return

	if self.lsTag == 'c':
	    flags = stat.S_IFCHR
	else:
	    flags = stat.S_IFBLK
        util.mkdirChain(os.path.dirname(target))
	os.mknod(target, flags, os.makedev(self.major, self.minor))
            
	File.restore(self, target, restoreContents)

class BlockDevice(DeviceFile):

    lsTag = "b"

class CharacterDevice(DeviceFile):

    lsTag = "c"
    
class RegularFile(File):

    streamList = File.streamList + (('contents', RegularFileStream ),)

    lsTag = "-"
    hasContents = 1

    def sizeString(self):
	return "%8d" % self.contents.size()

    def restore(self, fileContents, target, restoreContents):
	if restoreContents:
	    # this is first to let us copy the contents of a file
	    # onto itself; the unlink helps that to work
	    src = fileContents.get()

	    if os.path.exists(target) or os.path.islink(target):
		os.unlink(target)
	    else:
		path = os.path.dirname(target)
		util.mkdirChain(path)

	    f = open(target, "w")
            util.copyfileobj(src, f)
	    f.close()

	File.restore(self, target, restoreContents)

def FileFromFilesystem(path, fileId, possibleMatch = None,
                       requireSymbolicOwnership = False):
    s = os.lstat(path)

    try:
        owner = pwd.getpwuid(s.st_uid)[0]
    except KeyError, msg:
        if requireSymbolicOwnership:
            raise FilesError(
                "Error mapping uid %d to user name: %s" %(s.st_uid, msg))
        else:
	    owner = str(s.st_uid)

    try:
        group = grp.getgrgid(s.st_gid)[0]
    except KeyError, msg:
        if requireSymbolicOwnership:
            raise FilesError(
                "Error mapping gid %d to group name: %s" %(s.st_gid, msg))
        else:
            group = str(s.st_gid)

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
	f.devt.major(s.st_rdev >> 8)
	f.devt.minor(s.st_rdev & 0xff)
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
			   s.st_size == possibleMatch.contents.size())
			 ):
	    f.flags.set(possibleMatch.flags.value())
	    return possibleMatch

    if needsSha1:
	sha1 = sha1helper.hashFile(path)
	f.contents = RegularFileStream(s.st_size, sha1)

    return f

def ThawFile(frz, fileId):
    type = frz[0]

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

    assert(0)

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

    for (name, streamType) in RegularFile.streamList:
	size = struct.unpack("!H", diff[i:i+2])[0]
	i += 2
	
	if name == "contents":
	    return size != 0

	i += size

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
	cl = SocketDevice
    elif type == "l":
	cl = SymbolicLink
    elif type == "p":
	cl = NamedPipe
    else:
	assert(0)

    rc = []

    for (name, streamType) in cl.streamList:
	size = struct.unpack("!H", diff[i:i+2])[0]
	i += 2
	if not size: continue
	
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

    return rc

def tupleChanged(cl, diff):
    what = struct.unpack("B", diff[0])[0]

    rc = []
    for (i, (name, itemType, size)) in enumerate(cl.makeup):
	if what & (1 << i):
	    rc.append(name)

    return rc
