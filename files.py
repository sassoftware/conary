#
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
import time
import socket
import struct
import tempfile
import log

from deps import filedeps, deps

_FILE_FLAG_CONFIG = 1 << 0
_FILE_FLAG_INITSCRIPT = 1 << 1
_FILE_FLAG_SHLIB = 1 << 2
_FILE_FLAG_GCONFSCHEMA = 1<< 3

_STREAM_INFO	    = 0
_STREAM_SHORT	    = 1
_STREAM_INT	    = 2
_STREAM_LONGLONG    = 3
_STREAM_STRING	    = 4
_STREAM_DEVICE	    = 5
_STREAM_SIZESHA1    = 6
_STREAM_INODE	    = 7
_STREAM_FLAGS	    = 8
_STREAM_MTIME	    = 9
_STREAM_DEPENDENCIES = 10
_STREAM_STRINGS     = 11

class InfoStream(object):

    __slots__ = ()

    streamId = _STREAM_INFO

    def copy(self):
        return self.__class__(self.freeze())
    
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

    def __eq__(self, them):
	raise NotImplementedError

    def __ne__(self, them):
	return not self.__eq__(them)

class NumericStream(InfoStream):

    __slots__ = "val"

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

	newVal = struct.unpack(self.format, diff)[0]
	if self.val == base.val:
	    self.val = newVal
	    return False
	elif base.val != newVal:
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

    __slots__ = ( "val", )

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

    __slots__ = "s"
    streamId = _STREAM_STRING

    def value(self):
	return self.s

    def set(self, val):
        assert(type(val) is str)
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

    def __init__(self, s = ''):
	self.thaw(s)

class DependenciesStream(InfoStream):
    """
    Stores list of strings; used for requires/provides lists
    """

    __slots__ = 'deps'
    streamId = _STREAM_DEPENDENCIES

    def value(self):
	return self.deps

    def set(self, val):
	self.deps = val

    def merge(self, other):
        self.deps = other.deps

    def freeze(self):
        if self.deps is None:
            return ''
        return self.deps.freeze()

    def diff(self, them):
	if self.deps != them.deps:
	    return self.freeze()

	return ''

    def thaw(self, frz):
        self.deps = deps.ThawDependencySet(frz)
        
    def twm(self, diff, base):
	if not diff: return False

        self.thaw(diff)
        return True

    def __eq__(self, other):
	return other.__class__ == self.__class__ and self.deps == other.deps

    def __init__(self, dep = ''):
        assert(type(dep) is str)
        self.deps = None
        self.thaw(dep)

class StringsStream(InfoStream):
    """
    Stores list of arbitrary strings
    """

    __slots__ = 'l'
    streamId = _STREAM_STRINGS

    def value(self):
	return self.l

    def set(self, val):
        assert(type(val) is str)
	if val not in self.l:
	    self.l.append(val)
	    self.l.sort()
    
    def __contains__(self, val):
	return val in self.l

    def __delitem__(self, val):
	if val in self.l:
	    self.l.remove(val)

    def merge(self, other):
        self.l = other.l

    def freeze(self):
        if self.l is None:
            return ''
        return '\0'.join(self.l)

    def diff(self, them):
	if self.l != them.l:
	    return self.freeze()
	return ''

    def thaw(self, frz):
	if len(frz) == 0:
	    self.l = []
	else:
	    self.l = frz.split('\0')
        
    def twm(self, diff, base):
	if not diff:
	    return False
        self.thaw(diff)
        return True

    def __eq__(self, other):
	return other.__class__ == self.__class__ and self.l == other.l

    def __init__(self, frz = ''):
	self.thaw(frz)

class TupleStream(InfoStream):

    __slots__ = "items"

    def __eq__(self, other):
	return other.__class__ == self.__class__ and other.items == self.items

    def freeze(self):
	rc = []
	items = self.items
	makeup = self.makeup
	for (i, (name, itemType, size)) in enumerate(makeup):
	    if type(size) == int or (i + 1 == len(makeup)):
		rc.append(items[i].freeze())
	    else:
		s = items[i].freeze()
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
			raise AssertionError

		conflicts = conflicts or \
		    self.items[i].twm(diff[idx:idx + size], base.items[i])
		idx += size

	return conflicts

    def thaw(self, s):
	items = []
	makeup = self.makeup
	idx = 0
	for (i, (name, itemType, size)) in enumerate(makeup):
	    if type(size) == int:
		items.append(itemType(s[idx:idx + size]))
	    elif (i + 1) == len(makeup):
		items.append(itemType(s[idx:]))
		size = 0
	    else:
		if size == "B":
		    size = struct.unpack("B", s[idx])[0]
		    idx += 1
		elif size == "!H":
		    size = struct.unpack("!H", s[idx:idx + 2])[0]
		    idx += 2
		else:
		    raise AssertionError

		items.append(itemType(s[idx:idx + size]))

	    idx += size

	self.items = items

    def __init__(self, first = None, *rest):
	if first == None:
	    items = []
	    for (i, (name, itemType, size)) in enumerate(self.makeup):
		items.append(itemType())
	    self.items = items
	elif type(first) == str and not rest:
	    self.thaw(first)
	else:
	    all = (first, ) + rest
	    items = []
	    for (i, (name, itemType, size)) in enumerate(self.makeup):
		items.append(itemType(all[i]))
	    self.items = items

class DeviceStream(TupleStream):

    __slots__ = []

    makeup = (("major", IntStream, 4), ("minor", IntStream, 4))
    streamId = _STREAM_DEVICE

    def major(self):
        return self.items[0].value()

    def setMajor(self, value):
        return self.items[0].set(value)

    def minor(self):
        return self.items[1].value()

    def setMinor(self, value):
        return self.items[1].set(value)

class RegularFileStream(TupleStream):

    __slots__ = []

    makeup = (("size", LongLongStream, 8), ("sha1", StringStream, 40))

    def size(self):
        return self.items[0].value()

    def setSize(self, value):
        return self.items[0].set(value)

    def sha1(self):
        return self.items[1].value()

    def setSha1(self, value):
        return self.items[1].set(value)
    
    streamId = _STREAM_SIZESHA1

class InodeStream(TupleStream):

    __slots__ = []

    """
    Stores basic inode information on a file: perms, owner, group.
    """

    # this is permissions, mtime, owner, group
    makeup = (("perms", ShortStream, 2), ("mtime", MtimeStream, 4), 
              ("owner", StringStream, "B"), ("group", StringStream, "B"))
    streamId = _STREAM_INODE

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

class FlagsStream(IntStream):

    __slots__ = "val"
    streamId = _STREAM_FLAGS

    def isConfig(self, set = None):
	return self._isFlag(_FILE_FLAG_CONFIG, set)

    def isInitScript(self, set = None):
	return self._isFlag(_FILE_FLAG_INITSCRIPT, set)

    def isShLib(self, set = None):
	return self._isFlag(_FILE_FLAG_SHLIB, set)

    def isGconfSchema(self, set = None):
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

class File(object):

    lsTag = None
    hasContents = 0
    streamList = ( ("inode", InodeStream),
                   ("flags", FlagsStream),
		   ("tags", StringsStream) )
    __slots__ = [ "theId", "inode", "flags", "tags" ]

    def modeString(self):
	l = self.inode.permsString()
	return self.lsTag + string.join(l, "")

    def timeString(self):
	return self.inode.timeString()

    def sizeString(self):
	return "       0"

    def copy(self):
	new = copy.deepcopy(self)
        for name, streamClass in self.streamList:
            stream = self.__getattribute__(name).copy()
            new.__setattr__(name, stream)
        return new

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

    def initializeStreams(self, data):
	if not data: 
	    for (name, streamType) in self.streamList:
		self.__setattr__(name, streamType())
	else:
	    # skip over the file type for now
	    i = 1
            dataLen = len(data)
	    for (name, streamType) in self.streamList:
                assert(i < dataLen)
		(streamId, size) = struct.unpack("!BH", data[i:i+3])
		assert(streamId == streamType.streamId)
		i += 3
		self.__setattr__(name, streamType(data[i:i + size]))
		i += size

	    # FIXME
	    #assert(i == len(data))

    def diff(self, other):
	if self.lsTag != other.lsTag:
	    d = self.freeze()
	    return struct.pack("!BH", 0, len(d)) + d

	rc = [ "\x01", self.lsTag ]
	for (name, streamType) in self.streamList:
	    d = self.__getattribute__(name).diff(other.__getattribute__(name))
	    rc.append(struct.pack("!H", len(d)) + d)

	return "".join(rc)

    def twm(self, diff, base, skip = None):
	sameType = struct.unpack("B", diff[0])
	if not sameType: 
	    # XXX file type changed -- we don't support this yet
	    raise AssertionError
	assert(self.lsTag == base.lsTag)
	assert(self.lsTag == diff[1])
	i = 2
	conflicts = False
	
	for (name, streamType) in self.streamList:
	    size = struct.unpack("!H", diff[i:i+2])[0]
	    i += 2
	    if name != skip:
		w = self.__getattribute__(name).twm(diff[i:i+size], 
					       base.__getattribute__(name))
	    i += size
	    conflicts = conflicts or w

	assert(i == len(diff))

	return conflicts

    def __eq__(self, other):
	if other.lsTag != self.lsTag: return False

	for (name, streamType) in self.streamList:
	    if not self.__getattribute__(name) == other.__getattribute__(name):
		return False

	return True

    def __ne__(self, other):
	return not self.__eq__(other)

    def metadataEqual(self, other, ignoreOwnerGroup):
	if not ignoreOwnerGroup:
	    return self == other

	for (name, streamType) in self.streamList:
	    if name == 'inode':
		if not self.__getattribute__(name).metadataEqual(
		       other.__getattribute__(name)):
		    return False
	    elif not self.__getattribute__(name) == other.__getattribute__(name):
		return False

	return True

    def freeze(self):
	rc = [ self.lsTag ]
	for (name, streamType) in self.streamList:
	    s = self.__getattribute__(name).freeze()
	    rc.append(struct.pack("!BH", streamType.streamId, len(s)) + s)
	return "".join(rc)

    def __init__(self, fileId, streamData = None):
        assert(self.__class__ is not File)
	self.theId = fileId
	self.initializeStreams(streamData)

class SymbolicLink(File):

    lsTag = "l"
    streamList = File.streamList + (("target", StringStream ),)
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

    def restore(self, root, fileContents, target, restoreContents):
	if not os.path.isdir(target):
	    util.mkdirChain(target)

	File.restore(self, root, target, restoreContents)

    def remove(self, target):
	raise NotImplementedError

class DeviceFile(File):

    streamList = File.streamList + (("devt", DeviceStream ),)
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

    streamList = File.streamList + (('contents', RegularFileStream),
                                    ('provides', DependenciesStream),
                                    ('requires', DependenciesStream),
                                    ('flavor', DependenciesStream))
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
	sha1 = sha1helper.hashFile(path)
	f.contents = RegularFileStream(s.st_size, sha1)

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
	cl = Socket
    elif type == "l":
	cl = SymbolicLink
    elif type == "p":
	cl = NamedPipe
    else:
	raise AssertionError

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
