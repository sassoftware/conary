#
# Copyright (c) 2004-2005 Specifix, Inc.
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
"""
Defines the datastreams stored in a changeset
"""

import copy
import struct
import versions

from deps import deps

from lib import cstreams
IntStream = cstreams.IntStream
ShortStream = cstreams.ShortStream
StringStream = cstreams.StringStream
StreamSet = cstreams.StreamSet
LargeStreamSet = cstreams.LargeStreamSet
StreamSetDef = cstreams.StreamSetDef

class InfoStream(object):

    __slots__ = ()

    def __deepcopy__(self, mem):
        return self.__class__(self.freeze())

    def copy(self):
        return self.__class__(self.freeze())
    
    def freeze(self, skipSet = None):
	raise NotImplementedError
    
    def diff(self, them):
	raise NotImplementedError

    def twm(self, diff, base):
	"""
	Performs a three way merge. Base is the original information,
	diff is one of the changes, and self is the (already changed)
	object. Returns a boolean saying whether or not the merge failed
	"""
	raise NotImplementedError

    def __eq__(self, them, skipSet = None):
	raise NotImplementedError

    def __ne__(self, them):
	return not self.__eq__(them)

class NumericStream(InfoStream):

    __slots__ = "val"

    def __deepcopy__(self, mem):
        return self.__class__.thaw(self, self.freeze())

    def __call__(self):
	return self.val

    def set(self, val):
	self.val = val

    def freeze(self, skipSet = None):
        if self.val is None:
            return ""

	return struct.pack(self.format, self.val)

    def diff(self, them):
	if self.val != them.val:
            if self.val is None:
                return ''
	    return struct.pack(self.format, self.val)

	return None

    def thaw(self, frz):
        if frz == "":
            self.val = None
        else:
            self.val = struct.unpack(self.format, frz)[0]

    def twm(self, diff, base):
        if diff == '':
            newVal = None
        else:
            newVal = struct.unpack(self.format, diff)[0]
	if self.val == base.val:
	    self.val = newVal
	    return False
	elif self.val != newVal:
	    return True

	return False

    def __eq__(self, other, skipSet = None):
	return other.__class__ == self.__class__ and \
	       self.val == other.val

    def __init__(self, val = None):
	if type(val) == str:
	    self.thaw(val)
	else:
	    self.val = val

class ByteStream(NumericStream):

    format = "!B"

class MtimeStream(NumericStream):

    format = "!I"

    def __eq__(self, other, skipSet = None):
	# don't ever compare mtimes
	return True

    def twm(self, diff, base):
	# and don't let merges fail
	NumericStream.twm(self, diff, base)
	return False

class LongLongStream(NumericStream):

    format = "!Q"

class Md5Stream(StringStream):

    def freeze(self, skipSet = None):
	assert(len(self()) == 16)
	return self()

    def thaw(self, data):
	if data:
	    assert(len(data) == 16)
	    self.set(data)

    def twm(self, diff, base):
	assert(len(diff) == 16)
	assert(len(base()) == 16)
	assert(len(self()) == 16)
	StringStream.twm(self, diff, base)

    def set(self, val):
	assert(len(val) == 16)
        StringStream.set(val)

    def setFromString(self, val):
	s = struct.pack("!4I", int(val[ 0: 8], 16), 
			       int(val[ 8:16], 16), int(val[16:24], 16), 
			       int(val[24:32], 16))
        StringStream.set(s)

class Sha1Stream(StringStream):

    def freeze(self, skipSet = None):
	assert(len(self()) == 20)
	return StringStream.freeze(self, skipSet = skipSet)

    def twm(self, diff, base):
	assert(len(diff) == 20)
	assert(len(base()) == 20)
	assert(len(self()) == 20)
	StringStream.twm(self, diff, base)

    def set(self, val):
	assert(len(val) == 20)
        StringStream.set(self, val)

    def setFromString(self, val):
	s = struct.pack("!5I", int(val[ 0: 8], 16), 
			       int(val[ 8:16], 16), int(val[16:24], 16), 
			       int(val[24:32], 16), int(val[32:40], 16))
        StringStream.set(self, val)

class FrozenVersionStream(InfoStream):

    __slots__ = "v"

    def __call__(self):
	return self.v

    def set(self, val):
	assert(not val or min(val.timeStamps()) > 0)
	self.v = val

    def freeze(self, skipSet = {}):
        # If versionStrings is in skipSet, freeze the string w/o the timestamp.
        # This is a bit of a hack to allow trove signatures to exclude
        # the timestamps. Since those timestamps are reset on the server
        # at commit time, including them would make signing the to-be-committed
        # changeset impossible.
	if not self.v:
	    return ""
        elif skipSet and 'versionStrings' in skipSet:
	    return self.v.asString()
	else:
	    return self.v.freeze()

    def diff(self, them):
	if self.v != them.v:
	    return self.v.freeze()

	return None

    def thaw(self, frz):
	if frz:
	    self.v = versions.ThawVersion(frz)
	else:
	    self.v = None

    def twm(self, diff, base):
	if self.v == base.v:
	    self.v = diff
	    return False
	elif self.v != diff:
	    return True

	return False

    def __eq__(self, other, skipSet = None):
	return other.__class__ == self.__class__ and \
	       self.v == other.v

    def __init__(self, v = None):
	self.thaw(v)

class DependenciesStream(InfoStream):
    """
    Stores list of strings; used for requires/provides lists
    """

    __slots__ = 'deps'

    def __call__(self):
	return self.deps

    def set(self, val):
        assert(val is not None)
	self.deps = val

    def freeze(self, skipSet = None):
        if self.deps is None:
            return ''
        return self.deps.freeze()

    def diff(self, them):
	if self.deps != them.deps:
	    return self.freeze()

	return None

    def thaw(self, frz):
        self.deps = deps.ThawDependencySet(frz)
        
    def twm(self, diff, base):
        self.thaw(diff)
        return False

    def __eq__(self, other, skipSet = None):
	return other.__class__ == self.__class__ and self.deps == other.deps

    def __init__(self, dep = ''):
        assert(type(dep) is str)
        self.thaw(dep)

class StringsStream(list, InfoStream):
    """
    Stores list of arbitrary strings
    """

    def set(self, val):
	assert(type(val) is str)
	if val not in self:
	    self.append(val)
	    self.sort()

    def __eq__(self, other, skipSet = None):
        return list.__eq__(self, other)

    def freeze(self, skipSet = None):
        if not self:
            return ''
        return '\0'.join(self)

    def diff(self, them):
	if self != them:
	    return self.freeze()
	return None

    def thaw(self, frz):
	del self[:]

	if len(frz) != 0:
	    for s in frz.split('\0'):
		self.set(s)

    def twm(self, diff, base):
        self.thaw(diff)
        return False

    def __init__(self, frz = ''):
	self.thaw(frz)

class ReferencedTroveList(list, InfoStream):

    def freeze(self, skipSet = None):
	l = []
	for (name, version, flavor) in self:
	    version = version.freeze()
	    if flavor:
		flavor = flavor.freeze()
	    else:
		flavor = ""

	    l.append(name)
	    l.append(version)
	    l.append(flavor)

	return "\0".join(l)

    def thaw(self, data):
	del self[:]
	if not data: return

	l = data.split("\0")
	i = 0

	while i < len(l):
	    name = l[i]
	    version = versions.ThawVersion(l[i + 1])
	    flavor = l[i + 2]

            flavor = deps.ThawDependencySet(flavor)

	    self.append((name, version, flavor))
	    i += 3

    def __init__(self, data = None):
	list.__init__(self)
	if data is not None:
	    self.thaw(data)

class UnknownStream(Exception):

    pass
