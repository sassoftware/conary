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
from lib import util

DEP_CLASS_ABI		= 0
DEP_CLASS_IS		= 1
DEP_CLASS_OLD_SONAME	= 2
DEP_CLASS_FILES		= 3
DEP_CLASS_TROVES	= 4
DEP_CLASS_USE		= 5
DEP_CLASS_SONAME	= 6

dependencyClasses = {}

def _registerDepClass(classObj):
    global dependencyClasses
    dependencyClasses[classObj.tag] = classObj

class BaseDependency:
    """
    Implements a single dependency. This is relative to a DependencyClass,
    which is part of a DependencySet. Dependency Sets can be frozen and
    thawed.

    These are hashable, directly comparable, and implement a satisfies()
    method.
    """

    def __hash__(self):
        raise NotImplementedError

    def __eq__(self, other):
        raise NotImplementedError

    def __str__(self):
        raise NotImplementedError

    def freeze(self):
        raise NotImplementedError

    def satisfies(self, required):
        raise NotImplementedError

    def mergeFlags(self, other):
        raise NotImplementedError

    def getName(self):
        raise NotImplementedError

    def getFlags(self):
        raise NotImplementedError

    def __init__(self):
        raise NotImplementedError

class Dependency(BaseDependency):

    def __hash__(self):
	val = hash(self.name)
	for flag in self.flags.iterkeys():
	    val ^= hash(flag)
	return val
	
    def __eq__(self, other):
	return other.name == self.name and other.flags == self.flags

    def __str__(self):
	if self.flags:
	    flags = self.flags.keys()
	    flags.sort()
	    return "%s(%s)" % (self.name, " ".join(flags))
	else:
	    return self.name

    def freeze(self):
	if self.flags:
	    flags = self.flags.keys()
	    flags.sort()
	    return "%s:%s" % (self.name, ",".join(flags))
	else:
	    return self.name

    def satisfies(self, required):
	"""
	Returns whether or not this dependency satisfies the argument
	(which is a requires).

	@type required: Dependency
	"""
	if self.name != required.name: 
	    return False
	for requiredFlag in required.flags.iterkeys():
	    if not self.flags.has_key(requiredFlag): 
		return False

	return True

    def mergeFlags(self, other):
	"""
	Returns a new Dependency which merges the flags from the two
	existing dependencies. We don't want to merge in place as this
	Dependency could be shared between many objects (via a 
	DependencyGroup)
	"""
	allFlags = self.flags.copy()
	allFlags.update(other.flags)

	return Dependency(self.name, allFlags)

    def getName(self):
        return (self.name,)

    def getFlags(self):
        return (self.flags.keys(),)

    def __init__(self, name, flags = []):
	self.name = name
	if type(flags) == dict:
	    self.flags = flags
	else:
	    self.flags = {}
	    for flags in flags:
		self.flags[flags] = True

def ThawDependency(frozen):
    l = frozen.split(":")
    flags = []
    if len(l) > 1:
        flags = l[1].split(',')
    d = Dependency(l[0], flags)
    if not dependencyCache.has_key(d):
	dependencyCache[d] = d

    return dependencyCache[d]

class DependencyClass:

    def addDep(self, dep):
        assert(dep.__class__ == self.depClass)

	if self.members.has_key(dep.name):
	    # this is a little faster then doing all of the work when
	    # we could otherwise avoid it
	    if dep == self.members[dep.name]: return

	    # merge the flags, and add the newly created dependency
	    # into the class
	    dep = self.members[dep.name].mergeFlags(dep)
	    del self.members[dep.name]

	if not dependencyCache.has_key(dep):
	    dependencyCache[dep] = dep
	    grpDep = dep
	else:
	    grpDep = dependencyCache[dep]

	self.members[grpDep.name] = grpDep
	assert(not self.justOne or len(self.members) == 1)

    def satisfies(self, requirements):
	if self.tag != requirements.tag:
	    return False

	for requiredDep in requirements.members.itervalues():
	    if not self.members.has_key(requiredDep.name) or \
	       not self.members[requiredDep.name].satisfies(requiredDep):
		return False

	return True

    def union(self, other):
	if other is None: return
	for otherdep in other.members.itervalues():
	    # calling this for duplicates is a noop
	    self.addDep(otherdep)

    def getDeps(self):
        for name, dep in self.members.iteritems():
            yield dep

    def __hash__(self):
	val = self.tag
	for dep in self.members.itervalues():
	    val ^= hash(dep)

	return val

    def __eq__(self, other):
        if other is None:
            return False
	return self.tag == other.tag and \
	       self.members == other.members

    def __ne__(self, other):
        return not self == other

    def __str__(self):
	memberList = self.members.items()
	memberList.sort()
	return "\n".join([ "%s: %s" % (self.tagName, dep[1]) 
		    for dep in memberList ])

    def __init__(self):
	self.members = {}

class AbiDependency(DependencyClass):

    tag = DEP_CLASS_ABI
    tagName = "abi"
    exactMatch = True
    justOne = True
    depClass = Dependency
_registerDepClass(AbiDependency)

class InstructionSetDependency(DependencyClass):

    tag = DEP_CLASS_IS
    tagName = "is"
    exactMatch = True
    justOne = True
    depClass = Dependency
_registerDepClass(InstructionSetDependency)

class OldSonameDependencies(DependencyClass):

    tag = DEP_CLASS_OLD_SONAME
    tagName = "oldsoname"
    exactMatch = True
    justOne = False
    depClass = Dependency
_registerDepClass(OldSonameDependencies)

class SonameDependencies(DependencyClass):

    tag = DEP_CLASS_SONAME
    tagName = "soname"
    exactMatch = True
    justOne = False
    depClass = Dependency
_registerDepClass(SonameDependencies)

class FileDependencies(DependencyClass):

    tag = DEP_CLASS_FILES
    tagName = "file"
    exactMatch = True
    justOne = False
    depClass = Dependency
_registerDepClass(FileDependencies)

class TroveDependencies(DependencyClass):

    tag = DEP_CLASS_TROVES
    tagName = "trove"
    exactMatch = True
    justOne = False
    depClass = Dependency
_registerDepClass(TroveDependencies)

class UseDependency(DependencyClass):

    tag = DEP_CLASS_USE
    tagName = "use"
    # XXX this is a hack to avoid throwing out troves in the repos that
    # have a Use flag flavor.
    exactMatch = False
    justOne = True
    depClass = Dependency
_registerDepClass(UseDependency)

class DependencySet:

    def addDep(self, depClass, dep):
	assert(isinstance(dep, Dependency))

	tag = depClass.tag
	if not self.members.has_key(tag):
	    self.members[tag] = depClass()

	self.members[tag].addDep(dep)

    def satisfies(self, other):
	for tag in other.members:
            # XXX might not be the right semantic for exactMatch
            if not other.members[tag].exactMatch:
                continue
	    if not self.members.has_key(tag): 
		return False
	    if not self.members[tag].satisfies(other.members[tag]): 
		return False

	return True

    def getDepClasses(self):
        return self.members

    def union(self, other):
        if not other:
            return

	for tag in other.members:
	    if self.members.has_key(tag):
		self.members[tag].union(other.members[tag])
	    else:
		self.members[tag] = copy.deepcopy(other.members[tag])

    def __eq__(self, other):
        if other is None:
            return False
	for tag in other.members:
	    if not self.members.has_key(tag): 
		return False
	    if not self.members[tag] == other.members[tag]:
		return False

	return True

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
	h = 0
	for member in self.members.itervalues():
	    h ^= hash(member)
	return h

    def __nonzero__(self):
	return not(not(self.members))

    def __str__(self):
	memberList = self.members.items()
	memberList.sort()
	return "\n".join([ str(x[1]) for x in memberList])

    def freeze(self):
        rc = []
        for tag, depclass in self.getDepClasses().items():
            for dep in depclass.getDeps():
                rc.append('%d#%s' %(tag, dep.freeze()))
        return '|'.join(rc)

    def __init__(self):
	self.members = {}

def ThawDependencySet(frz):
    depSet = DependencySet()
    if frz == 'none' or frz is None:
        return None
    l = frz.split('|')
    for line in l:
        if not line:
            continue
        tag, frozen = line.split('#', 1)
        tag = int(tag)
        depSet.addDep(dependencyClasses[tag], ThawDependency(frozen))
    return depSet

dependencyCache = util.ObjectCache()
