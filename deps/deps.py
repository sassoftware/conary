#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

DEP_CLASS_ABI	    = 0
DEP_CLASS_IS	    = 1
DEP_CLASS_SONAME    = 2
DEP_CLASS_FILES	    = 3
DEP_CLASS_TROVES    = 4

import util

class Dependency:

    """
    Implements a single dependency. This is relative to a DependencyClass,
    which is part of a DependencySet. Dependency Sets can be frozen and
    thawed.

    These are hashable, directly comparable, and implement a satisfies()
    method.
    """

    def __hash__(self):
	val = hash(self.name)
	for flag in self.flags.iterkeys():
	    val ^= hash(flag)
	return val
	
    def __eq__(self, other):
	return other.name == self.name and other.flags == self.flags

    def __str__(self):
	if self.flags:
	    return "%s(%s)" % (self.name, " ".join(self.flags.iterkeys()))
	else:
	    return self.name

    def freeze(self):
	if self.flags:
	    return "%s %s" % (self.name, " ".join(self.flags.iterkeys()))
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

    def __init__(self, name, flags = []):
	self.name = name
	if type(flags) == dict:
	    self.flags = flags
	else:
	    self.flags = {}
	    for flags in flags:
		self.flags[flags] = True

def ThawDependency(frozen):
    l = frozen.split(" ")
    d = Dependency(l[0], l[1:])
    if not dependencyCache.has_key(d):
	dependencyCache[d] = d

    return dependencyCache[d]

class DependencyClass:

    def addDep(self, dep):
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
	for otherdep in other.members.itervalues():
	    # calling this for duplicates is a noop
	    self.addDep(otherdep)

    def __hash__(self):
	val = self.tag
	for dep in self.members.itervalues():
	    val ^= hash(dep)

	return val

    def __eq__(self, other):
	return self.tag == other.tag and \
	       self.members == other.members

    def __str__(self):
	return "\n".join([ "%s: %s" % (self.tagName, dep) 
		    for dep in self.members.itervalues() ])

    def __init__(self):
	self.members = {}

class AbiDependency(DependencyClass):

    tag = DEP_CLASS_ABI
    tagName = "abi"
    exactMatch = True
    justOne = True

class InstructionSetDependency(DependencyClass):

    tag = DEP_CLASS_IS
    tagName = "is"
    exactMatch = False
    justOne = True

class SonameDependencies(DependencyClass):

    tag = DEP_CLASS_SONAME
    tagName = "soname"
    exactMatch = True
    justOne = False

class FileDependencies(DependencyClass):

    tag = DEP_CLASS_FILES
    tagName = "file"
    exactMatch = True
    justOne = False

class TroveDependencies(DependencyClass):

    tag = DEP_CLASS_TROVES
    tagName = "trove"
    exactMatch = True
    justOne = False

class DependencySet:

    def addDep(self, depClass, dep):
	assert(isinstance(dep, Dependency))

	tag = depClass.tag
	if not self.members.has_key(tag):
	    self.members[tag] = depClass()

	self.members[tag].addDep(dep)

    def satisfies(self, other):
	for tag in other.members:
	    if not self.members.has_key(tag): 
		return False
	    if not self.members[tag].satisfies(other.members[tag]): 
		return False

	return True

    def union(self, other):
	for tag in other.members:
	    if self.members.has_key(tag):
		self.members[tag].union(other.members[tag])
	    else:
		self.members[tag] = other.members[tag]

    def __str__(self):
	return "\n".join([ str(x) for x in self.members.itervalues()])

    def __init__(self):
	self.members = {}

dependencyCache = util.ObjectCache()
