DEP_CLASS_ABI	    = 0
DEP_CLASS_IS	    = 1
DEP_CLASS_SONAME    = 2
DEP_CLASS_FILES	    = 3
DEP_CLASS_TROVES    = 4

class Dependency:

    """
    Implements a single dependency. This is relative to a DependencyClass,
    which is part of a DependencySet. Multiple DependencySets make up a
    DependencyGroup, which keeps multiple versions of the same dependency
    as references saving (significant amounts of) memory. DependencySets
    can be frozen, but that frozen version can only be thawed relative to 
    the proper DependencyGroup (which can also be frozen).

    These are hashable, directly comparable, and implement a satisfies()
    method.
    """

    def __hash__(self):
	val = hash(self.main)
	for flag in self.flags.iterkeys():
	    val ^= hash(flag)
	return val
	
    def __eq__(self, other):
	return other.main == self.main and other.flags == self.flags

    def __str__(self):
	if self.flags:
	    return "%s(%s)" % (self.main, " ".join(self.flags.iterkeys()))
	else:
	    return self.main

    def satisfies(self, other):
	"""
	Returns whether or not this dependency satisfies the argument
	(which is a requires).

	@type other: Dependency
	"""
	if self.main != other.main: 
	    return False
	for flag in other.flags.iterkeys():
	    if not self.flags.has_key(flag): 
		return False

	return True

    def __init__(self, main, flags = []):
	self.main = main
	self.flags = {}
	for flags in flags:
	    self.flags[flags] = True

class DependencyClass:

    def addDep(self, dep):
	if self.members.has_key(dep):
	    return

	if not self.dgroup.has_key(dep):
	    self.dgroup[dep] = dep
	    grpDep = dep
	else:
	    grpDep = self.dgroup[dep]

	self.members[grpDep] = True

    def satisfies(self, other):
	if self.tag != other.tag:
	    return False

	for otherdep in other.members.iterkeys():
	    if not self.members.has_key(otherdep):
		# this optimizes if there is an exact match for the dependency,
		# which there normally is
		if self.exactMatch:
		    return False

		satisfied = False

		for dep in self.members.iterkeys():
		    if dep.satisfies(otherdep):
			satisfied = True
			break

		if not satisfied:
		    return False

	return True

    def __hash__(self):
	val = self.tag
	for dep in self.members.iterkeys():
	    val ^= hash(dep)

	return val

    def __eq__(self, other):
	return self.tag == other.tag and \
	       self.members == other.members

    def __str__(self):
	return "\n".join([ "%s: %s" % (self.tagName, dep) 
		    for dep in self.members.iterkeys() ])

    def __init__(self, dgroup):
	assert(self.__class__ != DependencyClass)
	self.dgroup = dgroup
	self.members = {}

class AbiDependency(DependencyClass):

    tag = DEP_CLASS_ABI
    tagName = "abi"
    exactMatch = True

class InstructionSetDependency(DependencyClass):

    tag = DEP_CLASS_IS
    tagName = "is"
    exactMatch = False

class SonameDependency(DependencyClass):

    tag = DEP_CLASS_SONAME
    tagName = "soname"
    exactMatch = True

class FilesDependencies(DependencyClass):

    tag = DEP_CLASS_FILES
    tagName = "file"
    exactMatch = True

class TrovesDependencies(DependencyClass):

    tag = DEP_CLASS_TROVES
    tagName = "trove"
    exactMatch = True

class DependencySet:

    def addDep(self, depClass, dep):
	assert(isinstance(dep, Dependency))

	tag = depClass.tag
	if not self.members.has_key(tag):
	    self.members[tag] = depClass(self.dgroup)

	self.members[tag].addDep(dep)

    def satisfies(self, other):
	for tag in other.members:
	    if not self.members.has_key(tag): 
		return False
	    if not self.members[tag].satisfies(other.members[tag]): 
		return False

	return True

    def __str__(self):
	return "\n".join([ str(x) for x in self.members.itervalues()])

    def __init__(self, dgroup):
	self.dgroup = dgroup
	self.members = {}

class DependencyGroup(dict):

    pass
