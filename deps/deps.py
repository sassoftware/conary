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
import re
from lib import util

DEP_CLASS_ABI		= 0
DEP_CLASS_IS		= 1
DEP_CLASS_OLD_SONAME	= 2
DEP_CLASS_FILES		= 3
DEP_CLASS_TROVES	= 4
DEP_CLASS_USE		= 5
DEP_CLASS_SONAME	= 6

FLAG_SENSE_UNSPECIFIED  = 0         # used FlavorScore indices
FLAG_SENSE_REQUIRED     = 1
FLAG_SENSE_PREFERRED    = 2
FLAG_SENSE_PREFERNOT    = 3
FLAG_SENSE_DISALLOWED   = 4

DEP_MERGE_TYPE_NORMAL   = 1         # conflicts are reported
DEP_MERGE_TYPE_OVERRIDE = 2         # new data wins
DEP_MERGE_TYPE_PREFS    = 3         # like override, but !ssl beats out ~!ssl

senseMap = { FLAG_SENSE_REQUIRED   : "",
             FLAG_SENSE_PREFERRED  : "~",
             FLAG_SENSE_PREFERNOT  : "~!",
             FLAG_SENSE_DISALLOWED : "!" }

senseReverseMap = {}
for key, val in senseMap.iteritems():
    senseReverseMap[val] = key

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
	    flags = self.flags.items()
	    flags.sort()
	    return "%s(%s)" % (self.name, 
                    " ".join([ "%s%s" % (senseMap[x[1]], x[0]) for x in flags]))
	else:
	    return self.name

    def freeze(self):
	if self.flags:
	    flags = self.flags.items()
	    flags.sort()
	    return "%s:%s" % (self.name, 
                    ",".join([ "%s%s" % (senseMap[x[1]], x[0]) for x in flags]))
	else:
	    return self.name

    def score(self, required):
        """
        Returns a flavor matching score. This dependency is considered
        the "system" and the other is the flavor of the trove. In terms
        of dependencies, this set "provides" and the other "requires".

        False is returned if the two dependencies conflict.
        """
	if self.name != required.name: 
            return False

        score = 0
	for (requiredFlag, requiredSense) in required.flags.iteritems():
            thisSense = self.flags.get(requiredFlag, FLAG_SENSE_UNSPECIFIED)
            thisScore = flavorScores[(thisSense, requiredSense)]
            if thisScore is None:
                return False
            score += thisScore

        return score

    def satisfies(self, required):
	"""
	Returns whether or not this dependency satisfies the argument
	(which is a requires).

	@type required: Dependency
	"""
        return self.score(required) is not False

    def mergeFlags(self, other, mergeType = DEP_MERGE_TYPE_NORMAL):
	"""
	Returns a new Dependency which merges the flags from the two
	existing dependencies. We don't want to merge in place as this
	Dependency could be shared between many objects (via a 
	DependencyGroup).  Always pick an absolute flavor over a preference:
        e.g. when merging a set of flags with a ~foo and !foo, 
        make the merged flavor !foo.  
	"""
	allFlags = self.flags.copy()
        for (flag, otherSense) in other.flags.iteritems():
            if mergeType == DEP_MERGE_TYPE_PREFS and allFlags.has_key(flag) \
                    and otherSense == FLAG_SENSE_PREFERNOT \
                    and allFlags[flag] == FLAG_SENSE_DISALLOWED:
                allFlags[flag] = FLAG_SENSE_DISALLOWED
                continue
            elif mergeType == DEP_MERGE_TYPE_OVERRIDE or \
                 mergeType == DEP_MERGE_TYPE_PREFS    or \
                        not allFlags.has_key(flag):
                allFlags[flag] = otherSense
                continue

            thisSense = allFlags[flag]

            if thisSense == otherSense:
                # same flag, same sense
                continue

            if ((thisSense == FLAG_SENSE_REQUIRED and 
                        otherSense == FLAG_SENSE_DISALLOWED) or
                (thisSense == FLAG_SENSE_DISALLOWED and
                        otherSense == FLAG_SENSE_REQUIRED)   or
                (thisSense == FLAG_SENSE_PREFERRED and 
                        otherSense == FLAG_SENSE_PREFERNOT) or
                (thisSense == FLAG_SENSE_PREFERNOT and
                        otherSense == FLAG_SENSE_PREFERRED)):
                thisFlag = "%s%s" % (senseMap[thisSense], flag)
                otherFlag = "%s%s" % (senseMap[otherSense], flag)
                raise RuntimeError, ("Invalid flag combination in merge:"
                                     " %s and %s"  % (thisFlag, otherFlag))

            # know they aren't the same, and they are compatible
            if thisSense == FLAG_SENSE_REQUIRED or \
                    thisSense == FLAG_SENSE_DISALLOWED:
                continue
            elif otherSense == FLAG_SENSE_REQUIRED or \
                    otherSense == FLAG_SENSE_DISALLOWED:
                allFlags[flag] = otherSense
                continue

            # we shouldn't end up here
            assert(0)

        return Dependency(self.name, allFlags)

    def getName(self):
        return (self.name,)

    def getFlags(self):
        return (self.flags.items(),)

    def __init__(self, name, flags = []):
	self.name = name
	if type(flags) == dict:
	    self.flags = flags
	else:
	    self.flags = {}
	    for (flag, sense) in flags:
		self.flags[flag] = sense

class DependencyClass:

    def addDep(self, dep, mergeType = DEP_MERGE_TYPE_NORMAL):
        assert(dep.__class__ == self.depClass)

	if self.members.has_key(dep.name):
	    # this is a little faster then doing all of the work when
	    # we could otherwise avoid it
	    if dep == self.members[dep.name]: return

	    # merge the flags, and add the newly created dependency
	    # into the class
	    dep = self.members[dep.name].mergeFlags(dep, mergeType = mergeType)
	    del self.members[dep.name]

	if not dependencyCache.has_key(dep):
	    dependencyCache[dep] = dep
	    grpDep = dep
	else:
	    grpDep = dependencyCache[dep]

	self.members[grpDep.name] = grpDep
	assert(not self.justOne or len(self.members) == 1)

    def score(self, requirements):
	if self.tag != requirements.tag:
	    return False
        
        score = 0
	for requiredDep in requirements.members.itervalues():
	    if not self.members.has_key(requiredDep.name):
                return False

            thisScore = self.members[requiredDep.name].score(requiredDep)
            if thisScore is False:
                return False

            score += thisScore

        return thisScore

    def satisfies(self, requirements):
        return self.score(requirements) is not False

    def union(self, other, mergeType = DEP_MERGE_TYPE_NORMAL):
	if other is None: return
	for otherdep in other.members.itervalues():
	    # calling this for duplicates is a noop
	    self.addDep(otherdep, mergeType = mergeType)

    def getDeps(self):
        l = self.members.items()
        # sort by name
        l.sort()
        for name, dep in l:
            yield dep

    def thawDependency(frozen):
        l = frozen.split(":")
        flags = []
        if len(l) > 1:
            flags = l[1].split(',')

        for i, flag in enumerate(flags):
            kind = flag[0:2]

            if kind == '~!':
                flags[i] = (flag[2:], FLAG_SENSE_PREFERNOT)
            elif kind[0] == '!':
                flags[i] = (flag[1:], FLAG_SENSE_DISALLOWED)
            elif kind[0] == '~':
                flags[i] = (flag[1:], FLAG_SENSE_PREFERRED)
            else:
                flags[i] = (flag, FLAG_SENSE_REQUIRED)

        d = Dependency(l[0], flags)
        if not dependencyCache.has_key(d):
            dependencyCache[d] = d

        return dependencyCache[d]
    thawDependency = staticmethod(thawDependency)

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
    justOne = False
    depClass = Dependency
_registerDepClass(AbiDependency)

class InstructionSetDependency(DependencyClass):

    tag = DEP_CLASS_IS
    tagName = "is"
    justOne = False
    depClass = Dependency
_registerDepClass(InstructionSetDependency)

class OldSonameDependencies(DependencyClass):

    tag = DEP_CLASS_OLD_SONAME
    tagName = "oldsoname"
    justOne = False
    depClass = Dependency
_registerDepClass(OldSonameDependencies)

class SonameDependencies(DependencyClass):

    tag = DEP_CLASS_SONAME
    tagName = "soname"
    justOne = False
    depClass = Dependency
_registerDepClass(SonameDependencies)

class FileDependencies(DependencyClass):

    tag = DEP_CLASS_FILES
    tagName = "file"
    justOne = False
    depClass = Dependency
_registerDepClass(FileDependencies)

class TroveDependencies(DependencyClass):

    tag = DEP_CLASS_TROVES
    tagName = "trove"
    justOne = False
    depClass = Dependency

    def thawDependency(frozen):
        d = Dependency(frozen, [])
        if not dependencyCache.has_key(d):
            dependencyCache[d] = d

        return dependencyCache[d]
    thawDependency = staticmethod(thawDependency)

_registerDepClass(TroveDependencies)

class UseDependency(DependencyClass):

    tag = DEP_CLASS_USE
    tagName = "use"
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

    def copy(self):
        return copy.deepcopy(self)

    def score(self,other):
        score = 0
	for tag in other.members:
	    if not self.members.has_key(tag): 
		return False

	    thisScore = self.members[tag].score(other.members[tag])
            if thisScore is False:
		return False

            score += thisScore

        return score

    def satisfies(self, other):
        return self.score(other) is not False

    def getDepClasses(self):
        return self.members

    def union(self, other, mergeType = DEP_MERGE_TYPE_NORMAL):
        if not other:
            return

	for tag in other.members:
	    if self.members.has_key(tag):
		self.members[tag].union(other.members[tag],
                                        mergeType = mergeType)
	    else:
		self.members[tag] = copy.deepcopy(other.members[tag])

    def __eq__(self, other):
        if other is None:
            return False
        if other.members.keys() != self.members.keys():
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
        depClass = dependencyClasses[tag]
        depSet.addDep(depClass, depClass.thawDependency(frozen))
    return depSet

def formatFlavor(flavor):
    """
    Formats a flavor and returns a string which parseFlavor can 
    handle.
    """
    def _singleClass(depClass):
        dep = depClass.getDeps().next()

        flags = dep.getFlags()[0]

	if flags:
	    flags.sort()
	    return "%s(%s)" % (dep.getName()[0],
                    ",".join([ "%s%s" % (senseMap[x[1]], x[0]) 
                                            for x in flags]))
	else:
	    return dep.getName()[0]

    classes = flavor.getDepClasses()
    insSet = classes.get(DEP_CLASS_IS, None)
    useFlags = classes.get(DEP_CLASS_USE, None)

    if insSet is not None:
        insSet = _singleClass(insSet)

    if useFlags is not None:
        # strip the use() bit
        useFlags = _singleClass(useFlags)[4:-1]

    if insSet and useFlags:
        return "%s is: %s" % (useFlags, insSet)
    elif insSet:
        return "is: %s" % insSet
    elif useFlags:
        return useFlags

    return ""

def parseFlavor(s, mergeBase = None):
    # return a DependencySet for the string passed. format is
    # [arch[(flag,[flag]*)]] [use:flag[,flag]*]
    #
    # if mergeBase is set, the parsed flavor is merged into it. The
    # rules for the merge are different than those for union() though;
    # the parsed flavor is assumed to set the is:, use:, or both. If
    # either class is unset, it's taken from mergeBase.

    def _fixup(flag):
        flag = flag.strip()
        if senseReverseMap.has_key(flag[0:2]):
            sense = senseReverseMap[flag[0:2]]
            flag = flag[2:]
        elif senseReverseMap.has_key(flag[0]):
            sense = senseReverseMap[flag[0]]
            flag = flag[1:]
        else:
            sense = FLAG_SENSE_REQUIRED

        return (flag, sense)

    s = s.strip()

    needsInsSet = True
    needsUse = True

    match = flavorRegexp.match(s)
    if not match:
        return None

    groups = match.groups()

    set = DependencySet()

    baseInsSet = groups[3]
    if baseInsSet:
        needsInsSet = False
        if groups[4]:
            insSetFlags = groups[4].split(",")
            for i, flag in enumerate(insSetFlags):
                insSetFlags[i] = _fixup(flag)
        else:
            insSetFlags = []

        set.addDep(InstructionSetDependency, Dependency(baseInsSet, 
                                                        insSetFlags))
    elif groups[2]:
        needsInsSet = False

    if groups[1]:
        needsUse = False
        useFlags = groups[1].split(",")
        for i, flag in enumerate(useFlags):
            useFlags[i] = _fixup(flag)

        set.addDep(UseDependency, Dependency("use", useFlags))
    elif groups[0]:
        needsUse = False

    if needsInsSet and mergeBase:
        insSet = mergeBase.getDepClasses().get(DEP_CLASS_IS, None)
        if insSet is not None:
            insSet = insSet.getDeps().next()
            set.addDep(InstructionSetDependency, insSet)

    if needsUse and mergeBase:
        useSet = mergeBase.getDepClasses().get(DEP_CLASS_USE, None)
        if useSet is not None:
            useSet = useSet.getDeps().next()
            set.addDep(UseDependency, useSet)

    return set

dependencyCache = util.ObjectCache()

ident = '(?:[_A-Za-z][0-9A-Za-z_]*)'
flag = '(?:~?!?IDENT)'
useFlag = '(?:!|~!)?FLAG(?:\.IDENT)?'
archClause = '(is:) *(?:(IDENT)(?:\(( *FLAG(?: *, *FLAG)*)\))?)?'
useClause = '(USEFLAG *(?:, *USEFLAG)*)? *'
exp = '^(use:)? *(?:USECLAUSE)? *(?:ARCHCLAUSE)?$'

exp = exp.replace('ARCHCLAUSE', archClause)
exp = exp.replace('USECLAUSE', useClause)
exp = exp.replace('USEFLAG', useFlag)
exp = exp.replace('FLAG', flag)
exp = exp.replace('IDENT', ident)

flavorRegexp = re.compile(exp)

del ident, flag, useFlag, archClause, useClause, exp

# None means disallowed match
flavorScores = {
      (FLAG_SENSE_UNSPECIFIED, FLAG_SENSE_REQUIRED ) : None,
      (FLAG_SENSE_UNSPECIFIED, FLAG_SENSE_DISALLOWED):    0,
      (FLAG_SENSE_UNSPECIFIED, FLAG_SENSE_PREFERRED) :   -1,
      (FLAG_SENSE_UNSPECIFIED, FLAG_SENSE_PREFERNOT) :    1,

      (FLAG_SENSE_REQUIRED,    FLAG_SENSE_REQUIRED ) :    2,
      (FLAG_SENSE_REQUIRED,    FLAG_SENSE_DISALLOWED): None,
      (FLAG_SENSE_REQUIRED,    FLAG_SENSE_PREFERRED) :    1,
      (FLAG_SENSE_REQUIRED,    FLAG_SENSE_PREFERNOT) : None,

      (FLAG_SENSE_DISALLOWED,  FLAG_SENSE_REQUIRED ) : None,
      (FLAG_SENSE_DISALLOWED,  FLAG_SENSE_DISALLOWED):    2,
      (FLAG_SENSE_DISALLOWED,  FLAG_SENSE_PREFERRED) : None,
      (FLAG_SENSE_DISALLOWED,  FLAG_SENSE_PREFERNOT) :    1,

      (FLAG_SENSE_PREFERRED,   FLAG_SENSE_REQUIRED ) :    1,
      (FLAG_SENSE_PREFERRED,   FLAG_SENSE_DISALLOWED): None,
      (FLAG_SENSE_PREFERRED,   FLAG_SENSE_PREFERRED) :    2,
      (FLAG_SENSE_PREFERRED,   FLAG_SENSE_PREFERNOT) :   -1,

      (FLAG_SENSE_PREFERNOT,   FLAG_SENSE_REQUIRED ) :   -2,
      (FLAG_SENSE_PREFERNOT,   FLAG_SENSE_DISALLOWED):    1,
      (FLAG_SENSE_PREFERNOT,   FLAG_SENSE_PREFERRED) :   -1,
      (FLAG_SENSE_PREFERNOT,   FLAG_SENSE_PREFERNOT) :    1 
}
