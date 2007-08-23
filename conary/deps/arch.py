#
# Copyright (c) 2004-2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

from conary.deps import deps
import os

def x86flags(archTag, baseArch, extraFlags, ofInterest):
    try:
        lines = open("/proc/cpuinfo").read().split("\n")
    except IOError:
        lines=[]

    rc = [ (x, deps.FLAG_SENSE_PREFERRED) for x in extraFlags ]

    for line in lines:
	if not line.startswith("flags"): continue
	fields = line.split()
	if fields[0] != "flags": continue

	for flag in fields[2:]:
	    if ofInterest.has_key(flag): 
                rc.append((flag, deps.FLAG_SENSE_PREFERRED))

	return deps.Dependency(archTag, rc)

    return deps.Dependency(archTag)

def flags_ix86(baseArch):
    baseFlagMap = [ 'i686', 'i586', 'i486' ]
    i = baseFlagMap.index(baseArch)

    ofInterest = {}.fromkeys([ '3dnow', '3dnowext', 'mmx', 'mmxext', 'sse', 
                               'sse2', 'sse3', 'cmov', 'nx'])
    return [ [ x86flags('x86', baseArch, baseFlagMap[i:], ofInterest) ] ]

def flags_i686():
    return flags_ix86(baseArch = 'i686')

def flags_i586():
    return flags_ix86(baseArch = 'i586')

def flags_mips64():
    return [[ deps.Dependency('mipseb', [ ('mips64', deps.FLAG_SENSE_REQUIRED) ]) ]]

def flags_x86_64():
    baseFlagMap = [ ]
    ofInterest = {}.fromkeys([ '3dnow', '3dnowext', 'nx', 'sse3' ])

    x86_64 = x86flags('x86_64', baseArch, baseFlagMap, ofInterest)
    multiarch = flags_i686()
    multiarch[0].append(x86_64)
    return multiarch

def current():
    return currentArch

def any():
    return "any"

def canInstall(other):
    return other == "any" or other == currentArch

def initializeArch():
    global currentArch
    localNamespace = globals()
    if localNamespace.has_key("flags_" + baseArch):
        currentArch = localNamespace["flags_" + baseArch]()

    del localNamespace

baseArch = os.uname()[4]
currentArch = [[ deps.Dependency(baseArch) ]]
initializeArch()

class FlavorPreferences:
    @staticmethod
    def _getCurrentArchIS(arch):
        # Returns just the name of the current arch
        return ' '.join(sorted(dep.name for dep in arch[0]))

    # The flavor preferences table is keyed on the current arch
    flavorPreferences = {
        'alpha'         : ['is: alpha'],
        'ppc'           : ['is: ppc'],
        'ppc64'         : ['is: ppc ppc64', 'is: ppc64', 'is: ppc'],
        'ppc ppc64'     : ['is: ppc ppc64', 'is: ppc64', 'is: ppc'],
        's390'          : ['is: s390'],
        'sparc'         : ['is: sparc'],
        'sparc64'       : ['is: sparc sparc64', 'is: sparc64', 'is: sparc'],
        'sparc sparc64' : ['is: sparc sparc64', 'is: sparc64', 'is: sparc'],
        'x86'           : ['is: x86'],
        'x86_64'        : ['is: x86 x86_64', 'is: x86_64', 'is: x86', ],
        'x86 x86_64'    : ['is: x86 x86_64', 'is: x86_64', 'is: x86', ],
    }

    @staticmethod
    def getStringFlavorPreferences(arch):
        key = FlavorPreferences._getCurrentArchIS(arch)
        return FlavorPreferences.flavorPreferences.get(key, [])

    @staticmethod
    def getFlavorPreferences(arch):
        return [ deps.parseFlavor(x)
            for x in FlavorPreferences.getStringFlavorPreferences(arch)
        ]

def getFlavorPreferences(arch = currentArch):
    return FlavorPreferences.getFlavorPreferences(arch)

class IncompatibleInstructionSets(Exception):
    def __init__(self, is1, is2):
        Exception.__init__(self)
        self.is1 = is1
        self.is2 = is2

    def __str__(self):
        return "Incompatible architectures: %s: %s" % (self.is1, self.is2)

def getMajorArch(depList):
    """Return the major architecture from an instruction set dependency
    @type depList: list (iterable) of Dependency objects
    @param depGroupL a list (iterable) of Dependency objects.
    @raise IncompatibleInstructionSets: when incompatible architectures are
           present in the list.
    @rtype: instance of Dependency, or None
    @return: major architecture from the list, or None if the list is empty
    """

    # Compare instruction sets by looking at the flavor preferences -
    # the major architecture should be a superset of all other arches
    majorArch = None
    archTableSet = set([])
    for dep in depList:
        prefs = getFlavorPreferences([[dep]])
        prefsSet = set(prefs)
        if archTableSet.issubset(prefsSet):
            archTableSet = set(prefs)
            majorArch = dep
            continue
        if prefsSet.issubset(archTableSet):
            continue
        raise IncompatibleInstructionSets(majorArch.name, dep)

    return majorArch
