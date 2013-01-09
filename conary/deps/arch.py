#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
    # switch to just return muliarch when flavorPreferences are switched on.
    return multiarch
    #return [[ x86_64 ]] + multiarch

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
        'ppc64'         : ['is: ppc64', 'is:ppc'],
        'ppc ppc64'     : ['is: ppc64', 'is:ppc'],
        'sparc64'       : ['is: sparc64', 'is:sparc'],
        'sparc sparc64' : ['is: sparc64', 'is:sparc'],
        's390x'         : ['is: s390x', 'is:s390'],
        's390 s390x'    : ['is: s390x', 'is:s390'],
        'x86_64'        : ['is: x86_64', 'is:x86' ],
        'x86 x86_64'    : ['is: x86_64', 'is:x86'],
    }

    @staticmethod
    def getStringFlavorPreferences(arch):
        key = FlavorPreferences._getCurrentArchIS(arch)
        return FlavorPreferences.flavorPreferences.get(key, [])

    @staticmethod
    def getFlavorPreferences(arch):
        return [ deps.parseFlavor(x)
            for x in FlavorPreferences.getStringFlavorPreferences(arch) ]

def getFlavorPreferences(arch = currentArch):
    return FlavorPreferences.getFlavorPreferences(arch)

class IncompatibleInstructionSets(Exception):
    def __init__(self, is1, is2):
        Exception.__init__(self)
        self.is1 = is1
        self.is2 = is2

    def __str__(self):
        return "Incompatible architectures: %s: %s" % (self.is1, self.is2)

def getFlavorPreferencesFromFlavor(depSet):
    arch = getMajorArch(depSet.iterDepsByClass(deps.InstructionSetDependency))
    if arch is None:
        return None
    return getFlavorPreferences([[arch]])

def getMajorArch(depList):
    """Return the major architecture from an instruction set dependency
    @type depList: list (iterable) of Dependency objects representing
    architectures.
    @param depGroupL a list (iterable) of Dependency objects.
    @raise IncompatibleInstructionSets: when incompatible architectures are
           present in the list.
    @rtype: instance of Dependency, or None
    @return: major architecture from the list, or None if the list is empty
    """
    # Compare instruction sets by looking at the flavor preferences -
    # If a minor architecture is allowed to be set for this arch,
    # then it will have flavor preferences that describe the allowed
    # flavors.
    majorArch = None
    depList = list(depList) # accept generators
    if not depList:
        return None
    if len(depList) == 1:
        return depList[0]
    for dep in depList:
        prefs = set(getFlavorPreferences([[dep]]))
        if not prefs:
            continue
        majorArch = dep
        prefArches = set()
        for depSet in prefs:
            for dep in depSet.iterDepsByClass(deps.InstructionSetDependency):
                prefArches.add(dep.name)
        break
    if not majorArch:
        raise IncompatibleInstructionSets(depList[0].name, depList[1])
    for dep in depList:
        if dep.name != majorArch.name and getFlavorPreferences([[dep]]):
            raise IncompatibleInstructionSets(majorArch.name, dep)
        elif dep.name not in prefArches:
            raise IncompatibleInstructionSets(majorArch.name, dep)
    return majorArch
