#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import deps
import lib.elf

def findFileDependencies(path):
    # returns two DependencySets, one for what the file requires and
    # another for what it provides
    results = lib.elf.inspect(path)

    # the file isn't elf
    if results == None:
	return None

    rc = []
    
    for depList in results:
	set = deps.DependencySet()
	for (depClass, main, flags) in depList:
	    if depClass == 'soname':
		curClass = deps.SonameDependencies
	    elif depClass == 'abi':
		curClass = deps.AbiDependency
	    else:
		assert(0)

	    set.addDep(curClass, deps.Dependency(main, flags))
	rc.append(set)

    return rc

def findFileFlavor(path):
    # XXX get Use flags in here
    set = deps.DependencySet()
    results = lib.elf.inspect(path)
    if results is None:
        return set
    for depClass, main, flags in results[0]:
        if depClass == 'abi':
            abi, isnset = flags
            if isnset == 'x86':
                set.addDep(deps.InstructionSetDependency,
                           deps.Dependency('x86', []))
            else:
                set.addDep(deps.InstructionSetDependency,
                           deps.Dependency(isnset, []))
    return set
