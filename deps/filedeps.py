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

def findFileInstructionSet(path):
    results = lib.elf.inspect(path)
    if results is None:
        return ''
    for depClass, main, flags in results[0]:
        if depClass == 'abi':
            if main == 'x86':
                return deps.Dependency('i386', []).freeze()
            return deps.Dependency(main, []).freeze()

    raise AssertionError
