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

    abi = None
    for (depClass, main, flags) in results[0]:
        if depClass == 'abi':
            abi = (main, flags)
            break
    
    for depList in results:
	set = deps.DependencySet()
	for (depClass, main, flags) in depList:
	    if depClass == 'soname':
                assert(abi)
		curClass = deps.SonameDependencies
                dep = deps.Dependency(abi[0] + "/" + main, abi[1] + flags)
	    elif depClass == 'abi':
		curClass = deps.AbiDependency
                dep = deps.Dependency(main, flags)
	    else:
		assert(0)

	    set.addDep(curClass, dep)
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
            elif isnset == 'x86_64':
                set.addDep(deps.InstructionSetDependency,
                           deps.Dependency('x86', ['x86_64']))
            else:
                set.addDep(deps.InstructionSetDependency,
                           deps.Dependency(isnset, []))
    return set
