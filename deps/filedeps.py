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
