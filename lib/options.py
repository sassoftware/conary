#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import log
import sys

# 0 - arg may occur, no parameter
# 1 - arg may occur once, w/ parameter
# 2 - arg may occur N times, w/ parameter

class OptionError(Exception):
    def __init__(self, val):
        self.val = val

def processArgs(argDef, cfgMap, cfg, usage):
    otherArgs = [ sys.argv[0] ]
    argSet = {}

    for arg in cfgMap.keys():
	argDef[arg] = 1

    i = 1
    while i < len(sys.argv):
	if sys.argv[i][:2] != "--":
	    otherArgs.append(sys.argv[i])
	else:
	    arg = sys.argv[i][2:]
	    if not argDef.has_key(arg): raise OptionError(usage())

	    if not argDef[arg]:
		argSet[arg] = 1
	    else:
		# the argument takes a parameter
		i = i + 1
		if i >= len(sys.argv): raise OptionError(usage())

		if argDef[arg] == 1:
		    # exactly one parameter is allowd
		    if argSet.has_key(arg): raise OptionError(usage())
		    argSet[arg] = sys.argv[i]
		else:
		    # multiple parameters may occur
		    if argSet.has_key(arg):
			argSet[arg].append(sys.argv[i])
		    else:
			argSet[arg] = [sys.argv[i]]

	i = i + 1

    if argSet.has_key('config'):
	for param in argSet['config']:
	    cfg.configLine(param)

	del argSet['config']

    for (arg, name) in cfgMap.items():
	if argSet.has_key(arg):
	    cfg.configLine("%s %s" % (name, argSet[arg]))
	    del argSet[arg]

    if argSet.has_key('debug'):
	del argSet['debug']
	import pdb
	pdb.set_trace()

    if '-v' in otherArgs:
	otherArgs.remove('-v')
	log.setVerbosity(1)
    else:
	log.setVerbosity(0)

    return argSet, otherArgs
