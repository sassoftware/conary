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
"""
Command-line option handling
"""

import log
import sys




(NO_PARAM,   # arg may occur, no parameter
 ONE_PARAM,  # arg may occur once, req'd parameter
 OPT_PARAM,  # arg may occur once, optional parameter
 MULT_PARAM, # arg may occur N times, w/ parameter
 ) = range(0,4)

class OptionError(Exception):
    def __init__(self, val):
        self.val = val

def processArgs(argDef, cfgMap, cfg, usage, argv=sys.argv):
    otherArgs = [ argv[0] ]
    argSet = {}
    # don't mangle the command line
    argv = argv[:]

    for arg in cfgMap.keys():
	argDef[arg] = 1

    i = 1
    while i < len(argv):
	if argv[i][:2] != "--":
	    otherArgs.append(argv[i])
        # stop processing args after --
        elif argv[i] == '--':
                otherArgs.extend(argv[i+1:])
                break
	else:
            arg = argv[i][2:]
            arg_parts = arg.split('=', 1)
            if len(arg_parts) > 1:
                arg = arg_parts[0]
                # don't allow --foo=bar arg if foo doesn't exist
                # or doesn't take an arg.
                if not argDef.has_key(arg) and argDef[arg] != NO_PARAM:
                    raise OptionError(usage())
                argv[i] = arg
                argv.insert(i+1, arg_parts[1])
	    if not argDef.has_key(arg): raise OptionError(usage())

	    if argDef[arg] == NO_PARAM:
		argSet[arg] = True
	    elif argDef[arg] == OPT_PARAM:
		# max one setting
		if argSet.has_key(arg): raise OptionError(usage())
		if i >= len(argv): 
		    argSet[arg] = True
                else:
                    next_arg = argv[i+1]
                    if next_arg == '':
                        argSet[arg] = True
                        i = i + 1
                    elif next_arg[0:2] == '--': 
                        argSet[arg] = True
                    else:
                        argSet[arg] = next_arg
                        i = i + 1
	    else:
		# the argument takes a parameter
		i = i + 1
		if i >= len(argv): raise OptionError(usage())

		if argDef[arg] == ONE_PARAM:
		    # exactly one parameter is allowd
		    if argSet.has_key(arg): raise OptionError(usage())
		    argSet[arg] = argv[i]
		else:
		    # multiple parameters may occur
		    if argSet.has_key(arg):
			argSet[arg].append(argv[i])
		    else:
			argSet[arg] = [argv[i]]
	i = i + 1

    if 'config-file' in argSet:
	cfg.read(argSet['config-file'])
	del argSet['config-file']
	
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
