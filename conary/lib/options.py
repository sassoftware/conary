#
# Copyright (c) 2004-2005 rPath, Inc.
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

from conary.lib import log, util
import sys




(NO_PARAM,   # arg may occur, no parameter
 ONE_PARAM,  # arg may occur once, req'd parameter
 OPT_PARAM,  # arg may occur once, optional parameter
 MULT_PARAM, # arg may occur N times, w/ parameter
 ) = range(0,4)

class OptionError(Exception):
    
    val = 1

def processArgs(argDef, cfgMap, cfg, usage, argv=sys.argv):
    otherArgs = [ argv[0] ]
    argSet = {}
    # don't mangle the command line
    argv = argv[:]

    for arg in cfgMap.keys():
	argDef[arg] = 1
    argDef['debug'] = NO_PARAM
    argDef['debugger'] = NO_PARAM

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
                if not argDef.has_key(arg):
                    usage()
                    raise OptionError("Unknown Flag '%s'r" % arg)
                elif argDef[arg] == NO_PARAM:
                    usage()
                    raise OptionError(
                                "Flag '%s' does not take a parameter" % arg)
                argv[i] = arg
                argv.insert(i+1, arg_parts[1])
	    if not argDef.has_key(arg): 
                usage()
                raise OptionError("Unknown flag '%s'" % arg)

	    if argDef[arg] == NO_PARAM:
		argSet[arg] = True
	    elif argDef[arg] == OPT_PARAM:
		# max one setting
		if argSet.has_key(arg): 
                    raise OptionError(
                            "Flag '%s' takes at most one parameter" % arg)
		if i >= len(argv): 
		    argSet[arg] = True
                else:
                    if i + 1 < len(argv):
                        next_arg = argv[i+1]
                    else:
                        # option was last on the command line,
                        # and no optional paramater was given
                        next_arg = ''
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
		if i >= len(argv): 
                    usage()
                    raise OptionError("Flag '%s' requires a parameter" % arg)

		if argDef[arg] == ONE_PARAM:
		    # exactly one parameter is allowd
		    if argSet.has_key(arg): 
                        usage()
                        raise OptionError(
                            "Flag '%s' requires exactly one parameter" % arg)
		    argSet[arg] = argv[i]
		else:
		    # multiple parameters may occur
		    if argSet.has_key(arg):
			argSet[arg].append(argv[i])
		    else:
			argSet[arg] = [argv[i]]
	i = i + 1

    if 'config-file' in argSet:
        try:
            cfg.read(argSet['config-file'], exception = True)
        except IOError, msg:
            raise OptionError(msg)
	del argSet['config-file']
	
    if argSet.has_key('config'):
	for param in argSet['config']:
	    cfg.configLine(param)

	del argSet['config']

    for (arg, name) in cfgMap.items():
	if argSet.has_key(arg):
	    cfg.configLine("%s %s" % (name, argSet[arg]))
	    del argSet[arg]

    if argSet.has_key('debugger'):
	del argSet['debugger']
	from conary.lib import debugger
	debugger.set_trace()
        sys.excepthook = util.genExcepthook(cfg.dumpStackOnError, 
                                            debugCtrlC=True)


    if 'debug' in argSet:
	del argSet['debug']
	log.setVerbosity(log.DEBUG)
    else:
	log.setVerbosity(log.WARNING)

    return argSet, otherArgs
