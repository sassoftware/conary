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

import optparse
import StringIO

from conary.lib import log, util
import sys


(NO_PARAM,   # arg may occur, no parameter
 ONE_PARAM,  # arg may occur once, req'd parameter
 OPT_PARAM,  # arg may occur once, optional parameter
 MULT_PARAM, # arg may occur N times, w/ parameter
 ) = range(0,4)

class OptionError(Exception):
    val = 1

class OptionParser(optparse.OptionParser):
    def error(self, msg):
        raise OptionError(msg)

def optParamCallback(option, opt_str, value, parser, *args, **kw):
    value = True
    if parser.rargs:
        potentialParam = parser.rargs[0]
        if not potentialParam:
            del parser.rargs[0]
        elif potentialParam[0] != '-':
            value = potentialParam 
            del parser.rargs[0]
    setattr(parser.values, option.dest, value)

def processArgs(argDef, cfgMap, cfg, usage, argv=sys.argv):
    """Backwards-compatible (with earlier conary processArgs)
       function that uses optparse as its backend.
    """
    otherArgs = [ argv[0] ]
    argSet = {}
    # don't mangle the command line
    argv = argv[:]

    for arg in cfgMap.keys():
	argDef[arg] = 1
    argDef['debug'] = NO_PARAM
    argDef['debugger'] = NO_PARAM

    # historically, usage was generally a function to print out the usage 
    # message.  We want it to be a string.  For now, we
    # convert here to allow backwards compatibility.
    s = StringIO.StringIO()
    oldStdOut = sys.stdout
    sys.stdout = s
    try:
        usage()
    except SystemExit:
        # some of these old usage functions even exit after 
        # printing the usage message!
        pass
    sys.stdout = oldStdOut
    usage = s.getvalue()

    parser = OptionParser(usage=usage, add_help_option=False)

    for name, paramType in argDef.iteritems():
        if paramType == NO_PARAM:
            parser.add_option('--' + name, action='store_true', dest=name)
        elif paramType == ONE_PARAM:
            parser.add_option('--' + name, dest=name)
        elif paramType == OPT_PARAM:
            parser.add_option('--' + name, action='callback',
                               callback=optParamCallback, dest=name,
                               type='string', nargs=0)
        elif paramType == MULT_PARAM:
            parser.add_option('--' + name, action='append', dest=name)

    (options, otherArgs) = parser.parse_args(argv)
    argSet = {}
    for name in argDef:
        val = getattr(options, name)
        if val is None:
            continue
        argSet[name] = val

    if 'config-file' in argSet:
        try:
            cfg.read(argSet['config-file'], exception = True)
        except IOError, msg:
            raise OptionError(msg)
	del argSet['config-file']
	
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
