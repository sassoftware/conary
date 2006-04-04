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
    def __init__(self, msg, parser):
        Exception.__init__(self, msg)
        self.parser = parser

class OptionParser(optparse.OptionParser):
    forbiddenOpts = set(str(x) for x in range(0,9))

    def __init__(self, *args, **kw):
        self.hobbleShortOpts = kw.pop('hobbleShortOpts', True)
        optparse.OptionParser.__init__(self, *args, **kw)

    def error(self, msg):
        raise OptionError(msg, self)

    def _process_short_opts(self, rargs, values):
        if (self.hobbleShortOpts and 
            (len(self.rargs[0]) > 2 or self.rargs[0][1] in self.forbiddenOpts)):
            self.largs.append(self.rargs.pop(0))
        else:
            return optparse.OptionParser._process_short_opts(self, rargs,
                                                             values)

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

def addOptions(parser, argDef, skip=None):
    for name, data in sorted(argDef.iteritems()):
        if name == skip:
            continue
        if isinstance(data, dict):
            group = optparse.OptionGroup(parser, name)
            addOptions(group, data)
            parser.add_option_group(group)
            continue
        help = ''
        shortOpt = None
        meta = None
        if isinstance(data, (list, tuple)):
            if len(data) == 3:
                shortOpt = data[0]
                data = data[1:]
            if len(data) >= 2:
                help = data[1]
                if isinstance(help, (list, tuple)):
                    help, meta = help
            paramType = data[0]
        else:
            paramType = data
        flagNames = ['--' + name]
        if shortOpt:
            flagNames.append(shortOpt)

        if paramType == NO_PARAM:
            parser.add_option(action='store_true', dest=name, help=help, 
                              metavar=meta, *flagNames)
        elif paramType == ONE_PARAM:
            parser.add_option(dest=name, help=help, metavar=meta, *flagNames)
        elif paramType == OPT_PARAM:
            parser.add_option(action='callback',
                               callback=optParamCallback, dest=name,
                               type='string', nargs=0, help=help, 
                               metavar=meta, *flagNames)
        elif paramType == MULT_PARAM:
            parser.add_option(action='append', dest=name, help=help, 
                              metavar=meta, *flagNames)

def processArgs(argDef, cfgMap, cfg, usage, argv=sys.argv):
    """Mostly backwards-compatible (with earlier conary processArgs)
       function that uses optparse as its backend.
    """
    return _processArgs(argDef, cfgMap, cfg, usage, argv)[:2]

def _processArgs(params, cfgMap, cfg, usage, argv=sys.argv, version=None,
                commonParams=None, useHelp=False, defaultGroup=None,
                interspersedArgs=True):
    otherArgs = [ argv[0] ]
    argSet = {}
    # don't mangle the command line
    argv = argv[:]

    # historically, usage was generally a function to print out the usage 
    # message.  We want it to be a string.  For now, we
    # convert here to allow backwards compatibility.
    if hasattr(usage, '__call__'):
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


    if defaultGroup:
        d = params[defaultGroup]
    else:
        d = params
    d['debug'] = NO_PARAM, 'Print debugging information'
    d['debugger'] = (NO_PARAM, optparse.SUPPRESS_HELP)

    for (arg, name) in cfgMap.items():
        d[arg] = ONE_PARAM

    parser = getOptionParser(params, cfgMap, cfg, usage, version, useHelp,
                             defaultGroup, interspersedArgs)
    argSet, otherArgs, options = getArgSet(params, parser, argv)

    if 'config-file' in argSet:
        try:
            cfg.read(argSet['config-file'], exception = True)
        except IOError, msg:
            raise OptionError(msg, parser)
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

    return argSet, otherArgs, parser, options

def getOptionParser(params, cfgMap, cfg, usage, version=None, useHelp=False,
                    defaultGroup=None, interspersedArgs=True):
    parser = OptionParser(usage=usage, add_help_option=useHelp, version=version)
    if not interspersedArgs:
        parser.disable_interspersed_args()

    if defaultGroup in params:
        group = optparse.OptionGroup(parser, defaultGroup)
        addOptions(group, params[defaultGroup])
        parser.add_option_group(group)

    found = None
    for name, data in params.iteritems():
        if name == defaultGroup:
            continue
        if isinstance(data, dict):
            found = True
            break
        else:
            found = False

    if found is False:
        group = optparse.OptionGroup(parser, 'Command Options')
        addOptions(group, params, skip=defaultGroup)
        parser.add_option_group(group)
    else:
        addOptions(parser, params, skip=defaultGroup)

    return parser

def getArgSet(params, parser, argv=sys.argv):
    (options, otherArgs) = parser.parse_args(argv)

    argSet = {}

    for name, data in params.iteritems():
        if isinstance(data, dict):
            for name in data:
                val = getattr(options, name)
                if val is None:
                    continue
                argSet[name] = val
        else:
            val = getattr(options, name)
            if val is None:
                continue
            argSet[name] = val
    return argSet, otherArgs, options
