#
# Copyright (c) 2004-2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
"""
Command-line option handling
"""

import inspect
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
    matchPartialOptions = False

    def __init__(self, *args, **kw):
        self.hobbleShortOpts = kw.pop('hobbleShortOpts', False)
        optparse.OptionParser.__init__(self, *args, **kw)

    def _match_long_opt(self, opt):
        match = optparse._match_abbrev(opt, self._long_opt)
        if not self.matchPartialOptions and opt != match:
            raise optparse.BadOptionError("no such option: %s" % opt)
        return match

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
                interspersedArgs=True, hobbleShortOpts=False):
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
                             defaultGroup, interspersedArgs,
                             hobbleShortOpts=hobbleShortOpts)
    argSet, otherArgs, options = getArgSet(params, parser, argv)

    configFileList = argSet.pop('config-file', [])
    if not isinstance(configFileList, list):
        configFileList = [configFileList]
    for path in configFileList:
        try:
            cfg.read(path, exception = True)
        except IOError, msg:
            raise OptionError(msg, parser)
	
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
                    defaultGroup=None, interspersedArgs=True, 
                    hobbleShortOpts=False):
    parser = OptionParser(usage=usage, add_help_option=useHelp, version=version,
                          hobbleShortOpts=hobbleShortOpts)
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


class AbstractCommand(object):
    """
        Abstract command object to be subclassed used to represent commands
        in a command line interface.  To be used with MainHandler below.
        Assumes use of a lib.cfg.ConfigFile type configuration object.
    """
    commands = []
    paramHelp = '' # for each command will display <command> + paramHelp
                   # as part of usage.
    defaultGroup = 'Common Options' # The heading for options that aren't
                                    # put in any other group.

    docs = {} # add docs in the form 'long-option' : 'description'
              # or 'long-option' : ('description', 'KEYWORD').

    def __init__(self):
        self.parser = None

    def usage(self, errNo=1):
        if self.parser:
            self.parser.print_help()
        return errNo

    def setParser(self, parser):
        self.parser = parser

    def addParameters(self, argDef):
        pass

    def addConfigOptions(self, cfgMap, argDef):
        for name, (cfgName, paramType)  in cfgMap.items():
            # if it's a NO_PARAM
            if paramType == NO_PARAM:
                negName = 'no-' + name
                argDef[self.defaultGroup][negName] = NO_PARAM, optparse.SUPPRESS_HELP
                cfgMap[negName] = (cfgName, paramType)

            argDef[self.defaultGroup][name] = paramType

    def addDocs(self, argDef):
        """ Parse a docs dict assigned at the class level
            and add those docs to the parameters being sent to 
            parseOptions.
        """
        d = {}
        for class_ in reversed(inspect.getmro(self.__class__)):
            if not hasattr(class_, 'docs'):
                continue
            d.update(class_.docs)

        commandDicts = [argDef]
        while commandDicts:
            commandDict = commandDicts.pop()
            for name, value in commandDict.items():
                if isinstance(value, dict):
                    commandDicts.append(value)
                    continue
                if name in d:
                    if not isinstance(value, (list, tuple)):
                        value = [ value ]
                    else:
                        value = list(value)
                    value.append(d[name])
                    commandDict[name] = value


    def prepare(self):
        params = {}
        cfgMap = {}
        self.addParameters(params)
        self.addConfigOptions(cfgMap, params)
        self.addDocs(params)
        return params, cfgMap

    def processConfigOptions(self, cfg, cfgMap, argSet):
        """
            Manage any config maps we've set up, converting 
            assigning them to the config object.
        """ 
        configFileList = argSet.pop('config-file', [])
        if not isinstance(configFileList, list):
            configFileList = list(configFileList)

        for line in configFileList:
            cfg.read(path, exception=True)

        for (arg, (name, paramType)) in cfgMap.items():
            value = argSet.pop(arg, None)
            if value is not None:
                if arg.startswith('no-'):
                    value = not value

                cfg.configLine("%s %s" % (name, value))

        for line in argSet.pop('config', []):
            cfg.configLine(line)



    def runCommand(self, *args, **kw):
        raise NotImplementedError


class MainHandler(object):
    """
        Class to handle parsing and executing commands set up to use
        AbstractCommands
    """

    abstractCommand = None   # class to grab generic options from.  These
                             # can be used in front of 
    commandList = []         # list of commands to support.
    name = None              # name to use when showing usage messages.
    version = '<no version>' # version to return to --version

    hobbleShortOpts = False # whether or not to allow -mn to be used, or to
                            # require -m -n.
    configClass = None

    def __init__(self):
        self._supportedCommands = {}
        for command in self.commandList:
            self._registerCommand(command)

    def _registerCommand(self, commandClass):
        supportedCommands = self._supportedCommands
        inst = commandClass()
        if isinstance(commandClass.commands, str):
            supportedCommands[commandClass.commands] = inst
        else:
            for cmdName in commandClass.commands:
                supportedCommands[cmdName] = inst

    def _getPreCommandOptions(self, argv, cfg):
        """Allow the user to specify generic flags before they specify the
           command to run.
        """
        thisCommand = self.abstractCommand()
        params, cfgMap = thisCommand.prepare()
        defaultGroup = thisCommand.defaultGroup
        argSet, otherArgs, parser, optionSet = _processArgs(
                                                    params, {}, cfg,
                                                    self.usage,
                                                    argv=argv[1:],
                                                    version=self.version,
                                                    useHelp=True,
                                                    defaultGroup=defaultGroup,
                                                    interspersedArgs=False,
                                    hobbleShortOpts=self.hobbleShortOpts)
        return argSet, [argv[0]] + otherArgs

    def getConfigFile(self, argv):
        """
            Find the appropriate config file
        """
        if not self.configClass:
            raise RuntimeError, ('Must define a configClass to use with this'
                                 ' main handler')
        if '--skip-default-config' in argv:
            argv.remove('--skip-default-config')
            ccfg = self.configClass(readConfigFiles=False)
        else:
            ccfg = self.configClass(readConfigFiles=True)
        return ccfg

    def main(self, argv=sys.argv, debuggerException=Exception,
             cfg=None, **kw):
        """
            Process argv and execute commands as specified.
        """

        from conary import versions
        supportedCommands = self._supportedCommands

        if cfg is None:
            cfg = self.getConfigFile(argv)

        if '--version' in argv or '-v' in argv:
            print self.version
            return

        try:
            argSet, argv = self._getPreCommandOptions(argv, cfg)
        except debuggerException:
            raise
        except OptionError, e:
            self.usage()
            print >>sys.stderr, e
            sys.exit(e.val)

        if len(argv) < 2:
            # no command specified
            return self.usage()


        commandName = argv[1]
        if commandName not in self._supportedCommands:
            return self.usage()

        thisCommand = self._supportedCommands[commandName]
        params, cfgMap = thisCommand.prepare()
        defaultGroup = thisCommand.defaultGroup
        if self.name:
            progName = self.name
        else:
            progName = argv[0]
        commandUsage = '%s %s %s' % (progName, commandName,
                                     thisCommand.paramHelp)
        try:
            newArgSet, otherArgs, parser, optionSet = _processArgs(
                                        params, {}, cfg,
                                        commandUsage,
                                        argv=argv,
                                        version=self.version,
                                        useHelp=True,
                                        defaultGroup=defaultGroup,
                                        hobbleShortOpts=self.hobbleShortOpts)
        except debuggerException, e:
            raise
        except OptionError, e:
            e.parser.print_help()
            print >> sys.stderr, e
            sys.exit(e.val)
        except versions.ParseError, e:
            print >> sys.stderr, e
            sys.exit(1)

        thisCommand.setParser(parser)
        argSet.update(newArgSet)
        thisCommand.processConfigOptions(cfg, cfgMap, argSet)
        self.runCommand(thisCommand, cfg, argSet, otherArgs, **kw)

    def runCommand(self, thisCommand, *args, **kw):
        thisCommand.runCommand(*args, **kw)


