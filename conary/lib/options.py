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
 COUNT_PARAM, # arg may occur N times, value is the count
 STRICT_OPT_PARAM, # arg may occur once, optional parameter, stricter parsing
 ) = range(0,6)

(NORMAL_HELP,
 VERBOSE_HELP,  # only display in usage messages if -v is used
) = range(0,2)

class OptionError(Exception):
    val = 1
    def __init__(self, msg, parser):
        Exception.__init__(self, msg)
        self.parser = parser

class HelpFormatter(optparse.IndentedHelpFormatter):
    def format_option(self, option):
        if option.help_level == VERBOSE_HELP and log.getVerbosity() > log.INFO:
            return ''
        return optparse.IndentedHelpFormatter.format_option(self, option)

    def format_description(self, description):
        return description + '\n'

class Option(optparse.Option):
    ATTRS = optparse.Option.ATTRS[:]
    ATTRS.append('help_level')

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

    def _process_long_opt(self, rargs, values):
        opt = self._match_long_opt(rargs[0].split('=')[0])

        option = self._long_opt[opt]

        if '=' in rargs[0]:
            had_explicit_value = True
            if option.callback in (strictOptParamCallback, optParamCallback):
                rargs[:] = rargs[0].split('=', 1) + rargs[1:]
                self.rargs = rargs
        else:
            had_explicit_value = False

        option.had_explicit_value = had_explicit_value
        return optparse.OptionParser._process_long_opt(self, rargs, values)

class OptionGroup(optparse.OptionGroup):
     def format_help(self, formatter):
        if log.getVerbosity() > log.DEBUG:
            found = False
            for option in self.option_list:
                if (option.help_level == NORMAL_HELP 
                    and option.help != optparse.SUPPRESS_HELP):
                    found = True
                    break
            if not found:
                return ''
        return optparse.OptionGroup.format_help(self, formatter)

def optParamCallback(option, opt_str, value, parser, *args, **kw):
    strict = kw.pop('strictOpt', False)

    value = True
    if option.had_explicit_value:
        newValue = parser.rargs[0]
        del parser.rargs[0]
        if newValue: # handle --opt= - treat like --opt
            value = newValue
    elif (not strict and parser.rargs 
          and parser.rargs[0] and parser.rargs[0][0] != '-'):
        newValue = parser.rargs[0]
        del parser.rargs[0]
        if newValue: # handle --opt= - treat like --opt
            value = newValue
    setattr(parser.values, option.dest, value)

def strictOptParamCallback(*args, **kw):
    kw['strictOpt'] = True
    return optParamCallback(*args, **kw)


def addOptions(parser, argDef, skip=None):
    for name, data in sorted(argDef.iteritems()):
        if name == skip:
            continue
        if isinstance(data, dict):
            group = OptionGroup(parser, name)
            addOptions(group, data)
            parser.add_option_group(group)
            continue
        help = ''
        help_level = NORMAL_HELP
        shortOpt = None
        meta = None
        if isinstance(data, (list, tuple)):
            if isinstance(data[0], str) and data[0].startswith('-'):
                shortOpt = data[0]
                data = data[1:]
            if len(data) >= 2:
                help = data[-1]
                data = data[:-1]
                if isinstance(help, (list, tuple)):
                    if isinstance(help[0], int):
                        help_level = help[0]
                        help = help[1:]
                    if len(help) == 2:
                        help, meta = help
                    else:
                        help = help[0]
                elif isinstance(help, int):
                    help = ''
                    help_level = help
            assert(len(data) == 1)
            paramType = data[0]
        else:
            paramType = data
        flagNames = ['--' + name]
        if shortOpt:
            flagNames.append(shortOpt)

        attrs = {
            'dest': name,
            'help': help,
            'help_level': help_level,
            'metavar': meta,
            }
        if paramType == NO_PARAM:
            attrs['action'] = 'store_true'
        elif paramType == ONE_PARAM:
            pass
        elif paramType in (OPT_PARAM, STRICT_OPT_PARAM):
            attrs['action'] = 'callback'
            if paramType == OPT_PARAM:
                attrs['callback'] = optParamCallback
            else:
                attrs['callback'] = strictOptParamCallback
            attrs['nargs'] = 0
            attrs['type'] = 'string'
        elif paramType == MULT_PARAM:
            attrs['action'] = 'append'
        elif paramType == COUNT_PARAM:
            attrs['action'] = 'count'
        parser.add_option(*flagNames, **attrs)


def processArgs(argDef, cfgMap, cfg, usage, argv=sys.argv):
    """Mostly backwards-compatible (with earlier conary processArgs)
       function that uses optparse as its backend.
    """
    return _processArgs(argDef, cfgMap, cfg, usage, argv)[:2]

def _getUsageStr(usage):
    # historically, usage was generally a function to print out the usage 
    # message.  We want it to be a string.  For now, we
    # convert here to allow backwards compatibility.
    if hasattr(usage, '__call__'):
        stdout = StringIO.StringIO()
        stderr = StringIO.StringIO()
        oldStdOut = sys.stdout
        oldStdErr = sys.stderr
        # set a default message
        rc = 'An error occurred while generating the usage message'
        try:
            sys.stdout = stdout
            sys.stderr = stderr
            try:
                usage()
            except SystemExit:
                # some of these old usage functions even exit after 
                # printing the usage message!
                pass
            rc = stdout.getvalue() + stderr.getvalue()
        finally:
            sys.stdout = oldStdOut
            sys.stderr = oldStdErr
        usage = rc
    return usage

def _getParser(params, cfgMap, usage, version, useHelp, defaultGroup,
               interspersedArgs=True, hobbleShortOpts=False,
               addDebugOptions=True, addConfigOptions=True,
               addVerboseOptions=True,
               description=None):
    usage = _getUsageStr(usage)

    if defaultGroup:
        d = params[defaultGroup]
    else:
        d = params

    if addDebugOptions:
        d['debug'] = STRICT_OPT_PARAM, (VERBOSE_HELP, 'Print helpful debugging output (use --debug=all for internal debug info)')
        d['debugger'] = (NO_PARAM, optparse.SUPPRESS_HELP)

    if addVerboseOptions:
        d['verbose'] = STRICT_OPT_PARAM, (VERBOSE_HELP, 'Display more verbose output')

    d['help'] = NO_PARAM, optparse.SUPPRESS_HELP

    if addConfigOptions:
        for (arg, name) in cfgMap.items():
            d[arg] = ONE_PARAM


    return getOptionParser(params, usage, version, useHelp,
                           defaultGroup, interspersedArgs,
                           hobbleShortOpts=hobbleShortOpts,
                           description=description)



def _processArgs(params, cfgMap, cfg, usage, argv=sys.argv, version=None,
                 commonParams=None, useHelp=False, defaultGroup=None,
                 interspersedArgs=True, hobbleShortOpts=False,
                 addDebugOptions=True, addConfigOptions=True,
                 addVerboseOptions=True, description=None):
    argSet = {}
    # don't mangle the command line
    argv = argv[:]

    parser = _getParser(params, cfgMap, usage, version, useHelp, defaultGroup,
                        interspersedArgs, hobbleShortOpts,
                        addDebugOptions=addDebugOptions, 
                        addConfigOptions=addConfigOptions,
                        addVerboseOptions=addVerboseOptions,
                        description=description)
    argSet, otherArgs, options = getArgSet(params, parser, argv)

    if addConfigOptions:
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

    if addVerboseOptions:
        if 'verbose' in argSet:
            if argSet['verbose'] is True:
                log.setVerbosity(log.INFO)
            del argSet['verbose']

    if addDebugOptions:
        if argSet.has_key('debugger'):
            del argSet['debugger']
            from conary.lib import debugger
            debugger.set_trace()
            sys.excepthook = util.genExcepthook(debug=cfg.debugExceptions,
                                                debugCtrlC=True)

        if 'debug' in argSet:
            if argSet['debug'] is True:
                log.setVerbosity(log.DEBUG)
            else:
                log.setVerbosity(log.LOWLEVEL)
            del argSet['debug']
        else:
            if log.getVerbosity() > log.WARNING:
                log.setVerbosity(log.WARNING)


    return argSet, otherArgs, parser, options

def getOptionParser(params, usage, version=None, useHelp=False,
                    defaultGroup=None, interspersedArgs=True, 
                    hobbleShortOpts=False, description=None):
    parser = OptionParser(usage=usage, add_help_option=useHelp,
                          version=version,
                          hobbleShortOpts=hobbleShortOpts,
                          option_class=Option,
                          formatter=HelpFormatter(),
                          description=description)
    if not interspersedArgs:
        parser.disable_interspersed_args()

    if defaultGroup in params:
        group = OptionGroup(parser, defaultGroup)
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
        group = OptionGroup(parser, 'Command Options')
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
    help = ''      # short help
    description = None # longer help (defaults to __doc__)
    defaultGroup = 'Common Options' # The heading for options that aren't
                                    # put in any other group.
    commandGroup = 'Common Commands'
    docs = {} # add docs in the form 'long-option' : 'description'
              # or 'long-option' : ('description', 'KEYWORD').
    hidden = False # hide from the default usage message?
    hobbleShortOpts = None
    def __init__(self):
        self.parser = None
        self.mainHandler = None

    def usage(self, errNo=1):
        if not self.parser:
            self.setParser(self.mainHandler.getParser(self.commands[0]))
        self.parser.print_help()
        if log.getVerbosity() > log.INFO:
            print
            print '(Use --verbose to get a full option listing)'
        return errNo

    def setParser(self, parser):
        self.parser = parser

    def setMainHandler(self, mainHandler):
        self.mainHandler = mainHandler

    def addParameters(self, argDef):
        if self.defaultGroup not in argDef:
            argDef[self.defaultGroup] = {}

    def addConfigOptions(self, cfgMap, argDef):
        for name, data in cfgMap.items():
            if len(data) == 3:
                cfgName, paramType, shortOpt = data
            else:
                shortOpt = None
                cfgName, paramType = data

            # if it's a NO_PARAM
            if paramType == NO_PARAM:
                negName = 'no-' + name
                argDef[self.defaultGroup][negName] = NO_PARAM, optparse.SUPPRESS_HELP
                cfgMap[negName] = (cfgName, paramType)

            if shortOpt:
                argDef[self.defaultGroup][name] = shortOpt, paramType
            else:
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
        for (arg, data) in cfgMap.items():
            cfgName, paramType = data[0:2]
            value = argSet.pop(arg, None)
            if value is not None:
                if arg.startswith('no-'):
                    value = not value

                cfg.configLine("%s %s" % (cfgName, value))

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
    useConaryOptions = True # whether to add --config, --debug, --debugger
                            # and use cfgMap.

    def __init__(self):
        self._supportedCommands = {}
        for class_ in reversed(inspect.getmro(self.__class__)):
            if not hasattr(class_, 'commandList'):
                continue
            for command in class_.commandList:
                self._registerCommand(command)

    def _registerCommand(self, commandClass):
        supportedCommands = self._supportedCommands
        inst = commandClass()
        inst.setMainHandler(self)
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
                                                    version=None,
                                                    useHelp=False,
                                                    defaultGroup=defaultGroup,
                                                    interspersedArgs=False,
                                    hobbleShortOpts=self.hobbleShortOpts)
        return argSet, [argv[0]] + otherArgs

    def usage(self, rc = 1, showAll = False):
        # get the longest command to set the width of the command
        # column
        width = 0
        commandList = set(self._supportedCommands.itervalues())
        for command in commandList:
            if command.hidden:
                continue
            width = max(width, len('/'.join(command.commands)))
        # group the commands together
        groups = dict.fromkeys(x.commandGroup for x in commandList)
        for group in groups.iterkeys():
            groups[group] = [ x for x in commandList if
                              x.commandGroup == group ]
        # Sort the groups
        groupNames = groups.keys()
        groupNames.sort()
        for group in groupNames:
            if group == 'Hidden Commands':
                continue
            commands = groups[group]
            # filter out hidden commands
            if showAll:
                filtered = commands
            else:
                filtered = [ x for x in commands if not x.hidden ]
            if not filtered:
                continue
            # print the header for the command group
            print
            print group
            # sort the commands by the first command name
            for command in sorted(filtered, key=lambda x: x.commands[0]):
                print '  %-*s  %s' %(width, '/'.join(command.commands),
                                     command.help)
        return rc

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

    def getParser(self, command):
        thisCommand = self._supportedCommands[command]
        params, cfgMap = thisCommand.prepare()
        kwargs = self._getParserFlags(command)
        return _getParser(params, {}, **kwargs)

    def _getParserFlags(self, commandName):
        thisCommand = self._supportedCommands[commandName]
        if thisCommand.hobbleShortOpts is not None:
            hobbleShortOpts = thisCommand.hobbleShortOpts
        else:
            hobbleShortOpts = self.hobbleShortOpts
        defaultGroup = thisCommand.defaultGroup
        if self.name:
            progName = self.name
        else:
            progName = argv[0]
        commandUsage = '%s %s %s' % (progName, commandName,
                                     thisCommand.paramHelp)
        description = thisCommand.description
        if not description:
            description = thisCommand.__doc__
        if description is None:
            description = thisCommand.help
        return dict(usage=commandUsage, version=None,
                    useHelp=False,
                    defaultGroup=defaultGroup,
                    hobbleShortOpts=hobbleShortOpts,
                    addDebugOptions=self.useConaryOptions,
                    addConfigOptions=self.useConaryOptions,
                    description=description)

    def main(self, argv=sys.argv, debuggerException=Exception,
             cfg=None, **kw):
        """
            Process argv and execute commands as specified.
        """

        from conary import versions
        supportedCommands = self._supportedCommands

        if cfg is None:
            cfg = self.getConfigFile(argv)

        if '--version' in argv:
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

        if len(argv) == 1:
            # no command specified
            return self.usage()

        commandName = argv[1]
        if commandName not in self._supportedCommands:
            rc = self.usage()
            print "%s: unknown command: '%s'" % (self.name, commandName)
            return rc

        thisCommand = self._supportedCommands[commandName]
        params, cfgMap = thisCommand.prepare()
        kwargs = self._getParserFlags(commandName)

        try:
            newArgSet, otherArgs, parser, optionSet = _processArgs(
                                        params, {}, cfg,
                                        argv=argv, **kwargs)
        except debuggerException, e:
            raise
        except OptionError, e:
            e.parser.print_help()
            print >> sys.stderr, e
            sys.exit(e.val)
        except versions.ParseError, e:
            print >> sys.stderr, e
            sys.exit(1)

        if argSet.pop('help', False):
            thisCommand.usage()
            sys.exit(1)


        thisCommand.setParser(parser)
        argSet.update(newArgSet)
        thisCommand.processConfigOptions(cfg, cfgMap, argSet)
        return self.runCommand(thisCommand, cfg, argSet, otherArgs, **kw)

    def runCommand(self, thisCommand, *args, **kw):
        return thisCommand.runCommand(*args, **kw)


