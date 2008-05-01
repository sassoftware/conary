import inspect
import sys

from conary import errors
from conary.lib import options

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

    def registerCommand(self, commandClass):
        supportedCommands = self._supportedCommands
        inst = commandClass()
        inst.setMainHandler(self)
        if isinstance(commandClass.commands, str):
            supportedCommands[commandClass.commands] = inst
        else:
            for cmdName in commandClass.commands:
                supportedCommands[cmdName] = inst
    # this method is public; add private version for bw compat.
    _registerCommand = registerCommand

    def unregisterCommand(self, commandClass):
        if isinstance(commandClass.commands, str):
            del self._supportedCommands[commandClass.commands]
        else:
            for cmdName in commandClass.commands:
                del self._supportedCommands[cmdName]
    # this method is public; add private version for bw compat.
    _unregisterCommand = unregisterCommand

    def _getPreCommandOptions(self, argv, cfg):
        """Allow the user to specify generic flags before they specify the
           command to run.
        """
        thisCommand = self.abstractCommand()
        params, cfgMap = thisCommand.prepare()
        defaultGroup = thisCommand.defaultGroup
        kwargs = self._getParserFlags(thisCommand)
        argSet, otherArgs, parser, optionSet = options._processArgs(
                                                    params, {}, cfg,
                                                    usage=self.usage,
                                                    argv=argv[1:],
                                                    interspersedArgs=False,
                                                    **kwargs)
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
        usage = self._getUsage(command)
        kwargs = self._getParserFlags(thisCommand)
        return options._getParser(params, {}, usage=usage, **kwargs)

    def _getUsage(self, commandName):
        if self.name:
            progName = self.name
        else:
            progName = argv[0]

        thisCommand = self._supportedCommands[commandName]
        commandUsage = '%s %s %s' % (progName, commandName,
                                     thisCommand.paramHelp)
        return commandUsage

    def _getParserFlags(self, thisCommand):
        if thisCommand.hobbleShortOpts is not None:
            hobbleShortOpts = thisCommand.hobbleShortOpts
        else:
            hobbleShortOpts = self.hobbleShortOpts
        defaultGroup = thisCommand.defaultGroup
        description = thisCommand.description
        if not description:
            description = thisCommand.__doc__
        if description is None:
            description = thisCommand.help
        return dict(version=None,
                    useHelp=False,
                    defaultGroup=defaultGroup,
                    hobbleShortOpts=hobbleShortOpts,
                    addDebugOptions=self.useConaryOptions,
                    addConfigOptions=self.useConaryOptions,
                    addVerboseOptions=self.useConaryOptions,
                    description=description)

    def getCommand(self, argv, cfg):
        if len(argv) == 1:
            # no command specified
            return None

        commandName = argv[1]
        if commandName not in self._supportedCommands:
            rc = self.usage()
            raise errors.ParseError("%s: unknown command: '%s'" % (self.name, commandName))
        return self._supportedCommands[commandName]

    def main(self, argv=None, debuggerException=Exception,
             cfg=None, **kw):
        """
            Process argv and execute commands as specified.
        """
        if argv is None:
            argv=sys.argv
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
        except options.OptionError, e:
            self.usage()
            print >>sys.stderr, e
            sys.exit(e.val)
        thisCommand = self.getCommand(argv, cfg)
        if thisCommand is None:
            return self.usage()
        commandName = argv[1]
        params, cfgMap = thisCommand.prepare()
        kwargs = self._getParserFlags(thisCommand)

        try:
            newArgSet, otherArgs, parser, optionSet = options._processArgs(
                                        params, {}, cfg, 
                                        usage=self._getUsage(commandName),
                                        argv=argv, **kwargs)
        except debuggerException, e:
            raise
        except options.OptionError, e:
            e.parser.print_help()
            print >> sys.stderr, e
            sys.exit(e.val)
        except versions.ParseError, e:
            print >> sys.stderr, e
            sys.exit(1)

        argSet.update(newArgSet)
        if argSet.pop('help', False):
            thisCommand.usage()
            sys.exit(1)

        thisCommand.setParser(parser)
        thisCommand.processConfigOptions(cfg, cfgMap, argSet)
        return self.runCommand(thisCommand, cfg, argSet, otherArgs, **kw)

    def runCommand(self, thisCommand, *args, **kw):
        return thisCommand.runCommand(*args, **kw)
