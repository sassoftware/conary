#
# Copyright (c) 2004-2008 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import inspect
import optparse

from conary import errors
from conary.lib import log

__developer_api__ = True

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
            self.setParser(self.mainHandler.getParserByClass(self))
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
        from conary.lib.options import NO_PARAM
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

    def requireParameters(self, args, expected=None, allowExtra=False,
                          appendExtra=False, maxExtra=None):
        args = args[1:] # cut off argv[0]
        command = repr(args[0])
        if isinstance(expected, str):
            expected = [expected]
        if expected is None:
            expected = ['command']
        else:
            expected = ['command'] + expected
        if expected:
            missing = expected[len(args):]
            if missing:
                raise errors.ParseError('%s missing %s command'
                                        ' parameter(s): %s' % (
                                        command, len(missing),
                                        ', '.join(missing)))
        extra = len(args) - len(expected)
        if not allowExtra and not appendExtra:
            maxExtra = 0
        if maxExtra is not None and extra > maxExtra:
            if maxExtra:
                numParams = '%s-%s' % (len(expected)-1,
                                       len(expected) + maxExtra - 1)
            else:
                 numParams = '%s' % (len(expected)-1)
            raise errors.ParseError('%s takes %s arguments, received %s' % (command, numParams, len(args)-1))

        if appendExtra:
            # final parameter is list 
            return args[:len(expected)-1] + [args[len(expected)-1:]]
        elif allowExtra:
            return args[:len(expected)] + [args[len(expected):]]
        else:
            return args

    def runCommand(self, *args, **kw):
        raise NotImplementedError

