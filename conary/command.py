#
# Copyright (c) 2006 rPath, Inc.
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

import os, sys, optparse
from conary.lib import options, log
from conary import state, versions

(NO_PARAM,  ONE_PARAM)  = (options.NO_PARAM, options.ONE_PARAM)
(OPT_PARAM, MULT_PARAM) = (options.OPT_PARAM, options.MULT_PARAM)

class ConaryCommand(options.AbstractCommand):
    docs = {'build-label'        : ('Use build label LABEL as default search'
                                    ' loc', 'LABEL'),
            'components'         : ('Do not hide components'),
            'config'             : ('Set config KEY to VALUE', '"KEY VALUE"'),
            'config-file'        : ('Read PATH config file', 'PATH'),
            'context'            : 'Set the current context',
            'install-label'      : ('Set the install label', 'LABEL'),
            'interactive'        : ('ask questions before performing actions '
                                    'that change system or repository state'),
            'flavors'            : 'Display complete flavors where applicable',
            'full-versions'      : ('Always display complete version strings'),
            'labels'             : 'Always display labels for versions',
            'profile'            : optparse.SUPPRESS_HELP,
            'lsprof'             : optparse.SUPPRESS_HELP,
            'pubring'            : '',
            'skip-default-config': "Don't read default configs",
            'quiet'              : ('do not display extra information when '
                                    'running'),
            'root'               : 'use conary database at location ROOT',
            }

    def addParameters(self, argDef):
        d = {}
        d['config'] = '-c', MULT_PARAM
        d['config-file'] = MULT_PARAM
        d['context'] = ONE_PARAM
        d['install-label'] = MULT_PARAM
        d['profile'] = NO_PARAM
        d['lsprof'] = NO_PARAM
        d['skip-default-config'] = NO_PARAM
        argDef[self.defaultGroup] = d

    def addConfigOptions(self, cfgMap, argDef):
        cfgMap['build-label']   = 'buildLabel', ONE_PARAM,
        cfgMap['pubring']       = 'pubRing', ONE_PARAM
        cfgMap['quiet']         = 'quiet', NO_PARAM,
        cfgMap['root']          = 'root', ONE_PARAM, '-r'
        cfgMap['flavors']       = 'fullFlavors', NO_PARAM
        cfgMap['full-versions'] = 'fullVersions', NO_PARAM
        cfgMap['interactive']   = 'interactive', NO_PARAM,
        options.AbstractCommand.addConfigOptions(self, cfgMap, argDef)

    def setContext(self, cfg, argSet):
        context = cfg.context
        where = 'specified as the default context in the conary configuration'
        if os.access('CONARY', os.R_OK):
            conaryState = state.ConaryStateFromFile('CONARY', parseSource=False)
            if conaryState.hasContext():
                context = conaryState.getContext()
                where = 'specified in the CONARY state file'

        if 'CONARY_CONTEXT' in os.environ:
            context = os.environ['CONARY_CONTEXT']
            where = 'specified in the CONARY_CONTEXT environment variable'
        if 'context' in argSet:
            context = argSet.pop('context')
            where = 'specified on the command line'

        if context:
            if not cfg.getContext(context):
                log.error('context "%s" (%s) does not exist', context, where)
                sys.exit(1)
            cfg.setContext(context)

    def processConfigOptions(self, cfg, cfgMap, argSet):
        self.setContext(cfg, argSet)

        options.AbstractCommand.processConfigOptions(self, cfg, cfgMap, argSet)
        l = []
        for labelStr in argSet.get('install-label', []):
            l.append(versions.Label(labelStr))
        if l:
            cfg.installLabelPath = l
            del argSet['install-label']

        for k,v in cfg.environment.items():
            if v == '':
                cfg.environment.pop(k)
                os.environ.pop(k, None)
                continue
            os.environ[k] = v

class ConfigCommand(ConaryCommand):
    commands = ['config']
    help = 'Display the current configuration'
    docs = {'show-contexts'  : 'display contexts as well as current config',
            'show-passwords' : 'do not mask passwords'}
    commandGroup = 'Information Display'

    def addParameters(self, argDef):
        ConaryCommand.addParameters(self, argDef)
        argDef["show-contexts"] = NO_PARAM
        argDef["show-passwords"] = NO_PARAM

    def runCommand(self, cfg, argSet, args, **kwargs):
        showPasswords = argSet.pop('show-passwords', False)
        showContexts = argSet.pop('show-contexts', False)
        try:
            prettyPrint = sys.stdout.isatty()
        except AttributeError:
            prettyPrint = False
        cfg.setDisplayOptions(hidePasswords=not showPasswords,
                              showContexts=showContexts,
                              prettyPrint=prettyPrint)
        if argSet: return self.usage()
        if (len(args) > 2):
            return self.usage()
        else:
            cfg.display()

class HelpCommand(options.AbstractCommand):
    commands = ['help']
    help = 'Display help information'
    commandGroup = 'Information Display'

    def runCommand(self, cfg, argSet, args, **kwargs):
        if len(args) == 3:
            command = args[2]
            commands = self.mainHandler._supportedCommands
            if not command in commands:
                print "%s: no such command: '%s'" % (self.mainHandler.name,
                                                     command)
                sys.exit(1)
            commands[command].usage()
        elif len(args) == 2:
            self.mainHandler.usage(showAll=True)
            return 0
        else:
            print "%s: too many arguments: '%s'" % (self.mainHandler.name,
                                                    ' '.join(args[2:]))
            sys.exit(1)

class MainHandler(options.MainHandler):
    commandList = [ ConfigCommand, HelpCommand ]
