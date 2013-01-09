#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import os, sys, optparse

from conary.conaryclient import cmdline
from conary.lib import options
from conary import versions

(NO_PARAM,  ONE_PARAM)  = (options.NO_PARAM, options.ONE_PARAM)
(OPT_PARAM, MULT_PARAM) = (options.OPT_PARAM, options.MULT_PARAM)

class ConaryCommand(options.AbstractCommand):
    docs = {'build-label'        : (options.VERBOSE_HELP,
                                    'Use build label LABEL as default search'
                                    ' loc', 'LABEL'),
            'components'         : (options.VERBOSE_HELP,
                                    'Do not hide components'),
            'config'             : (options.VERBOSE_HELP,
                                    'Set config KEY to VALUE', '"KEY VALUE"'),
            'config-file'        : (options.VERBOSE_HELP,
                                    'Read PATH config file', 'PATH'),
            'context'            : (options.VERBOSE_HELP,
                                    'Set the current context'),
            'exclude-troves'     : (options.VERBOSE_HELP,
                                    'Do not install troves matching REGEXP',
                                    'REGEXP'),
            'install-label'      : (options.VERBOSE_HELP,
                                    'Set the install label', 'LABEL'),
            'interactive'        : (options.VERBOSE_HELP,
                                    'ask questions before performing actions '
                                    'that change system or repository state'),
            'flavors'            : (options.VERBOSE_HELP,
                                    'Display complete flavors where applicable'),
            'full-versions'      : (options.VERBOSE_HELP,
                                    'Always display complete version strings'),
            'labels'             : (options.VERBOSE_HELP,
                                    'Always display labels for versions'),
            'profile'            : optparse.SUPPRESS_HELP,
            'lsprof'             : optparse.SUPPRESS_HELP,
            'pubring'            : (options.VERBOSE_HELP, ''),
            'skip-default-config': (options.VERBOSE_HELP,
                                    "Don't read default configs"),
            'quiet'              : (options.VERBOSE_HELP,
                                    'do not display extra information when '
                                    'running'),
            'root'               : (options.VERBOSE_HELP,
                                   'use conary database at location ROOT'),
            'trust-threshold'    : (options.VERBOSE_HELP,
                                    'Set trust threshold', 'INT')
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
        cmdline.setContext(cfg, argSet.pop('context', None),
                                 searchCurrentDir=True)

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
        argDef["show-files"] = NO_PARAM

    def runCommand(self, cfg, argSet, args, **kwargs):
        showPasswords = argSet.pop('show-passwords', False)
        showContexts = argSet.pop('show-contexts', False)
        showLineOrigins = argSet.pop('show-files', False)
        try:
            prettyPrint = sys.stdout.isatty()
        except AttributeError:
            prettyPrint = False
        cfg.setDisplayOptions(hidePasswords=not showPasswords,
                              showContexts=showContexts,
                              prettyPrint=prettyPrint,
                              showLineOrigins=showLineOrigins)
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
