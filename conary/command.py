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

import os, sys
from conary.lib import options, log
from conary import state, versions

class ConaryCommand(options.AbstractCommand):
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
