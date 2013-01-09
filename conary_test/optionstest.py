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


import os

from conary_test import rephelp

from conary import conarycfg
from conary.lib import options

class ConaryCfgTest(rephelp.RepositoryHelper):
    cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
    argDef = {}
    cfgMap = {}
    cfgMap["cfgmap"] = "root"
    (NO_PARAM,  ONE_PARAM)  = (options.NO_PARAM, options.ONE_PARAM)
    (OPT_PARAM, MULT_PARAM) = (options.OPT_PARAM, options.MULT_PARAM)
    STRICT_OPT_PARAM = options.STRICT_OPT_PARAM
    argDef['no'] = NO_PARAM
    argDef['one'] = ONE_PARAM
    argDef['opt'] = OPT_PARAM
    argDef['mult'] = MULT_PARAM
    argDef['strict'] = STRICT_OPT_PARAM

    def usage(rc=1):
        return rc

    def testOptions(self):
        argv = ['conary', '--no', 'other1', '--one=onev', 'other2', '--opt=opt', '--mult', 'multv1', 'other3', '--mult', '--multv2', 'other4']
        argSet, otherArgs = options.processArgs(self.argDef, self.cfgMap, self.cfg, self.usage, argv=argv)
        assert(otherArgs==['conary', 'other1', 'other2', 'other3', 'other4'])
        assert(argSet['no'] is True)
        assert(argSet['one'] == 'onev')
        assert(argSet['opt'] == 'opt')
        assert(argSet['mult'] == ['multv1', '--multv2'])

    def testBadParams(self):
        argv = ['conary', '--unknown']
        try: 
            options.processArgs(self.argDef, self.cfgMap, self.cfg, 
                                                self.usage, argv=argv)
            raise RuntimeError
        except options.OptionError, msg:
            assert(msg[0] == 'no such option: --unknown')
        argv = ['conary', '--one']
        try: 
            options.processArgs(self.argDef, self.cfgMap, self.cfg, 
                                                self.usage, argv=argv)
            raise RuntimeError
        except options.OptionError, msg:
            assert(msg[0] == '--one option requires an argument')
        argv = ['conary', '--no=optone']
        try: 
            options.processArgs(self.argDef, self.cfgMap, self.cfg, 
                                                self.usage, argv=argv)
            raise RuntimeError
        except options.OptionError, msg:
            assert(msg[0] == '--no option does not take a value')


    def testOptionalParam(self):
        argv = ['conary', '--opt', '--', '--one=onev', 'other2' ]
        argSet, otherArgs = options.processArgs(self.argDef, self.cfgMap, self.cfg, self.usage, argv=argv)
        assert(otherArgs==['conary', '--one=onev', 'other2'])

        argv = ['conary', '--opt=', 'one' ]
        argSet, otherArgs = options.processArgs(self.argDef, self.cfgMap, self.cfg, self.usage, argv=argv)
        assert(otherArgs==['conary', 'one'])
        assert(argSet['opt'] is True)

        # test an optional param argument when it is the last argument on the
        # command line and no param is given
        argv = ['conary', '--opt' ]
        argSet, otherArgs = options.processArgs(self.argDef, self.cfgMap, self.cfg, self.usage, argv=argv)
        assert(argSet['opt'] is True)
        argv = ['conary', '--opt', 'foo' ]
        argSet, otherArgs = options.processArgs(self.argDef, self.cfgMap, self.cfg, self.usage, argv=argv)
        assert(argSet['opt'] == 'foo')

    def testStrictOptionalParam(self):
        argv = ['conary', '--strict', '--', '--one=onev', 'other2' ]
        argSet, otherArgs = options.processArgs(self.argDef, self.cfgMap, self.cfg, self.usage, argv=argv)
        assert(otherArgs==['conary', '--one=onev', 'other2'])

        argv = ['conary', '--strict=', 'one' ]
        argSet, otherArgs = options.processArgs(self.argDef, self.cfgMap, self.cfg, self.usage, argv=argv)
        assert(otherArgs==['conary', 'one'])
        assert(argSet['strict'] is True)

        # test an optional param argument when it is the last argument on the
        # command line and no param is given
        argv = ['conary', '--strict' ]
        argSet, otherArgs = options.processArgs(self.argDef, self.cfgMap, self.cfg, self.usage, argv=argv)
        assert(argSet['strict'] is True)
        argv = ['conary', '--strict', 'foo' ]
        argSet, otherArgs = options.processArgs(self.argDef, self.cfgMap, self.cfg, self.usage, argv=argv)
        assert(argSet['strict'] is True)


    def testCfgMap(self):
        argv = ['conary', '--cfgmap=rootval' ]
        argSet, otherArgs = options.processArgs(self.argDef, self.cfgMap, self.cfg, self.usage, argv=argv)
        assert(otherArgs==['conary'])
        assert(self.cfg.root == os.getcwd() + '/rootval')
