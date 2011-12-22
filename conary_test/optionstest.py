#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
