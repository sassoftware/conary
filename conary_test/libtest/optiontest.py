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


from testrunner import testhelp

#conary
from conary.lib import options
from conary import conarycfg

#test

class OptionTest(testhelp.TestCase):

    def testStrictOptionalParamTesting(self):
        parser = options.OptionParser()
        parser.add_option('--coverage',
                         help='get code coverage numbers',
                         action='callback', nargs=0,
                         callback=options.strictOptParamCallback,
                         dest='coverage')
        option, args = parser.parse_args(['foo', '--coverage', 'bar'])
        assert(option.coverage is True)
        assert(args == ['foo', 'bar'])
        option, args = parser.parse_args(['foo', '--coverage=bar'])
        assert(option.coverage == 'bar')
        assert(args == ['foo'])


    def testOptionalParamTesting(self):
        parser = options.OptionParser()
        parser.add_option('--coverage',
                         help='get code coverage numbers',
                         action='callback', nargs=0,
                         callback=options.optParamCallback,
                         dest='coverage')
        option, args = parser.parse_args(['foo', '--coverage', 'bar'])
        assert(option.coverage is 'bar')
        assert(args == ['foo'])
        option, args = parser.parse_args(['foo', '--coverage=bar'])
        assert(option.coverage == 'bar')
        assert(args == ['foo'])

    def testOptionaParamsThroughArgDef(self):
        flags, args = options._processArgs(
                             {'coverage' : options.OPT_PARAM}, {},
                             conarycfg.ConaryConfiguration(False),
                             '', argv=['foo', '--coverage', 'bar'])[0:2]
        assert(flags['coverage'] == 'bar')
        assert(args == ['foo'])
        flags, args = options._processArgs(
                             {'coverage' : options.OPT_PARAM}, {},
                             conarycfg.ConaryConfiguration(False),
                             '', argv=['foo', '--coverage=bar'])[0:2]
        assert(flags['coverage'] == 'bar')
        assert(args == ['foo'])


    def testStrictOptionaParamsThroughArgDef(self):
        flags, args = options._processArgs(
                             {'coverage' : options.STRICT_OPT_PARAM}, {},
                             conarycfg.ConaryConfiguration(False),
                             '', argv=['foo', '--coverage', 'bar'])[0:2]
        assert(flags['coverage'] == True)
        assert(args == ['foo', 'bar'])
        flags, args = options._processArgs(
                             {'coverage' : options.STRICT_OPT_PARAM}, {},
                             conarycfg.ConaryConfiguration(False),
                             '', argv=['foo', '--coverage=bar'])[0:2]
        assert(flags['coverage'] == 'bar')
        assert(args == ['foo'])
