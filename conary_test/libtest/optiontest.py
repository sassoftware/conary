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
