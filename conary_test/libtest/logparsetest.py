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

from conary.lib import logparse
from conary_test import resources


class ConaryLogParseTest(testhelp.TestCase):

    # Note: when the log format changes, append some log entries
    # to the log and make sure that the correct number is added
    # to the length test
    def testGetConaryLogEventList(self):
        events = logparse.getConaryLogEventList(
            logparse.getConaryLogLineList(
                resources.get_archive() + '/conarylog'))
        self.assertEquals(len(events), 3136)
        # test the complete contents of a particular entry
        self.assertEquals(events[3093],
                ('Oct 02 14:56:17', [
                    'version 1.0.22: update dstat',
                    'installed dstat:runtime=/contrib.rpath.org@rpl:devel/0.6.2-2-1[]',
                    'installed dstat=/contrib.rpath.org@rpl:devel/0.6.2-2-1[]',
                    'installed dstat:doc=/contrib.rpath.org@rpl:devel/0.6.2-2-1[]',
                    'installed dstat:data=/contrib.rpath.org@rpl:devel/0.6.2-2-1[]',
                    'command complete']))
        # test the datestamp of the last entry in the initial tested format
        # DO NOT CHANGE 3132 in the next line
        self.assertEquals(events[3132][0], 'Nov 14 14:14:54')
        # When adding new formats, append lines to the log and then
        # test some of them

        # For the addition of the year to the date timestamp:
        self.assertEquals(events[3133],
                ('2006 Dec 18 14:50:54', [
                    'version 1.0.40: update inkscape=conary.rpath.com@rpl:1',
                    'updated inkscape:data=/conary.rpath.com@rpl:devel/0.44-1-1[~!inkscape.lcms is: x86]--/conary.rpath.com@rpl:devel//1/0.42.2-9-0.1[is: x86]',
                    'updated inkscape:runtime=/conary.rpath.com@rpl:devel/0.44-1-1[~!inkscape.lcms is: x86]--/conary.rpath.com@rpl:devel//1/0.42.2-9-0.1[is: x86]',
                    'updated inkscape=/conary.rpath.com@rpl:devel/0.44-1-1[~!inkscape.lcms is: x86]--/conary.rpath.com@rpl:devel//1/0.42.2-9-0.1[is: x86]',
                    'updated inkscape:doc=/conary.rpath.com@rpl:devel/0.44-1-1[~!inkscape.lcms is: x86]--/conary.rpath.com@rpl:devel//1/0.42.2-9-0.1[is: x86]',
                    'updated inkscape:locale=/conary.rpath.com@rpl:devel/0.44-1-1[~!inkscape.lcms is: x86]--/conary.rpath.com@rpl:devel//1/0.42.2-9-0.1[is: x86]',
                    'command complete']))

        # Ensure that Tracebacks are presented correctly
        self.assertEquals(events[3134][0], '2007 Jan 24 17:24:45')
        self.assertEquals(events[3134][1][0],
                          'version 1.1.15: remove logrotate')
        self.assertEquals(events[3134][1][-1],
                          'ProgrammingError: database disk image is malformed')
