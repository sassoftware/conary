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
