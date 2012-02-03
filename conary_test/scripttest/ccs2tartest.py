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


from testrunner.decorators import requireBinary

import os
import unittest
from conary_test import resources

#from conary.lib import util

class Ccs2TarTest(unittest.TestCase):
    def setUp(self):
        p = resources.get_path('scripts/ccs2tar')
        if not os.path.exists(p):
            p = '/usr/bin/ccs2tar'
        self.ccs2tar = p

    @requireBinary("tar")
    def testCcs2Tar(self):
        f = os.popen("%s %s/tartest.ccs | tar tf -"
                     % (self.ccs2tar, resources.get_archive()))
        fileLines = [x.strip() for x in f.readlines()]
        self.assertEquals(fileLines, [ 'dummy/', ])
