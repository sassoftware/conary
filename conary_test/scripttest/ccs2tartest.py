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
