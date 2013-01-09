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


"""
Tests for functions in the password module
"""

import getpass

#testsuite
from conary_test import rephelp

#conary
from conary.conaryclient import password

class PasswordTest(rephelp.RepositoryHelper):
    def testGetPassword(self):
        # CNY-2497

        userName, ret = password.getPassword('foo')
        self.assertEqual(userName, None)
        self.assertEqual(ret, None)

        def mockedGetpass(prompt):
            return prompt

        self.mock(getpass, 'getpass', mockedGetpass)
        userName, ret = password.getPassword("server.com", userName="Johnny")
        self.assertEqual(userName, "Johnny")
        self.assertEqual(ret, "Enter the password for Johnny on server.com:")
