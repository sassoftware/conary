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
from conary.lib.http.request import URL


class URLTest(testhelp.TestCase):

    def testUrlJoin(self):
        base = URL('https://user:password@example.com/one/two?three/four')
        self.assertEqual(base.join('foo'),
                URL('https://user:password@example.com/one/foo'))
        self.assertEqual(base.join('./foo'),
                URL('https://user:password@example.com/one/foo'))
        self.assertEqual(base.join('../foo'),
                URL('https://user:password@example.com/foo'))
        self.assertEqual(base.join('../foo?bar=baz/bork'),
                URL('https://user:password@example.com/foo?bar=baz/bork'))
        self.assertEqual(base.join('../../../../../etc/passwd'),
                URL('https://user:password@example.com/etc/passwd'))
        self.assertEqual(base.join('..'),
                URL('https://user:password@example.com'))
        self.assertEqual(base.join('/foo'),
                URL('https://user:password@example.com/foo'))
        self.assertEqual(base.join('/'),
                URL('https://user:password@example.com/'))
        self.assertEqual(base.join('//subdomain.example.com/barbaz'),
                URL('https://subdomain.example.com/barbaz'))
        self.assertEqual(base.join('//subdomain.example.com'),
                URL('https://subdomain.example.com'))
        self.assertEqual(base.join('http://fullyqualified.example.com'),
                URL('http://fullyqualified.example.com'))
        self.assertEqual(base.join('http://fullyqualified.example.com/bork'),
                URL('http://fullyqualified.example.com/bork'))

        base = URL('https://user:password@example.com/')
        self.assertEqual(base.join('foo'),
                URL('https://user:password@example.com/foo'))
        self.assertEqual(base.join('../foo'),
                URL('https://user:password@example.com/foo'))

        base = URL('https://user:password@example.com')
        self.assertEqual(base.join('foo'),
                URL('https://user:password@example.com/foo'))
