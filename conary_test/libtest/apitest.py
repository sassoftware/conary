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


from conary_test import rephelp

from conary.lib import api

class ApiTest(rephelp.RepositoryHelper):
    def testPublicApiDecorator(self):
        class A:
            def a(self):
                "function test a"
            @api.publicApi
            def b(self):
                "function test b"
        foo = A()
        self.assertFalse(foo.a.__doc__.endswith("(PUBLIC API)"))
        if not foo.b.__doc__.endswith("(PUBLIC API)"):
            raise RuntimeError('foo.b.__doc__ == %r' % foo.b.__doc__)
