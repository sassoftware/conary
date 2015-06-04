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
from conary_test import rephelp

from conary.lib import javadeps

class BasicTest(testhelp.TestCase):
    def testValidTLD(self):
        self.assertEquals(javadeps._isValidTLD('foo'), False)
        self.assertEquals(javadeps._isValidTLD('org.foo.test'), True)

class JavaDepsTest(rephelp.RepositoryHelper):
    def testGetDeps(self):
        class FakeSymbolTable(javadeps._javaSymbolTable):
            def __init__(x):
                javadeps._javaSymbolTable.__init__(x)
                x.stringList = {
                            1 : 'badTld1',
                            2 : 'org/good/Tld2',
                            3 : '[LbadTld3;',
                            4 : '[Lorg/good/Tld4;',
                            6 : 'badTld5',
                            7 : '[Lorg/good/Tld6;',
                            9 : '[Lorg/good/Tld2;',
                            }

                x.classRef = dict(zip(range(1, 6), range(1, 6)))
                x.typeRef = dict((i, (i, i)) for i in range(6, 10))
                x._attributes = {}
                x.classNameIndex = 1

        mockParse = lambda x: (FakeSymbolTable(), 0)
        self.mock(javadeps, '_parseSymbolTable', mockParse)

        deps = javadeps.getDeps('fakecontents')
        self.assertEquals(deps[1], set(['org.good.Tld2',
                                        'org.good.Tld4',
                                        'org.good.Tld6']))
