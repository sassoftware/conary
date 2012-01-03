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
                x.typeRef = dict(zip(range(6, 10), range(6, 10)))

        mockParse = lambda x: (FakeSymbolTable(), 'JavaClass', 0)
        self.mock(javadeps, '_parseSymbolTable', mockParse)

        deps = javadeps.getDeps('fakecontents')
        self.assertEquals(deps[1], set(['org.good.Tld2',
                                        'org.good.Tld4',
                                        'org.good.Tld6']))
