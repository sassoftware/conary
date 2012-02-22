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

class MockTest(testhelp.TestCase):
    # Tests that self.mock() does the right thing
    # If it doesn't, either test1 or test2 will fail
    def test1(self):
        fakever = "9.99"
        import sys

        self.assertFalse(sys.version == fakever)
        self.mock(sys, "version", fakever)
        self.assertEqual(sys.version, fakever)

        self.assertFalse(hasattr(sys, 'blah'))
        self.mock(sys, "blah", 100)
        self.assertEqual(sys.blah, 100)

        self.assertFalse(hasattr(self, 'goo'))
        self.mock(self, "goo", lambda x: x + 1)
        self.assertEqual(self.goo(1), 2)

    test2 = test1

    class BaseClass1(object):
        foo = 'A'

    class ChildClass1(BaseClass1):
        pass

    class BaseClass2:
        foo = 'A'

    class ChildClass2(BaseClass2):
        foo = 'A'

    def test3(self):
        # Make sure if an attribute came in through inheritance that we're
        # restoring it corectly in child classes (BaseClass1, ChildClass1)
        # BaseClass2 and ChildClass2 make sure old-style classes work too
        # (and we don't mistakenly interpret two equal values as the same,
        # inadvertently removing the object in ChildClass2)
        self.assertEqual(self.BaseClass1.foo, "A")
        self.assertEqual(self.ChildClass1.foo, "A")
        self.assertEqual(self.BaseClass2.foo, "A")
        self.assertEqual(self.ChildClass2.foo, "A")

        self.BaseClass1.foo = 'AA'
        self.assertEqual(self.ChildClass1.foo, "AA")
        self.BaseClass1.foo = 'A'

        self.BaseClass2.foo = 'AA'
        self.assertEqual(self.ChildClass2.foo, "A")
        self.BaseClass2.foo = 'A'

        self.mock(self.ChildClass1, 'foo', 'B')
        c = self.ChildClass1()
        self.assertEqual(c.foo, "B")

        self.mock(self.ChildClass2, 'foo', 'B')
        c = self.ChildClass2()
        self.assertEqual(c.foo, "B")

        self.mock(self.BaseClass2, 'foo', 'BB')
        c = self.BaseClass2()
        self.assertEqual(c.foo, "BB")

    test4 = test3

    class BaseClassStatic1(object):
        @staticmethod
        def foo():
            return 1

    def testStaticMethods1(self):
        self.assertEqual(self.BaseClassStatic1.foo(), 1)
        self.assertEqual(self.BaseClassStatic1().foo(), 1)

        def mockFoo():
            return 2

        self.mock(self.BaseClassStatic1, 'foo', mockFoo)
        self.assertEqual(self.BaseClassStatic1.foo(), 2)
        self.assertEqual(self.BaseClassStatic1().foo(), 2)

    testStaticMethods2 = testStaticMethods1

    class BaseClassMethod1(object):
        @classmethod
        def foo(cls):
            return 1

    def testClassMethods1(self):
        self.assertEqual(self.BaseClassMethod1.foo(), 1)
        self.assertEqual(self.BaseClassMethod1().foo(), 1)

        def mockFoo(cls):
            return 2

        self.mock(self.BaseClassMethod1, 'foo', mockFoo)
        self.assertEqual(self.BaseClassMethod1.foo(), 2)
        self.assertEqual(self.BaseClassMethod1().foo(), 2)

    testClassMethods2 = testClassMethods1
