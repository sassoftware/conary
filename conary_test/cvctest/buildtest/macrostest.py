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


from conary_test import rephelp

from conary.build import macros

class TestUse(rephelp.RepositoryHelper):

    def testMacros(self):
        m1 = macros.Macros()
        m1.a = 'foo'
        assert(m1.a == 'foo')
        m2 = m1.copy()
        m2.a = 'bar'
        assert(m1.a == 'foo')
        assert(m2.a == 'bar')
        m3 = m2.copy(False)
        m3.a = 'baz'
        assert(m2.a == 'bar')
        assert(m3.a == 'baz')
        m4 = m3
        m4.a = 'blah'
        assert(m3.a == 'blah')
        m1.b = '%(a)s/asdf'
        assert(m1.b == 'foo/asdf')
        m1.trackChanges()
        m1.c = 'foo'
        assert(m1.getTrackedChanges() == ['c'])
        m1.trackChanges(False)
        m1.d = 'bar'
        assert(m1.getTrackedChanges() == ['c'])
        m1.e = '1'
        m1._override('e', '2')
        m1.e = '3'
        assert(m1.e == '2')
        m1.r = 'foo++'
        assert(m1.r == 'foo++')
        assert(m1['r'] == 'foo++')
        assert(str(m1['r.literalRegex']) == 'foo\+\+')
        assert(str("%(r.literalRegex)s" % m1) == 'foo\+\+')

    def testIterItems(self):
        m1 = macros.Macros()
        m1.a = 'a'
        m1.b = 'b'
        m2 = m1.copy()
        m2.c = 'c'
        iterkeys = [ x for x in m2.iterkeys() ]
        iterkeys.sort()
        assert(iterkeys == ['a', 'b', 'c'])
        keys = m2.keys()
        keys.sort()
        assert(keys == ['a', 'b', 'c'])
        iteritems = [ x for x in m2.iteritems() ]
        iteritems.sort()
        assert(iteritems == [('a', 'a'), ('b', 'b'), ('c', 'c')])

    def testUpdate(self):
        m1 = macros.Macros()
        m1.a = 'a'
        m1.b = 'b'
        m2 = m1.copy()
        m2.c = 'c'
        m3 = macros.Macros()
        m3.d = 'd'
        m3.e = 'e'
        m4 = m3.copy()
        m4.f = 'f'
        m2.update(m4)
        keys = m2.keys()
        keys.sort()
        assert(keys == ['a', 'b', 'c', 'd', 'e', 'f'])

    def testGet(self):
        m1 = macros.Macros()
        m1.march = 'i386'
        m1.target = '%(march)s-unknown-linux'
        assert(m1.target == 'i386-unknown-linux')
        assert(m1._get('target') == '%(march)s-unknown-linux')

    def testCallback(self):
        a = [1]
        m1 = macros.Macros()
        def myfun(name):
            a.append(2)
        m1.setCallback('foo', myfun)
        m1.foo = 'hello'
        assert('%(foo)s' % m1 == 'hello')
        assert(a == [1,2])
