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

from conary.lib import dirset

class DirSetTest(testhelp.TestCase):

    def testDirSet(self):
        d = dirset.DirectorySet([ '/usr', '/usr/bin', '/etc', '/var/tmp' ])
        assert('/usr/bin' in d)
        assert('/usr/lib' in d)
        assert('/usr' in d)
        assert('/' not in d)
        self.assertEquals(sorted(list(d)), [ '/etc', '/usr', '/var/tmp'])

        assert('/var' not in d)
        assert('/var/tmp' in d)
        assert('/var/tmp/other' in d)
        d.add('/var')
        assert('/var' in d)
        self.assertEquals(sorted(list(d)), [ '/etc', '/usr', '/var'])

    def testDirDict(self):
        d = dirset.DirectoryDict()
        d['/etc'] = 'etc'
        d['/usr/bin'] = 'usrbin'
        d['/usr/lib'] = 'usrlib'
        self.assertEquals(d['/usr/bin/vi'], 'usrbin')
        self.assertEquals(d['/usr/bin'], 'usrbin')
        self.assertEquals(d['/usr/lib/libc'], 'usrlib')
        self.assertRaises(KeyError, d.__getitem__, '/usr')
        self.assertEquals(d.get('/usr', None), None)
        self.assertEquals(d.get('/usr/bin', None), 'usrbin')
        self.assertEquals(sorted(list(d.iterkeys())),
                          [ '/etc', '/usr/bin', '/usr/lib' ])
        self.assertEquals(sorted(list(d.itertops())),
                          [ '/etc', '/usr/bin', '/usr/lib' ])

        d['/usr'] = 'usr'
        self.assertEquals(d['/usr/bin/vi'], 'usrbin')
        self.assertEquals(d['/usr'], 'usr')
        self.assertEquals(sorted(list(d.itertops())),
                          [ '/etc', '/usr' ])
