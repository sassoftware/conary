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
