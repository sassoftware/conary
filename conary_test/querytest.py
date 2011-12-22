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

from conary import errors
from conary.cmds import query
from conary.deps import deps


class QueryTest(rephelp.RepositoryHelper):
    def testPathQuery(self):
        db = self.openDatabase()
        self.addComponent('foo:runtime', '1.0', '', ['/usr/bin/foo'])
        self.updatePkg('foo:runtime')
        tups, primary = query.getTrovesToDisplay(db, [], ['/usr/bin/foo'])
        assert(tups[0][0] == 'foo:runtime')

    def testProvidesQuery(self):
        db = self.openDatabase()
        self.addComponent('foo:runtime', '1.0',
                          provides='soname: ELF32/foo.so.3(GLIBC_2.0)')
        self.updatePkg('foo:runtime')
        tups, primary = query.getTrovesToDisplay(db, [], [],
                        [deps.parseDep('soname: ELF32/foo.so.3(GLIBC_2.0)'), 
                         deps.parseDep('trove:foo:runtime')])
        assert(tups[0][0] == 'foo:runtime')

    def testExactFlavorQuery(self):
        db = self.openDatabase()
        self.addComponent('foo:runtime[~ssl]')
        self.updatePkg('foo:runtime')
        self.assertRaises(errors.TroveNotFound,
            query.getTrovesToDisplay, db, ['foo:runtime'],
             exactFlavors=True)
        tups, primary = query.getTrovesToDisplay(db, ['foo:runtime[~ssl]'], exactFlavors=True)
