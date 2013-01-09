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
