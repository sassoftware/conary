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


from testrunner.testhelp import context

import os
import shutil
from StringIO import StringIO
import gzip

import conary_test
from conary_test import recipes
from conary_test import rephelp

from conary.cmds.clone import _convertLabelOrBranch
from conary.conaryclient import clone
from conary import conarycfg, conaryclient, errors, trove, versions
from conary.deps import deps
from conary.lib import util
from conary.repository import filecontainer
from conary.versions import VersionFromString as VFS
from conary.versions import Label

class CloneTest(rephelp.RepositoryHelper):

    @context('clone')
    def testBasic(self):
        def _get(repos, *args):
            info = repos.findTrove(self.cfg.installLabelPath, args)
            assert(len(info) == 1)

            return repos.getTrove(*info[0])

        os.chdir(self.workDir)
        self.newpkg("testcase")
        os.chdir("testcase")
        self.writeFile("testcase.recipe", recipes.testTransientRecipe1)
        self.addfile("testcase.recipe")
        self.commit()
        self.cookFromRepository('testcase')

        self.mkbranch("1.0-1", "localhost@rpl:shadow", "testcase:source", 
                      shadow = True)

        os.chdir(self.workDir)
        shutil.rmtree("testcase")
        self.checkout("testcase", "localhost@rpl:shadow")
        os.chdir("testcase")
        self.writeFile("testcase.recipe", recipes.testTransientRecipe2)
        self.commit()

        self.cfg.buildLabel = versions.Label("localhost@rpl:shadow")
        self.cookFromRepository('testcase')
        self.cfg.buildLabel = versions.Label("localhost@rpl:linux")

        self.clone('/localhost@rpl:linux', 
                   'testcase:source=localhost@rpl:shadow')
        self.clone('/localhost@rpl:linux', 
                   'testcase=localhost@rpl:shadow',
                   fullRecurse=False)

        # recloning should noop 
        self.logCheck(self.clone,
                      ('/localhost@rpl:linux', 
                       'testcase:source=localhost@rpl:shadow',
                       'testcase=localhost@rpl:shadow'),
                      "warning: Nothing to clone!")

        repos = self.openRepository()

        trv = _get(repos, 'testcase:source', '1.1-1', None)
        assert(trv.getVersion().asString() == '/localhost@rpl:linux/1.1-1')
        assert([ x[3].asString() for x in trv.iterFileList() ] ==
                    [ '/localhost@rpl:linux/1.1-1' ] )

        trv = _get(repos, 'testcase', '1.1-1-1', None)
        assert(trv.getVersion().asString() == '/localhost@rpl:linux/1.1-1-1')
        assert(set([ x[1].asString() for x \
                        in trv.iterTroveList(strongRefs=True) ]) ==
                                set([ '/localhost@rpl:linux/1.1-1-1' ]) )

        trv = _get(repos, 'testcase:runtime', '1.1-1-1', None)
        assert(trv.getVersion().asString() == '/localhost@rpl:linux/1.1-1-1')
        assert(set([ x[3].asString() for x in trv.iterFileList() ]) ==
                    set([ '/localhost@rpl:linux/1.1-1-1' ]) )

        self.cookFromRepository('testcase')

        trv = _get(repos, 'testcase:runtime', '1.1-1-2', None)
        assert(trv.getVersion().asString() == '/localhost@rpl:linux/1.1-1-2')
        assert(set([ x[3].asString() for x in trv.iterFileList() ]) ==
               set([ '/localhost@rpl:linux/1.1-1-1' ]) )

        try:
            self.clone('/localhost@rpl:linux//localhost@rpl:shadow', 
                       'testcase:source=localhost@rpl:linux')
            assert 0, "Should have raised CloneError"
        except clone.CloneError, msg:
            assert(str(msg) == ("clone only supports cloning troves to sibling"
                                " branches, parents, and siblings of parent"
                                " branches"))

        os.chdir(self.workDir)
        shutil.rmtree("testcase")
        self.mkbranch("localhost@rpl:shadow", "localhost@rpl:deepshadow", 
                      "testcase:source", shadow = True)
        self.checkout("testcase", "localhost@rpl:deepshadow")
        os.chdir("testcase")
        self.writeFile("testcase.recipe", recipes.testTransientRecipe3)
        self.commit()

        self.cfg.buildLabel = versions.Label("localhost@rpl:deepshadow")
        self.cookFromRepository('testcase')
        self.cfg.buildLabel = versions.Label("localhost@rpl:linux")

        self.clone('/localhost@rpl:linux//localhost@rpl:shadow', 
                   'testcase:source=localhost@rpl:deepshadow',
                   'testcase=localhost@rpl:deepshadow',
                   fullRecurse=False)

        trv = _get(repos, 'testcase:source', 'localhost@rpl:shadow', None)
        assert(trv.getVersion().asString() == 
                      '/localhost@rpl:linux//shadow/1.2-0.1')
        assert([ x[3].asString() for x in trv.iterFileList() ] ==
                    [ '/localhost@rpl:linux//shadow/1.2-0.1' ] )

        trv = _get(repos, 'testcase:runtime', 'localhost@rpl:shadow', None)
        assert(trv.getVersion().asString() == 
                      '/localhost@rpl:linux//shadow/1.2-0.1-1')

        repos = self.openRepository(1)
        self.mkbranch("localhost@rpl:deepshadow", "localhost1@rpl:distshadow", 
                      "testcase:source", shadow = True)

        os.chdir(self.workDir)
        shutil.rmtree("testcase")
        self.checkout("testcase", "localhost1@rpl:distshadow")
        os.chdir("testcase")
        self.writeFile("testcase.recipe", recipes.testTransientRecipe4)
        self.commit()

        self.cfg.buildLabel = versions.Label("localhost1@rpl:distshadow")
        self.cookFromRepository('testcase')
        self.cfg.buildLabel = versions.Label("localhost@rpl:linux")

        trv = _get(repos, 'testcase:runtime', 'localhost1@rpl:distshadow', 
                   None)
        self.clone('/localhost@rpl:linux//localhost@rpl:shadow//localhost@rpl:deepshadow', 
                   'testcase:source=localhost1@rpl:distshadow',
                   'testcase=localhost1@rpl:distshadow')
        self.updatePkg(self.rootDir, 'testcase:runtime',
                       version = 'localhost@rpl:deepshadow')

        self.clone('/localhost@rpl:sibling',
                   'testcase:source=localhost@rpl:linux')
        trv = _get(repos, 'testcase:source', 'localhost@rpl:sibling', None)
        assert(trv.getVersion().asString() == '/localhost@rpl:sibling/1.1-1')
        assert([ x[3].asString() for x in trv.iterFileList() ] ==
                    [ '/localhost@rpl:sibling/1.1-1' ] )

    @context('clone')
    def testFileVersions(self):
        def _get(repos, *args):
            info = repos.findTrove(self.cfg.installLabelPath, args)
            assert(len(info) == 1)

            return repos.getTrove(*info[0])

        repos = self.openRepository()

        self.addQuickTestComponent('test:source', '1.0-1', 
                   fileContents = [ ('src1', 'contents1-1.0-1', "1.0-1"),
                                    ('src2', 'contents2-1.0-1', "1.0-1") ])
        self.addQuickTestComponent('test:runtime', '1.0-1', 
                   fileContents = [ ('bin1', 'contents1-1.0-1', "1.0-1"),
                                    ('bin2', 'contents2-1.0-1', "1.0-1") ])
        self.addCollection('test', '1.0-1', [':runtime'])
        self.clone('/localhost@rpl:sibling',
                   'test:source=localhost@rpl:linux',
                   'test=localhost@rpl:linux')

        self.addQuickTestComponent('test:source', '1.1-1', 
                   fileContents = [ ('src1', 'contents1-1.0-1', "1.0-1"),
                                    ('src2', 'contents2-1.1-1', "1.1-1") ])
        self.addQuickTestComponent('test:runtime', '1.1-1', 
                   fileContents = [ ('bin1', 'contents1-1.0-1', "1.0-1"),
                                    ('bin2', 'contents2-1.1-1', "1.1-1") ])
        self.addCollection('test', '1.1-1', [':runtime'])

        self.clone('/localhost@rpl:sibling',
                   'test:source=localhost@rpl:linux',
                   'test=localhost@rpl:linux')

        trv = _get(repos, 'test:source', 'localhost@rpl:sibling', None)
        assert(trv.getVersion().asString() == '/localhost@rpl:sibling/1.1-1')
        assert(sorted([ (x[1], x[3].asString()) for x in trv.iterFileList() ])\
                == [ ("src1", '/localhost@rpl:sibling/1.0-1' ),
                     ("src2", '/localhost@rpl:sibling/1.1-1') ])
        
        trv = _get(repos, 'test:runtime', 'localhost@rpl:sibling', None)
        assert(trv.getVersion().asString() == '/localhost@rpl:sibling/1.1-1-1')
        assert(sorted([ (x[1], x[3].asString()) for x in trv.iterFileList() ])\
                == [ ("/bin1", '/localhost@rpl:sibling/1.0-1-1' ),
                     ("/bin2", '/localhost@rpl:sibling/1.1-1-1') ])

        # Misa: as of CNY-1294 - cloning keeps track of the parent not the 
        # absolute parent
        self.clone('/localhost@rpl:sibling2',
                   'test:source=localhost@rpl:sibling')
        trv = _get(repos, 'test:source', 'localhost@rpl:sibling2', None)
        self.failUnlessEqual(trv.troveInfo.clonedFrom().asString(),
                             '/localhost@rpl:sibling/1.1-1')
        self.failUnlessEqual([ str(x) for x in trv.troveInfo.clonedFromList ],
            [ '/localhost@rpl:linux/1.1-1', '/localhost@rpl:sibling/1.1-1' ])

    @context('clone')
    def testSingleBranchClone(self):
        def _get(repos, *args):
            info = repos.findTrove(self.cfg.installLabelPath, args)
            assert(len(info) == 1)

            return repos.getTrove(*info[0])

        repos = self.openRepository()

        self.addQuickTestComponent('test:source', '1.0-1', 
                   fileContents = [ ('src1', 'contents1-1.0-1', "1.0-1"),
                                    ('src2', 'contents2-1.0-1', "1.0-1") ])

        self.addQuickTestComponent('test:source', '1.1-1', 
                   fileContents = [ ('src1', 'contents1-1.0-1', "1.0-1"),
                                    ('src2', 'contents2-1.1-1', "1.1-1") ])

        self.clone('/localhost@rpl:linux', 'test:source=1.0-1')

        trv = _get(repos, 'test:source', 'localhost@rpl:linux', None)
        assert(trv.getVersion().asString() == '/localhost@rpl:linux/1.0-2')
        assert(sorted([ (x[1], x[3].asString()) for x in trv.iterFileList() ])\
                == [ ("src1", '/localhost@rpl:linux/1.0-1' ),
                     ("src2", '/localhost@rpl:linux/1.0-1') ])
        assert(trv.troveInfo.clonedFrom().asString() ==
               '/localhost@rpl:linux/1.0-1')

    def _get(self, repos, *args):
        info = repos.findTrove(self.cfg.installLabelPath, args)
        assert(len(info) == 1)

        return repos.getTrove(*info[0])

    @context('clone')
    def testRequirementsRewrite(self):
        repos = self.openRepository()

        self.addQuickTestComponent("req:source", "2.0-1", filePrimer = 0)
        self.addQuickTestComponent("req:runtime", "2.0-1-1", filePrimer = 1)
        self.updatePkg(self.rootDir, "req:runtime")
        self.addCollection('req', '2.0-1-1', [':runtime'])

        self.addQuickTestComponent("load:source", "3.0-1",
                    fileContents = [ ('load.recipe', 
"""
class LoadRecipe(PackageRecipe):
    name = "load"
    version = "1.0"
""")])

        self.addQuickTestComponent("foo:source", "1.0-1",
                    fileContents = [ ('foo.recipe',
"""
loadRecipe('load')
class FooRecipe(LoadRecipe):
    name = "foo"
    version = "1.0"
    clearBuildReqs()
    buildRequires = [ 'req:runtime' ]
    def setup(r):
        r.Create("/etc/config")
        r.ComponentSpec('runtime', '/etc/config')
""")])
        self.cookFromRepository("foo")
        self.clone('/localhost@rpl:target', 'foo:source=1.0-1',
                       'foo=1.0-1-1', 'req=2.0-1-1', 
                       'req:source=2.0-1', 'load:source')
        trv = self._get(repos, 'foo', 'localhost@rpl:target', None)
        assert([ x[1].asString() for x in trv.getLoadedTroves() ] 
            == [ '/localhost@rpl:target/3.0-1' ])
        assert([ x[1].asString() for x in trv.getBuildRequirements() ] 
            == [ '/localhost@rpl:target/2.0-1-1' ])

        # we're rewriting the loaded recipe but not the build requirements
        self.clone('/localhost@rpl:target2', 'foo:source=1.0-1',
                        'foo=1.0-1-1', 'load:source')
        trv = self._get(repos, 'foo', 'localhost@rpl:target2', None)
        assert([ x[1].asString() for x in trv.getLoadedTroves() ]
            == [ '/localhost@rpl:target2/3.0-1' ])
        assert([ x[1].asString() for x in trv.getBuildRequirements() ] 
            == [ '/localhost@rpl:linux/2.0-1-1' ])

        # we're rewriting the build requirements but not the loaded recipe
        self.clone('/localhost@rpl:target3', 'foo:source=1.0-1',
                        'foo=1.0-1-1', 'req:source=2.0-1', 'req=2.0-1-1')
        trv = self._get(repos, 'foo', 'localhost@rpl:target3', None)
        assert([ x[1].asString() for x in trv.getLoadedTroves() ]
            == [ '/localhost@rpl:linux/3.0-1' ])
        assert([ x[1].asString() for x in trv.getBuildRequirements() ] 
            == [ '/localhost@rpl:target3/2.0-1-1' ])


        # force the following reclone to work by putting something else on
        # head
        self.addQuickTestComponent("foo:runtime", 
                                   "/localhost@rpl:target/1.0-1-2")
        self.addQuickTestCollection("foo", "/localhost@rpl:target/1.0-1-2", 
                        [ ("foo:runtime", "/localhost@rpl:target/1.0-1-2") ] )

        # everything should be rewritten since the other troves have already
        # been cloned, and are still on head
        self.clone('/localhost@rpl:target', 'foo=1.0-1-1')
        trv = self._get(repos, 'foo', 'localhost@rpl:target', None)
        assert([ x[1].asString() for x in trv.getLoadedTroves() ] 
            == [ '/localhost@rpl:target/3.0-1' ])
        assert([ x[1].asString() for x in trv.getBuildRequirements() ] 
            == [ '/localhost@rpl:target/2.0-1-1' ])

    @context('clone')
    def testGroupClone(self):
        repos = self.openRepository()
        
        self.addComponent("foo:source", "2.0-1")
        self.addComponent("foo:runtime", "2.0-1-1")
        self.addCollection("foo", "2.0-1-1", [ ("foo:runtime" ) ] )
        self.addComponent("group-a:source", "1.0-1")
        self.addCollection("group-a", "1.0-1-1",
                           [ ("foo", "2.0-1-1" ) ],
                           labelPath=[versions.Label('localhost@rpl:linux')])
        
        # this works recursively on group-a! no need to include everything
        # explicitly
        self.clone('/localhost@rpl:target', 'group-a', 'group-a:source',
                   'foo:source', fullRecurse=True)
        trv = self._get(repos, 'group-a', 'localhost@rpl:target', None)
        self.failUnlessEqual(set(x[1].asString() for x in trv.iterTroveList(strongRefs=True)),
                             set(['/localhost@rpl:target/2.0-1-1']))
        labelPath = trv.getLabelPath()
        self.failUnlessEqual(list(labelPath), [Label('localhost@rpl:target')])
        
    @context('clone')
    def testGroupClone2(self):
        repos = self.openRepository()

        self.addQuickTestComponent("foo:source", "2.0-1")
        self.addQuickTestComponent("foo:runtime", "2.0-1-1")
        self.addQuickTestCollection("foo", "2.0-1-1", [ ("foo:runtime" ) ] )
        self.addQuickTestComponent("group-a:source", "1.0-1")
        self.addQuickTestCollection("group-a", "1.0-1-1", 
                                    [ ("foo", "2.0-1-1" ) ] )


        # this works recursively on group-a! no need to include everything
        # explicitly
        self.clone('/localhost@rpl:target', 'group-a', cloneSources=True,
                   fullRecurse=True)
        trv = self._get(repos, 'group-a', 'localhost@rpl:target', None)
        assert(set(x[1].asString() for x in trv.iterTroveList(strongRefs=True)) 
                == set(['/localhost@rpl:target/2.0-1-1']))

        self.addQuickTestCollection("group-a", "1.0-1-2", 
                                    [ ("foo:runtime", "2.0-1-1" ) ],
                                    weakRefList = [ ("foo", "2.0-1-1" ) ] )
        self.clone('/localhost@rpl:target', 'group-a', fullRecurse=True)
        trv = self._get(repos, 'group-a', 'localhost@rpl:target', None)
        assert(set(x[1].asString() for x in trv.iterTroveList(strongRefs=True)) 
                == set(['/localhost@rpl:target/2.0-1-1']))

    @context('clone')
    def testGroupClone3(self):
        def _assertGetFails(repos, *args):
            try:
                info = repos.findTrove(self.cfg.installLabelPath, args)
            except errors.TroveNotFound:
                pass
            else:
                assert(0)

        repos = self.openRepository()

        self.addComponent("test:runtime", "2.0-1-1")
        self.addCollection("test", "2.0-1-1", [ ("test:runtime" ) ] )
        self.addComponent("group-first:source", "1.0-1",'',
                          [('group-first.recipe',
                           rephelp.RegularFile(
                                    contents=recipes.basicSplitGroup))])
        self.cookItem(repos, self.cfg, 'group-first')

        # this works recursively on group-a! no need to include everything
        # explicitly
        self.clone('/localhost@rpl:target', 'group-first',
                   fullRecurse=False, cloneSources=True)
        trv = self._get(repos, 'group-first', 'localhost@rpl:target', None)
        trv = self._get(repos, 'group-second', 'localhost@rpl:target', None)
        _assertGetFails(repos, 'test', 'localhost@rpl:target', None)

    @context('clone')
    def testGroupClone4(self):
        repos = self.openRepository()

        self.addComponent("foo:source", "2.0-1")
        self.addComponent("foo:runtime", "2.0-1-1")
        self.addComponent("foo:lib", "2.0-1-1")
        self.addComponent("foo:devel", "2.0-1-1")
        self.addCollection("foo", "2.0-1-1", [ "foo:runtime", "foo:lib", "foo:devel" ],
                           sourceName = "foo:source")

        self.addComponent("bar:source", "2.1-2")
        self.addComponent("bar:runtime", "2.1-2-1")
        self.addComponent("bar:lib", "2.1-2-1")
        self.addComponent("bar:data", "2.1-2-1")
        self.addComponent("bar:doc", "2.1-2-1")
        self.addCollection("bar", "2.1-2-1", [ "bar:runtime", "bar:lib", "bar:data", "bar:doc" ],
                           sourceName = "bar:source")

        self.addComponent("group-inc:source", "1.1-1")
        self.addCollection("group-inc", "1.1-1-1", [
            ("foo", "2.0-1-1" ),
            ("bar:lib", "2.1-2-1"),
            ("bar:data", "2.1-2-1"),
            ("bar", "2.1-2-1")
            ], labelPath=[versions.Label('localhost@rpl:linux')])

        self.addComponent("group-a:source", "1.0-1")
        self.addCollection("group-a", "1.0-1-1", [
            ("group-inc", "1.1-1-1"),
            ("bar:runtime", "2.1-2-1"),
            ("bar:doc", "2.1-2-1"),
            ], labelPath=[versions.Label('localhost@rpl:linux')])
        # clone the sources over
        self.clone('/localhost@rpl:target', 'foo:source', "bar:source") 
        # clone group-a and figure out if the clone suceeded correctly
        self.clone('/localhost@rpl:target', 'group-a',
                   'group-a:source', "group-inc:source",
                   fullRecurse=True)
        trv = self._get(repos, 'group-a', 'localhost@rpl:target', None)
        self.failUnlessEqual(set(x[1].asString() for x in trv.iterTroveList(strongRefs=True)),
                             set(['/localhost@rpl:target/1.1-1-1',
                                  '/localhost@rpl:target/2.1-2-1',
                                  ] ))
        labelPath = trv.getLabelPath()
        self.failUnlessEqual(list(labelPath), [Label('localhost@rpl:target')])
        # check out that all the bar troves have made it in correctly.
        trvsrc = self._get(repos, 'bar:source', 'localhost@rpl:target', None)
        trvbin = self._get(repos, 'bar:runtime', 'localhost@rpl:target', None)
        trvbars = repos.getTrovesBySource(trvsrc.getName(), trvsrc.getVersion())
        self.failUnlessEqual(set([v for n,v,f in trvbars]),
                             set([trvbin.getVersion()]))
        self.failUnlessEqual(set([n for n,v,f in trvbars]),
                             set(["bar:runtime", "bar:lib", "bar:data",
                                  "bar:doc", "bar"]))

    @context('clone')
    def testSourceClone(self):
        repos = self.openRepository()

        self.makeSourceTrove("testcase", recipes.testTransientRecipe1)
        self.clone('/localhost@rpl:target', 'testcase:source')
        self.updateSourceTrove("testcase", recipes.testTransientRecipe2,
                               '/localhost@rpl:target')
        # make sure the clonedFrom attribute got reset
        trv = self._get(repos, 'testcase:source', 'localhost@rpl:target', None)
        assert(trv.troveInfo.clonedFrom() is None)
        assert(list(trv.troveInfo.clonedFromList) == [])

    @context('clone')
    def testSiblingShadowVersions(self):
        repos = self.openRepository()

        self.addComponent("foo:source", 
                        "/localhost@rpl:devel//shadow/2.0-3")
        self.addComponent("foo:source", 
                        "/localhost@rpl:devel//shadow/2.0-3.1")
        self.clone('/localhost@rpl:devel//target', 
                   'foo:source=localhost@rpl:shadow')
        trv = self._get(repos, 'foo:source', 'localhost@rpl:target', None)
        assert(trv.getVersion().asString() == 
                    '/localhost@rpl:devel//target/2.0-3.1')

        self.addComponent("foo:source", 
                        "/localhost@rpl:devel//shadow/3.0-1")
        self.clone('/localhost@rpl:devel//target', 
                   'foo:source=localhost@rpl:shadow')
        trv = self._get(repos, 'foo:source', 'localhost@rpl:target', None)
        assert(trv.getVersion().asString() == 
                    '/localhost@rpl:devel//target/3.0-1')

        self.addComponent("foo:source", 
                        "/localhost@rpl:devel//shadow/3.0-1.1")
        self.clone('/localhost@rpl:devel//target', 
                   'foo:source=localhost@rpl:shadow')
        trv = self._get(repos, 'foo:source', 'localhost@rpl:target', None)
        assert(trv.getVersion().asString() == 
                    '/localhost@rpl:devel//target/3.0-1.1')

        self.resetRepository()
        self.addComponent("foo:source", 
                        "/localhost@rpl:devel//shadow//shadow2/5.0-7")
        self.addComponent("foo:source", 
                        "/localhost@rpl:devel//shadow//shadow2/5.0-7.7.1")
        self.clone('/localhost@rpl:devel//shadow//target',
                   'foo:source=localhost@rpl:shadow2')
        trv = self._get(repos, 'foo:source', 'localhost@rpl:target', None)
        assert(trv.getVersion().asString() == 
                    '/localhost@rpl:devel//shadow//target/5.0-7.7.1')

        self.resetRepository()
        trv = self.addComponent("foo:source", 
                        "/localhost@rpl:devel/5.0-7/shadow//shadow2/7")
        self.clone('/localhost@rpl:devel/5.0-7/shadow//target',
                   'foo:source=localhost@rpl:shadow2')
        trv = self._get(repos, 'foo:source', 'localhost@rpl:target', None)
        assert(trv.getVersion().asString() == 
                    '/localhost@rpl:devel/5.0-7/shadow//target/7')


    @context('clone')
    def testUphillShadowVersions(self):
        repos = self.openRepository()

        self.addQuickTestComponent("foo:source", 
                        "/localhost@rpl:devel//shadow/2.0-3")
        self.addQuickTestComponent("foo:source", 
                        "/localhost@rpl:devel//shadow/2.0-3.1")
        self.clone('/localhost@rpl:devel',
                   'foo:source=localhost@rpl:shadow')
        trv = self._get(repos, 'foo:source', 'localhost@rpl:devel', None)
        assert(trv.getVersion().asString() == 
                    '/localhost@rpl:devel/2.0-3')

        self.addQuickTestComponent("foo:source", 
                        "/localhost@rpl:devel//shadow/3.0-1")
        self.clone('/localhost@rpl:devel/', 
                   'foo:source=localhost@rpl:shadow')
        trv = self._get(repos, 'foo:source', 'localhost@rpl:devel', None)
        assert(trv.getVersion().asString() == 
                    '/localhost@rpl:devel/3.0-1')

        self.addQuickTestComponent("foo:source", 
                        "/localhost@rpl:devel//shadow/3.0-1.1")
        self.clone('/localhost@rpl:devel/', 
                   'foo:source=localhost@rpl:shadow')
        trv = self._get(repos, 'foo:source', 'localhost@rpl:devel', None)
        assert(trv.getVersion().asString() == 
                    '/localhost@rpl:devel/3.0-2')

        self.addQuickTestComponent("foo:source", 
                        "/localhost@rpl:devel//shadow//shadow2/5.0-7")
        self.addQuickTestComponent("foo:source", 
                        "/localhost@rpl:devel//shadow//shadow2/5.0-7.0.1")
        self.clone('/localhost@rpl:devel',
                   'foo:source=localhost@rpl:shadow2')
        trv = self._get(repos, 'foo:source', 'localhost@rpl:devel', None)
        assert(str(trv.getVersion()) == '/localhost@rpl:devel/5.0-7')

    @context('clone')
    def testUphillShadowBinaries(self):
        repos = self.openRepository()
        self.addComponent("foo:source", 
                        "/localhost@rpl:devel/2.0-3")
        #self.addComponent("foo:source", 
        #                "/localhost@rpl:devel//shadow/2.0-3")
        self.addComponent("foo:runtime", 
                                "/localhost@rpl:devel//shadow/2.0-3-1")
        self.addCollection('foo', "/localhost@rpl:devel//shadow/2.0-3-1", 
                           [':runtime'])
        self.clone('/localhost@rpl:devel', 'foo=:shadow')
        trv = self._get(repos, 'foo', 'localhost@rpl:devel', None)
        assert(str(trv.getVersion()) == '/localhost@rpl:devel/2.0-3-1')

        self.addComponent("foo:source", 
                          "/localhost@rpl:devel//shadow/2.0-3.1")
        self.addComponent("foo:runtime",
                                "/localhost@rpl:devel//shadow/2.0-3.1-0.1")
        self.addCollection('foo', "/localhost@rpl:devel//shadow/2.0-3.1-0.1", 
                           [':runtime'])
        # we modified the source on the shadow, we need to shadow that back.
        try:
            self.clone('/localhost@rpl:devel', 'foo=:shadow')
            assert 0, 'should have raised error'
        except clone.CloneError, msg:
              assert(str(msg) == "Cannot find cloned source for foo:source=/localhost@rpl:devel//shadow/2.0-3.1")



    @context('clone')
    def testLatestComponentVersDiffer(self):
        # foo:config was cloned, then another version of foo was built
        # that did not result in a foo:config component.  When recloning
        # foo (and foo:config), make sure foo:config is recloned even though
        # the latest version on the target branch is an exact duplicate 
        # (foo and foo:config version #s must match)

        self.addQuickTestComponent("foo:source", "1.0-1", filePrimer = 0)
        self.addQuickTestComponent("foo:runtime", "1.0-1-1", filePrimer = 1)
        self.addQuickTestComponent("foo:config",  "1.0-1-1", filePrimer = 2)
        self.addQuickTestCollection("foo", '1.0-1-1', ['foo:runtime', 
                                                       'foo:config'])
        self.clone('/localhost@rpl:target',
                   'foo:source', 'foo')

        self.addQuickTestComponent("foo:runtime", ":target/1.0-1-2", 
                                                    filePrimer = 1)
        self.addQuickTestCollection("foo", ':target/1.0-1-2', ['foo:runtime'])

        self.clone('/localhost@rpl:target', 'foo')

        repos = self.openRepository()
        trv = self._get(repos, 'foo', 'localhost@rpl:target', None)
        for (n, v, f) in trv.iterTroveList(strongRefs=True):
            assert(v == trv.getVersion())

    @context('clone')
    def testLastestComponentVersDiffer2(self):
        # foo:runtime and foo have different latest versions on the target
        # branch.  Ensure they have the same version after being cloned.
        self.addQuickTestComponent("foo:source", "1.0-1", filePrimer = 0)
        self.addQuickTestComponent("foo:runtime", "1.0-1-1", filePrimer = 1)
        self.addQuickTestComponent("foo:config",  "1.0-1-1", filePrimer = 2)
        self.addQuickTestCollection("foo", '1.0-1-1', ['foo:runtime', 
                                                       'foo:config'])
        self.clone('/localhost@rpl:target', 'foo:source')

        self.addQuickTestComponent("foo:runtime", ":target/1.0-1-1", 
                                                    filePrimer = 1)
        self.addQuickTestCollection("foo", ':target/1.0-1-1', ['foo:runtime'])

        self.addQuickTestComponent("foo:config", ":target/1.0-1-2", 
                                                    filePrimer = 1)
        self.addQuickTestCollection("foo", ':target/1.0-1-2', ['foo:config'])

        self.clone('/localhost@rpl:target', 'foo')

        repos = self.openRepository()
        trv = self._get(repos, 'foo', 'localhost@rpl:target', None)
        for (n, v, f) in trv.iterTroveList(strongRefs=True):
            assert(v == trv.getVersion())

    @context('clone')
    def testCloneToSameBranch(self):
        repos = self.openRepository()
        self.addQuickTestComponent("foo:source", "1.0-1", filePrimer = 0)
        self.addQuickTestComponent("foo:runtime", "1.0-1-1", filePrimer = 1)
        self.addQuickTestComponent("foo:config",  "1.0-1-1", filePrimer = 2)
        self.addQuickTestCollection("foo", '1.0-1-1', ['foo:runtime', 
                                                       'foo:config'])
        trv1 = self._get(repos, 'foo', str(self.cfg.buildLabel), None)
        v1 = trv1.getVersion()

        self.logCheck2("warning: Nothing to clone!",
                       self.clone,'/%s' % self.cfg.buildLabel, 'foo')

    @context('clone')
    def testCloneNoFileContents(self):
        # test cloning a file with no file contents
        # this tickled a bug where we were not lining up fileObj with their fileIds correctly
        # in cloning, but that bug was dependent on fileId ordering
        self.openRepository(1)
        testRecipe=r"""\
class TestRecipe(PackageRecipe):
    name = 'testcase'
    version = '1.0'
    clearBuildReqs()
    def setup(r):
        r.MakeDirs('/blam')
        r.ExcludeDirectories(exceptions='/blam')
        r.Create('/foo', contents='bar')
        r.Create('/zed/foo', contents='bam')
"""
        self.makeSourceTrove('testcase', testRecipe)
        self.cookFromRepository('testcase')
        self.clone('/localhost1@rpl:linux', 'testcase', 'testcase:source')


    @context('clone')
    def testCloneComponent(self):
        self.makeSourceTrove('simple', recipes.simpleRecipe)
        self.cookFromRepository('simple')
        try:
            self.clone('/localhost@rpl:clone', 'simple:runtime')
        except errors.ParseError, err:
            assert(str(err) == 'Cannot clone components: simple:runtime')

    @context('clone')
    def testUphillShadowBinariesWithSiblingBuildReqs(self):
        repos = self.openRepository()
        myRecipe = recipes.simpleRecipe
        myRecipe = myRecipe.replace('clearBuildReqs()', 
                    'clearBuildReqs(); buildRequires = ["foo:runtime=:shadow2"]')
        self.addComponent("simple:source", 
                        "/localhost@rpl:devel/2.0-3", '',
                        [('simple.recipe', myRecipe)])
        self.addComponent("simple:source", 
                        "/localhost@rpl:devel//shadow/2.0-3",'',
                        [('simple.recipe', myRecipe)])
        self.addComponent("foo:runtime", 
                          "/localhost@rpl:devel/2.0-3-1")
        self.addComponent("foo:runtime", 
                          "/localhost@rpl:devel//shadow2/2.0-3-1")
        self.updatePkg('foo:runtime=:shadow2')
        self.cookFromRepository('simple=:shadow')

        # conary used to complain that this clone was "incomplete" 
        # because the buildreq on :shadow2 also could possibly be cloned
        # onto :devel.  But of course that was a bug (CNY-499)
        self.clone('/localhost@rpl:devel', 'simple=:shadow')

    @context('clone')
    def testErrors(self):
        try:
            self.clone('/localhost@rpl:devel/1.0-1-1', 'simple=:shadow')
        except errors.ParseError, err:
            assert(str(err) == 'Cannot specify full version "/localhost@rpl:devel/1.0-1-1" to clone to - must specify target branch')
        else:
            assert(0)



    @context('clone')
    def testInfoOnly(self):
        self.addQuickTestComponent('test:source', '1.0-1', 
                   fileContents = [ ('src1', 'contents1-1.0-1', "1.0-1"),
                                    ('src2', 'contents2-1.0-1', "1.0-1") ])
        txt, rc = self.captureOutput(self.clone, '/localhost@rpl:sibling',
                                    'test:source=localhost@rpl:linux', info=True)
    def _setupChangeLog(self, message):
        editor = self.workDir + '/changelog'
        if message:
            open(editor, 'w').write('''
#!/bin/sh
WORKDIR="%s"
cat >$WORKDIR/output <<EOF
%s
EOF
cat $1 >> $WORKDIR/output
mv $WORKDIR/output $1
''' % (self.workDir, message))
        else:
            open(editor, 'w').write('#!/bin/sh\n') # do nothing
        os.chmod(editor, 0755)
        oldEditor = os.environ.get('EDITOR', None)
        os.environ['EDITOR'] = editor
        return oldEditor

    @context('clone')
    def testCallback(self):
        def _get(repos, *args):
            info = repos.findTrove(self.cfg.installLabelPath, args)
            assert(len(info) == 1)

            return repos.getTrove(*info[0])


        self.addQuickTestComponent('test:source', '1.0-1', 
                   fileContents = [ ('src1', 'contents1-1.0-1', "1.0-1"),
                                    ('src2', 'contents2-1.0-1', "1.0-1") ])

        txt, rc = self.captureOutput(self.clone, '/localhost@rpl:sibling',
                                     'test:source=localhost@rpl:linux',
                                     verbose=True, test=True)
        # annoyingly, if the output is not a tty, we don't get any
        # feedback...so, to really test this, I'd have to change to
        # a capture output method that mimiced a tty.
        oldEditor = self._setupChangeLog('blam!\n')
        txt, rc = self.captureOutput(self.clone, '/localhost@rpl:sibling',
                                     'test:source=localhost@rpl:linux',
                                     verbose=True, message=None)
        repos = self.openRepository()
        trv = _get(repos, 'test:source', 'localhost@rpl:sibling', None)
        assert(trv.getChangeLog().getMessage() == 'blam!\n')

        # test when we don't give a message
        self._setupChangeLog('')
        self.logFilter.add()
        txt, rc = self.captureOutput(self.clone, '/localhost@rpl:sibling2',
                                     'test:source=localhost@rpl:sibling',
                                     verbose=True, message=None)
        self.logFilter.compare(['error: no change log message was given for test:source.'])
        if oldEditor is not None:
            os.environ['EDITOR'] = oldEditor

    @context('clone')
    @context('labelmultiplicity')
    def testCloneCreatesLabelMultiplicity(self):
        target = 'localhost@rpl:target'
        self.addComponent('foo:source', '/localhost@rpl:linux/1-1')
        self.addComponent('foo:source', '/localhost@rpl:linux//target/1-1')
        rc, txt = self.captureOutput(self.clone, '/localhost@rpl:target',
                                     'foo:source=localhost@rpl:linux', 
                                     ignoreConflicts=False)
        assert(not txt)

    @context('clone')
    @context('labelmultiplicity')
    def testCloneFlavorsFalseLabelMultiplicity(self):
        target = '/localhost@rpl:target'
        repos = self.openRepository()

        version = '1.0-1'
        troveNames = ['foo', ]
        flavors = ['is: x86', 'is: x86_64']
        comps = [':runtime', ]
        compslen = len(comps)

        def _createTroves(label):
            ver = '%s/%s' % (label, version)
            for tn in troveNames:
                # Source components don't have flavors
                self.addComponent(tn + ':source', ver, '', filePrimer=compslen)
                for fl in flavors:
                    for i, comp in enumerate(comps):
                        self.addComponent(tn + comp, ver, fl,
                            filePrimer=i, repos=repos)
                    self.addCollection(tn, ver, [ tn + comp for comp in comps ],
                        defaultFlavor=fl, repos=repos)
                    yield tn + '=%s[%s]' % (label, fl)

        label = '/localhost@rpl:linux'
        tspecs = [x for x in _createTroves(label)]

        # The key here is not to see the warnings about label multiplicity
        expOutput = """\
The following clones will be created:
   Clone  foo:source           (/localhost@rpl:target/1.0-1)
   Clone  foo                  (/localhost@rpl:target/1.0-1-1[is: x86])
   Clone  foo:runtime          (/localhost@rpl:target/1.0-1-1[is: x86])
   Clone  foo                  (/localhost@rpl:target/1.0-1-1[is: x86_64])
   Clone  foo:runtime          (/localhost@rpl:target/1.0-1-1[is: x86_64])
"""

        self.cfg.fullVersions = True
        rc, output = self.captureOutput(self.clone, target, 
            cloneSources=True, updateBuildInfo=False, 
            ignoreConflicts=False, info=True, *tspecs)
        self.failUnlessEqual(output, expOutput)

    def testSiblingCloneShadow(self):
        self.addComponent('foo:source', '/localhost@rpl:linux/1-1')
        self.addComponent('foo:source', '/localhost@rpl:linux//branch/1-1')
        self.addComponent('foo:source', '/localhost@rpl:linux//branch2/1-1')
        self.addComponent('foo:run', '/localhost@rpl:linux//branch/1-1-1')
        self.addCollection('foo', '/localhost@rpl:linux//branch/1-1-1', 
                           [':run'])
        self.clone('/localhost@rpl:linux//branch2',
                   'foo:source=localhost@rpl:branch')
        self.clone('/localhost@rpl:linux//branch2',
                   'foo=localhost@rpl:branch', cloneSources=False)

    def testCloneFlavorsWithPathIdConflict(self):
        repos = self.openRepository()
        self.openRepository(1)

        unchangedFile = rephelp.RegularFile(contents = 'unchanging',
                                            pathId = '1')
        unchangedConfigFile = rephelp.RegularFile(contents = 'unchanging',
                                                  pathId = '2')

        self.addComponent('test:source', '1.0-1',
                          fileContents = [ 'test.recipe' ] )

        self.addComponent('test:runtime', '1.0-1-1', flavor = 'foo',
                          fileContents = [ ('/bin/foo', unchangedFile),
                                           ('/etc/foo', unchangedConfigFile) ])
        self.addCollection('test', '1.0-1-1',
                           [ ('test:runtime', '1.0-1-1', 'foo') ] )

        self.addComponent('test:runtime', '1.0-1-1', flavor = '!foo',
                          fileContents = [ ('/bin/foo', unchangedFile),
                                           ('/etc/foo', unchangedConfigFile) ])
        self.addCollection('test', '1.0-1-1',
                           [ ('test:runtime', '1.0-1-1', '!foo') ] )

        self.clone('/localhost1@rpl:branch',
                   'test[foo]','test[!foo]',
                   cloneSources=True)

    def testMissingContact(self):
        cfg = conarycfg.ConaryConfiguration()
        cfg.name = "Test Suite"

        cb = conaryclient.callbacks.CloneCallback(cfg)
        trv = trove.Trove('test',
                          versions.ThawVersion('/localhost@foo:bar/1:1.1-1'),
                          deps.parseFlavor('') )
        try:
            cb.getCloneChangeLog(trv)
        except ValueError, e:
            assert(str(e) ==
                    "name and contact information must be set for clone")
        else:
            assert(0)

    def testMultiClone1(self):
        repos = self.openRepository()

        os.chdir(self.workDir)
        self.newpkg("simple")
        os.chdir("simple")
        self.writeFile("simple.recipe", recipes.simpleRecipe)
        self.addfile("simple.recipe")
        self.commit()
        #self.cookFromRepository('simple')

        self.mkbranch("localhost@rpl:linux/1-1", 
                      "localhost@rpl:v1", "simple:source", shadow=True)

        self.mkbranch("localhost@rpl:v1/1-1", 
                      "localhost@rpl:v2", "simple:source", shadow=True)

        os.chdir(self.workDir)
        shutil.rmtree("simple")
        self.checkout("simple", "localhost@rpl:v2")
        os.chdir("simple")
        self.writeFile("simple.recipe", recipes.simpleRecipe.replace(
            "'1'", "'2'"))
        self.commit()
        self.cookFromRepository("simple")

        # Clone uphill
        self.clone('/localhost@rpl:linux//v1', 
                   'simple:source=localhost@rpl:v2')

        self.clone('/localhost@rpl:linux', 
                   'simple:source=/localhost@rpl:linux//v1//v2')
        trv = repos.findTrove(self.cfg.installLabelPath, 
            ('simple:source', '/localhost@rpl:linux', None))[0]
        self.failUnlessEqual(trv[1].asString(), '/localhost@rpl:linux/2-1')

        self.logFilter.add()
        self.clone('/localhost@rpl:linux', 
                   'simple:source=localhost@rpl:v1')
        self.logFilter.compare('warning: Nothing to clone!')
        self.logFilter.remove()

        trv = repos.findTrove(self.cfg.installLabelPath, 
            ('simple:source', '/localhost@rpl:linux', None))[0]
        self.failUnlessEqual(trv[1].asString(), '/localhost@rpl:linux/2-1')

    def testMultiClone2(self):
        def _setupTest():
            self.resetRepository()
            self.resetWork()
            repos = self.openRepository()

            os.chdir(self.workDir)
            self.newpkg("simple")
            os.chdir("simple")
            self.writeFile("simple.recipe", recipes.simpleRecipe)
            self.addfile("simple.recipe")
            self.commit()
            self.cookFromRepository('simple')
            return repos

        repos = _setupTest()

        # Sibling clone
        self.clone('/localhost@rpl:v1', 
                   'simple:source=/localhost@rpl:linux')
        trv = repos.findTrove(self.cfg.installLabelPath, 
            ('simple:source', '/localhost@rpl:v1', None))[0]
        self.failUnlessEqual(trv[1].asString(), '/localhost@rpl:v1/1-1')

        self.clone('/localhost@rpl:v2', 
                   'simple:source=/localhost@rpl:v1')
        trv = repos.findTrove(self.cfg.installLabelPath, 
            ('simple:source', '/localhost@rpl:v2', None))[0]
        self.failUnlessEqual(trv[1].asString(), '/localhost@rpl:v2/1-1')

        self.logFilter.add()
        self.clone('/localhost@rpl:v2', 
                   'simple:source=/localhost@rpl:v1')
        self.logFilter.compare('warning: Nothing to clone!')
        self.logFilter.remove()
        trv = repos.findTrove(self.cfg.installLabelPath, 
            ('simple:source', '/localhost@rpl:v2', None))[0]
        self.failUnlessEqual(trv[1].asString(), '/localhost@rpl:v2/1-1')

        self.logFilter.add()
        self.clone('/localhost@rpl:v2', 
                   'simple:source=/localhost@rpl:linux')
        self.logFilter.compare('warning: Nothing to clone!')

        def _clone(cloneSources):
            if not cloneSources:
                # Make sure the source is there
                trv = repos.findTrove(self.cfg.installLabelPath, 
                    ('simple:source', '/localhost@rpl:v1', None))[0]
                self.failUnlessEqual(trv[1].asString(), '/localhost@rpl:v1/1-1')

            self.logFilter.add()
            self.clone('/localhost@rpl:v1', 
                       'simple=/localhost@rpl:linux',
                       cloneSources=cloneSources)
            trv = repos.findTrove(self.cfg.installLabelPath, 
                ('simple', '/localhost@rpl:v1', None))[0]
            self.failUnlessEqual(trv[1].asString(), '/localhost@rpl:v1/1-1-1')
            self.logFilter.remove()

            if not cloneSources:
                # Make sure the source is there
                trv = repos.findTrove(self.cfg.installLabelPath, 
                    ('simple:source', '/localhost@rpl:v2', None))[0]
                self.failUnlessEqual(trv[1].asString(), '/localhost@rpl:v2/1-1')

            self.clone('/localhost@rpl:v2', 
                       'simple=/localhost@rpl:v1', cloneSources=cloneSources)
            trv = repos.findTrove(self.cfg.installLabelPath, 
                ('simple', '/localhost@rpl:v2', None))[0]
            self.failUnlessEqual(trv[1].asString(), '/localhost@rpl:v2/1-1-1')

            self.logFilter.add()
            self.clone('/localhost@rpl:v2', 
                       'simple=/localhost@rpl:v1', cloneSources=cloneSources)
            self.logFilter.compare('warning: Nothing to clone!')
            self.logFilter.remove()

            trv = repos.findTrove(self.cfg.installLabelPath, 
                ('simple', '/localhost@rpl:v2', None))[0]
            self.failUnlessEqual(trv[1].asString(), '/localhost@rpl:v2/1-1-1')

        _clone(cloneSources=False)

        self.resetRepository()
        repos = _setupTest()
        _clone(cloneSources=True)

    def testParentClonedFrom(self):
        def _get(repos, trvspec):
            info = repos.findTrove([], trvspec)
            return repos.getTrove(*info[0])

        for x in range(0, 5):
            repos = self.openRepository(x)

        self.addComponent("foo:source", "1.0-1")

        self.mkbranch("localhost@rpl:linux", "localhost1@rpl:l1", 
                      "foo:source", shadow = True)

        self.mkbranch("localhost1@rpl:l1", "localhost2@rpl:l2", 
                      "foo:source", shadow = True)

        self.mkbranch("localhost@rpl:linux", "localhost3@rpl:l3", 
                      "foo:source", shadow = True)

        self.mkbranch("localhost3@rpl:l3", "localhost4@rpl:l4", 
                      "foo:source", shadow = True)

        lv = '/localhost@rpl:linux//localhost1@rpl:l1//localhost2@rpl:l2/1.0-2'
        v = versions.VersionFromString(lv, timeStamps = [ 1172248550.4594 ])
        self.addComponent("foo:source", v, repos=repos)
        self.clone("/localhost@rpl:linux//localhost1@rpl:l1", 
            "foo:source=%s" % lv)
        self.clone("/localhost@rpl:linux", 
            "foo:source=/localhost@rpl:linux//localhost1@rpl:l1")

        trv = _get(repos, ('foo:source', 'localhost1@rpl:l1', None))
        self.failUnlessEqual(trv.getVersion().asString(),
            '/localhost@rpl:linux//localhost1@rpl:l1/1.0-2')
        self.failUnlessEqual(trv.troveInfo.clonedFrom().asString(),
            '/localhost@rpl:linux//localhost1@rpl:l1//localhost2@rpl:l2/1.0-2')

        trv = _get(repos, ('foo:source', 'localhost@rpl:linux', None))
        self.failUnlessEqual(trv.getVersion().asString(),
            '/localhost@rpl:linux/1.0-2')
        self.failUnlessEqual(trv.troveInfo.clonedFrom().asString(),
            '/localhost@rpl:linux//localhost1@rpl:l1/1.0-2')

        # Now clone on the other tree

        lv = '/localhost@rpl:linux//localhost3@rpl:l3//localhost4@rpl:l4/1.0-2.1.1'
        v = versions.VersionFromString(lv, timeStamps = [ 1172248541.4594 ])
        self.addComponent("foo:source", v, repos=repos)
        self.clone("/localhost@rpl:linux//localhost3@rpl:l3", 
            "foo:source=%s" % lv)
        self.clone("/localhost@rpl:linux", 
            "foo:source=/localhost@rpl:linux//localhost3@rpl:l3")

        trv = _get(repos, ('foo:source', 'localhost@rpl:linux', None))
        self.failUnlessEqual(trv.getVersion().asString(),
            '/localhost@rpl:linux/1.0-3')
        self.failUnlessEqual(trv.troveInfo.clonedFrom().asString(),
            '/localhost@rpl:linux//localhost3@rpl:l3/1.0-2.1')

        # Re-clone from l4 directly to rpl:linux, should be a no-op
        self.logFilter.add()
        self.clone("/localhost@rpl:linux", "foo:source=%s" % lv)
        self.logFilter.compare("warning: Nothing to clone!")
        self.logFilter.remove()
        trv = _get(repos, ('foo:source', 'localhost@rpl:linux', None))
        self.failUnlessEqual(trv.getVersion().asString(),
            '/localhost@rpl:linux/1.0-3')

        self.stopRepository(4)
        self.stopRepository(3)
        self.stopRepository(2)
        self.stopRepository(1)
        self.stopRepository(0)

    def testPromote(self):
        self.addComponent('foo:source', '1')
        self.addComponent('foo:run', '1')
        self.addCollection('foo', '1', [':run'])
        self.addCollection('group-foo', ':branch', [('foo', '1')])
        cs = self.promote('group-foo=:branch', ':linux--:1',
                          cloneSources=True)

        assert(set((x.getName(), str(x.getNewVersion().branch())) 
                    for x in cs.iterNewTroveList())
               == set([('foo', '/localhost@rpl:1'),
                       ('foo:run', '/localhost@rpl:1'),
                       ('foo:source', '/localhost@rpl:1')]))

    @context('redirect', 'clone')
    def testPromoteRedirect(self):
        repos = self.openRepository()
        self.addComponent('target:source', '1-1')
        self.addComponent('target:runtime', '1-1-1')
        self.addCollection('target', '1-1-1', [':runtime'])
        cs = self.promote('target', ':linux--:1',
                          cloneSources=True)

        assert(set((x.getName(), str(x.getNewVersion().branch())) 
                    for x in cs.iterNewTroveList())
               == set([('target', '/localhost@rpl:1'),
                       ('target:runtime', '/localhost@rpl:1'),
                       ('target:source', '/localhost@rpl:1')]))

        recipestr = """
class testRedirect(RedirectRecipe):
    name = 'redirect'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        l = "localhost@rpl:linux"
        r.addRedirect("target", l)
"""
        self.addComponent('redirect:source=1-1')
        self.addComponent('redirect:runtime=1-1-1')
        self.addCollection('redirect=1-1-1', [':runtime'])
        self.addComponent('redirect:source=1.0-1')
        built = self.build(recipestr, "testRedirect")
        import epdb;epdb.stc('f')
        cs = self.promote('redirect', ':linux--:1',
                          cloneSources=True)
        trvs = [x for x in cs.iterNewTroveList()]
        assert(set((x.getName(), str(x.getNewVersion().branch())) 
                    for x in trvs)
               == set([('redirect', '/localhost@rpl:1'),
                       ('redirect:runtime', '/localhost@rpl:1'),
                       ('redirect:source', '/localhost@rpl:1')]))

        trvCs = [ x for x in cs.newTroves.itervalues()
                  if x.getName().endswith(':runtime') ][0]
        oldTrv = repos.getTrove(*trvCs.getOldNameVersionFlavor())
        oldTrv.applyChangeSet(trvCs)
        redirList = [x for x in oldTrv.redirects.iter()]
        self.assertEquals(len(redirList), 1)
        redir = redirList[0]
        self.assertEquals(redir.branch().asString(), '/localhost@rpl:1')

    def testPromoteOnlyByDefault(self):
        def _getTroves(cs):
            troves = {}
            for troveCs in cs.iterNewTroveList():
                troves[troveCs.getName()] = troveCs
            return troves

        self.addComponent('foo:source', '1')
        self.addComponent('foo:run', '1')
        debuginfo = self.addComponent('foo:debuginfo', '1')
        origFoo = self.addCollection('foo', '1',
                             [':run', (':debuginfo', False)],
                             buildReqs=[debuginfo.getNameVersionFlavor()])
        self.addComponent('foo-pkg:run', '1', sourceName='foo:source')
        self.addCollection('foo-pkg', '1', [':run'], sourceName='foo:source')
        self.addComponent('bar:source', '1', filePrimer=2)
        self.addComponent('bar:run', '1', filePrimer=3)
        self.addCollection('bar', '1', [':run'])
        self.addCollection('group-foo', ':branch', [('foo', '1'),
                                                    ('foo-pkg', '1'),
                                                    ('bar', '1', '', False)])
        cs = self.promote('group-foo=:branch', ':linux--:1',
                          cloneOnlyByDefaultTroves=True,
                          cloneSources=True)


        # make sure reclone does not trigger when nothing's changed
        self.logCheck2("warning: Nothing to clone!",
                       self.promote, 'group-foo=:branch', ':linux--:1',
                       cloneOnlyByDefaultTroves=True)
        troves = _getTroves(cs)
        # no :debuginfo
        assert(set(troves) == set(['foo', 'foo:run', 'foo:source',
                                   'foo-pkg', 'foo-pkg:run']))
        foo = origFoo.copy()
        foo.applyChangeSet(troves['foo'])
        childNames = [x[0] for x in foo.iterTroveList(strongRefs=True,
                                                      weakRefs=True) ]
        assert(childNames == ['foo:run'])
        # but since debuginfo was a build requirement, we leave it there 
        # (on the original label) in the build requirements list.
        assert(foo.getBuildRequirements()
               == [debuginfo.getNameVersionFlavor()])

        self.addCollection('group-foo', ':branch/2', [('foo', '1'),
                                                      ('foo-pkg', '1'),
                                                      ('bar', '1', '', False)],
                            weakRefList=[('foo:debuginfo', '1', '', True),
                                         ('foo:run', '1', '', True),
                                         ('bar:run', '1', '', False),
                                         ('foo-pkg:run', '1', '', True)])
        cs = self.promote('group-foo=:branch', ':linux--:1',
                          cloneOnlyByDefaultTroves=True)
        troves = _getTroves(cs)
        # no :source, since that was already cloned.
        # foo-pkg is recloned because it came from the same source as foo,
        # so those version #'s should be in line.
        assert(set(troves) == set(['foo', 'foo:run', 'foo:debuginfo',
                                   'foo-pkg', 'foo-pkg:run']))
        foo = origFoo.copy()
        foo.applyChangeSet(troves['foo'])
        childNames = [x[0] for x in foo.iterTroveList(strongRefs=True,
                                                      weakRefs=True) ]
        assert(sorted(childNames) == ['foo:debuginfo', 'foo:run'])
        assert(str(foo.getBuildRequirements()[0][1].branch()) == '/localhost@rpl:1')

        # now promote the other group-foo again, resulting in yet a third
        # clone.
        cs = self.promote('group-foo=:branch/1', ':linux--:1',
                          cloneOnlyByDefaultTroves=True)
        troves = _getTroves(cs)
        assert(set(troves) == set(['foo', 'foo:run',
                                   'foo-pkg', 'foo-pkg:run']))
        foo = origFoo.copy()
        foo.applyChangeSet(troves['foo'])
        childNames = [x[0] for x in foo.iterTroveList(strongRefs=True,
                                                      weakRefs=True) ]
        assert(childNames == ['foo:run'])
        assert(foo.getBuildRequirements() == [debuginfo.getNameVersionFlavor()])

    def testPromoteOnlyByDefaultEmptyGroup(self):
        # the one thing that's in this group is byDefault False.
        # that would result in an empty group, which would be bad.
        self.addComponent('foo:source', '1')
        self.addComponent('foo:run', '1', filePrimer=1)
        self.addCollection('foo', '1', [':run'])
        self.addCollection('group-foo', '1', [('foo', False)])
        self.addComponent('group-foo:source', '1', filePrimer=2)
        try:
            self.promote('group-foo', ':linux--:1',
                         cloneOnlyByDefaultTroves=True,
                         cloneSources=True)
            assert(0)
        except clone.CloneError, msg:
            assert(str(msg) == 'Clone would result in empty collection group-foo=/localhost@rpl:linux/1-1-1[]')

    def testPromoteOnlyPackagesOnDifferentLabel(self):
        # We're not promoting foo, so we don't change 
        self.addComponent('foo:source=1')
        self.addComponent('foo:run=1', filePrimer=1)
        self.addComponent('foo:debuginfo=1', filePrimer=2)
        self.addCollection('foo=1', [':run', (':debuginfo', False)])
        self.addCollection('group-foo=:branch/1', ['foo=:linux/1'])
        self.addComponent('group-foo:source=:branch/1', filePrimer=2)
        cs = self.promote('group-foo=:branch', ':branch--:1',
                     cloneOnlyByDefaultTroves=True,
                     cloneSources=True)
        trvCs, = [ x for x in cs.iterNewTroveList() 
                   if x.getName() == 'group-foo']
        # promote now generates a relative cs, so get the old trove
        # and apply the trvCs to make sure that the weak troves from labels
        # not rewritten are unchanged
        repos = self.openRepository()
        t = repos.getTrove('group-foo', trvCs.oldVersion(), trvCs.oldFlavor())
        t.applyChangeSet(trvCs)
        assert('foo:debuginfo' in
                [ x[0] for x in t.iterTroveList(weakRefs=True) ])


    def testPromoteConflicts(self):
        self.addComponent('foo:run', '1', flavor='is:x86')
        self.addCollection('foo', '1', [':run'], defaultFlavor='is:x86')
        self.addComponent('foo:run', ':branch/1', flavor='is:x86', filePrimer=1)
        self.addCollection('foo', ':branch/1', [':run'], defaultFlavor='is:x86')

        try:
            # note - there's no way to do --:1 from the command line,
            # because it looks like a long option.
            cs = self.promote('foo[is:x86]', 'foo=:branch[is:x86]', '--:1',
                              cloneSources=True)
            assert(0)
        except clone.CloneError, msg:
            self.assertEquals(str(msg), 'Cannot clone multiple versions of foo[is: x86] to branch /localhost@rpl:1 at the same time.  Attempted to clone versions /localhost@rpl:branch/1-1-1 and /localhost@rpl:linux/1-1-1')

    def testRepromote(self):
        self.addComponent('foo:source', '1')
        self.addComponent('foo:run', '1')
        debuginfo = self.addComponent('foo:debuginfo', '1')
        origFoo = self.addCollection('foo', '1',
                           [':run', (':debuginfo', False)],
                           buildReqs=[debuginfo.getNameVersionFlavor()])
        self.addComponent('bar:source', '1', filePrimer=2)
        self.addComponent('bar:run', '1', filePrimer=3)
        self.addCollection('bar', '1', [':run'])
        self.addCollection('group-foo', ':branch', [('foo', '1'),   
                                                    ('bar', '1', '', False)])
        cs = self.promote('group-foo=:branch', ':linux--:1',
                          cloneOnlyByDefaultTroves=True,
                          cloneSources=True)

        troves = {}
        for troveCs in cs.iterNewTroveList():
            troves[troveCs.getName()] = troveCs
        # no :debuginfo
        assert(set(troves) == set(['foo', 'foo:run', 'foo:source']))
        foo = origFoo.copy()
        foo.applyChangeSet(troves['foo'])
        childNames = [x[0] for x in foo.iterTroveList(strongRefs=True,
                                                      weakRefs=True) ]
        assert(childNames == ['foo:run'])
        # but since debuginfo was a build requirement, we leave it there 
        # (on the original label) in the build requirements list.
        assert(foo.getBuildRequirements()
               == [debuginfo.getNameVersionFlavor()])

    def testPromoteSingleComponent(self):
        # CNY-2941 - do not allow promotion of inidividual components
        self.addComponent('foo:runtime')
        self.addComponent('bar:runtime')
        self.addComponent('bar:source')
        self.addCollection('bar', ['bar:runtime'])
        try:
            self.promote('foo:runtime', 'bar:runtime',
                         'localhost@rpl:linux--localhost@branch:2')
        except errors.CvcError, msg:
            self.assertEquals(str(msg),  "Cannot promote/clone components: 'bar:runtime','foo:runtime'.  Please specify package names instead.")
        # it's ok if you specify the package _and_ the component
        self.promote('bar', 'bar:runtime',
                     'localhost@rpl:linux--localhost@branch:2',
                     cloneSources=True)

    def testConvertLabel(self):
        def _test(labelStr, expectedLabel, context=None):
            if context is None:
                context = self.cfg.buildLabel
            foundLabel = _convertLabelOrBranch(labelStr, context)
            assert(str(foundLabel) == expectedLabel)
        _test(':branch', 'localhost@rpl:branch')
        _test('@foo:branch', 'localhost@foo:branch')
        _test('foo@foo:branch', 'foo@foo:branch')
        _test('', 'None')

    def testComputeLabelPath(self):
        l1 = Label('l@rpl:1')
        l2 = Label('l@rpl:2')
        l3 = Label('l@rpl:3')
        l4 = Label('l@rpl:4')
        l5 = Label('l@rpl:5')
        labelPathMap = ((l1, set([l2])),
                         (l3, set([l4])),
                         (l2, set([l5])),
                         (l4, set()))
        assert(clone._computeLabelPath('bar', labelPathMap) == [l2, l4, l5])

        labelPathMap = ((l1, set([l2, l3])),)
        try:
            clone._computeLabelPath('bar', labelPathMap)
            assert(0)
        except clone.CloneError, err:
            self.assertEquals(str(err),
                    'Multiple clone targets for label l@rpl:1'
                    ' - cannot build labelPath for bar')

    def testPromoteNoSource(self):
        self.addComponent('foo:source', '1')
        self.addComponent('foo:run', '1')
        self.addCollection('foo', '1', [':run'])
        try:
            self.promote('foo', ':linux--:branch', cloneSources=False)
        except clone.CloneError, err:
            self.assertEquals(str(err),
                    'Cannot find cloned source for foo:source=/localhost@rpl:linux/1-1')

    def testCloneNoSource(self):
        self.addComponent('foo:source', '1')
        self.addComponent('foo:run', '1')
        self.addCollection('foo', '1', [':run'])
        try:
            self.clone('/localhost@rpl:branch', 'foo', cloneSources=False)
        except clone.CloneError, err:
            self.assertEquals(str(err),
                    'Cannot find cloned source for foo:source=/localhost@rpl:linux/1-1')

    def testPromoteDifferentVersionAndFlavor(self):
        # CNY-1443 - clone different versions + flavors of the same
        # trove.  The source components can't both be cloned so there's
        # a conflict here.
        self.addComponent('foo:source', '1')
        self.addComponent('foo:source', '2')
        self.addComponent('foo:run', '1', 'is:x86')
        self.addComponent('foo:run', '2', 'is:x86_64')
        self.addCollection('foo', '1', [':run'], defaultFlavor='is:x86')
        self.addCollection('foo', '2', [':run'], defaultFlavor='is:x86_64')
        try:
            self.promote('foo[is:x86]', 'foo[is:x86_64]', 
                         ':linux--:branch', cloneSources=True)
            assert 0
        except Exception, err:
            self.assertEquals(str(err), 'Cannot clone multiple versions of foo:source to branch /localhost@rpl:branch at the same time.  Attempted to clone versions /localhost@rpl:linux/1-1 and /localhost@rpl:linux/2-1')

    def testAllFlavors(self):
        self.addComponent('foo:source', '1')
        self.addComponent('foo:source', '2')
        self.addComponent('foo:run', '1', 'ssl is:x86')
        self.addCollection('foo', '1', [':run'], defaultFlavor='ssl is:x86')
        self.addComponent('foo:run', '2', 'is:x86')
        self.addComponent('foo:run', '2', 'is:x86_64')
        self.addCollection('foo', '2', [':run'], defaultFlavor='is:x86')
        self.addCollection('foo', '2', [':run'], defaultFlavor='is:x86_64')
        cs = self.promote('foo', ':linux--:branch', cloneSources=True,
                          allFlavors=True)
        flavors = sorted(str(x.getNewFlavor()) for x in cs.iterNewTroveList()
                      if ':' not in x.getName())
        assert(flavors == ['is: x86', 'is: x86_64'])

    def testUphillSidewaysClone(self):
        self.openRepository()
        repos = self.openRepository(1)
        self.addComponent('foo:source', '/localhost@rpl:linux/1')
        self.addComponent('foo:run', '1')
        self.addCollection('foo', '1', [':run'])
        self.mkbranch("1-1-1", "localhost@rpl:shadow", "foo", shadow = True)
        cs = self.promote('foo=:shadow', ':shadow--/localhost@rpl:branch', 
                          cloneSources=True)
        trv = self.findAndGetTrove('foo:run=localhost@rpl:branch')
        fileVersion = [ x[3] for x in trv.iterFileList() ][0]
        # make sure the file version gets rewritten too
        assert(fileVersion == trv.getVersion())
        assert(trv.getVersion().depth() == 1)
        self.mkbranch(["foo=:shadow", 'foo:source=:shadow'],
                      "localhost@rpl:shadow2", shadow = True)
        cs = self.promote('foo=:shadow2',
                          ':shadow2--/localhost@rpl:linux//branch2',
                          cloneSources=True)
        trv = self.findAndGetTrove('foo:run=localhost@rpl:branch2')
        fileVersion = [ x[3] for x in trv.iterFileList() ][0]
        # make sure the file version gets rewritten too
        assert(str(fileVersion.trailingLabel()) == 'localhost@rpl:linux')
        assert(trv.getVersion().depth() == 2)

    def testCloneWhenSubSetFlavorsExist(self):
        # RMK-415 - don't blow up if only subsets of the current flavor
        # exist on the target label
        self.addComponent('foo:source', '1')
        self.addComponent('foo:run', '1', 'foo is:x86')
        self.addCollection('foo', '1', [':run'], defaultFlavor='foo is:x86')

        self.addComponent('foo:source', ':branch/1')
        self.addComponent('foo:run', ':branch/1', 'is:x86')
        self.addCollection('foo', ':branch/1', [':run'], defaultFlavor='is:x86')
        cs = self.promote('/localhost@rpl:linux--/localhost@rpl:branch',
                          'foo[foo]', cloneSources=True)

    def testCloneTwoFlavorsOfGroup(self):
        # CNY-1692 - x86 version exists on the label already,
        # now we're cloning a new x86 and x86_64 version, make sure
        # they end up at the same version # on the target label.
        self.addComponent('bar:source', '1')
        self.promote('/localhost@rpl:linux--/localhost@rpl:branch',
                     'bar:source', cloneSources=True)
        self.addComponent('bar:run', '1', 'is:x86')
        self.addCollection('bar', '1', [':run'], defaultFlavor='is:x86')
        self.addComponent('bar:run', '1', 'is:x86_64')
        self.addCollection('bar', '1', [':run'], defaultFlavor='is:x86_64')
        self.addComponent('bar:run', ':branch/1', 'is:x86')
        self.addCollection('bar', ':branch/1', [':run'], defaultFlavor='is:x86')

        cs = self.promote('/localhost@rpl:linux--/localhost@rpl:branch',
                          'bar', cloneSources=False, allFlavors=True)
        self.assertEquals(
            len(set( [ x.getNewVersion() for x in cs.iterNewTroveList()])), 1)

    def testAlwaysBumpGroupVersions(self):
        self.addComponent('group-foo:source', '1')
        self.addComponent('foo:source', '1')
        self.addComponent('foo:run', '1', 'readline')
        self.addCollection('foo', '1', [':run'], defaultFlavor='readline')
        self.addCollection('group-foo', '1', [('foo', '1')], 
                            defaultFlavor='readline')

        self.promote('/localhost@rpl:linux--:branch', 'group-foo:source',
                          'foo:source')
        self.addComponent('foo:run', ':branch/1', '!readline')
        self.addCollection('foo', ':branch/1', [':run'], 
                            defaultFlavor='!readline')
        self.addCollection('group-foo', ':branch/1', [('foo', ':branch/1')],
                           defaultFlavor='!readline')

        cs = self.promote('/localhost@rpl:linux--:branch', 'group-foo')
        groupVersion = [ x.getNewVersion() for x in cs.iterNewTroveList()
                         if x.getName() == 'group-foo' ][0]
        self.assertEquals(str(groupVersion), '/localhost@rpl:branch/1-1-2')
        fooVersion = [ x.getNewVersion() for x in cs.iterNewTroveList()
                         if x.getName() == 'foo' ][0]
        # we changed the version of group-foo because we have the bump
        # group version behavior turned on, but moved foo to be side-by-side
        # w/ the existing foo because we don't have any reason not to.
        self.assertEquals(str(fooVersion), '/localhost@rpl:branch/1-1-1')

        # ok, now, what if we now realize we forgot to promote flavor ssl
        # and want to go back and promote it now?
        # They both get bumped to a new version, which is good, since
        # there might be a third flavor or something still sitting at 1-1-2
        self.addCollection('group-foo', '1', [('foo', '1')], 
                            defaultFlavor='readline', flavor='ssl')
        cs = self.promote('/localhost@rpl:linux--:branch', 'group-foo')
        self.assertEquals([ x.getNewVersion() for x in cs.iterNewTroveList() ],
                          [VFS('/localhost@rpl:branch/1-1-3'),
                           VFS('/localhost@rpl:branch/1-1-3')])
        self.logFilter.add()
        cs = self.promote('/localhost@rpl:linux--:branch', 'group-foo')
        self.logFilter.remove()
        self.logFilter.compare(['warning: Nothing to clone!'])
        assert(not cs) # should not clone since there's nothing new to clone.
                       # although, it's possible that where you used to 
                       # have 3 things at tip you now want just one...

    def testCloneSpecificFlavors(self):
        # We've switched from cloning one flavor by default to cloning
        # all flavors by default.
        # This confirms that you can still clone one flavor if desired.
        self.addComponent('foo:source', '1')
        for flavor in ['readline', '~!readline', '~readline', '!readline',
                       '']:
            self.addComponent('foo:run', '1', flavor)
            self.addCollection('foo', '1', [':run'], defaultFlavor=flavor)

        self.promote('localhost@rpl:linux--:branch','foo:source')
        cs = self.promote('localhost@rpl:linux--:branch', 'foo[!readline]',
                          exactFlavors=True)
        flavors = [x.getNewFlavor() for x in cs.iterNewTroveList()]
        assert(len(set(flavors)) == 1)
        assert(str(flavors[0]) == '!readline')
        cs = self.promote('localhost@rpl:linux--:branch', 'foo[]',
                          exactFlavors=True)
        flavors = [x.getNewFlavor() for x in cs.iterNewTroveList()]
        assert(len(set(flavors)) == 1)
        assert(str(flavors[0]) == '')
        cs = self.promote('localhost@rpl:linux--:branch', 'foo[~readline]',
                          exactFlavors=True)
        flavors = [x.getNewFlavor() for x in cs.iterNewTroveList()]
        assert(len(set(flavors)) == 1)
        assert(str(flavors[0]) == '~readline')
        cs = self.promote('localhost@rpl:linux--:branch', 'foo[readline]',
                          exactFlavors=True)
        flavors = [x.getNewFlavor() for x in cs.iterNewTroveList()]
        assert(len(set(flavors)) == 1)
        assert(str(flavors[0]) == 'readline')
        # Note: no way to clone the '' flavored one all on its own.
        # Hm.

    def testPromoteExactFlavor(self):
        self.addComponent('bar:source=1')
        self.addComponent('bar:run=1[~ssl]')
        self.addCollection('bar=1[~ssl]', [':run'])
        self.assertRaises(errors.TroveNotFound, self.promote,
                      '/localhost@rpl:linux--/localhost@rpl:branch',
                     'bar[ssl]', cloneSources=True, exactFlavors=True)
        cs = self.promote('/localhost@rpl:linux--/localhost@rpl:branch',
                          'bar[~ssl]', cloneSources=True, exactFlavors=True)
        assert(cs)

    def testCloneTwoFlavorsFromSameSource(self):
        self.addComponent('bar:source=1')
        self.addComponent('bar:run=1-1-1[ssl]')
        self.addComponent('bar:run=1-1-2[!ssl]')
        self.addComponent('bar:run=:branch/1-1-1[ssl]')
        self.addCollection('bar=1-1-1[ssl]', [':run'])
        self.addCollection('bar=1-1-2[!ssl]', [':run'])
        self.addCollection('bar=:branch/1-1-1[ssl]', [':run'])
        cs = self.promote('/localhost@rpl:linux--:branch',
                          'bar=1-1-1', 'bar=1-1-2', 
                          cloneSources=True, allFlavors=True)
        versions = ( x.getNewVersion() for x in cs.iterNewTroveList())
        versions = set([ x for x in versions if not x.isSourceVersion()])
        assert(len(versions) == 1)

    def testCloneWithMissingDebuginfo(self):
        #self.addComponent('foo:run') # this is not added because
        # we want to emulate a mirror missing a component
        self.addCollection('foo=1', [':run'])
        trv = self.addComponent('group-foo:source', ':branch/1')
        self.addCollection('group-foo=:branch/1', ['foo=1'])
        cs = self.promote(':branch--:branch2', 'group-foo=:branch',
                          cloneSources=True)

    def testCloneWithMissingPackage(self):
        trv = self.addComponent('group-foo:source', ':branch/1')
        self.addCollection('group-foo=:branch/1', ['foo=1'],
                            weakRefList=['foo:run=1'])
        cs = self.promote(':branch--:branch2', 'group-foo=:branch',
                          cloneSources=True)

    def testCloneOverMarkRemoved(self):
        self.addComponent('foo:source=:1')
        self.addComponent('foo:source=:branch')
        self.markRemoved('foo:source=:branch')
        cs = self.promote(':1--:branch', 'foo:source=:1')

    def testCloneWithMissingComponentOnTarget(self):
        # set up foo:source, foo:runtime, foo:doc on :1
        trv = self.addComponent('foo:source', ':1/1.0-1')
        trv = self.addComponent('foo:runtime', ':1/1.0-1-1')
        trv = self.addComponent('foo:doc', ':1/1.0-1-1')
        self.addCollection("foo", ":1/1.0-1-1",
                           [ "foo:runtime", "foo:doc" ],
                           sourceName = "foo:source")

        # create the same structure on :branch, but leave :doc out
        trv = self.addComponent('foo:source', ':branch')
        trv = self.addComponent('foo:runtime', ':branch')
        self.addCollection("foo", ":branch/1.0-1-1",
                           [ "foo:runtime", "foo:doc" ],
                           sourceName = "foo:source")

        cs = self.promote(':1--:branch', 'foo=:1',
                          cloneSources=True)

    def testCloneFileSet(self):
        self.addComponent('foo:runtime', '/localhost@rpl:2/2-1-1', ['/foo'])
        self.addComponent('fileset-foo:source=1')
        cs = self.promote(':linux--:branch', 'fileset-foo:source')
        origTrv = self.addComponent('fileset-foo=1',
                          [('/foo', 'contents', '/localhost@rpl:2/1-1-1')])
        assert(str(origTrv.iterFileList().next()[3]) ==
                        '/localhost@rpl:2/1-1-1')

        cs = self.promote(':linux--:branch', 'fileset-foo', cloneSources=True,
                           test=True)
        trvCs = cs.iterNewTroveList().next()
        trv = origTrv.copy()
        trv.applyChangeSet(trvCs)
        fileId, path, pathId, fileVersion = trv.iterFileList().next()
        assert((path, str(fileVersion)) == ('/foo', '/localhost@rpl:2/1-1-1'))

        # now do a clone where we _are_ transporting the file's branch
        # in that case, just make sure that the file gets copied into the 
        # fileset's new location.
        cs = self.promote(':linux--:branch', ':2--:3',
                          'fileset-foo', cloneSources=True,
                          test=True)
        trvCs = cs.iterNewTroveList().next()
        trv = origTrv.copy()
        trv.applyChangeSet(trvCs)
        fileId, path, pathId, fileVersion = trv.iterFileList().next()
        assert((path, str(fileVersion)) == ('/foo',
                                            '/localhost@rpl:branch/1-1-1'))

    def testPromoteGroupWithReferenceToCloneAndOriginal(self):
        self.addComponent('foo:source')
        self.addComponent('foo:run')
        self.addCollection('foo', [':run'])
        self.promote('foo', ':linux--:branch', cloneSources=True)
        self.addComponent('group-foo:source')
        self.addCollection('group-foo', ['foo', 'foo=:branch/1.0-1-1'])
        self.promote('group-foo', ':linux--:branch', cloneSources=True)

    def testCloneTwoOfTheSameFileWithRelativeChangeSets(self):
        # CNY-2346 - test multiple relative promote changesets that both 
        # require the same file from another host
        self.openRepository()
        self.openRepository(1)
        shadowVer = '/localhost@rpl:devel//localhost1@rpl:branch/1-1-0.1'
        shadowSourceVer = '/localhost@rpl:devel//localhost1@rpl:branch/1-1'
        for f in '[is:x86]', '[is:x86_64]':
            self.addComponent('foo:run=:1%s' % f, [('/foobar', "contents2\n")])
            self.addCollection('foo=:1%s' % f, [':run'])
            self.addComponent('foo:run=:devel%s' % f, 
                               [('/foobar', "contents\n")])
            self.addComponent('foo:run=%s%s' % (shadowVer, f), 
                    [('/foo', "contents\n", '/localhost@rpl:devel/1-1-1')])
            self.addCollection('foo=%s%s' % (shadowVer, f), [':run'])
        self.addComponent('foo:source=:1')
        self.addComponent('foo:source=%s' % shadowSourceVer)
        self.promote('localhost1@rpl:branch--/localhost@rpl:1', 
                     'foo', cloneSources=True)


    def testUncleCloneOnlyIncreasesNumbers(self):
        # CNY-2108 - make sure cloning uphill and sideways increases
        # version #s
        self.addComponent(
                        'foo:source=/localhost@rpl:linux//1//1-branch/1-3.5.15')
        self.addComponent('foo:source=/localhost@rpl:linux//1-devel/1-3.6')
        cs = self.promote(':1-branch--/localhost@rpl:linux//1-devel',
                          'foo:source=:1-branch', cloneSources=True, test=True)
        trvCs = cs.iterNewTroveList().next()
        assert(str(trvCs.getNewVersion()) ==
                        '/localhost@rpl:linux//1-devel/1-3.7')

    def testCloneFileHandling(self):
        # tests cross-repository clones (where files need to be copied), and
        # fiddles with the internal thresholds of clone.py to force various
        # file handling code paths
        self.addComponent('foo:runtime', '1.0-1-1',
                          fileContents =
                            [ ('/%d' % x, '%d' % x) for x in range(10) ])
        self.addCollection('foo', '1.0-1-1', [ 'foo:runtime' ],
                          sourceName = 'foo:source')
        self.addComponent('foo:source', '1.0-1')

        self.openRepository(1)

        # default
        cs = self.promote(':linux--localhost1@rpl:promote', 'foo',
                          cloneSources = True)
        assert(len(cs.files) == 11)

        # force multiple passes for the clone
        oldMCF = clone.MAX_CLONE_FILES
        oldCM = clone.CHANGESET_MULTIPLE

        try:
            self.resetRepository(1)
            clone.MAX_CLONE_FILES = 1
            cs = self.promote(':linux--localhost1@rpl:promote', 'foo',
                              cloneSources = True)
            clone.MAX_CLONE_FILES = oldMCF
            assert(len(cs.files) == 11)

            # force everything through getFileVersions
            self.resetRepository(1)
            clone.CHANGESET_MULTIPLE = 0
            cs = self.promote(':linux--localhost1@rpl:promote', 'foo',
                              cloneSources = True)
            assert(len(cs.files) == 11)

            # force everything through createChangeSet
            self.resetRepository(1)
            clone.CHANGESET_MULTIPLE = 100
            cs = self.promote(':linux--localhost1@rpl:promote', 'foo',
                              cloneSources = True)
            assert(len(cs.files) == 11)
        finally:
            clone.CHANGESET_MULTIPLE = oldCM
            clone.MAX_CLONE_FILES = oldMCF

    def testCloneDuplicateFiles(self):
        self.openRepository(1)
        # all of these have duplicate contents
        self.addComponent('foo:runtime', '1.0-1-1',
                          fileContents =
                            [ ('/%d' % x,
                               rephelp.RegularFile(contents = 'contents1',
                                                   version = '1',
                                                   pathId = str(x)) )
                              for x in range(1,5) ] +
                            [ ('/9',
                               rephelp.RegularFile(contents = 'contents2',
                                                   version = '1',
                                                   pathId = '9' ) ) ] )
        self.addCollection('foo', '1.0-1-1', [ 'foo:runtime' ],
                          sourceName = 'foo:source')
        self.addComponent('foo:source', '1.0-1')

        self.promote(':linux--localhost1@rpl:promote', 'foo',
                     cloneSources = True)

        # note that file /9 (with the last pathId) hasn't changed a whit
        self.addComponent('foo:runtime', '2.0-1-1',
                          fileContents =
                            [ ('/%d' % x,
                               rephelp.RegularFile(contents = 'contents2',
                                                   version = '2',
                                                   pathId = str(x)) )
                              for x in range(1,5) ] +
                            [ ('/9',
                               rephelp.RegularFile(contents = 'contents2',
                                                   version = '1',
                                                   pathId = '9' ) ) ] )
        self.addCollection('foo', '2.0-1-1', [ 'foo:runtime' ],
                          sourceName = 'foo:source')
        self.addComponent('foo:source', '2.0-1')

        # this promote gets a changeset for the foo:source=2.0 in order to get
        # the file contents to commit for this promote. however, all of the
        # contents for that changeset point to the contents for /9, which isn't
        # part of the promote (since it's unchanged)

        cs = self.promote(':linux--localhost1@rpl:promote', 'foo',
                          cloneSources = True)
        cs.reset()
        cs.writeToFile(self.workDir + '/promote.ccs')

        # changeset should have ptr's for a while, and then the item which
        # is pointed to
        fc = filecontainer.FileContainer(
                            util.ExtendedFile(self.workDir + '/promote.ccs',
                            buffering = False))
        (name, tag, fcf) = fc.getNextFile()
        assert(name == 'CONARYCHANGESET')

        target = None
        while True:
            info = fc.getNextFile()
            if info is None: break
            (name, tag, fcf) = info

            assert(tag.endswith('ptr'))
            # gotta uncompress this puppy
            thisTarget = gzip.GzipFile(None, "r",
                                     fileobj = StringIO(fcf.read())).read()
            assert(target is None or thisTarget == target)
            target = thisTarget

        # the target doesn't get included, but it's already on the server so
        # it doesn't matter
        assert(tag.endswith('ptr'))

    def testCloneUsesSearchPath(self):
        # CNY-2235
        self.addComponent('foo:source', ':devel/1.0-1')
        self.addComponent('foo:source', ':1/2.0-1')
        self.addComponent('bar:source', ':1/3.0-1')
        cs = self.promote(':linux--localhost1@rpl:promote', 'foo:source',
                          'bar:source', ':devel--:devel2', ':1--:2')
        for troveCs in cs.iterNewTroveList():
            if troveCs.getName() == 'foo:source':
                assert(troveCs.getNewVersion().trailingRevision().getVersion() 
                            == '1.0')
            else:
                assert(troveCs.getNewVersion().trailingRevision().getVersion() 
                            == '3.0')

    def testSourceCloneReCloneShadow(self):
        # CNY-2441
        self.addComponent('foo:source=/localhost@rpl:linux//1/1-1')
        self.addComponent('foo:source=/localhost@rpl:linux//1/1-1.1')
        cs = self.promote(':1--:1',
                          'foo:source=:1/1-1', cloneSources=True, test=True)
        trvCs = cs.iterNewTroveList().next()
        assert(str(trvCs.getNewVersion()) == '/localhost@rpl:linux//1/1-1.2')

    def _checkMetadata(self, d, **kw):
        for key, value in d.items():
            if value is None:
                assert(key not in kw or kw[key] == None)
            else:
                assert(value == kw[key])

    def testPromoteMetadata(self):
        mi = self.createMetadataItem(licenses='GPL', shortDesc='foo:source')
        self.addComponent('foo:source', metadata=mi)
        mi2 = self.createMetadataItem(licenses='GPLv3', shortDesc='foo package')
        self.addComponent('foo:run', metadata=mi2)
        self.addCollection('foo', [':run'], metadata=mi2)
        self.promote(':linux--:branch', 'foo', cloneSources=True)
        d = self.findAndGetTrove('foo:source=:branch').getMetadata()
        self._checkMetadata(d, licenses=['GPL'], shortDesc='foo:source')
        d = self.findAndGetTrove('foo:run=:branch').getMetadata()
        self._checkMetadata(d, licenses=['GPLv3'], shortDesc='foo package')
        d = self.findAndGetTrove('foo=:branch').getMetadata()
        self._checkMetadata(d, licenses=['GPLv3'], shortDesc='foo package')

    def testExcludeGroups(self):
        # CNY-2801 - excludeGroups
        self.addComponent('group-foo:debuginfo', filePrimer=1)
        self.addCollection('foo', [':runtime'], createComps=True)
        self.addComponent('group-foo:source')
        self.addComponent('foo:source')
        self.addCollection('group-foo', [':debuginfo', 'foo'])
        cs = self.promote('group-foo', ':linux--:branch', excludeGroups=True,
                          cloneSources=True)
        names = sorted(x.getNewNameVersionFlavor()[0]
                       for x in cs.iterNewTroveList())
        assert(names == ['foo', 'foo:runtime', 'foo:source'])

    def testUphillCloneTwice(self):
        self.addComponent('foo:source=1-1')
        self.addComponent('foo:source=1-2')
        self.addComponent('foo:source=/localhost@rpl:linux//shadow/1-1')
        cs = self.promote(':shadow--/localhost@rpl:linux',
                          'foo:source=:shadow', cloneSources=True, test=True)
        assert(str(cs.iterNewTroveList().next().getNewVersion())
                == '/localhost@rpl:linux/1-3')

    def testCloneAcrossReposDuplicateContents(self):
        # CNY-2978
        self.openRepository(1)

        self.addComponent('foo:source=/localhost@rpl:linux/1-1')
        self.addComponent('foo:runtime=/localhost@rpl:linux/1-1-1',
            fileContents = [
                 ('/2',
                  rephelp.RegularFile(pathId = '2',
                          contents = 'hello',
                          version = '/localhost@rpl:linux/1-1-1' ) ) ] )
        self.addCollection('foo=/localhost@rpl:linux/1-1-1',
                           [ 'foo:runtime' ] )

        self.addComponent('foo:source=/localhost@rpl:linux//shadow/2-1')
        self.addComponent('foo:runtime=/localhost@rpl:linux//shadow/2-1-1',
            fileContents = [
                 ('/1',
                  rephelp.RegularFile(pathId = '1',
                          contents = 'hello',
                          version = '/localhost@rpl:linux//shadow/2-1-1' ) ),
                 ('/2',
                  rephelp.RegularFile(pathId = '2',
                          contents = 'hello',
                          version = '/localhost@rpl:linux/1-1-1' ) ) ] )
        self.addCollection('foo=/localhost@rpl:linux//shadow/2-1-1',
                           [ 'foo:runtime' ] )

        self.promote(':shadow--localhost1@rpl:target',
                     'foo=:shadow', cloneSources=True)
        self.updatePkg('foo=localhost1@rpl:target')

    def testCloneFileRename(self):
        self.openRepository()
        repos = self.openRepository(1)
        fooFile = rephelp.RegularFile(contents='foo', pathId='1',
                                      version='/localhost@rpl:linux/1-1')
        self.addComponent('foo:source=/localhost@rpl:linux/1-1',
                            [('foo.recipe', fooFile)])
        self.addComponent('foo:source=/localhost@rpl:linux//shadow/1-1',
                            [('foo.recipe', fooFile)])
        self.promote(':shadow--localhost1@rpl:branch', 'foo:source')
        # change name and make sure promote works
        self.addComponent('foo:source=/localhost@rpl:linux//shadow/2-1', 
                          [('foo.recipe2', fooFile)])
        self.promote(':shadow--localhost1@rpl:branch', 'foo:source')
        self.changeset(repos, ['foo:source=localhost1@rpl:branch'], 
                       self.workDir + '/foo.ccs')

    def testCloneDoesntCloneOldSource(self):
        self.addComponent('foo:source=1')
        self.addComponent('foo:runtime=1')
        self.addCollection('foo=1', [':runtime'])
        self.promote(':linux--:branch', 'foo', cloneSources=True)
        self.addComponent('foo:source=:branch/1-2')
        self.logFilter.add()
        cs = self.promote(':linux--:branch', 'foo', cloneSources=True)
        assert(not cs)
        self.addComponent('foo:runtime=1[ssl]')
        self.addCollection('foo=1[ssl]', [':runtime'])
        cs = self.promote(':linux--:branch', 'foo', cloneSources=True,
                          test=True )
        # if we're promoting over a new flavor, then bump the source.
        troveList = [ x.getNewNameVersionFlavor() for x in cs.iterNewTroveList()]
        assert(set(str(x[1]) for x in troveList)
               == set(['/localhost@rpl:branch/1-3', 
                       '/localhost@rpl:branch/1-3-1']))
        assert(len(troveList) == 5)

    def testNoRecloneBinary(self):
        self.addComponent('foo:source=:branch/1-1')
        self.addComponent('foo:runtime=:branch/1-1-1')
        self.addCollection('foo=:branch/1-1-1', [':runtime'])
        self.promote(':branch--:linux', 'foo', cloneSources=True)
        self.addCollection('group-foo=1', ['foo'])
        self.addComponent('group-foo:source=1')
        cs = self.promote(':linux--:branch', 'group-foo', cloneSources=True)
        troveNames = sorted(x.getName() for x in cs.iterNewTroveList())
        assert(troveNames == ['group-foo', 'group-foo:source'])

    @conary_test.rpm
    def testCloneContainer(self):
        self.addComponent('simple:source=:branch/1.0-1')
        self.addRPMComponent('simple:rpm=:branch/1.0-1-1',
                             'simple-1.0-1.i386.rpm')
        self.addCollection('simple=:branch/1.0-1-1', [ ':rpm' ])
        self.promote(':branch--:linux', 'simple', cloneSources=True)
        self.updatePkg('simple')
        self.resetRoot()

        self.openRepository(1)
        self.openRepository(2)

        cs = self.promote(':branch--localhost1@foo:bar', 'simple',
                          cloneSources=True)

        try:
            # force the code path where promote fetches individual files,
            # not whole changesets
            old = conaryclient.clone.CHANGESET_MULTIPLE
            conaryclient.clone.CHANGESET_MULTIPLE = 0
            cs = self.promote(':branch--localhost2@foo:bar', 'simple',
                              cloneSources=True)
        finally:
            conaryclient.clone.CHANGESET_MULTIPLE = old

        # Make sure the promotes to the other repositories are complete
        self.stopRepository(0)
        for x in (1, 2):
            self.updatePkg('simple=localhost%d@foo:bar' % x)
            self.resetRoot()
