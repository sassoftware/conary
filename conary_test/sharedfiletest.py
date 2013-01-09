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
import os
import grp, pwd

from conary_test import rephelp
from conary_test.rephelp import RegularFile as RegF


class SharedFileTest(rephelp.RepositoryHelper):

    def owners(self, path):
        db = self.openDatabase()
        return set( x[0:3] for x in db.iterFindPathReferences(
                                            path, justPresent = True) )

    def checkOwners(self, path, troves):
        assert(self.owners(path) ==
                set( x.getNameVersionFlavor() for x in troves ))

    @testhelp.context('rollback', 'fileoverlap')
    def testBasicSharedFiles(self):
        # these share fileId's
        foo1 = self.addComponent('foo:runtime=1[is:x86]',
                                 fileContents = [ ('/foo', '1') ])
        bar1 = self.addComponent('bar:runtime=1[is:x86]',
                                 fileContents = [ ('/foo', '1') ])
        baz1 = self.addComponent('baz:runtime=1[is:x86]',
                                 fileContents = [ ('/foo', '1') ])
        foo2 = self.addComponent('foo:runtime=2[is:x86]',
                                 fileContents = [ ('/foo', '2') ])

        self.updatePkg('foo:runtime=1')
        self.updatePkg('bar:runtime=1')
        self.checkOwners('/foo', [ bar1, foo1 ] )
        self.rollback(self.rootDir, 1)
        self.verifyFile(self.rootDir + '/foo', '1')
        self.checkOwners('/foo', [ foo1 ] )
        self.rollback(self.rootDir, 0)
        assert(not os.path.exists(self.rootDir + '/foo'))

        # what if we install them at the same time?
        self.resetRoot()
        self.updatePkg(['foo:runtime=1', 'bar:runtime=1'])
        self.checkOwners('/foo', [ bar1, foo1 ])
        self.updatePkg('baz:runtime=1', keepExisting = True)
        self.checkOwners('/foo', [ bar1, baz1, foo1 ])

        # now blow away the shared file
        self.updatePkg('foo:runtime=2', replaceFiles = True)
        self.checkOwners('/foo', [ foo2 ])

        self.rollback(self.rootDir, 2)
        self.checkOwners('/foo', [ bar1, baz1, foo1 ])
        self.rollback(self.rootDir, 1)
        self.checkOwners('/foo', [ bar1, foo1 ])
        self.rollback(self.rootDir, 0)
        assert(not os.path.exists(self.rootDir + '/foo'))

    @testhelp.context('rollback', 'fileoverlap')
    def testSharedUpdate(self):
        foo1 = self.addComponent('foo:runtime=1',
                                 fileContents = [ ('/foo', '1') ])
        bar1 = self.addComponent('bar:runtime=1',
                                 fileContents = [ ('/foo', '1') ])
        foo2 = self.addComponent('foo:runtime=2',
                                 fileContents = [ ('/foo', '2') ])
        bar2 = self.addComponent('bar:runtime=2',
                                 fileContents = [ ('/foo', '2') ])

        self.updatePkg([ 'foo:runtime=1', 'bar:runtime=1' ])
        self.verifyFile(self.rootDir + '/foo', '1')
        self.updatePkg([ 'foo:runtime=2', 'bar:runtime=2' ])
        self.checkOwners('/foo', [ foo2, bar2 ])
        self.verifyFile(self.rootDir + '/foo', '2')
        self.rollback(1)
        self.verifyFile(self.rootDir + '/foo', '1')
        self.checkOwners('/foo', [ foo1, bar1 ])

    @testhelp.context('rollback', 'fileoverlap')
    def testSharedErasures(self):
        foo1 = self.addComponent('foo:runtime=1',
                                 fileContents = [ ('/foo', '1') ])
        bar1 = self.addComponent('bar:runtime=1',
                                 fileContents = [ ('/foo', '1') ])
        self.updatePkg([ 'foo:runtime', 'bar:runtime' ])
        self.checkOwners('/foo', [ foo1, bar1 ])
        self.erasePkg(self.rootDir, 'foo:runtime')
        self.verifyFile(self.rootDir + '/foo', '1')
        self.checkOwners('/foo', [ bar1 ])

    @testhelp.context('rollback', 'fileoverlap')
    def testSharedHardLinks(self):
        info = {
            'user': pwd.getpwuid(os.getuid())[0],
            'group': grp.getgrgid(os.getgid())[0],
        }

        foo = self.addComponent('foo:runtime', '1.0-1-1',
            fileContents = [
                ( '/a', rephelp.RegularFile(contents = "a1", pathId = "1",
                  linkGroup = "\0" * 16, owner = info['user'],
                  group = info['group'] ) ),
                ( '/foo', rephelp.RegularFile(contents = "a1", pathId = "2",
                  linkGroup = "\0" * 16) ),
            ]
        )

        bar = self.addComponent('bar:runtime', '1.0-1-1',
            fileContents = [
                ( '/a', rephelp.RegularFile(contents = "a1", pathId = "1",
                  linkGroup = "\1" * 16, owner = info['user'],
                  group = info['group'] ) ),
                ( '/bar', rephelp.RegularFile(contents = "a1", pathId = "2",
                  linkGroup = "\1" * 16) ),
            ]
        )

        self.updatePkg('foo:runtime')
        self.updatePkg('bar:runtime', keepExisting = True)
        self.checkOwners('/a', [ foo, bar ])

        # make sure we can build a group with both troves
        groupRecipe = r"""
class GroupConflicts(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    clearBuildRequires()
    autoResolve = False

    def setup(r):
        r.VersionConflicts(exceptions = 'group-dist')
        r.add('foo:runtime')
        r.add('bar:runtime')
"""
        built, d = self.buildRecipe(groupRecipe, "GroupConflicts")

    @testhelp.context('fileoverlap')
    def testSharedIntialContents(self):
        foo = self.addComponent('foo:runtime', fileContents = [
            ('/ic', RegF(contents = 'hello', initialContents = True) ) ])
        bar = self.addComponent('bar:runtime', fileContents = [
            ('/ic', RegF(contents = 'world', initialContents = True) ) ])

        self.updatePkg('foo:runtime')
        self.updatePkg('bar:runtime')
        self.checkOwners('/ic', [ foo, bar ])

        self.resetRoot()
        self.updatePkg(['foo:runtime', 'bar:runtime'])
        self.checkOwners('/ic', [ foo, bar ])

        self.resetRoot()
        self.updatePkg(['foo:runtime', 'bar:runtime'], justDatabase = True)
        self.checkOwners('/ic', [ foo, bar ])

        # make sure we can build a group with both troves
        groupRecipe = r"""
class GroupConflicts(GroupRecipe):
    name = 'group-dist'
    version = '1.0'
    clearBuildRequires()
    autoResolve = False

    def setup(r):
        r.VersionConflicts(exceptions = 'group-dist')
        r.add('foo:runtime')
        r.add('bar:runtime')
"""
        built, d = self.buildRecipe(groupRecipe, "GroupConflicts")

    @testhelp.context('fileoverlap')
    def testSharedFileDifferentContentTypes(self):
        # sometimes the updates give files, sometimes diffs. that's
        # reconcileable
        info = {
            'owner': pwd.getpwuid(os.getuid())[0],
            'group': grp.getgrgid(os.getgid())[0],
        }

        # it is important here that the pathId is the same as this is
        # testing conflicts in contents in the config files (CNY-3631)
        # we also test bar as well as gax to test merge orders against foo
        # (alphabetical). also tests (CNy-3635) which is the same, but
        # different (group-bar=3 in particular)
        self.addComponent('foo:runtime=1', fileContents = [
                ( '/etc/f', RegF(contents='hello', **info) ) ] )

        self.addComponent('bar:runtime=2', fileContents = [
                ( '/etc/f', RegF(contents='world', **info) ) ] )
        self.addComponent('foo:runtime=2', fileContents = [
                ( '/etc/f', RegF(contents='world', **info) ) ] )
        self.addComponent('gax:runtime=2', fileContents = [
                ( '/etc/f', RegF(contents='world', **info) ) ] )

        self.addComponent('bar:runtime=3', fileContents = [
                ( '/etc/f', RegF(contents='hello world', **info) ) ] )
        self.addComponent('foo:runtime=3', fileContents = [
                ( '/etc/f', RegF(contents='hello world', **info) ) ] )

        self.addCollection('group-bar=2', [ 'foo:runtime', 'bar:runtime' ] )
        self.addCollection('group-bar=3', [ 'foo:runtime', 'bar:runtime' ] )
        self.addCollection('group-gax=2', [ 'foo:runtime', 'gax:runtime' ] )

        self.updatePkg('foo:runtime=1')
        self.updatePkg('group-bar=2', replaceFiles = True)
        self.updatePkg('group-bar=3')

        self.resetRoot()

        self.updatePkg('foo:runtime=1')
        self.updatePkg('group-gax', replaceFiles = True)

    @testhelp.context('fileoverlap')
    def testDoubleReplaceFiles(self):
        self.addComponent('foo:runtime=1', fileContents = [
            ('/f', RegF(contents = 'hello') ) ] )
        self.addComponent('bar1:runtime=1', fileContents = [
            ('/f', RegF(contents = 'world') ) ] )
        self.addComponent('bar2:runtime=1', fileContents = [
            ('/f', RegF(contents = 'world') ) ] )

        self.addCollection('group-bar=1', [ 'bar1:runtime', 'bar2:runtime' ])
        self.updatePkg('foo:runtime')
        self.updatePkg('group-bar', replaceFiles = True)
        self.verifyFile(self.rootDir + '/f', 'world')
