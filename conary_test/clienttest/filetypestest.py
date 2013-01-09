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
from StringIO import StringIO
import tempfile

from conary.conaryclient import filetypes
from conary.deps import deps
from conary.lib import util

pathId = 16 * chr(0)

class ClientNewTroveTest(testhelp.TestCase):
    def testRegularFileBasics(self):
        foo = filetypes.RegularFile(contents = 'foo1')
        fileObj = foo.get(pathId)
        f = foo.getContents()
        self.assertEquals(f.read(), 'foo1')
        self.assertEquals(fileObj.flags(), 0)
        self.assertEquals(fileObj.flavor(), deps.Flavor())
        self.assertEquals(fileObj.provides(), deps.DependencySet())
        self.assertEquals(fileObj.requires(), deps.DependencySet())
        self.assertEquals(fileObj.inode.perms(), 0644)
        self.assertEquals(fileObj.inode.owner(), 'root')
        self.assertEquals(fileObj.inode.group(), 'root')
        self.assertEquals(fileObj.lsTag, '-')
        self.assertEquals(fileObj.linkGroup(), None)
        self.assertEquals(fileObj.fileId(),
              '(\x01\x9a\xcbz\xbb\x93\x15\x01c\xcf\xd5\x14\xef\xf7,S\xbb\xf8p')

        requires = deps.ThawDependencySet('4#foo::runtime')
        provides = deps.ThawDependencySet('11#foo')
        flv = deps.parseFlavor('xen,domU is: x86')
        bar = filetypes.RegularFile(contents = StringIO('bar'),
                config = True, provides = provides, requires = requires,
                flavor = flv, owner = 'foo', group = 'bar', perms = 0700,
                mtime = 12345, tags = ['tag1', 'tag2'])
        fileObj = bar.get(pathId)
        self.assertEquals(bool(fileObj.flags.isInitialContents()), False)
        self.assertEquals(bool(fileObj.flags.isTransient()), False)
        self.assertEquals(bool(fileObj.flags.isConfig()), True)
        self.assertEquals(fileObj.requires(), requires)
        self.assertEquals(fileObj.provides(), provides)
        self.assertEquals(fileObj.flavor(), flv)
        self.assertEquals(fileObj.inode.perms(), 0700)
        self.assertEquals(fileObj.inode.owner(), 'foo')
        self.assertEquals(fileObj.inode.group(), 'bar')
        self.assertEquals(fileObj.inode.mtime(), 12345)
        self.assertEquals(fileObj.tags(), ['tag1', 'tag2'])

    def testRegularFileDeps(self):
        reqStr = 'trove: bar:lib'
        provStr = 'python: tarfile(2.4 lib64)'
        flavorStr = '~sse2 is: x86_64'
        foo = filetypes.RegularFile(requires = reqStr,
                provides = provStr, flavor = flavorStr)
        fileObj = foo.get(pathId)
        self.assertEquals(fileObj.flavor(), deps.parseFlavor(flavorStr))
        self.assertEquals(fileObj.requires(), deps.parseDep(reqStr))
        self.assertEquals(fileObj.provides(), deps.parseDep(provStr))

    def testRegularFileContents(self):
        foo = filetypes.RegularFile(contents = StringIO('foo1'))
        fileObj = foo.get(pathId)
        f = foo.getContents()
        self.assertEquals(f.read(), 'foo1')

        tmpDir = tempfile.mkdtemp()
        try:
            tmpPath = os.path.join(tmpDir, 'foo.txt')
            f = open(tmpPath, 'w')
            f.write('foo2')
            f.close()
            f = open(tmpPath)
            foo = filetypes.RegularFile(contents = f)
            f = foo.getContents()
            self.assertEquals(f.read(), 'foo2')
        finally:
            util.rmtree(tmpDir)

    def testSimpleTypes(self):
        for klass, lsTag in ((filetypes.Directory, 'd'),
                             (filetypes.NamedPipe, 'p'),
                             (filetypes.Socket, 's')):
            foo = klass(owner = 'foo', group = 'foo')
            fileObj = foo.get(pathId)
            self.assertEquals(fileObj.inode.perms(), 0755)
            self.assertEquals(fileObj.lsTag, lsTag)

            self.assertRaises(filetypes.ParameterError, klass,
                    initialContents = True)
            self.assertRaises(filetypes.ParameterError, klass,
                    transient = True)
            self.assertEquals(foo.getContents(), None)

    def testSymlink(self):
        foo = filetypes.Symlink('/bar')
        fileObj = foo.get(pathId)
        self.assertEquals(fileObj.target(), '/bar')
        self.assertEquals(fileObj.lsTag, 'l')
        self.assertRaises(filetypes.ParameterError, filetypes.Symlink,
                '/bar', initialContents = True)
        self.assertRaises(filetypes.ParameterError, filetypes.Symlink,
                '/bar', transient = True)
        self.assertRaises(filetypes.ParameterError, filetypes.Symlink,
                '/bar', perms = 0600)
        self.assertRaises(filetypes.ParameterError, filetypes.Symlink,
                '/bar', mode = 0755)
        self.assertEquals(foo.getContents(), None)

    def testBlockDevice(self):
        foo = filetypes.BlockDevice(8, 1)
        fileObj = foo.get(pathId)
        self.assertEquals(fileObj.lsTag, 'b')
        self.assertEquals(fileObj.devt.major(), 8)
        self.assertEquals(fileObj.devt.minor(), 1)
        self.assertEquals(foo.getContents(), None)

        requires = deps.ThawDependencySet('4#foo::runtime')
        provides = deps.ThawDependencySet('11#foo')
        foo = filetypes.BlockDevice(8, 1, provides = provides,
                requires = requires)
        fileObj = foo.get(pathId)
        self.assertEquals(fileObj.provides(), provides)
        self.assertEquals(fileObj.requires(), requires)

    def testCharacterDevice(self):
        foo = filetypes.CharacterDevice(1, 5)
        fileObj = foo.get(pathId)
        self.assertEquals(fileObj.lsTag, 'c')
        self.assertEquals(fileObj.devt.major(), 1)
        self.assertEquals(fileObj.devt.minor(), 5)
        self.assertEquals(foo.getContents(), None)

        requires = deps.ThawDependencySet('4#foo::runtime')
        provides = deps.ThawDependencySet('11#foo')
        foo = filetypes.CharacterDevice(1, 5, provides = provides,
                requires = requires)
        fileObj = foo.get(pathId)
        self.assertEquals(fileObj.provides(), provides)
        self.assertEquals(fileObj.requires(), requires)

    def testLinkGroup(self):
        foo = filetypes.RegularFile(linkGroup = '12345')
        fileObj = foo.get(pathId)
        self.assertEquals(fileObj.linkGroup(), '12345')

    def testTags(self):
        # tags is a list. ensure nothing sloppy is done wrt class attributes
        foo = filetypes.RegularFile(tags = ['1', '2', '3'])
        fileObj = foo.get(pathId)
        self.assertEquals(fileObj.tags(), ['1', '2', '3'])

        # test that each invocation is separate
        foo = filetypes.RegularFile(tags = ['4', '5'])
        fileObj = foo.get(pathId)
        self.assertEquals(fileObj.tags(), ['4', '5'])

        # test that we didn't affect the default
        foo = filetypes.RegularFile()
        fileObj = foo.get(pathId)
        self.assertEquals(fileObj.tags(), [])

    def testConflictingFlags(self):
        self.assertRaises(filetypes.ConflictingFlags,
                filetypes.RegularFile, config = True, transient = True)
        self.assertRaises(filetypes.ConflictingFlags,
                filetypes.RegularFile, config = True, initialContents = True)
        self.assertRaises(filetypes.ConflictingFlags,
                filetypes.RegularFile, transient = True,
                initialContents = True)
        self.assertRaises(filetypes.ConflictingFlags,
                filetypes.RegularFile, config = True, transient = True,
                initialContents = True)

    def testModeAlias(self):
        foo = filetypes.RegularFile(mode = 0777)
        bar = filetypes.RegularFile(perms = 0777)
        self.assertEquals(foo.get(pathId).inode.perms(),
                bar.get(pathId).inode.perms())

        self.assertRaises(filetypes.ParameterError, filetypes.RegularFile,
                mode = 0600, perms = 0600)

        self.assertRaises(filetypes.ParameterError, filetypes.RegularFile,
                mode = 0600, perms = 0700)

    def testPathIdParam(self):
        pathId1 = 16 * '1'
        pathId2 = 16 * '2'

        foo = filetypes.RegularFile(mode = 0777, mtime = 1)

        fileObj1 = foo.get(pathId1)
        fileObj2 = foo.get(pathId2)

        self.assertEquals(fileObj1.freeze(), fileObj2.freeze())
        self.assertNotEquals(fileObj1.pathId(), fileObj2.pathId())
