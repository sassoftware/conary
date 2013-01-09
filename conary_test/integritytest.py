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


import gzip
import os
import tempfile

from conary_test import recipes
from conary_test import rephelp

from conary import files
from conary.lib import sha1helper, util
from conary.repository import changeset
from conary.repository import errors
from conary.repository import filecontainer
from conary.repository import filecontents
from conary import deps
from conary import trove
from conary import versions

def open(fn, mode='r', buffering=False):
    return util.ExtendedFile(fn, mode, buffering)

class IntegrityTest(rephelp.RepositoryHelper):

    def testFileChanges(self):
        (built, d) = self.buildRecipe(recipes.testRecipe1, "TestRecipe1")
        (name, version, flavor) = built[0]
        version = versions.VersionFromString(version)

        (fd, path) = tempfile.mkstemp()
        os.close(fd)

        repos = self.openRepository()
        repos.createChangeSetFile(
            [(name, (None, None), (version, flavor), 1)], path)

        infc = filecontainer.FileContainer(open(path))
        os.unlink(path)

        (fd, path) = tempfile.mkstemp()
        os.close(fd)
        outfc = filecontainer.FileContainer(open(path, "w"))

        # first is the trove delta
        (name, tag, f) = infc.getNextFile()
        outfc.addFile(name, filecontents.FromFile(f), tag,
                      precompressed = True)

        # next let's modify the file a bit
        (name, tag, f) = infc.getNextFile()
        contents = gzip.GzipFile(None, "r", fileobj = f).read()
        contents = chr(ord(contents[0]) ^ 0xff) + contents[1:]
        outfc.addFile(name, filecontents.FromString(contents), tag)

        next = infc.getNextFile()
        while next is not None:
            (name, tag, f) = next
            outfc.addFile(name, filecontents.FromFile(f), tag,
                          precompressed = True)
            next = infc.getNextFile()

        infc.close()
        outfc.close()

        self.resetRepository()
        repos = self.openRepository()

        try:
            self.assertRaises(errors.IntegrityError,
                              self.updatePkg, self.rootDir, path)
            self.assertRaises(errors.IntegrityError,
                              repos.commitChangeSetFile, path)
        finally:
            os.unlink(path)

    def testFileObjMissing(self):
        # create an absolute changeset
        cs = changeset.ChangeSet()

        # add a pkg diff
        flavor = deps.deps.parseFlavor('')
        v = versions.VersionFromString('/%s/1.0-1-1'
                                       %self.cfg.buildLabel.asString()).copy()
        v.resetTimeStamps()
        t = trove.Trove('test:test', v, flavor, None)
        path = self.workDir + '/blah'
        f = open(path, 'w')
        f.write('hello, world!\n')
        f.close()
        pathId = sha1helper.md5String('/blah')
        f = files.FileFromFilesystem(path, pathId)
        # add the file, and SKIP including
        # the filestream by using cs.addFile().  This creates an
        # incomplete changeset
        t.addFile(pathId, '/blah', v, f.fileId())
        cs.addFileContents(pathId, f.fileId(), changeset.ChangedFileTypes.file,
                           filecontents.FromFilesystem(path),
                           f.flags.isConfig())

        t.computeDigests()
        diff = t.diff(None, absolute = 1)[0]
        cs.newTrove(diff)

        repos = self.openRepository()
        try:
            repos.commitChangeSet(cs)
            assert 0, "Did not raise IntegrityError"
        except errors.IntegrityError, e:
            assert(str(e).startswith("Incomplete changeset specified: missing pathId e806729b6a2b568fa7e77c3efa3a9684 fileId"))

    def testFileIdWrong(self):
        # create an absolute changeset
        cs = changeset.ChangeSet()

        # add a pkg diff
        flavor = deps.deps.parseFlavor('')
        v = versions.VersionFromString('/%s/1.0-1-1'
                                       %self.cfg.buildLabel.asString()).copy()
        v.resetTimeStamps()
        t = trove.Trove('test:test', v, flavor, None)
        path = self.workDir + '/blah'
        f = open(path, 'w')
        f.write('hello, world!\n')
        f.close()
        pathId = sha1helper.md5String('/blah')
        f = files.FileFromFilesystem(path, pathId)
        # add the file, but munge the fileid
        brokenFileId = ''.join(reversed(f.fileId()))
        cs.addFile(None, brokenFileId, f.freeze())
        t.addFile(pathId, '/blah', v, brokenFileId)
        t.computeDigests()

        diff = t.diff(None, absolute = 1)[0]
        cs.newTrove(diff)

        repos = self.openRepository()
        try:
            repos.commitChangeSet(cs)
            assert 0, "Integrity Error not raised"
        except errors.TroveIntegrityError, e:
            assert(str(e) == 'fileObj.fileId() != fileId in changeset for '
                             'pathId %s' % sha1helper.md5ToString(pathId))

    def testFileContentsMissing(self):
        # currently causes a 500 error
        #raise testhelp.SkipTestException
        # create an absolute changeset
        cs = changeset.ChangeSet()

        # add a pkg diff
        flavor = deps.deps.parseFlavor('')
        v = versions.VersionFromString('/%s/1.0-1-1'
                                       %self.cfg.buildLabel.asString()).copy()
        v.resetTimeStamps()
        t = trove.Trove('test:test', v, flavor, None)
        path = self.workDir + '/blah'
        f = open(path, 'w')
        f.write('hello, world!\n')
        f.close()
        pathId = sha1helper.md5String('/blah')
        f = files.FileFromFilesystem(path, pathId)
        # add the file, but munge the fileid
        fileId = f.fileId()
        cs.addFile(None, fileId, f.freeze())
        t.addFile(pathId, '/blah', v, fileId)
        # skip adding the file contents
        t.computeDigests()

        diff = t.diff(None, absolute = 1)[0]
        cs.newTrove(diff)

        repos = self.openRepository()
        try:
            repos.commitChangeSet(cs)
            assert 0, "Did not raise integrity error"
        except errors.IntegrityError, e:
            assert(str(e).startswith("Missing file contents for pathId e806729b6a2b568fa7e77c3efa3a9684, fileId"))

    def testSourceItemCollision(self):
        repos = self.openRepository()
        # add the conflicting source item troves
        self.addComponent("foo:source", "1.0")
        self.addComponent("test:runtime", "1.0", "foo,!bar",
                          sourceName="foo:source")
        self.addComponent("test:lib", "1.0", "foo,!bar",
                          sourceName="foo:source")
        self.addCollection("test", "1.0", [
            ("test:runtime", "1.0", "foo,!bar"),
            ("test:lib", "1.0", "foo,!bar"), ])
        # same troves, different flavor based on a different source
        self.addComponent("bar:source", "1.0")
        self.assertRaises(errors.InvalidSourceNameError, self.addComponent,
                          "test:runtime", "1.0", "!foo,bar", sourceName = "bar:source")
