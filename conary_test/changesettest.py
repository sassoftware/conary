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


import os
import tempfile

from conary_test import rephelp
import StringIO

from conary import errors
from conary.repository import changeset, filecontainer, filecontents
from conary.lib import sha1helper, util


class ChangeSetTest(rephelp.RepositoryHelper):

    def testCreateChangeSet(self):
        self.addQuickTestComponent('test:runtime', '1.0-1-1')
        self.addQuickTestCollection('test', '1.0-1-1', ['test:runtime'])
        self.addQuickTestCollection('group-test', '1.0-1-1', 
                                    # byDefault = False
                                    [('test', None, None, False)])
        repos = self.openRepository()
        csPath = self.workDir + '/group-test.ccs'
        self.changeset(repos, ['group-test'], csPath)

        # make sure x bit isn't set
        assert(os.stat(csPath).st_mode & 0111 == 0)

        cs = changeset.ChangeSetFromFile(csPath)

        # make sure the byDefault False troves are not in the changeset.
        assert(set([x.getName() for x in cs.iterNewTroveList()])
                                                    == set(['group-test']))

        # make sure this changeset is installable
        self.updatePkg(self.rootDir, [csPath])

    def testMergedDiffOrder(self):
        t1 = self.addComponent('test:runtime', '1.0-1-1',
                               fileContents = [ ("/etc/cfg", "contents1\n") ] )
        t2 = self.addComponent('test:runtime', '2.0-1-1',
                               fileContents = [ ("/etc/cfg", "contents2\n") ] )
        # This filename is magically designed to have it's pathId before
        # the pathId for /etc/cfg (the pathId in addComponent is a
        # simple md5 of the path)
        o  = self.addComponent('other:runtime', '1.0-1-1',
                               fileContents = [ ("/etc/one", "something") ] )

        repos = self.openRepository()
        (fd, path) = tempfile.mkstemp()
        os.close(fd)
        repos.createChangeSetFile(
                    [ (o.getName(),  (None, None),
                                     (o.getVersion(), o.getFlavor()),
                       True),
                      (t1.getName(), (t1.getVersion(), t1.getFlavor()),
                                     (t2.getVersion(), t2.getFlavor()),
                       False) ], path)

        f = util.ExtendedFile(path, "r", buffering = False)
        os.unlink(path)
        fc = filecontainer.FileContainer(f)

        # first comes the set of troveCs objects
        (name, tag, size) = fc.getNextFile()
        assert(name == 'CONARYCHANGESET')

        # next is the diff
        (name, tag, size) = fc.getNextFile()
        assert(tag == "1 diff")

        # and then the config file
        (name, tag, size) = fc.getNextFile()
        assert(tag == "1 file")

        # and that's it
        rc = fc.getNextFile()
        assert(rc is None)

    def testMergedConfigOrder(self):
        # Make sure that config files from absolute change sets are merged
        # correctly relative to config files from relative ones.
        t1 = self.addComponent('test:runtime', '1.0-1-1',
                               fileContents = [ ("/etc/cfg", "contents1\n") ] )
        # This filename is magically designed to have it's pathId before
        # the pathId for /etc/cfg (the pathId in addComponent is a
        # simple md5 of the path)
        o  = self.addComponent('other:runtime', '1.0-1-1',
                               fileContents = [ ("/etc/one", "something") ] )

        repos = self.openRepository()
        (fd, path) = tempfile.mkstemp()
        os.close(fd)
        repos.createChangeSetFile(
                    [ (o.getName(),  (None, None),
                                     (o.getVersion(), o.getFlavor()),
                       False),
                      (t1.getName(), (None, None),
                                     (t1.getVersion(), t1.getFlavor()),
                       True) ], path)

        f = util.ExtendedFile(path, "r", buffering = False)
        os.unlink(path)
        fc = filecontainer.FileContainer(f)

        # first comes the set of troveCs objects
        (name, tag, size) = fc.getNextFile()
        assert(name == 'CONARYCHANGESET')

        # next is the diff
        (name, tag, size) = fc.getNextFile()
        assert(name[0:16] == sha1helper.md5String("/etc/one"))

        # and then the config file
        (name, tag, size) = fc.getNextFile()
        assert(name[0:16] == sha1helper.md5String("/etc/cfg"))

        # and that's it
        rc = fc.getNextFile()
        assert(rc is None)

    def testConfigFileMerges(self):
        # make sure config files are merged properly; at one point we only
        # merged diffs, now all our merged
        cs1 = changeset.ReadOnlyChangeSet()
        cs2 = changeset.ChangeSet()

        cs2.addFileContents('0' * 16, '0' * 20, changeset.ChangedFileTypes.file,
                            filecontents.FromString('first'), cfgFile = True)
        cs2.addFileContents('1' * 16, '1' * 20, changeset.ChangedFileTypes.file,
                            filecontents.FromString('first'), cfgFile = True)

        cs1.merge(cs2)
        assert(len(cs1.configCache) == 2)

    def testConfigFilesRaisePathIdsConflict(self):
        # test to make sure that one changeset's config cache doesn't
        # override another's
        cs1 = changeset.ChangeSet()
        cs2 = changeset.ChangeSet()
        mergeSet = changeset.ReadOnlyChangeSet()

        # build two changesets, both with config file diffs that have the same
        # pathid and fileid
        cs1.addFileContents('0' * 16, '0' * 20, changeset.ChangedFileTypes.diff,
                            filecontents.FromString('first'), cfgFile = True)
        cs2.addFileContents('0' * 16, '0' * 20, changeset.ChangedFileTypes.diff,
                            filecontents.FromString('second'), cfgFile = True)
        mergeSet.merge(cs1)
        # second merge now handled without ChangeSetKeyConflictError: CNY-3635
        mergeSet.merge(cs1)

        cs1 = changeset.ChangeSet()
        cs1.addFileContents('0' * 16, '0' * 20, changeset.ChangedFileTypes.diff,
                            filecontents.FromString('first'), cfgFile = True)
        try:
            cs1.addFileContents('0' * 16, '0' * 20,
                                changeset.ChangedFileTypes.diff,
                                filecontents.FromString('second'),
                                cfgFile = True)
        except changeset.ChangeSetKeyConflictError, e:
            assert str(e) == 'ChangeSetKeyConflictError: 30303030303030303030303030303030,3030303030303030303030303030303030303030'
        else:
            assert(0)

        cs1 = changeset.ChangeSet()
        # this is blatantly illegal; diff non-config files!
        cs1.addFileContents('0' * 16, '0' * 20, changeset.ChangedFileTypes.diff,
                            filecontents.FromString('first'), cfgFile = False)
        try:
            cs1.addFileContents('0' * 16, '0' * 20,
                                changeset.ChangedFileTypes.diff,
                                filecontents.FromString('second'),
                                cfgFile = False)
        except changeset.ChangeSetKeyConflictError, e:
            assert str(e) == 'ChangeSetKeyConflictError: 30303030303030303030303030303030,3030303030303030303030303030303030303030'
        else:
            assert(0)

        # build a changeset, both with two config files with the same
        # pathid and fileid. One is a diff and the other is not.  This should
        # be ok the diff will be used.
        cs1 = changeset.ChangeSet()
        cs1.addFileContents('0' * 16, '0' * 20, 
                            changeset.ChangedFileTypes.diff,
                            filecontents.FromString('some diff'),
                            cfgFile = True)
        cs1.addFileContents('0' * 16, '0' * 20,
                            changeset.ChangedFileTypes.file,
                            filecontents.FromString('some config'),
                            cfgFile = True)

        # build a changeset, both with two config files with the same
        # pathid and fileid. Both are identical diffs.  This should
        # be ok the diff will be used.
        cs1 = changeset.ChangeSet()
        cs1.addFileContents('0' * 16, '0' * 20, 
                            changeset.ChangedFileTypes.diff,
                            filecontents.FromString('the diff'),
                            cfgFile = True)
        cs1.addFileContents('0' * 16, '0' * 20,
                            changeset.ChangedFileTypes.diff,
                            filecontents.FromString('the diff'),
                            cfgFile = True)

    def testBadChangeSet(self):
        csPath = self.workDir + '/test.ccs'
        open(csPath, "w").write("some junk")
        try:
            changeset.ChangeSetFromFile(csPath)
        except errors.ConaryError, e:
            assert(str(e) == 'File %s is not a valid conary changeset.' % 
                        csPath)

    def testCreateChangeSetInvalidFile(self):
        # Tests that createChangeSetFile raises FilesystemError if it tries to
        # write the changeset to an invalid file (directory, not writeable
        # etc)
        t1 = self.addComponent('test:runtime', '1.0-1-1',
                               fileContents = [ ("/etc/cfg", "contents1\n") ] )
        o  = self.addComponent('other:runtime', '1.0-1-1',
                               fileContents = [ ("/etc/one", "something") ] )

        jobList = [ 
            (o.getName(), (None, None), (o.getVersion(), o.getFlavor()),
                          False),
            (t1.getName(), (None, None), (t1.getVersion(), t1.getFlavor()),
                       True),
        ]

        # Create read-only file
        (fd, rofile) = tempfile.mkstemp()
        os.close(fd)
        os.chmod(rofile, 0400)
        bad_paths = [rofile, '/tmp']

        repos = self.openRepository()
        
        for path in bad_paths:
            self.assertRaises(errors.FilesystemError,
                repos.createChangeSetFile, jobList, path)

        # Test the exception arguments while we're at it
        try:
            repos.createChangeSetFile(jobList, rofile)
        except errors.FilesystemError, e:
            self.assertEqual(e.errorCode, 13)
            self.assertEqual(e.path, rofile)
            self.assertEqual(e.errorString, 'Permission denied')
        except:
            self.fail()
        else:
            self.fail()

        # Cleanup
        os.chmod(rofile, 0600)
        os.unlink(rofile)

    def testReset(self):

        class UncloseableFile(StringIO.StringIO):

            def close(self):
                assert(0)

        # Make sure writing a changeset doesn't close the filecontent objects
        # inside of it.
        otherCs = changeset.ChangeSet()
        otherCs.addFileContents("0pathId", "0fileId",
                                changeset.ChangedFileTypes.file,
                                filecontents.FromFile(UncloseableFile("foo")),
                                1)
        otherCs.addFileContents("1pathId", "1fileId",
                                changeset.ChangedFileTypes.file,
                                filecontents.FromFile(UncloseableFile("foo")),
                                0)
        otherCs.writeToFile(self.workDir + "/test1.ccs")

        cs = changeset.ReadOnlyChangeSet()
        cs.merge(otherCs)
        cs.writeToFile(self.workDir + "/test2.ccs")
        assert(open(self.workDir + "/test1.ccs").read() ==
              (open(self.workDir + "/test2.ccs").read()))

        cs.reset()
        cs.writeToFile(self.workDir + "/test3.ccs")
        assert(open(self.workDir + "/test1.ccs").read() ==
              (open(self.workDir + "/test3.ccs").read()))

        otherCs = changeset.ChangeSetFromFile(self.workDir + "/test1.ccs")
        cs1 = changeset.ReadOnlyChangeSet()
        cs1.merge(otherCs)

        otherCs.reset()
        cs2 = changeset.ReadOnlyChangeSet()
        cs2.merge(otherCs)

        assert([ x[0] for x in cs1.fileQueue ] ==
                    [ x[0] for x in cs2.fileQueue ])
        assert(cs1.configCache == cs2.configCache)

    def testChangeSetFromFileSetsFilename(self):
        repos = self.openRepository()
        self.addComponent('foo:run', '1')
        self.addCollection('foo', '1', [':run'])
        csname = os.path.join(self.workDir, "changeset-lazyFileCache.ccs")
        self.changeset(repos, 'foo', csname)

        self.assertTrue(os.path.exists(csname))
        lfc = util.LazyFileCache()
        cs = changeset.ChangeSetFromFile(lfc.open(csname))
        self.assertEqual(cs.fileName, csname)

    def testGitDiff(self):
        t1 = self.addComponent('foo:run=1', fileContents = [])
        t2 = self.addComponent('foo:run=2',
            fileContents = [
                ('/text', 'new contents\n'),
                ('/etc/config', 'config contents\n'),
                ('/binary', '\x80'),
            ])
        t21 = self.addComponent('foo:run=2.1',
            fileContents = [
                ('/text', 'new contents\n'),
                ('/etc/config', 'config contents\n'),
                ('/binary', 'utf-8'),
            ])
        t3 = self.addComponent('foo:run=3',
            fileContents = [
                ('/text', 'text contents\n'),
                ('/etc/config', 'changed config\n'),
                ('/binary', '\x80'),
            ])
        t4 = self.addComponent('foo:run=4',
            fileContents = [
                ('/text', 'text contents\n'),
                ('/etc/config', 'changed config\n'),
                ('/binary', rephelp.RegularFile(contents = '\x80',
                                                perms = 0755)),
            ])
        t5 = self.addComponent('foo:run=5',
            fileContents = [
                ('/etc/config', 'changed config\n'),
                ('/pipe', rephelp.NamedPipe()),
            ])

        repos = self.openRepository()
        cs = repos.createChangeSet([ ('foo:run',  t1.getNameVersionFlavor()[1:],
                                      t2.getNameVersionFlavor()[1:], False) ])
        diff = "".join(x for x in cs.gitDiff(repos))
        self.assertEquals(diff,
                "diff --git a/etc/config b/etc/config\n"
                "new user root\n"
                "new group root\n"
                "new mode 100644\n"
                "--- a/dev/null\n"
                "+++ b/etc/config\n"
                "@@ -1,0 +1,1 @@\n"
                "+config contents\n"
                "diff --git a/text b/text\n"
                "new user root\n"
                "new group root\n"
                "new mode 100644\n"
                "--- a/dev/null\n"
                "+++ b/text\n"
                "@@ -1,0 +1,1 @@\n"
                "+new contents\n"
                "diff --git a/binary b/binary\n"
                "new user root\n"
                "new group root\n"
                "new mode 100644\n"
                "GIT binary patch\n"
                "literal 1\n"
                "Ic${kh004mifdBvi\n"
                "\n")

        cs = repos.createChangeSet([ ('foo:run',  t2.getNameVersionFlavor()[1:],
                                      t3.getNameVersionFlavor()[1:], False) ])
        diff = "".join(x for x in cs.gitDiff(repos))
        self.assertEquals(diff,
            "diff --git a/etc/config b/etc/config\n"
            "--- a/etc/config\n"
            "+++ b/etc/config\n"
            "@@ -1,1 +1,1 @@\n"
            "-config contents\n"
            "+changed config\n"
            "diff --git a/text b/text\n"
            "--- a/text\n"
            "+++ b/text\n"
            "@@ -1,1 +1,1 @@\n"
            "-new contents\n"
            "+text contents\n")

        cs = repos.createChangeSet([ ('foo:run',  t3.getNameVersionFlavor()[1:],
                                      t4.getNameVersionFlavor()[1:], False) ])
        diff = "".join(x for x in cs.gitDiff(repos))
        self.assertEquals(diff,
            "diff --git a/binary b/binary\n"
            "old mode 100644\n"
            "new mode 100755\n")

        cs = repos.createChangeSet([ ('foo:run',  t4.getNameVersionFlavor()[1:],
                                      t5.getNameVersionFlavor()[1:], False) ])
        diff = "".join(x for x in cs.gitDiff(repos))
        self.assertEquals(diff,
            "diff --git a/text b/text\n"
            "deleted file mode 100644\n"
            "Binary files /text and /dev/null differ\n"
            "diff --git a/binary b/binary\n"
            "deleted file mode 100755\n"
            "Binary files /binary and /dev/null differ\n"
            "diff --git a/pipe b/pipe\n"
            "new user root\n"
            "new group root\n"
            "new mode 10755\n")

        cs = repos.createChangeSet([ ('foo:run',  (None, None),
                                      t5.getNameVersionFlavor()[1:], False) ])
        diff = "".join(x for x in cs.gitDiff(repos))
        self.assertEquals(diff,
            "diff --git a/etc/config b/etc/config\n"
            "new user root\n"
            "new group root\n"
            "new mode 100644\n"
            "--- a/dev/null\n"
            "+++ b/etc/config\n"
            "@@ -1,0 +1,1 @@\n"
            "+changed config\n"
            "diff --git a/pipe b/pipe\n"
            "new user root\n"
            "new group root\n"
            "new mode 10755\n")

        cs = repos.createChangeSet([ ('foo:run', t2.getNameVersionFlavor()[1:],
                                      t21.getNameVersionFlavor()[1:], False) ])
        diff = "".join(x for x in cs.gitDiff(repos))
        self.assertEquals(diff,
            "diff --git a/binary b/binary\n"
            "GIT binary patch\n"
            "literal 5\n"
            "Mc$_OONz=6e00rU!wEzGB\n"
            "\n")

        cs = repos.createChangeSet([ ('foo:run', t21.getNameVersionFlavor()[1:],
                                      t2.getNameVersionFlavor()[1:], False) ])
        diff = "".join(x for x in cs.gitDiff(repos))
        self.assertEquals(diff,
            "diff --git a/binary b/binary\n"
            "GIT binary patch\n"
            "literal 1\n"
            "Ic${kh004mifdBvi\n"
            "\n")
