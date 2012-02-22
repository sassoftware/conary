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


import os

import gzip
from StringIO import StringIO

from conary_test import rephelp

from conary import errors, files, trove, versions
from conary.deps import deps
from conary.lib import sha1helper, util
from conary.repository import changeset, filecontainer, filecontents, netclient


class ChangesetTest(rephelp.RepositoryHelper):
    def testBadChangeset(self):
        csFile = self.workDir + '/foo.ccs'
        try:
            changeset.ChangeSetFromFile(csFile)
        except errors.ConaryError, err:
            assert(str(err) == "Error opening changeset '%s': No such file or directory" % csFile)
        else:
            assert(0)

        open(csFile, 'w').close()
        os.chmod(csFile, 0000)
        try:
            changeset.ChangeSetFromFile(csFile)
        except errors.ConaryError, err:
            assert(str(err) == "Error opening changeset '%s': Permission denied" % csFile)
        else:
            assert(0)
        os.chmod(csFile, 0666)


    def testChangeSetFromFile(self):
        # ensure that absolute changesets that are read from disk
        # that contain config files write out changesets to a file
        # that do not change the file type to a diff.
                # set up a file with some contents
        cont = self.workDir + '/contents'
        f = open(cont, 'w')
        f.write('hello, world!\n')
        f.close()
        pathId = sha1helper.md5FromString('0' * 32)
        f = files.FileFromFilesystem(cont, pathId)
        f.flags.isConfig(1)

        # create an absolute changeset
        cs = changeset.ChangeSet()

        # add a pkg diff
        v = versions.VersionFromString('/localhost@rpl:devel/1.0-1-1',
                                       timeStamps = [1.000])
        flavor = deps.parseFlavor('')
        t = trove.Trove('test', v, flavor, None)
        t.addFile(pathId, '/contents', v, f.fileId())
        diff = t.diff(None, absolute = 1)[0]
        cs.newTrove(diff)

        # add the file and file contents
        cs.addFile(None, f.fileId(), f.freeze())
        cs.addFileContents(pathId, f.fileId(), changeset.ChangedFileTypes.file,
                           filecontents.FromFilesystem(cont),
                           f.flags.isConfig())

        # write out the changeset
        cs.writeToFile(self.workDir + '/foo.ccs')
        # read it back in
        cs2 = changeset.ChangeSetFromFile(self.workDir + '/foo.ccs')
        # write it out again (there was a bug where all config files
        # became diffs)
        cs2.writeToFile(self.workDir + '/bar.ccs')
        # read it again
        cs3 = changeset.ChangeSetFromFile(self.workDir + '/bar.ccs')
        # verify that the file is a file, not a diff
        ctype, contents = cs3.getFileContents(pathId, f.fileId())
        assert(ctype == changeset.ChangedFileTypes.file)

    def testIndexByPathIdConversion(self):
        def _testCs(repos, troves, idxLength, fileCount):
            job = [ (x.getName(), (None, None),
                     (x.getVersion(), x.getFlavor() ), True) for x in troves ]
            repos.createChangeSetFile(job, self.workDir + '/foo.ccs')
            fc = filecontainer.FileContainer(
                        util.ExtendedFile(self.workDir + '/foo.ccs', "r",
                                          buffering = False))

            info = fc.getNextFile()
            assert(info[0] == 'CONARYCHANGESET')

            info = fc.getNextFile()
            while info is not None:
                assert(len(info[0]) == idxLength)
                fileCount -= 1

                if 'ptr' in info[1]:
                    s = info[2].read()
                    s = gzip.GzipFile(None, "r", fileobj = StringIO(s)).read()
                    assert(len(s) == idxLength)

                info = fc.getNextFile()

            assert(fileCount == 0)

        f1 = rephelp.RegularFile(pathId = '1', contents = '1')
        f2 = rephelp.RegularFile(pathId = '1', contents = '2')

        t1 = self.addComponent('foo:runtime', fileContents = [ ( '/1', f1 ) ] )
        t2 = self.addComponent('bar:runtime', fileContents = [ ( '/2', f2 ) ] )

        repos = self.openRepository()

        _testCs(repos, [ t1 ], 36, 1)
        _testCs(repos, [ t1, t2 ], 36, 2)

        repos.c['localhost'].setProtocolVersion(41)
        _testCs(repos, [ t1 ], 16, 1)
        self.assertRaises(changeset.PathIdsConflictError,
                          _testCs, repos, [ t1, t2 ], 16, 1)

        # now test PTR types to make sure they get converted
        self.resetRepository()
        repos = self.openRepository()
        f1 = rephelp.RegularFile(pathId = '1', contents = '1')
        f2 = rephelp.RegularFile(pathId = '2', contents = '1')

        t1 = self.addComponent('foo:runtime', 
                    fileContents = [ ( '/1', f1 ), ( '/2', f2) ] )
        _testCs(repos, [ t1 ], 36, 2)

        repos.c['localhost'].setProtocolVersion(41)
        _testCs(repos, [ t1 ], 16, 2)

        # make sure we can install old-format changesets with PTRs
        self.updatePkg([ 'foo:runtime' ])
        self.verifyFile(self.rootDir + '/1', '1')
        self.verifyFile(self.rootDir + '/2', '1')

    def testGetNativeChangesetVersion(self):
        # When adding things here, make sure you update netclient's
        # FILE_CONTAINER_* constants too
        self.assertEqual(changeset.getNativeChangesetVersion(37),
                             filecontainer.FILE_CONTAINER_VERSION_NO_REMOVES)
        self.assertEqual(changeset.getNativeChangesetVersion(38),
                             filecontainer.FILE_CONTAINER_VERSION_WITH_REMOVES)
        self.assertEqual(changeset.getNativeChangesetVersion(42),
                             filecontainer.FILE_CONTAINER_VERSION_WITH_REMOVES)
        self.assertEqual(changeset.getNativeChangesetVersion(43),
                             filecontainer.FILE_CONTAINER_VERSION_FILEID_IDX)
        current = netclient.CLIENT_VERSIONS[-1]
        self.assertEqual(changeset.getNativeChangesetVersion(current),
                             filecontainer.FILE_CONTAINER_VERSION_FILEID_IDX)

    def testDictAsCsf(self):
        self.mock(changeset.DictAsCsf, 'maxMemSize', 256)
        def testOne(s):
            # test compression of large files for CNY-1896
            d = changeset.DictAsCsf(
                         { 'id' : ( changeset.ChangedFileTypes.file,
                                    filecontents.FromString(s), False ) } )
            f = d.getNextFile()[2]
            gzf = gzip.GzipFile('', 'r', fileobj = f)
            assert(gzf.read() == s)
            return f

        # this doesn't need to open any files
        fobj = testOne('short contents')
        self.assertEqual(fobj.getBackendType(), 'memory')

        fobj = testOne('0123456789' * 20000)
        self.assertEqual(fobj.getBackendType(), 'file')

    def testChangeSetMerge(self):
        os.chdir(self.workDir)

        cs1 = changeset.ChangeSet()
        p1 = '0' * 16; f1 = '0' * 20
        cs1.addFileContents(p1, f1, changeset.ChangedFileTypes.file,
                            filecontents.FromString('zero'), False)
        assert(cs1.writeToFile('foo.ccs') == 129)

        cs2 = changeset.ReadOnlyChangeSet()
        cs2.merge(cs1)
        assert(cs2.writeToFile('foo.ccs') == 129)
        cs2.reset()
        assert(cs2.writeToFile('foo.ccs') == 129)
        cs2.reset()

        cs3 = changeset.ReadOnlyChangeSet()
        cs3.merge(cs2)
        assert(cs3.writeToFile('foo.ccs') == 129)
        cs3.reset()
        assert(cs3.writeToFile('foo.ccs') == 129)

    def testChangeSetFilter(self):
        def addFirst():
            return self.addComponent('first:run')

        def addSecond():
            return self.addComponent('second:run')

        def job(trv):
            return (trv.getName(), (None, None),
                    trv.getNameVersionFlavor()[1:], True)

        first = addFirst()
        second = addSecond()

        repos = self.openRepository()
        cs = repos.createChangeSet([ job(first), job(second) ])

        self.resetRepository()
        repos = self.openRepository()

        addFirst()
        cs.removeCommitted(repos)
        repos.commitChangeSet(cs)

        cs = repos.createChangeSet([ job(first), job(second) ])
