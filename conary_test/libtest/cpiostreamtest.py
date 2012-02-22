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


from testrunner import testcase
import os, shutil

from conary import rpmhelper
from conary.lib import cpiostream, digestlib
from conary_test import resources


class CpioStreamTest(testcase.TestCaseWithWorkDir):
    testDirName = 'conarytest-'

    def setUp(self):
        testcase.TestCaseWithWorkDir.setUp(self)
        self.archiveDir = resources.get_archive()

    def oldtest(self):
        cpioPath = self._createCpio()
        sha1sum = digestlib.sha1(file(cpioPath).read()).hexdigest()
        resultFilePath = os.path.join(self.workDir, 'result.cpio')
        # Use a variety of sizes, to try to come up with different chunking
        # solutions
        for bufferSize in [1001, 1003, 3001]:
            f = file(resultFilePath, "w")
            src = cpiostream.CpioStream(file(cpioPath))
            while 1:
                buf = src.read(bufferSize)
                if not buf:
                    break
                f.write(buf)
            f.close()
            nsha1sum = digestlib.sha1(file(resultFilePath).read()).hexdigest()
            self.assertEqual(nsha1sum, sha1sum)

    def testIterate(self):
        cpioPath = self._createCpio()
        src = cpiostream.CpioStream(file(cpioPath))
        fileNames = [ x.filename for x in src ]
        self.assertEqual(fileNames,
            [
                './usr/bin/ptar',
                './usr/bin/ptardiff',
                './usr/lib/perl5/5.10.0/Archive/Tar',
                './usr/lib/perl5/5.10.0/Archive/Tar.pm',
                './usr/lib/perl5/5.10.0/Archive/Tar/Constant.pm',
                './usr/lib/perl5/5.10.0/Archive/Tar/File.pm',
                './usr/share/man/man1/ptar.1.gz',
                './usr/share/man/man1/ptardiff.1.gz',
                './usr/share/man/man3/Archive::Tar.3pm.gz',
                './usr/share/man/man3/Archive::Tar::File.3pm.gz',
            ])

    def testIterateAndRead(self):
        cpioPath = self._createCpio()
        src = cpiostream.CpioStream(file(cpioPath))
        for ent in src:
            ent.payload.read()

    def testIterateAndReadAlternate(self):
        # We only read every other file - this tests that we are properly
        # rolling the cpio stream forward
        cpioPath = self._createCpio()
        src = cpiostream.CpioStream(file(cpioPath))
        for i, ent in enumerate(src):
            if i % 2 == 0:
                ent.payload.read()

    def testIterateAndPartialRead2(self):
        # Read only a portion of the payload
        cpioPath = self._createCpio()
        src = cpiostream.CpioStream(file(cpioPath))
        for i, ent in enumerate(src):
            if i % 2 == 0:
                amt = min(10, ent.header.filesize)
            else:
                amt = min(155, ent.header.filesize)
            ent.payload.read(amt)

    def testOutOfOrderRead(self):
        cpioPath = self._createCpio()
        src = cpiostream.CpioStream(file(cpioPath))
        ent = src.next()
        # Advance to the next entry, the first one should no longer be able to
        # read
        src.next()
        self.assertRaises(cpiostream.OutOfOrderRead, ent.payload.read)

    def testOutOfOrderRead2(self):
        cpioPath = self._createCpio()
        src = cpiostream.CpioStream(file(cpioPath))
        ent = src.next()
        # The cpio stream advances one byte. This should be enough to kill the
        # reads for the entry
        src.read(1)
        self.assertRaises(cpiostream.OutOfOrderRead, ent.payload.read)

    def _createCpio(self, rpmName = None):
        if rpmName is None:
            rpmName = 'perl-Archive-Tar-1.46-68.fc11.x86_64.rpm'
        cpioPath = os.path.join(self.workDir, 'archive.cpio')
        rpmFile = file(os.path.join(self.archiveDir, rpmName))
        rpmhelper.extractRpmPayload(rpmFile, file(cpioPath, "w"))
        return cpioPath

    def testExpansion(self):
        cpioPath = self._createCpio()
        target = self.workDir + '/root'
        expander = cpiostream.CpioExploder(file(cpioPath))
        expander.explode(target)
        sha1sum = digestlib.sha1(file(
            target + '/usr/lib/perl5/5.10.0/Archive/Tar.pm').read()).hexdigest()
        self.assertEquals(sha1sum, 'cbe78d8a0d26a86436e4fc56f8581ffd3db4bd83')

        shutil.rmtree(self.workDir)
        os.mkdir(self.workDir)

        cpioPath = self._createCpio(rpmName = 'simple-1.1-1.i386.rpm')
        expander = cpiostream.CpioExploder(file(cpioPath))
        expander.explode(target)
        assert(os.path.isdir(target + '/dir'))
        sha1sum = digestlib.sha1(file(target + '/normal').read()).hexdigest()
        self.assertEquals(sha1sum, '5662cdf7d378e7505362c59239f73107b6edf1d3')
