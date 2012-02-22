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


import errno
import os
import gzip
from conary_test import rephelp

from conary.lib import util, sha1helper
from conary.lib.ext import file_utils
from conary.lib.ext import digest_uncompress

class MiscTest(rephelp.RepositoryHelper):

    def testMkdirIfMissing(self):
        umask = os.umask(022)
        os.umask(umask)

        assert(not util.exists(self.workDir + '/newdir'))
        file_utils.mkdirIfMissing(self.workDir + '/newdir')
        assert((os.stat(self.workDir + '/newdir').st_mode & 0777)==
                (0777 & ~umask))
        assert(util.exists(self.workDir + '/newdir'))
        file_utils.mkdirIfMissing(self.workDir + '/newdir')

        try:
            file_utils.mkdirIfMissing(self.workDir + '/first/second')
        except OSError, e:
            assert(e.errno == errno.ENOENT)
        else:
            raise AssertionError, "mkdir should fail"

        self.writeFile(self.workDir + '/dirent', '')
        file_utils.mkdirIfMissing(self.workDir + '/dirent')

    def _testSha1CopyAndUncompress(self, offset):
        infd = -1
        outfd = -1
        try:
            # set up some constants
            teststr = ' ' * 1000
            path = self.workDir + '/testfile'
            # open a sparse file and seek out to the requested offset
            f = open(path, 'w')
            f.seek(offset)
            # write a gzip file containing the test string
            gz = util.BoundedStringIO()
            compressor = gzip.GzipFile(None, "w", fileobj = gz)
            compressor.write(teststr)
            compressor.close()
            gz.seek(0)
            s = gz.read()
            f.write(s)
            f.close()
            # open using unbuffered io
            infd = os.open(path, os.O_RDONLY)
            outfd = os.open(path + '-copy', os.O_CREAT | os.O_WRONLY)
            # copy from the large sparse file to the output file,
            # decompressing the data and returning a sha1 of the uncompressed
            # contents
            sha = digest_uncompress.sha1Copy((infd, offset, len(s)), [outfd])
            # also decompress to a target file, while performing a sha1sum
            # of the uncompressed contents
            target = path + '-uncompressed'
            sha2 = digest_uncompress.sha1Uncompress((infd, offset, len(s)),
                                       os.path.dirname(target),
                                       os.path.basename(target),
                                       target)
            # make sure the sha matches what we expect
            expected = sha1helper.sha1String(teststr)
            self.assertEqual(sha, expected)
            self.assertEqual(sha2, expected)
            # make sure that the copied file matches the gzip compressed
            # string
            f = open(path + '-copy')
            self.assertEqual(f.read(), s)
            # and that it also is correctly uncompressed
            f = open(path + '-uncompressed')
            self.assertEqual(f.read(), teststr)
        finally:
            if infd > 0:
                os.close(infd)
            if outfd > 0:
                os.close(outfd)
            file_utils.removeIfExists(path)
            file_utils.removeIfExists(path + '-copy')

    def testSha1CopyAndUncompress(self):
        # CNY-3065
        self._testSha1CopyAndUncompress(0)
        self._testSha1CopyAndUncompress((2 * 1024 * 1024 * 1024) - 1)
        self._testSha1CopyAndUncompress(2 * 1024 * 1024 * 1024)
        self._testSha1CopyAndUncompress(2 * 1024 * 1024 * 1024 + 1)
        self._testSha1CopyAndUncompress(4 * 1024 * 1024 * 1024 + 1)
