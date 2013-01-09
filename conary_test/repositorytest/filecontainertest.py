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
import gzip
import tempfile
import unittest

from conary.lib import util
from conary.repository import filecontainer
from conary.repository.filecontainer import FileContainer
from conary.repository.filecontents import FromFile, FromString


def fileCount():
    # this adds one because os.listdir has an open directory
    l = os.listdir("/proc/%d/fd" % os.getpid())
    return len(l) - 1

def checkFiles(c, names, data, tags):
    names = names[:]
    
    i = 0
    rc = c.getNextFile()
    while rc:
        name, tag, f = rc
        assert(name == names[0])
        del names[0]
        s = gzip.GzipFile(None, "r", fileobj = f).read()
        if s != data[i]:
            raise AssertionError, "bad data for %s" % names[i]
        if tag != tags[i]:
            raise AssertionError, "bad tag for %s" % names[i]

        i += 1
        rc = c.getNextFile()

    if names:
        raise AssertionError, "files not found: %s" % " ".join(names)

class FilecontainerTest(unittest.TestCase):
    def setUp(self):
        fd, self.fn = tempfile.mkstemp()
        #self.fn = "foo"

    def test(self):
        count = fileCount()

        # let's make sure we can't open an arbitrary file as a container
        f = util.ExtendedFile("/bin/ls", "r", buffering = False)

        self.assertRaises(filecontainer.BadContainer, FileContainer, f)

        f.close()
        if (count != fileCount()):
            raise AssertionError, "too many files are open %s" % count

        # create a new container
        f = util.ExtendedFile(self.fn, "w+", buffering = False)
        c = FileContainer(f)
        c.close()

        data = []
        tags = []
        names = []
        c = FileContainer(f)

        self.assertRaises(AssertionError, c.addFile, "name", 
                          FromString("data"), "tag")

        c.close()
        os.unlink(self.fn)
        f = util.ExtendedFile(self.fn, "w+", buffering = False)
        c = FileContainer(f)

        data.append("contents of file1")
        tags.append("extra data")
        names.append("file1")
        c.addFile(names[0], FromString(data[0]), tags[0])

        data.append("file2 gets some contents")
        tags.append("tag")
        names.append("file2")
        c.addFile(names[1], FromString(data[1]), tags[1])

        data.append("")
        tags.append("empty")
        names.append("")
        c.addFile(names[2], FromString(data[2]), tags[2])

        c.close()

        c = FileContainer(f)
        checkFiles(c, names, data, tags)

        f = util.ExtendedFile(self.fn, "r+", buffering = False)
        c = FileContainer(f)
        checkFiles(c, names, data, tags)
        c.reset()
        checkFiles(c, names, data, tags)
        c.close()

        f = util.ExtendedFile(self.fn, "r+", buffering = False)
        c = FileContainer(f)
        name, tag, f = c.getNextFile()
        assert(name == names[0])

    def testLargeFiles(self):
        # test adding files > 4gig to a filecontainer. we replace the write
        # call with one which handles sparsity
        class SparseFile(util.ExtendedFile):

            def __init__(self, *args, **kwargs):
                self.needsWrite = False
                util.ExtendedFile.__init__(self, *args, **kwargs)

            def write(self, s):
                if len(s) > 100 and s[0] == '\0' and s[-1] == '\0':
                    self.seek(len(s) - 1, 2)
                    self.needsWrite = True
                    return len(s)

                return util.ExtendedFile.write(self, s)

            def close(self):
                if self.needsWrite:
                    self.write('\0')
                    self.needsWrite = False

            def seek(self, *args):
                if self.needsWrite:
                    self.write('\0')
                    self.needsWrite = False

                return util.ExtendedFile.seek(self, *args)

        class FalseFile:

            def __init__(self, size):
                self.size = size
                self.offset = 0

            def seek(self, offset, whence = 0):
                assert(whence == 0)
                self.offset = offset

            def read(self, bytes):
                self.offset += bytes
                if self.offset > self.size:
                    self.offset -= bytes
                    bytes = self.size - self.offset
                    self.offset = self.size

                return "\0" * bytes

        f = SparseFile(self.fn, "w+", buffering = False)
        c = FileContainer(f)
        totalSize = 0x100001000
        c.addFile('test', FromFile(FalseFile(totalSize)), 'testdata',
                  precompressed = True)
        c.addFile('end', FromString('endcontents'), 'enddata',
                  precompressed = True)
        c.close()

        c = FileContainer(util.ExtendedFile(self.fn, 'r', buffering = False))
        name, tag, f = c.getNextFile()
        storedSize = f.seek(0, 2)
        assert(storedSize == totalSize)
        assert(tag == 'testdata')

        name, tag, f = c.getNextFile()
        assert(name == 'end')
        assert(tag == 'enddata')
        s = f.read()
        assert(s == 'endcontents')

    def tearDown(self):
        os.unlink(self.fn)
