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

from conary.lib import ar

class ArTest(rephelp.RepositoryHelper):
    def getArchiveFile(self, path):
        return file('%s/%s' %(self.sourceSearchDir, path))

    def testDeb(self):
        a = ar.Archive(self.getArchiveFile('testempty.deb'))
        b = [x.name for x in a]
        self.assertEquals(b, ['debian-binary', 'ignore1', 'control.tar.gz', 'ignore2'])

        a = ar.Archive(self.getArchiveFile('test.deb'))
        b = [x.name for x in a]
        self.assertEquals(b, ['debian-binary', 'control.tar.bz2', 'data.tar.bz2', 'ignore1'])

        a = ar.Archive(self.getArchiveFile('bash.deb'))
        c = [x for x in a]
        b = [x.name for x in c]
        self.assertEquals(b, ['debian-binary', 'control.tar.gz', 'data.tar.gz'])
        magic = [x.data.read() for x in c if x.name == 'debian-binary'][0]
        self.assertEquals(magic, '2.0\n')

    def testArchiveRepr(self):
        timestamp = 1145659903
        import time
        timestr = time.ctime(timestamp)
        a = ar.Archive(self.getArchiveFile('bash.deb'))
        b = [repr(x) for x in a]
        self.assertEquals(b, [
         '<ArFile 0744 0:0          4 %s debian-binary>' % timestr,
         '<ArFile 0744 0:0       5555 %s control.tar.gz>' % timestr,
         '<ArFile 0744 0:0     757749 %s data.tar.gz>' % timestr])

    def testGNUArchiveLongNames(self):
        a = ar.Archive(self.getArchiveFile('sortedlen.a'))
        b = [x.name for x in a]
        self.assertEquals(b, ['short', 'notlong', 'thisisareallylongfilename', 'ANOTHERREALLYLONGFILENAME'])

        a = ar.Archive(self.getArchiveFile('mixedlen.a'))
        b = [x.name for x in a]
        self.assertEquals(b, ['short', 'thisisareallylongfilename', 'notlong', 'ANOTHERREALLYLONGFILENAME'])

    def testGNUArchiveSymbolTable(self):
        a = ar.Archive(self.getArchiveFile('unstripped_archive.a'))
        b = [x.name for x in a]
        self.assertEquals(b, ['/', 'unstripped_archive.o'])

    def testNotAnArArchive(self):
        a = ar.Archive(self.getArchiveFile('unstripped_archive.o'))
        def parseArchive():
            b = [x.name for x in a]
        self.assertRaises(ar.ArchiveError, parseArchive)

    def testTruncatedArchive(self):
        a = ar.Archive(self.getArchiveFile('testshortheader.a'))
        def parseArchive():
            b = [x.name for x in a]
        self.assertRaises(ar.ArchiveError, parseArchive)
