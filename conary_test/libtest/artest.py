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
