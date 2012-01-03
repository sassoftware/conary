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
import bz2
import gzip
import sys
import tarfile
import zipfile

from conary_test import rephelp
from conary_test import resources

from conary.lib import magic, util


class MagicTest(rephelp.RepositoryHelper):
    def testRpmMagic(self):
        packages = [
            ('rpm-with-bzip-5.0.29-1.i386.rpm',
                ('rpm-with-bzip', None, '5.0.29', '1', 'i386', False,
                'RPM with a bzip payload', 'junk', 'GPL')),
            ('rpm-with-bzip-5.0.29-1.src.rpm',
                ('rpm-with-bzip', None, '5.0.29', '1', 'i386', True,
                'RPM with a bzip payload', 'junk', 'GPL')),
            ('fileless-0.1-1.noarch.rpm',
                ('fileless', None, '0.1', '1', 'noarch', False,
                'Foo', 'Foo', 'GPL')),
            ('fileless-0.1-1.src.rpm',
                ('fileless', None, '0.1', '1', 'noarch', True,
                'Foo', 'Foo', 'GPL')),
            ('tags-1.2-3.noarch.rpm',
                ('tags', 4, '1.2', '3', 'noarch', False,
                'Some Summary', 'Some Description', 'Some License')),
            ('tags-1.2-3.src.rpm',
                ('tags', 4, '1.2', '3', 'noarch', True,
                'Some Summary', 'Some Description', 'Some License')),
        ]
        for (pkg, data) in packages:
            pn, pe, pv, pr, pa, isSource, summary, description, license = data
            fpath = os.path.join(resources.get_archive(), pkg)
            m = magic.magic(fpath)
            self.failUnlessEqual(m.contents['name'], pn)
            self.failUnlessEqual(m.contents['version'], pv)
            self.failUnlessEqual(m.contents['release'], pr)
            self.failUnlessEqual(m.contents['arch'], pa)
            self.failUnlessEqual(m.contents['epoch'], pe)
            self.failUnlessEqual(m.contents['isSource'], isSource)
            self.failUnlessEqual(m.contents['summary'], summary)
            self.failUnlessEqual(m.contents['description'], description)
            self.failUnlessEqual(m.contents['license'], license)

    def testCarArchive(self):
        # for now, just make sure we don't treat them as zip
        m = magic.magic(os.path.join(resources.get_archive(), 'Transmitter.car'))
        if sys.version_info[:2] == (2, 6):
            self.failIf(m is None)
        else:
            self.failUnlessEqual(m, None)

    def testDeb(self):
        m = magic.magic(os.path.join(resources.get_archive(), 'bash.deb'))
        self.failUnless(isinstance(m, magic.deb))

    def testJavaMagic(self):
        def _p(fpath):
            return os.path.join(resources.get_archive(), fpath)
        # Copy the jar and the war with names that don't suggest the type with
        # the extension
        ojar = os.path.join(self.workDir, "aJarFile")
        util.copyfile(_p('servlet-api.jar'), ojar)
        oear = os.path.join(self.workDir, "aEarFile")
        util.copyfile(_p('stockgrant.ear'), oear)

        # the EAR file convenienty has a WAR file in it, so let's use it
        warPath = os.path.join(self.workDir, "warfile")
        z = zipfile.ZipFile(oear)
        file(warPath, "a").write(z.read("war-ic.war"))

        tests = [
            (_p('broken.jar'),      magic.jar, None),
            (_p('servlet-api.jar'), magic.jar, None),
            (_p('stockgrant.ear'),  magic.EAR,
                ('stockgrant3', 'Application description')),
            (_p('ownerships.zip'),  magic.ZIP, None),
            (ojar,                  magic.jar, None),
            (oear,                  magic.EAR,
                ('stockgrant3', 'Application description')),
            (warPath,               magic.WAR,
                ('WebApp1', None)),
        ]

        for fpath, expClass, metadata in tests:
            m = magic.magic(fpath)
            self.failUnlessEqual(m.__class__, expClass)
            if not metadata:
                continue
            displayName, description = metadata
            self.failUnlessEqual(m.contents['displayName'], displayName)
            if description is not None:
                self.failUnlessEqual(m.contents['description'], description)

    def testEARErrors(self):
        # No zip file object
        e = self.failUnlessRaises(ValueError, magic.EAR, '/dev/null')
        self.failUnlessEqual(str(e), "Expected a Zip file object")
        # Dummy ear file
        fdir = os.path.join(self.workDir, "somedir")
        metainfDir = os.path.join(fdir, "META-INF")
        util.mkdirChain(metainfDir)
        ddPath = os.path.join(metainfDir, "application.xml")
        file(ddPath, "a").write( "Some non-XML content")
        zipfilename = os.path.join(self.workDir, "some-fake-ear")
        z = zipfile.ZipFile(zipfilename, "w")
        ddRelPath = "META-INF/application.xml"
        z.write(ddPath, ddRelPath)
        z.close()

        z = zipfile.ZipFile(zipfilename)
        e = magic.EAR(zipfilename, '', z, set(ddRelPath))
        self.failIf('displayName' in e.contents)
        self.failIf('description' in e.contents)
        z.close()

    def testGzipFile(self):
        archivePath = os.path.join(self.workDir, 'archive.gz')
        g = gzip.GzipFile(archivePath, 'w')
        g.write('testing some random file that must be as least 262 bytes' \
                'long to test that the tar magic test in conary.lib.magic' \
                'will not mistakenly identify these file contents as a tar' \
                'archive. Originally there was a typo in the magic logic,' \
                'and anything that was longer than 262 bytes would trigger')
        g.close()
        m = magic.magic(archivePath)
        self.assertEquals(m.name, 'gzip')
        self.assertEquals(sorted(m.contents.keys()), ['compression', 'name'])

    def testGzipCompressedTarArchives(self):
        fooFile = open(os.path.join(self.workDir, 'foo'), 'w')
        fooFile.write('test file')

        archivePath = os.path.join(self.workDir, 'archive.tar.gz')
        g = gzip.GzipFile(archivePath, 'w')
        t = tarfile.TarFile(mode = 'w', fileobj = g)
        t.add(fooFile.name)
        t.close()
        g.close()
        m = magic.magic(archivePath)
        self.assertEquals(m.name, 'tar_gz')
        self.assertEquals(sorted(m.contents.keys()),
                ['GNU', 'compression', 'name'])
        # tarfile in python 2.4 creates tar archives without the complete GNU magic
        # POSIX = "ustar\0"
        # GNU   = "ustar  \0"
        isGnu = (sys.version_info[:2] == (2, 6))
        self.assertEquals(m.contents['GNU'], isGnu)

    def testGzipCompressedTarArchives3(self):
        fpath = os.path.join(resources.get_archive(), 'logrotate-3.7.1.tar.gz')
        m = magic.magic(fpath)
        self.assertEquals(m.name, 'tar_gz')
        self.assertEquals(sorted(m.contents.keys()), ['GNU', 'compression'])
        self.assertEquals(m.contents['GNU'], True)

    def testBadGzipContents(self):
        archivePath = os.path.join(self.workDir, 'busted.tar.gz')
        f = open(archivePath, 'w')
        # this is the magic signature of a gzip file
        f.write('\x1f\x8b')
        # and some garbage to fill out the file
        f.write('some random data')
        f.close()
        m = magic.magic(archivePath)

    def testGzipContentsBasedir(self):
        archivePath = os.path.join('nested_dir', 'busted.tar.gz')
        fullPath = os.path.join(self.workDir, archivePath)
        util.mkdirChain(os.path.dirname(fullPath))

        g = gzip.GzipFile(fullPath, 'w')
        g.write('some random data')
        g.close()
        m = magic.magic(archivePath, basedir = self.workDir)
        self.assertEquals(m.name, 'gzip')

    def testBzipFile(self):
        archivePath = os.path.join(self.workDir, 'archive.bz')
        b = bz2.BZ2File(archivePath, 'w')
        b.write('testing some random file')
        b.close()
        m = magic.magic(archivePath)
        self.assertEquals(m.name, 'bzip')
        self.assertEquals(sorted(m.contents.keys()), ['compression'])

    def testBzip2CompressedTarArchives(self):
        fooFile = open(os.path.join(self.workDir, 'foo'), 'w')
        fooFile.write('test file')

        archivePath = os.path.join(self.workDir, 'archive.tar.bz2')
        b = bz2.BZ2File(archivePath, 'w')
        t = tarfile.TarFile(mode = 'w', fileobj = b)
        t.add(fooFile.name)
        t.close()
        b.close()
        m = magic.magic(archivePath)
        self.assertEquals(m.name, 'tar_bz2')
        self.assertEquals(sorted(m.contents.keys()), ['GNU', 'compression'])

    def testBadBzipContents(self):
        archivePath = os.path.join(self.workDir, 'busted.tar.gz')
        f = open(archivePath, 'w')
        # this is the magic signature of a bzip file
        f.write('BZh')
        # and some garbage to fill out the file
        f.write('some random data')
        f.close()
        m = magic.magic(archivePath)

    def testBzipContentsBasedir(self):
        archivePath = os.path.join('nested_dir', 'busted.tar.gz')
        fullPath = os.path.join(self.workDir, archivePath)
        util.mkdirChain(os.path.dirname(fullPath))

        b = bz2.BZ2File(fullPath, 'w')
        b.write('some random data')
        b.close()
        m = magic.magic(archivePath, basedir = self.workDir)
        self.assertEquals(m.name, 'bzip')

    def testTarArchives(self):
        fooFile = open(os.path.join(self.workDir, 'foo'), 'w')
        fooFile.write('test file')

        archivePath = os.path.join(self.workDir, 'archive.tar')
        t = tarfile.TarFile(archivePath, mode = 'w')
        t.add(fooFile.name)
        t.close()
        m = magic.magic(archivePath)
        self.assertEquals(m.name, 'tar')
        self.assertEquals(m.contents.keys(), ['GNU'])

    def testXzFile(self):
        m = magic.magic(os.path.join(resources.get_archive(), 'foo.tar.xz'))
        self.assertEquals(m.name, 'xz')
