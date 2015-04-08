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
            self.assertEqual(m.contents['name'], pn)
            self.assertEqual(m.contents['version'], pv)
            self.assertEqual(m.contents['release'], pr)
            self.assertEqual(m.contents['arch'], pa)
            self.assertEqual(m.contents['epoch'], pe)
            self.assertEqual(m.contents['isSource'], isSource)
            self.assertEqual(m.contents['summary'], summary)
            self.assertEqual(m.contents['description'], description)
            self.assertEqual(m.contents['license'], license)

    def testCarArchive(self):
        # for now, just make sure we don't treat them as zip
        m = magic.magic(os.path.join(resources.get_archive(), 'Transmitter.car'))
        if sys.version_info[:2] == (2, 6):
            self.assertFalse(m is None)
        else:
            self.assertEqual(m, None)

    def testDeb(self):
        m = magic.magic(os.path.join(resources.get_archive(), 'bash.deb'))
        self.assertTrue(isinstance(m, magic.deb))

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
            self.assertEqual(m.__class__, expClass)
            if not metadata:
                continue
            displayName, description = metadata
            self.assertEqual(m.contents['displayName'], displayName)
            if description is not None:
                self.assertEqual(m.contents['description'], description)

    def testJavaDepsInnerClasses(self):
        # APPENG-3334
        # Make sure anonymous inner classes and private named inner classes
        # are not exposed as dependencies
        # The sources are in tv.cny.vincent.jar - there are various levels of
        # inner classes with different access levels
        def _m(fpath):
            return magic.magic(os.path.join(resources.get_archive(), fpath))
        mobj = _m('tv.cny.vincent.jar')
        self.assertEquals(sorted(mobj.contents['requires']), [
            'cny.tv.vincent.ConsumerInnerClass',
            'cny.tv.vincent.ConsumerNestedLevel1',
            'cny.tv.vincent.ConsumerNestedLevel11',
            'cny.tv.vincent.ConsumerNestedLevel12',
            'cny.tv.vincent.ConsumerNestedLevel2',
            'cny.tv.vincent.ConsumerNestedLevel21',
            'cny.tv.vincent.ConsumerNestedLevel22',
            'cny.tv.vincent.ConsumerPublicInnerClass',
            'cny.tv.vincent.ConsumerPublicStaticInnerClass',
            'cny.tv.vincent.ConsumerStaticInnerClass',
            'cny.tv.vincent.InnerClasses',
            'cny.tv.vincent.InnerClasses$InnerClass',
            'cny.tv.vincent.InnerClasses$Nested1',
            'cny.tv.vincent.InnerClasses$Nested1$Nested11',
            'cny.tv.vincent.InnerClasses$Nested1$Nested12',
            'cny.tv.vincent.InnerClasses$Nested2',
            'cny.tv.vincent.InnerClasses$Nested2$Nested21',
            'cny.tv.vincent.InnerClasses$Nested2$Nested22',
            'cny.tv.vincent.InnerClasses$PublicInnerClass',
            'cny.tv.vincent.InnerClasses$PublicStaticInnerClass',
            'cny.tv.vincent.InnerClasses$StaticInnerClass',
            'java.lang.Object',
            ])
        self.assertEquals(sorted(mobj.contents['provides']), [
            'cny.tv.vincent.ConsumerInnerClass',
            'cny.tv.vincent.ConsumerNestedLevel1',
            'cny.tv.vincent.ConsumerNestedLevel11',
            'cny.tv.vincent.ConsumerNestedLevel12',
            'cny.tv.vincent.ConsumerNestedLevel2',
            'cny.tv.vincent.ConsumerNestedLevel21',
            'cny.tv.vincent.ConsumerNestedLevel22',
            'cny.tv.vincent.ConsumerPublicInnerClass',
            'cny.tv.vincent.ConsumerPublicStaticInnerClass',
            'cny.tv.vincent.ConsumerStaticInnerClass',
            'cny.tv.vincent.InnerClasses',
            'cny.tv.vincent.InnerClasses$InnerClass',
            'cny.tv.vincent.InnerClasses$Nested1',
            'cny.tv.vincent.InnerClasses$Nested1$Nested11',
            'cny.tv.vincent.InnerClasses$Nested1$Nested12',
            'cny.tv.vincent.InnerClasses$Nested2',
            'cny.tv.vincent.InnerClasses$Nested2$Nested21',
            'cny.tv.vincent.InnerClasses$Nested2$Nested22',
            'cny.tv.vincent.InnerClasses$PublicInnerClass',
            'cny.tv.vincent.InnerClasses$PublicStaticInnerClass',
            'cny.tv.vincent.InnerClasses$StaticInnerClass',
            ])
        self.assertEquals(
                sorted((x, y[0], sorted(y[1])) for (x, y) in mobj.contents['files'].items()), [
            ('cny/tv/vincent/ConsumerInnerClass.class',
                'cny.tv.vincent.ConsumerInnerClass',
                ['cny.tv.vincent.ConsumerInnerClass',
                    'cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$InnerClass',
                    'java.lang.Object']),
            ('cny/tv/vincent/ConsumerNestedLevel1.class',
                'cny.tv.vincent.ConsumerNestedLevel1',
                ['cny.tv.vincent.ConsumerNestedLevel1',
                    'cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$Nested1',
                    'java.lang.Object']),
            ('cny/tv/vincent/ConsumerNestedLevel11.class',
                'cny.tv.vincent.ConsumerNestedLevel11',
                ['cny.tv.vincent.ConsumerNestedLevel11',
                    'cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$Nested1',
                    'cny.tv.vincent.InnerClasses$Nested1$Nested11',
                    'java.lang.Object']),
            ('cny/tv/vincent/ConsumerNestedLevel12.class',
                'cny.tv.vincent.ConsumerNestedLevel12',
                ['cny.tv.vincent.ConsumerNestedLevel12',
                    'cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$Nested1',
                    'cny.tv.vincent.InnerClasses$Nested1$Nested12',
                    'java.lang.Object']),
            ('cny/tv/vincent/ConsumerNestedLevel2.class',
                'cny.tv.vincent.ConsumerNestedLevel2',
                ['cny.tv.vincent.ConsumerNestedLevel2',
                    'cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$Nested2',
                    'java.lang.Object']),
            ('cny/tv/vincent/ConsumerNestedLevel21.class',
                'cny.tv.vincent.ConsumerNestedLevel21',
                ['cny.tv.vincent.ConsumerNestedLevel21',
                    'cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$Nested2',
                    'cny.tv.vincent.InnerClasses$Nested2$Nested21',
                    'java.lang.Object']),
            ('cny/tv/vincent/ConsumerNestedLevel22.class',
                'cny.tv.vincent.ConsumerNestedLevel22',
                ['cny.tv.vincent.ConsumerNestedLevel22',
                    'cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$Nested2',
                    'cny.tv.vincent.InnerClasses$Nested2$Nested22',
                    'java.lang.Object']),
            ('cny/tv/vincent/ConsumerPublicInnerClass.class',
                'cny.tv.vincent.ConsumerPublicInnerClass',
                ['cny.tv.vincent.ConsumerPublicInnerClass',
                    'cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$PublicInnerClass',
                    'java.lang.Object']),
            ('cny/tv/vincent/ConsumerPublicStaticInnerClass.class',
                'cny.tv.vincent.ConsumerPublicStaticInnerClass',
                ['cny.tv.vincent.ConsumerPublicStaticInnerClass',
                    'cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$PublicStaticInnerClass',
                    'java.lang.Object']),
            ('cny/tv/vincent/ConsumerStaticInnerClass.class',
                'cny.tv.vincent.ConsumerStaticInnerClass',
                ['cny.tv.vincent.ConsumerStaticInnerClass',
                    'cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$StaticInnerClass',
                    'java.lang.Object']),
            ('cny/tv/vincent/InnerClasses$1.class',
                None,
                ['cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$InnerClass']),
            ('cny/tv/vincent/InnerClasses$2.class',
                None,
                ['cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$StaticInnerClass']),
            ('cny/tv/vincent/InnerClasses$InnerClass.class',
                'cny.tv.vincent.InnerClasses$InnerClass',
                ['cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$InnerClass',
                    'java.lang.Object']),
            ('cny/tv/vincent/InnerClasses$Nested1$Nested11.class',
                'cny.tv.vincent.InnerClasses$Nested1$Nested11',
                ['cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$Nested1',
                    'cny.tv.vincent.InnerClasses$Nested1$Nested11',
                    'java.lang.Object']),
            ('cny/tv/vincent/InnerClasses$Nested1$Nested12.class',
                'cny.tv.vincent.InnerClasses$Nested1$Nested12',
                ['cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$Nested1',
                    'cny.tv.vincent.InnerClasses$Nested1$Nested12',
                    'java.lang.Object']),
            ('cny/tv/vincent/InnerClasses$Nested1.class',
                'cny.tv.vincent.InnerClasses$Nested1',
                ['cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$Nested1',
                    'cny.tv.vincent.InnerClasses$Nested1$Nested11',
                    'cny.tv.vincent.InnerClasses$Nested1$Nested12',
                    'java.lang.Object']),
            ('cny/tv/vincent/InnerClasses$Nested2$Nested21.class',
                'cny.tv.vincent.InnerClasses$Nested2$Nested21',
                ['cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$Nested2',
                    'cny.tv.vincent.InnerClasses$Nested2$Nested21',
                    'java.lang.Object']),
            ('cny/tv/vincent/InnerClasses$Nested2$Nested22.class',
                'cny.tv.vincent.InnerClasses$Nested2$Nested22',
                ['cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$Nested2',
                    'cny.tv.vincent.InnerClasses$Nested2$Nested22',
                    'java.lang.Object']),
            ('cny/tv/vincent/InnerClasses$Nested2.class',
                'cny.tv.vincent.InnerClasses$Nested2',
                ['cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$Nested2',
                    'cny.tv.vincent.InnerClasses$Nested2$Nested21',
                    'cny.tv.vincent.InnerClasses$Nested2$Nested22',
                    'java.lang.Object']),
            ('cny/tv/vincent/InnerClasses$PrivateInnerClass$PrivateInnerNestedClassPrivate.class',
                None,
                ['cny.tv.vincent.InnerClasses', 'java.lang.Object']),
            ('cny/tv/vincent/InnerClasses$PrivateInnerClass$PrivateInnerNestedClassPublic.class',
                None,
                ['cny.tv.vincent.InnerClasses', 'java.lang.Object']),
            ('cny/tv/vincent/InnerClasses$PrivateInnerClass.class',
                None,
                ['cny.tv.vincent.InnerClasses', 'java.lang.Object']),
            ('cny/tv/vincent/InnerClasses$PrivateStaticInnerClass.class',
                None,
                ['cny.tv.vincent.InnerClasses', 'java.lang.Object']),
            ('cny/tv/vincent/InnerClasses$PublicInnerClass$1$1$NamedInnerClassInAnonymousClass.class',
                None,
                ['cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$PublicInnerClass',
                    'java.lang.Object']),
            ('cny/tv/vincent/InnerClasses$PublicInnerClass$1$1.class',
                None,
                ['cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$PublicInnerClass']),
            ('cny/tv/vincent/InnerClasses$PublicInnerClass$1.class',
                None,
                ['cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$PublicInnerClass']),
            ('cny/tv/vincent/InnerClasses$PublicInnerClass.class',
                'cny.tv.vincent.InnerClasses$PublicInnerClass',
                ['cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$PublicInnerClass',
                    'java.lang.Object']),
            ('cny/tv/vincent/InnerClasses$PublicStaticInnerClass.class',
                'cny.tv.vincent.InnerClasses$PublicStaticInnerClass',
                ['cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$PublicStaticInnerClass',
                    'java.lang.Object']),
            ('cny/tv/vincent/InnerClasses$StaticInnerClass.class',
                'cny.tv.vincent.InnerClasses$StaticInnerClass',
                ['cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$StaticInnerClass',
                    'java.lang.Object']),
            ('cny/tv/vincent/InnerClasses.class',
                'cny.tv.vincent.InnerClasses',
                ['cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$InnerClass',
                    'cny.tv.vincent.InnerClasses$Nested1',
                    'cny.tv.vincent.InnerClasses$Nested2',
                    'cny.tv.vincent.InnerClasses$PublicInnerClass',
                    'cny.tv.vincent.InnerClasses$PublicStaticInnerClass',
                    'cny.tv.vincent.InnerClasses$StaticInnerClass',
                    'java.lang.Object']),
                ])

        mobj = _m('tv.cny.jules.jar')
        self.assertEquals(sorted(mobj.contents['requires']), [
            'cny.tv.jules.Consumer', 'cny.tv.jules.ConsumerNestedLevel1',
            'cny.tv.jules.ConsumerNestedLevel2',
            'cny.tv.jules.ConsumerPublicInnerClass',
            'cny.tv.jules.ConsumerPublicStaticInnerClass',
            'cny.tv.vincent.InnerClasses',
            'cny.tv.vincent.InnerClasses$Nested2',
            'cny.tv.vincent.InnerClasses$Nested2$Nested22',
            'cny.tv.vincent.InnerClasses$PublicInnerClass',
            'cny.tv.vincent.InnerClasses$PublicStaticInnerClass',
            'java.lang.Object'])
        self.assertEquals(sorted(mobj.contents['provides']), [
            'cny.tv.jules.Consumer', 'cny.tv.jules.ConsumerNestedLevel1',
            'cny.tv.jules.ConsumerNestedLevel2',
            'cny.tv.jules.ConsumerPublicInnerClass',
            'cny.tv.jules.ConsumerPublicStaticInnerClass'])
        self.assertEquals(
                sorted((x, y[0], sorted(y[1])) for (x, y) in mobj.contents['files'].items()), [
            ('cny/tv/jules/Consumer.class',
                'cny.tv.jules.Consumer',
                ['cny.tv.jules.Consumer', 'java.lang.Object']),
            ('cny/tv/jules/ConsumerNestedLevel1.class',
                'cny.tv.jules.ConsumerNestedLevel1',
                ['cny.tv.jules.ConsumerNestedLevel1',
                    'cny.tv.vincent.InnerClasses',
                    'cny.tv.vincent.InnerClasses$Nested2',
                    'java.lang.Object']),
                ('cny/tv/jules/ConsumerNestedLevel2.class',
                    'cny.tv.jules.ConsumerNestedLevel2',
                    ['cny.tv.jules.ConsumerNestedLevel2',
                        'cny.tv.vincent.InnerClasses',
                        'cny.tv.vincent.InnerClasses$Nested2',
                        'cny.tv.vincent.InnerClasses$Nested2$Nested22',
                        'java.lang.Object']),
                ('cny/tv/jules/ConsumerPublicInnerClass.class',
                    'cny.tv.jules.ConsumerPublicInnerClass',
                    ['cny.tv.jules.ConsumerPublicInnerClass',
                        'cny.tv.vincent.InnerClasses',
                        'cny.tv.vincent.InnerClasses$PublicInnerClass',
                        'java.lang.Object']),
                ('cny/tv/jules/ConsumerPublicStaticInnerClass.class',
                    'cny.tv.jules.ConsumerPublicStaticInnerClass',
                    ['cny.tv.jules.ConsumerPublicStaticInnerClass',
                        'cny.tv.vincent.InnerClasses',
                        'cny.tv.vincent.InnerClasses$PublicStaticInnerClass',
                        'java.lang.Object']),
                ])

    def testEARErrors(self):
        # No zip file object
        e = self.assertRaises(ValueError, magic.EAR, '/dev/null')
        self.assertEqual(str(e), "Expected a Zip file object")
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
        self.assertFalse('displayName' in e.contents)
        self.assertFalse('description' in e.contents)
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

    def testShortGzipContents(self):
        # example short file from perl-PerlIO-gzip
        m = magic.magic(os.path.join(resources.get_archive(), 'ok50.gz.short'))

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

    def testLzoFile(self):
        m = magic.magic(resources.get_archive('foo.tar.lzo'))
        self.assertEquals(m.name, 'lzo')
