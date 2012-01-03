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


from testrunner import testhelp

import gzip
import os
import StringIO

from conary import rpmhelper
from conary.lib import util
from conary_test import resources


class RPMHelperTest(testhelp.TestCase):
    def setUp(self):
        testhelp.TestCase.setUp(self)
        self.archiveDir = resources.get_archive()

    def testExtractRpmPayload(self):
        archives = [
            ('popt-1.5-4x.i386.rpm', 78252),
            ('rpm-with-bzip-5.0.29-1.i386.rpm', 248),
        ]
        # We cannot unpack xz-compressed contents with /usr/bin/lzma
        if os.path.exists('/usr/bin/xz'):
            archives.extend([
                ('popt-1.13-6.fc12.i686.rpm', 85544),
            ])
        for rpmName, expectedSize in archives:
            fileobj = file(os.path.join(self.archiveDir, rpmName))
            sio = StringIO.StringIO()
            rpmhelper.extractRpmPayload(fileobj, sio)
            sio.seek(0, 2)
            self.failUnlessEqual(sio.tell(), expectedSize)

        # Test exception codepaths
        header = {rpmhelper.PAYLOADFORMAT: "super-payload format"}
        self.mock(rpmhelper, "readHeader", lambda x: header)
        e = self.failUnlessRaises(rpmhelper.UnknownPayloadFormat,
            rpmhelper.extractRpmPayload, file("/dev/null"), None)
        self.failUnlessEqual(e.args[0], "super-payload format")

        # No payload format, it should assume CPIO. Test compression
        header = {rpmhelper.PAYLOADCOMPRESSOR: "lame"}
        e = self.failUnlessRaises(rpmhelper.UnknownCompressionType,
            rpmhelper.extractRpmPayload, file("/dev/null"), None)
        self.failUnlessEqual(e.args[0], "lame")

        # No payload compressor defined we should detect gzip or
        # assume uncompressed
        header = {rpmhelper.PAYLOADFORMAT: "cpio"}
        fileIn = StringIO.StringIO()
        fileIn.write('\x1f\x8bThis should be some gziped data')
        fileIn.seek(0)

        def gzipFunc(*args, **kw):
            return 'Gzip File'
        self.mock(util, "GzipFile", gzipFunc)
        self.failUnlessEqual(rpmhelper.UncompressedRpmPayload(fileIn),
                             'Gzip File')
        fileIn = StringIO.StringIO()
        fileIn.write('This is just some uncompressed data')
        fileIn.seek(0)
        self.failUnlessEqual(rpmhelper.UncompressedRpmPayload(fileIn),
                             fileIn)


    def testParseNEVRA(self):
        tests = [
            ("n-v-r.a.rpm", ("n", None, "v", "r", "a")),
            ("n-v-r.a", ("n", None, "v", "r", "a")),
            # Dash in name
            ("n-devel-v-r.a", ("n-devel", None, "v", "r", "a")),
            # Epoch is present
            ("n-0:v-r.a", ("n", 0, "v", "r", "a")),
            ("n-1:v-r.a", ("n", 1, "v", "r", "a")),
            # Arch with dash
            ("n-v-r.a-dash", ("n", None, "v", "r", "a-dash")),
            # Dot in name
            ("n.1-v-r.a", ("n.1", None, "v", "r", "a")),
            # Dot in version
            ("n.1-v.1-r.a", ("n.1", None, "v.1", "r", "a")),
        ]
        NEVRA = rpmhelper.NEVRA
        for filename, expected in tests:
            self.failUnlessEqual(NEVRA.parse(filename), expected)
            self.failUnlessEqual(NEVRA.parse(NEVRA.filename(*expected)),
                expected)

    def testRpmHeader(self):
        rpmName = 'tmpwatch-2.9.7-1.1.el5.2.x86_64.rpm'
        rpmPath = os.path.join(self.archiveDir, rpmName)
        fileObj = file(rpmPath)
        header = rpmhelper.readHeader(fileObj)
        # both NAME and SIG_SIZE are 1000, but their value should be different
        self.failUnlessEqual(header[rpmhelper.NAME], 'tmpwatch')
        self.failUnlessEqual(header[rpmhelper.SIG_SIZE][0], 18624)

    def testRpmDeps(self):
        rpmName = 'depstest-0.1-1.x86_64.rpm'
        rpmPath = os.path.join(self.archiveDir, rpmName)
        fileObj = file(rpmPath)
        header = rpmhelper.readHeader(fileObj)
        reqset, provset = header.getDeps()
        self.assertEquals(str(reqset), '\n'.join((
            'file: /bin/sh',
            'rpm: ld-linux.so.2(GLIBC_PRIVATE)',
            'rpm: libc.so.6(GLIBC_2.0 GLIBC_2.1.3)',
            'rpmlib: CompressedFileNames',
            'rpmlib: PayloadFilesHavePrefix')))
        self.assertEquals(str(provset), '\n'.join((
            'rpm: depstest',
            'rpm: depstest[x86-64]',
            'rpm: libm.so.6(GLIBC_2.0 GLIBC_2.1 GLIBC_2.2 GLIBC_2.4)')))

    def testRpmDepsPerl(self):
        rpmName = 'perl-Archive-Tar-1.46-68.fc11.x86_64.rpm'
        rpmPath = os.path.join(self.archiveDir, rpmName)
        fileObj = file(rpmPath)
        header = rpmhelper.readHeader(fileObj)
        reqset, provset = header.getDeps()
        self.assertEquals(str(reqset), '\n'.join((
            'file: /usr/bin/perl',
            'rpm: perl',
            'rpm: perl[Archive::Tar::Constant]',
            'rpm: perl[Archive::Tar::File]',
            'rpm: perl[Archive::Tar]',
            'rpm: perl[Carp]',
            'rpm: perl[Compress::Zlib]',
            'rpm: perl[Config]',
            'rpm: perl[Cwd]',
            'rpm: perl[Data::Dumper]',
            'rpm: perl[Exporter]',
            'rpm: perl[File::Basename]',
            'rpm: perl[File::Find]',
            'rpm: perl[File::Path]',
            'rpm: perl[File::Spec::Unix]',
            'rpm: perl[File::Spec]',
            'rpm: perl[Getopt::Std]',
            'rpm: perl[IO::File]',
            'rpm: perl[IO::Handle]',
            'rpm: perl[IO::Zlib]',
            'rpm: perl[Package::Constants]',
            'rpm: perl[constant]',
            'rpm: perl[strict]',
            'rpm: perl[vars]',
            'rpmlib: CompressedFileNames',
            'rpmlib: FileDigests',
            'rpmlib: PayloadFilesHavePrefix',
            'rpmlib: VersionedDependencies')))
        self.assertEquals(str(provset), '\n'.join((
            'rpm: perl-Archive-Tar',
            'rpm: perl-Archive-Tar[x86-64]',
            'rpm: perl[Archive::Tar::Constant]',
            'rpm: perl[Archive::Tar::File]',
            'rpm: perl[Archive::Tar]')))


    def testVerifySig(self):
        rpmName = 'tmpwatch-2.9.7-1.1.el5.2.x86_64.rpm'
        rpmPath = os.path.join(self.archiveDir, rpmName)
        fileObj = file(rpmPath)
        header = rpmhelper.readHeader(fileObj)
        from conary.lib import openpgpfile
        sig = openpgpfile.readSignature(header[rpmhelper.SIG_GPG])

        fileObj.seek(0)
        rpmhelper.readSignatureHeader(fileObj)
        k = openpgpfile.getKeyFromString('E8562897',
            openpgpfile.parseAsciiArmorKey(pgpKeyCentos))

        rpmhelper.verifySignatures(fileObj, [ k ])

        # Similar deal, fileObj is an ExtendedFile
        fileObj = util.ExtendedFile(rpmPath, buffering = False)
        rpmhelper.verifySignatures(fileObj, [ k ])

        # Finally, StringIO
        fileObj.seek(0)
        fileObj = StringIO.StringIO(fileObj.read())
        rpmhelper.verifySignatures(fileObj, [ k ])

        # Replace last byte
        fileObj = StringIO.StringIO(fileObj.getvalue()[:-1])
        fileObj.seek(0, 2)
        fileObj.write("\xff")
        fileObj.seek(0)
        e = self.failUnlessRaises(rpmhelper.MD5SignatureError,
            rpmhelper.verifySignatures, fileObj, [ k ])
        self.failUnlessEqual(str(e), 'The MD5 digest fails to verify: '
            'expected 6cc7c546c3a5de90bb272b11be2f3d67, got 744d88f4164ec2974b49839a69ea589d')

    def testExtractFilesFromCpio(self):
        rpmName = 'popt-1.5-4x.i386.rpm'
        fileobj = file(os.path.join(self.archiveDir, rpmName))
        payload = rpmhelper.UncompressedRpmPayload(fileobj)
        fileList = [
            './usr/include/popt.h',
            'usr/lib/libpopt.so.0',
            '/usr/lib/libpopt.so.0.0.0'
        ]
        ret = rpmhelper.extractFilesFromCpio(payload, fileList)
        self.failUnlessEqual( [ self._fileSize(x) for x in ret ],
            [ 5396, None, 20971 ])

    def testExtractFilesFromCpioHardlinks(self):
        payload = gzip.GzipFile(
            os.path.join(self.archiveDir, 'hardlinks.cpio.gz'))
        fileList = [
            '/b/regular-linked-b',
            'b/regular-linked-c',
            './b/empty',
            'a',
            './a/regular',
            'nosuchfile',
        ]
        ret = rpmhelper.extractFilesFromCpio(payload, fileList)
        self.failUnlessEqual( [ self._fileSize(x) for x in ret ],
            [ 2, 2, 0, None, 2, None ])
        self.failUnlessEqual( [ self._fileContents(x) for x in ret ],
            [ '2\n', '2\n', '', None, '1\n', None ])

    def testExtractRpmPayloadFdLeak(self):
        fdCount0 = util.countOpenFileDescriptors()
        self.testExtractRpmPayload()
        fdCount1 = util.countOpenFileDescriptors()
        self.failUnlessEqual(fdCount1 - fdCount0, 0)

    @classmethod
    def _fileSize(cls, fileObj):
        if fileObj is None:
            return None
        return os.fstat(fileObj.fileno()).st_size

    @classmethod
    def _fileContents(cls, fileObj):
        if fileObj is None:
            return None
        fileObj.seek(0)
        return fileObj.read()


pgpKeyCentos = """
-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: GnuPG v1.2.6 (GNU/Linux)

mQGiBEWfB6MRBACrnYW6yKMT+MwJlCIhoyTxGf3mAxmnAiDEy6HcYN8rivssVTJk
CFtQBlBOpLV/OW2YtKrCO2xHn46eNfnMri8FGT8g+9JF3MUVi7kiV1He4iJynHXB
+F2ZqIvHf3IaUj1ys+p8TK64FDFxDQDrGQfIsD/+pkSGx53/877IrvdwjwCguQcr
Ioip5TH0Fj0OLUY4asYVZH8EAIqFHEqsY+9ziP+2R3/FyxSllKkjwcMLrBug+cYO
LYDD6eQXE9Mq8XKGFDj9ZB/0+JzK/XQeStheeFG75q3noq5oCPVFO4czuKErIRAB
qKbDBhaTj3JhOgM12XsUYn+rI6NeMV2ZogoQCC2tWmDETfRpYp2moo53NuFWHbAy
XjETA/sHEeQT9huHzdi/lebNBj0L8nBGfLN1nSRP1GtvagBvkR4RZ6DTQyl0UzOJ
RA3ywWlrL9IV9mrpb1Fmn60l2jTMMCc7J6LacmPK906N+FcN/Docj1M4s/4CNanQ
NhzcFhAFtQL56SNyLTCk1XzhssGZ/jwGnNbU/aaj4wOj0Uef5LRGQ2VudE9TLTUg
S2V5IChDZW50T1MgNSBPZmZpY2lhbCBTaWduaW5nIEtleSkgPGNlbnRvcy01LWtl
eUBjZW50b3Mub3JnPohkBBMRAgAkBQJFnwekAhsDBQkSzAMABgsJCAcDAgMVAgMD
FgIBAh4BAheAAAoJEKikR9zoViiXKlEAmwSoZDvZo+WChcg3s/SpNoWCKhMAAJwI
E2aXpZVrpsQnInUQWwkdrTiL5YhMBBMRAgAMBQJFnwiSBYMSzAIRAAoJEDjCFhY5
bKCk0hAAn134bIx3wSbq58E6P6U5RT7Z2Zx4AJ9VxnVkoGHkVIgSdsxHUgRjo27N
F7kBDQRFnwezEAQA/HnJ5yiozwgtf6jt+kii8iua+WnjqBKomPHOQ8moxbWdv5Ks
4e1DPhzRqxhshjmub4SuJ93sgMSAF2ayC9t51mSJV33KfzPF2gIahcMqfABe/2hJ
aMzcQZHrGJCEX6ek8l8SFKou7vICzyajRSIK8gxWKBuQknP/9LKsoczV+xsAAwUD
/idXPkk4vRRHsCwc6I23fdI0ur52bzEqHiAIswNfO521YgLk2W1xyCLc2aYjc8Ni
nrMX1tCnEx0/gK7ICyJoWH1Vc7//79sWFtX2EaTO+Q07xjFX4E66WxJlCo9lOjos
Vk5qc7R+xzLDoLGFtbzaTRQFzf6yr7QTu+BebWLoPwNTiE8EGBECAA8FAkWfB7MC
GwwFCRLMAwAACgkQqKRH3OhWKJfvvACfbsF1WK193zM7vSc4uq51XsceLwgAoI0/
9GxdNhGQEAweSlQfhPa3yYXH
=o/Mx
-----END PGP PUBLIC KEY BLOCK-----
"""
