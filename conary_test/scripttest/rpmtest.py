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
from testrunner.decorators import requireBinary
from testutils import mock

import gzip
import os
import tempfile
import unittest

from conary import rpmhelper
from conary.lib import util
from conary_test import resources


class RpmTest(unittest.TestCase):
    def setUp(self):
        p = resources.get_path('scripts/rpm2cpio')
        if os.path.exists(p):
            self.rpm2cpio = p
        else:
            self.rpm2cpio = '/usr/bin/rpm2cpio'

    def testSeekToData(self):
        f = open(resources.get_archive() + "/basesystem-8.0-2.src.rpm")
        rpmhelper.seekToData(f)
        gz = gzip.GzipFile(fileobj=f)
        assert (gz.read(4) == "0707")
        gz.close()

        f = open("/bin/ls")
        self.assertRaises(IOError, rpmhelper.seekToData, f)

    @requireBinary("cpio")
    def testRpmToCpio(self):
        f = os.popen("%s %s/basesystem-8.0-2.src.rpm | cpio -t --quiet"
                     % (self.rpm2cpio, resources.get_archive()))
        assert(f.readlines() == [ 'basesystem.spec\n' ])

    @requireBinary("cpio")
    def testRpmToCpioBz2(self):
        f = os.popen("%s %s/initscripts-10-11.src.rpm | cpio -t --quiet"
                     % (self.rpm2cpio, resources.get_archive()))
        assert(f.readlines() == [ 'initscripts.spec\n',
                                  'initscripts.tar.gz\n' ])

    @requireBinary("unlzma")
    def testRpmToCpioLzma(self):
        # ensure that we are testing the older /usr/bin/unlzma path here
        # to make sure this still works on SLES10
        realExists = os.path.exists
        def access_lzma(*args):
            if args[0].endswith('/xz'):
                return False
            return realExists(*args)
        mock.mock(os.path, 'exists', access_lzma)
        f = os.popen("%s %s/gnome-main-menu-0.9.10-26.x86_64.rpm | cpio -t --quiet"
                     % (self.rpm2cpio, resources.get_archive()))
        l = f.readlines()
        assert(len(l)>0)
        self.failUnlessEqual(l[0], './etc/gconf/schemas/application-browser.schemas\n')
        mock.unmockAll()

    @requireBinary("xz")
    def testRpmToCpioXz(self):
        # ensure that we are testing the /usr/bin/xz path here
        realExists = os.path.exists
        def access_xz(*args):
            if args[0].endswith('/unlzma'):
                return False
            return realExists(*args)
        mock.mock(os.path, 'exists', access_xz)

        f = os.popen("%s %s/gnome-main-menu-0.9.10-26.x86_64.rpm | cpio -t --quiet"
                     % (self.rpm2cpio, resources.get_archive()))
        l = f.readlines()
        assert(len(l)>0)
        self.failUnlessEqual(l[0], './etc/gconf/schemas/application-browser.schemas\n')
        mock.unmockAll()

    def testCorruptedRpm(self):
        # test enforcement of size and sha1 fields from rpm signature block
        f = open(resources.get_archive() + "/basesystem-8.0-2.src.rpm")
        rpmhelper.readHeader(f)
        f.seek(0)

        # change the size
        tmp = tempfile.TemporaryFile()
        util.copyfileobj(f, tmp)
        f.seek(0)
        tmp.write(' ')
        tmp.seek(0)
        try:
            rpmhelper.readHeader(tmp)
        except IOError, e:
            assert(str(e) == 'file size does not match size specified by '
                             'header')
        else:
            assert(0)

        # change a byte in the header. the offset we write to here happens
        # to work for basesystem-8.0-2.src.rpm; if that file changes this
        # offset needs to change too
        tmp = tempfile.TemporaryFile()
        util.copyfileobj(f, tmp)
        tmp.seek(2000)
        tmp.write('X')
        tmp.seek(0)
        try:
            rpmhelper.readHeader(tmp)
        except IOError, e:
            assert(str(e) == "bad header sha1")
        else:
            assert(0)

    def testFilelessRpm(self):
        # Test that reading the paths for an rpm that has no files still works
        f = open(resources.get_archive() + "/fileless-0.1-1.noarch.rpm")
        h = rpmhelper.readHeader(f)
        self.failUnlessEqual(list(h.paths()), [])
        tags = [ rpmhelper.DIRINDEXES, rpmhelper.BASENAMES, rpmhelper.DIRNAMES,
                 rpmhelper.FILEUSERNAME, rpmhelper.FILEGROUPNAME,
                 rpmhelper.OLDFILENAMES, ]
        for t in tags:
            self.failUnlessEqual(h[t], [])

    def testGetDeps(self):
        f = open(resources.get_archive() + '/gnome-main-menu-0.9.10-26.x86_64.rpm')
        h = rpmhelper.readHeader(f)
        req, prov = h.getDeps()
        self.failUnlessEqual(req.freeze(), '3#/bin/sh|3#/sbin/ldconfig|3#/usr/bin/gconftool-2|16#coreutils|16#dbus-1-glib|16#eel|16#gnome-main-menu-lang|16#gnome-panel|16#hal|16#libICE.so.6[64bit]|16#libORBit-2.so.0[64bit]|16#libORBitCosNaming-2.so.0[64bit]|16#libSM.so.6[64bit]|16#libX11.so.6[64bit]|16#libXau.so.6[64bit]|16#libXrender.so.1[64bit]|16#libart_lgpl_2.so.2[64bit]|16#libasound.so.2[64bit]|16#libatk-1.0.so.0[64bit]|16#libaudiofile.so.0[64bit]|16#libavahi-client.so.3[64bit]|16#libavahi-common.so.3[64bit]|16#libavahi-glib.so.1[64bit]|16#libbonobo-2.so.0[64bit]|16#libbonobo-activation.so.4[64bit]|16#libbonoboui-2.so.0[64bit]|16#libc.so.6[64bit]:GLIBC_2.2.5:GLIBC_2.3.4:GLIBC_2.4|16#libcairo.so.2[64bit]|16#libcrypto.so.0.9.8[64bit]|16#libdbus-1.so.3[64bit]|16#libdbus-glib-1.so.2[64bit]|16#libdl.so.2[64bit]|16#libeel-2.so.2[64bit]|16#libesd.so.0[64bit]|16#libexpat.so.1[64bit]|16#libfontconfig.so.1[64bit]|16#libfreetype.so.6[64bit]|16#libgailutil.so.18[64bit]|16#libgconf-2.so.4[64bit]|16#libgdk-x11-2.0.so.0[64bit]|16#libgdk_pixbuf-2.0.so.0[64bit]|16#libgio-2.0.so.0[64bit]|16#libglade-2.0.so.0[64bit]|16#libglib-2.0.so.0[64bit]|16#libglitz.so.1[64bit]|16#libgmodule-2.0.so.0[64bit]|16#libgnome-2.so.0[64bit]|16#libgnome-desktop-2.so.2[64bit]|16#libgnome-keyring.so.0[64bit]|16#libgnome-menu.so.2[64bit]|16#libgnomecanvas-2.so.0[64bit]|16#libgnomeui-2.so.0[64bit]|16#libgnomevfs-2.so.0[64bit]|16#libgobject-2.0.so.0[64bit]|16#libgthread-2.0.so.0[64bit]|16#libgtk-x11-2.0.so.0[64bit]|16#libgtop-2.0.so.7[64bit]|16#libhal-storage.so.1[64bit]|16#libhal.so.1[64bit]|16#libiw.so.29[64bit]|16#libjpeg.so.62[64bit]|16#libm.so.6[64bit]|16#libnm-util.so.0[64bit]|16#libnm_glib.so.0[64bit]|16#libnsl.so.1[64bit]|16#libnspr4.so[64bit]|16#libnss3.so[64bit]|16#libnssutil3.so[64bit]|16#libpanel-applet-2.so.0[64bit]|16#libpango-1.0.so.0[64bit]|16#libpangocairo-1.0.so.0[64bit]|16#libpangoft2-1.0.so.0[64bit]|16#libpcre.so.0[64bit]|16#libplc4.so[64bit]|16#libplds4.so[64bit]|16#libpng12.so.0[64bit]|16#libpopt.so.0[64bit]|16#libpthread.so.0[64bit]:GLIBC_2.2.5|16#libresolv.so.2[64bit]|16#librsvg-2.so.2[64bit]|16#librt.so.1[64bit]|16#libslab.so.0[64bit]|16#libsmime3.so[64bit]|16#libssl.so.0.9.8[64bit]|16#libssl3.so[64bit]|16#libssui|16#libstartup-notification-1.so.0[64bit]|16#libutil.so.1[64bit]|16#libuuid.so.1[64bit]|16#libxcb-render-util.so.0[64bit]|16#libxcb-render.so.0[64bit]|16#libxcb-xlib.so.0[64bit]|16#libxcb.so.1[64bit]|16#libxml2.so.2[64bit]|16#libz.so.1[64bit]|16#tango-icon-theme|16#wireless-tools|17#CompressedFileNames|17#PayloadFilesHavePrefix|17#PayloadIsLzma')
        self.failUnlessEqual(prov.freeze(), '16#gnome-main-menu|16#libslab.so.0[64bit]')
        f = open(resources.get_archive() + '/popt-1.5-4x.i386.rpm')
        h = rpmhelper.readHeader(f)
        req, prov = h.getDeps()
        self.failUnlessEqual(req.freeze(), '')
        self.failUnlessEqual(prov.freeze(), '16#libpopt.so.0')

    def testRpmLibProvidesSet(self):
        try:
            import rpm
        except ImportError:
            raise testhelp.SkipTestException('rpm python module not installed')
        ds = rpmhelper.getRpmLibProvidesSet(rpm)
        # Note: hopefully these won't change in rpm all that much...
        # if it does, we'll need to do something different.

        dsStr = str(ds) + '\n'

        for flag in [ 'CompressedFileNames' , 'ExplicitPackageProvide',
                      'PartialHardlinkSets', 'VersionedDependencies' ]:
            assert(('rpmlib: %s\n' % flag) in dsStr)

    def testTestSuiteRpmLockOverride(self):
        try:
            import rpm
        except ImportError:
            raise testhelp.SkipTestException('rpm python module not installed')

        class simpleCallback:
            def __init__(self):
                self.fdnos = {}

            def callback(self, what, amount, total, mydata, wibble):
                if what == rpm.RPMCALLBACK_INST_OPEN_FILE:
                    hdr, path = mydata
                    fd = os.open(path, os.O_RDONLY)
                    nvr = '%s-%s-%s' % (hdr['name'],
                                        hdr['version'],
                                        hdr['release'])
                    self.fdnos[nvr] = fd
                    return fd

        def go(root):
            ts = rpm.TransactionSet(root)
            ts.setVSFlags(~(rpm.RPMVSF_NORSA|rpm.RPMVSF_NODSA))
            ts.initDB()
            rpmloc = resources.get_archive() + '/epoch-1.0-1.i386.rpm'
            fdno = os.open(rpmloc, os.O_RDONLY)
            hdr = ts.hdrFromFdno(fdno)
            os.close(fdno)
            ts.addInstall(hdr, (hdr, rpmloc), 'u')
            ts.check()
            ts.order()
            cb = simpleCallback()
            ts.run(cb.callback,'')
            ts.closeDB()
        d = tempfile.mkdtemp()
        os.chmod(d, 0777)
        try:
            go(d + '/root1')
            util.rmtree(d + '/root1')
            go(d + '/root2')
            self.failUnless(os.path.exists(d+'/root2/normal'))
        finally:
            util.rmtree(d)
