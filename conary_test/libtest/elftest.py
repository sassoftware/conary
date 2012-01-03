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
import os
import shutil
import subprocess
import tempfile

from conary.lib import elf
from conary_test import resources


class ElfTest(testhelp.TestCase):
    def testGetRPATH(self):
        d = tempfile.mkdtemp()
        cwd = os.getcwd()
        try:
            os.chdir(d)
            # write a simple C program...
            f = open('foo.c', 'w')
            f.write('int main(void) { return 0; }\n')
            f.close()

            # test to make sure that setting a -rpath gives us the right
            # answer
            subprocess.call(('gcc', '-o', 'foo', 'foo.c', '-Wl,-rpath,/foo'))
            assert(elf.getRPATH('foo') == '/foo')
            # check multiple -rpath command line entries (this basically
            # tests to make sure the linker behaves as we expect)
            subprocess.call(('gcc', '-o', 'foo', 'foo.c', '-Wl,-rpath,/foo',
                             '-Wl,-rpath,/bar'))
            assert(elf.getRPATH('foo') == '/foo:/bar')
            # test to make sure that the new RUNPATH tag gets parsed correctly
            subprocess.call(('gcc', '-o', 'foo', 'foo.c', '-Wl,-rpath,/foo',
                             '-Wl,--enable-new-dtags'))
            assert(elf.getRPATH('foo') == '/foo')
            # empty rpath (makes sure that an empty string can be returned)
            subprocess.call(('gcc', '-o', 'foo', 'foo.c', '-Wl,-rpath,'))
            assert(elf.getRPATH('foo') == '')
            # test attempting to get an RPATH from an .a archive
            subprocess.call(('gcc', '-c', '-o', 'foo.o', 'foo.c'))
            null = open('/dev/null', 'w')
            p = subprocess.Popen(('ar', 'q', 'foo.a', 'foo.o'),
                                 stdout=null, stderr=null)
            os.waitpid(p.pid, 0)
            assert(elf.getRPATH('foo.a') == None)
            # test no rpath set
            subprocess.call(('gcc', '-o', 'foo', 'foo.c'))
            assert(elf.getRPATH('foo') == None)
        finally:
            os.chdir(cwd)
            shutil.rmtree(d)

    def testGetType(self):
        d = tempfile.mkdtemp()
        cwd = os.getcwd()
        try:
            os.chdir(d)
            # write a simple C program...
            f = open('foo.c', 'w')
            f.write('int main(void) { return 0; }\n')
            f.close()

            subprocess.call(('gcc', '-o', 'foo', 'foo.c'))
            assert(elf.getType('foo') == elf.ET_EXEC)

            subprocess.call(('gcc', '-o', 'foo', 'foo.c', '-fPIC', '-shared'))
            assert(elf.getType('foo') == elf.ET_DYN)

        finally:
            os.chdir(cwd)
            shutil.rmtree(d)

    def testGetDynSym(self):
        # Get the path to our lib directory
        libPath = os.path.dirname(elf.__file__)
        for soPrefix in ['elf', 'ext/streams', 'ext/file_utils']:
            fname = os.path.join(libPath, soPrefix + '.so')
            syms = elf.getDynSym(fname)
            initfunc = 'init' + os.path.basename(soPrefix)
            self.failUnless(initfunc in syms,
                            "%s not in %s" % (initfunc, syms))

        # Not a python module
        fname = os.path.join(libPath, 'filename_wrapper.so')
        if os.path.exists(fname):
            syms = elf.getDynSym(fname)
            self.failUnless('chdir' in syms)

        # Grab a random, non-.so file, expect an error
        fname = os.path.join(resources.get_archive(), 'basesystem-8.0-2.src.rpm')
        self.failUnlessRaises(elf.error, elf.getDynSym, fname)

    def testPrelink(self):
        archiveDir = resources.get_archive()
        assert(not elf.prelinked(archiveDir + '/prelinktest'))
        assert(    elf.prelinked(archiveDir + '/prelinktest-prelinked'))
        assert(not elf.prelinked(archiveDir + '/partial.patch'))
