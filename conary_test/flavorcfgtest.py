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
import tempfile

from conary_test import rephelp

#conary
from conary import flavorcfg
from conary.build import use
from conary.conarycfg import ParseError
from conary.deps import deps


class FlavorCfgTest(rephelp.RepositoryHelper):
    def testArchFile(self):
        archfile = """
name x86
archProp LE True 
archProp BE False
archProp bits32 True
archProp bits64 False
unameArch i386-kernel
targetArch i386-target
optFlags i386 opts

[i486]
subsumes
unameArch i486-kernel
targetArch i486-target
optFlags i486 opts

[i586]
unameArch i586-kernel
targetArch i586-target
optFlags i586 opts
subsumes i486

[i686]
unameArch i686-kernel
targetArch i686-target
optFlags i686 opts
subsumes i486,i586

[3dnow]
buildName threednow

[3dnowext]
buildName threednowext
subsumes 3dnow
"""
        dir = tempfile.mkdtemp()
        cwd = os.getcwd()
        os.chdir(dir)
        f = open('x86', 'w')
        f.write(archfile)
        f.close()
        use.clearFlags()
        x86 = flavorcfg.ArchConfig('x86')
        x86.read('./x86')
        x86.addArchFlags()
        assert(not use.Arch.x86.i486._subsumes)
        assert(use.Arch.x86.i586._subsumes == ['i486'])
        assert(use.Arch.x86.i686._subsumes == ['i486', 'i586'])
        assert(use.Arch.x86._attrs['threednow'] is not None)
        use.Arch._setArch('x86', ['3dnow'])
        assert(use.Arch._getMacro('targetarch') == 'i386-target')
        assert(use.Arch._getMacro('unamearch') == 'i386-kernel')
        assert(use.Arch.LE and use.Arch.bits32 and not use.Arch.BE 
                and not use.Arch.bits64)
        assert(use.Arch._getMacro('optflags') == 'i386 opts')
        use.Arch._setArch('x86', ['3dnow', 'i486'])
        assert(use.Arch._getMacro('targetarch') == 'i486-target')
        assert(use.Arch._getMacro('unamearch') == 'i486-kernel')
        assert(use.Arch._getMacro('optflags') == 'i486 opts')
        use.Arch._setArch('x86', ['3dnow', 'i686'])
        assert(use.Arch._getMacro('targetarch') == 'i686-target')
        assert(use.Arch._getMacro('unamearch') == 'i686-kernel')
        assert(use.Arch._getMacro('optflags') == 'i686 opts')
        archfile2 = """
name foo
archProp LE True 
archProp BE False
archProp bits32 True
archProp bits64 False

[subarch]
"""
        f = open('foo', 'w')
        f.write(archfile2)
        f.close()
        foo = flavorcfg.ArchConfig('foo')
        foo.read('./foo')
        foo.addArchFlags()
        use.Arch._setArch('foo')
        assert(use.Arch._getMacro('targetarch') == 'foo')
        assert(use.Arch._getMacro('unamearch') == 'foo')
        try:
            use.Arch._getMacro('optflags')
            assert(False)
        except KeyError:
            pass
        use.Arch._setArch('foo', ['subarch'])
        assert(use.Arch._getMacro('targetarch') == 'foo')
        assert(use.Arch._getMacro('unamearch') == 'foo')
        try:
            assert(use.Arch._getMacro('optflags') == None)
            assert(False)
        except KeyError:
            pass


    def testUseFile(self):
        self.writeFile(self.workDir + '/4Suite', 
        """
name 4Suite
sense required
buildName fourSuite
buildRequired False
shortDoc hello world
longDoc hello world2
        """)
        u = flavorcfg.UseFlagConfig('4Suite')
        u.read(self.workDir + '/4Suite')
        assert(u.name == '4Suite')
        assert(u.buildName == 'fourSuite')
        assert(u.sense == deps.FLAG_SENSE_REQUIRED)
        assert(u.shortDoc == 'hello world')
        assert(u.longDoc == 'hello world2')
        use.clearFlags()
        u.addUseFlag()
        assert('4Suite' in use.Use)
        assert(use.Use.fourSuite._required == False)

        self.writeFile(self.workDir + '/4Suite', 
        """
name 4Suite
sense sixth
buildName fourSuite
buildRequired False
shortDoc hello world
longDoc hello world2
        """)
        try:
            u = flavorcfg.UseFlagConfig('4Suite')
            u.read(self.workDir + '/4Suite')
        except ParseError, e:
            assert(str(e) == "%s/4Suite:3: unknown use value 'sixth' for configuration item 'sense'" % self.workDir)
        else:
            raise

        self.writeFile(self.workDir + '/4Suite', 
        """
name 4Suite
sense preferred
buildName fourSuite
buildRequired Falsees
shortDoc hello world
longDoc hello world2
        """)
        try:
            u = flavorcfg.UseFlagConfig('4Suite')
            u.read(self.workDir + '/4Suite')
        except ParseError, e:
            assert(str(e) == "%s/4Suite:5: expected True or False "
                   "for configuration item 'buildRequired'" %self.workDir)
        else:
            raise
