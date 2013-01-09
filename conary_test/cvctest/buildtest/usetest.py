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
from conary.build import use
from conary.deps import arch, deps
from conary.deps.deps import parseFlavor

class TestUse(rephelp.RepositoryHelper):

    def testErrors(self):
        try:
            bool(use.Use.foo)
        except use.NoSuchUseFlagError:
            pass
        else:
            raise RuntimeError
        use.Use._setStrictMode(False)
        assert(not bool(use.Use.foo))

    def testUseExtras(self):
        Use = use.Use
        Flags = use.LocalFlags
        Arch = use.Arch
        use.clearFlags()
        Arch._setArchProps('LE', 'BE', 'bits32', 'bits64')
        Arch._addFlag('x86', archProps={'LE' : True, 'BE' : False, 'bits32':True, 'bits64' : False } )
        Arch.x86._addFlag('sse')
        Arch.x86._addFlag('sse2', subsumes=['sse'])
        Arch.x86._addFlag('i686', subsumes=['i586', 'i486'])
        Arch.x86._addFlag('i586', subsumes=['i486'])
        Arch.x86._addFlag('i486')
        Arch.x86._addFlag('3dnow')
        Arch.x86._addAlias('3dnow', 'threednow')
        try:
            Arch.x86._addAlias('3dnow', 'threednow')
        except RuntimeError, e:
            assert(str(e) == 'alias is already set')
        else:
            raise
        try:
            Arch.x86._addAlias('3dnow', 'fourdnow')
        except RuntimeError, e:
            assert(str(e) == 'key 3dnow already has an alias')
        else:
            raise
        Arch._setArch('x86', ['i686', 'sse'])
        Use._addFlag('builddocs')
        Flags.smp = True
        assert(Flags.smp == True)
        assert(repr(Flags) == 'Flags: {smp: True}')
        f = use.createFlavor('kernel', [Arch.x86, Arch.x86.sse, Arch.x86.sse2, Use.builddocs, Flags.smp ])
        assert(f.freeze() == '1#x86:sse:~!sse2|5#use:~!builddocs:~kernel.smp') 
        use.Use._addFlag('bootstrap', required=False, value=True)
        assert(use.Use.bootstrap._toDependency().freeze() == '5#use:~bootstrap')

    def testArch(self):
        Arch = use.ArchCollection()
        Arch._setArchProps('LE', 'BE', 'bits32', 'bits64')
        Arch._addFlag('x86', archProps={'LE' : True, 'BE' : False, 'bits32':True, 'bits64' : False } )
        Arch.x86._addFlag('sse')
        Arch.x86._addFlag('sse2', subsumes=['sse'])
        Arch.x86._addFlag('i686', subsumes=['i586', 'i486'])
        Arch.x86._addFlag('i586', subsumes=['i486'])
        Arch.x86._addFlag('i486')
        Arch.x86._addFlag('3dnow')
        Arch.x86._addAlias('3dnow', 'threednow')
        Arch._addFlag('ppc', archProps={'LE' : False, 'BE' : True, 'bits32':False, 'bits64' : True } )
        Arch.ppc._addFlag('ppc64')
        Arch._setArch('ppc', ['ppc64'])
        assert(Arch.ppc and Arch.ppc.ppc64 and not Arch.LE and not Arch.bits32)
        assert(Arch.bits64)
        Arch._setArch('x86', ['i686'])
        assert(not Arch.ppc and not Arch.ppc.ppc64 and Arch.LE and Arch.bits32)
        assert(not Arch.bits64)
        Arch._trackUsed(True)
        bool(Arch.x86)
        bool(Arch.x86.sse2)
        bool(Arch.x86.i686)
        bool(Arch.getCurrentArch().i686)
        bool(Arch.x86.threednow)
        bool(Arch.ppc.ppc64)
        bool(Arch.BE)
        bool(Arch.LE)
        Arch._trackUsed(False)
        assert(str(use.createFlavor(None, Arch._iterUsed())) \
                   == 'is: x86(~!3dnow,i486,i586,i686,~!sse2)')
        Arch._setArch('x86', ['i586'])
        Arch._resetUsed()
        Arch._trackUsed(True)
        bool(Arch.x86.i586) == True
        f = use.createFlavor(None, Arch._iterUsed())
        assert(str(f) == 'is: x86(i486,i586)')
        f = str(use.createFlavor(None, Arch.x86._iterAll()))
        assert(f.find('is: LE') == -1)
        assert(f.find('3dnow') != -1)
        #now let's try turning required off
        Arch._setArch('x86', ['i586', '3dnow', 'sse'])
        Arch._resetUsed()
        Arch._trackUsed(True)
        assert(Arch.x86.i586 == True)
        assert(Arch.x86.threednow == True)
        assert(Arch.x86.sse == True)
        assert(str(Arch.ppc.ppc64) == 'Arch.ppc.ppc64: False')
        Arch.x86.i586.setRequired(False)
        Arch.x86.threednow.setRequired(False)
        f = use.createFlavor(None, Arch._iterUsed())
        assert(str(f) == 'is: x86(~3dnow,~i486,~i586,sse)')
        #touching a any arch or subarch should turn on the trove's
        #major architecture flag
        Arch._setArch('x86', ['i586', '3dnow', 'sse'])
        Arch._resetUsed()
        Arch._trackUsed(True)
        bool(Arch.ppc)
        f = use.createFlavor(None, Arch._iterUsed())
        assert(str(f) == 'is: x86')
        Arch._setArch('x86', ['i586', '3dnow', 'sse'])
        Arch._resetUsed()
        Arch._trackUsed(True)
        bool(Arch.ppc.ppc64)
        f = use.createFlavor(None, Arch._iterUsed())
        assert(str(f) == 'is: x86')


    def testTrack(self):
        Use = use.UseCollection()
        Use._addFlag('foo', value=True) 
        Use._addFlag('bar', value=False) 
        Use._addFlag('bam', value=False) 
        assert(Use._getUsed() == [])
        Use._trackUsed(True)
        assert(bool(Use.foo))
        assert(str(Use._getUsed()) == '[foo: True]')
        Use._trackUsed(False)
        bool(Use.bar)
        assert(str(Use._getUsed()) == '[foo: True]')
        Use._resetUsed()
        assert(Use._getUsed() == [])
        Use.foo._set(False)
        Use._trackUsed(True)
        assert(not bool(Use.foo))
        Use._getUsed()
        assert(str(Use._getUsed()) == '[foo: False]')
        Use._trackUsed(False)
        use.setUsed([Use.bar])
        Use._getUsed()
        assert(str(Use._getUsed()) == '[foo: False, bar: False]')
        Use._resetUsed()
        Use._trackUsed(True)
        Use.bar._set()
        assert(bool(Use.bar))
        Use._getUsed()
        assert(str(Use._getUsed()) == '[bar: True]')
        Use._resetUsed()
        Use.bar._set()
        Use.foo._set(False)
        Use._trackUsed(True)
        assert(Use.bar | Use.foo)
        assert(set(x._name for x in Use._iterUsed())  == set(['bar', 'foo']))
        Use._resetUsed()
        Use._trackUsed(True)
        assert(not Use.foo & Use.bar)
        assert(set(x._name for x in Use._iterUsed())  == set(['bar', 'foo']))
        Use._resetUsed()
        Use._trackUsed(True)
        assert(not (Use.foo & Use.bar & Use.bam))
        sorted(x._name for x in Use._iterUsed())
        assert(set(x._name for x in Use._iterUsed()) == \
                set(['bam', 'bar', 'foo']))
        Use._resetUsed()
        Use._trackUsed(True)
        assert(Use.foo | Use.bar | Use.bam)
        assert(set(x._name for x in Use._iterUsed()) == \
                set(['bam', 'bar', 'foo']))
        Use._resetUsed()
        Use._trackUsed(True)
        assert(not (Use.foo == True))
        assert([x._name for x in Use._iterUsed()] == ['foo'])
        Use._resetUsed()
        Use._trackUsed(True)
        assert(True != Use.foo)
        assert([x._name for x in Use._iterUsed()] == ['foo'])
        Arch = use.Arch
        use.resetUsed()
        use.track(True)
        assert(not bool(Arch.ppc.ppc64))
        use.track(False)
        assert(use.getUsed() == [Arch.getCurrentArch()])

    def testSetBuildFlagsFromFlavor(self):
        Flavor = deps.parseFlavor
        use.setBuildFlagsFromFlavor(None, Flavor('is: ppc'))
        assert(use.Arch.ppc)
        self.assertRaises(RuntimeError, use.setBuildFlagsFromFlavor,
            None, Flavor('is: x86 ppc'))
        use.setBuildFlagsFromFlavor(None, Flavor('is: x86'))
        assert(use.Arch.x86)
        assert(not use.Arch.ppc)
        self.assertRaises(RuntimeError, use.setBuildFlagsFromFlavor, None, Flavor('is: ppc x86_64'))
        self.assertRaises(AttributeError, use.setBuildFlagsFromFlavor,
                          None, 'fjdkf')
        self.logFilter.add()
        use.setBuildFlagsFromFlavor(None, Flavor('fjdkf,!ssl'), error=False,
                                     warn=True)
        self.logFilter.compare('warning: ignoring unknown Use flag fjdkf')
        assert(not use.Use.ssl)
        use.setBuildFlagsFromFlavor(None, Flavor('ssl'), error=False)

    def testUseFlagPaths(self):
        # Check to make sure that the _path instance variable indicates
        # which file defined the Use flag. (CNY-1179)
        Use = use.Use
        for name, value in Use.iteritems():
            self.assertTrue(value._path == '%s/%s' % (self.cfg.useDirs[0], name))

    def testPackageFlags(self):
        assert(not use.PackageFlags.kernel.pae)
        self.assertRaises(RuntimeError, setattr,
                          use.PackageFlags.kernel, 'pae', True)
        self.assertRaises(RuntimeError, setattr,
                          use.PackageFlags, 'foo', True)
        self.assertRaises(RuntimeError, bool, use.PackageFlags)

        assert(not use.PackageFlags.kernel.pae)
        use.setBuildFlagsFromFlavor(None, 
                                    parseFlavor('kernel.pae'), error=False)
        assert(bool(use.PackageFlags.kernel.pae))
        use.clearFlags()
        use.setBuildFlagsFromFlavor(None, 
                                    parseFlavor('!kernel.pae'), error=False)
        assert(not bool(use.PackageFlags.kernel.pae))
        assert(str(use.PackageFlags.kernel.pae) == 'PackageFlags.kernel.pae: False')

    def testTargetFlavor(self):
        use.setBuildFlagsFromFlavor(None, self.cfg.buildFlavor, error=False)
        targetFlavor = str(use.createFlavor(None, use.Arch._iterAll(), targetDep=True))
        assert(targetFlavor)
        flavor = str(use.createFlavor(None, use.Arch._iterAll(), targetDep=False))
        assert(targetFlavor.replace('target', 'is') == flavor)

    def testMultilib(self):
        depSet = deps.DependencySet()
        depSet.addDeps(deps.InstructionSetDependency, arch.flags_x86_64()[0])
        use.setBuildFlagsFromFlavor(None, depSet)
        self.assertTrue(use.Arch.x86_64)
        self.assertFalse(use.Arch.x86)
