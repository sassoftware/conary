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


from testrunner import testhelp
from conary.deps import arch, deps


class ArchTest(testhelp.TestCase):
    def testFlavorPreferences(self):
        self.mock(arch, 'baseArch', 'i686')
        currentArch = arch.flags_i686()
        self.assertEqual(arch.getFlavorPreferences(currentArch),
            [])
        self.unmock()

        self.mock(arch, 'baseArch', 'x86_64')
        currentArch = arch.flags_x86_64()
        self.assertEqual(arch.getFlavorPreferences(currentArch),
            [ arch.deps.parseFlavor(x)
                for x in ['is: x86_64', 'is: x86'] ])
        self.unmock()

    def testCurrentArch(self):
        self.mock(arch, 'baseArch', 'x86_64')
        currentArch = arch.flags_x86_64()
        archKey = arch.FlavorPreferences._getCurrentArchIS(currentArch)
        self.assertEqual(archKey, 'x86 x86_64')

    def testGetMajorArch(self):
        deplist = deps.parseFlavor('is: x86 x86_64').iterDepsByClass(
            deps.InstructionSetDependency)
        self.assertEqual(arch.getMajorArch(deplist).name, 'x86_64')

        deplist = deps.parseFlavor('is: x86 ppc').iterDepsByClass(
            deps.InstructionSetDependency)
        self.assertRaises(arch.IncompatibleInstructionSets,
                              arch.getMajorArch, deplist)
