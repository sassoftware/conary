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
from conary.deps import arch, deps


class ArchTest(testhelp.TestCase):
    def testFlavorPreferences(self):
        self.mock(arch, 'baseArch', 'i686')
        currentArch = arch.flags_i686()
        self.failUnlessEqual(arch.getFlavorPreferences(currentArch),
            [])
        self.unmock()

        self.mock(arch, 'baseArch', 'x86_64')
        currentArch = arch.flags_x86_64()
        self.failUnlessEqual(arch.getFlavorPreferences(currentArch),
            [ arch.deps.parseFlavor(x)
                for x in ['is: x86_64', 'is: x86'] ])
        self.unmock()

    def testCurrentArch(self):
        self.mock(arch, 'baseArch', 'x86_64')
        currentArch = arch.flags_x86_64()
        archKey = arch.FlavorPreferences._getCurrentArchIS(currentArch)
        self.failUnlessEqual(archKey, 'x86 x86_64')

    def testGetMajorArch(self):
        deplist = deps.parseFlavor('is: x86 x86_64').iterDepsByClass(
            deps.InstructionSetDependency)
        self.failUnlessEqual(arch.getMajorArch(deplist).name, 'x86_64')

        deplist = deps.parseFlavor('is: x86 ppc').iterDepsByClass(
            deps.InstructionSetDependency)
        self.failUnlessRaises(arch.IncompatibleInstructionSets,
                              arch.getMajorArch, deplist)
