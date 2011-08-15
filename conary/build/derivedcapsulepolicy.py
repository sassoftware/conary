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


from conary import trove
from conary.build import filter, policy
from conary.build import capsulepolicy, derivedpolicy

class ComponentSpec(capsulepolicy.ComponentSpec):
    processUnmodified = True

    def doProcess(self, recipe):
        # map paths into the correct components
        for trvCs in self.recipe.cs.iterNewTroveList():
            trv = trove.Trove(trvCs)

            if not trv.isCollection():
                f = filter.PathSet((x[1] for x in trv.iterFileList()),
                                  name = trv.getName().split(':')[1])
                self.derivedFilters.append(f)
        capsulepolicy.ComponentSpec.doProcess(self, recipe)

PackageSpec = derivedpolicy.PackageSpec
Flavor = derivedpolicy.Flavor
Requires = derivedpolicy.Requires
Provides = derivedpolicy.Provides
ComponentRequires = derivedpolicy.ComponentRequires
ComponentProvides = derivedpolicy.ComponentRequires

ByDefault = derivedpolicy.ByDefault
TagSpec = derivedpolicy.TagSpec

class CapsuleModifications(policy.Policy):
    """
    This policy is used to mark files which are modified in or added to a
    derived capsule component.
    Do not call it directly; it is for internal use only.
    """
    bucket = policy.PACKAGE_CREATION
    filetree = policy.PACKAGE
    processUnmodified = False

    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('ExcludeDirectories', policy.REQUIRED_PRIOR),
    )

    def doFile(self, filename):
        capPath = self.recipe._getCapsulePathsForFile(filename)
        for pkg in self.recipe.autopkg.findComponents(filename):
            f = pkg.getFile(filename)
            f.flags.isCapsuleOverride(True)
            if not capPath:
                f.flags.isCapsuleAddition(True)
            # CNY-3577
            f.flags.isEncapsulatedContent(False)
