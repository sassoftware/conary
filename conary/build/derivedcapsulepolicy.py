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
