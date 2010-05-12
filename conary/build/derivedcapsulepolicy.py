#
# Copyright (c) 2010 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
import re, os

from conary import files, trove
from conary.build import destdirpolicy, filter, policy
from conary.build import packagepolicy, capsulepolicy, derivedpolicy
from conary.deps import deps

class ComponentSpec(capsulepolicy.ComponentSpec):
    processUnmodified = True

    def doProcess(self, recipe):
        # map paths into the correct components
        for trvCs in self.recipe.cs.iterNewTroveList():
            trv = trove.Trove(trvCs)

            if not trv.isCollection():
                regexs = [ re.escape(x[1]) for x in trv.iterFileList() ]
                f = filter.Filter(regexs, self.recipe.macros,
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



