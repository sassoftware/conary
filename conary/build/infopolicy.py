#
# Copyright (c) 2007-2008 rPath, Inc.
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

from conary.build import packagepolicy, policy

class ComponentSpec(packagepolicy.ComponentSpec, policy.UserGroupBasePolicy):
    requires = (
        ('Config', policy.CONDITIONAL_PRIOR),
        ('PackageSpec', policy.REQUIRED_SUBSEQUENT),
    )

class PackageSpec(packagepolicy.PackageSpec, policy.UserGroupBasePolicy):
    pass

class ExcludeDirectories(packagepolicy.ExcludeDirectories,
        policy.UserGroupBasePolicy):
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('Ownership', policy.CONDITIONAL_PRIOR),
        ('MakeDevices', policy.CONDITIONAL_PRIOR),
    )

ProcessGroupInfoPackage = packagepolicy.ProcessGroupInfoPackage
ProcessUserInfoPackage = packagepolicy.ProcessUserInfoPackage

class reportErrors(packagepolicy.reportErrors, policy.UserGroupBasePolicy):
    pass
