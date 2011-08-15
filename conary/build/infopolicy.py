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
