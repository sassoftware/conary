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

try:
    from conary.local import rpmcapsule as _rpmcapsule
    rpmcapsule = _rpmcapsule
except ImportError:
    rpmcapsule = None


class RpmCapsule(testhelp.TestCase):
    def test_checkReplaceManagedFiles(self):
        if rpmcapsule is None:
            raise testhelp.SkipTestException('rpm not installed')
        # CNY-3662 - code defensively

        class OldFlags(object):
            def __init__(self):
                # Make sure we convert to bool
                self.replaceManagedFiles = 'adfad'

        class NewFlags(object):
            def replaceManagedFiles(self, path):
                return (path == "managed")

        meth = rpmcapsule.RpmCapsuleOperation._checkReplaceManagedFiles
        self.failUnlessEqual(meth(OldFlags(), 'aaa'), True)
        self.failUnlessEqual(meth(NewFlags(), 'managed'), True)
        self.failUnlessEqual(meth(NewFlags(), 'unmanaged'), False)
