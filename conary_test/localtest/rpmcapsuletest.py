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
        self.assertEqual(meth(OldFlags(), 'aaa'), True)
        self.assertEqual(meth(NewFlags(), 'managed'), True)
        self.assertEqual(meth(NewFlags(), 'unmanaged'), False)
