#!/usr/bin/python
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


import sys
from testrunner import suite

from conary_test import resources


class Suite(suite.TestSuite):
    testsuite_module = sys.modules[__name__]
    topLevelStrip = 0

    def setupPaths(self):
        # turn off rpm locking via a DSO override. We have to
        # keep a reference to the handle or else dlclose() will be
        # called on it. Yes, this is ugly, but for some reason RPM
        # has a global variable for the location of the lock file
        # that only gets filled in the first time you ask for the rpm
        # database lock. Thus you can't use more than one root directory
        # during any single execution of rpmlib code.
        assert 'rpm._rpm' not in sys.modules
        from conary_test import norpmlock
        norpmlock.open(resources.get_path('conary_test', '_norpmlock.so'))

    def getCoverageExclusions(self, handler, environ):
        return ['scripts/.*', 'epdb.py', 'stackutil.py']

    def getCoverageDirs(self, handler, environ):
        # TODO: policy
        return  [ resources.get_path('conary') ]

    def sortTests(self, tests):
        # Filter out e.g. conary.pysqlite3.test
        return [x for x in tests if x.startswith('conary_test')]


_s = Suite()
setup = _s.setup
main = _s.main

if __name__ == '__main__':
    _s.run()
