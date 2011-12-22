#!/usr/bin/python
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
