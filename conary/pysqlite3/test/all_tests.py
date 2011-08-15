#!/usr/bin/env python
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


"""
This combines all PySQLite test suites into one big one.
"""

import unittest, sys
import api_tests, logging_tests, lowlevel_tests, pgresultset_tests
import type_tests, userfunction_tests, transaction_tests, concurrency_tests

def suite():
    suite = unittest.TestSuite((lowlevel_tests.suite(), api_tests.suite(),
            type_tests.suite(), userfunction_tests.suite(),
            transaction_tests.suite(), pgresultset_tests.suite(),
            logging_tests.suite(), concurrency_tests.suite()))
    return suite

def main():
    runner = unittest.TextTestRunner()
    results = runner.run(suite())
    return len(results.errors) + len(results.failures)

if __name__ == "__main__":
    sys.exit(main())
