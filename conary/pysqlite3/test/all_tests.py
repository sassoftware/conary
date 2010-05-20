#!/usr/bin/env python
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
