#!/usr/bin/env python
#
# Copyright (c) 2004 rPath, Inc.
#
# Permission to use, copy, modify, and distribute this software and its
# documentation for any purpose and without fee is hereby granted, provided
# that the above copyright notice appear in all copies and that both that
# copyright notice and this permission notice appear in supporting
# documentation,
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.
#

import testsupport
import unittest
import sqlite

class PrecompiledTests(unittest.TestCase, testsupport.TestSupport):
    def setUp(self):
        self.filename = self.getfilename()
        self.cnx = sqlite.connect(self.filename)
        self.cur = self.cnx.cursor()
        self.cur.execute("CREATE TABLE TEST(FOO INTEGER)")
        self.cur.execute("INSERT INTO TEST(FOO) VALUES (%i)", (5,))
        self.cur.execute("INSERT INTO TEST(FOO) VALUES (%i)", (10,))

    def tearDown(self):
        try:
            self.cnx.close()
            self.removefile()
        except AttributeError:
            pass
        except sqlite.InterfaceError:
            pass

    def CheckPrecompiled(self):
        prec = self.cnx.prepare("SELECT * FROM TEST")
        row = prec.fetchone()
        if row != (5,):
            self.fail("expected (5,), got " + str(row))
        prec.reset()
        row = prec.fetchone()
        if row != (5,):
            self.fail("expected (5,), got " + str(row))
        row = prec.fetchone()
        if row != (10,):
            self.fail("expected (10,), got " + str(row))
            
    def CheckBind(self):
        prec = self.cnx.prepare("SELECT * FROM TEST WHERE FOO=?")
        prec.execute((10,))
        row = prec.fetchone()
        if row != (10,):
            self.fail("expected (10,), got " + str(row))
        
    def CheckMultipleStatements(self):
        try:
            prec = self.cnx.prepare("SELECT * FROM TEST; SELECT * FROM TEST")
        except Exception, e:
            if not isinstance(e, sqlite.ProgrammingError):
                self.fail("expected ProgrammingError, got " + e.__class__.__name__)
        else:
            self.fail("expected exception")

def suite():
    precompiled_suite = unittest.makeSuite(PrecompiledTests, "Check")
    return precompiled_suite

def main():
    runner = unittest.TextTestRunner()
    runner.run(suite())

if __name__ == "__main__":
    main()
