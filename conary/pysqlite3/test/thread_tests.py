#!/usr/bin/env python
#
# Copyright (c) 2005 rPath, Inc.
#

import testsupport
import os, sys, tempfile, time, unittest, thread
import sqlite3 as sqlite

class ThreadTests(unittest.TestCase):
    def CheckTwoThreads(self):
        def reader(dbpath):
            reader = sqlite.connect(dbpath, timeout=2000)
            cu = reader.cursor()
            while True:
                l = [ x for x in cu.execute('SELECT * from foo') ]
                if l == [ None ]:
                    break
            thread.exit()

        fd, dbfile = tempfile.mkstemp()
        os.close(fd)
        writer = sqlite.connect(dbfile, timeout=500)
        cu = writer.cursor()
        cu.execute('CREATE TABLE foo(bar int)')
        writer.commit()
        thread.start_new_thread(reader, (dbfile,))
        for i in xrange(100):
            cu.execute('INSERT INTO foo values(?)', (i,))
            if i % 10 == 0:
                writer.commit()
            if i % 20 == 0:
                cu.execute('DELETE FROM foo')
                writer.commit()
        cu.execute('INSERT INTO foo VALUES(NULL)')
        writer.commit()

def suite():
    thread_suite = unittest.makeSuite(ThreadTests, "Check")
    test_suite = unittest.TestSuite((thread_suite,))
    return test_suite

def main():
    runner = unittest.TextTestRunner()
    runner.run(suite())

if __name__ == "__main__":
    main()
