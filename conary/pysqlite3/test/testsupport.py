#!/usr/bin/env python
import _sqlite3
import os, tempfile, unittest

class TestSupport:
    def getfilename(self):
        if _sqlite3.sqlite_version_info() >= (2, 8, 2):
            return ":memory:"
        else:
            return tempfile.mktemp()

    def removefile(self):
        if self.filename != ":memory:":
            os.remove(self.filename)
