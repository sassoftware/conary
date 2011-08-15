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


import testsupport
import StringIO, unittest
import sqlite3 as sqlite

class LogFileTemplate:
    def write(self, s):
        pass

class LogFile:
    def __init__(self):
        pass

def init_LogFile():
    LogFile.write = LogFileTemplate.write

class CommandLoggingTests(unittest.TestCase, testsupport.TestSupport):
    def tearDown(self):
        try:
            self.cnx.close()
            self.removefile()
        except AttributeError:
            pass
        except sqlite.InterfaceError:
            pass

    def CheckNoWrite(self):
        init_LogFile()
        del LogFile.write
        logger = LogFile()
        try:
            self.cnx = sqlite.connect(self.getfilename(),
                command_logfile=logger)

            self.fail("ValueError not raised")
        except ValueError:
            pass

    def CheckWriteNotCallable(self):
        logger = LogFile()
        logger.write = 5
        try:
            self.cnx = sqlite.connect(self.getfilename(),
                command_logfile=logger)

            self.fail("ValueError not raised")
        except ValueError:
            pass

    def CheckLoggingWorks(self):
        logger = StringIO.StringIO()

        expected_output = ";\n".join([
            sqlite.main._BEGIN, "CREATE TABLE TEST(FOO INTEGER)",
            "INSERT INTO TEST(FOO) VALUES (?)",
            "ROLLBACK"]) + ";\n"

        self.cnx = sqlite.connect(self.getfilename(),
            command_logfile=logger)
        cu = self.cnx.cursor()
        cu.execute("CREATE TABLE TEST(FOO INTEGER)")
        cu.execute("INSERT INTO TEST(FOO) VALUES (?)", (5,))
        self.cnx.rollback()

        logger.seek(0)
        real_output = logger.read()

        if expected_output != real_output:
            self.fail("Logging didn't produce expected output.")

def suite():
    command_logging_suite = unittest.makeSuite(CommandLoggingTests, "Check")
    return command_logging_suite

def main():
    runner = unittest.TextTestRunner()
    runner.run(suite())

if __name__ == "__main__":
    main()
