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


import os, tempfile, unittest
from conary.repository.filecontents import FromFile


class FileContentsTest(unittest.TestCase):

    def testFile(self):
        (fd, name) = tempfile.mkstemp()
        os.close(fd)

        try:
            f = open(name, "w")
            f.write("hello")
            f.close()
            f = open(name, "r")
            fc = FromFile(f)

            assert(fc.get().read() == "hello")
            assert(fc.get().read() == "hello")
        finally:
            os.unlink(name)
