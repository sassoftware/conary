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


from testrunner import testhelp
import os
import tempfile


from conary.lib import digestlib, util

class CopyTest(testhelp.TestCase):
    def testCopyFileObjDigest(self):
       tmpDir = tempfile.mkdtemp()
       try:
           buf = 'test data'

           # rpepare source and destination files
           srcFn = os.path.join(tmpDir, 'srcfile')
           destFn = os.path.join(tmpDir, 'destfile')
           open(srcFn, 'w').write(buf)
           src = open(srcFn)
           dest = open(destFn, 'w')

           # filter the digest through copyfileobj
           sha1 = digestlib.sha1()
           util.copyfileobj(src, dest, digest = sha1, sizeLimit = len(buf))
           res = sha1.hexdigest()

            # now compare the resulting hash to reference data
           sha1 = digestlib.sha1()
           sha1.update(buf)
           ref = sha1.hexdigest()
           self.assertEquals(ref, res)
       finally:
           util.rmtree(tmpDir)
