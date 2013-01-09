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
