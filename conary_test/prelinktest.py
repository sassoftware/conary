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


import grp
import os
import pwd

import conary_test
from conary import files
from conary.cmds import verify
from conary.lib import digestlib
from conary.local import database
from conary_test import rephelp
from conary_test import resources


class PrelinkTest(rephelp.RepositoryHelper):

    @conary_test.rpm
    def testPrelink(self):
        user = pwd.getpwuid(os.getuid()).pw_name
        group = grp.getgrgid(os.getgid()).gr_name
        archivePath = resources.get_archive()
        self.addComponent('test:foo=1', fileContents =
            [ ( '/prelinktest', rephelp.RegularFile(
                 contents = open(archivePath + '/prelinktest'),
                 owner = user, group = group, mode = 0755) ),
              ( '/prelinktest-orig', rephelp.RegularFile(
                 contents = open(archivePath + '/prelinktest'),
                 owner = user, group = group, mode = 0755) ) ] )
        self.updatePkg('test:foo=1')

        db = database.Database(self.rootDir, self.cfg.dbPath)
        rc, str = self.captureOutput(verify.verify, ['test:foo'], db, self.cfg) 
        self.assertEquals(str, '')

        binary = self.rootDir +'/prelinktest'
        # the test suite can't set the mtime on the file; we'll preserve
        # it ourself
        sb = os.stat(binary)
        os.system("cp %s/prelinktest-prelinked %s" % (archivePath, binary))
        os.utime(binary, (sb.st_atime, sb.st_mtime))

        self.assertEquals(files.PRELINK_CMD, ('/usr/sbin/prelink', ))
        oldCmd = files.PRELINK_CMD
        try:
            files.PRELINK_CMD = (archivePath + '/prelink', )
            files._havePrelink = None
            rc, str = self.captureOutput(verify.verify, ['test:foo'], db,
                                         self.cfg, forceHashCheck = True)
            self.assertEquals(str, '')

            # Also verify a path used by addCapsule
            f, nlinks, devino = files.FileFromFilesystem(binary,
                    pathId='\0' * 16, inodeInfo=True)
            self.assertEquals(digestlib.sha1(open(binary).read()).hexdigest(),
                    '1114f3a978b60d76d7618dc43aaf207bc999f997')
            self.assertEquals(f.contents.sha1().encode('hex'),
                    '23ad3a2c940a30809b68a5b8a13392196004efab')
        finally:
            files.PRELINK_CMD = oldCmd
            files._havePrelink = None
