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
