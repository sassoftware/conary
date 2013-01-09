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
import stat
import sys

import conary_test
from conary_test import rephelp


class RepairTest(rephelp.RepositoryHelper):

    @testhelp.context('repair')
    def testRegularFiles(self):
        self.addComponent('foo:run', fileContents = [
            ( '/f', rephelp.RegularFile(contents = 'orig\n', mode = 0600) ),
            ( '/c', rephelp.RegularFile(contents = 'ocfg\n', mode = 0660,
                                        config = True) ) ] )

        self.updatePkg('foo:run')
        for p in 'f', 'c':
            path = self.rootDir + '/' + p
            self.writeFile(path, "new contents\n")
            os.chmod(path, 0400)

        self.repairTroves([ 'foo:run' ])

        self.assertEquals(os.stat(self.rootDir + '/f').st_mode & 0777, 0600)
        self.assertEquals(os.stat(self.rootDir + '/c').st_mode & 0777, 0660)
        self.verifyFile(self.rootDir + '/f', 'orig\n')
        self.verifyFile(self.rootDir + '/c', 'ocfg\n')

        self.resetRoot()
        self.updatePkg('foo:run')
        os.unlink(self.rootDir + '/c')
        os.unlink(self.rootDir + '/f')
        rc, s = self.captureOutput(self.repairTroves, [ 'foo:run' ])
        self.assertEquals(s, '')
        self.verifyFile(self.rootDir + '/f', 'orig\n')
        self.verifyFile(self.rootDir + '/c', 'ocfg\n')

    @testhelp.context('repair')
    def testFileTypeChange(self):
        self.addComponent('foo:run=1', fileContents = [
            ( '/f', rephelp.RegularFile(contents = 'orig\n', mode = 0600) ) ])
        self.addComponent('foo:run=2', fileContents = [
            ( '/f', rephelp.Symlink(target = '/targ') ) ])

        self.updatePkg('foo:run=1')
        os.unlink(self.rootDir + '/f')
        os.symlink('/', self.rootDir + '/f')
        rc, s = self.captureOutput(self.repairTroves, [ 'foo:run' ])
        self.assertEquals(s, '')
        self.verifyFile(self.rootDir + '/f', 'orig\n')

        self.updatePkg('foo:run=2')
        os.unlink(self.rootDir + '/f')
        self.writeFile(self.rootDir + '/f', 'new')
        rc, s = self.captureOutput(self.repairTroves, [ 'foo:run' ])
        self.assertEquals(s, '')
        self.assertEquals(os.readlink(self.rootDir + '/f'), '/targ')

    @testhelp.context('repair')
    def testMissingDirectory(self):
        self.addComponent('foo:run=1',
            fileContents = [ ('/dir', rephelp.Directory() ) ])
        self.updatePkg('foo:run')
        os.rmdir(self.rootDir + '/dir')
        rc, s = self.captureOutput(self.repairTroves, [ 'foo:run' ])
        self.assertEquals(s, '')
        sb = os.stat(self.rootDir + '/dir')
        assert(stat.S_ISDIR(sb.st_mode))

    @conary_test.rpm
    @testhelp.context('repair')
    def testRpmMissingFiles(self):
        if sys.version_info < (2, 6):
            raise testhelp.SkipTestException(
                    'RPM repair requires python 2.6 or later')

        cmp = self.addRPMComponent("simple:rpm=1.0", 'simple-1.0-1.i386.rpm')
        self.updatePkg('simple:rpm')
        os.unlink(self.rootDir + '/config')
        os.unlink(self.rootDir + '/normal')
        os.rmdir(self.rootDir + '/dir')

        rc, s = self.captureOutput(self.repairTroves, [ 'simple:rpm' ])
        self.assertEquals(s, '')

        self.verifyFile(self.rootDir + '/config', "config\n")
        self.verifyFile(self.rootDir + '/normal', "normal\n")
        assert(os.path.isdir(self.rootDir + '/dir'))

    @conary_test.rpm
    def testRepairGhostFile(self):
        if sys.version_info < (2, 6):
            raise testhelp.SkipTestException(
                    'RPM repair requires python 2.6 or later')
        self.addRPMComponent("ghost:rpm=1.0", 'ghost-1.0-1.i386.rpm')
        self.updatePkg('ghost:rpm', raiseError=True)
        rc, s = self.captureOutput(self.repairTroves, [ 'ghost:rpm' ])
        self.assertEquals(s, '')
        self.verifyFile(self.rootDir + '/foo/ghost', '')
