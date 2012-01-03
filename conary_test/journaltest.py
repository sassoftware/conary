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

import errno
import os

from conary_test import rephelp

from conary import conaryclient
from conary.lib import util
from conary.local import database

realUnlink = os.unlink
realRename = os.rename
realSha1Uncompress = util.sha1Uncompress
realLink = os.link
revertMsg = 'error: a critical error occured -- reverting filesystem changes'

class Counter:

    def adjust(self, amount):
        self.val += amount

    def __call__(self):
        return self.val

    def __init__(self, initialValue):
        self.val = initialValue

def protect(fn):
    def protection(*args, **kwargs):
        try:
            rv = fn(*args, **kwargs)
        finally:
            os.unlink = realUnlink
            os.rename = realRename
            os.link = realLink
            util.sha1Uncompress = realSha1Uncompress

        return rv

    protection.func_name = fn.func_name

    return protection

class JournalTest(rephelp.RepositoryHelper):

    @staticmethod
    def unlinkStub(path, failPaths):
        base = os.path.basename(path)
        if base in failPaths:
            raise OSError(errno.EACCES, 'Permission denied', path)
        else:
            return realUnlink(path)

    @staticmethod
    def exitOnUnlink(path, failPaths):
        base = os.path.basename(path)
        if base in failPaths:
            os._exit(0)

        return realUnlink(path)

    @staticmethod
    def linkStub(oldPath, newPath, failPaths):
        base = os.path.basename(newPath)
        if base in failPaths:
            raise OSError(errno.EACCES, 'Permission denied', newPath)
        else:
            return realLink(oldPath, newPath)

    @staticmethod
    def sha1UncompressStub(info, path, name, target, failPaths = [],
                           counter = None):
        base = os.path.basename(target)
        if base in failPaths:
            if counter:
                if not counter():
                    return realSha1Uncompress(info, path, name, target)
                                              
                counter.adjust(-1)

            raise OSError(errno.EACCES, 'Permission denied', path)
        else:
            return realSha1Uncompress(info, path, name, target)

    @staticmethod
    def renameStub(oldPath, newPath, failNewPaths, counter = None,
                  failAfter = False):
        base = os.path.basename(newPath)
        if base[0] == '.':
            # this rename is part of restoring the journal; let it go
            return realRename(oldPath, newPath)

        if base in failNewPaths:
            if counter:
                if not counter():
                    return realRename(oldPath, newPath)
                counter.adjust(-1)

            if failAfter:
                realRename(oldPath, newPath)

            raise OSError(errno.EACCES, 'Permission denied', newPath)
        else:
            return realRename(oldPath, newPath)

    @protect
    def testUnlink(self):
        self.addComponent('foo:runtime', '1.0-1-1',
            fileContents = [
                ( '/a', rephelp.RegularFile(contents = "a1", pathId = "1") ),
                ( '/b', rephelp.RegularFile(contents = "b1", pathId = "2",
                                            perms = 0644) )
            ]
        )

        self.addComponent('foo:runtime', '2.0-2-2',
            fileContents = [ ]
        )

        # files are removed in reverse path order, so /b is removed before
        # /a -- make sure it gets put back if removing /a fails
        self.updatePkg('foo:runtime=1.0-1-1')
        os.chmod(self.rootDir + '/b', 0600)
        os.unlink = lambda x: self.unlinkStub(x, [ 'a' ] )
        self.logCheck(self.assertRaises,
                  (OSError, self.updatePkg, 'foo:runtime=2.0-2-2'),
                  [ revertMsg,
                    'error: /a could not be removed: Permission denied' ])

        self.verifyFile(self.rootDir + '/b', 'b1')
        assert(os.stat(self.rootDir + '/b').st_mode & 0777 == 0600)

        assert(not os.path.exists(self.rootDir + self.cfg.dbPath + '/journal'))

        self.logCheck2([ revertMsg,
                         'error: /a could not be removed: Permission denied' ],
                       self.assertRaises,
                       OSError, self.updatePkg, 'foo:runtime=2.0-2-2',
                       keepJournal = True)

        assert(os.path.exists(self.rootDir + self.cfg.dbPath + '/journal'))

    @protect
    def testChange(self):
        self.addComponent('foo:runtime', '1.0-1-1',
            fileContents = [
                ( '/a', rephelp.RegularFile(contents = "a1", pathId = "1") ),
                ( '/b', rephelp.RegularFile(contents = "b1", pathId = "2") )
            ]
        )

        self.addComponent('foo:runtime', '2.0-2-2',
            fileContents = [
                ( '/a', rephelp.RegularFile(contents = "a2", pathId = "1") ),
                ( '/b', rephelp.RegularFile(contents = "b2", pathId = "2") )
            ]
        )

        self.updatePkg('foo:runtime=1.0-1-1')

        # sorted by pathIds, so /b happens last

        counter = Counter(1)
        util.sha1Uncompress = lambda *args: self.sha1UncompressStub(
                                                 failPaths = [ 'b' ],
                                                 *args)

        self.logCheck(self.assertRaises,
                      (OSError, self.updatePkg, 'foo:runtime=2.0-2-2'),
                      revertMsg)

        self.verifyFile(self.rootDir + '/a', 'a1')

    def testRevert(self):
        self.addComponent('foo:runtime', '1.0-1-1',
            fileContents = [
                ( '/a', rephelp.RegularFile(contents = "a1", pathId = "1") ),
            ]
        )

        self.updatePkg('foo:runtime', keepJournal = True)
        os.remove(self.cfg.root + '/var/lib/conarydb/syslock')

        # FIXME: uncomment when RAA-313 is implemented
        self.assertRaises(database.ExistingJournalError, self.erasePkg,
                          self.rootDir, 'foo:runtime')

        db = conaryclient.ConaryClient.revertJournal(self.cfg)
        self.logCheck(conaryclient.ConaryClient.revertJournal, (self.cfg,),
                "error: Unable to open database %s/var/lib/conarydb: "
                "journal error: No such file or directory" % self.cfg.root)

    @protect
    def testHardLinks(self):
        self.addComponent('foo:runtime', '1.0-1-1',
            fileContents = [
                ( '/a', rephelp.RegularFile(contents = "a1", pathId = "1",
                  linkGroup = "\0" * 16) ),
                ( '/b', rephelp.RegularFile(contents = "a1", pathId = "2",
                  linkGroup = "\0" * 16) ),
                ( '/c', rephelp.RegularFile(contents = "a1", pathId = "3",
                  linkGroup = "\0" * 16) )
            ]
        )

        self.addComponent('foo:runtime', '2.0-2-2',
            fileContents = [
                ( '/a', rephelp.RegularFile(contents = "a2", pathId = "1" ) ),
                ( '/b', rephelp.RegularFile(contents = "b2", pathId = "2",
                  linkGroup = "\1" * 16) ),
                ( '/c', rephelp.RegularFile(contents = "b2", pathId = "3",
                  linkGroup = "\1" * 16) ),
                ( '/d', rephelp.RegularFile(contents = "d2", pathId = "4") ),
            ]
        )

        self.updatePkg('foo:runtime=1.0-1-1')

        util.sha1Uncompress = lambda *args: self.sha1UncompressStub(
                                                 failPaths = [ 'd' ],
                                                 *args)
        self.logCheck(self.assertRaises,
                      (OSError, self.updatePkg, 'foo:runtime=2.0-2-2'),
                      revertMsg)

        assert(sorted(os.listdir(self.rootDir)) == [ 'a', 'b', 'c', 'var' ] )
        self.verifyFile(self.rootDir + '/a', "a1")
        assert(os.stat(self.rootDir + '/a').st_ino ==
               os.stat(self.rootDir + '/b').st_ino)
        assert(os.stat(self.rootDir + '/a').st_ino ==
               os.stat(self.rootDir + '/c').st_ino)

    @protect
    def testFailedHardLinks(self):
        # 1.0 -> 2.0 update tests adding a new file, where the target of the
        # link already exists on the system. we fail after the rename() to
        # the new link name occurs, testing that the new link name gets restored
        # to its old contents
        self.addComponent('foo:runtime', '1.0-1-1',
            fileContents = [
                ( '/a', rephelp.RegularFile(contents = "a1", pathId = "1",
                  linkGroup = "\0" * 16) ),
                ( '/b', rephelp.RegularFile(contents = "a1", pathId = "2",
                  linkGroup = "\0" * 16) ),
                ( '/c', rephelp.RegularFile(contents = "other", pathId = "3" ) )
            ]
        )

        self.addComponent('foo:runtime', '2.0-2-2',
            fileContents = [
                ( '/a', rephelp.RegularFile(contents = "a1", pathId = "1",
                  linkGroup = "\0" * 16) ),
                ( '/b', rephelp.RegularFile(contents = "a1", pathId = "2",
                  linkGroup = "\0" * 16) ),
                ( '/c', rephelp.RegularFile(contents = "a1", pathId = "3",
                  linkGroup = "\0" * 16) ),
            ]
        )

        self.updatePkg('foo:runtime=1.0-1-1')
        counter = Counter(1)
        os.rename = lambda x, y: self.renameStub(x, y, [ 'c' ],
                                                 failAfter = True,
                                                 counter = counter)
        self.logCheck(self.assertRaises,
                      (OSError, self.updatePkg, 'foo:runtime=2.0-2-2'),
                      revertMsg)

        self.verifyFile(self.rootDir + '/a', "a1")
        self.verifyFile(self.rootDir + '/b', "a1")
        self.verifyFile(self.rootDir + '/c', "other")

    @protect
    def testRename(self):
        self.addComponent('foo:runtime', '1.0-1-1',
            fileContents = [
                ( '/a', rephelp.RegularFile(contents = "a1", pathId = "1") ),
            ]
        )

        self.addComponent('foo:runtime', '2.0-2-2',
            fileContents = [
                ( '/anew', rephelp.RegularFile(contents = "a1", pathId = "1") ),
                ( '/b',    rephelp.RegularFile(contents = "b2", pathId = "2") ),
            ]
        )

        self.updatePkg('foo:runtime=1.0-1-1')

        util.sha1Uncompress = lambda *args: self.sha1UncompressStub(
                                                 failPaths = [ 'b' ],
                                                 *args)

        self.logCheck(self.assertRaises,
                      (OSError, self.updatePkg, 'foo:runtime=2.0-2-2'),
                      revertMsg)

        self.verifyFile(self.rootDir + '/a', 'a1')
        assert(sorted(os.listdir(self.rootDir)) == [ 'a', 'var' ] )

    @protect
    def testDirectoryHandling(self):
        # test directory normalization
        self.rootDir = '/' + self.rootDir
        self.addComponent('foo:runtime', '1.0-1-1',
            fileContents = [
                ( '/a', rephelp.Directory(pathId = "1") ),
                ( '/a/1', rephelp.RegularFile(contents = "a1", pathId = "2") ),
            ]
        )

        self.addComponent('foo:runtime', '2.0-2-2',
            fileContents = [
                ( '/a', rephelp.Directory(pathId = "1") ),
                ( '/a/1', rephelp.RegularFile(contents = "a1", pathId = "2") ),
                ( '/a/2', rephelp.RegularFile(contents = "a2", pathId = "3") ),
            ]
        )

        self.updatePkg('foo:runtime=1.0-1-1')
        self.updatePkg('foo:runtime=2.0-2-2')
        assert(sorted(os.listdir(self.rootDir)) == [ 'a', 'var' ] )
        assert(sorted(os.listdir(self.rootDir + "/a")) == [ '1', '2' ] )

    @protect
    def testMkdir(self):
        self.addComponent('foo:runtime', '1.0-1-1',
            fileContents = [
                ( '/b', rephelp.RegularFile(contents = "b1", pathId = "2") ),
            ]
        )

        self.addComponent('foo:runtime', '2.0-2-2',
            fileContents = [
                ( '/a', rephelp.Directory(pathId = "1") ),
                ( '/b', rephelp.RegularFile(contents = "b2", pathId = "2") ),
                ( '/c', rephelp.RegularFile(contents = "c2", pathId = "3") ),
            ]
        )

        self.updatePkg('foo:runtime=1.0-1-1')

        util.sha1Uncompress = lambda *args: self.sha1UncompressStub(
                                                 failPaths = [ 'c' ],
                                                 *args)

        self.logCheck(self.assertRaises,
                      (OSError, self.updatePkg, 'foo:runtime=2.0-2-2'),
                      revertMsg)

        assert(sorted(os.listdir(self.rootDir)) == [ 'b', 'var' ] )

    @protect
    @testhelp.context('rollback')
    def testHldrContent(self):
        # CNY-2596, for example
        self.addComponent('foo:runtime', '1.0',
            fileContents = [
                ( '/a', rephelp.RegularFile(contents = "1") )
            ]
        )
        self.addComponent('foo:runtime', '2.0',
            fileContents = [
                ( '/a', rephelp.RegularFile(contents = "2") )
            ]
        )

        self.updatePkg('foo:runtime=1.0')
        self.writeFile(self.rootDir + '/a', 'local')

        self.cfg.localRollbacks = True
        try:
            self.updatePkg('foo:runtime=2.0', replaceFiles = True)
        finally:
            self.cfg.localRollbacks = False

        counter = Counter(1)
        os.rename = lambda x, y: self.renameStub(x, y, [ 'a' ],
                                                 failAfter = True,
                                                 counter = counter)
        self.logCheck(self.assertRaises,
                      (OSError, self.rollback, 1),
                      revertMsg)

        self.verifyFile(self.rootDir + '/a', '2')

    @testhelp.context('rollback')
    def testSuddenFailure(self):
        # CNY-2592
        self.addComponent('foo:runtime', '1.0',
            fileContents = [
                ( '/a', rephelp.RegularFile(contents = "1") )
            ]
        )
        self.updatePkg('foo:runtime')
        assert(self.rollbackCount() == 0)

        childPid = os.fork()
        if childPid == 0:
            os.unlink = lambda x: self.exitOnUnlink(x, [ 'a' ] )
            self.erasePkg(self.rootDir, 'foo:runtime')
            os._exit(1)

        status = os.waitpid(childPid, 0)[1]
        assert(os.WEXITSTATUS(status) == 0)

        assert(self.rollbackCount() == 1)

        db = conaryclient.ConaryClient.revertJournal(self.cfg)

        assert(self.rollbackCount() == 0)
