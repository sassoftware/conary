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


import grp, os, pwd
from conary_test import recipes

from conary.local import database
from conary.cmds import verify
from conary.repository import changeset
from conary_test import rephelp


class VerifyTest(rephelp.RepositoryHelper):
    def testDisplay(self):
        userDict = {}
        userDict['user'], userDict['group'] = self._getUserGroup()
        self.resetRepository()
        self.resetRoot()

        (built, d) = self.buildRecipe(recipes.testRecipe1, "TestRecipe1")
        pkgname, version = built[0][:2]
        self.updatePkg(self.rootDir, 'testcase', version)
        self.writeFile(self.rootDir + '/usr/bin/hello', 'newtext')
        sb = os.stat(self.rootDir + '/usr/bin/hello')
        # we need the time to change; conary ignores size changes on
        # executables to allow it to handle prelink sanely
        os.utime(self.rootDir + '/usr/bin/hello', (sb.st_mtime + 1,
                                                   sb.st_mtime + 1))

        db = database.Database(self.rootDir, self.cfg.dbPath)
        rc, str = self.captureOutput(verify.verify, ['testcase'], db, self.cfg) 
        # XXX verify that output is correct here...will have to ignore
        # uid/gid information, as localcs expects everything to be owned
        # by root.  Can share parsing code with showchangesettest
        rc, str2 = self.captureOutput(verify.verify, [], db, self.cfg, all=True) 
        assert(str == str2)
        assert('testcase:runtime' in str)
        assert('/usr/bin/hello' in str)
        assert(' 7 ' in str)
        assert('           20 ' in str) # make sure original size of file is displayed
        assert(' -rwxr-xr-x ' in str) # make sure original mode of file is 
                                      # display (Even though that wasn't changed)

        rc, str = self.captureOutput(verify.verify, ['testcase:runtime'], db,
                                     self.cfg, diffBinaries=True)
        self.assertEquals(str,
            'diff --git a/etc/changedconfig b/etc/changedconfig\n'
            'old user root\n'
            'new user %(user)s\n'
            'old group root\n'
            'new group %(group)s\n'
            'diff --git a/etc/unchangedconfig b/etc/unchangedconfig\n'
            'old user root\n'
            'new user %(user)s\n'
            'old group root\n'
            'new group %(group)s\n'
            'diff --git a/usr/share/changed b/usr/share/changed\n'
            'old user root\n'
            'new user %(user)s\n'
            'old group root\n'
            'new group %(group)s\n'
            'diff --git a/usr/share/unchanged b/usr/share/unchanged\n'
            'old user root\n'
            'new user %(user)s\n'
            'old group root\n'
            'new group %(group)s\n'
            'diff --git a/usr/bin/hello b/usr/bin/hello\n'
            'old user root\n'
            'new user %(user)s\n'
            'old group root\n'
            'new group %(group)s\n'
            'GIT binary patch\n'
            'literal 7\n'
            'Oc$~{iEiXx}C;<Qr9Rm;m\n'
            '\n' % userDict)

        self.logFilter.add()
        verify.verify(['unknownpkg'], db, self.cfg) 
        verify.verify(['unknownpkg=@rpl:linux'], db, self.cfg) 
        self.logFilter.remove()
        self.logFilter.compare(('error: trove unknownpkg is not installed',
            'error: version @rpl:linux of trove unknownpkg is not installed'))

    def testVerifyWithSignatures(self):
        # Make sure that verify works with troves that have
        # missing components, which means that the collection's signature
        # is no good...
        self.addComponent('foo:runtime', '1.0', '',
                            ['/foo'])
        self.addComponent('foo:data', '1.0')
        self.addCollection('foo', '1.0', [':runtime', ':data'])
        self.updatePkg(['foo', 'foo:runtime'], recurse=False)
        self.writeFile(self.rootDir + '/foo', 'newtext')

        db = database.Database(self.rootDir, self.cfg.dbPath)
        self.captureOutput(verify.verify, ['foo'], db, self.cfg)

    def testVerifyRemovedFiles(self):
        # CNY-950
        self.addComponent('foo:runtime', '1.0', fileContents = ['/foo'])
        self.updatePkg('foo:runtime')
        self.removeFile(self.rootDir, '/foo')
        db = database.Database(self.rootDir, self.cfg.dbPath)
        s = self.captureOutput(verify.verify, ['foo:runtime'], db, self.cfg)
        assert(not s[1])

    @staticmethod
    def _getUserGroup():
        user = pwd.getpwuid(os.getuid()).pw_name
        group = grp.getgrgid(os.getgid()).gr_name
        return user, group

    def testVerifyToFile(self):
        db = database.Database(self.rootDir, self.cfg.dbPath)
        os.chdir(self.workDir)

        user, group = self._getUserGroup()

        self.addComponent('foo:runtime', '1.0',
                  fileContents = [('/foo',
                                   rephelp.RegularFile(owner = user,
                                                       group = group))])
        self.updatePkg('foo:runtime')

        s = verify.verify(['foo:runtime'], db, self.cfg,
                          changesetPath = 'foo.ccs')
        cs = changeset.ChangeSetFromFile('foo.ccs')
        assert(list(cs.iterNewTroveList()) == [])

        f = open(self.rootDir + '/foo', "a")
        f.write("mod")
        f.close()

        s = self.captureOutput(verify.verify, ['foo:runtime'], db, self.cfg,
                               changesetPath = 'foo.ccs')
        assert(not s[1])
        cs = changeset.ChangeSetFromFile('foo.ccs')
        assert(list(cs.iterNewTroveList())[0].getName() == 'foo:runtime')

    def testVerifyAll(self):
        os.chdir(self.workDir)
        self.addComponent('foo:runtime', '1.0', fileContents = ['/bin/b'])
        self.addComponent('bar:lib', '1.0', fileContents = ['/lib/l'])
        self.addCollection('foo', [ ':runtime' ])
        self.addCollection('bar', [ ':lib' ])
        db = database.Database(self.rootDir, self.cfg.dbPath)
        self.updatePkg('foo')
        self.updatePkg('bar')

        verify.verify([], db, self.cfg, all = True, changesetPath = 'foo.ccs')
        cs = changeset.ChangeSetFromFile('foo.ccs')
        assert(sorted([ x.getName() for x in cs.iterNewTroveList() ]) ==
                    [ 'bar:lib', 'foo:runtime' ] )

    def testHashCheck(self):
        # by default, we trust the size/date timestamps
        repos = self.openRepository()
        db = database.Database(self.rootDir, self.cfg.dbPath)
        os.chdir(self.workDir)

        user, group = self._getUserGroup()
        trv = self.addComponent('foo:runtime',
            fileContents = [ ( '/a', rephelp.RegularFile(contents = '1234',
                                                         owner = user,
                                                         group = group)) ] )
        fileInfo = trv.iterFileList().next()
        self.updatePkg('foo:runtime')

        f = open(self.rootDir + '/a', "w")
        f.write('abcd')
        f.close()

        f = repos.getFileVersions([(fileInfo[0], fileInfo[2], fileInfo[3])])[0]
        st = os.stat(self.rootDir + '/a')
        os.utime(self.rootDir + '/a', (f.inode.mtime(), f.inode.mtime()))

        s = self.captureOutput(verify.verify, ['foo:runtime'], db, self.cfg,
                               changesetPath = 'foo.ccs')
        assert(not s[1])

        verify.verify(['foo:runtime'], db, self.cfg, forceHashCheck = True,
                      changesetPath = 'foo.ccs')
        cs = changeset.ChangeSetFromFile('foo.ccs')
        assert(cs.files)

    def testNewFiles(self):
        userDict = {}
        userDict['user'], userDict['group'] = self._getUserGroup()

        self.addComponent('foo:run=1',
              fileContents = [ ('/bin/ls',
                                rephelp.RegularFile(owner = userDict['user'],
                                        group = userDict['group'],
                                        contents = 'content\n')) ])
        self.updatePkg('foo:run=1')
        db = self.openDatabase()
        s = self.captureOutput(verify.verify, ['foo:run'], db, self.cfg,
                               asDiff = True)[1]
        self.assertEquals(s, '')

        # we don't notice the new file unless all is given because
        # nothing owns /bin
        self.writeFile(self.rootDir + '/bin/new-file', 'newtext\n')
        s = self.captureOutput(verify.verify, ['foo:run'], db, self.cfg,
                               asDiff = True, newFiles = True)[1]
        self.assertEquals(s, '')

        s = self.captureOutput(verify.verify, [], db, self.cfg,
                               asDiff = True, newFiles = True,
                               all = True)[1]
        self.assertEquals(s,
             'diff --git a/bin/new-file b/bin/new-file\n'
             'new user %(user)s\n'
             'new group %(group)s\n'
             'new mode 100644\n'
             '--- a/dev/null\n'
             '+++ b/bin/new-file\n'
             '@@ -1,0 +1,1 @@\n'
             '+newtext\n' % userDict)

        # check the normal output format as well
        s = self.captureOutput(verify.verify, [], db, self.cfg,
                               newFiles = True, all = True)[1]
        # filter out the timestamp
        s = ' '.join(s.split()[0:8] + s.split()[10:])
        self.assertEquals(s,
            'Install @new:files=1.0-1-1 New -rw-r--r-- 1 %(user)s %(group)s '
            '8 UTC /bin/new-file' % userDict)

        # if we add don't check /bin to the exclude list the diff should
        # go away
        oldCfg = self.cfg.verifyDirsNoNewFiles[:]
        try:
            self.cfg.verifyDirsNoNewFiles.append('/bin')
            s = self.captureOutput(verify.verify, [], db, self.cfg,
                                   asDiff = True, newFiles = True,
                                   all = True)[1]
            self.assertEquals(s, '')
        finally:
            self.cfg.verifyDirsNoNewFiles = oldCfg

        # make a package own /bin, and then verifying that package w/
        # --new-files should make it show up
        self.addComponent('foo:dir=1',
              fileContents = [ ('/bin',
                                rephelp.Directory(owner = userDict['user'],
                                                  group = userDict['group'],))])
        self.updatePkg('foo:dir=1')
        s = self.captureOutput(verify.verify, ['foo:dir'], db, self.cfg,
                               asDiff = True, newFiles = True)[1]
        self.assertEquals(s,
             'diff --git a/bin/new-file b/bin/new-file\n'
             'new user %(user)s\n'
             'new group %(group)s\n'
             'new mode 100644\n'
             '--- a/dev/null\n'
             '+++ b/bin/new-file\n'
             '@@ -1,0 +1,1 @@\n'
             '+newtext\n' % userDict)

    def testNewFileOwnership(self):
        # make sure files found with --new-files get assigned to the right
        # troves
        user, group = self._getUserGroup()

        self.addComponent('foo:bin=0',
              fileContents = [ ('/bin',
                                rephelp.Directory(owner = user,
                                                  group = group,)) ])
        self.addComponent('foo:lib=1',
              fileContents = [ ('/lib',
                                rephelp.Directory(owner = user,
                                                  group = group,)) ])

        self.updatePkg([ 'foo:bin', 'foo:lib' ])
        db = self.openDatabase()
        self.writeFile(self.rootDir + '/bin/new', 'newtext\n')
        self.writeFile(self.rootDir + '/lib/new', 'newtext\n')
        self.writeFile(self.rootDir + '/rootfile', 'newtext\n')

        os.chdir(self.workDir)
        verify.verify([], db, self.cfg, all = True, newFiles = True,
                      changesetPath = 'foo.ccs')
        cs = changeset.ChangeSetFromFile('foo.ccs')

        trvCsByName = dict((x.getName(), x) for x in cs.iterNewTroveList())
        self.assertEquals(
            [ x[1] for x in trvCsByName['foo:bin'].getNewFileList() ],
            [ '/bin/new'] )
        self.assertEquals(
            [ x[1] for x in trvCsByName['foo:lib'].getNewFileList() ],
            [ '/lib/new'] )
        self.assertEquals(
            [ x[1] for x in trvCsByName['@new:files'].getNewFileList() ],
            [ '/rootfile'] )
