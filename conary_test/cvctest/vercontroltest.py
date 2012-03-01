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

import os, tempfile

from conary_test import rephelp

from conary.lib import util
from conary.state import ConaryStateFromFile
from conary import checkin


class _VersionControlTestClass:

    def testVersions(self):
        self.logFilter.add()
        oldTmpDir = self.cfg.tmpDir
        # some version control systems modify files in user's $HOME
        oldHome = os.environ['HOME']
        os.environ['HOME'] = self.workDir
        try:
            self.srcRepoPath = self.workDir + '/srcrepo/testproject'
            self.srcWorkingDir = self.workDir + '/srcwork'
            e,l = self.captureOutput(self.vcInit)
            assert not e
            e,l = self.captureOutput(self.vcCheckout, self.srcWorkingDir)
            assert not e
            os.chdir(self.srcWorkingDir)
            self.writeFile('testfile', 'contents 1\n')
            e,l = self.captureOutput(self.vcAdd, 'testfile')
            assert not e
            e,l = self.captureOutput(self.vcCommit, 'commit1')
            assert not e
            e,l = self.captureOutput(self.vcTag, 'tagged-test')
            assert not e
            os.chdir(self.workDir)
            self.newpkg('test')
            os.chdir('test')
            self.writeFile('test.recipe',
                    "class TestRecipe(PackageRecipe):\n"
                    "    version = '1.0'\n"
                    "    name = 'test'\n"
                    "    clearBuildReqs()\n"
                    "    def setup(r):\n" +
                    "        %s\n" % self.vcAddLine() +
                    "        r.Install('testfile', '/testfile')\n")
            self.addfile('test.recipe')
            e, l = self.captureOutput(self.commit)
            assert not e
            self.checkInitialCommitOutput(l)

            firstState = ConaryStateFromFile('CONARY').getSourceState()
            [ recipePathId, ]  = [ x[0] for x in firstState.iterFileList() if
                                    x[1] == 'test.recipe' ]
            [ archivePathId, ] = [ x[0] for x in firstState.iterFileList() if
                                    x[1] != 'test.recipe' ]
            archivePath = firstState.getFile(archivePathId)[0]

            self.captureOutput(self.cookFromRepository, 'test')
            self.captureOutput(self.updatePkg, 'test')
            self.verifyFile(self.rootDir  +'/testfile', 'contents 1\n')

            # change the archive tip, force a recommit, and make sure the
            # snapshot hasn't changed
            os.chdir(self.srcWorkingDir)
            self.writeFile('testfile', 'contents 1.1\n')
            # force the timestamp to change; svn needs this
            ts = os.stat('testfile').st_mtime + 1
            os.utime('testfile', (ts, ts))
            self.captureOutput(self.vcCommit, 'commit2')

            os.chdir(self.workDir + '/test')
            self.captureOutput(self.commit)
            newState = ConaryStateFromFile('CONARY').getSourceState()
            assert(newState == firstState)

            open('test.recipe', "a").write('\n')
            self.captureOutput(self.commit)
            newState = ConaryStateFromFile('CONARY').getSourceState()
            assert(newState.getFile(recipePathId) !=
                   firstState.getFile(recipePathId))
            assert(newState.getFile(archivePathId) ==
                   firstState.getFile(archivePathId))

            # a refresh should force the snapshot to get updated though
            dirName = os.getcwd()
            os.chdir('..')
            repos = self.openRepository()
            # test checkin.refresh as an API that takes directory name
            e, l = self.captureOutput(checkin.refresh, repos, self.cfg,
                refreshPatterns=[archivePath], dirName=dirName)
            os.chdir(dirName)
            self.checkRefreshOutput(l)
            open('test.recipe', "a").write('\n')
            self.captureOutput(self.commit)
            newState = ConaryStateFromFile('CONARY').getSourceState()
            assert(newState.getFile(recipePathId) !=
                   firstState.getFile(recipePathId))
            assert(newState.getFile(archivePathId) !=
                   firstState.getFile(archivePathId))
            self.captureOutput(self.cookFromRepository, 'test')
            self.captureOutput(self.updatePkg, 'test')
            self.verifyFile(self.rootDir  +'/testfile', 'contents 1.1\n')

            # now test tagging
            self.writeFile('test.recipe',
                    "class TestRecipe(PackageRecipe):\n"
                    "    version = '1.0'\n"
                    "    name = 'test'\n"
                    "    clearBuildReqs()\n"
                    "    def setup(r):\n" +
                    "        %s\n" % self.vcAddLine(tag = 'tagged-%(name)s') +
                    "        r.Install('testfile', '/testfile')\n")
            e, l = self.captureOutput(self.commit)
            self.checkTagOutput(l)
            newState = ConaryStateFromFile('CONARY').getSourceState()
            [ newArchivePathId, ] = [ x[0] for x in newState.iterFileList() if
                                        x[1] != 'test.recipe' ]
            newArchivePath = newState.getFile(newArchivePathId)[0]
            assert(newArchivePathId != archivePathId)
            assert(newArchivePath != archivePath)
            self.captureOutput(self.cookFromRepository, 'test')
            self.captureOutput(self.updatePkg, 'test')
            self.verifyFile(self.rootDir  +'/testfile', 'contents 1\n')

            # Mock checkPath to make sure we can still build even if we don't
            # have the version control into the path
            mockCheckPath = lambda x: None
            self.mock(util, "checkPath", mockCheckPath)
            self.logFilter.clear()
            self.logFilter.add()
            self.cookFromRepository('test')
            self.logFilter.remove()
            for es in ['tar', 'bzip2']:
                self.assertIn('warning: Failed to find possible build '
                    'requirement for path "%s"' % es,
                    self.logFilter.records)
        finally:
            os.environ['HOME'] = oldHome
            # adding sources shouldn't tweak tmpDir
            assert(self.cfg.tmpDir == oldTmpDir)
            self.logFilter.clear()

    def checkInitialCommitOutput(self, l):
        pass

    def checkRefreshOutput(self, l):
        pass

    def checkTagOutput(self, l):
        pass

class MercurialTest(rephelp.RepositoryHelper, _VersionControlTestClass):
    _cmd = "hg"

    def vcInit(self):
        util.mkdirChain(os.path.dirname(self.srcRepoPath))
        file(self.workDir + '/.hgrc', 'w').write(
            '[ui]\nusername=me <me@example.com>')
        os.system('hg init %s' % self.srcRepoPath)

    def vcCheckout(self, target):
        os.system('hg -q clone %s %s' % (self.srcRepoPath, target))

    def vcAdd(self, path):
        os.system('hg -q add %s' % path)

    def vcCommit(self, message):
        os.system('hg -q commit -m"%s"; hg -q push' %message)

    def vcAddLine(self, tag = None):
        # CNY-1614: expand macros in URLs, tags etc
        repoPath = self.srcRepoPath.replace("test", "%(name)s")
        if tag is None:
            return 'r.addMercurialSnapshot("%s")' % repoPath

        else:
            return 'r.addMercurialSnapshot("%s", tag = "%s")' % \
                        (repoPath, tag)

    def vcTag(self, tag):
        os.system('hg -q tag %s; hg -q push %s' % (tag, self.srcRepoPath))

    def checkInitialCommitOutput(self, l):
        assert ('\nsummary:     Added tag tagged-test for changeset' in l or \
                '\ndescription:\nAdded tag tagged-test for changeset' in l), 'missing changelog entry'
        assert l.startswith('changeset:   1:'), 'Missing changeset initial log message'

    def checkRefreshOutput(self, l):
        assert ('\nsummary:     commit2\n' in l or \
                '\ndescription:\ncommit2\n' in l), 'missing changelog entry'
        assert l.startswith('changeset:   2:'), 'Missing changeset secondary log message'

    def checkTagOutput(self, l):
        # Note: latest in repository, NOT commit message for what was tagged
        assert ('\nsummary:     commit2\n' in l or \
                '\ndescription:\ncommit2\n' in l), 'missing changelog entry'

class CvsTest(rephelp.RepositoryHelper, _VersionControlTestClass):
    _cmd = "cvs"

    def vcInit(self):
        l = self.srcRepoPath.split('/')
        self.root = '/'.join(l[:-1])
        self.project = l[-1]
        os.system('cvs -d %s init' % self.root)
        emptyDir = tempfile.mkdtemp()
        os.system('cd %s; cvs -Q -d %s import -m "import msg" %s '
                  'importtag starttag' % (emptyDir, self.root, self.project))
        os.rmdir(emptyDir)

    def vcCheckout(self, target):
        os.system('cvs -Q -d %s checkout -d %s %s' %
                  (self.root, target, self.project))

    def vcAdd(self, path):
        os.system('cvs -Q add %s' % path)

    def vcCommit(self, message):
        os.system('cvs -Q commit -f -m"%s"' %message)

    def vcAddLine(self, tag = None):
        root = self.root.replace("test", "%(name)s")
        project = self.project.replace("test", "%(name)s")
        if tag is None:
            return 'r.addCvsSnapshot("%s", "%s")' % (root, project)

        else:
            return 'r.addCvsSnapshot("%s", "%s", tag = "%s")' % \
                        (root, project, tag)

    def vcTag(self, tag):
        os.system('cvs -Q tag %s' % tag)

class SvnTest(rephelp.RepositoryHelper, _VersionControlTestClass):
    _cmd = "svn"

    def vcInit(self):
        util.mkdirChain(os.path.dirname(self.srcRepoPath))
        os.system('svnadmin create %s' % self.srcRepoPath)
        os.system('svn -q mkdir file://localhost%s/tags --message foo'
                        % self.srcRepoPath)
        os.system('svn -q mkdir file://localhost%s/trunk --message foo'
                        % self.srcRepoPath)

    def vcCheckout(self, target):
        os.system('svn -q checkout file://localhost%s/trunk %s' %
                  (self.srcRepoPath, target))

    def vcAdd(self, path):
        os.system('svn -q add %s' % path)

    def vcCommit(self, message):
        os.system('svn -q commit --message "%s"' %message)

    def vcAddLine(self, tag = None):
        repoPath = self.srcRepoPath.replace("test", "%(name)s")
        if tag is None:
            return 'r.addSvnSnapshot("file://localhost%s/trunk")' \
                        % repoPath

        else:
            return 'r.addSvnSnapshot("file://localhost%s/tags/%s")' \
                        % (repoPath, tag)

    def vcTag(self, tag):
        os.system('svn -q copy $PWD file://localhost%s/tags/%s --message tagCommitMessage' 
                        % (self.srcRepoPath, tag) )

    def checkInitialCommitOutput(self, l):
        assert '\nr3 ' in l
        assert '\ncommit1\n' in l, 'wrong commit message'

    def checkRefreshOutput(self, l):
        assert '\nr5 ' in l
        assert '\ncommit2\n' in l, 'wrong commit message'

    def checkTagOutput(self, l):
        assert '\nr4 ' in l
        assert '\ntagCommitMessage\n' in l, 'wrong commit message'

class BzrTest(rephelp.RepositoryHelper, _VersionControlTestClass):
    _cmd = "bzr"

    def setUp(self):
        if not util.checkPath('bzr'):
            raise testhelp.SkipTestException('bzr not installed')
        rephelp.RepositoryHelper.setUp(self)

    def vcInit(self):
        os.system('bzr whoami "me <me@example.com>"')
        util.mkdirChain(os.path.dirname(self.srcRepoPath))
        os.system('bzr init %s' % self.srcRepoPath)
        # XXX needed to support tags in directory-based repositories
        os.system('cd %s ; bzr upgrade --dirstate-tags' % self.srcRepoPath)

    def vcCheckout(self, target):
        os.system('bzr branch %s %s' % (self.srcRepoPath, target))

    def vcAdd(self, path):
        os.system('bzr add %s' % path)

    def vcCommit(self, message):
        os.system('bzr commit -m"%s" ; bzr push %s' % (message, self.srcRepoPath))

    def vcAddLine(self, tag = None):
        # CNY-1614: expand macros in URLs, tags etc
        repoPath = self.srcRepoPath.replace("test", "%(name)s")
        if tag is None:
            return 'r.addBzrSnapshot("%s")' % repoPath
        else:
            return 'r.addBzrSnapshot("%s", tag = "%s")' % \
                        (repoPath, tag)

    def vcTag(self, tag):
        os.system('bzr tag %s ; bzr commit -m"tagCommitMessage" ;'
                  ' bzr push %s' % (tag, self.srcRepoPath))

    def checkInitialCommitOutput(self, l):
        assert '\nrevno: 1\n' in l
        assert '\ntags: tagged-test\n' in l
        assert '\nmessage:\n  commit1\n' in l, 'wrong commit message'

    def checkRefreshOutput(self, l):
        assert '\nrevno: 2\n' in l
        assert '\nbranch nick: srcwork\n' in l
        assert '\nmessage:\n  commit2\n' in l, 'wrong commit message'

    checkTagOutput = checkInitialCommitOutput

class GitTest(rephelp.RepositoryHelper, _VersionControlTestClass):
    _cmd = "git"

    def setUp(self):
        if not util.checkPath('git'):
            raise testhelp.SkipTestException('git not installed')
        rephelp.RepositoryHelper.setUp(self)

    def vcInit(self):
        util.mkdirChain(self.srcRepoPath)
        os.system('cd %s; git init --bare;'
                  ' git config user.name author;'
                  ' git config user.email author@foo.com;'
                  ' git add . foo; git commit -a -m "initialized"'
                  % self.srcRepoPath)

    def vcCheckout(self, target):
        os.system('git clone %s %s; cd %s;'
                  ' git config user.name author;'
                  ' git config user.email author@foo.com;'
                  % (self.srcRepoPath, target, target))

    def vcAdd(self, path):
        os.system('git add %s' % path)

    def vcCommit(self, message):
        os.system('git commit -a -m"%s"; git push --all' %message)

    def vcAddLine(self, tag = None):
        # CNY-1614: expand macros in URLs, tags etc
        repoPath = self.srcRepoPath.replace("test", "%(name)s")
        if tag is None:
            return 'r.addGitSnapshot("%s")' % repoPath

        else:
            return 'r.addGitSnapshot("%s", tag = "%s")' % (repoPath, tag)

    def vcTag(self, tag):
        os.system('git tag -a -m "" %s ; git push --tags' % tag)

    def checkInitialCommitOutput(self, l):
        assert '\nAuthor: author <author@foo.com>\n' in l
        assert '\n    commit1\n' in l

    def checkRefreshOutput(self, l):
        assert l.startswith('commit ')
        assert '\n    commit2\n' in l

    def checkTagOutput(self, l):
        assert '\n    commit2\n' in l
