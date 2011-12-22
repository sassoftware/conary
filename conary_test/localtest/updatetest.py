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
from conary_test import rephelp

import itertools
import os
import signal
import shutil

from conary import conaryclient, errors, trove, versions
from conary.build import tags
from conary.conaryclient import filetypes
from conary.deps import deps
from conary.repository import changeset, filecontainer
from conary.lib import util, log
from conary.local import database, update

import conary_test
from conary_test import recipes
from conary_test import resources


class LocalUpdateTest(rephelp.RepositoryHelper):

    def testPathUnderSymlinkUpdated(self):
        # We update to a package that references /foo/foo.h explicitly,
        # where before it was merely created by symlink from foo -> foo-1
        # and /foo-1/foo.h.  Conary can't handle this currently due to 
        # the way it performs its updates.

        # NOTE: no way to create symlinks with addQuickTestComponent
        raise testhelp.SkipTestException('CNY-298 (closed, would require major changes - path switched to be under symlink dir fails')
        recipe1 = """\
class Foo(PackageRecipe):
    name ='foo'
    version='1.0'
    clearBuildReqs()
    def setup(r):
        r.Create('/foo-1/foo.h')
        r.Symlink('/foo-1', '/foo')
"""
        self.buildRecipe(recipe1, "Foo")
        self.addQuickTestComponent('foo:runtime', '2.0', '', 
                                   ['/foo/foo.h'])
        self.updatePkg(self.rootDir, ['foo:runtime=1.0'])
        self.updatePkg(self.rootDir, ['foo:runtime=2.0'])


    @testhelp.context('initialcontents')
    def testInitialContentsSwitchesPathId(self):
        # make sure that initial contents are preserved even when the pathId
        # changes between versions and when the file moves components
        self.addComponent('foo:runtime', '1.0',
                      pathIdSalt = '1',
                      fileContents = [ ('/usr/share/foo',
                          rephelp.RegularFile(contents = '/bar\n',
                                              initialContents = True) ) ])

        self.updatePkg('foo:runtime')
        self.writeFile(self.rootDir + '/usr/share/foo', 'bam\nbam\n')

        self.addComponent('foo:runtime', '1.1',
                      pathIdSalt = '2',
                      fileContents = [ ('/usr/share/foo',
                          rephelp.RegularFile(contents = '/bar\n',
                                              initialContents = True) ) ])

        self.updatePkg('foo:runtime')
        self.verifyFile(self.rootDir + '/usr/share/foo', 'bam\nbam\n')

        self.addComponent('foo:lib', '1.2',
                      pathIdSalt = '3',
                      fileContents = [ ('/usr/share/foo',
                          rephelp.RegularFile(contents = '/bar\n',
                                              initialContents = True) ) ])
        self.updatePkg([ '-foo:runtime', '+foo:lib' ])
        self.verifyFile(self.rootDir + '/usr/share/foo', 'bam\nbam\n')

    def testFileSwitchesPathId(self):
        self.addComponent('foo:runtime', '1.0',
                      pathIdSalt = '1',
                      fileContents = [ ('/usr/share/foo',
                          rephelp.RegularFile(contents = '/bar\n') ) ])

        self.updatePkg('foo:runtime')
        self.writeFile(self.rootDir + '/usr/share/foo', 'bam\nbam\n')

        self.addComponent('foo:runtime', '1.1',
                      pathIdSalt = '2',
                      fileContents = [ ('/usr/share/foo',
                          rephelp.RegularFile(contents = '/bar\n') ) ])

        self.updatePkg('foo:runtime')

    @testhelp.context('initialcontents')
    def testInitialContentsSourcePointer(self):
        # Conary shares contents between files when possible in its changeset 
        #  - if two files have exactly the same contents, those contents
        # are only stored in the changeset once.  When restoring, conary
        # just copies the contents from one location to the other.
        # However, that assumes that the contents on disk at location 1
        # are actually the same as in the pristine version - that's only
        # true when the file is not a config or initialcontents.
        # (CNY-1084).
        nonInitial = rephelp.RegularFile(contents = '/bar\n',
                                              initialContents = False)
        nonInitial2 = rephelp.RegularFile(contents = '/bar1\n',
                                              initialContents = False)
        initial = rephelp.RegularFile(contents = '/bar\n',
                                      initialContents = True)
        initial2 = rephelp.RegularFile(contents = '/bar1\n',
                                      initialContents = True)
        self.addComponent('foo:runtime', '1.0',
                          fileContents = [ ('/usr/share/bar', initial) ])
        self.addComponent('foo:runtime', '2.0',
                          fileContents = [ ('/usr/share/bar', initial2),
                                           ('/usr/share/foo', nonInitial2) ])
        self.updatePkg('foo:runtime=1.0')
        self.writeFile('%s/usr/share/bar' % self.cfg.root, 'blammo!\n')
        self.updatePkg('foo:runtime')
        self.verifyFile('%s/usr/share/bar' % self.cfg.root, 'blammo!\n')
        self.verifyFile('%s/usr/share/foo' % self.cfg.root, '/bar1\n')

    def testRename(self):
        orig = rephelp.RegularFile(pathId = '1', contents = '1')

        self.addComponent('foo:runtime', '1.0',
                          fileContents = [ ('/usr/first', orig) ])
        self.addComponent('foo:runtime', '2.0',
                          fileContents = [ ('/usr/second', orig) ])

        self.updatePkg('foo:runtime=1.0')
        self.updatePkg('foo:runtime=2.0')
        assert(not os.path.exists(self.rootDir + '/usr/first'))
        self.verifyFile(self.rootDir + '/usr/second', '1')

        self.updatePkg('foo:runtime=1.0', keepExisting = True)
        self.verifyFile(self.rootDir + '/usr/first', '1')
        self.verifyFile(self.rootDir + '/usr/second', '1')

    def testPathIdChangeSetWithPtr(self):
        # CNY-1407
        t = self.addComponent('foo:runtime', '1.0',
                          fileContents = [ ('/usr/first', 'contents'),
                                           ('/usr/second', 'contents') ] )

        repos = self.openRepository()
        repos.c['localhost'].setProtocolVersion(41)
        cs = repos.createChangeSet([ ('foo:runtime', 
                (None, None), (t.getVersion(), t.getFlavor()), True) ] )
        self.updatePkg(self.rootDir, cs)

    @testhelp.context('fileoverlap', 'rollback')
    def testUpdateWithDuplicates(self):
        def _verify():
            for d in ( '/etc', '/bin', '/lib' ):
                for p in ( '/foo', '/bar' ):
                    self.verifyFile(self.rootDir + d + p, 'foo\n')

        f = rephelp.RegularFile(pathId = '1', contents = 'foo\n')
        cf = rephelp.RegularFile(pathId = '2', contents = 'foo\n')
        o = rephelp.RegularFile(pathId = '3', contents = 'foo\n')
        self.addComponent('foo:runtime', '1.0-1-1',
                          [ ( '/bin/foo', f), ( '/etc/foo', cf ),
                            ( '/lib/foo', o) ] )
        self.addComponent('bar:runtime', '1.0-1-1',
                          [ ( '/bin/bar', f), ( '/etc/bar', cf ),
                            ( '/lib/bar', o) ] )

        # this works because both troves provide contents which are
        # separately available through the in-memory merge. good to
        # test, and we test what happens when the contents only appears
        # once below
        self.updatePkg([ 'foo:runtime', 'bar:runtime' ])
        _verify()

        self.resetRoot()
        csPath = self.workDir + '/foo.ccs'
        repos = self.openRepository()
        self.changeset(repos, ['foo:runtime', 'bar:runtime' ], csPath)
        # make sure the file contents really were written once
        fc = filecontainer.FileContainer(util.ExtendedFile(csPath, "r", buffering=False))
        fc.getNextFile()              # CONARYCHANGESET
        fc.getNextFile()              # config file contents
        fc.getNextFile()              # normal pathId 1 file contents
        fc.getNextFile()              # normal pathId 2 file contents
        assert(fc.getNextFile() is None)
        self.updatePkg(self.rootDir, csPath)
        _verify()
        self.erasePkg(self.rootDir, [ 'foo:runtime', 'bar:runtime' ])
        self.rollback(self.rootDir, 1)
        _verify()

        cf2foo = rephelp.RegularFile(pathId = '2', contents = 'foobar\n')
        cf2bar = rephelp.RegularFile(pathId = '2', contents = 'bar\n')
        self.addComponent('foo:runtime', '2.0-1-1',
                          [ ( '/bin/foo', f), ( '/etc/foo', cf2foo ) ]  )
        self.addComponent('bar:runtime', '2.0-1-1',
                          [ ( '/bin/bar', f), ( '/etc/bar', cf2bar ) ]  )
        # this succeeds because the incoming files have different fileIds
        self.updatePkg([ 'foo:runtime', 'bar:runtime' ])

        # this fails because the incoming config files have the same fileIds
        self.assertRaises(changeset.ChangeSetKeyConflictError,
            self.updatePkg, [ 'foo:runtime=1.0-1-1', 'bar:runtime=1.0-1-1' ])

    @testhelp.context('initialcontents')
    def testInitialContentsReplaceSymlink(self):
        self.addComponent('foo:runtime', '1.0',
                      fileContents = [ ('/usr/foo',
                          rephelp.RegularFile(initialContents = True) ) ])
        util.mkdirChain(self.rootDir + '/usr')
        os.symlink('bar', self.rootDir + '/usr/foo')
        self.updatePkg('foo:runtime')
        assert(os.readlink(self.rootDir + '/usr/foo') == 'bar')
        self.erasePkg(self.rootDir, 'foo:runtime')
        assert(not os.path.exists(self.rootDir + '/usr/foo'))

    @staticmethod
    def mockRunTroveScript(job, script, tagScript, tmpDir, root, callback,
                           isPre = False, collectList = None, returnCode = 0,
                           scriptId = "unknown", oldCompatClass = None,
                           newCompatClass = None):
        argSet = { 'job' : job,
                   'script' : script,
                   'tagScript' : tagScript,
                   'tmpDir' : tmpDir,
                   'root' : root,
                   'callback' : callback,
                   'scriptId' : scriptId,
                   'newCompatClass' : newCompatClass,
                   'oldCompatClass' : oldCompatClass,
                   'isPre' : isPre }
        collectList.append(argSet)

        return returnCode

    @testhelp.context('trovescripts', 'rollback')
    def testGroupScriptsUpdate(self):
        self._testGroupScripts(migrate = False)

    @testhelp.context('trovescripts', 'rollback')
    def testGroupScriptsUpdateNotThreaded(self):
        # CNY-3431
        self.cfg.threaded = False
        self._testGroupScripts(migrate = False)

    @testhelp.context('trovescripts', 'rollback', 'migrate')
    def testGroupScriptsMigrate(self):
        self._testGroupScripts(migrate = True)

    def _testGroupScripts(self, migrate = False):
        oldRunTroveScript = update.runTroveScript

        try:
            runInfo = []
            update.runTroveScript = (lambda *args, **kwargs:
                        self.mockRunTroveScript(collectList = runInfo, *args,
                                                **kwargs) )
            self.addComponent('foo:runtime', '1.0')
            self.addCollection('group-foo', '1.0', [ ('foo:runtime', '1.0' ) ],
                               postInstallScript = 'postinstall',
                               preUpdateScript = 'preupdate',
                               postUpdateScript = 'postupdate',
                               preInstallScript = 'preinstall',
                               preEraseScript = 'preerase',
                               postEraseScript = 'posterase',)

            self.addComponent('bar:runtime', '1.0', filePrimer = 1)
            self.addCollection('group-bar', '1.0', [ ('bar:runtime', '1.0' ) ],
                               preInstallScript = 'preinstall',
                               postInstallScript = 'postinstall',
                               preEraseScript = 'preerase',
                               postEraseScript = 'posterase',)

            self.addComponent('baz:runtime', '1.0', filePrimer = 2)
            self.addCollection('group-baz', '1.0', [ ('baz:runtime', '1.0' ) ],
                               preEraseScript = 'preerase',
                               postEraseScript = 'posterase',)

            assertEq = self.failUnlessEqual

            self.updatePkg(['group-foo', 'group-bar'])
            assertEq(len(runInfo), 4)

            for idx, groupName, scriptVal, isPre in [
                    (0, 'group-bar', 'preinstall', True),
                    (1, 'group-bar', 'postinstall', False),
                    (2, 'group-foo', 'preinstall', True),
                    (3, 'group-foo', 'postinstall', False)]:
                assert(not runInfo[idx]['tagScript'])
                assertEq(runInfo[idx]['root'], self.rootDir)
                assert(runInfo[idx]['callback'])
                assertEq(runInfo[idx]['script'], scriptVal)
                assertEq(runInfo[idx]['scriptId'],
                         "%s %s" % (groupName, scriptVal))
                assertEq(runInfo[idx]['newCompatClass'], 0)
                assertEq(runInfo[idx]['oldCompatClass'], None)
                assertEq(runInfo[idx]['isPre'], isPre)

            del runInfo[:]

            self.addCollection('group-foo', '1.1', [ ('foo:runtime', '1.0' ) ],
                               compatClass = 1,
                               postInstallScript = 'postintall-1.1',
                               preUpdateScript = 'preupdate-1.1',
                               postUpdateScript = 'postupdate-1.1',
                               preInstallScript = 'preinstall-1.1',
                               preEraseScript = 'preerase-1.1',
                               postEraseScript = 'posterase-1.1',
                               preRollbackScript = 'prerollback',
                               postRollbackScript =
                                rephelp.RollbackScript(script= 'postrollback',
                                                       conversions = [ 0 ] ) )
            self.updatePkg(['group-foo', 'group-baz'], migrate = migrate)

            # Pre before post
            groupFooScripts = [ x['script'] for x in runInfo
                                if x['scriptId'].startswith('group-foo ') ]
            groupBarScripts = [ x['script'] for x in runInfo
                                if x['scriptId'].startswith('group-bar ') ]
            assertEq(groupFooScripts, ['preupdate-1.1', 'postupdate-1.1'])
            if migrate:
                assertEq(groupBarScripts, ['preerase', 'posterase'])
            else:
                assertEq(groupBarScripts, [])

            # Sort the scripts by operation and trove name
            sortKey = lambda x: list(reversed(x['scriptId'].split()))
            updateScripts = sorted([ x for x in runInfo
                                     if x['scriptId'].endswith('update') ],
                                   key = sortKey)
            eraseScripts = sorted([ x for x in runInfo
                                    if x['scriptId'].endswith('erase') ],
                                   key = sortKey)

            assertEq(len(updateScripts), 2)
            if migrate:
                assertEq(len(eraseScripts), 2)
            else:
                assertEq(len(eraseScripts), 0)

            assertEq(updateScripts[1]['script'], 'preupdate-1.1')
            assertEq(updateScripts[1]['scriptId'], 'group-foo preupdate')
            assert(updateScripts[1]['isPre'])
            assert(updateScripts[1]['callback'])
            assertEq(updateScripts[1]['oldCompatClass'], 0)
            assertEq(updateScripts[1]['newCompatClass'], 1)

            assertEq(updateScripts[0]['script'], 'postupdate-1.1')
            assertEq(updateScripts[0]['scriptId'], 'group-foo postupdate')
            assert(not updateScripts[0]['isPre'])
            assert(updateScripts[0]['callback'])
            assertEq(updateScripts[0]['oldCompatClass'], 0)
            assertEq(updateScripts[0]['newCompatClass'], 1)

            preeraseScripts = [ x for x in eraseScripts
                                if x['scriptId'].endswith('preerase') ]
            posteraseScripts = [ x for x in eraseScripts
                                if x['scriptId'].endswith('posterase') ]
            if migrate:
                assertEq(len(preeraseScripts), 1)
                assertEq(len(posteraseScripts), 1)
                assertEq([ x['scriptId'].split()[0] for x in preeraseScripts ],
                         ['group-bar'])
                assertEq([ x['scriptId'].split()[0] for x in posteraseScripts ],
                         ['group-bar'])
            else:
                assertEq(len(preeraseScripts), 0)
                assertEq(len(posteraseScripts), 0)

            for s in preeraseScripts:
                assert(s['isPre'])
                assert(s['callback'])
                assertEq(s['oldCompatClass'], 0)
                assertEq(s['newCompatClass'], None)

            for s in posteraseScripts:
                assert(not s['isPre'])
                assert(s['callback'])
                assertEq(s['oldCompatClass'], 0)
                assertEq(s['newCompatClass'], None)

            del(runInfo[:])

            self.rollback(self.rootDir, 1)
            if migrate:
                assertEq(len(runInfo), 8)
            else:
                assertEq(len(runInfo), 6)

            # Pre before post
            groupFooScripts = [ x['script'] for x in runInfo
                                if x['scriptId'].startswith('group-foo ') ]
            groupBarScripts = [ x['script'] for x in runInfo
                                if x['scriptId'].startswith('group-bar ') ]
            groupBazScripts = [ x['script'] for x in runInfo
                                if x['scriptId'].startswith('group-baz ') ]

            assertEq(groupFooScripts, ['prerollback', 'preupdate', 'postupdate',
                                       'postrollback'])
            if migrate:
                assertEq(groupBarScripts, ['preinstall', 'postinstall'])
            else:
                assertEq(groupBarScripts, [])
            assertEq(groupBazScripts, ['preerase', 'posterase'])

            updateScripts = sorted([ x for x in runInfo
                                     if x['scriptId'].endswith('update') ],
                                   key = sortKey)
            eraseScripts = sorted([ x for x in runInfo
                                    if x['scriptId'].endswith('erase') ],
                                   key = sortKey)
            installScripts = sorted([ x for x in runInfo
                                    if x['scriptId'].endswith('install') ],
                                   key = sortKey)
            rollbackScripts = [ x for x in runInfo
                                    if x['scriptId'].endswith('rollback') ]

            assertEq(len(updateScripts), 2)
            assertEq(len(eraseScripts), 2)
            assertEq(len(rollbackScripts), 2)
            if migrate:
                assertEq(len(installScripts), 2)
            else:
                assertEq(len(installScripts), 0)

            for s in updateScripts:
                if s['scriptId'].endswith('preupdate'):
                    action = 'preupdate'
                else:
                    action = 'postupdate'
                assertEq(s['script'], action)
                assertEq(s['scriptId'], 'group-foo ' + action)
                assert(s['isPre'] == (action.startswith('pre')))
                assert(s['callback'])
                assertEq(s['oldCompatClass'], 1)
                assertEq(s['newCompatClass'], 0)

            for s in eraseScripts:
                if s['scriptId'].endswith('preerase'):
                    action = 'preerase'
                else:
                    action = 'posterase'
                oldCompatClass = 0
                assertEq(s['script'], action)
                assert(s['isPre'] == (action.startswith('pre')))
                assert(s['callback'])
                assertEq(s['oldCompatClass'], oldCompatClass)
                assertEq(s['newCompatClass'], None)

            for s in installScripts:
                if s['scriptId'].endswith('preinstall'):
                    action = 'preinstall'
                else:
                    action = 'postinstall'
                assertEq(s['script'], action)
                assertEq(s['scriptId'], 'group-bar ' + action)
                assert(s['isPre'] == (action.startswith('pre')))
                assert(s['callback'])
                assertEq(s['oldCompatClass'], None)
                assertEq(s['newCompatClass'], 0)

            for s, action in zip(rollbackScripts, ['prerollback', 'postrollback']):
                assertEq(s['script'], action)
                assertEq(s['scriptId'], 'group-foo ' + action)
                assert(s['isPre'] == (action.startswith('pre')))
                assert(s['callback'])
                assertEq(s['oldCompatClass'], 1)
                assertEq(s['newCompatClass'], 0)

            del(runInfo[:])

            # now try an update which invalidates the rollback stack
            self.addCollection('group-foo', '1.2', [ ('foo:runtime', '1.0' ) ],
                               compatClass = 1,
                               postUpdateScript =
                                rephelp.TroveScript(script = 'postupdate-1.2'))
            self.updatePkg('group-foo', migrate = migrate)
            del(runInfo[:])

            self.logFilter.add()
            try:
                self.rollback(self.rootDir, 1)
            except database.RollbackDoesNotExist, e:
                self.failUnlessEqual(str(e), 'rollback r.1 does not exist')
                self.logFilter.compare("error: rollback 'r.1' not present")
            else:
                self.fail("Expected exception")
            self.logFilter.clear()
            assert(not self.rollbackList(self.rootDir))

            # test the client call which removes them
            l = os.listdir(self.rootDir + self.cfg.dbPath + '/rollbacks')
            assert(sorted(l) == [ '0', '1', 'status' ])
            client = self.getConaryClient()
            client.removeInvalidRollbacks()
            l = os.listdir(self.rootDir + self.cfg.dbPath + '/rollbacks')
            assert(sorted(l) == [ 'status' ])

            # go back to version 1, but have the preupdate script fail, which
            # should make the entire update fail
            update.runTroveScript = (lambda *args, **kwargs:
                        self.mockRunTroveScript(collectList = runInfo,
                                                returnCode = 1, *args,
                                                **kwargs) )
            self.logCheck2('error: error: preupdate script failed',
                           self.updatePkg, 'group-foo=1.1', migrate = migrate)

            self.resetRoot()

            # Update from changesets

            update.runTroveScript = (lambda *args, **kwargs:
                        self.mockRunTroveScript(collectList = runInfo, *args,
                                                **kwargs) )

            version12 = "/%s//%s/1.2-1-0.1" % (self.defLabel,
                                               versions.CookLabel.name)
            groupBarCcs = util.joinPaths(self.workDir, 'group-bar-1.2.ccs')
            self.addCollection('group-bar', version12,
                               [ ('bar:runtime', '1.0' ) ],
                               preInstallScript = 'preinstall-ccs',
                               postInstallScript = 'postinstall-ccs',
                               preEraseScript = 'preerase-ccs',
                               postEraseScript = 'posterase-ccs',
                               changeSetFile = groupBarCcs)

            del(runInfo[:])
            self.updatePkg(['group-foo=1.0', 'group-bar=%s' % version12],
                           fromFiles = [groupBarCcs])
            assertEq(len(runInfo), 4)

            groupFooCcs = util.joinPaths(self.workDir, 'group-foo-1.2.ccs')
            self.addCollection('group-foo', version12,
                               [ ('foo:runtime', '1.0' ) ],
                               compatClass = 1,
                               postInstallScript = 'postintall-ccs-1.2',
                               preUpdateScript = 'preupdate-ccs-1.2',
                               postUpdateScript = 'postupdate-ccs-1.2',
                               preInstallScript = 'preinstall-ccs-1.2',
                               preEraseScript = 'preerase-ccs-1.2',
                               postEraseScript = 'posterase-ccs-1.2',
                               postRollbackScript =
                                rephelp.RollbackScript(script='postrollback-ccs',
                                                       conversions = [ 0 ]),
                               changeSetFile = groupFooCcs)

            groupBazCcs = util.joinPaths(self.workDir, 'group-baz-1.2.ccs')
            self.addCollection('group-baz', version12,
                               [ ('baz:runtime', '1.0' ) ],
                               preEraseScript = 'preerase-ccs',
                               postEraseScript = 'posterase-ccs',
                               changeSetFile = groupBazCcs)

            import epdb; epdb.stc(11)
            del(runInfo[:])
            self.updatePkg(['group-foo=%s' % version12,
                            'group-baz=%s' % version12],
                           fromFiles = [groupFooCcs, groupBazCcs],
                           migrate = migrate)

            # Pre before post
            groupFooScripts = [ x['script'] for x in runInfo
                                if x['scriptId'].startswith('group-foo ') ]
            groupBarScripts = [ x['script'] for x in runInfo
                                if x['scriptId'].startswith('group-bar ') ]
            assertEq(groupFooScripts, ['preupdate-ccs-1.2', 'postupdate-ccs-1.2'])
            if migrate:
                assertEq(groupBarScripts, ['preerase-ccs', 'posterase-ccs'])
            else:
                assertEq(groupBarScripts, [])

            del(runInfo[:])

            # Now the real point - roll back, see if we get the right scripts
            self.rollback(self.rootDir, 1)
            if migrate:
                assertEq(len(runInfo), 7)
            else:
                assertEq(len(runInfo), 5)

            self.resetRoot()

        finally:
            update.runTroveScript = oldRunTroveScript

    @testhelp.context('trovescripts')
    def testGroupScriptsPostRollback(self):
        # CNY-2829: make sure we execute the post-rollback script at the _end_
        # of the rollback
        updateInfo = conaryclient.CriticalUpdateInfo()
        client = self.getConaryClient()

        self.addComponent('critical:runtime', '1.0', filePrimer = 1)
        self.addComponent('critical:runtime', '1.1', filePrimer = 1)

        self.addComponent('noncritical:runtime', '1.0', filePrimer = 2)
        self.addComponent('noncritical:runtime', '1.1', filePrimer = 2)

        self.addCollection('group-foo', '1.0',
            [ ('%s:runtime' % x, '1.0') for x in ['critical', 'noncritical']])
        self.addCollection('group-foo', '1.1',
            [ ('%s:runtime' % x, '1.1') for x in ['critical', 'noncritical']],
            compatClass = 1,
            postRollbackScript = rephelp.RollbackScript(script= 'postrollback',
                                                        conversions = [ 0 ]) )

        updateInfo = conaryclient.CriticalUpdateInfo()
        updateInfo.setCriticalTroveRegexps(['critical:.*'])
        try:
            self.discardOutput(self.updatePkg, 'group-foo=1.0',
                criticalUpdateInfo = updateInfo, raiseError = True)
        except errors.ReexecRequired, e:
            self.discardOutput(self.updatePkg, 'group-foo=1.0',
                restartInfo=e.data)

        updateInfo = conaryclient.CriticalUpdateInfo()
        updateInfo.setCriticalTroveRegexps(['critical:.*'])
        try:
            self.discardOutput(self.updatePkg, 'group-foo=1.1',
                criticalUpdateInfo = updateInfo, raiseError = True)
        except errors.ReexecRequired, e:
            self.discardOutput(self.updatePkg, 'group-foo=1.1',
                criticalUpdateInfo = updateInfo, restartInfo=e.data)

        runInfo = []
        runTroveScript = (lambda *args, **kwargs:
                    self.mockRunTroveScript(collectList = runInfo, *args,
                                            **kwargs) )
        self.mock(update, 'runTroveScript', runTroveScript)

        self.rollback(self.rootDir, 3)
        self.failIf(runInfo)

        self.rollback(self.rootDir, 2)
        self.failUnlessEqual([(x['scriptId'], x['script']) for x in runInfo],
                             [('group-foo postrollback', 'postrollback')])
        self.failUnlessEqual([(x['oldCompatClass'], x['newCompatClass'])
                                for x in runInfo],
                             [(1, 0)])

        # Now pretend we're running an older version of Conary
        updateInfo = conaryclient.CriticalUpdateInfo()
        updateInfo.setCriticalTroveRegexps(['critical:.*'])
        try:
            self.discardOutput(self.updatePkg, 'group-foo=1.1',
                criticalUpdateInfo = updateInfo, raiseError = True)
        except errors.ReexecRequired, e:
            file(os.path.join(e.data, "features"), "w").write("")
            self.discardOutput(self.updatePkg, 'group-foo=1.1',
                criticalUpdateInfo = updateInfo, restartInfo=e.data)

        # We should have the post-rollback script stored in the last rollback
        fpath = os.path.join(self.rootDir,
                             "var/lib/conarydb/rollbacks/3/post-scripts.meta")
        self.failUnless(os.path.exists(fpath), fpath)

    @testhelp.context('trovescripts')
    def testGroupScriptsPostRollbackInstall(self):
        # CNY-2844
        self.addComponent('foo:runtime', '1.0')
        self.addCollection('group-foo', '1.0', [('foo:runtime', '1.0')],
            compatClass = 1,
            postRollbackScript = rephelp.RollbackScript(script= 'postrollback',
                                                        conversions = [ 0 ]) )

        # Install the group
        self.updatePkg('group-foo')

        runInfo = []
        runTroveScript = (lambda *args, **kwargs:
                    self.mockRunTroveScript(collectList = runInfo, *args,
                                            **kwargs) )
        self.mock(update, 'runTroveScript', runTroveScript)

        self.rollback(self.rootDir, 0)

        # XXX we should _not_ run the script to roll back an install
        self.failUnlessEqual(len(runInfo), 0)


    @testhelp.context('trovescripts')
    def testGroupScriptsNonToplevel(self):
        runInfo = []
        runTroveScript = (lambda *args, **kwargs:
                            runInfo.append((args, kwargs)))

        self.mock(update, 'runTroveScript', runTroveScript)

        self.addComponent('foo:runtime', '1.0')
        self.addCollection('group-foo', '1.0', [ ('foo:runtime', '1.0' ) ],
                           postInstallScript = 'postinstall',
                           preUpdateScript = 'preupdate',
                           postUpdateScript = 'postupdate')

        self.addCollection('group-bar', '1.0', [ ('group-foo', '1.0') ])

        self.addCollection('group-foo', '1.1', [ ('foo:runtime', '1.0' ) ],
                           compatClass = 1,
                           postInstallScript = 'postintall-1.1',
                           preUpdateScript = 'preupdate-1.1',
                           postUpdateScript = 'postupdate-1.1')
        self.addCollection('group-bar', '1.1', [ ('group-foo', '1.1') ])

        self.updatePkg('group-bar=1.0')
        self.assertEqual(len(runInfo), 1)
        self.assertEqual(runInfo[0][0][1], 'postinstall')
        del runInfo[:]

        self.updatePkg('group-bar=1.1', migrate = True)
        self.assertEqual(len(runInfo), 2)
        self.assertEqual(runInfo[0][0][1], 'preupdate-1.1')
        self.assertEqual(runInfo[1][0][1], 'postupdate-1.1')
        del(runInfo[:])


    @testhelp.context('trovescripts')
    def testRunTroveScript(self):
        def _verifyScript(ts, checkLines, scriptContents = None,
                          otherArgs = {}):
            lines = open(ts).readlines()
            os.unlink(ts)
            assert(len(lines) == len(checkLines))
            # grab the script name from the rm command, and cut off the
            # trailing \n
            scriptName = lines[1].split(" ")[1][:-1]

            substDict = { 'script' : scriptName }
            substDict.update(otherArgs)

            for (line, check) in itertools.izip(lines, checkLines):
                assert(line == check % substDict)

            scriptPath = util.joinPaths(self.rootDir, scriptName)
            self.verifyFile(scriptPath, scriptContents)
            os.unlink(scriptPath)

        class MockCallback:

            def __init__(self):
                self.reset()

            def warning(self, s, *args):
                self.warnings.append(s % args)

            def troveScriptOutput(self, typ, msg):
                self.output.append((typ, msg))

            def troveScriptStarted(self, typ):
                self.started = (typ, True)

            def troveScriptFinished(self, typ):
                self.finished = (typ, True)

            def troveScriptFailure(self, typ, errcode):
                self.errcode = (typ, errcode)

            def reset(self):
                self.errcode = None
                self.output = []
                self.warnings = []
                self.started = False
                self.finished = False

        class MockChroot:

            def __init__(self, fail = False):
                self.fail = fail

            def __call__(self, path):
                if self.fail:
                    raise OSError

                os.write(1, "ROOT=%s\n" % path)

        class MockExecve:
            def __init__(self, root):
                self.root = root

            def __call__(self, path, args, environ):
                # make sure the file in the chroot is executable
                sb = os.stat(self.root + args[0])
                assert(sb.st_mode & 0100)
                self.args = args
                self.environ = environ
                os.write(1, "GOOD\n")
                os._exit(0)

        conaryLog = self.rootDir + '/conarylog'
        oldSysLog = log.syslog
        log.syslog = None
        log.openSysLog(self.rootDir, 'conarylog')

        v1 = versions.ThawVersion("/conary.rpath.com@test:trunk/10:1.0-1-1")
        v2 = versions.ThawVersion("/conary.rpath.com@test:trunk/20:2.0-1-1")
        f = deps.parseFlavor('is:x86')

        t1 = trove.Trove('t', v1, f)
        t2 = trove.Trove('t', v2, f)

        util.mkdirChain(self.rootDir + self.cfg.tmpDir)

        absTrvCs = t2.diff(None, absolute = True)[0]
        relTrvCs = t2.diff(t1)[0]
        absJob = absTrvCs.getJob()
        relJob = relTrvCs.getJob()

        cb = MockCallback()
        update.runTroveScript(absJob, 'action', None, self.cfg.tmpDir,
                              self.rootDir, cb, isPre = False)
        assert(cb.warnings ==
                ['Not running script for t due to insufficient permissions '
                 'for chroot()'] )

        ts = self.workDir + '/ts'
        update.runTroveScript(absJob, 'action', ts, self.cfg.tmpDir,
                              self.rootDir, cb, isPre = False,
                              newCompatClass = 1)

        _verifyScript(ts,
            [ "CONARY_NEW_COMPATIBILITY_CLASS='1' "
               "CONARY_NEW_FLAVOR='is: x86' "
               "CONARY_NEW_NAME='t' "
               "CONARY_NEW_VERSION='/conary.rpath.com@test:trunk/2.0-1-1' "
               "PATH='/usr/bin:/usr/sbin:/bin:/sbin' "
               "%(script)s\n",
              "rm %(script)s\n" ], scriptContents = 'action')

        for (hash, isPre) in [ ('', True), ('# ', False) ]:
            update.runTroveScript(relJob, 'action', ts, self.cfg.tmpDir,
                                  self.rootDir, cb, isPre = False,
                                  oldCompatClass = 1,
                                  newCompatClass = 2)
            _verifyScript(ts,
                [ "CONARY_NEW_COMPATIBILITY_CLASS='2' "
                   "CONARY_NEW_FLAVOR='is: x86' "
                   "CONARY_NEW_NAME='t' "
                   "CONARY_NEW_VERSION='/conary.rpath.com@test:trunk/2.0-1-1' "
                   "CONARY_OLD_COMPATIBILITY_CLASS='1' "
                   "CONARY_OLD_FLAVOR='is: x86' "
                   "CONARY_OLD_VERSION='/conary.rpath.com@test:trunk/1.0-1-1' "
                   "PATH='/usr/bin:/usr/sbin:/bin:/sbin' "
                   "%(script)s\n",
                  "rm %(script)s\n" ], scriptContents = 'action',
                  otherArgs = { 'hash' : hash })

        # now try actually doing something
        goodScript = "#!/bin/sh\ntouch %s/RAN; echo RAN" % self.workDir
        badScript = "#!/bin/sh\nexit 1"
        brokenScript = "#!/bin/doesnotexist\n"
        self.failUnlessEqual(cb.started, False)
        self.failUnlessEqual(cb.finished, False)
        rc = update.runTroveScript(absJob, goodScript, None, self.cfg.tmpDir,
                                   '/', cb, isPre = False, scriptId = 'sn')
        self.verifyFile(self.workDir + '/RAN', '')
        os.unlink(self.workDir + '/RAN')
        self.failUnlessEqual(cb.output, [('sn', 'RAN\n')])
        self.failUnlessEqual(cb.errcode, None)
        self.failUnlessEqual(cb.started, ('sn', True))
        self.failUnlessEqual(cb.finished, ('sn', True))
        scriptOut = [ x.strip()[23:] for x in file(conaryLog).readlines() ]
        self.failUnlessEqual(scriptOut, ['running script sn',
            '[sn] RAN', 'script sn finished'])
        file(conaryLog, 'w') # truncate
        assert(rc == 0)
        cb.reset()

        # Make sure we did reset the callback
        self.failUnlessEqual(cb.started, False)
        self.failUnlessEqual(cb.finished, False)

        rc = update.runTroveScript(absJob, badScript, None, self.cfg.tmpDir,
                                   '/', cb, isPre = False, scriptId = 'sn2')
        assert(rc == 1)
        self.failUnlessEqual(cb.errcode, ('sn2', 1))
        self.failUnlessEqual(cb.started, ('sn2', True))
        scriptOut = [ x.strip()[23:] for x in file(conaryLog).readlines() ]
        self.failUnlessEqual(scriptOut, ['running script sn2',
            'script sn2 failed with exit code 1'])
        file(conaryLog, 'w') # truncate
        # Finished script should not have been executed here,
        self.failUnlessEqual(cb.finished, False)
        cb.reset()

        rc = update.runTroveScript(absJob, brokenScript, None, self.cfg.tmpDir,
                                   '/', cb, isPre = False, scriptId = 'sn2')
        self.failUnlessEqual(cb.errcode, ('sn2', 1))
        scriptOut = [ x.strip()[23:] for x in file(conaryLog).readlines() ]
        self.failUnlessEqual(scriptOut, ['running script sn2',
            '[sn2] [Errno 2] No such file or directory',
            'script sn2 failed with exit code 1'])
        file(conaryLog, 'w') # truncate
        cb.reset()

        oldRoot = os.chroot
        oldGetuid = os.getuid
        oldExecve = os.execve

        # When mocking execve, we have to mock the closing of file descriptors
        # too
        def mockMassCloseFileDescriptors(start, count):
            pass
        self.mock(util, 'massCloseFileDescriptors',
                  mockMassCloseFileDescriptors)

        try:
            os.getuid = lambda : 0
            os.execve = MockExecve(self.rootDir)
            os.chroot = MockChroot()

            rc = update.runTroveScript(absJob, goodScript, None,
                                       self.cfg.tmpDir, self.rootDir, cb,
                                       isPre = False, scriptId = 'sn')
            assert(rc == 0)
            self.failUnlessEqual(cb.output,
                [ ('sn', 'ROOT=%s\n' % self.rootDir), ('sn', 'GOOD\n') ])
            assert(cb.errcode is None)
            cb.reset()
            scriptOut = [ x.strip()[23:] for x in file(conaryLog).readlines() ]
            self.failUnlessEqual(scriptOut, ['running script sn',
                '[sn] ROOT=%s' %self.rootDir,
                '[sn] GOOD',
                'script sn finished'])
            file(conaryLog, 'w') # truncate

            rootDir = self.rootDir + '/'
            rc = update.runTroveScript(absJob, goodScript, None,
                                       self.cfg.tmpDir, rootDir, cb,
                                       isPre = False, scriptId = 'sn2')
            assert(rc == 0)
            self.failUnlessEqual(cb.output,
                [ ('sn2', 'ROOT=%s\n' % rootDir), ('sn2', 'GOOD\n') ])
            scriptOut = [ x.strip()[23:] for x in file(conaryLog).readlines() ]
            self.failUnlessEqual(scriptOut, ['running script sn2',
                '[sn2] ROOT=%s/' %self.rootDir,
                '[sn2] GOOD',
                'script sn2 finished'])
            file(conaryLog, 'w') # truncate
            assert(cb.errcode is None)
            cb.reset()

            os.chroot = MockChroot(fail = True)
            rc = update.runTroveScript(absJob, goodScript, None,
                                       self.cfg.tmpDir, self.rootDir, cb,
                                       isPre = False, scriptId = 'sn3')
            assert(rc == 1)
            assert(cb.errcode == ('sn3', 1))
            scriptOut = [ x.strip()[23:] for x in file(conaryLog).readlines() ]
            self.failUnlessEqual(scriptOut, ['running script sn3',
                'script sn3 failed with exit code 1'])
            file(conaryLog, 'w') # truncate
        finally:
            os.chroot = oldRoot
            os.getuid = oldGetuid
            os.execve = oldExecve
            log.syslog = oldSysLog

    @testhelp.context('trovescripts')
    def testTroveScriptExecutionOrder(self):
        # CNY-2570
        runInfo = []
        self.mock(update, 'runTroveScript',
            lambda *args, **kwargs: self.mockRunTroveScript(
                                                collectList = runInfo, *args,
                                                **kwargs) )

        self.addComponent('foo:runtime', '1', filePrimer=10)
        self.addCollection('foo', '1', [ ('foo:runtime', '1' ) ],
                           preInstallScript = 'preinstall',
                           postInstallScript = 'postinstall',
                           preEraseScript = 'preerase',
                           postEraseScript = 'posterase')

        self.addComponent('bar:runtime', '1', fileContents =
                          [("/usr/share/bar", "contents bar 1", None,
                            deps.parseDep("trove: foo"))])
        self.addCollection('bar', '1', [ ('bar:runtime', '1' ) ],
                           preInstallScript = 'preinstall',
                           postInstallScript = 'postinstall',
                           preEraseScript = 'preerase',
                           postEraseScript = 'posterase')

        del runInfo[:]
        self.updatePkg(['bar', 'foo'])
        for bucket in ['preinstall', 'postinstall']:
            trvNames = [ x['job'][0] for x in runInfo
                         if x['scriptId'].endswith(bucket) ]
            self.failUnlessEqual(trvNames, ['foo', 'bar'])

        self.addComponent('foo:runtime', '2', fileContents =
                          [("/usr/share/foo", "contents foo 2", None,
                             (None, deps.parseDep("soname: ELF32/foo(a)")))])
        self.addCollection('foo', '2', [ ('foo:runtime', '2' ) ],
                           preUpdateScript = 'preupdate-2',
                           postUpdateScript = 'postupdate-2',
                           preEraseScript = 'preerase-2',
                           postEraseScript = 'posterase-2')

        self.addComponent('bar:runtime', '2', fileContents =
                          [("/usr/share/bar", "contents bar 2", None,
                            deps.parseDep("soname: ELF32/foo(a)"))])
        self.addCollection('bar', '2', [ ('bar:runtime', '2' ) ],
                           preUpdateScript = 'preupdate-2',
                           postUpdateScript = 'postupdate-2',
                           preEraseScript = 'preerase-2',
                           postEraseScript = 'posterase-2')

        del runInfo[:]
        self.updatePkg(['bar', 'foo'])
        for bucket in ['preupdate', 'postupdate', 'preerase', 'posterase']:
            trvNames = [ x['job'][0] for x in runInfo
                         if x['scriptId'].endswith(bucket) ]
            if bucket in ['preerase', 'posterase']:
                self.failUnlessEqual(trvNames, [])
            else:
                self.failUnlessEqual(trvNames, ['foo', 'bar'])

        raise testhelp.SkipTestException("Erasure script ordering not working yet")

        del runInfo[:]
        self.updatePkg(['-foo', '-bar'])
        for bucket in ['preerase', 'posterase']:
            trvNames = [ x['job'][0] for x in runInfo
                         if x['scriptId'].endswith(bucket) ]
            self.failUnlessEqual(trvNames, ['foo', 'bar'])


    def testTagScriptRelativePath(self):
        # CNY-2523
        tagScript = util.joinPaths(self.workDir, 'tag-script')

        # Make sure self.rootDir is not part of the script path

        job = ['some-trove', (None, '1'), ('', '')]
        update.runTroveScript(job, script = 'blah', tagScript = tagScript,
            tmpDir = '/', root = self.rootDir, callback = None,
            isPre = False)
        lines = file(tagScript).readlines()
        for line in lines:
            self.failIf(self.rootDir in line, "%s in %s" % (self.rootDir,
                repr(line)))

    def testLinksWithSharedShas(self):
        # This tests the case where file A needs the shared contents from C,
        # but C is created as a hardlink to B (with B not needing shared
        # contents)
        self.openRepository(1)
        self.addComponent('foo:runtime', '/localhost1@rpl:devel/1.0-1-1',
            fileContents = [
                ( '/b', rephelp.RegularFile(contents = "a", pathId = "2",
                                linkGroup = "\1" * 16) ) ] )

        self.addComponent('foo:runtime', '1.0-1-1',
            fileContents = [
                ( '/a', rephelp.RegularFile(contents = "a", pathId = "1" ) ),
                ( '/b', rephelp.RegularFile(contents = "a", pathId = "2",
                                linkGroup = "\1" * 16,
                                version = '/localhost1@rpl:devel/1.0-1-1' ) ),
                ( '/c', rephelp.RegularFile(contents = "b", pathId = "3" ) ),
                ( '/d', rephelp.RegularFile(contents = "a", pathId = "4",
                                linkGroup = "\1" * 16) ) ] )

        self.updatePkg('foo:runtime=1.0-1-1')
        assert(os.stat(self.rootDir + '/b').st_ino ==
               os.stat(self.rootDir + '/d').st_ino)

    @testhelp.context('rollback')
    def testConfigFilesWithoutNewline(self):
        # CNY-1979
        self.addComponent('foo:runtime', '1.0',
                          fileContents = [ ('/etc/foo', 'first contents\n') ] )
        self.addComponent('foo:runtime', '2.0',
                          fileContents = [ ('/etc/foo', 'second contents\n') ] )

        self.updatePkg('foo:runtime=1.0')
        self.writeFile(self.rootDir + '/etc/foo',
                       'first contents\nnew contents')

        self.updatePkg('foo:runtime=2.0')

        self.verifyFile(self.rootDir + '/etc/foo',
                        'second contents\nnew contents')

        self.rollback(1)

        self.verifyFile(self.rootDir + '/etc/foo',
                        'first contents\nnew contents')

    def testUpdateFileToDirectoryReplaceFiles(self):
        self.addComponent('foo:runtime=1', [('/foo/1', 'hello\n')])
        os.symlink(self.rootDir + '/foo.1', self.rootDir + '/foo')
        try:
            self.updatePkg('foo:runtime')
        except:
            raise testhelp.SkipTestException('CNY-1976 - unowned directory replacing unowned file tracebacks')
        assert(0), 'remove skiptest'

    @conary_test.rpm
    def testUpdateWithNoScript(self):
        recipestr = """
class Test(CapsuleRecipe):
    name = 'scripts'
    version = '1.0'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('scripts-1.0-1.x86_64.rpm')
        r.WarnScriptSharedLibrary(exceptions='scripts-1.0-1.x86_64.rpm/(pre(in|un)|postun)')
"""
        self.callCount = 0

        def shouldNotBeCalled(*args, **kw):
            self.callCount += 1

        self.mock(update.FilesystemJob, 'preapply', shouldNotBeCalled)
        self.mock(update.FilesystemJob, 'runPostTagScripts', shouldNotBeCalled)
        self.mock(update.FilesystemJob, 'runPostScripts', shouldNotBeCalled)
        self.mock(update.FilesystemJob, 'clearPostScripts', shouldNotBeCalled)
        self.mock(database.UpdateJob, 'runPreScripts', shouldNotBeCalled)
        filename = 'scripts-1.0-1.x86_64.rpm'
        pkgname = 'scripts'

        repos = self.openRepository()
        recipename = pkgname + '.recipe'

        origDir = os.getcwd()
        try:
            os.chdir(self.workDir)
            self.newpkg(pkgname)
            os.chdir(pkgname)
            self.writeFile(recipename, recipestr)
            shutil.copyfile(
                resources.get_archive() + '/' + filename,
                filename)
            self.addfile(recipename)
            self.addfile(filename)
            self.commit()
            built, out = self.cookItem(repos, self.cfg, pkgname)
            rc, s = self.captureOutput(self.updatePkg, self.rootDir,
                                         [pkgname], depCheck=False,
                                         noScripts=True)
        finally:
            os.chdir(origDir)
            self.unmock()
        self.failUnlessEqual(self.callCount, 0)

    def testSignals(self):
        # make sure signals (SIGTERM here) doesn't interrupt w/o cleaning
        # up the journal

        # open the repository first or the repository process will inherit
        # the SIG_IGN we're setting for SIGTERM
        self.openRepository(0)

        signal.signal(signal.SIGTERM, signal.SIG_IGN)

        self.addComponent('foo:runtime', '1')
        self.mock(update.FilesystemJob, 'apply',
                  lambda *args, **kwargs: os.kill(os.getpid(), signal.SIGTERM))
        self.logCheck2(['error: a critical error occured -- reverting '
                        'filesystem changes'], self.updatePkg, 'foo:runtime')

        signal.signal(signal.SIGTERM, signal.SIG_DFL)

    def testDuplicateFiles(self):
        # two components have files with the same pathId, fileId, version,
        # and path. in conary < 1.2.5, the update to bar=2.0, where the
        # file is absolutely unchanged, would cause bar to steal the
        # (not present) file from foo!
        now = 1193581818            # arbitrary, fixed time
        foo = self.addComponent('foo:runtime', '1.0',
                      pathIdSalt = '1',
                      fileContents = [ ('/file',
                          rephelp.RegularFile(contents = '/bar\n',
                                              version = '/localhost@a:b/1.0',
                                              mtime = now) ) ] )

        self.addComponent('bar:runtime', '1.0',
                      pathIdSalt = '1',
                      fileContents = [ ('/file',
                          rephelp.RegularFile(contents = '/bar\n',
                                              version = '/localhost@a:b/1.0',
                                              mtime = now) ) ] )

        self.addComponent('bar:runtime', '2.0',
                      pathIdSalt = '1',
                      fileContents = [ ('/file',
                          rephelp.RegularFile(contents = '/bar\n',
                                              version = '/localhost@a:b/1.0',
                                              mtime = now) ) ] )

        self.updatePkg('foo:runtime=1.0')
        db = self.openDatabase()
        self.updatePkg('bar:runtime=1.0')
        assert(len(list(db.db.iterFindByPath('/file'))) == 2)
        self.updatePkg('bar:runtime=2.0')

        assert(len(list(db.db.iterFindByPath('/file'))) == 2)

        trv = db.getTrove(*foo.getNameVersionFlavor())
        assert(trv.verifyDigests())

    def testReferencedUnneededContent(self):
        # CNY-2595
        self.addComponent('foo:runtime', '1.0', fileContents = [
                ( '/2', rephelp.RegularFile(pathId = '2',
                                            contents = "orig content" ) ) ] )

        self.updatePkg('foo:runtime')
        self.removeFile(self.rootDir, "/2")

        self.addComponent('foo:runtime', '2.0', fileContents = [
                ( '/1', rephelp.RegularFile(pathId = '1',
                                            contents = "new content") ),
                ( '/2', rephelp.RegularFile(pathId = '2',
                                            contents = "new content" ) ) ] )

        self.updatePkg('foo:runtime')
        # Make sure we didn't leave temp files around
        self.failUnlessEqual(sorted(os.listdir(self.rootDir)),
            ['1', 'var'])

    def testUpdateDoesNotWriteToExistingFiles(self):
        # CNY-2596
        realOpen = os.open
        realFileObject = file

        def mockOpen(path, flag, mode = 0777):
            if (path.startswith(self.rootDir) and
                    not path.startswith(self.rootDir + self.cfg.dbPath) and
                    os.path.exists(path)):
                assert(not(flag & (os.O_RDWR | os.O_WRONLY)))

            return realOpen(path, flag, mode)

        class mockFile(file):

            def __init__(fileObj, path, flags = "r", buffering = True):
                if (path.startswith(self.rootDir) and
                        not path.startswith(self.rootDir + self.cfg.dbPath) and
                        os.path.exists(path)):
                    assert(not("a" in flags or "w" in flags))

                file.__init__(fileObj, path, flags, buffering)

        # The update path needs to make files and rename(2) them into
        # place to avoid ETXTBSY. This test currently tests non-config
        # files only.
        self.addComponent('foo:runtime', '1.0', fileContents = [
                            ('/foo', 'contents') ] )
        self.addComponent('foo:runtime', '2.0', fileContents = [
                            ('/foo', 'repos change') ] )

        # While it would be nice to replace the open/file object in
        # sys.modules['__builtin__'], and hence everywhere on the system,
        # doing so turns out to be difficult because ExtendedFile is derived
        # from file, and ends up calling the wrong superclass. This ought
        # to be good enough.
        self.mock(os, 'open', mockOpen)
        import conary.local.update
        self.mock(conary.local.update, 'open', mockFile)
        self.mock(conary.local.update, 'file', mockFile)

        self.updatePkg('foo:runtime=1.0')
        self.updatePkg('foo:runtime=2.0')

        self.resetRoot()
        self.updatePkg('foo:runtime=1.0')

        self.writeFile(self.rootDir + '/foo', 'local change')
        self.cfg.localRollbacks = True
        try:
            self.updatePkg('foo:runtime=2.0', replaceFiles = True)
        finally:
            self.cfg.localRollbacks = False

        self.rollback(1)
        self.verifyFile(self.rootDir + '/foo', 'local change')

    def testSingleLeadingSlashForTaggedFiles(self):
        # CNY-3141, CNY-3142
        tagFile = os.path.join(self.workDir, 'tag-file')
        file(tagFile, "w").write("""
datasource stdin
file /foobar
implements files update
implements files remove
""")
        # This is where the tag handler records the files
        tagResultFile = os.path.join(self.workDir, "tag-results")

        firstUser1 = self.build(recipes.firstTagUserRecipe1, "FirstTagUser",
            returnTrove = ['firsttaguser', 'firsttaguser:runtime'])
        secondUser1 = self.build(recipes.secondTagUserRecipe1, "SecondTagUser",
            returnTrove = ['secondtaguser', 'secondtaguser:runtime'])

        # Install this, so we can exercise the removal part
        self.updatePkg(self.rootDir, 'secondtaguser')

        repos = self.openRepository()
        csList = [
            (x.getName(), (None, None), (x.getVersion(), x.getFlavor()), True)
             for x in firstUser1 ]
        cs = repos.createChangeSet(csList, withFiles = True)

        # These troves get removed
        for trv in secondUser1:
            cs.oldTrove(trv.getName(), trv.getVersion(), trv.getFlavor())

        fsTroveDict = {}
        fsTroveDict = dict((x.getName(), x) for x in firstUser1)
        fsTroveDict.update(dict((x.getName(), x) for x in secondUser1))
        root = '/'

        client = self.getConaryClient()
        fsJob = update.FilesystemJob(client.db, cs, fsTroveDict, root)

        # Start mocking things
        self.mock(os, "getuid", lambda: 0)
        def mockExecve(cmd0, cmd, env):
            f = file(tagResultFile, "a")
            import sys
            for line in sys.stdin:
                f.write("%s %s" % (' '.join(cmd), line))
            f.close()
            os._exit(0)
        self.mock(os, "execve", mockExecve)

        tagSet = dict(testtag = tags.TagFile(tagFile, {}))
        fsJob.runPostTagScripts(tagSet = tagSet)

        expected = [
            "/foobar files update /etc/testfirst.1",
            "/foobar files update /etc/testfirst.2",
            "/foobar files remove /etc/testsecond.1",
        ]
        self.failUnlessEqual([ x.rstrip() for x in file(tagResultFile) ],
            expected)

    def testSingleLeadingSlashArgs(self):
        # CNY-3141, CNY-3142
        tagFile = os.path.join(self.workDir, 'tag-file')
        file(tagFile, "w").write("""
file /foobar
implements files update
implements files remove
""")
        # This is where the tag handler records the files
        tagResultFile = os.path.join(self.workDir, "tag-results")

        # Add a file that gets patched
        recipe = recipes.firstTagUserRecipe1 + r"""
        r.Create("/etc/blahblah", contents = "a\nb\nc\n")
"""
        firstUser1 = self.build(recipe, "FirstTagUser",
            returnTrove = ['firsttaguser', 'firsttaguser:runtime'])
        recipe = recipes.secondTagUserRecipe1
        secondUser1 = self.build(recipe, "SecondTagUser",
            returnTrove = ['secondtaguser', 'secondtaguser:runtime'])

        # Install this, so we can exercise the removal part
        self.updatePkg(self.rootDir, ['secondtaguser', 'firsttaguser'])

        # Rebuild firsttaguser, with no blahblah
        recipe = recipes.firstTagUserRecipe1 + r"""
        r.Create("/etc/blahblah", contents = "a\nb\nc\nd\ne\nf\n")
"""
        recipe = recipe.replace('\nfirst.1', '\nfirst.11')
        recipe = recipe.replace('\nfirst.2', '\nfirst.22')
        firstUser2 = self.build(recipe, "FirstTagUser",
            returnTrove = ['firsttaguser', 'firsttaguser:runtime'])

        client = self.getConaryClient()

        csList = [
            (t1.getName(), (t0.getVersion(), t0.getFlavor()),
                (t1.getVersion(), t1.getFlavor()), False)
             for (t0, t1) in zip(firstUser1, firstUser2) ]
        csList.extend((x.getName(),
            (x.getVersion(), x.getFlavor()), (None, None), False)
            for x in secondUser1 )
        cs = client.createChangeSet(csList, withFiles = True)

        flags = update.UpdateFlags(missingFilesOkay = True)
        troveList = [
            (client.db.getTrove(
                trv.getName(), trv.getVersion(), trv.getFlavor(),
                pristine = False),
             trv,
             trv.getVersion().createShadow(versions.LocalLabel()), flags)
            for trv in firstUser1 ]
        troveList.extend(
            (client.db.getTrove(
                trv.getName(), trv.getVersion(), trv.getFlavor(),
                pristine = False),
             trv,
             trv.getVersion().createShadow(versions.RollbackLabel()), flags)
            for trv in secondUser1)

        root = '/'
        result = update.buildLocalChanges(client.db, troveList, root = self.rootDir)
        retList = result[1]
        fsTroveDict = {}
        for (changed, fsTrove) in retList:
            fsTroveDict[fsTrove.getNameVersionFlavor()] = fsTrove

        # Start mocking things
        from conary import files
        origFileFromFilesystem = files.FileFromFilesystem
        def FakeFileFromFilesystem(path, *args, **kwargs):
            self.failIf(path.startswith('//'), path)
            return origFileFromFilesystem(self.rootDir + path, *args, **kwargs)
        self.mock(files, 'FileFromFilesystem', FakeFileFromFilesystem)

        fsJob = update.FilesystemJob(client.db, cs, fsTroveDict, root)

        self.mock(os, "getuid", lambda: 0)
        def mockExecve(cmd0, cmd, env):
            f = file(tagResultFile, "a")
            f.write("%s\n" % (' '.join(cmd)))
            f.close()
            os._exit(0)
        self.mock(os, "execve", mockExecve)

        tagSet = dict(testtag = tags.TagFile(tagFile, {}))
        fsJob.runPostTagScripts(tagSet = tagSet)

        expected = [
            "/foobar files update /etc/testfirst.1 /etc/testfirst.2",
            "/foobar files remove /etc/testsecond.1",
        ]
        self.failUnlessEqual([ x.rstrip() for x in file(tagResultFile) ],
            expected)

        for fileNames in fsJob.tagUpdates.values():
            for fileName in fileNames:
                self.failIf(fileName.startswith("//"), fileName)
        for fileNames in fsJob.tagRemoves.values():
            for fileName in fileNames:
                self.failIf(fileName.startswith("//"), fileName)
        for fileName in fsJob.restores:
            self.failIf(fileName.startswith("//"), fileName)

    def _testTroveScriptExecutionOrderInit(self, prefix=''):
        # CNY-2705
        runInfo = []
        self.mock(update, 'runTroveScript',
            lambda *args, **kwargs: self.mockRunTroveScript(
                                                collectList = runInfo, *args,
                                                **kwargs) )

        self.addComponent('foo:runtime', '1', filePrimer=10)
        self.addCollection('%sfoo' % prefix, '1', [ ('foo:runtime', '1' ) ],
                           preInstallScript = 'preinstall',
                           postInstallScript = 'postinstall',
                           preEraseScript = 'preerase',
                           postEraseScript = 'posterase')

        self.addComponent('bar:runtime', '1', filePrimer=11)
        self.addCollection('%sbar' % prefix, '1', [ ('bar:runtime', '1' ) ],
                           preInstallScript = 'preinstall',
                           postInstallScript = 'postinstall',
                           preEraseScript = 'preerase',
                           postEraseScript = 'posterase')

        def formatVF(t):
            version, flavor = t
            if version is None:
                return (None, None)
            return str(version), str(flavor)

        origCommitChangeSet = database.Database.commitChangeSet
        def mockCommitChangeSet(slf, cs, *args, **kwargs):
            riData = []
            for trvcs in cs.iterNewTroveList():
                job = trvcs.getJob()
                riData.append((job[0], formatVF(job[1]),
                               formatVF(job[2]), job[3]))
            runInfo.append(dict(scriptId = ('jobs', riData)))
            origCommitChangeSet(slf, cs, *args, **kwargs)

        self.mock(database.Database, "commitChangeSet", mockCommitChangeSet)

        return runInfo

    def testTroveScriptExecutionOrder2(self):
        # CNY-2705
        runInfo = self._testTroveScriptExecutionOrderInit(prefix='group-')

        del runInfo[:]
        self.updatePkg(['group-bar', 'group-foo'])
        self.failUnlessEqual([ x['scriptId'] for x in runInfo ],
            ['group-bar preinstall',
            ('jobs', [('bar:runtime', (None, None),
                ('/localhost@rpl:linux/1-1-1', ''), False)]),
            ('jobs', [('group-bar', (None, None),
                ('/localhost@rpl:linux/1-1-1', ''), False)]),
            'group-bar postinstall',
            'group-foo preinstall',
            ('jobs', [('foo:runtime', (None, None),
                ('/localhost@rpl:linux/1-1-1', ''), False)]),
            ('jobs', [('group-foo', (None, None),
                ('/localhost@rpl:linux/1-1-1', ''), False)]),
            'group-foo postinstall',
            ])

        util.rmtree(self.rootDir)

        # Install packages, old-style (pre-scripts ordering missing)
        # Pre scripts are all executed at the beginning.
        self.mock(database.UpdateJob, "hasJobPreScriptsOrder", lambda x: False)

        del runInfo[:]
        self.updatePkg(['group-bar', 'group-foo'])
        self.failUnlessEqual([ x['scriptId'] for x in runInfo ],
            ['group-bar preinstall',
            'group-foo preinstall',
            ('jobs', [('bar:runtime', (None, None),
                ('/localhost@rpl:linux/1-1-1', ''), False)]),
            ('jobs', [('group-bar', (None, None),
                ('/localhost@rpl:linux/1-1-1', ''), False)]),
            'group-bar postinstall',
            ('jobs', [('foo:runtime', (None, None),
                ('/localhost@rpl:linux/1-1-1', ''), False)]),
            ('jobs', [('group-foo', (None, None),
                ('/localhost@rpl:linux/1-1-1', ''), False)]),
            'group-foo postinstall',
            ])


    def testTroveScriptExecutionOrder3(self):
        # CNY-2705
        runInfo = self._testTroveScriptExecutionOrderInit(prefix='')

        del runInfo[:]
        self.updatePkg(['bar', 'foo'])

        # This is not right, it's here just for documentation purposes. We
        # will have to change the ordering to match what rpms expect
        # We'd have to force the installation and execution of pre/post
        # scripts for foo _after_ installation of bar, which works fine for
        # groups
        self.failUnlessEqual([ x['scriptId'] for x in runInfo ],
            ['bar preinstall',
            'foo preinstall',
            ('jobs', [
                ('bar:runtime', (None, None),
                    ('/localhost@rpl:linux/1-1-1', ''), False),
                ('bar', (None, None),
                    ('/localhost@rpl:linux/1-1-1', ''), False),
                ('foo:runtime', (None, None),
                    ('/localhost@rpl:linux/1-1-1', ''), False),
                ('foo', (None, None),
                    ('/localhost@rpl:linux/1-1-1', ''), False),
            ]),
            'bar postinstall',
            'foo postinstall',
            ])

        util.rmtree(self.rootDir)

    def testDirectoryChangesVersion(self):
        # CNY-3202
        # Directory with version change, but same pathId/fileId

        pathId = ''.join("%02x" % x for x in range(16))

        cfiles = conaryclient.filetypes
        # Force a directory to have the same fileId but a different version
        self.addComponent('foo:runtime', 'localhost@linux:1', fileContents = [
            ('/usr/share', cfiles.Directory()) ])
        self.addComponent('foo:runtime', 'localhost@linux:2', fileContents = [
            ('/usr/share', cfiles.Directory()) ])

        self.updatePkg('foo:runtime=localhost@linux:1')

        # Mock _restore. We know there is only one file (directory) that
        # changes version, but not contents/permissions/etc.
        orig_restore = update.FilesystemJob._restore
        def mockedRestore(*args, **kwargs):
            self._restoreFile = kwargs['restoreFile']
            return orig_restore(*args, **kwargs)
        self.mock(update.FilesystemJob, '_restore', mockedRestore)
        self.updatePkg('foo:runtime=localhost@linux:2')
        self.failIf(self._restoreFile)

    def testPtrFileChangesContent(self):
        self.addComponent('foo:runtime', fileContents = [
            ('/usr/file1', 'Some content'),
            ('/usr/file2', 'Some content'),
            ('/usr/file3', filetypes.CharacterDevice(1, 5)), # CNY-3208
        ])
        origRestore = update.files.RegularFile.restore
        self._count = 0
        def mockedRestore(slf, fileContents, root, target, *args, **kwargs):
            self._count += 1
            ret = origRestore(slf, fileContents, root, target, *args, **kwargs)
            if self._count == 1:
                # Mess up the file on disk
                file(target, "w").write("Other content")
            return ret
        self.mock(update.files.RegularFile, 'restore', mockedRestore)
        self.updatePkg('foo:runtime')
        # Make sure we didn't leave any junk around. Note that file3 is a
        # character device and did not get restored due to lack of permissions.
        self.failUnlessEqual(sorted(os.listdir("%s/usr" % self.rootDir)),
            ['file1', 'file2'])

    def testVerifyCircularDependency(self):
        # CNY-3352
        trv = self.addComponent("foo:runtime")

        repos = self.openRepository()
        csList = [
            (trv.getName(), (None, None),
            (trv.getVersion(), trv.getFlavor()), True) ]
        cs = repos.createChangeSet(csList, withFiles = True)

        fsTroveDict = {}
        fsTroveDict = dict([(trv.getName(), trv)])
        root = '/'

        client = self.getConaryClient()
        fsJob = update.FilesystemJob(client.db, cs, fsTroveDict, root)

        # This class will count how many times the object has been freed.
        class DelCounter(object):
            counter = 0
            def __del__(slf):
                DelCounter.counter += 1
        fsJob._counter = DelCounter()
        del fsJob
        self.failUnlessEqual(DelCounter.counter, 1)
