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


from testrunner.testhelp import context

import os
import shlex
import sys
import subprocess
import tempfile

from testutils import mock

from conary_test import rephelp
from conary_test import resources

from conary import constants, errors
from conary import cvc as cvccmd # test legacy import location
from conary.cmds import conarycmd
from conary.cmds import queryrep
from conary.deps import deps
from conary.lib import cfg, log, options, util
from conary.build import cook
from conary.conaryclient import cml, systemmodel
from conary.repository import trovesource
from conary import versions


class CmdLineTest(rephelp.RepositoryHelper):

    def setUp(self):
        self.conaryMod = conarycmd
        self.cvcMod = cvccmd
        self.skipDefaultConfig=True
        log.resetErrorOccurred()
        rephelp.RepositoryHelper.setUp(self)

    def _prepCmd(self, cmd):
        if self.skipDefaultConfig:
            cmd += ' --skip-default-config'
        cmd += ' --root=%s' % self.rootDir
        configPath = '%s/conaryrc' % self.rootDir
        if os.path.exists(configPath):
            cmd += ' --config-file=%s' % configPath
        return cmd

    def checkConary(self, cmd, fn, expectedArgs, cfgValues={},
                    returnVal=None, ignoreKeywords=False, **expectedKw):
        cmd = self._prepCmd(cmd)
        return self.checkCommand(self.conaryMod.main, 'conary ' + cmd, fn,
                                 expectedArgs, cfgValues, returnVal,
                                 ignoreKeywords, **expectedKw)

    def checkCvc(self, cmd, fn, expectedArgs, cfgValues={},
                 returnVal=None, ignoreKeywords=False, **expectedKw):
        cmd = self._prepCmd(cmd)
        return self.checkCommand(self.cvcMod.main, 'cvc ' + cmd, fn,
                                 expectedArgs, cfgValues, returnVal,
                                 ignoreKeywords, **expectedKw)


    def testVersion(self):
        assert( constants.version )
        assert( constants.changeset )

    def testConaryNoArgs(self):
        rc, txt = self.captureOutput(self.conaryMod.main, ['conary'])
        assert(rc == 1)
        assert(txt.startswith('Conary Software Configuration Management System'))

    def testBasicConary(self):
        self.writeFile('%s/conaryrc' % self.rootDir,
                       'installLabelPath %s' % self.cfg.buildLabel)

        self.checkConary('update test',
                         'conary.cmds.updatecmd.doUpdate',
                         [None, ['test']], ignoreKeywords=True,
                         updateByDefault=True)

        self.checkConary('erase test',
                         'conary.cmds.updatecmd.doUpdate',
                         [None, ['test']], ignoreKeywords=True,
                         noRestart=True, updateByDefault=False)

        self.checkConary('update test --info', 'conary.cmds.updatecmd.doUpdate',
                         [None, ['test']], info=True,
                         ignoreKeywords=True,
                         cfgValues={'fullVersions'   :False,
                                    'fullFlavors'    :False,
                                    'showComponents' :False,
                                    'root': self.rootDir})

        self.checkConary('update test --disconnected',
                         'conary.cmds.updatecmd.doUpdate',
                         [None, ['test']], disconnected = True,
                         ignoreKeywords=True)

        # basic --no-restart tests. note that the --root is *always* something
        # other than / to make the test suite work
        self.checkConary(
                'update test --root /foo',
                'conary.cmds.updatecmd.doUpdate',
                [None, ['test']], noRestart = True, ignoreKeywords=True)

        self.checkConary(
                'update test --no-restart --root /foo',
                'conary.cmds.updatecmd.doUpdate',
                [None, ['test']], noRestart = True, ignoreKeywords=True)

        self.checkConary(
                'update test --root /foo',
                'conary.cmds.updatecmd.doUpdate',
                [None, ['test']], noRestart = True, ignoreKeywords=True)

        self.checkConary(
                'update test --restart --root /foo',
                'conary.cmds.updatecmd.doUpdate',
                [None, ['test']], noRestart = True, ignoreKeywords=True)

        self.checkConary(
                'updateall --root /foo',
                'conary.cmds.updatecmd.updateAll',
                [None ], noRestart = True, ignoreKeywords=True)

        self.checkConary(
                'updateall --no-restart --root /foo',
                'conary.cmds.updatecmd.updateAll',
                [None ], noRestart = True, ignoreKeywords=True)

        self.checkConary(
                'updateall --restart --root /foo',
                'conary.cmds.updatecmd.updateAll',
                [None ], noRestart = True, ignoreKeywords=True)

        self.checkConary(
                'updateall --root /foo',
                'conary.cmds.updatecmd.updateAll',
                [None ], noRestart = True, ignoreKeywords=True)

        # make sure cfgMap items are working
        self.checkConary(
                  'update test --info --flavors --full-versions --components'
                  ' --apply-critical --restart-info=/tmp/foo --exact-flavors',
                  'conary.cmds.updatecmd.doUpdate',
                  [None, ['test']], info=True, applyCriticalOnly=True,
                  exactFlavors=True,
                  ignoreKeywords=True, restartInfo='/tmp/foo',
                  cfgValues={'fullVersions'   :True,
                             'fullFlavors'    :True,
                             'showComponents' :True})

        # Check --no-restart
        self.checkConary(
                  'update test --no-restart --config "root %s/foo"' % self.rootDir,
                  'conary.cmds.updatecmd.doUpdate',
                  [None, ['test']], noRestart=True, exactFlavors=False,
                  ignoreKeywords=True,
                  cfgValues={'root' : '%s/foo' % self.rootDir, })

        # make sure --config overrides
        self.checkConary('updateall --config "root %s/foo" --apply-critical '
                                    '--restart-info=/tmp/foo' % self.rootDir,
                         'conary.cmds.updatecmd.updateAll', [None],
                         checkPathConflicts=True,
                         applyCriticalOnly=True, restartInfo='/tmp/foo',
                         noRestart=True,
                         replaceManagedFiles = False,
                         replaceUnmanagedFiles = False,
                         replaceModifiedFiles = False,
                         replaceModifiedConfigFiles = False,
                         justDatabase = False,
                         systemModel=False, model=False,
                         modelGraph=None,
                         modelTrace=None,
                         cfgValues={'root': '%s/foo' % self.rootDir,
                                    'keepRequired' : False},)

        self.checkConary('updateall --no-conflict-check --keep-required',
                         'conary.cmds.updatecmd.updateAll', [None],
                          checkPathConflicts=False,
                          ignoreKeywords=True,
                          systemModel=False, model=False,
                          modelGraph=None,
                          cfgValues={'keepRequired' : True})

        # Check --no-restart
        self.checkConary( 'updateall --no-restart --config "root %s/foo"' %
                                                                self.rootDir,
                          'conary.cmds.updatecmd.updateAll', [None],
                          noRestart=True, ignoreKeywords=True,
                          systemModel=False, model=False,
                          modelGraph=None,
                          cfgValues={'root' : '%s/foo' % self.rootDir,
                                     'keepRequired' : False})

        # Check various rdiff options
        flagmap = [
            ('all-troves', 'showAllTroves'),
            ('buildreqs', 'showBuildReqs'),
            'capsules',
            'deps',
            ('diff', 'asDiff'),
            ('diff-binaries', 'diffBinaries'),
            ('exact-flavors', 'exactFlavors'),
            ('file-deps', 'fileDeps'),
            ('file-flavors', 'fileFlavors'),
            ('file-versions', 'fileVersions'),
            'ids',
            'info',
            'ls',
            'lsl',
            'recurse',
            'sha1s',
            ('show-changes', 'showChanges'),
            ('signatures', 'digSigs'),
            'tags',
            ('trove-flags', 'showTroveFlags'),
            ('trove-headers', 'alwaysDisplayHeaders'),
            ('troves', 'showTroves'),
            ('weak-refs', 'weakRefs'),
        ]
        flagmap = [isinstance(x, basestring) and (x, x) or x for x in flagmap]
        kworig = dict((x[1], False) for x in flagmap)
        kworig['recurse'] = None
        cfgmap = [
            ('labels', 'showLabels'),
            ('flavors', 'fullFlavors'),
            ('full-versions', 'fullVersions'),
            ]
        cfgorig = dict((x[1], False) for x in cfgmap)

        flv1 = deps.parseFlavor('is: x')
        flv2 = deps.parseFlavor('is: y')
        cmdline = 'rdiff foo=1[is:x]--2[is:y]'
        expArg = 'foo=1[is:x]--2[is:y]'
        self.checkConary(cmdline, 'conary.cmds.queryrep.rdiffCommand',
                [None, None, None, expArg], cfgValues=cfgorig, **kworig)

        for flag, option in flagmap:
            kw = kworig.copy()
            kw[option] = True
            self.checkConary(cmdline + ' --' + flag,
                'conary.cmds.queryrep.rdiffCommand',
                [None, None, None, expArg], cfgValues=cfgorig, **kw)
        for flag, option in cfgmap:
            newcfg = cfgorig.copy()
            newcfg[option] = True
            self.checkConary(cmdline + ' --' + flag,
                'conary.cmds.queryrep.rdiffCommand',
                [None, None, None, expArg], cfgValues=newcfg, **kworig)

        # make sure we can specify generic flags before the command
        self.checkConary(
                  '--flavors --full-versions --components update test --info ',
                  'conary.cmds.updatecmd.doUpdate',
                  [None, ['test']], info=True,
                  ignoreKeywords=True,
                  cfgValues={'fullVersions'   :True,
                             'fullFlavors'    :True,
                             'showComponents' :True})

        # make sure we can specify generic flags before the command
        self.checkConary(
                  '-c "fullVersions True" -r /tmp/foo update -i test',
                  'conary.cmds.updatecmd.doUpdate',
                  [None, ['test']], info=True,
                  ignoreKeywords=True,
                  cfgValues={'fullVersions'   :True,
                             #'root'           :'/tmp/foo'
                             # can't test root - its overridden higher up.
                             })

    @context('sysmodel')
    def testModelUpdate(self):
        self.writeFile('%s/conaryrc' % self.rootDir,
                       'installLabelPath %s' % self.cfg.buildLabel)
        util.mkdirChain('%s/etc/conary' % self.rootDir)
        self.writeFile('%s/etc/conary/system-model' % self.rootDir, '')
        sysmodel = mock.MockObject()
        modelFile = mock.MockObject()
        self.mock(cml, 'CML', sysmodel)
        self.mock(systemmodel, 'SystemModelFile', modelFile)

        sysmodelO = sysmodel(cfg)
        modelFileO = modelFile(sysmodelO)
        modelFileO.snapshotExists._mock.setDefaultReturn(False)

        self.checkConary('sync',
                         'conary.cmds.updatecmd.doModelUpdate',
                         [None, sysmodelO, modelFileO, []], {},
                         ignoreKeywords=True)

        self.checkConary('update foo',
                         'conary.cmds.updatecmd.doModelUpdate',
                         [None, sysmodelO, modelFileO, ['foo']], {},
                         model=False,
                         keepExisting=False,
                         ignoreKeywords=True)

        self.checkConary('update foo --keep-existing',
                         'conary.cmds.updatecmd.doModelUpdate',
                         [None, sysmodelO, modelFileO, ['foo']], {},
                         model=False,
                         keepExisting=True,
                         ignoreKeywords=True)

        self.checkConary('install foo',
                         'conary.cmds.updatecmd.doModelUpdate',
                         [None, sysmodelO, modelFileO, ['foo']], {},
                         model=False,
                         keepExisting=True,
                         ignoreKeywords=True)

        self.checkConary('update foo --model',
                         'conary.cmds.updatecmd.doModelUpdate',
                         [None, sysmodelO, modelFileO, ['foo']], {},
                         model=True,
                         ignoreKeywords=True)

        self.checkConary('erase foo',
                         'conary.cmds.updatecmd.doModelUpdate',
                         [None, sysmodelO, modelFileO, ['foo']], {},
                         model=False,
                         ignoreKeywords=True)

        self.checkConary('patch group-errata',
                         'conary.cmds.updatecmd.doModelUpdate',
                         [None, sysmodelO, modelFileO, []], {},
                         model=False,
                         ignoreKeywords=True,
                         patchSpec=['group-errata'])

        self.checkConary('update -foo --model',
                         'conary.cmds.updatecmd.doModelUpdate',
                         [None, sysmodelO, modelFileO, ['-foo']], {},
                         model=True,
                         ignoreKeywords=True)

        self.checkConary('migrate asdf',
                         'conary.lib.log.error',
                         ['The "migrate" command does not function'
                          ' with a system model'])

        self.checkConary('migrate',
                         'conary.lib.log.error',
                         ['The "migrate" command does not function'
                          ' with a system model'])

        self.checkConary('sync asdf',
                         'conary.lib.log.error',
                         ['The "sync" command cannot take trove arguments'
                          ' with a system model'])

        self.checkConary('updateall',
                         'conary.cmds.updatecmd.updateAll', [None],
                          systemModel=sysmodelO,
                          systemModelFile=modelFileO,
                          model=False,
                          ignoreKeywords=True)

        self.checkConary('updateall --model',
                         'conary.cmds.updatecmd.updateAll', [None],
                          systemModel=sysmodelO,
                          systemModelFile=modelFileO,
                          model=True,
                          ignoreKeywords=True)

        # make sure that --ignore-model is honored for important commands
        self.checkConary('update foo --ignore-model',
                         'conary.cmds.updatecmd.doUpdate',
                         [None, ['foo']], {},
                         ignoreKeywords=True)

        self.checkConary('install foo --ignore-model',
                         'conary.cmds.updatecmd.doUpdate',
                         [None, ['foo']], {},
                         ignoreKeywords=True)

        self.checkConary('erase foo --ignore-model',
                         'conary.cmds.updatecmd.doUpdate',
                         [None, ['foo']], {},
                         ignoreKeywords=True)

    @context('sysmodel')
    def testModelUpdateBlockedBySnapshot(self):
        self.writeFile('%s/conaryrc' % self.rootDir,
                       'installLabelPath %s' % self.cfg.buildLabel)
        util.mkdirChain('%s/etc/conary' % self.rootDir)
        self.writeFile('%s/etc/conary/system-model' % self.rootDir, '')
        self.writeFile('%s/etc/conary/system-model.next' % self.rootDir, '')
        sysmodel = mock.MockObject()
        modelFile = mock.MockObject()
        self.mock(cml, 'CML', sysmodel)
        self.mock(systemmodel, 'SystemModelFile', modelFile)

        sysmodelO = sysmodel(cfg)
        modelFileO = modelFile(sysmodelO)

        self.checkConary('install foo',
                         'conary.lib.log.error',
                         ['The previous update was aborted;'
                          ' resume with "conary sync" or'
                          ' revert with "conary rollback 1"',]
                         )

        self.checkConary('update foo',
                         'conary.lib.log.error',
                         ['The previous update was aborted;'
                          ' resume with "conary sync" or'
                          ' revert with "conary rollback 1"',]
                         )

        self.checkConary('updateall',
                         'conary.lib.log.error',
                         ['The previous update was aborted;'
                          ' resume with "conary sync" or'
                          ' revert with "conary rollback 1"',]
                         )

        self.checkConary('sync',
                         'conary.cmds.updatecmd.doModelUpdate',
                         [None, sysmodelO, modelFileO, []], {},
                         ignoreKeywords=True)

        # rollback is tested in updatetest.py:testModelUpdateFailResume
        # because snapshot model handling is not in the command line
        # handling there

    @context('sysmodel')
    def testModelSearch(self):
        self.checkConary('search foo',
                         'conary.cmds.search.search',
                         [ None, [ "foo" ] ])

    def testSync(self):
        self.writeFile('%s/conaryrc' % self.rootDir,
                       'installLabelPath %s' % self.cfg.buildLabel)
        self.checkConary(
                  'syncchildren test --update-only --exact-flavors',
                  'conary.cmds.updatecmd.doUpdate',
                  [None, ['test']], syncChildren=True, updateOnly=True,
                  syncUpdate=False, removeNotByDefault=False,
                  applyCriticalOnly=False, restartInfo=None,
                  ignoreKeywords=True, exactFlavors=True)
        self.checkConary(
                  'syncchildren test --update-only --apply-critical '
                  '--restart-info=/tmp/foo',
                  'conary.cmds.updatecmd.doUpdate',
                  [None, ['test']], syncChildren=True, syncUpdate=False,
                  removeNotByDefault=False, updateOnly=True,
                  applyCriticalOnly=True, restartInfo='/tmp/foo',
                  ignoreKeywords=True)
        self.checkConary(
                  'syncchildren test --full',
                  'conary.cmds.updatecmd.doUpdate',
                  [None, ['test']], syncChildren=True, syncUpdate=False,
                  updateOnly=False, removeNotByDefault=True,
                  ignoreKeywords=True)
        self.checkConary(
                  'sync test --update-only --full',
                  'conary.cmds.updatecmd.doUpdate',
                  [None, ['test']], syncChildren=False, syncUpdate=True,
                  updateOnly=True, removeNotByDefault=True,
                  ignoreKeywords=True)
        self.checkConary(
                  'sync test --update-only --full --current',
                  'conary.cmds.updatecmd.doUpdate',
                  [None, ['test']], syncChildren=True, syncUpdate=False,
                  updateOnly=True, removeNotByDefault=True,
                  ignoreKeywords=True)


    def testMigrate(self):
        self.writeFile('%s/conaryrc' % self.rootDir,
                       'installLabelPath %s' % self.cfg.buildLabel)
        self.checkConary('migrate test',
                         'conary.cmds.updatecmd.doUpdate',
                         [None, ['test']], ignoreKeywords=True,
                         migrate=True, applyCriticalOnly=False,
                         restartInfo=None)
        self.checkConary('migrate test --apply-critical --restart-info=/tmp/foo',
                         'conary.cmds.updatecmd.doUpdate',
                         [None, ['test']], ignoreKeywords=True,
                         migrate=True, applyCriticalOnly=True,
                         restartInfo='/tmp/foo')



    def testConaryChangeset(self):
        self.checkConary('changeset tmpwatch foo bar.ccs --no-recurse',
                         'conary.cmds.cscmd.ChangeSetCommand',
                         [None, ['tmpwatch', 'foo'], 'bar.ccs'], callback=None,
                         recurse=False)
        self.checkConary('changeset tmpwatch foo bar.ccs --quiet',
                         'conary.cmds.cscmd.ChangeSetCommand',
                         [None, ['tmpwatch', 'foo'], 'bar.ccs'],
                         callback=None, recurse=True,
                         cfgValues={'quiet': True})

    def testConaryCommit(self):
        self.checkConary('commit bar.ccs',
                         'conary.cmds.commit.doCommit',
                         [None, 'bar.ccs', None])
        self.checkConary('commit bar.ccs --target-branch /c.r.c@rpl:devel',
                         'conary.cmds.commit.doCommit',
                         [None, 'bar.ccs', '/c.r.c@rpl:devel'])

    def testConaryConfig(self):
        self.checkConary('config',
                         'conary.lib.cfg._Config.display', [None])

    def testContextFlag(self):
        self.writeFile('%s/conaryrc' % self.rootDir,
                       '[foo]\n'
                       'installLabelPath conary.rpath.com@rpl:1\n')
        self.checkConary('--context foo config',
                         'conary.lib.cfg._Config.display', [None],
                         cfgValues={'context':'foo'})



    def testConaryEmerge(self):
        self.checkConary('emerge bar',
                         'conary.build.cook.cookCommand',
                         [None, ['bar'], False, {}], emerge = True,
                         cookIds=None, ignoreDeps=False)

    def testConaryLocalCs(self):
        self.checkConary('localcs pkgname outfile',
                         'conary.cmds.verify.LocalChangeSetCommand',
                         [None, None, 'pkgname'],
                         changeSetPath = 'outfile' )

    def testConaryPin(self):
        self.checkConary('pin tmpwatch foo',
                         'conary.cmds.updatecmd.changePins',
                         [None, ['tmpwatch', 'foo']], pin=True,
                         systemModel=False, systemModelFile=None)
        self.checkConary('unpin tmpwatch foo',
                         'conary.cmds.updatecmd.changePins',
                         [None, ['tmpwatch', 'foo']], pin=False,
                         systemModel=False, systemModelFile=None,
                         callback=None)

    @context('sysmodel')
    def testModelConaryPin(self):
        self.writeFile('%s/conaryrc' % self.rootDir,
                       'installLabelPath %s' % self.cfg.buildLabel)
        util.mkdirChain('%s/etc/conary' % self.rootDir)
        self.writeFile('%s/etc/conary/system-model' % self.rootDir, '')
        sysmodel = mock.MockObject()
        modelFile = mock.MockObject()
        self.mock(cml, 'CML', sysmodel)
        self.mock(systemmodel, 'SystemModelFile', modelFile)

        sysmodelO = sysmodel(cfg)
        modelFileO = modelFile(sysmodelO)

        self.checkConary('pin tmpwatch foo',
                         'conary.cmds.updatecmd.changePins',
                         [None, ['tmpwatch', 'foo']], pin=True,
                         systemModel=False,
                         systemModelFile=None)
        self.checkConary('unpin tmpwatch foo',
                         'conary.cmds.updatecmd.changePins',
                         [None, ['tmpwatch', 'foo']], pin=False,
                         systemModel=sysmodelO,
                         systemModelFile=modelFileO,
                         callback=None)

    def testConaryRemove(self):
        db = self.openDatabase()
        self.addComponent('foo:lib', '1', '', ['/usr/bin/foo'],
                          changeSetFile=self.workDir + '/foo.ccs')
        self.updatePkg([self.workDir + '/foo.ccs'])
        self.checkConary('remove /usr/bin/foo',
                         'conary.local.database.Database.removeFiles',
                         [None, ['/usr/bin/foo']])

    @context('rollback')
    def testConaryRbList(self):
        self.checkConary('rblist', 'conary.cmds.rollbacks.listRollbacks',
                         [None, None])

    @context('rollback', 'fileoverlap')
    def testConaryRollback(self):
        self.checkConary('rollback r.106',
                         'conary.cmds.rollbacks.applyRollback',
                         [None, 'r.106'],
                         ignoreKeywords=True)
        self.checkConary('rb r.106',
                         'conary.cmds.rollbacks.applyRollback',
                         [None, 'r.106'],
                         ignoreKeywords=True)
        self.checkConary('rollback r.106 --tag-script=/foo',
                         'conary.cmds.rollbacks.applyRollback',
                         [None, 'r.106'], tagScript = "/foo",
                         ignoreKeywords=True)
        self.checkConary('rollback r.106 --replace-files',
                         'conary.cmds.rollbacks.applyRollback',
                         [None, 'r.106'], replaceFiles=True,
                         justDatabase = False,
                         ignoreKeywords=True)
        self.checkConary('rollback r.106 --just-db',
                         'conary.cmds.rollbacks.applyRollback',
                         [None, 'r.106'], justDatabase=True,
                         ignoreKeywords=True)
        self.checkConary('rollback r.106 --from-file foo --from-file bar',
                         'conary.cmds.rollbacks.applyRollback',
                         [None, 'r.106'],
                         capsuleChangesets = [ 'foo', 'bar' ],
                         ignoreKeywords=True)

    @context('rollback')
    def testConaryRemoveRollback(self):
        self.checkConary('rmrollback r.106',
                         'conary.cmds.rollbacks.removeRollbacks',
                         [None, 'r.106'])
        self.checkConary('rmrb 106',
                         'conary.cmds.rollbacks.removeRollbacks',
                         [None, '106'])

    def testQuery(self):
        self.checkConary(
                  'q --trove-headers --recurse --no-recurse'
                  ' --deps --info --weak-refs --trove-flags'
                  ' --all-troves --signatures --file-deps --file-flavors'
                  ' --exact-flavors'
                  ' --what-provides trove:foo'
                  ' --what-provides trove:bar',
                  'conary.cmds.query.displayTroves',
                  [None, None, []],
                  alwaysDisplayHeaders=True,
                  recurse=False, showTroveFlags=True,
                  ignoreKeywords=True, weakRefs=True,
                  showAllTroves=True, digSigs=True, info=True,
                  showDeps=True, fileDeps=True, fileFlavors=True,
                  exactFlavors=True,
                  whatProvidesList=['trove:foo', 'trove:bar'])

        self.checkConary(
                  'q --capsules --ls',
                  'conary.cmds.query.displayTroves',
                  [None, None, []],
                  ignoreKeywords = True,
                  recurse=None, ls=True, capsules=True)

    def testRepQuery(self):
        self.writeFile('%s/conaryrc' % self.rootDir,
                       'installLabelPath %s' % self.cfg.buildLabel)
        self.checkConary(
                  ('rq --trove-headers --recurse --no-recurse'
                   ' --deps --info --weak-refs --trove-flags'
                   ' --all-troves --signatures --file-deps --file-flavors'
                   ' --what-provides trove:foo'
                   ' --what-provides trove:bar'
                   ' --show-removed --exact-flavors'),
                  'conary.cmds.queryrep.displayTroves',
                  [None, [], [], ['trove:foo', 'trove:bar'],
                   queryrep.VERSION_FILTER_LATEST,
                   queryrep.FLAVOR_FILTER_EXACT], alwaysDisplayHeaders=True,
                  recurse=False, showTroveFlags=True,
                  ignoreKeywords=True, weakRefs=True,
                  showAllTroves=True, digSigs=True, info=True,
                  fileFlavors=True, showBuildLog=False,
                  showDeps=True, fileDeps=True,
                  troveTypes=trovesource.TROVE_QUERY_ALL)
        self.checkConary(
                  'rq --path /foo --path /bar bam',
                  'conary.cmds.queryrep.displayTroves',
                  [None, ['bam'],
                  ['/foo', '/bar'], [], queryrep.VERSION_FILTER_LATEST,
                   queryrep.FLAVOR_FILTER_BEST], alwaysDisplayHeaders=False,
                  recurse=None, showTroveFlags=False,
                  ignoreKeywords=True, weakRefs=False,
                  showAllTroves=False, digSigs=False, info=False,
                  showDeps=False, fileDeps=False, fileFlavors=False,
                  troveTypes=trovesource.TROVE_QUERY_PRESENT)
        self.checkConary(
                  'rq --ls --capsules',
                  'conary.cmds.queryrep.displayTroves',
                  [None, [], [], [], queryrep.VERSION_FILTER_LATEST,
                   queryrep.FLAVOR_FILTER_BEST],
                  ignoreKeywords=True,
                  recurse=None,
                  ls=True, capsules=True,
                  troveTypes=trovesource.TROVE_QUERY_PRESENT)

    def testBuildLogShowFileError(self):
        rc, s = self.captureOutput(self.conaryMod.main,
                                   ['conary', 'rq', 'foo', '--build-log', '--show-file=/foo/file'])
        assert(s=='error: can\'t use --build-log and --show-file together\n')
        
    def testBuildLogError(self):
        rc, s = self.captureOutput(self.conaryMod.main,
                                   ['conary', 'rq', 'foo', 'bar', '--build-log'])
        assert(s=='Error: can not show build log for several packages. Please specify one\n')
        
    def testBuildLog(self):
        self.checkConary(
            ('rq foo --build-log'),
            'conary.cmds.queryrep.displayTroves',
            [None, ['foo'], [], [], 1, 2], 
            sha1s = False, showAllTroves = False, lsl = False, 
            showBuildLog = True, showBuildReqs = False, 
            showDeps = False, digSigs = False, showTroves = False, 
            ls = False, showTroveFlags = False, filesToShow = [], 
            fileVersions = False, fileDeps = False, tags = False, 
            info = False, useAffinity = False, recurse = None, 
            ids = False, alwaysDisplayHeaders = False, capsules = False,
            fileFlavors = False, troveTypes = 1, weakRefs = False)

    def testShowFile(self):
        self.checkConary(
            ('rq foo --show-file=/foo/file'),
            'conary.cmds.queryrep.displayTroves',
            [None, ['foo'], [], [], 1, 2], 
            sha1s = False, showAllTroves = False, lsl = False, 
            showBuildLog = False, showBuildReqs = False, 
            showDeps = False, digSigs = False, showTroves = False, 
            ls = False, showTroveFlags = False, filesToShow = ['/foo/file'], 
            fileVersions = False, fileDeps = False, tags = False, 
            info = False, useAffinity = False, recurse = None, 
            ids = False, alwaysDisplayHeaders = False, capsules = False,
            fileFlavors = False, troveTypes = 1, weakRefs = False)

    def testShowChangeset(self):
        self.addComponent('foo:run', '1')
        self.changeset(self.openRepository(), ['foo:run'],
                       self.workDir + '/foo.ccs')

        self.checkConary(
                  'showcs %s/foo.ccs foo:runtime --trove-headers --recurse'
                  ' --no-recurse --deps --info --weak-refs --trove-flags'
                  ' --all-troves --signatures --file-deps --file-flavors --exact-flavors' % self.workDir,
                  'conary.cmds.showchangeset.displayChangeSet',
                  [None, None, ['foo:runtime'], None],
                  alwaysDisplayHeaders=True,
                  recurse=False, showTroveFlags=True,
                  ignoreKeywords=True, weakRefs=True,
                  showAllTroves=True, digSigs=True, info=True,
                  fileFlavors=True, deps=True, fileDeps=True,
                  exactFlavors=True, capsules=False)

        self.checkConary(
                  'showcs %s/foo.ccs foo:runtime --capsules --ls'
                      % self.workDir, 'conary.cmds.showchangeset.displayChangeSet',
                  [None, None, ['foo:runtime'], None],
                  ignoreKeywords=True,
                  recurse=None, capsules=True)

        self.checkConary(
                  'showcs %s/foo.ccs foo:runtime --capsules --ls --diff'
                      % self.workDir, 'conary.cmds.showchangeset.displayChangeSet',
                  [None, None, ['foo:runtime'], None],
                  ignoreKeywords=True,
                  recurse=None, capsules=True, asDiff=True)

    def testUpdateConary(self):
        self.writeFile('%s/conaryrc' % self.rootDir,
                       'installLabelPath %s' % self.cfg.buildLabel)
        self.checkConary('updateconary',
                         'conary.cmds.updatecmd.updateConary',
                         [None, constants.version])

    def testVerify(self):
        self.checkConary('verify tmpwatch',
                         'conary.cmds.verify.verify.__init__',
                         [ None, ['tmpwatch'], None, None ], all=False,
                         changesetPath = None, forceHashCheck=False,
                         repos = None, asDiff = False, newFiles=False,
                         diffBinaries=False)
        self.checkConary('verify tmpwatch --changeset=foo.ccs',
                         'conary.cmds.verify.verify.__init__',
                         [ None, ['tmpwatch'], None, None], all=False,
                         changesetPath = 'foo.ccs', forceHashCheck=False,
                         repos = None, asDiff = False, newFiles=False,
                         diffBinaries=False)
        self.checkConary('verify tmpwatch --changeset=foo.ccs --diff',
                         'conary.cmds.verify.verify.__init__',
                         [ None, ['tmpwatch'], None, None], all=False,
                         changesetPath = 'foo.ccs', forceHashCheck=False,
                         repos = None, asDiff = True, newFiles=False,
                         diffBinaries=False)
        self.checkConary('verify tmpwatch --changeset=foo.ccs --new-files',
                         'conary.cmds.verify.verify.__init__',
                         [ None, ['tmpwatch'], None, None], all=False,
                         changesetPath = 'foo.ccs', forceHashCheck=False,
                         repos = None, asDiff = False, newFiles=True,
                         diffBinaries=False)

    def testCvcNoArgs(self):
        rc, txt = self.captureOutput(self.cvcMod.main, ['cvc'])
        assert(rc == 1)
        assert(txt.startswith('Conary Version Control'))

    def testCvcAdd(self):
        self.checkCvc('add foo',
                      'conary.checkin.addFiles',
                      [ [ 'foo' ] ],
                      text = False, binary = False, repos = None,
                      defaultToText = False)

        self.checkCvc('add foo --text',
                      'conary.checkin.addFiles',
                      [ [ 'foo' ] ],
                      text = True, binary = False, repos = None,
                      defaultToText = False)

        self.checkCvc('add foo --binary',
                      'conary.checkin.addFiles',
                      [ [ 'foo' ] ],
                      text = False, binary = True, repos = None,
                      defaultToText = False)

    def testCvcAnnotate(self):
        self.checkCvc(
                'annotate foo.file',
                'conary.checkin.annotate',
                (None,'foo.file') )

    def testCvcDiff(self):
        self.checkCvc('diff foo',
                      'conary.checkin.diff',
                      [None, None, [ 'foo' ]] )

    def testCvcCheckout(self):
        self.checkCvc(
                'co footrove=cooldistro.rpath.org@rpl:devel',
                'conary.checkin.checkout',
                (None, None, None,
                    ['footrove=cooldistro.rpath.org@rpl:devel'], None))

        self.checkCvc(
                'co footrove=cooldistro.rpath.org@rpl:devel blah',
                'conary.checkin.checkout',
                (None, None, None,
                    ['footrove=cooldistro.rpath.org@rpl:devel', 'blah'], None))

        self.checkCvc(
                'checkout footrove=cooldistro.rpath.org@rpl:devel '
                    '--dir bardirectory',
                'conary.checkin.checkout',
                (None, None, 'bardirectory',
                    ['footrove=cooldistro.rpath.org@rpl:devel'], None))

        self.checkCvc(
                'checkout footrove=cooldistro.rpath.org@rpl:devel blah'
                ' --dir bardirectory',
                'conary.cmds.cvccmd.CheckoutCommand.usage',
                (None,))

    @context('clone')
    def testCvcClone(self):
        self.checkCvc('clone /foo.rpath.org@rpl:devel/ '
                            'bar:source=bar.rpath.org@rpl:devel',
                      'conary.cmds.clone.CloneTrove',
                      [ None, '/foo.rpath.org@rpl:devel/',
                            ['bar:source=bar.rpath.org@rpl:devel'], True  ],
                      info = False, cloneSources = False, test=False,
                      message=None, fullRecurse = False)

        self.checkCvc('clone --skip-build-info --info --test -m foo --full-recurse'
                            ' /foo.rpath.org@rpl:devel/ '
                            'bar:source=bar.rpath.org@rpl:devel --with-sources',
                      'conary.cmds.clone.CloneTrove',
                      [ None, '/foo.rpath.org@rpl:devel/',
                            ['bar:source=bar.rpath.org@rpl:devel'], False ],
                      info = True, cloneSources = True, test=True,
                      message='foo', fullRecurse = True)

        # -i
        self.checkCvc('clone --skip-build-info -i --test -m foo --full-recurse'
                            ' /foo.rpath.org@rpl:devel/ '
                            'bar:source=bar.rpath.org@rpl:devel --with-sources',
                      'conary.cmds.clone.CloneTrove',
                      [ None, '/foo.rpath.org@rpl:devel/',
                            ['bar:source=bar.rpath.org@rpl:devel'], False ],
                      info = True, cloneSources = True, test=True,
                      message='foo', fullRecurse = True)

    @context('clone')
    def testCvcPromote(self):
        self.checkCvc('promote group-foo group-bar --exact-flavors'
                            ' conary.rpath.com@rpl:1--conary.rpath.com@rpl:devel'
                            ' foo.rpath.org@rpl:2--:3',
                      'conary.cmds.clone.promoteTroves',
                      [ None, ['group-foo', 'group-bar'],
                            [['conary.rpath.com@rpl:1', 'conary.rpath.com@rpl:devel'],
                             ['foo.rpath.org@rpl:2', ':3']]],
                      info = False, skipBuildInfo=False, message=None,
                      test=False, cloneSources=True, allFlavors=True,
                      cloneOnlyByDefaultTroves=False, targetFile=None,
                      exactFlavors=True, excludeGroups=False)
        self.checkCvc('promote group-foo group-bar'
                        ' conary.rpath.com@rpl:1--conary.rpath.com@rpl:devel'
                        ' foo.rpath.org@rpl:2--:3 --skip-build-info -m "foo" -i --test --with-sources',
                      'conary.cmds.clone.promoteTroves',
                      [ None, ['group-foo', 'group-bar'],
                            [['conary.rpath.com@rpl:1', 'conary.rpath.com@rpl:devel'],
                             ['foo.rpath.org@rpl:2', ':3']]],
                      info = True, skipBuildInfo=True, message="foo", test=True, cloneOnlyByDefaultTroves=False,
                      cloneSources=True, allFlavors=True, targetFile=None,
                      exactFlavors=False, excludeGroups=False)
        self.checkCvc('promote group-foo group-bar --to-file foo.ccs'
                        ' conary.rpath.com@rpl:1--conary.rpath.com@rpl:devel'
                        ' foo.rpath.org@rpl:2--:3 --without-sources --all-flavors --default-only',
                      'conary.cmds.clone.promoteTroves',
                      [ None, ['group-foo', 'group-bar'],
                            [['conary.rpath.com@rpl:1',
                              'conary.rpath.com@rpl:devel'],
                             ['foo.rpath.org@rpl:2', ':3']]],
                      info = False, skipBuildInfo=False, cloneSources=False,
                      test=False, message=None, allFlavors=True,
                      cloneOnlyByDefaultTroves=True, targetFile='foo.ccs',
                      exactFlavors=False, excludeGroups=False)


    def testCvcCommit(self):
        self.checkCvc('commit',
                      'conary.checkin.commit',
                      [ None, None, None,  ],
                      callback = None, test=False)
        self.checkCvc('ci --message "commit message"',
                      'conary.checkin.commit',
                      [ None, None, 'commit message' ],
                      callback = None, test=False)
        self.checkCvc('ci -m "commit message"',
                      'conary.checkin.commit',
                      [ None, None, 'commit message' ],
                      callback = None, test=False)
        self.checkCvc('ci -m "commit message" --test',
                      'conary.checkin.commit',
                      [ None, None, 'commit message' ],
                      callback = None, test=True)
        self.checkCvc('ci -m"commit message" --test',
                      'conary.checkin.commit',
                      [ None, None, 'commit message' ],
                      callback = None, test=True)

        # Need temporary file here
        fd, tmpf = tempfile.mkstemp()
        commitMessage = "some commit message"
        os.write(fd, commitMessage)
        # The library will strip trailing newlines
        os.write(fd, "\n\n\n")
        os.close(fd)

        self.checkCvc('ci -l %s --test' % tmpf,
                      'conary.checkin.commit',
                      [ None, None, commitMessage ],
                      callback = None, test=True)

        # Replace stdin temporarily, to test input from stdin
        oldStdin = sys.stdin
        sys.stdin = open(tmpf)

        self.checkCvc('ci --log-file - --test',
                      'conary.checkin.commit',
                      [ None, None, commitMessage ],
                      callback = None, test=True)

        # replace stdin back
        sys.stdin = oldStdin

        # Make sure --message and --log-file are conflicting
        c = cvccmd.CommitCommand()
        argSet = {
            'message'   : 'nothing',
            'log-file'  : tmpf,
            'test'      : True,
        }
        # Don't use failUnlessRaises, we want to test the exception's content
        # too
        try:
            c.runCommand(None, argSet, ['cvc', 'commit'],
                callback=None, repos=None)
        except errors.ConaryError, e:
            self.failUnlessEqual(e.args[0],
                "options --message and --log-file are "
                "mutually exclusive")
        else:
            self.fail("ConaryError not raised")

        os.unlink(tmpf)

        # Run it again just with the log file - should fail to read the log
        # file
        argSet = {
            'log-file'  : tmpf,
            'test'      : True,
        }

        try:
            c.runCommand(None, argSet, ['cvc', 'commit'],
                callback=None, repos=None)
        except errors.ConaryError, e:
            self.failUnlessEqual(e.args[0],
                "While opening %s: %s" % (tmpf, "No such file or directory"))
        else:
            self.fail("ConaryError not raised")

    def testCvcConfig(self):
        self.checkConary('config -c "fullVersions True" -r /tmp/foo',
                         'conary.lib.cfg._Config.display', [None],
                         cfgValues={'fullVersions'   :True})

    def testCvcContext(self):
        self.checkCvc('context',
                      'conary.checkin.setContext',
                      [ None, None ],
                      ask = False, repos = None)

        self.checkCvc('context foobarbaz',
                      'conary.checkin.setContext',
                      [ None, 'foobarbaz' ],
                      ask = False, repos = None)

        self.checkCvc('context --ask',
                      'conary.checkin.setContext',
                      [ None, None ],
                      ask = True, repos = None)

    def testCvcCook(self):
        self.writeFile('%s/conaryrc' % self.rootDir,
                       'installLabelPath %s' % self.cfg.buildLabel)
        self.checkCvc(
                  'cook test.recipe --no-clean --signature-key ffff',
                  'conary.build.cook.cookCommand',
                  [None, ['test.recipe'], False, None],
                  ignoreKeywords=True,
                  cfgValues={'cleanAfterCook': False,
                             'signatureKey' : 'ffff'},
                  groupOptions=cook.GroupCookOptions(True, True, False))

        self.checkCvc(
                  'cook test.recipe --macro "foo bar" --no-deps --prep --allow-flavor-change --config "ShortenGroupFlavors True"',
                  'conary.build.cook.cookCommand',
                  [None, ['test.recipe'], True, None],
                  ignoreKeywords=True,
                  ignoreDeps=True,
                  groupOptions=cook.GroupCookOptions(True, False, True),
                  cfgValues = {'macros' : {'foo' : 'bar'},
                               'cleanAfterCook' : True})

        # Create macros file
        fd, macrosFile = tempfile.mkstemp(dir=self.workDir)
        os.write(fd, "\n")
        os.write(fd, "foo1 bar1\n")
        os.write(fd, "foo2 bar2\n")
        os.write(fd, "  # foo1 bar2\n")
        os.close(fd)

        self.checkCvc(
                  'cook test.recipe --macros %s --no-deps --prep' % macrosFile,
                  'conary.build.cook.cookCommand',
                  [None, ['test.recipe'], True, None],
                  ignoreKeywords=True,
                  ignoreDeps=True,
                  groupOptions=cook.GroupCookOptions(True, True),
                  cfgValues = {'macros' :
                                    {'foo1' : 'bar1', 'foo2' : 'bar2'},
                               'cleanAfterCook' : True})

        # Command-line macros take precedence over file macros
        self.checkCvc(
                  'cook test.recipe --macro "foo1 baz" '
                      '--macros %s --no-deps --prep' % macrosFile,
                  'conary.build.cook.cookCommand',
                  [None, ['test.recipe'], True, None],
                  ignoreKeywords=True,
                  ignoreDeps=True,
                  cfgValues = {'macros' :
                                    {'foo1' : 'baz', 'foo2' : 'bar2'},
                               'cleanAfterCook' : True})


        #self.checkCvc(
        #          'cook test.recipe --resume 10 --cross x86_64 --flavor "[ipv6,ssl is: x86_64]"',
        #          'conary.build.cook.cookCommand',
        #          [None, ['test.recipe'], False, None],
        #          crossCompile=(None, 'x86_64', False),
        #          ignoreKeywords=True, resume = '10',
        #          cfgValues = { 'flavor': '[ipv6,ssl is: x86_64]' } )

    def testCvcShadow(self):
        self.checkCvc(
                'shadow newlabel.domain.org@foo:bar baz=baz.rpath.org@rpl:devel '
                    'purpel=potatoe.rpath.org@bad:spelling',
                'conary.cmds.branch.branch',
                [None, None, 'newlabel.domain.org@foo:bar',
                    [ 'baz=baz.rpath.org@rpl:devel',
                    'purpel=potatoe.rpath.org@bad:spelling'] ],
                makeShadow = True, info = False,
                sourceOnly = False, binaryOnly = False, targetFile = None)

        self.checkCvc(
                'shadow newlabel.domain.org@foo:bar '
                    'postfix=conary.rpath.com@rpl:devel --info --source-only',
                'conary.cmds.branch.branch',
                [None, None, 'newlabel.domain.org@foo:bar',
                    [ 'postfix=conary.rpath.com@rpl:devel' ] ],
                makeShadow = True, info = True,
                sourceOnly = True, binaryOnly = False, targetFile = None)

        # use -i for info
        self.checkCvc(
                'shadow newlabel.domain.org@foo:bar '
                    'postfix=conary.rpath.com@rpl:devel -i --source-only',
                'conary.cmds.branch.branch',
                [None, None, 'newlabel.domain.org@foo:bar',
                    [ 'postfix=conary.rpath.com@rpl:devel' ] ],
                makeShadow = True, info = True,
                sourceOnly = True, binaryOnly = False, targetFile = None)

        self.checkCvc(
                'shadow newlabel.domain.org@foo:bar '
                    'postfix=conary.rpath.com@rpl:devel --to-file foo.ccs '
                    '--source-only',
                'conary.cmds.branch.branch',
                [None, None, 'newlabel.domain.org@foo:bar',
                    [ 'postfix=conary.rpath.com@rpl:devel' ] ],
                makeShadow = True, info = False,
                sourceOnly = True, binaryOnly = False,
                targetFile = 'foo.ccs')


        self.checkCvc(
                'shadow newlabel.domain.org@foo:bar '
                    'postfix=conary.rpath.com@rpl:devel --binary-only',
                'conary.cmds.branch.branch',
                [None, None, 'newlabel.domain.org@foo:bar',
                    [ 'postfix=conary.rpath.com@rpl:devel' ] ],
                makeShadow = True, info = False,
                sourceOnly = False, binaryOnly = True, targetFile = None)

    def testCvcSign(self):
        oldSigKeyMap = self.cfg.signatureKeyMap
        try:
            self.cfg.signatureKeyMap = []
            self.cfg.configLine('context foo')
            self.cfg.configLine('[foo]')
            self.cfg.configLine('signatureKeyMap .* FINGERPRINT')

            self.checkCvc(
                    'sign trove --signature-key NEW_FINGERPRINT --recurse',
                    'conary.build.signtrove.signTroves',
                    [None, ['trove'], True],
                    cfgValues = {'signatureKeyMap' : [] })
        finally:
            self.cfg.signatureKeyMap = oldSigKeyMap

    def testCvcStat(self):
        self.checkCvc('stat',
                       'conary.checkin.stat_',
                       [ None, ],
                       cfgValues = {})

    def testCvcStatExtraArgs(self):
        # Replace the usage function with our own, to make sure we properly
        # fail if more arguments are passed in
        usageRetval = -1432
        def usage(): return usageRetval

        c = cvccmd.CvcMain()
        thisCommand = c._supportedCommands['stat']
        thisCommand.usage = usage
        try:
            ret = c.runCommand(thisCommand, self.cfg, {},
                         ['cvc', 'stat', 'foo'])
        except TypeError:
            self.fail("Passing extra argument fails")
        self.failUnlessEqual(ret, usageRetval)

    def testCvcBasic(self):
        #make updateCmdsure we can specify options before the command
        self.checkCvc('--flavors commit --debug --debug',
                       'conary.checkin.commit',
                       [ None, None, None,  ],
                       cfgValues = {'fullFlavors' : True },
                       callback = None, test = False)

    def testCvcSet(self):
        self.checkCvc('set foo',
                      'conary.checkin.setFileFlags',
                      [ None, [ 'foo' ] ],
                      text = False, binary = False)

        self.checkCvc('set --text foo',
                      'conary.checkin.setFileFlags',
                      [ None, [ 'foo' ] ],
                      text = True, binary = False)

        self.checkCvc('set --binary foo',
                      'conary.checkin.setFileFlags',
                      [ None, [ 'foo' ] ],
                      text = False, binary = True)

    def testDebugLevels(self):
        def checkDebugLevel(level):
            def _checkDebugLevel(*args, **kw):
                assert(log.getVerbosity() == level)
            return _checkDebugLevel
        self.checkCvc('context --debug=all',
                      'conary.checkin.setContext',
                      [ None, None ],
                      ask = False, repos=None,
                      checkCallback=checkDebugLevel(log.LOWLEVEL))

        self.checkCvc('context --debug',
                      'conary.checkin.setContext',
                      [ None, None ],
                      ask = False, repos=None,
                      checkCallback=checkDebugLevel(log.DEBUG))
        self.checkCvc('context',
                      'conary.checkin.setContext',
                      [ None, None ],
                      ask = False, repos=None,
                      checkCallback=checkDebugLevel(log.INFO))

    #def testCvcContext(self):
    #    self.checkCvc('context',
    #                  'conary.checkin.setContext',
    #                  [ None, None ],
    #                  askpass = None)
    def testBadContext(self):
        # test bad --context command line arg
        argv = shlex.split(self._prepCmd('conary --context doesnotexist config'))
        self.logFilter.add()
        try:
            self.conaryMod.main(argv)
        except SystemExit, e:
            assert(e.code == 1)
        self.logFilter.compare('error: context "doesnotexist" (given '
                               'manually) does not exist')
        self.logFilter.remove()

        # test bad CONARY state file
        f = open('CONARY', 'w')
        f.write('context doesnotexist\n')
        f.close()
        argv = shlex.split(self._prepCmd('conary config'))
        self.logFilter.add()
        try:
            self.conaryMod.main(argv)
        except SystemExit, e:
            assert(e.code == 1)
        self.logFilter.compare('error: context "doesnotexist" (specified '
                               'in the CONARY state file) does not exist')
        self.logFilter.remove()
        os.unlink('CONARY')

        # test bad conaryrc
        f = open(self.rootDir + '/conaryrc', 'w')
        f.write('context doesnotexist\n')
        f.close()
        argv = shlex.split(self._prepCmd('conary config'))
        self.logFilter.add()
        try:
            self.conaryMod.main(argv)
        except SystemExit, e:
            assert(e.code == 1)
        self.logFilter.compare('error: context "doesnotexist" (specified '
                               'as the default context in the conary '
                               'configuration) does not exist')
        self.logFilter.remove()
        os.unlink(self.rootDir + '/conaryrc')

        # test bad environment variable
        try:
            os.environ['CONARY_CONTEXT'] = 'doesnotexist'
            argv = shlex.split(self._prepCmd('conary config'))
            self.logFilter.add()
            try:
                self.conaryMod.main(argv)
            except SystemExit, e:
                assert(e.code == 1)
            self.logFilter.compare('error: context "doesnotexist" (specified '
                                   'in the CONARY_CONTEXT environment '
                                   'variable) does not exist')
            self.logFilter.remove()
        finally:
            os.environ.pop('CONARY_CONTEXT')

    def testRQHelp(self):
        try:
            self.checkConary('rq --help',
                             'conary.cmds.conarycmd.RepQueryCommand.usage',
                             (None,))
        except SystemExit, e:
            assert(e.code == 1)

    def testVerboseHelp(self):
        # this might should be in options help
        rc, s = self.captureOutput(self.conaryMod.main,
                                   ['conary', 'help', 'update'])
        self.failUnless('--root' not in s)
        rc, s = self.captureOutput(self.conaryMod.main,
                                   ['conary', 'help', 'update', '--verbose'])
        self.failUnless('--root' in s)

    def testEraseHelp(self):
        rc, s = self.captureOutput(self.conaryMod.main,
                                   ['conary', 'help', 'erase'])
        self.failUnless('Erase Options' in s)

    def testInstallLabel(self):
        class Foo:
            pass
        def mockRunCommand(self, thisCommand, cfg, argSet, args, *a, **kw):
            Foo.cfgobj = cfg

        self.mock(options.MainHandler, "runCommand", mockRunCommand)
        self.conaryMod.main(["conary", "rblist", "--skip-default-config"])
        self.failUnless(hasattr(Foo.cfgobj, 'installLabel'))
        self.failUnless(hasattr(Foo.cfgobj, 'buildLabel'))

        del Foo.cfgobj

        self.cvcMod.main(["cvc", "cook", "--skip-default-config"])
        self.failUnless(hasattr(Foo.cfgobj, 'installLabel'))
        self.failUnless(hasattr(Foo.cfgobj, 'buildLabel'))

    def testUpdateReplaceFiles(self):
        def _test(managed, unmanaged, modified, config, basic):
            cmd = "update test"
            if managed:
                cmd += ' --replace-managed-files'
            if unmanaged:
                cmd += ' --replace-unmanaged-files'
            if modified:
                cmd += ' --replace-modified-files'
            if config:
                cmd += ' --replace-config-files'
            if basic:
                cmd += ' --replace-files'
                managed = True
                unmanaged = True
                modified = True
                config = True

            self.checkConary(cmd,
                             'conary.cmds.updatecmd.doUpdate',
                             [None, ['test']], ignoreKeywords=True,
                             noRestart=True, updateByDefault=True,
                             replaceManagedFiles = managed,
                             replaceUnmanagedFiles = unmanaged,
                             replaceModifiedFiles = modified,
                             replaceModifiedConfigFiles = config)

            cmd = cmd.replace('update ', 'erase ')

            self.checkConary(cmd,
                             'conary.cmds.updatecmd.doUpdate',
                             [None, ['test']], ignoreKeywords=True,
                             noRestart=True, updateByDefault=False,
                             replaceManagedFiles = managed,
                             replaceUnmanagedFiles = unmanaged,
                             replaceModifiedFiles = modified)

        _test(False, False, False, False, False)
        _test(False, False, False, False, True)
        _test(True, False, False, False, False)
        _test(False, True, False, False, False)
        _test(False, False, True, False, False)
        _test(False, False, False, True, False)

    def testUpdateAllReplaceFiles(self):
        def _testAll(managed, unmanaged, modified, config, basic):
            cmd = "updateall"
            if managed:
                cmd += ' --replace-managed-files'
            if unmanaged:
                cmd += ' --replace-unmanaged-files'
            if modified:
                cmd += ' --replace-modified-files'
            if config:
                cmd += ' --replace-config-files'
            if basic:
                cmd += ' --replace-files'
                managed = True
                unmanaged = True
                modified = True
                config = True

            self.checkConary(cmd,
                             'conary.cmds.updatecmd.updateAll',
                             [None], ignoreKeywords=True,
                             noRestart=True,
                             replaceManagedFiles = managed,
                             replaceUnmanagedFiles = unmanaged,
                             replaceModifiedFiles = modified,
                             replaceModifiedConfigFiles = config)


        _testAll(False, False, False, False, False)
        _testAll(False, False, False, False, True)
        _testAll(True, False, False, False, False)
        _testAll(False, True, False, False, False)
        _testAll(False, False, True, False, False)
        _testAll(False, False, False, True, False)

    def testCvcKeyManagement(self):
        self.checkCvc('addkey a b c d',
                      'conary.cmds.cvccmd.AddKeyCommand.usage',
                      [ None ])
        self.checkCvc('addkey',
                      'conary.keymgmt.addKey',
                      [ None, rephelp.NoneArg, rephelp.NoneArg,  ])
        self.checkCvc('addkey username',
                      'conary.keymgmt.addKey',
                      [ None, rephelp.NoneArg, 'username',  ])
        self.checkCvc('addkey --server servername username',
                      'conary.keymgmt.addKey',
                      [ None, 'servername', 'username',  ])

        self.checkCvc('listkeys a b c d',
                      'conary.cmds.cvccmd.ListKeysCommand.usage',
                      [ None ])
        self.checkCvc('listkeys',
                      'conary.keymgmt.displayKeys',
                      [ None, rephelp.NoneArg, rephelp.NoneArg,  ],
                      showFingerprints = False)
        self.checkCvc('listkeys --fingerprints',
                      'conary.keymgmt.displayKeys',
                      [ None, rephelp.NoneArg, rephelp.NoneArg,  ],
                      showFingerprints = True)
        self.checkCvc('listkeys --server servername username',
                      'conary.keymgmt.displayKeys',
                      [ None, 'servername', 'username'  ],
                      showFingerprints = False)

        self.checkCvc('getkey',
                      'conary.cmds.cvccmd.GetKeyCommand.usage',
                      [ None ])
        self.checkCvc('getkey ABCD1234',
                      'conary.keymgmt.showKey',
                      [ None, rephelp.NoneArg, 'ABCD1234',  ])
        self.checkCvc('getkey --server servername 12345678',
                      'conary.keymgmt.showKey',
                      [ None, 'servername', '12345678',  ])

    def testCvcDerivePackage(self):
        self.checkCvc('derive',
                      'conary.cmds.cvccmd.DeriveCommand.usage',
                      [ None ])

        self.checkCvc('derive trv=c.r.c@rpl:1 --target c.r.c@rpl:devel',
                      'conary.build.derive.derive',
                      [None, None, versions.Label('c.r.c@rpl:devel'),
                      'trv=c.r.c@rpl:1'],
                      checkoutDir = None, extract = False,
                      callback = None, info = False)
        self.cfg.buildLabel = versions.Label('localhost@rpl:linux')
        self.checkCvc(('derive trv=c.r.c@rpl:1'
                       ' --config "buildLabel localhost@rpl:linux"'
                       ' --extract'),
                      'conary.build.derive.derive',
                      [None, None, self.cfg.buildLabel, 'trv=c.r.c@rpl:1'],
                      checkoutDir = None, extract = True,
                      callback = None, info = False)

    def textCvcExplainNoParams(self):
        self.checkCvc('explain', 'conary.build.explain.docAll', [None])

    def testCvcExplainOneParam(self):
        self.checkCvc('explain Move', 'conary.build.explain.docObject', [None, 'Move'])

    def testCvcFactoryNoParams(self):
        self.checkCvc('factory', 'conary.checkin.factory', [ ])

    def testCvcFactoryOneParam(self):
        self.checkCvc('factory foo', 'conary.checkin.factory', [ 'foo' ])

    def testCvcImportNoSysArgv(self):
        # CNY-2786
        cmd = [ "python" ,"-c",
            "import sys; sys.path.insert(0, '%s'); del sys.argv; "
            "from conary import cvc" % resources.get_path() ]
        p = subprocess.Popen(cmd, stdout = subprocess.PIPE,
            stderr = subprocess.PIPE)
        stdout, stderr = p.communicate()
        self.failUnlessEqual(stdout, '')
        self.failUnlessEqual(stderr, '')

    def testMissingCommand(self):
        # CNY-3364
        rc, s = self.captureOutput(self.conaryMod.main,
                                   ['conary', '--what-requires', ])

    def testLocalOperationWithBadConaryrc(self):
        self.writeFile('%s/conaryrc' % self.workDir,
                        'connectAttempts 1\n'
                       'includeConfigFile http://does-not-exist/')
        os.chdir(self.workDir)

        self.skipDefaultConfig=False
        self.testQuery()
        self.testConaryRbList()
        self.testConaryRemove()
        self.testConaryRemoveRollback()
        self.testShowChangeset()
        self.testConaryRemoveRollback()
        self.testVerify()

        # verify failure for network operations
        rc, txt = self.captureOutput(self.assertRaises,
                                     AssertionError, self.testRepQuery)
        self.failUnlessEqual(txt, "error: conaryrc:2: when processing "
                "includeConfigFile: Error reading config file "
                "http://does-not-exist/: Name or service not known\n")
