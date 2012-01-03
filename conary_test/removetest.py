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


import os
from testrunner import testhelp

from conary_test import rephelp

from conary import trove
from conary.local import database
from conary.repository import errors, netclient


class RemoveTest(rephelp.RepositoryHelper):
    contexts = ['foo']

    @testhelp.context('remove')
    def testMarkRemoved(self):

        def _checkMissing(*args):
            for t in args:
                trv = repos.getTrove(t.getName(), t.getVersion(),
                                     t.getFlavor())
                assert(trv.type() == trove.TROVE_TYPE_REMOVED)

        def _checkEmptyStore():
            # the files store should be empty now
            for root, dirs, files in os.walk(self.reposDir + '/contents'):
                assert(not files)

        repos = self.openRepository()

        t = self.addComponent('test:runtime', '1.0-1-1')
        info = (t.getName(), t.getVersion(), t.getFlavor() )
        label = info[1].trailingLabel()
        self.markRemoved('test:runtime')
        _checkMissing(t)
        _checkEmptyStore()

        ver = repos.getTroveLatestVersion('test:runtime', info[1].branch(),
                                  troveTypes = netclient.TROVE_QUERY_ALL)
        assert(str(ver) == '/localhost@rpl:linux/1.0-1-1')
        self.assertRaises(errors.TroveMissing, repos.getTroveLatestVersion,
                          'test:runtime', info[1].branch())

        # we shouldn't be able to update to a removed trove
        rc = self.logCheck(self.updatePkg, ("test:runtime",),
                           'error: test:runtime was not found on path '
                           'localhost@rpl:linux')

        # trying to add the same component should bomb
        self.assertRaises(errors.CommitError, self.addComponent,
                          'test:runtime', '1.0-1-1')

        t1 = self.addComponent('test:runtime', '1.0-1-2')
        t2 = self.addCollection("test", "1.0-1-2", ["test:runtime"])
        self.updatePkg('test')
        self.markRemoved('test')
        _checkMissing(t1, t2)
        _checkEmptyStore()
        ver = repos.getTroveLatestVersion('test:runtime', info[1].branch(),
                                  troveTypes = netclient.TROVE_QUERY_ALL)
        assert(str(ver) == '/localhost@rpl:linux/1.0-1-2')
        self.assertRaises(errors.TroveMissing, repos.getTroveLatestVersion,
                          'test:runtime', info[1].branch())

        d = repos.getTroveVersionsByLabel({ 'test:runtime' :
                                          { info[1].trailingLabel() : None } } )
        assert(not d)
        d = repos.getTroveVersionsByLabel({ 'test:runtime' :
                                      { info[1].trailingLabel() : None } },
                                      troveTypes = netclient.TROVE_QUERY_ALL)
        assert([ str(x) for x in sorted(d['test:runtime'].keys()) ] ==
                    [ '/localhost@rpl:linux/1.0-1-1',
                      '/localhost@rpl:linux/1.0-1-2' ])

        d = repos.getAllTroveLeaves('localhost', { 'test:runtime' : None })
        assert(not d)
        d = repos.getAllTroveLeaves('localhost', { 'test:runtime' : None },
                                    troveTypes = netclient.TROVE_QUERY_ALL)
        assert([ str(x) for x in d['test:runtime'].keys() ] ==
                    [ '/localhost@rpl:linux/1.0-1-2' ] )

        d = repos.getTroveVersionList('localhost', { 'test:runtime' : None })
        assert(not d)
        d = repos.getTroveVersionList('localhost', { 'test:runtime' : None },
                                      troveTypes = netclient.TROVE_QUERY_ALL)
        assert([ str(x) for x in sorted(d['test:runtime'].keys()) ] ==
                    [ '/localhost@rpl:linux/1.0-1-1',
                      '/localhost@rpl:linux/1.0-1-2' ])

        d = repos.getTroveLeavesByLabel({ 'test:runtime' : { label : None } })
        assert(not d)
        d = repos.getTroveLeavesByLabel({ 'test:runtime' : { label : None } },
                        troveTypes = netclient.TROVE_QUERY_ALL)
        assert([ str(x) for x in d['test:runtime'].keys() ] ==
                    [ '/localhost@rpl:linux/1.0-1-2' ] )

        d = repos.getTroveVersionsByLabel({ 'test:runtime' : { label : None } })
        assert(not d)
        d = repos.getTroveVersionsByLabel({ 'test:runtime' : { label : None } },
                        troveTypes = netclient.TROVE_QUERY_ALL)
        assert([ str(x) for x in sorted(d['test:runtime'].keys()) ] ==
                    [ '/localhost@rpl:linux/1.0-1-1',
                      '/localhost@rpl:linux/1.0-1-2' ])

        d = repos.getTroveVersionFlavors({ 'test:runtime' :
                                             { t2.getVersion() : None } })
        assert(not d)
        d = repos.getTroveVersionFlavors({ 'test:runtime' :
                                             { t2.getVersion() : None } },
                                         troveTypes = netclient.TROVE_QUERY_ALL)
        assert(len(d['test:runtime']) == 1)

        d = repos.getTroveLeavesByBranch(
                    { 'test:runtime' : { t1.getVersion().branch() : None } })
        assert(not d)
        d = repos.getTroveLeavesByBranch(
                    { 'test:runtime' : { t1.getVersion().branch() : None } },
                    troveTypes = netclient.TROVE_QUERY_ALL)
        assert([ str(x) for x in d['test:runtime'].keys() ] ==
                    [ '/localhost@rpl:linux/1.0-1-2' ] )

        d = repos.getTroveVersionsByBranch(
                    { 'test:runtime' : { t1.getVersion().branch() : None } })
        assert(not d)
        d = repos.getTroveVersionsByBranch(
                    { 'test:runtime' : { t1.getVersion().branch() : None } },
                    troveTypes = netclient.TROVE_QUERY_ALL)
        assert([ str(x) for x in sorted(d['test:runtime'].keys()) ] ==
                    [ '/localhost@rpl:linux/1.0-1-1',
                      '/localhost@rpl:linux/1.0-1-2' ])

        t1 = self.addComponent('test:runtime', '1.0-1-3')
        t2 = self.addCollection("test", "1.0-1-3", ["test:runtime"])
        self.updatePkg("test")
        # this failed when CNY-2403 was broken
        self.verifyDatabase()
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(db.hasTrove(*t1.getNameVersionFlavor()))
        assert(db.hasTrove(*t2.getNameVersionFlavor()))

        self.resetRoot()
        self.markRemoved('test:runtime')
        # this update implicitly checks server cache invalidation
        rc = self.logCheck(self.updatePkg, ("test",),
                "error: The following troves no longer exist in the "
                "repository and cannot be installed: "
                "test:runtime=/localhost@rpl:linux/1.0-1-3[]")

    @testhelp.context('remove')
    def testMarkRemovedRedirects(self):
        # test for CNY-717, marking a redirect trove as removed.
        trv = self.addComponent('foo:runtime', '1', redirect=['bar:runtime'])
        self.markRemoved('foo:runtime')
        repos = self.openRepository()
        trv = repos.getTrove(*trv.getNameVersionFlavor())
        assert(trv.isRemoved())

    @testhelp.context('remove')
    def testMarkRemovedGroup(self):
        # test for CNY-1504, marking a redirect trove as removed.
        comp = self.addComponent('foo:runtime', '1', redirect=['bar:runtime'])
        grp = self.addCollection('group-foo', '1', [ 'foo:runtime' ])
        self.markRemoved('group-foo')
        repos = self.openRepository()

        grp = repos.getTrove(*grp.getNameVersionFlavor())
        assert(grp.isRemoved())

        comp2 = repos.getTrove(*comp.getNameVersionFlavor())
        assert(not comp2.isRemoved())
        assert(comp == comp2)

    @testhelp.context('remove')
    def testMarkRemovedNotByDefaultComponent(self):
        # test that a byDefault=False component mark removed doesn't
        # make the rest of a package unusable
        self.addComponent('foo:lib', '1', filePrimer=1)
        self.addComponent('foo:debuginfo', '1', filePrimer=2)
        self.addCollection('foo', '1', [':lib', (':debuginfo', False)])
        self.markRemoved('foo:debuginfo')
        self.checkUpdate('foo',
                        ['foo', 'foo:lib' ])

    @testhelp.context('remove')
    def testDuplicateRemoves(self):
        # CNY-1654
        comp = self.addComponent('foo:lib', '1', filePrimer=2)
        grp = self.addCollection('foo', '1', [':lib' ])
        self.markRemoved('foo:lib')
        self.markRemoved('foo')

        repos = self.openRepository()
        comp = repos.getTrove(*comp.getNameVersionFlavor())
        assert(comp.isRemoved())
        grp = repos.getTrove(*grp.getNameVersionFlavor())
        assert(grp.isRemoved())

    @testhelp.context('remove')
    def testRemovedTroveNames(self):
        # CNY-1838 -- make sure removed/not present troves don't show up
        # in troveNames call

        def _test(correct, **kwargs):
            l = sorted(repos.troveNames(self.cfg.buildLabel, **kwargs))
            assert(l == correct)
            l = sorted(repos.troveNamesOnServer('localhost', **kwargs))
            assert(l == correct)

        self.addComponent('removed:run', '1.0')
        self.addComponent('redir:run', '1.0', redirect = [])
        self.addCollection('group-foo', '1.0', [ 'removed:run', 'bar:run' ] )
        self.markRemoved('removed:run')

        repos = self.openRepository()

        _test([ 'group-foo', 'redir:run' ] )
        _test([ 'group-foo', 'redir:run', 'removed:run' ],
              troveTypes = netclient.TROVE_QUERY_ALL)
        _test([ 'group-foo' ],
              troveTypes = netclient.TROVE_QUERY_NORMAL)

        # old servers always did TROVE_QUERY_ALL
        repos.c['localhost'].setProtocolVersion(59)
        _test([ 'group-foo', 'redir:run', 'removed:run' ],
              troveTypes = netclient.TROVE_QUERY_ALL)
        _test([ 'group-foo', 'redir:run', 'removed:run' ],
              troveTypes = netclient.TROVE_QUERY_NORMAL)
        _test([ 'group-foo', 'redir:run', 'removed:run' ],
              troveTypes = netclient.TROVE_QUERY_PRESENT)

    @testhelp.context("remove")
    def testRemoveFlavors(self):
        # CNY-2802 - test removing troves with non-unique flavors
        grp1 = self.addCollection("group-removed", "1.0", [
            # these are missing components
            ("removed1:run", "1.0", "foo"),
            ("removed2:run", "1.0", "bar"),
            ])
        grp2 = self.addCollection("group-removed", "2.0", [
            # these are missing components
            ("removed1:run", "2.0", "foo"),
            ("removed2:run", "2.0", "bar"),
            ])

        repos = self.openRepository()
        self.markRemoved("group-removed=2.0[foo,bar]")
        ret = repos.getTrove(*grp1.getNameVersionFlavor())
        self.failIf(ret.isRemoved())
        ret = repos.getTrove(*grp2.getNameVersionFlavor())
        self.failUnless(ret.isRemoved())
