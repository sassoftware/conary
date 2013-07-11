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
import re
import bz2
import itertools
from conary_test import rephelp

from conary import conaryclient, errors
from conary.deps import deps
from conary import conarycfg
from conary import versions
from conary.cmds.queryrep import VERSION_FILTER_ALL, VERSION_FILTER_LATEST
from conary.cmds.queryrep import VERSION_FILTER_LEAVES
from conary.cmds.queryrep import FLAVOR_FILTER_ALL, FLAVOR_FILTER_AVAIL
from conary.cmds.queryrep import FLAVOR_FILTER_BEST, FLAVOR_FILTER_EXACT
from conary.cmds import queryrep
from conary.conaryclient import cmdline
from conary.repository import trovesource
from conary.repository.trovesource import TROVE_QUERY_NORMAL
from conary.repository.trovesource import TROVE_QUERY_PRESENT
from conary.repository.trovesource import TROVE_QUERY_ALL

from conary.versions import VersionFromString as VFS

class RepQueryTest(rephelp.RepositoryHelper):

    def _rdiff(self, troveSpec, **kwargs):
        client = conaryclient.ConaryClient(self.cfg)
        return self.captureOutput(queryrep.rdiffCommand, self.cfg,
                                  client, client.getDatabase(), troveSpec,
                                  **kwargs)

    def testBadQuery(self):
        try:
            queryrep.getTrovesToDisplay(None, ['=conary.rpath.com@'], [], [],
                    VERSION_FILTER_ALL, FLAVOR_FILTER_ALL, 
                    self.cfg.installLabelPath, self.cfg.flavor,
                    None)
        except errors.ParseError, msg:
            assert(str(msg) == 'Error with spec "=conary.rpath.com@": Trove name is required')
        else:
            assert(0)


    def _checkTupVers(self, tups, vers):
        for tup, item in itertools.izip(tups, vers):
            if isinstance(item, (list, tuple)):
                verStr, flStr = item
            else:
                verStr = item
                flStr = None

            if '-' in verStr:
                assert(str(tup[1].trailingRevision()) == verStr)
            else:
                assert(str(tup[1].trailingRevision().version) == verStr)
            if flStr:
                assert(str(tup[2]) == flStr)


    def testGetTrovesVersions(self):
        self.addComponent('foo:foo', '1.0', 'ssl')
        self.addComponent('foo:foo', '1.0', '~ssl')
        self.addComponent('foo:foo', '1.0', '!ssl')
        self.addComponent('foo:foo', '1.0', 'readline')
        self.addComponent('foo:foo', '2.0-1-1', 'readline')
        self.addComponent('foo:foo', '2.0-2-1', 'readline')
        self.addComponent('foo:foo', '3.0', '!readline')
        repos = self.openRepository()
        targetFlavor = [ deps.parseFlavor('~readline,ssl is:x86') ]

        def _check(troveSpecs, versionFilter, flavorFilter, expected):
            tups = queryrep.getTrovesToDisplay(repos, troveSpecs, [], [],
                                        versionFilter, flavorFilter,
                                        self.cfg.installLabelPath,
                                        targetFlavor,
                                        None)
            self._checkTupVers(tups, expected)


        # test ALL
        _check(['foo:foo'], VERSION_FILTER_ALL, FLAVOR_FILTER_BEST, 
                ['1.0', '2.0-1-1', '2.0-2-1'])
        _check(['foo:foo=2.0'], VERSION_FILTER_ALL, FLAVOR_FILTER_BEST, 
                ['2.0-1-1', '2.0-2-1'])
        _check(['foo:foo[!readline]'], VERSION_FILTER_ALL, FLAVOR_FILTER_BEST, 
                ['1.0', '3.0'])
        _check(['foo:foo[!readline]'], VERSION_FILTER_ALL, FLAVOR_FILTER_AVAIL, 
                ['1.0', '1.0', '3.0'])
        _check(['foo:foo[!readline]'], VERSION_FILTER_ALL, FLAVOR_FILTER_ALL, 
                ['1.0', '1.0', '1.0', '3.0'])
        _check(['foo:foo'], VERSION_FILTER_ALL, FLAVOR_FILTER_ALL, 
                ['1.0', '1.0', '1.0', '1.0', '2.0', '2.0', '3.0'])

        # test ALL w/ no spec
        _check([], VERSION_FILTER_ALL, FLAVOR_FILTER_BEST, 
               ['1.0', '2.0-1-1', '2.0-2-1'])
        _check([], VERSION_FILTER_ALL, FLAVOR_FILTER_AVAIL, 
                # ssl, ~ssl, readline, 
                ['1.0', '1.0', '1.0', '2.0-1-1', '2.0-2-1'])
        _check([], VERSION_FILTER_ALL, FLAVOR_FILTER_ALL, 
                ['1.0', '1.0', '1.0', '1.0', '2.0', '2.0', '3.0'])

        # test LEAVES
        _check(['foo:foo'], VERSION_FILTER_LEAVES, FLAVOR_FILTER_BEST, 
                ['2.0-2-1'])
        _check(['foo:foo=2.0'], VERSION_FILTER_LEAVES, FLAVOR_FILTER_BEST, 
                ['2.0-2-1'])
        _check(['foo:foo[!readline]'], VERSION_FILTER_LEAVES, 
                FLAVOR_FILTER_BEST, ['3.0'])
        _check(['foo:foo[!readline]'], VERSION_FILTER_LEAVES,  # does not have
                                                               # !ssl
                FLAVOR_FILTER_AVAIL, ['1.0', '1.0', '3.0'])
        _check(['foo:foo[!readline]'], VERSION_FILTER_LEAVES, # include !ssl
                FLAVOR_FILTER_ALL, ['1.0', '1.0', '1.0', '3.0'])
        _check(['foo:foo'], VERSION_FILTER_LEAVES, FLAVOR_FILTER_ALL, 
                ['1.0', '1.0', '1.0', '2.0-2-1', '3.0'])

        # check LEAVES with no spec
        _check([], VERSION_FILTER_LEAVES, FLAVOR_FILTER_BEST, 
                # best 1.0 flavor, best 2.0 flavor, no other version nodes
                # have compatible flavor leaves.
               ['1.0', '2.0-2-1'])
        _check([], VERSION_FILTER_LEAVES, FLAVOR_FILTER_AVAIL, 
                # add in ~ssl because it was also at 1.0 node
                ['1.0', '1.0', '2.0-2-1'])
        _check([], VERSION_FILTER_LEAVES, FLAVOR_FILTER_ALL, 
                ['1.0', '1.0', '1.0', '2.0', '3.0'])

 
        # test LATEST
        _check(['foo:foo'], VERSION_FILTER_LATEST, FLAVOR_FILTER_BEST, 
                ['2.0-2-1'])
        _check(['foo:foo=2.0'], VERSION_FILTER_LATEST, FLAVOR_FILTER_BEST, 
                ['2.0-2-1'])
        _check(['foo:foo[!readline]'], VERSION_FILTER_LATEST, 
                FLAVOR_FILTER_BEST, ['3.0'])
        _check(['foo:foo[!readline]'], VERSION_FILTER_LATEST,
                FLAVOR_FILTER_AVAIL, ['3.0'])
        _check(['foo:foo[!readline]'], VERSION_FILTER_LATEST, # include !ssl
                FLAVOR_FILTER_ALL, ['3.0'])
        _check(['foo:foo'], VERSION_FILTER_LATEST, FLAVOR_FILTER_ALL, 
                ['3.0'])
        # to really test latest, we need to make the latest node more 
        # interesting..

        # we've already got !readline there.
        self.addComponent('foo:foo', '3.0', 'readline')
        self.addComponent('foo:foo', '3.0', '!ssl')
        self.addComponent('foo:foo', '3.0', 'ssl')
        self.addComponent('foo:foo', '3.0', '~ssl')
        _check(['foo:foo'], VERSION_FILTER_LATEST, FLAVOR_FILTER_BEST, 
                ['3.0'])
        _check(['foo:foo[!readline]'], VERSION_FILTER_LATEST, 
                FLAVOR_FILTER_BEST, ['3.0'])
        _check(['foo:foo[!readline]'], VERSION_FILTER_LATEST,
                # !readline, ssl, ~ssl
                FLAVOR_FILTER_AVAIL, ['3.0', '3.0', '3.0'])
        _check(['foo:foo[!readline]'], VERSION_FILTER_LATEST,
                # !readline, ssl, ~ssl, !ssl
                FLAVOR_FILTER_ALL, ['3.0', '3.0', '3.0', '3.0'])

        # test LATEST w/ no spec
        _check([], VERSION_FILTER_LATEST, FLAVOR_FILTER_BEST, 
                ['3.0'])
        _check([], VERSION_FILTER_LATEST,
                # readline, ssl, ~ssl
                FLAVOR_FILTER_AVAIL, ['3.0', '3.0', '3.0'])
        _check([], VERSION_FILTER_LATEST,
                # readline, ssl, ~ssl, !ssl
                FLAVOR_FILTER_ALL, ['3.0', '3.0', '3.0', '3.0'])

    def testQueryByTroveType(self):
        repos = self.openRepository()
        def _check(troveSpecs, versionFilter, flavorFilter, expected, all=False,
                   present=False):
            if all:
                troveTypes = TROVE_QUERY_ALL
            elif present:
                troveTypes = TROVE_QUERY_PRESENT
            else:
                troveTypes = TROVE_QUERY_NORMAL

            tups = queryrep.getTrovesToDisplay(repos, troveSpecs, [], [],
                                        versionFilter, flavorFilter,
                                        self.cfg.installLabelPath,
                                        self.cfg.flavor,
                                        None,
                                        troveTypes=troveTypes)
            self._checkTupVers(tups, expected)

        # foo is replaced by a redirect, bar is replaced by a redirect and a removed trove
        self.addComponent('foo:run', '1.0')
        self.addComponent('foo:run', '2.0', redirect=['bar:run'])
        self.addComponent('bar:run', '1.0')
        self.addComponent('bar:run', '2.0', redirect=['foo:run'])
        self.addComponent('bar:run', '3.0')
        self.markRemoved('bar:run=3.0')

        # test ALL
        _check([], VERSION_FILTER_ALL, FLAVOR_FILTER_ALL, 
                ['1.0', '1.0'])

        _check([], VERSION_FILTER_ALL, FLAVOR_FILTER_ALL, 
               ['1.0', '2.0', '1.0', '2.0'], present=True)

        _check([], VERSION_FILTER_ALL, FLAVOR_FILTER_ALL, 
               ['1.0', '2.0', '3.0', '1.0', '2.0'], all=True)

        # test LEAVES
        _check([], VERSION_FILTER_LEAVES, FLAVOR_FILTER_ALL, [])

        _check([], VERSION_FILTER_LEAVES, FLAVOR_FILTER_ALL, 
                ['2.0', '2.0'], present=True)

        _check([], VERSION_FILTER_LEAVES, FLAVOR_FILTER_ALL, 
                ['3.0', '2.0'], all=True)

        # test LATEST
        _check([], VERSION_FILTER_LATEST, FLAVOR_FILTER_ALL, [])

        _check([], VERSION_FILTER_LATEST, FLAVOR_FILTER_ALL, ['2.0', '2.0'], present=True)

        _check([], VERSION_FILTER_LATEST, FLAVOR_FILTER_ALL, ['3.0', '2.0'], all=True)

    def testGetTrovesLeavesMultiRepos(self):
        v1 = self.addComponent('foo:foo', '1.0').getVersion()
        v2 = self.addComponent('foo:foo', ':branch/1.0').getVersion()
        installLabelPath = conarycfg.CfgLabelList(
                    [versions.Label('localhost@rpl:branch'), 
                     self.cfg.buildLabel])

        repos = self.openRepository()
        tups = queryrep.getTrovesToDisplay(repos, [], [], [],
                                           VERSION_FILTER_LEAVES, 
                                           FLAVOR_FILTER_ALL,
                                           installLabelPath,
                                           self.cfg.flavor, affinityDb=None)
        assert(set([x[1] for x in tups]) == set((v1, v2)))
        tups = queryrep.getTrovesToDisplay(repos, ['foo:foo'], [], [],
                                           VERSION_FILTER_LEAVES,
                                           FLAVOR_FILTER_ALL,
                                           installLabelPath,
                                           self.cfg.flavor, affinityDb=None)
        assert(set([x[1] for x in tups]) == set((v1, v2)))
        tups = queryrep.getTrovesToDisplay(repos, ['foo:foo'], [], [],
                                           VERSION_FILTER_LATEST,
                                           FLAVOR_FILTER_ALL,
                                           installLabelPath,
                                           self.cfg.flavor, affinityDb=None)
        assert(set([x[1] for x in tups]) == set((v1, v2)))
        tups = queryrep.getTrovesToDisplay(repos, [], [], [],
                                           VERSION_FILTER_LATEST,
                                           FLAVOR_FILTER_ALL,
                                           installLabelPath,
                                           self.cfg.flavor, affinityDb=None)
        assert(set([x[1] for x in tups]) == set((v1, v2)))

    def testGetTrovesLatestByLabel(self):
        # test out the no trove queries that now use getTrovesLatestByLabel
        trv1 = self.addComponent('foo:run', '/localhost@rpl:branch//rpl:linux/1.0-1-1')
        trv2 = self.addComponent('foo:run', '1.0-1-2')
        trv3 = self.addComponent('foo:run', '/localhost@rpl:branch//rpl:linux/1.0-1-3', 'ssl')
        repos = self.openRepository()
        tups = queryrep.getTrovesToDisplay(repos, [], [], [],
                                       VERSION_FILTER_LATEST,
                                       FLAVOR_FILTER_BEST,
                                       self.cfg.installLabelPath,
                                       self.cfg.flavor, affinityDb=None)
        assert(len(tups) == 1)
        assert(tups[0] == trv3.getNameVersionFlavor())
        tups = queryrep.getTrovesToDisplay(repos, [], [], [],
                                       VERSION_FILTER_LEAVES,
                                       FLAVOR_FILTER_BEST,
                                       self.cfg.installLabelPath,
                                       self.cfg.flavor, affinityDb=None)
        assert(len(tups) == 2)
        assert(set(tups) == set([trv3.getNameVersionFlavor(),
                                 trv2.getNameVersionFlavor()]))



    def testLatestIsOfWrongFlavor(self):
        # CNY-784 - if the latest version was of an incompatible flavor,
        # conary rq <no args> would display nothing for that trove
        v1 = self.addComponent('foo:foo', '1.0', 'is:x86').getVersion()
        v2 = self.addComponent('foo:foo', '1.1', 'is:x86_64').getVersion()
        targetFlavor = [ deps.parseFlavor('is:x86') ]
        repos = self.openRepository()
        tups = queryrep.getTrovesToDisplay(repos, [], [], [],
                                           VERSION_FILTER_LATEST, 
                                           FLAVOR_FILTER_BEST,
                                           self.cfg.installLabelPath,
                                           targetFlavor, affinityDb=None)
        assert(set([x[1] for x in tups]) == set((v1,)))
        tups = queryrep.getTrovesToDisplay(repos, [], [], [],
                                           VERSION_FILTER_LATEST, 
                                           FLAVOR_FILTER_AVAIL,
                                           self.cfg.installLabelPath,
                                           targetFlavor, affinityDb=None)
        assert(set([x[1] for x in tups]) == set((v1,)))

    def testExactFlavor(self):
        self.addComponent('foo:run[~ssl]')
        repos = self.openRepository()

        def _get(troveSpec):
            try:
                return queryrep.getTrovesToDisplay(repos, [troveSpec], [], [],
                                                   VERSION_FILTER_LATEST,
                                                   FLAVOR_FILTER_EXACT,
                                                   self.cfg.installLabelPath,
                                                   self.cfg.flavor, None)
            except errors.TroveNotFound:
                return []
        assert(not _get('foo:run[ssl]'))
        assert(not _get('foo:run'))
        assert(_get('foo:run[~ssl]'))

    def testTroveNames(self):
        for x in "12":
            for ver in "12":
                self.addComponent("trv%s:lib" % x, ver)
                self.addComponent("trv%s:runtime" % x, ver)
                self.addCollection("trv%s" % x, ver, [":lib", ":runtime"])
        repos = self.openRepository()
        ret = repos.troveNamesOnServer("localhost")
        self.assertEqual(set(ret),
                             set(['trv1:lib', 'trv1:runtime', 'trv1',
                                  'trv2:lib', 'trv2:runtime', 'trv2']))
        self.markRemoved("trv1=1") 
        ret = repos.troveNamesOnServer("localhost")
        self.assertEqual(set(ret),
                             set(['trv1:lib', 'trv1:runtime', 'trv1',
                                  'trv2:lib', 'trv2:runtime', 'trv2']))
        self.markRemoved("trv1=2") 
        ret = repos.troveNamesOnServer("localhost")
        self.assertEqual(set(ret), set(['trv2:lib', 'trv2:runtime', 'trv2']))

        self.addCollection("group-trv", [("trv2:runtime", "2"), ("trv3:runtime", "0")])
        ret = repos.troveNamesOnServer("localhost")
        # trv3:runtime is not present thus it shouldn't appear in the list
        self.assertEqual(set(ret), set(['trv2:lib', 'trv2:runtime', 'trv2', "group-trv"]))
        self.markRemoved("trv2=1")
        ret = repos.troveNamesOnServer("localhost")
        # trv2=2 is still there
        self.assertEqual(set(ret), set(['trv2:lib', 'trv2:runtime', 'trv2', "group-trv"]))
        self.markRemoved("trv2=2")
        ret = repos.troveNamesOnServer("localhost")
        self.assertEqual(set(ret), set(["group-trv"]))
        self.markRemoved("group-trv")
        ret = repos.troveNamesOnServer("localhost")
        self.assertEqual(set(ret), set())

        self.addCollection("group-other", ["foo:runtime", "foo:lib"])
        self.addComponent("foo:lib", "999")
        ret = repos.troveNamesOnServer("localhost")
        self.assertEqual(set(ret), set(["foo:lib", "group-other"]))
        self.markRemoved("foo:lib=999")
        ret = repos.troveNamesOnServer("localhost")
        self.assertEqual(set(ret), set(["group-other"]))

    def testAffinity(self):
        self.addComponent('foo:r', '/localhost@rpl:branch/1.0-1-1', '!readline',
                          ['/usr/bin/foo'])
        self.addComponent('foo:r', '/localhost@rpl:branch/2.0-1-1', 
                          'readline,~!ssl', ['/usr/bin/foo'])
        self.addComponent('foo:r', '/localhost@rpl:branch/2.0-1-1', 
                          'readline,~ssl', ['/usr/bin/foo'])
        self.addComponent('foo:r', '/localhost@rpl:branch/2.0-1-1', 
                          '!readline,~ssl', ['/usr/bin/foo'])
        self.addComponent('foo:r', '/localhost@rpl:branch/2.0-1-1', 
                          '!readline,~!ssl', ['/usr/bin/foo'])

        # orig branch - found by very few queries
        self.addComponent('foo:r', 
                          '/localhost@rpl:linux/1.0-1-1', 'readline')
        self.updatePkg('foo:r=:branch/1.0[!readline]')

        repos = self.openRepository()

        def _get(affinityDb, versionFilter, flavorFilter, troveSpec):
            return queryrep.getTrovesToDisplay(repos, troveSpec, [], [],
                                               versionFilter,
                                               flavorFilter, 
                                               self.cfg.installLabelPath,
                                               self.cfg.flavor, affinityDb)

        db = self.openDatabase()
        troveTups = _get(db, VERSION_FILTER_LATEST, 
                         FLAVOR_FILTER_BEST, ['foo:r'])
        assert(len(troveTups) == 1)
        assert(troveTups[0][1].branch() == VFS('/localhost@rpl:branch'))
        assert(str(troveTups[0][2]) == '!readline,~ssl')

        troveTups = _get(db, VERSION_FILTER_LATEST, 
                         FLAVOR_FILTER_AVAIL, ['foo:r'])

        assert(len(troveTups) == 2)
        flavors = set(str(x[2]) for x in troveTups)
        assert('readline,~ssl' in flavors)
        assert('!readline,~ssl' in flavors)

        # system compatible, should ignore db
        troveTups = _get(None, VERSION_FILTER_LATEST, 
                         FLAVOR_FILTER_AVAIL, ['foo:r'])

        assert(len(troveTups) == 1)
        assert(troveTups[0][1].branch() == VFS('/localhost@rpl:linux'))
        flavors = set(str(x[2]) for x in troveTups)
        assert('readline' in flavors)

    def testQueryByPath(self):
        for troveName in 'foo:run', 'bar:run':
            self.addComponent(troveName, '1.0', 'ssl', ['/usr/bin/foo'])
            self.addComponent(troveName, '1.0', '~ssl', ['/usr/bin/foo'])
            self.addComponent(troveName, '1.0', '!ssl', ['/usr/bin/foo'])
            self.addComponent(troveName, '1.0', 'readline', ['/usr/bin/foo'])
            self.addComponent(troveName, '2.0-1-1', 'readline', ['/usr/bin/foo'])
            self.addComponent(troveName, '2.0-2-1', 'readline', ['/usr/bin/foo'])
            self.addComponent(troveName, '3.0', '!readline', ['/usr/bin/foo'])

        repos = self.openRepository()
        targetFlavor = [ deps.parseFlavor('~readline,ssl is:x86') ]

        def _getByPath(versionFilter, flavorFilter, pathList=['/usr/bin/foo']):
            return queryrep.getTrovesByPath(repos, pathList,
                                            versionFilter, flavorFilter,
                                            self.cfg.installLabelPath,
                                            targetFlavor)


        def _check(tups, troveSpecs):
            source = trovesource.SimpleTroveSource(tups)
            source.searchAsDatabase()

            troveSpecs = [ cmdline.parseTroveSpec(x) for x in troveSpecs ]
            results = source.findTroves(None, troveSpecs)
            receivedTups = itertools.chain(*results.itervalues())
            assert(set(receivedTups) == set(tups))



        assert(len(_getByPath(VERSION_FILTER_ALL, FLAVOR_FILTER_ALL)) == 14)

        tups = _getByPath(VERSION_FILTER_LATEST, FLAVOR_FILTER_ALL)
        _check(tups, ['bar:run=3.0', 'foo:run=3.0'])

        # check leaves, should leave out 2.0-1-1 readline and 1.0 readline.
        tups = _getByPath(VERSION_FILTER_LEAVES, FLAVOR_FILTER_ALL)
        assert(len(tups) == 10)

        # we don't really need to check both foo and bar here...
        tups = [ x for x in tups if x[0] == 'bar:run' ]
        _check(tups, ['bar:run=1.0[ssl]', 'bar:run=2.0-2-1[readline]', 
                      'bar:run=1.0[!ssl]', 'bar:run=1.0[~ssl]',
                      'bar:run=3.0[!readline]'])

        # get all compatible flavors, should leave out !readline and !ssl
        tups = _getByPath(VERSION_FILTER_ALL, FLAVOR_FILTER_AVAIL)
        assert(len(tups) == 10)
        tups = [ x for x in tups if x[0] == 'bar:run' ]
        _check(tups, ['bar:run=1.0[ssl]', 'bar:run=1.0[readline]',
                      'bar:run=2.0-1-1[readline]', 'bar:run=2.0-2-1[readline]',
                      'bar:run=1.0[~ssl]'])


        # get best best flavors for each version
        tups = _getByPath(VERSION_FILTER_ALL, FLAVOR_FILTER_BEST)
        tups = [ x for x in tups if x[0] == 'bar:run' ]
        _check(tups, ['bar:run=1.0[ssl]', 'bar:run=2.0-1-1[readline]', 
                      'bar:run=2.0-2-1[readline]'])

        tups = _getByPath(VERSION_FILTER_LEAVES, FLAVOR_FILTER_BEST)
        assert(len(tups) == 2)
        tups = [ x for x in tups if x[0] == 'bar:run' ]
        _check(tups, [ 'bar:run=2.0-2-1[readline]'])

        tups = _getByPath(VERSION_FILTER_LATEST, FLAVOR_FILTER_BEST)
        assert(len(tups) == 2)
        _check(tups, ['bar:run=2.0-2-1[readline]', 'foo:run=2.0-2-1[readline]'])

        # add another path for testing querying two paths at once,
        # with different latest versions to test leaves handing.
        self.addComponent('foo:lib', '1.0', 'ssl', ['/usr/lib/foo'])
        self.addComponent('bar:lib', '1.0', 'ssl', ['/usr/lib/foo'])
        self.addComponent('bar:lib', '2.0', 'ssl', ['/usr/lib/foo'])


        tups = _getByPath(VERSION_FILTER_LATEST, FLAVOR_FILTER_BEST, 
                          ['/usr/lib/foo', '/usr/bin/foo'])
        _check(tups, ['bar:run=2.0-2-1[readline]', 'foo:run=2.0-2-1[readline]',
                      'foo:lib=1.0', 'bar:lib=2.0'])



    def testWhatProvides(self):
        targetFlavor = [ deps.parseFlavor('is:x86') ]

        def _get(versionFilter, flavorFilter, whatProvidesList):
            return queryrep.getTrovesToDisplay(repos, [], [],
                                [deps.parseDep(x) for x in whatProvidesList],
                                               versionFilter,
                                               flavorFilter,
                                               self.cfg.installLabelPath,
                                               targetFlavor, None)
        self.addComponent('foo:run', '1', 'is:x86')
        self.addComponent('foo:run', '2', 'is:x86')
        self.addComponent('foo:run', '2', 'is:x86_64')
        repos = self.openRepository()

        troveTups = _get(VERSION_FILTER_LATEST, FLAVOR_FILTER_BEST, ['trove:foo:run'])
        assert(len(troveTups) == 1)

        troveTups = _get(VERSION_FILTER_LATEST, FLAVOR_FILTER_ALL, ['trove:foo:run'])
        assert(len(troveTups) == 2)

        troveTups = _get(VERSION_FILTER_ALL, FLAVOR_FILTER_ALL, ['trove:foo:run'])

        # --all-versions doesn't really work with what-provides
        assert(len(troveTups) == 2)

    def testBuildLogDisplay(self):
        buildlog = 'This is test buildlog';
        bz2compressor = bz2.BZ2Compressor()
        bz2compressor.compress(buildlog)
        contents = bz2compressor.flush()
        
        self.addComponent('foo:runtime')
        self.addComponent('foo:debuginfo', [('/usr/bin/foo', rephelp.RegularFile(contents=contents, tags=['buildlog']))])
        self.addCollection('foo', [(':runtime', True), (':debuginfo', False)])
        repos = self.openRepository()

        output = self.captureOutput(queryrep.displayTroves, self.cfg, ['foo'], [], [], 
                            queryrep.VERSION_FILTER_LATEST, queryrep.FLAVOR_FILTER_BEST, showBuildLog = True)
        self.assertEqual(output[1], buildlog)
        
    def testShowFile(self):
        contents1 = 'This is test content';
        contents2 = 'This is another test content';
        
        self.addComponent('foo:runtime', [('/usr/bin/foofile', contents1), ('/usr/bin/barfile', contents2)])
        self.addCollection('foo', [':runtime'])
        repos = self.openRepository()

        output = self.captureOutput(queryrep.displayTroves, self.cfg, ['foo'], [], [], 
                            queryrep.VERSION_FILTER_LATEST, queryrep.FLAVOR_FILTER_BEST, filesToShow = ['/usr/bin/barfile'])
        self.assertEqual(output[1], contents2)

    def testRdiff1(self):
        req1 = 'soname: ELF32/libfoo1(blah)'
        req2 = 'soname: ELF32/lib/foo2(blah)'
        req3 = 'soname: ELF32/lib/foo3(blah) trove:bar(1)'
        prov1 = "trove:bar(1) trove:baz(1)"
        prov2 = "trove:baz(1) trove:bloop(1)"
        prov3 = "trove:bloop(2) trove:bar(1)"
        buildReqs1 = [ ('py', '1', 'is: x'), ('by', '1', 'is: y'),
            ('ty', '1', 'is: z')]
        buildReqs2 = [ ('py', '1', 'is: x'), ('my', '1', 'is: y'),
            ('by', '2', 'is: z')]
        rf1 = rephelp.RegularFile(contents='1\n2\n3\n4\n5\n6\n7\n8\n',
            perms = 0644, provides = prov1, requires = req1,
            mtime = 1136921017,)
        rf2 = rephelp.RegularFile(contents='1\n2\n4\n5\n6\n7\n8\n9\n',
            perms = 0755, provides = prov2, requires = req2,
            mtime = 1136921317, tags=['tag2', 'tag1', 'tag3'])
        rf3 = rephelp.RegularFile(contents='1\n2\n4\n5\n6\n7\n8\n10\n',
            perms = 0400, provides = prov3, requires = req3,
            mtime = 1136921017)
        # rf5 differs from rf1 just by tags
        rf5 = rephelp.RegularFile(contents='1\n2\n3\n4\n5\n6\n7\n8\n',
            perms = 0644, provides = prov1, requires = req1,
            mtime = 1136921017, tags=['tag2', 'tag1', 'tag3'])
        self.addComponent('foo:run', '1', 'is:x86',
            [('/usr/bin/foo', rf1),
             ('/usr/bin/bar', rf2),
             ('/usr/bin/file1', rf1),
             ])
        self.addComponent('foo:supdoc', '1', 'is:x86',
                          [('/usr/share/doc/foo1', rf1)])
        self.addCollection('foo', '1', 
                           [(x, '1', 'is:x86') for x in [':run', ':supdoc']],
                           buildReqs=buildReqs1)

        self.addComponent('foo:run', '2', 'is:x86_64',
            [('/usr/bin/foo', rf2),
             ('/usr/bin/file1', rf5),
             ('/usr/bin/baz', rf3),])
        self.addComponent('foo:doc', '2', 'is:x86_64',
                          [('/usr/share/doc/foo2', rf2)])
        self.addCollection('foo', '2', 
                           [(x, '2', 'is:x86_64') for x in [':run', ':doc']],
                           buildReqs=buildReqs2)

        # Force search flavor to x86_64 to get consistent output on x86
        self.cfg.flavor = [deps.parseFlavor('is: x86 x86_64')]
        repos = self.openRepository()

        troveSpec = 'foo=1[is:x86]--2[is:x86_64]'

        ret, outs = self._rdiff(troveSpec)
        self.assertEqual(outs, expOutput1noargs)

        self.cfg.fullFlavors = True
        ret, outs = self._rdiff(troveSpec)
        self.assertEqual(outs, expOutput1fullFlavors)
        self.cfg.fullFlavors = False

        self.cfg.fullVersions = True
        ret, outs = self._rdiff(troveSpec)
        self.assertEqual(outs, expOutput1fullVersions)
        self.cfg.fullVersions = False

        ret, outs = self._rdiff(troveSpec, ls = True)
        self.assertEqual(outs, expOutput1withFiles)

        ret, outs = self._rdiff(troveSpec, fileVersions = True)
        self.assertEqual(outs, expOutput1withFileVersions)

        ret, outs = self._rdiff(troveSpec, lsl = True)
        self.assertEqual(outs, expOutput1withFilesStat)

        ret, outs = self._rdiff(troveSpec, tags = True)
        self.assertEqual(outs, expOutput1withFileTags)

        # Diffing against ourselves
        troveSpec = 'foo=1[is:x86]--1[is:x86]'
        ret, outs = self._rdiff(troveSpec, tags = True)
        self.assertEqual(outs, 'Identical troves\n')

    def testRdiff2(self):
        # Test showing of troves with no changes
        req1 = 'soname: ELF32/lib/foo3(blah) trove:bar(1)'
        req2 = 'soname: ELF32/lib/foo2(blah)'
        prov1 = "trove:bar(1) trove:baz(1)"
        prov2 = "trove:bar(1) trove:baz(1) soname: ELF32/lib/foo2(blah)"
        rf1 = rephelp.RegularFile(contents='1\n2\n3\n4\n5\n6\n7\n8\n',
            perms = 0644, provides = prov1, requires = req1,
            mtime = 1176921017,)
        rf2 = rephelp.RegularFile(contents='1\n2\n4\n5\n6\n7\n8\n9\n',
            perms = 0755, provides = prov2, requires = req2,
            mtime = 1176921317, tags=['tag2', 'tag1', 'tag3'])

        self.addComponent('foo:run', '1', 'is:x86', [('/usr/bin/foo', rf1)])
        self.addComponent('foo:supdoc', '1', 'is:x86', [('/usr/doc/foo1', rf2)])
        self.addCollection('foo', '1', 
                           [(x, '1', 'is:x86') for x in [':run', ':supdoc']])
        self.addComponent('bar:run', '1', [ ('/usr/bin/bar', rf1) ])
        self.addCollection('bar', '1', [':run'])
        self.addCollection('group-bar', '1', ['bar'])

        self.addCollection('group-foo', '1',
                           [('foo', '1', 'is:x86'), 'group-bar'])

        self.addComponent('foo:run', '2', 'is:x86', [('/usr/bin/foo', rf1)])
        self.addComponent('foo:doc', '2', 'is:x86', [('/usr/doc/foo1', rf2)])
        self.addCollection('foo', '2',
                           [(x, '2', 'is:x86') for x in [':run', ':doc']])

        self.addCollection('group-foo', '2', [('foo', '2', 'is:x86'),
                                              ('group-bar', '1', '')])

        troveSpec = 'group-foo=1--2'

        ret, outs = self._rdiff(troveSpec)
        self.assertEqual(outs, expOutput2)

    def testRdiff3(self):
        # Have a file change from regular file to symbolic link
        rf1 = rephelp.RegularFile(contents='1\n2\n3\n4\n5\n6\n7\n8\n',
            perms = 0644, mtime = 1136921017)
        lf1 = rephelp.Symlink("/etc/passwd")

        self.addComponent('foo:run', '1', [('/usr/bin/foo', rf1)])
        self.addCollection('foo', '1', [':run'])

        self.addComponent('foo:run', '2',
            [('/etc/passwd', rf1), ('/usr/bin/foo', lf1)])
        self.addCollection('foo', '2', [':run'])

        ret, outs = self._rdiff('foo=1--2', lsl = True)
        #re.sub("Symbolic", "<TIMESTRING> (Symbolic", outs)
        outs = re.sub(" [0-9]*-[0-9]*-[0-9]* [0-9]*:[0-9]*:[0-9]* ", " <TIMESTRING  TIMESTAMP> ", outs)
        self.assertEqual(outs, expOutput3)

    def testRdiff4(self):
        # test trove dependencies

        req1 = 'soname: ELF32/lib/foo3(blah) trove:bar(1)'
        req2 = 'soname: ELF32/lib/foo2(blah)'
        prov1 = "trove:bar(1) trove:baz(1)"
        prov2 = "trove:bar(1) trove:baz(1) soname: ELF32/lib/foo2(blah)"
        rf1 = rephelp.RegularFile(contents='1\n2\n3\n4\n5\n6\n7\n8\n',
            perms = 0644, provides = prov1, requires = req1,
            mtime = 1176921017,)
        rf2 = rephelp.RegularFile(contents='1\n2\n4\n5\n6\n7\n8\n9\n',
            perms = 0755, provides = prov2, requires = req2,
            mtime = 1176921317, tags=['tag2', 'tag1', 'tag3'])

        self.addComponent('foo:run', '1', [('/usr/bin/foo', rf1)])
        self.addCollection('foo', '1', [':run'])

        self.addComponent('foo:run', '2', [('/usr/bin/foo', rf2)])
        self.addCollection('foo', '2', [':run'])

        ret, outs = self._rdiff('foo=1--2')
        self.assertEqual(outs, expOutput4)

        ret, outs = self._rdiff('foo:run=1--2', deps = True)
        self.assertEqual(outs, expOutput4withTroveDeps)

    def testRdiff5(self):
        # CNY-1605
        # Create two flavors of the same trove and add them to the same group

        flv1 = '~ssl'
        flv2 = '~!ssl'

        rf11 = rephelp.RegularFile(contents='1\n2\n3\n4\n5\n6\n7\n8\n',
                                   flavor=flv1)
        rf12 = rephelp.RegularFile(contents='1\n2\n4\n5\n6\n7\n8\n9\n',
                                   flavor=flv2)
        rf21 = rephelp.RegularFile(contents='1\n2\n4\n5\n6\n7\n8\n9\n',
                                   flavor=flv1)
        rf22 = rephelp.RegularFile(contents='1\n2\n3\n4\n5\n6\n7\n8\n',
                                   flavor=flv2)

        files = [ (rf11, rf12), (rf21, rf22) ]

        for v, fileobjs in zip([ '1', '2' ], files):
            file1 = fileobjs[0]
            file2 = fileobjs[1]

            t1 = self.addComponent('foo:run', v, fileContents=[('/bin/foo', file1)])
            p1 = self.addCollection('foo', v, [(':run', v, t1.getFlavor())])

            t2 = self.addComponent('foo:run', v, fileContents=[('/bin/foo', file2)])
            p2 = self.addCollection('foo', v, [(':run', v, t2.getFlavor())])

            self.addCollection('group-foo', v,
                [('foo', v, flv1), ('foo', v, flv2)])

        troveSpec = 'group-foo=1--2'

        self.cfg.fullFlavors = True
        ret, outs = self._rdiff(troveSpec)
        self.cfg.fullFlavors = False
        self.assertEqual(outs, expOutput5)

    def testRdiff6(self):
        # Test that added and removed troves show up properly

        self.addComponent('foo:run', '1', filePrimer=1)
        self.addCollection('foo', '1', [':run'])

        self.addComponent('erased:run', '1', filePrimer=1)
        self.addCollection('erased', '1', [':run'])

        self.addComponent('added:run', '1', filePrimer=1)
        self.addCollection('added', '1', [':run'])

        self.addCollection('group-foo', '1',
                           [ ('foo', '1'), ('erased', '1') ])
        self.addCollection('group-foo', '2',
                           [ ('foo', '1'), ('added', '1') ])

        troveSpec = 'group-foo=1--2'

        ret, outs = self._rdiff(troveSpec)
        self.assertEqual(outs, expOutput6)

    def testRdiff8(self):
        # CNY-1753
        # Different files living on different branches
        raise testhelp.SkipTestException("Unable to reproduce CNY-1753 in a test case")
        # Manifested in running conary rdiff
        # mkinitrd=conary.rpath.com@rpl:1--usplash.rb.rpath.com@rpl:1

        rf1 = rephelp.RegularFile(contents='\000\001\002\003',
            perms = 0644, mtime = 1176921017,)
        rf2 = rephelp.RegularFile(contents='\000\001\003\005',
            perms = 0644, mtime = 1176921317,)

        v1 = versions.ThawVersion('/localhost@rpl:1/1:1-1-1')
        v2 = versions.ThawVersion('/localhost1@rpl:2/2:2-2-2')

        self.openRepository()
        self.openRepository(1)

        self.addComponent('foo:run', v1, [('/bin/foo', rf1)])
        self.addCollection('foo', v1, [':run'])

        self.addComponent('foo:run', v2, [('/bin/foo', rf2)])
        self.addCollection('foo', v2, [':run'])

        troveSpec = cmdline.parseChangeList('foo=%s--%s' % (v1, v2))[0]
        ret, outs = self.captureOutput(queryrep.diffTroves,
            self.cfg, troveSpec)
        self.assertEqual(outs, '')

    def testRdiff9(self):
        """Binary changes to config; using --diff"""
        rf1 = rephelp.RegularFile(contents='1\n2\n3\n4\n5\n6\n7\n8\n',
            perms = 0644, mtime = 1136921017, config=False)
        rf2 = rephelp.RegularFile(contents='1\n2\n4\n5\n6\n7\n8\n9\n',
            perms = 0644, mtime = 1136921317, config=True)

        self.addComponent('foo:config', '1', [('/etc/foo', rf1)])
        self.addComponent('foo:config', '2', [('/etc/foo', rf2)])
        ret, outs = self._rdiff('foo:config=1--2', asDiff=True)
        self.assertEqual(outs, expOutput9)

expOutput1noargs = """\
Update  foo(:run) (1-1-1[is: x86] -> 2-1-1[is: x86_64])
Install foo:doc=2-1-1
Erase   foo:supdoc=1-1-1
"""

expOutput1fullFlavors = """\
Update  foo(:run) (1-1-1[is: x86] -> 2-1-1[is: x86_64])
Install foo:doc=2-1-1[is: x86_64]
Erase   foo:supdoc=1-1-1[is: x86]
"""

expOutput1fullVersions = """\
Update  foo(:run) (/localhost@rpl:linux/1-1-1[is: x86] -> /localhost@rpl:linux/2-1-1[is: x86_64])
Install foo:doc=/localhost@rpl:linux/2-1-1
Erase   foo:supdoc=/localhost@rpl:linux/1-1-1
"""

expOutput1withFiles = """\
  /usr/share/doc/foo2
  /usr/bin/bar
  /usr/bin/baz
  /usr/bin/file1
  /usr/bin/foo
  /usr/share/doc/foo1
"""

expOutput1withFileVersions = """\
  /usr/share/doc/foo2    2-1-1
  /usr/bin/bar    1-1-1
  /usr/bin/baz    2-1-1
  /usr/bin/file1    2-1-1
  /usr/bin/foo    2-1-1
  /usr/share/doc/foo1    1-1-1
"""

expOutput1withFilesStat = """\
   New -rwxr-xr-x    1 root     root           16 2006-01-10 19:28:37 UTC /usr/share/doc/foo2
   Del -rwxr-xr-x    1 root     root           16 2006-01-10 19:28:37 UTC /usr/bin/bar
   New -r--------    1 root     root           17 2006-01-10 19:23:37 UTC /usr/bin/baz
   Mod -rw-r--r--    1 root     root           16 2006-01-10 19:23:37 UTC /usr/bin/file1
   Mod -rwxr-xr-x    1 root     root           16 2006-01-10 19:28:37 UTC /usr/bin/foo
   Del -rw-r--r--    1 root     root           16 2006-01-10 19:23:37 UTC /usr/share/doc/foo1
"""

expOutput1withFileTags = """\
  /usr/share/doc/foo2 {tag1 tag2 tag3}
  /usr/bin/bar {tag1 tag2 tag3}
  /usr/bin/baz
  /usr/bin/file1 {tag1 tag2 tag3}
  /usr/bin/foo {tag1 tag2 tag3}
  /usr/share/doc/foo1
"""

expOutput2 = """\
Update  foo(:run) (1-1-1 -> 2-1-1)
Install foo:doc=2-1-1
Erase   foo:supdoc=1-1-1
Update  group-foo (1-1-1 -> 2-1-1)
"""

expOutput3 = """\
   New -rw-r--r--    1 root     root           16 <TIMESTRING  TIMESTAMP> UTC /etc/passwd
   Mod lrwxrwxrwx    1 root     root           11 <TIMESTRING  TIMESTAMP> UTC /usr/bin/foo -> /etc/passwd
"""

expOutput4 = """\
Update  foo(:run) (1-1-1 -> 2-1-1)
"""

expOutput4withTroveDeps = """\
Update  foo:run (1-1-1 -> 2-1-1)\nProvides:\n  trove: bar(1)\n  trove: baz(1)\n  trove: foo:run\n  soname: ELF32/lib/foo2(blah)\n\nRequires:\n  soname: ELF32/lib/foo2(blah)\n
"""

expOutput5 = """\
Update  foo(:run) (1-1-1[~!ssl] -> 2-1-1[~!ssl])
Update  foo(:run) (1-1-1[~ssl] -> 2-1-1[~ssl])
Update  group-foo (1-1-1 -> 2-1-1)
"""

expOutput6 = """\
Install added(:run)=1-1-1
Erase   erased=1-1-1
Update  group-foo (1-1-1 -> 2-1-1)
"""

expOutput9 = """\
diff --git a/etc/foo b/etc/foo
--- a/etc/foo
+++ b/etc/foo
@@ -1,8 +1,8 @@
 1
 2
-3
 4
 5
 6
 7
 8
+9
"""

class MultiRepQueryTest(rephelp.RepositoryHelper):
    def setUp(self):
        rephelp.RepositoryHelper.setUp(self)
    def tearDown(self):
        self.servers.stopServer(1)
        self.servers.stopServer(0)

    def _rdiff(self, troveSpec, **kwargs):
        client = conaryclient.ConaryClient(self.cfg)
        return self.captureOutput(queryrep.rdiffCommand, self.cfg,
                                  client, client.getDatabase(), troveSpec,
                                  **kwargs)

    def _openRepository(self, idx, serverName="localhost"):
        # this could be left open from a previoius testsuite running
        label = versions.Label("%s@foo:bar" % serverName)
        self.servers.stopServer(idx)
        repo = self.openRepository(idx, serverName=[serverName])
        self.resetRepository(idx)
        self.addUserAndRole(repo, label, "user", "pass")
        repo.addAcl(label, "user", None, None, write=True, remove=True)
        return repo 

    def testRdiffMulti(self):
        # CNY-2544 - groups including troves from foreign repos
        r0 = self._openRepository(0, "localhost")
        r1 = self._openRepository(1, "otherhost")
        c = self.getRepositoryClient("user", "pass")

        self.addComponent("other:runtime", "/otherhost@foo:bar/9", repos = c)
        self.addComponent("other:lib", "/otherhost@foo:bar/9", repos = c)
        trv = self.addCollection("other", "/otherhost@foo:bar/9", [ ":runtime", ":lib"], repos = c)
        grpfuu = self.addCollection("group-fuu", "/localhost@foo:bar/1", [ trv.getNameVersionFlavor() ], repos = c)
        grpfoo1 = self.addCollection("group-foo", "/localhost@foo:bar/1", [ grpfuu.getNameVersionFlavor() ], repos = c)
        grpfoo2 = self.addCollection("group-foo", "/localhost@foo:bar/2", [ trv.getNameVersionFlavor() ], repos = c)

        ret, outs = self._rdiff(
            'group-foo=localhost@foo:bar/1--localhost@foo:bar/2')
        self.assertEqual(outs, expOutput7)
        

expOutput7 = """\
Update  group-foo (1-1-1 -> 2-1-1)
Erase   group-fuu=1-1-1
Install other(:lib :runtime)=9-1-1
"""
