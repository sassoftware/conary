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
import copy

#testsuite
from conary_test import rephelp

#conary
from conary.conaryclient import cmdline
from conary import conarycfg
from conary import trovetup
from conary import versions
from conary.cmds import updatecmd
from conary.deps import deps
from conary.local import database
from conary.repository import errors
from conary.repository import trovesource
from conary.repository.netrepos import versionops
from conary import errors as conaryerrors
from conary.versions import Label
from conary.deps.deps import parseFlavor


class FindTroveTest(rephelp.RepositoryHelper):

    def testFindTroves(self):
        repos = self.openRepository()
        # add test1:source
        self.addTestPkg(1)

        self.addComponent('test1:runtime', '/localhost@rpl:linux/1.0-1-1',
                             flavor='readline is:x86')
        self.addComponent('test1:runtime', '/localhost@rpl:linux/1.0-1-0/branch/1',
                             flavor='readline is:x86')
        self.addComponent('test1:runtime', '/localhost@rpl:linux/2.0-1-1',
                             flavor='~!readline is:x86')
        self.addComponent('test1:runtime', '/localhost@rpl:linux/2.0-1-0/branch/1',
                             flavor='readline is:x86')
        self.addComponent('test2:runtime', '/localhost@rpl:linux/1.0-1-1',
                             flavor='ssl is:x86')
        
        # test passing an invalid label as the versionStr
        oldPath = self.cfg.installLabelPath
        n = 'test1:runtime'
        src = 'test1:source'
        v1 = versions.VersionFromString('/localhost@rpl:linux/1.0-1-1')
        v2 = versions.VersionFromString('/localhost@rpl:linux/2.0-1-1')
        v1s = versions.VersionFromString('/localhost@rpl:linux/1.0-1')

        v1b = versions.VersionFromString('/localhost@rpl:linux/1.0-1-0/branch/1')
        v2b = versions.VersionFromString('/localhost@rpl:linux/2.0-1-0/branch/1')
        rl = deps.parseFlavor('readline is:x86')
        ssl = deps.parseFlavor('ssl is:x86')
        norl = deps.parseFlavor('~!readline is:x86')
        nodeps = deps.parseFlavor('')

        req = {
               ('test2:runtime', '', None): [('test2:runtime', v1, ssl)],
               ('test1:runtime', '', None): [(n, v2, norl)],
               ('test1:runtime', '', rl) : [(n, v1, rl)], 
               ('test1:runtime', '1.0-1-1', None): [(n, v1, rl)],
               ('test2:runtime', '1.0-1-1', None): [('test2:runtime', v1, ssl)],
               ('test1:runtime', '1.0', None): [(n, v1, rl)],
               ('test1:runtime', 'localhost@rpl:linux', None) : [(n, v2, norl)],
               ('test1:runtime', '@rpl:linux', None) : [(n, v2, norl)],
               ('test1:runtime', 'localhost@', None) : [(n, v2, norl)],
               ('test1:runtime', ':linux', None) :  [(n, v2, norl)], 
               ('test1:source', '/localhost@rpl:linux', None) :  [(src, v1s, 
                                                                    nodeps)], 
               ('test1:runtime', ':branch', None) : [(n, v2b, rl) ], 
               ('test1:runtime', '/localhost@rpl:linux', None) : [(n, v2, norl)], 
               ('test1:runtime', '/localhost@rpl:linux/1.0-1-1', None) : [(n, v1, rl)],
               ('test1:runtime', '/localhost@rpl:linux/1.0-1-1', rl) : [(n, v1, rl)],
               ('test1:runtime', 'localhost@rpl:branch', None) : [(n, v2b, rl)],

               ('test1:runtime', '@rpl:branch/1.0', None) : [(n, v1b, rl)],
               ('test1:runtime', ':branch/2.0', None) : [(n, v2b, rl)],
               ('test1:runtime', 'localhost@/1.0', None) : [(n, v1, rl)],
           }
                
        
        result = repos.findTroves(self.cfg.installLabelPath, req.keys(),
                                  self.cfg.flavor)
        reqcopy = copy.deepcopy(req)
        for trove in req:
            for foundTrove in result[trove]:
                assert(foundTrove in req[trove])
                req[trove].remove(foundTrove)
                self.assertTrue(isinstance(foundTrove, trovetup.TroveTuple))
            assert(not req[trove])

        # now search with a bogus primary flavor on the flavorPath
        x86_64 = deps.parseFlavor('is:x86_64')
        x86_64 = deps.overrideFlavor(self.cfg.flavor[0], x86_64)
        newFlavorPath = [x86_64] +  self.cfg.flavor + [x86_64]

        req = reqcopy
        troveSpec = ('test1:runtime', '/localhost@rpl:linux', None)
        result = repos.findTroves(self.cfg.installLabelPath, (troveSpec,), None)
        assert(set(result[troveSpec]) == set([(n,v1, rl), (n,v2,norl)]))

        # okay, now search with no flavor
        newFlavorPath = None
        result = repos.findTroves(self.cfg.installLabelPath, req.keys(),
                                  newFlavorPath)





        result = repos.findTroves(self.cfg.installLabelPath, req.keys(),
                                  self.cfg.flavor)

        req = [ 'nonesuch=/localhost@rpl:linux', 
                'nonesuch=/localhost@rpl:linux/1.0-1-1', 
                'nonesuch=1.0-1-1', 
                'nonesuch=localhost@rpl:linux' ] 
        try:
            troves = [updatecmd.parseTroveSpec(x) for x in req]
            result = repos.findTroves(self.cfg.installLabelPath, troves, None)
        except errors.TroveNotFound, msg:
            lines = ('%s' % msg).split('\n')
            for i, (n,v,f) in enumerate(troves):
                assert(n in lines[i + 1] and v in lines[i + 1])
        else:
            assert(0)
        
        try:
            repos.findTroves(self.cfg.installLabelPath, 
                             [('nonesuch', ':rpl/1.0', None)])

        except errors.TroveNotFound, msg:
            assert(str(msg) == 'revision 1.0 of nonesuch was not found'
                               ' on label(s) localhost@rpl:rpl')


        # ensure labels are only listed once in "not found on label(s) message
        try:
            result = repos.findTroves(self.cfg.installLabelPath,
                                      (('nonesuch', '1.0-1-1', None),
                                       ('nonesuch', '', None)),
                                      newFlavorPath)
        except errors.TroveNotFound, msg:
            assert(str(msg) == '2 troves not found:\nrevision 1.0-1-1 of nonesuch was not found on label(s) localhost@rpl:linux\nnonesuch was not found on path localhost@rpl:linux\n')
        else:
            assert(False)


        # test querying by invalid label
        try:
            repos.findTroves(self.cfg.installLabelPath,
                             [('test1:runtime', 'localhost@rpl:li:nux', None)],
                             None)
        except errors.TroveNotFound, msg:
            assert(str(msg) == 'invalid version localhost@rpl:li:nux')

        # test querying by invalid Release
        try:
            repos.findTroves(self.cfg.installLabelPath,
                             [('test1:runtime', '1-0-1-1', None)],
                             None)
        except errors.TroveNotFound, msg:
            assert(str(msg) == "too many '-' characters in release string")

        # can't have two slashes in a version string
        try:
            repos.findTroves(self.cfg.installLabelPath,
                             [('test1:runtime', ':rpl/devel/1.0', None)],
                             None)
        except errors.TroveNotFound, msg:
            assert(str(msg) == 'incomplete version string'
                               ' :rpl/devel/1.0 not allowed')

        try:
            repos.findTroves(self.cfg.installLabelPath,
                             [('test1:runtime', 'rpl/1.0', None)],
                             None)
        except errors.TroveNotFound, msg:
            assert(str(msg) == 'Illegal version prefix rpl/1.0'
                               ' for test1:runtime')

    def testNotGetLeaves(self):
        # make sure that when getLeaves is false, our version filtering
        # is returning all versions
        self.addComponent('test1:runtime', '2.0-1-1')
        self.addComponent('test1:runtime', '2.0-1-2')
        repos = self.openRepository()
        tups = repos.findTrove(self.cfg.installLabelPath,
                               ('test1:runtime', '2.0', None),
                               None, getLeaves=False)
        assert(len(tups) == 2)

        # now test the branch + ver matching
        db = database.Database(self.cfg.root, self.cfg.dbPath)
        self.addQuickDbTestPkg(db, 'test1:runtime', 
                               '/localhost@rpl:linux/1.0-1-1', '')


        tups = repos.findTrove(self.cfg.installLabelPath,
                               ('test1:runtime', '2.0', None),
                               affinityDatabase=db, getLeaves=False)
        assert(len(tups) == 2)

    def testAffinity(self):
        repos = self.openRepository()
        db = database.Database(self.cfg.root, self.cfg.dbPath)

        self.addComponent('test1:runtime', '/localhost@rpl:linux/1:1.0-1-1',
                             flavor='ssl,readline is:x86')
        self.addComponent('test1:runtime', 
                             '/localhost@rpl:linux/1:1.0-1-1/branch/2:1',
                             flavor='!ssl,!readline,foo is:x86')

        self.addComponent('test1:runtime', '/localhost@rpl:linux/2:2.0-1-1',
                             flavor='ssl,!readline,foo is:x86')
        self.addComponent('test1:runtime', 
                             '/localhost@rpl:linux/1:1.0-1-1/branch/3:2.0-1-1',
                             flavor='!ssl,!readline,foo is:x86')


        # install base package
        self.addQuickDbTestPkg(db, 'test1:runtime', 
                               '/localhost@rpl:linux/1.0-1-1',
                               flavor='!readline')

        # add a second base package
        self.addQuickDbTestPkg(db, 'test1:runtime', 
                               '/localhost@rpl:linux/1.0-1-1/branch/1',
                               flavor='!readline')

        # install test2:runtime, which is a local version
        self.addQuickDbTestPkg(db, 'test2:runtime', 
                               '/localhost@rpl:linux/1.0-1-1/local@local:COOK/1',
                               flavor='!readline')

        ssl_rl = deps.parseFlavor('ssl,readline is:x86')
        ssl_norl = deps.parseFlavor('ssl,!readline,foo is:x86')
        nossl_rl = deps.parseFlavor('!ssl,readline,foo is:x86')
        nossl_norl = deps.parseFlavor('!ssl,!readline,foo is:x86')
        ssl = deps.parseFlavor('ssl')
        rl = deps.parseFlavor('readline')
        n = 'test1:runtime'
        v1 = versions.VersionFromString('/localhost@rpl:linux/1.0-1-1')
        v1b = versions.VersionFromString('/localhost@rpl:linux/1.0-1-1/branch/1')
        v2 = versions.VersionFromString('/localhost@rpl:linux/2.0-1-1')
        v2b = versions.VersionFromString('/localhost@rpl:linux/1.0-1-1/branch/2.0-1-1')
        t1bno = (n, v1b, nossl_norl)
        t1 = (n, v1, ssl_rl)
        t1b = (n, v1b, rl)
        t2 = (n, v2, ssl_norl)
        t2b = (n, v2b, nossl_norl)
        req = {
               ('test1:runtime', '', None): [t2, t2b ],
               ('test1:runtime', None, ssl) : [t2], 
               ('test1:runtime', '1.0-1-1', None): [t1bno],
               ('test1:runtime', '2.0-1-1', None): [t2, t2b],
               ('test1:runtime', ':linux/2.0-1-1', None): [t2],
               ('test1:runtime', 'localhost@rpl:linux', None) : [t2],
                ('test1:runtime', '/localhost@rpl:linux', None) :  [t2], 
               ('test1:runtime', 
                 '/localhost@rpl:linux/2.0-1-1', None) : [t2],
               ('test2:runtime', '', None) : []
           }

        x86_64 = deps.parseFlavor('is:x86_64')

        fp_x86_64 = deps.overrideFlavor(self.cfg.flavor[0], x86_64)
        fp_ssl_norl = deps.overrideFlavor(self.cfg.flavor[0], ssl_norl)
        fp_nossl_rl = deps.overrideFlavor(self.cfg.flavor[0], nossl_rl)

        fp_nossl_norl = deps.overrideFlavor(self.cfg.flavor[0], nossl_norl)

        newFlavorPath = [fp_x86_64, fp_nossl_rl, fp_ssl_norl, fp_nossl_norl] 

        result = repos.findTroves(self.cfg.installLabelPath, req.keys(),
                                  newFlavorPath,
                                  affinityDatabase=db)
        reqcopy = copy.deepcopy(req)
        for trove in reqcopy:
            for foundTrove in result[trove]:
                assert(foundTrove in reqcopy[trove])
                reqcopy[trove].remove(foundTrove)
            assert(not reqcopy[trove])

        result = repos.findTroves(self.cfg.installLabelPath, req.keys(),
                                  newFlavorPath, 
                                  affinityDatabase=db,
                                  acrossLabels=True)
        reqcopy = copy.deepcopy(req)
        for trove in reqcopy:
            for foundTrove in result[trove]:
                assert(foundTrove in reqcopy[trove])
                reqcopy[trove].remove(foundTrove)
            assert(not reqcopy[trove])

    def testAffinity2(self):
        # test cases where the flavor of the trove is strong
        db = database.Database(self.cfg.root, self.cfg.dbPath)
        self.addComponent('test1:runtime', '1.0', 'is:x86(sse2)')
        self.addDbComponent(db, 'test1:runtime', '1.0', 'is:x86(sse2)')

        self.addComponent('test1:runtime', '2.0', 'is:x86(~!sse2)')

        flavor = [deps.parseFlavor('is:x86(~sse2)')]
        repos = self.openRepository()
        troveTups = repos.findTrove(self.cfg.buildLabel,
                                 ('test1:runtime', None, None), flavor,
                                 affinityDatabase=db)
        assert(len(troveTups) == 1)
        assert(troveTups[0][1].trailingRevision().version == '2.0')

    def testAffinity3(self):
        # two troves installed on the same label with different flavors,
        # both have upgrades available
        db = self.openDatabase()
        self.addQuickDbTestPkg(db, 'test1:runtime',
                               '/localhost@rpl:linux/1.0-1-1', 'ssl,!readline')
        self.addQuickDbTestPkg(db, 'test1:runtime',
                               '/localhost@rpl:linux/1.0-1-1', 'readline,!ssl')
        flavor = self.cfg.flavor[0]
        self.cfg.flavor = [deps.overrideFlavor(flavor, 
                                               deps.parseFlavor('is:x86')),
                           deps.overrideFlavor(flavor, 
                                               deps.parseFlavor('is:x86_64'))]
        self.addComponent('test1:runtime', '2.0-1-1', 'ssl,!readline is:x86')
        self.addComponent('test1:runtime', '2.0-1-1', 'readline,!ssl is:x86_64')
        repos = self.openRepository()
        result = repos.findTrove([], ('test1:runtime', None, None), 
                                 self.cfg.flavor,
                                 affinityDatabase=db)
        assert(len(result) == 2)

    def testBranchAffinityWhenFlavorSpecified(self):
        # test findTrove knows what branch to look on for an update
        # even when you search for something by flavor.

        db = database.Database(self.cfg.root, self.cfg.dbPath)
        self.addComponent('test1:runtime', '1.0-1-1')
        self.addComponent('test1:runtime', '2.0-1-1')
        self.addQuickDbTestPkg(db, 'test1:runtime', 
                               '/localhost@rpl:linux/1.0-1-1', '')
        repos = self.openRepository()
        labelPath = [versions.Label('localhost@foo:bar')]

        query = ('test1:runtime', None, deps.parseFlavor('foo'))

        result = repos.findTrove(labelPath, query, self.cfg.flavor, 
                                 affinityDatabase=db)
        assert(result)

    def testAcrossRepositories(self):
        self.cfg.flavor = [deps.parseFlavor('readline,ssl is:x86')]
        repos = self.openRepository()
        self.addComponent('test1:runtime', '/localhost@rpl:linux/1:1.0-1-1',
                             flavor='ssl')
        self.addComponent('test1:runtime', 
                             '/localhost@rpl:linux/1:1.0-1-1/branch/2:1',
                             flavor='ssl')

       
        v1 = versions.VersionFromString('/localhost@rpl:linux/1.0-1-1')
        v1b = versions.VersionFromString('/localhost@rpl:linux/1.0-1-1/branch/1')

        ssl = deps.parseFlavor('ssl')
        nossl = deps.parseFlavor('!ssl')
        t1 = ('test1:runtime', v1, ssl)
        t1nossl = ('test1:runtime', v1, nossl)
        t1b = ('test1:runtime', v1b, ssl)
        t1bnossl = ('test1:runtime', v1b, nossl)

        req = {('test1:runtime', '', None): [t1, t1b]}

        labelPath = [versions.Label('localhost@rpl:linux'),
                     versions.Label('localhost@rpl:branch') ]

        result = repos.findTroves(labelPath, req.keys(),
                                  self.cfg.flavor, 
                                  acrossLabels=True)

        reqcopy = copy.deepcopy(req)
        for trove in reqcopy:
            for foundTrove in result[trove]:
                assert(foundTrove in reqcopy[trove])
                reqcopy[trove].remove(foundTrove)
            assert(not reqcopy[trove])

        self.addComponent('test1:runtime', '/localhost@rpl:linux/1:1.0-1-1',
                             flavor='!ssl')
        self.addComponent('test1:runtime', 
                             '/localhost@rpl:linux/1:1.0-1-1/branch/2:1',
                             flavor='!ssl')
        flavorPath = self.cfg.flavor + [ deps.overrideFlavor(self.cfg.flavor[0],
                                                           nossl) ]

        req = {('test1:runtime', '', None): [t1, t1nossl]}
        result = repos.findTroves(labelPath, req.keys(),
                                  flavorPath, 
                                  acrossFlavors=True)

        reqcopy = copy.deepcopy(req)
        for trove in reqcopy:
            for foundTrove in result[trove]:
                assert(foundTrove in reqcopy[trove])
                reqcopy[trove].remove(foundTrove)
            assert(not reqcopy[trove])

        req = {('test1:runtime', '', None): [t1, t1nossl, t1b, t1bnossl]}
        result = repos.findTroves(labelPath, req.keys(),
                                  flavorPath, acrossLabels=True,
                                  acrossFlavors=True)

        reqcopy = copy.deepcopy(req)
        for trove in reqcopy:
            for foundTrove in result[trove]:
                assert(foundTrove in reqcopy[trove])
                reqcopy[trove].remove(foundTrove)
            assert(not reqcopy[trove])

    def testAffinityWhenTroveNotInRepository(self):
        # this makes sure that findTroves can cope with a trove that
        # is installed in the database disappearing from the repository
        repos = self.openRepository()
        db = database.Database(self.cfg.root, self.cfg.dbPath)
        self.addQuickDbTestPkg(db, 'test1:runtime', '/localhost@rpl:linux/1.0-1-1',
                               flavor='ssl,!readline is:x86')
        try:
            result = repos.findTroves(self.cfg.installLabelPath,
                                      [('test1:runtime', None, None)],
                                      self.cfg.flavor, 
                                      affinityDatabase=db)

        # the result should be no troves found on that label
        except errors.TroveNotFound, e:
            assert(str(e) == 'test1:runtime was not found on path localhost@rpl:linux')
        else:
            raise

    def testAffinityLocalBranch(self):
        # this makes sure that findTroves can cope with an affinity
        # trove that is on a local branch
        repos = self.openRepository()
        db = database.Database(self.cfg.root, self.cfg.dbPath)
        self.addQuickDbTestPkg(db, 'test1:runtime', "/foo.com@spc:bar/1.2-3/local@local:COOK/3",
                               flavor='ssl,!readline is:x86')
        result = repos.findTroves(self.cfg.installLabelPath,
                                  [('test1:runtime', None, None)],
                                  self.cfg.flavor, 
                                  affinityDatabase=db)
        assert(result == {('test1:runtime', None, None) : []})

    def testLabelPathErrorOrder(self):
        repos = self.openRepository()
        installLabelPath = conarycfg.CfgLabelList(
            [ versions.Label('localhost@rpl:%s' % x) for x in range(5) ])
        try:
            repos.findTroves(installLabelPath,
                             [('foo', None, None)],
                             self.cfg.flavor)
        except errors.TroveNotFound, msg:
            assert(str(msg) == 'foo was not found on path localhost@rpl:0,'
                               ' localhost@rpl:1, localhost@rpl:2,'
                               ' localhost@rpl:3, localhost@rpl:4')

        try:
            repos.findTroves(installLabelPath,
                             [('foo', '1', None)],
                             self.cfg.flavor)
        except errors.TroveNotFound, msg:
            assert(str(msg) == 'revision 1 of foo was not found on label(s)'
                               ' localhost@rpl:0, localhost@rpl:1,'
                               ' localhost@rpl:2, localhost@rpl:3,'
                               ' localhost@rpl:4')

    def testFindTroveFlavorPathAndLabelPath(self):
        self.addComponent('foo:run', '1', 'is:x86')
        self.addComponent('foo:run', '1', 'is:x86_64')
        self.addComponent('foo:run', ':branch/1', 'is:x86')
        installLabelPath = conarycfg.CfgLabelList(
                [ versions.Label('localhost@rpl:linux'),
                  versions.Label('localhost@rpl:branch') ] )
        flavorPath = [ deps.parseFlavor('is:x86_64'),
                       deps.parseFlavor('is:x86') ]
        repos = self.openRepository()
        repos.findTroves(installLabelPath,
                         [('foo:run', None, None)],
                         flavorPath, acrossLabels=True)

    def _testFindTroveByType(self, searchSource):
        repos = self.openRepository()
        t1 = self.addComponent('f:r', '1').getNameVersionFlavor()
        t2 = self.addComponent('f:r', '2', redirect=['blah:r']).getNameVersionFlavor()
        t3 = self.addComponent('f:r', '3').getNameVersionFlavor()
        self.markRemoved('f:r=3')


        def _find(n, v=None, all=False, present=False):
            if all:
                troveTypes = repos.TROVE_QUERY_ALL
            elif present:
                troveTypes = repos.TROVE_QUERY_PRESENT
            else:
                troveTypes = repos.TROVE_QUERY_NORMAL

            try:
                r = searchSource.findTrove(self.cfg.installLabelPath, 
                                    (n, v, None),
                                    self.cfg.flavor, troveTypes=troveTypes)
            except errors.TroveNotFound:
                r = []
            return sorted(r, key=lambda x: x[0])

        assert(_find('f:r') == [])
        assert(_find('f:r', '1') == [t1])
        assert(_find('f:r', '/localhost@rpl:linux') == [])
        assert(_find('f:r', '/localhost@rpl:linux/1-1-1') == [t1])
        assert(_find('f:r', 'localhost@rpl:linux/1-1-1') == [t1])
        assert(_find('f:r', '2') == [])
        assert(_find('f:r', '/localhost@rpl:linux/2-1-1') == [])
        assert(_find('f:r', '3') == [])
        assert(_find('f:r', '/localhost@rpl:linux/3-1-1') == [])

        assert(_find('f:r', present=True) == [t2])
        assert(_find('f:r', '1', present=True) == [t1])
        assert(_find('f:r', '/localhost@rpl:linux', present=True) == [t2])
        assert(_find('f:r', '/localhost@rpl:linux/1-1-1', present=True) == [t1])
        assert(_find('f:r', 'localhost@rpl:linux/1-1-1', present=True) == [t1])
        assert(_find('f:r', '2', present=True) == [t2])
        assert(_find('f:r', '/localhost@rpl:linux/2-1-1', present=True) == [t2])
        assert(_find('f:r', '3', present=True) == [])
        assert(_find('f:r', '/localhost@rpl:linux/3-1-1', present=True) == [])

        assert(_find('f:r', all=True) == [t3])
        assert(_find('f:r', '1', all=True) == [t1])
        assert(_find('f:r', '/localhost@rpl:linux', all=True) == [t3])
        assert(_find('f:r', '/localhost@rpl:linux/1-1-1', all=True) == [t1])
        assert(_find('f:r', 'localhost@rpl:linux/1-1-1', all=True) == [t1])
        assert(_find('f:r', '2', all=True) == [t2])
        assert(_find('f:r', '/localhost@rpl:linux/2-1-1', all=True) == [t2])
        assert(_find('f:r', '3', all=True) == [t3])
        assert(_find('f:r', '/localhost@rpl:linux/3-1-1', all=True) == [t3])

        assert(_find('f:r', present=True) == [t2])
        assert(_find('f:r', '1', present=True) == [t1])
        assert(_find('f:r', '/localhost@rpl:linux', present=True) == [t2])
        assert(_find('f:r', '/localhost@rpl:linux/1-1-1', present=True) == [t1])
        assert(_find('f:r', 'localhost@rpl:linux/1-1-1', present=True) == [t1])


        assert(_find('f:r', '3') == [])
        assert(_find('f:r', present=True) == [t2])
        assert(_find('f:r', all=True) == [t3])

    def testFindTroveByType(self):
        self.cfg.flavor = [deps.parseFlavor('readline,ssl is:x86')]
        repos = self.openRepository()
        self._testFindTroveByType(self.openRepository())
        self.resetRepository()
        source = trovesource.TroveSourceStack(trovesource.SimpleTroveSource(),
                                     self.openRepository())
        self._testFindTroveByType(source)

    def testFindTrovesWithNoLabelPath(self):
        def _test(repos, v, failStr=None, flavor=None):
            db = self.openDatabase()
            try:
                results = repos.findTroves(None, [('foo:run', v, None)],
                                           affinityDatabase=db)
            except conaryerrors.LabelPathNeeded, err:
                if not failStr:
                    raise
                assert(str(err) == failStr)
                return
            else:
                assert(results['foo:run', v, None])

        # findTroves should work in several cases even when there's no labelPath
        # in other cases it should give a readable error back.
        self.addComponent('foo:run', '1')
        self.addComponent('foo:run', ':branch/1')
        repos = self.openRepository()
        _test(repos, 'localhost@rpl:linux')
        _test(repos, '/localhost@rpl:linux')
        _test(repos, '', 'No search label path given and no label specified for trove foo:run - set the installLabelPath')
        _test(repos, '@rpl:1', 'No search label path given and partial label specified for trove foo:run=@rpl:1 - set the installLabelPath')
        _test(repos, ':1', 'No search label path given and partial label specified for trove foo:run=:1 - set the installLabelPath')
        _test(repos, 'localhost@', 'No search label path given and partial label specified for trove foo:run=localhost@ - set the installLabelPath')
        _test(repos, '1', 'No search label path given and no label specified for trove foo:run=1 - set the installLabelPath')
        self.updatePkg('foo:run=1')
        _test(repos, '')
        _test(repos, '1')
        _test(repos, ':1', 'No search label path given and partial label specified for trove foo:run=:1 - set the installLabelPath')
        self.updatePkg('foo:run=--:branch/1')
        _test(repos, '1')
        _test(repos, '1', 'No search label path given and no label specified for trove foo:run=1 - set the installLabelPath', flavor='blah')

    def testFindTrovesWithTarget(self):
        t1 = self.addComponent('foo:run', '1', 'is:x86')
        t2 = self.addComponent('foo:run', '1', 'is:x86 x86_64')
        repos = self.openRepository()
        results = repos.findTrove(self.cfg.buildLabel, ('foo:run', None, deps.parseFlavor('is:x86 target:x86_64')))
        self.assertEqual(results[0], t1.getNameVersionFlavor())

    def testFindTrovesWithFlavorPreferences(self):
        Flavor = deps.parseFlavor
        v1 = '/localhost@rpl:linux/1:1'
        v2 = '/localhost@rpl:linux/2:2'
        x86_64 = self.addComponent('foo:run', v1, 'is:x86_64').getNameVersionFlavor()
        x86 = self.addComponent('foo:run', v2, 'is:x86').getNameVersionFlavor()
        repos = self.openRepository()
        flavorPreferences=(Flavor('is:x86_64'), Flavor('is:x86'))
        repos.setFlavorPreferenceList(flavorPreferences)
        results = repos.findTrove(self.cfg.buildLabel,
                            ('foo:run', None, Flavor('ssl is:x86 x86_64')))
        assert(results == [x86_64])
        results = repos.findTrove(self.cfg.buildLabel,
                            ('foo:run', None, Flavor('ssl is:x86')))
        assert(results == [x86])
        repos.setFlavorPreferenceList(None)
        # default uses internal flavor preference list
        results = repos.findTrove(self.cfg.buildLabel, 
                            ('foo:run', None, Flavor('ssl is:x86 x86_64')))
        assert(results == [x86_64])

    def testFindTrovesNeverDropAnArchAffinity(self):
        raise testhelp.SkipTestException('Waiting on CNY-525')
        Flavor = deps.parseFlavor
        v1 = '/localhost@rpl:linux/1:1'
        v2 = '/localhost@rpl:linux/2:2'
        v3 = '/localhost@rpl:linux/3:3'
        installed = self.addComponent('foo:run', v1, '~!desktop is:x86').getNameVersionFlavor()
        x86 = self.addComponent('foo:run', v2, 'desktop is:x86').getNameVersionFlavor()
        x86_noflavor = self.addComponent('foo:run', v3, '').getNameVersionFlavor()

        self.updatePkg('foo:run=1', raiseError=True)

        repos = self.openRepository()
        repos.setFlavorPreferenceList([])
        db = self.openDatabase()
        results = repos.findTrove(self.cfg.buildLabel, ('foo:run', None, None),
                                  affinityDatabase=db)
        assert(len(results) == 1)
        assert(str(results[0][2]) == 'desktop is: x86')

        results = repos.findTrove(self.cfg.buildLabel, ('foo:run', 'localhost@rpl:linux', None),
                                  affinityDatabase=db)
        assert(len(results) == 1)
        assert(str(results[0][2]) == 'desktop is: x86')

        results = repos.findTrove(self.cfg.buildLabel, ('foo:run', '', Flavor('ssl is: x86')),
                                  affinityDatabase=db)
        #because we specified a flavor, affinity doesn't hold any more.
        # (but someday we'll add required flavors)
        assert(len(results) == 1)
        assert(str(results[0][2]) == '')
        results = repos.findTrove(self.cfg.buildLabel, ('foo:run', '/localhost@rpl:linux', None),
                                  affinityDatabase=db)
        assert(len(results) == 1)
        assert(str(results[0][2]) == 'desktop is: x86')

    def testFindTrovesNeverDropAnArchAffinityMultiples(self):
        raise testhelp.SkipTestException('Waiting on CNY-525')
        Flavor = deps.parseFlavor
        v1 = '/localhost@rpl:linux/1:1'
        v2 = '/localhost@rpl:linux/2:2'
        v3 = '/localhost@rpl:linux/3:3'
        v1b = '/localhost@rpl:branch/1:1'
        v2b = '/localhost@rpl:branch/2:2'
        v3b = '/localhost@rpl:branch/3:3'

        installed = self.addComponent('foo:run', v1, '~!desktop is:x86').getNameVersionFlavor()
        installed = self.addComponent('foo:run', v1b, '~!desktop is:x86 x86_64').getNameVersionFlavor()

        x86 = self.addComponent('foo:run', v2, 'desktop is:x86').getNameVersionFlavor()
        x86_noflavor = self.addComponent('foo:run', v3, '').getNameVersionFlavor()

        x86 = self.addComponent('foo:run', v2b, 'desktop is:x86 x86_64').getNameVersionFlavor()
        x86_noflavor = self.addComponent('foo:run', v3b, '').getNameVersionFlavor()

        curFlavor = self.cfg.flavor[0]
        self.cfg.flavor = [ deps.overrideFlavor(curFlavor, Flavor('is:x86')),
                            deps.overrideFlavor(curFlavor, Flavor('!ssl is:x86 x86_64'))]
        self.updatePkg('foo:run=:branch/1[!ssl is:x86 x86_64]', raiseError=True)
        self.updatePkg('foo:run=:linux/1', raiseError=True, keepExisting=True)

        repos = self.openRepository()
        db = self.openDatabase()
        results = repos.findTrove(self.cfg.buildLabel, ('foo:run', None, None), self.cfg.flavor,
                                  affinityDatabase=db)
        assert(len(results) == 2)
        assert(set([str(x[1].trailingRevision()) for x in results]) == set(['2-1-1']))
        results = repos.findTrove(self.cfg.buildLabel, ('foo:run', 'localhost@', None), self.cfg.flavor,
                                  affinityDatabase=db)
        # you'd think localhost@ would update localhost@rpl:branch, but
        # the labelpath isn't changed to include other packages
        assert(len(results) == 1)
        assert(set([str(x[1].trailingRevision()) for x in results]) == set(['2-1-1']))
        assert(set([str(x[1].trailingLabel()) for x in results]) == set(['localhost@rpl:linux']))


    def testFindTrovesNeverDropAnArchAffinityMultiples2(self):
        raise testhelp.SkipTestException('Waiting on CNY-525')
        Flavor = deps.parseFlavor
        v1 = '/localhost@rpl:linux/1:1'
        v2 = '/localhost@rpl:linux/2:2'
        v3 = '/localhost@rpl:linux/3:3'

        installed = self.addComponent('foo:run', v1, '~!desktop is:x86').getNameVersionFlavor()
        installed = self.addComponent('foo:run', v1, '~!desktop is:x86_64').getNameVersionFlavor()
        x86 = self.addComponent('foo:run', v2, 'desktop is:x86').getNameVersionFlavor()
        noflavor = self.addComponent('foo:run', v3, '').getNameVersionFlavor()
        x86_64 = self.addComponent('foo:run', v2, 'desktop is:x86_64').getNameVersionFlavor()
        self.updatePkg('foo:run=:linux/1[is:x86]', raiseError=True)
        self.updatePkg('foo:run=:linux/1[is:x86_64]', raiseError=True, keepExisting=True)
        repos = self.openRepository()
        db = self.openDatabase()
        results = repos.findTrove(self.cfg.buildLabel, ('foo:run', None, None), self.cfg.flavor,
                                  affinityDatabase=db)
        assert(len(results) == 2)
        assert(set([str(x[1].trailingRevision()) for x in results]) == set(['2-1-1']))

        results = repos.findTrove(self.cfg.buildLabel, ('foo:run', ':linux', None), self.cfg.flavor,
                                  affinityDatabase=db)
        assert(len(results) == 2)
        assert(set([str(x[1].trailingRevision()) for x in results]) == set(['2-1-1']))

    def testFindTrovesWithAffinityAndMultipleLabels(self):
        repos = self.openRepository()
        self.addComponent('foo:runtime=1[is:x86]')
        self.updatePkg('foo:runtime')
        db = self.openDatabase()
        installLabelPath = [versions.Label('localhost@rpl:branch'),
                            versions.Label('localhost@foo:linux')]
        results = repos.findTrove(installLabelPath, ('foo:runtime', ':linux', 
                                                     None),
                                  self.cfg.flavor, affinityDatabase=db)

    def testRebuildLatest(self):
        # tests findTrove afetr a latest rebuild
        repos = self.openRepository()
        trv1 = self.addComponent('foo:run', "1")
        ret = repos.findTrove(self.cfg.installLabelPath, ('foo:run', None, None),
                              troveTypes=repos.TROVE_QUERY_NORMAL)
        self.assertEqual(trv1.getNameVersionFlavor(), ret[0])
        
        trv2 = self.addComponent('foo:run', "2")
        ret = repos.findTrove(self.cfg.installLabelPath, ('foo:run', None, None),
                              troveTypes=repos.TROVE_QUERY_NORMAL)
        self.assertEqual(trv2.getNameVersionFlavor(), ret[0])
        
        trv3 = self.addComponent('foo:run', "0", redirect=['bar:run'])
        ret = repos.findTrove(self.cfg.installLabelPath, ('foo:run', None, None),
                              troveTypes=repos.TROVE_QUERY_PRESENT)
        self.assertEqual(trv3.getNameVersionFlavor(), ret[0])
        self.assertRaises(errors.TroveNotFound, repos.findTrove,
                          self.cfg.installLabelPath, ('foo:run', None, None),
                          troveTypes=repos.TROVE_QUERY_NORMAL)
        # there are so many things wrong with poking so deeply inside
        # the rephelp structures... 
        db = self.servers.getServer(0).reposDB.connect()
        cu = db.cursor()
        cu.execute('DELETE FROM LatestCache')
        latest = versionops.LatestTable(db)
        latest.rebuild()
        db.commit()
        ret = repos.findTrove(self.cfg.installLabelPath, ('foo:run', None, None),
                              troveTypes=repos.TROVE_QUERY_PRESENT)
        self.assertEqual(trv3.getNameVersionFlavor(), ret[0])
        self.assertRaises(errors.TroveNotFound, repos.findTrove,
                          self.cfg.installLabelPath, ('foo:run', None, None),
                          troveTypes=repos.TROVE_QUERY_NORMAL)

    def testAffinityWithOneLocal(self):
        db = self.openDatabase()
        self.addDbComponent(db, 'foo:run', '1-1-1', 'is:x86')
        self.addDbComponent(db, 'foo:run', '/local@local:COOK/1:1-1-1', 'is:x86_64')
        self.addComponent('foo:run', '1', 'is:x86')
        repos = self.openRepository()
        trvs = repos.findTrove(self.cfg.installLabelPath, 
                               ('foo:run', None, None),
                                affinityDatabase=db)

    def testFindTrovesWithFlavorPreferences2(self):
        repos = self.openRepository()
        db = self.openDatabase()
        Flavor = deps.parseFlavor
        def _find(spec, getLeaves=True, bestFlavor=True):
            cfg = self.cfg
            spec = cmdline.parseTroveSpec(spec)
            tupList = repos.findTrove(cfg.installLabelPath, spec, cfg.flavor,
                                      affinityDatabase=db, getLeaves=getLeaves,
                                      bestFlavor=bestFlavor)
            tupList = sorted([('%s=%s[%s]' % (x[0],
                                          x[1].trailingRevision().getVersion(),
                                          x[2])) for x in tupList ])
            return tupList

        repos.setFlavorPreferenceList([Flavor('is: x86_64'), Flavor('is:x86')])
        self.cfg.flavor = [Flavor('is:x86(~cmov) x86_64(~cmov)')]

        self.addComponent('foo:run=1[is:x86]')
        self.addComponent('foo:run=1[]')
        assert(_find('foo:run') == ['foo:run=1[is: x86]'])
        assert(_find('foo:run', bestFlavor=False)
                == ['foo:run=1[]', 'foo:run=1[is: x86]'])

        self.addComponent('foo:run=2[]')
        assert(_find('foo:run') == ['foo:run=2[]'])
        assert(_find('foo:run', bestFlavor=False) == ['foo:run=1[is: x86]',
                                                        'foo:run=2[]'])
        assert(_find('foo:run', bestFlavor=False, getLeaves=False) == ['foo:run=1[]', 'foo:run=1[is: x86]', 'foo:run=2[]'])
        assert(_find('foo:run', getLeaves=False) == ['foo:run=1[is: x86]', 'foo:run=2[]'])

        self.addComponent('foo:run=2[is:x86_64]')
        assert(_find('foo:run') == ['foo:run=2[is: x86_64]'])
        assert(_find('foo:run', getLeaves=False, bestFlavor=False) == ['foo:run=1[]', 'foo:run=1[is: x86]', 'foo:run=2[]', 'foo:run=2[is: x86_64]'])
        #  a little odd - get the best flavor for each version.
        assert(_find('foo:run', getLeaves=False) == ['foo:run=1[]',
                                                     'foo:run=2[is: x86_64]'])
        assert(_find('foo:run', bestFlavor=False)
           == ['foo:run=1[is: x86]', 'foo:run=2[]', 'foo:run=2[is: x86_64]'])

        self.addComponent('foo:run=2[is:x86 x86_64]')
        assert(_find('foo:run') == ['foo:run=2[is: x86 x86_64]'])
        assert(_find('foo:run', bestFlavor=False)
           == ['foo:run=1[is: x86]', 'foo:run=2[]',
               'foo:run=2[is: x86 x86_64]', 'foo:run=2[is: x86_64]'])
        assert(_find('foo:run', getLeaves=False) == ['foo:run=1[]', 'foo:run=2[is: x86 x86_64]'])

        self.addComponent('foo:run=3[is:x86]')
        assert(_find('foo:run') == ['foo:run=2[is: x86 x86_64]'])
        assert(_find('foo:run', getLeaves=False) == ['foo:run=1[]', 'foo:run=2[is: x86 x86_64]'])
        assert(_find('foo:run', bestFlavor=False)
           == ['foo:run=2[]', 'foo:run=2[is: x86 x86_64]',
               'foo:run=2[is: x86_64]', 'foo:run=3[is: x86]'])

        self.addComponent('foo:run=3[]')
        assert(_find('foo:run') == ['foo:run=3[]'])
        assert(_find('foo:run', getLeaves=False) \
                == ['foo:run=1[]', 'foo:run=2[is: x86 x86_64]', 'foo:run=3[]'])
        assert(_find('foo:run', bestFlavor=False)
           == ['foo:run=2[is: x86 x86_64]',
               'foo:run=2[is: x86_64]', 'foo:run=3[]',
               'foo:run=3[is: x86]'])
        self.addComponent('foo:run=3[is:x86_64]')
        assert(_find('foo:run') == ['foo:run=3[is: x86_64]'])

        self.resetRepository()
        self.addComponent('foo:run=1[is:x86]')
        self.addComponent('foo:run=1[]')
        assert(_find('foo:run') == ['foo:run=1[is: x86]'])
        self.updatePkg('foo:run')
        assert(_find('foo:run') == ['foo:run=1[is: x86]'])

        self.addComponent('foo:run=2[]')
        assert(_find('foo:run') == ['foo:run=2[]'])

        self.addComponent('foo:run=2[is:x86_64]')
        assert(_find('foo:run') == ['foo:run=2[]'])

        self.addComponent('foo:run=2[is:x86 x86_64]')
        assert(_find('foo:run') == ['foo:run=2[]'])

        self.addComponent('foo:run=3[is:x86(cmov)]')
        assert(_find('foo:run') == ['foo:run=3[is: x86(cmov)]'])

        self.addComponent('foo:run=3[]')
        assert(_find('foo:run') == ['foo:run=3[is: x86(cmov)]'])
        self.updatePkg('foo:run')
        assert(_find('foo:run') == ['foo:run=3[is: x86(cmov)]'])

        # what about when you have an x86_64 package installed?
        self.resetRepository()
        self.addComponent('foo:run=1[is:x86_64]')
        self.addComponent('foo:run=1[]')
        self.updatePkg('foo:run[is:x86_64]')
        assert(_find('foo:run') == ['foo:run=1[is: x86_64]'])

        self.addComponent('foo:run=2[]')
        assert(_find('foo:run') == ['foo:run=2[]'])

        self.addComponent('foo:run=2[is:x86]')
        assert(_find('foo:run') == ['foo:run=2[]'])

        self.addComponent('foo:run=2[is:x86 x86_64]')
        assert(_find('foo:run') == ['foo:run=2[]'])

        self.addComponent('foo:run=2[is:x86_64]')
        assert(_find('foo:run') == ['foo:run=2[is: x86_64]'])

        self.addComponent('foo:run=3[is:x86_64(cmov)]')
        assert(_find('foo:run') == ['foo:run=3[is: x86_64(cmov)]'])

        self.addComponent('foo:run=3[]')
        assert(_find('foo:run') == ['foo:run=3[is: x86_64(cmov)]'])
        self.updatePkg('foo:run')
        assert(_find('foo:run') == ['foo:run=3[is: x86_64(cmov)]'])

    def testFindTrovesWithFlavorPreferences3(self):
        # Now we don't have any flavor preferences set, but we're
        # grabbing the flavor preferences based on our query.
        repos = self.openRepository()
        db = self.openDatabase()
        Flavor = deps.parseFlavor
        def _find(spec, getLeaves=True, bestFlavor=True):
            cfg = self.cfg
            spec = cmdline.parseTroveSpec(spec)
            tupList = repos.findTrove(cfg.installLabelPath, spec, cfg.flavor,
                                      affinityDatabase=db, getLeaves=getLeaves,
                                      bestFlavor=bestFlavor)
            tupList = sorted([('%s=%s[%s]' % (x[0],
                                          x[1].trailingRevision().getVersion(),
                                          x[2])) for x in tupList ])
            return tupList

        self.cfg.flavor = deps.parseFlavor('is:x86')
        self.addComponent('foo:run=1[is:x86 x86_64]')
        self.addComponent('foo:run=2[is:x86]')
        assert(_find('foo:run[is:x86 x86_64]') == ['foo:run=1[is: x86 x86_64]'])

    def testFindTrovesWithFlavorPreferences4(self):
        repos = self.openRepository()
        db = self.openDatabase()
        Flavor = deps.parseFlavor
        def _find(spec, getLeaves=True, bestFlavor=True):
            cfg = self.cfg
            spec = cmdline.parseTroveSpec(spec)
            tupList = repos.findTrove(cfg.installLabelPath, spec, cfg.flavor,
                                      affinityDatabase=db, getLeaves=getLeaves,
                                      bestFlavor=bestFlavor)
            tupList = sorted([('%s=%s[%s]' % (x[0],
                                          x[1].trailingRevision().getVersion(),
                                          x[2])) for x in tupList ])
            return tupList

        repos.setFlavorPreferenceList([Flavor('is: x86_64'), Flavor('is:x86')])
        self.cfg.flavor = [Flavor('is:x86(~sse,~sse2) x86_64')]

        self.addComponent('foo:run=1[is:x86(sse,sse2)]')
        self.addComponent('foo:run=1[is:x86 x86_64]')
        self.addComponent('foo:run=2[is:x86(sse,sse2)]')
        self.addComponent('foo:run=2[is:x86 x86_64]')
        self.addComponent('foo:run=3[is:x86(sse,sse2)]')
        self.addComponent('foo:run=3[is:x86 x86_64]')
        #   get the best flavor for each version.
        expectedResults = ['foo:run=1[is: x86 x86_64]',
                          'foo:run=2[is: x86 x86_64]',
                          'foo:run=3[is: x86 x86_64]']
        try:
            results = _find('foo:run', getLeaves=False)
            assert(results == expectedResults)
        except AssertionError:
           print "\nexpected: ['%s']" % "', '".join(expectedResults) 
           print "got ['%s']" % "', '".join(results)
           raise

    def testExactFlavors(self):
        self.addComponent('foo:run[~ssl]')
        self.addComponent('foo:run=2[~ssl,readline]')
        repos = self.openRepository()
        Flavor = deps.parseFlavor
        trv, = repos.findTrove(self.cfg.installLabelPath, 
                               ('foo:run', None, Flavor('~ssl')), 
                               self.cfg.flavor,
                                exactFlavors=True)
        assert(trv[2] == Flavor('~ssl'))
        self.assertRaises(errors.TroveNotFound,
                          repos.findTrove, self.cfg.installLabelPath, 
                          ('foo:run', None, Flavor('ssl')), self.cfg.flavor,
                          exactFlavors=True)
        self.assertRaises(errors.TroveNotFound,
                          repos.findTrove, self.cfg.installLabelPath, 
                          ('foo:run', None, None), self.cfg.flavor,
                           exactFlavors=True)

    def testFindRevision(self):
        self.addComponent('foo:run=1.1-1-1')
        self.addComponent('foo:run=1.1-1-2')
        repos = self.openRepository()
        troveList = repos.findTrove(self.cfg.installLabelPath, 
                                ('foo:run', '1.1', None))
        assert(len(troveList) == 1)

    def testFlavorSpecificationOverridesAffinity(self):
        self.addComponent('foo:run=1[!ssl]')
        self.addComponent('foo:run=2[!ssl,!readline]')
        self.addComponent('foo:run=2[ssl,!readline]')
        self.updatePkg('foo:run[!ssl]')
        db = self.openDatabase()
        repos = self.openRepository()
        self.checkUpdate('foo:run[!readline]',
                        ['foo:run[!ssl,!readline]'])
        self.checkUpdate('foo:run=/localhost@rpl:linux/2-1-1[!readline]',
                        ['foo:run[!ssl,!readline]'])
        self.checkUpdate('foo:run=/localhost@rpl:linux[!readline]',
                        ['foo:run[!ssl,!readline]'])

    def testErrorMessages(self):
        repos = self.openRepository()
        db = self.openDatabase()
        def _test(query, message):
            query = cmdline.parseTroveSpec(query)
            try:
                repos.findTrove(self.cfg.installLabelPath, query, self.cfg.flavor, affinityDatabase=db)
                assert(0)
            except errors.TroveNotFound, err:
                self.assertEquals(str(err), message)
        def _testWorks(query):
            query = cmdline.parseTroveSpec(query)
            repos.findTrove(self.cfg.installLabelPath, query, self.cfg.flavor)
            # just making sure this query returns something

        self.addComponent('foo:run=1[!ssl]')
        self.addComponent('foo:run=2[!ssl,!readline]')
        self.addComponent('foo:run=2[ssl,!readline]')
        self.addComponent('foo:run=2[ssl,!readline is:x86(!i686)]')
        self.addComponent('foo:run=2[is:x86_64(!i686)]')
        self.cfg.flavor = [deps.parseFlavor('readline,ssl is:x86(i686)')]
        _test('foo:run', 'foo:run was not found on path localhost@rpl:linux (Closest alternate flavors found: [~!readline], [~!ssl])')
        _testWorks('foo:run[~!ssl]')
        _testWorks('foo:run[~!readline]')
        _test('foo:run=/localhost@rpl:linux', 'foo:run was not found on branch /localhost@rpl:linux (Closest alternate flavors found: [~!readline], [~!ssl])')
        _test('foo:run=/localhost@rpl:linux/1-1-1', 'version /localhost@rpl:linux/1-1-1 of foo:run was not found (Closest alternate flavors found: [~!ssl])')
        # next test error messages when multiple labels are in the ILP
        self.cfg.installLabelPath = [ Label('localhost@rpl:branch'),
                                      Label('localhost@rpl:linux') ]
        self.addComponent('foo:run=:branch/1[bootstrap]')
        _test('foo:run=1', 
            'revision 1 of foo:run was not found on label(s) localhost@rpl:branch, localhost@rpl:linux (Closest alternate flavors found: [~bootstrap], [~!ssl])')
        _test('foo:run', 
            'foo:run was not found on path localhost@rpl:branch, localhost@rpl:linux (Closest alternate flavors found: [~bootstrap], [~!readline], [~!ssl])')

        # then test error messages when multiple flavors flavors are available
        self.cfg.flavor = [ parseFlavor('ssl,readline is:x86(i686)'),
                            parseFlavor('ssl,readline is:x86_64(i686)') ]
        _test('foo:run', 'foo:run was not found on path localhost@rpl:branch, localhost@rpl:linux (Closest alternate flavors found: [is: x86_64(~!i686)], [~bootstrap], [~!readline], [~!ssl])')

        # finally test error messages in these conditions when there's affinity
        # troves providing multiple labels or flavors (not both, as then we
        # really should find something)
        self.addDbComponent(db, 'foo:run', 'localhost@rpl:branch', 
                           '!readline,!ssl')
        self.addDbComponent(db, 'foo:run', 'localhost@rpl:branch', 
                            '!bootstrap')
        _test('foo:run=localhost@rpl:branch', 'foo:run was not found on path localhost@rpl:branch (Closest alternate flavors found: [~bootstrap])')

    def testErrorMessagesBiarch(self):
        repos = self.openRepository()
        def _test(query, message):
            query = cmdline.parseTroveSpec(query)
            try:
                repos.findTrove(self.cfg.installLabelPath, query, self.cfg.flavor)
                assert(0)
            except errors.TroveNotFound, err:
                self.assertEquals(str(err), message)

        self.addComponent('foo:run=1[is:x86 x86_64]')
        self.addComponent('foo:run=1[is:x86]')
        self.cfg.flavor = [ parseFlavor('ssl is:x86_64') ]
        _test('foo:run', 'foo:run was not found on path localhost@rpl:linux (Closest alternate flavors found: [is: x86 x86_64])')
        self.resetRepository()
        repos = self.openRepository()
        self.addComponent('foo:run=1[is:x86 x86_64]')
        self.addComponent('foo:run=1[!ssl]')
        _test('foo:run', 'foo:run was not found on path localhost@rpl:linux (Closest alternate flavors found: [~!ssl])')
