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


import os

from conary.deps import deps
from conary.local import database
from conary_test import rephelp
from conary.repository import changeset
from conary.repository import errors
from conary.repository.trovesource import (
        ChangesetFilesTroveSource,
        ChangeSetJobSource,
        SimpleTroveSource,
        stack,
        TroveListTroveSource,
        TroveSourceStack,
        )
from conary.versions import Label
from conary.versions import VersionFromString as VFS, ThawVersion


class SimplestFindTroveSource(SimpleTroveSource):

        def addTroves(self, *troveTups):
            for troveTup in troveTups:
                self.addTrove(troveTup[0], troveTup[1], troveTup[2])

        def reset(self):
            self._trovesByName = {}

def troveDictMatches(troveDict, troveTups):
    troveTups = set(troveTups)
    troveCount = len(troveTups)
    d = troveDict.copy()

    foundCount = 0
    for n, versionDict in troveDict.iteritems():
        for v, flavors in versionDict.iteritems():
            for f in flavors:
                if (n,v,f) not in troveTups:
                    return False
                foundCount += 1
    return foundCount == troveCount


class TroveSourceTest(rephelp.RepositoryHelper):

    def testVersionMatching(self):
        v1 = ThawVersion('/localhost@rpl:devel/1:1.0-1-1')
        v2 = ThawVersion('/localhost@rpl:devel/2:2.0-1-1')
        v1b = ThawVersion('/localhost@rpl:branch/3:1.0-1-1')
        v2b = ThawVersion('/localhost@rpl:branch/4:2.0-1-1')
        nodeps = deps.parseFlavor('')
        s = SimplestFindTroveSource()
        n = 'test'
        n2 = 'test2'

        t_v1 = (n, v1, nodeps)
        t_v2 = (n, v2, nodeps)
        t_v1b = (n, v1b, nodeps)
        t_v2b = (n, v2b, nodeps)

        t2_v1 = (n2, v1, nodeps)
        t2_v2 = (n2, v2, nodeps)
        t2_v1b = (n2, v1b, nodeps)
        t2_v2b = (n2, v2b, nodeps)

        lbl_a = v1.branch().label()
        lbl_b = v1b.branch().label()

        s.addTroves(t_v1, t_v2, t_v1b, t_v2b, t2_v1, t2_v2, t2_v1b, t2_v2b)

        d = s.getTroveLeavesByLabel({n : {lbl_a : None}, n2: {lbl_b: None}})
        assert(troveDictMatches(d, [t_v2, t2_v2b]))

        d = s.getTroveVersionsByLabel({n : {lbl_a : None}, n2 :{lbl_b:None}})
        assert(troveDictMatches(d, [t_v1, t_v2, t2_v2b, t2_v1b]))

        d = s.getTroveVersionsByLabel({n : {lbl_a : None, lbl_b: None}})
        assert(troveDictMatches(d, [t_v1, t_v2, t_v1b, t_v2b]))

        d = s.getTroveLeavesByBranch({n : {v1.branch() : None}, 
                                      n2: {v2b.branch() : None}})
        assert(troveDictMatches(d, [t_v2, t2_v2b]))

        d = s.getTroveVersionsByBranch({n : {v1.branch() : None},
                                       n2: {v2b.branch() : None}})
        assert(troveDictMatches(d, [t_v1, t_v2, t2_v1b, t2_v2b]))

        d = s.getTroveVersionFlavors({n  : {v1 : None, v2b : None},
                                      n2 : {v2 : None, v1b : None}})
        assert(troveDictMatches(d, [t_v1, t_v2b, t2_v2, t2_v1b]))

    def testFlavorMatchingAsRepository(self):
        # presumes that version matches work
        # and just tests flavoring
        # uses repository style 
        v1 = ThawVersion('/localhost@rpl:devel/1:1.0-1-1')
        v2 = ThawVersion('/localhost@rpl:devel/2:2.0-1-1')
        nodeps = deps.parseFlavor('')
        rl_nossl = deps.parseFlavor('readline,!ssl')
        rl_ssl = deps.parseFlavor('readline,ssl')
        norl_ssl = deps.parseFlavor('!readline,ssl')
        norl_nossl = deps.parseFlavor('!readline,!ssl')
        p_rl_nossl = deps.parseFlavor('~readline,~!ssl')
        p_rl_ssl = deps.parseFlavor('~readline,~ssl')
        p_norl_ssl = deps.parseFlavor('~!readline,~ssl')
        p_norl_nossl = deps.parseFlavor('~!readline,~!ssl')
        ssl = deps.parseFlavor('ssl')
        rl = deps.parseFlavor('readline')

        s = SimplestFindTroveSource()
        s.searchAsRepository()
        n = 'test:run'
        n2 = 'test2:run'
        t_v1_rl_nossl = (n, v1, rl_nossl)
        t_v1_norl_ssl = (n, v1, norl_ssl)
        t_v2_norl_nossl = (n, v2, norl_nossl)
        t_v2_rl_ssl = (n, v2, rl_ssl)
        lbl_a = v1.branch().label()

        s.addTroves(t_v1_rl_nossl, t_v1_norl_ssl, t_v2_norl_nossl, t_v2_rl_ssl)
        for trv in t_v1_rl_nossl, t_v1_norl_ssl, t_v2_norl_nossl, t_v2_rl_ssl:
            self.addComponent(*trv)
        repos = self.openRepository()

        assert(s.hasTroves([t_v1_rl_nossl])[0])
        for src in s, repos:
            d = s.getTroveLeavesByLabel({n : {lbl_a : [rl_nossl]}})
            assert(troveDictMatches(d, [t_v1_rl_nossl]))
        
            # if you specify flavors, and bestFlavor is False you all 
            # allowed flavors back,  not just the best match.  In this
            # case ~rl, does not match !readline, so we only get the rl,ssl
            # trove returned
            d = src.getTroveLeavesByLabel({n : {lbl_a : [p_rl_nossl]}}, 
                                        bestFlavor=False)
            assert(troveDictMatches(d, [t_v2_rl_ssl, t_v1_rl_nossl]))


            d = src.getTroveLeavesByLabel({n : {lbl_a : [rl_nossl, rl_ssl]}},
                                          bestFlavor=True)
            assert(troveDictMatches(d, [t_v1_rl_nossl, t_v2_rl_ssl]))

            d = src.getTroveLeavesByLabel({n : {lbl_a : [nodeps]}},
                                          bestFlavor=True)
            assert(troveDictMatches(d, [t_v2_norl_nossl]))

            d = src.getTroveLeavesByLabel({n : {lbl_a : [ssl]}},
                                          bestFlavor=True)
            assert(troveDictMatches(d, [t_v1_norl_ssl]))

            d = src.getTroveLeavesByLabel({n : {lbl_a : [p_norl_ssl]}},
                                          bestFlavor=True)
            assert(troveDictMatches(d, [t_v2_rl_ssl]))

        s.searchAsDatabase()

        # with database, only strong flavors are allowed, so only the
        # rl_nossl trove matches even with bestFlavor = False
        d = s.getTroveLeavesByLabel({n : {lbl_a : [p_rl_nossl]}}, 
                                    bestFlavor=False)
        assert(troveDictMatches(d, [t_v1_rl_nossl]))
        
        # same w/ by database
        d = s.getTroveLeavesByLabel({n : {lbl_a : [rl_nossl, rl_ssl]}})
        assert(troveDictMatches(d, [t_v1_rl_nossl, t_v2_rl_ssl]))

        d = s.getTroveLeavesByLabel({n : {lbl_a : [nodeps]}})
        # either of these are acceptable -- both flavors score the same
        # against nodeps.
        assert(troveDictMatches(d, [t_v2_norl_nossl]) or 
               troveDictMatches(d, [t_v2_rl_ssl]))

        # different w/ by database - partial match is good enough
        d = s.getTroveLeavesByLabel({n : {lbl_a : [ssl]}})
        assert(troveDictMatches(d, [t_v2_rl_ssl]))

        # different w/ by database - partial match is good enough
        d = s.getTroveLeavesByLabel({n : {lbl_a : [rl]}})
        assert(troveDictMatches(d, [t_v2_rl_ssl]))

        # different w/ by database - strong flavor means ~!rl must match
        # an entry with either !rl or ~!rl, cannot match rl 
        d = s.getTroveLeavesByLabel({n : {lbl_a : [p_norl_ssl]}})
        assert(troveDictMatches(d, [t_v1_norl_ssl]))

    def setUpGroupOddities(self):
        def _commitRecipe(name, recipe, new=True):
            oldDir = os.getcwd()
            os.chdir(self.workDir)
            if new:
                self.newpkg(name)
            else:
                self.checkout(name)
            os.chdir(name)
            self.writeFile('%s.recipe' % name, recipe)
            if new:
                self.addfile('%s.recipe' % name)
            self.commit()
            os.chdir(oldDir)

        groupTrove = """
class GroupFoo(GroupRecipe):
    name = 'group-foo'
    version = '1.0'
    checkPathConflicts = False
    clearBuildRequires()

    imageGroup = False

    def setup(r):
        r.setLabelPath('localhost@rpl:linux', 'localhost@rpl:branch1')
        r.createGroup('group-bam')
        r.addTrove('test1')
        r.addTrove('test2:runtime', None, '!readline')
        r.addTrove('test2:runtime', None, 'readline')
        r.addTrove('test3', None, 'is:x86_64')
        r.addTrove('test4-on-branch', source='test4')
        r.addNewGroup('group-bam')
        r.addTrove('group-bar', ':branch2', groupName='group-bam')
        Use.readline.setPlatform(True)
"""

        groupTrove2 = """
class GroupBar(GroupRecipe):
    name = 'group-bar'
    version = '1.0'
    clearBuildRequires()

    imageGroup = False

    def setup(r):
        r.createGroup('group-bar2')
        r.addTrove('test5')
        r.addNewGroup('group-bar2')
        r.addTrove('test1:runtime', ':linux', groupName='group-bar2')
        Use.readline.setPlatform(True)
"""
        self.addTestPkg(1)
        self.cookTestPkg(1)
        self.addTestPkg(1, content="#changed")

        self.addTestPkg(2, flags='Use.readline')
        self.overrideBuildFlavor('!readline')
        self.cookTestPkg(2)
        self.overrideBuildFlavor('readline')
        self.cookTestPkg(2)

        self.addTestPkg(3, flags='Arch.x86')
        self.overrideBuildFlavor('is:x86_64')
        self.cookTestPkg(3)

        self.overrideBuildFlavor('is:x86')

        self.cfg.buildLabel = Label('localhost@rpl:branch1')
        self.addTestPkg(4, content='r.PackageSpec("test4-on-branch", ".*")')
        self.cookTestPkg(4)
        self.cfg.buildLabel = Label('localhost@rpl:branch2')
        self.addTestPkg(5)
        self.cookTestPkg(5)
        _commitRecipe('group-bar', groupTrove2)
        self.cookFromRepository('group-bar')

        self.cfg.buildLabel = Label('localhost@rpl:linux')
        _commitRecipe('group-foo', groupTrove)
        self.cookFromRepository('group-foo')

        # add a trove with just a source version 
        # and one with neither source nor binary
        groupTrove += '''
        r.addTrove('test6')
        r.addTrove('test7')
'''
        self.addTestPkg(6)
        _commitRecipe('group-foo', groupTrove, new=False)

    def testFindTroves(self):
        self.repos = self.openRepository()
        self.setUpGroupOddities()
        ts = TroveListTroveSource(self.repos, [('group-foo', 
                                  VFS('/localhost@rpl:linux/1.0-1-1'), 
                                  deps.parseFlavor('readline is: x86_64'))])
        assert(len(ts.trovesByName('test4-on-branch')) == 1)
        assert(len(ts.trovesByName('test4-on-branch:runtime')) == 1)
        assert(len(ts.findTrove(None, ('test4-on-branch', None, None))) == 1)

        ts.searchAsDatabase()
        assert(len(ts.findTrove(None, ('test4-on-branch', 
                                       '/localhost@rpl:branch1', None))) == 1)

    def testFindTrovesNotAcrossLabels(self):
        # Even when acrossLabels is false, if the trove source is not
        # ordered, don't discriminate by label path.
        # This may result in odd results but it is really the only sane
        # thing to do.
        v = ThawVersion('/localhost@rpl:linux/1:1.0-1-1')
        v2 = ThawVersion('/localhost@rpl:branch/2:1.0-1-1')
        ts = SimpleTroveSource([('a:run', v, 
                                  deps.parseFlavor('')),
                                 ('a:run', v2, 
                                  deps.parseFlavor(''))])
        queries = [('a:run', None, None),
                   ('a:run', '1.0', None),
                   ('a:run', '1.0-1-1', None),
                   ('a:run', 'localhost@', None)]
        for q in queries:
            results = ts.findTroves(None, [q], 
                                     deps.parseFlavor(''), acrossLabels=False)
            assert(len(results[q]) == 2)

class ChangesetFilesTest(rephelp.RepositoryHelper):

    def testSimple(self):
        r1 = self.addComponent('test:runtime', '1.0-1-1', fileContents = [], requires='trove: test:doc')
        r2 = self.addComponent('test:runtime', '2.0-1-1', fileContents = [], requires='soname: ELF32/foo.so.0(a b c) trove: test:doc')
        d1 = self.addComponent('test:doc', '1.0-1-1', filePrimer = 1)
        l1 = self.addComponent('test:lib', '1.0-1-1', filePrimer = 2,
                               provides='soname: ELF32/foo.so.0(a b c d)')

        db = database.Database(':memory:', ':memory:')
        db.addTrove(r1)

        repos = self.openRepository()
        relCs = repos.createChangeSet([ 
                    ('test:runtime', (r1.getVersion(), r1.getFlavor()),
                                     (r2.getVersion(), r2.getFlavor()),
                     False) ])

        absCs = repos.createChangeSet([ 
                    ('test:doc',     (None, None),
                                     (d1.getVersion(), d1.getFlavor()),
                     False),
                    ('test:lib',     (None, None),
                                     (l1.getVersion(), l1.getFlavor()),
                     False) ])

        trvSrc = ChangesetFilesTroveSource(db, storeDeps=True)
        trvSrc.addChangeSet(relCs)
        trvSrc.addChangeSet(absCs)

        for trv in (r2, d1, l1):
            [ otherTrv ] = trvSrc.getTroves([ (trv.getName(), trv.getVersion(),
                                               trv.getFlavor()) ], 
                                            withFiles = False)
            assert(trv == otherTrv)

        suggMap = trvSrc.resolveDependencies(None, [r2.getRequires(),
                                                    r1.getRequires()])
        assert(suggMap[r1.getRequires()] == [[(d1.getName(), d1.getVersion(), 
                                              d1.getFlavor())]])
        suggs = suggMap[r2.getRequires()]
        assert([(d1.getName(), d1.getVersion(), d1.getFlavor())] in suggs)
        assert([(l1.getName(), l1.getVersion(), l1.getFlavor())] in suggs)


    def testUselessRelChangeset(self):
        # don't consider a new version of a trove 'present' if it requires
        # that a relative changeset be applied to a trove that we don't
        # have installed.
        r1 = self.addCollection('foo', '1', [':run'], createComps=True)
        r2 = self.addCollection('foo', '2', [':run'], createComps=True)

        db = database.Database(':memory:', ':memory:')
        db.addTrove(r1) # this doesn't add foo:run to the db!



        chg = ('foo', (r1.getVersion(), r1.getFlavor()),
                      (r2.getVersion(), r2.getFlavor()), False)
        repos = self.openRepository()
        relCs = repos.createChangeSet([chg], recurse=True)

        trvSrc = ChangesetFilesTroveSource(db, storeDeps=True)
        trvSrc.addChangeSet(relCs)
        fooRun = ('foo:run', r2.getVersion(), r2.getFlavor())
        assert(trvSrc.hasTroves([fooRun]) == [False])
        assert(trvSrc.hasTroves([r2.getNameVersionFlavor()]) == [True])

    def testCreateNewChangesetNotInSource(self):
        # Test creating a relative changeset when all we have in the
        # source is absolute changesets, and vice versa.
        r1 = self.addCollection('foo', '1', [':run'], createComps=True)
        self.addComponent('foo:run', '2', filePrimer=2)
        r2 = self.addCollection('foo', '2', [':run'])

        # create two absolute changesets, we want one relative one out
        absChg = [('foo', (None, None), (r1.getVersion(), r1.getFlavor()), True),
               ('foo', (None, None), (r2.getVersion(), r2.getFlavor()), True)]
        repos = self.openRepository()
        absCs = repos.createChangeSet(absChg, recurse=True)

        trvSrc = ChangesetFilesTroveSource(None)
        trvSrc.addChangeSet(absCs)

        relChg = ('foo', (r1.getVersion(), r1.getFlavor()),
                         (r2.getVersion(), r2.getFlavor()), False)
        newCs, remainder = trvSrc.createChangeSet([relChg], recurse=False,
                                       withFiles=False, withFileContents=False)
        assert(not remainder)
        trvCs = newCs.iterNewTroveList().next()
        assert(trvCs.getNewVersion() == r2.getVersion())
        assert(trvCs.getOldVersion() == r1.getVersion())

        # now we'll try to get a relative changeset out when we don't 
        # have the new trove that we'd need to make the relative changeset 
        # work
        badChg = ('foo', (r1.getVersion(), r1.getFlavor()),
                      (r2.getVersion(), deps.parseFlavor('foo')), False)
        newCs, remainder = trvSrc.createChangeSet([badChg], recurse=False,
                                       withFiles=False, withFileContents=False)
        assert(remainder)
        assert(newCs.isEmpty())

        # now we'll try to get a relative changeset out when we don't 
        # have the _old_ trove that we'd need to make the relative changeset 
        # work
        badChg = ('foo', (r1.getVersion(), deps.parseFlavor('foo')),
                      (r2.getVersion(), r2.getFlavor()), False)
        newCs, remainder = trvSrc.createChangeSet([badChg], recurse=False,
                                       withFiles=False, withFileContents=False)
        assert(remainder)
        assert(newCs.isEmpty())

        # Now test getting absolute changesets when the trovesource only has
        # relative ones.  This requires using the database to grab information
        # from the installed system about the old versions of troves.

        # First try creating those absolute changesets when there is
        # no information about the old trove on the system
        relCs = repos.createChangeSet([relChg], recurse=True)
        db = self.openDatabase()
        trvSrc = ChangesetFilesTroveSource(db)
        trvSrc.addChangeSet(relCs)
        newCs, remainder = trvSrc.createChangeSet(absChg, recurse=False,
                                       withFiles=False, withFileContents=False)
        assert(len(remainder) == 2)
        assert(newCs.isEmpty())

        # now create them when the old trove has been installed.
        self.updatePkg(['foo=%s' % r1.getVersion()])
        # FIXME - we need to recreate the changeset files source here -
        # apparently some information pertaining to what is installed is
        # cached
        trvSrc = ChangesetFilesTroveSource(db)
        trvSrc.addChangeSet(relCs)
        newCs, remainder = trvSrc.createChangeSet(absChg, recurse=False,
                                       withFiles=False, withFileContents=False)
        assert(not remainder)
        assert(len(list(newCs.iterNewTroveList())) == 2)

    def testTroveSourceStack(self):
        # Test the behaviour of TroveSourceStack instances

        s1 = SimplestFindTroveSource()
        s2 = SimplestFindTroveSource()
        s3 = SimplestFindTroveSource()

        nodeps = deps.parseFlavor('')

        count = 3

        names = []
        versions = []
        for v in range(count):
            sv = str(v + 1)
            versions.append(ThawVersion('/localhost@rpl:devel/%s:1.0-1-1' % sv))
            names.append("test%s" % sv)

        # Generate troves
        t = []
        for i in range(count):
            l = []
            t.append(l)
            name = names[i]
            for j in range(count):
                l.append((name, versions[j], nodeps))

        s1.addTroves(t[0][1])
        s2.addTroves(t[0][0], t[1][1])
        s3.addTroves(t[0][2], t[1][0], t[2][2])

        # Catch an early bug with a bad index
        try:
            ts1 = stack(s1)
        except Exception, e:
            # No exception expected
            self.fail("Caught exception: %s" % e)
        # The implementation of stack says so...
        self.assertEqual(ts1, s1)

        ret = ts1.hasTroves([t[0][0], t[1][1], t[2][2]])
        self.assertEqual(ret, [True, False, False])

        ts2 = stack(ts1)
        ret = ts1.hasTroves([t[0][2], t[1][0], t[2][1]])
        self.assertEqual(ret, [True, False, False])

        ts3 = stack(s1, s3)
        ts4 = stack(s2, ts3)
        # Same deal, but use the constructor for TroveSourceStack
        ts5 = TroveSourceStack(s2, ts3)

        for s in [s1, s2, s3]:
            self.assertTrue(ts4.hasSource(s))
            self.assertTrue(ts5.hasSource(s))

        for (rs, es) in zip(ts4.iterSources(), [s2, s1, s3]):
            self.assertEqual(rs, es)

        for (rs, es) in zip(ts5.iterSources(), [s2, s1, s3]):
            self.assertEqual(rs, es)

        # same source specified twice - obtain a stack
        ts6 = stack(s1, s1)
        self.assertNotEqual(ts6, s1)
        self.assertTrue(isinstance(ts6, TroveSourceStack))

        ts7 = stack(ts6, ts6)
        self.assertEqual(ts7, ts6)

        ts8 = stack(ts3, ts4)
        # Old implementation was adding ts4 as a source for ts3, instead of
        # adding the subsources of ts4 to ts3
        for s in ts8.iterSources():
            self.assertFalse(isinstance(s, TroveSourceStack))

    def testUnreachableSource(self):
        """createChangeSet must not mask network errors without a good reason

        @tests: CNY-3732
        """
        repos_alive = self.openRepository(0)
        repos_dead = self.openRepository(1)
        trv = self.addComponent('foo:runtime', 'localhost1@rpl:linux',
                repos=repos_dead)
        grp = self.addCollection('group-foo', [trv], repos=repos_alive)
        self.stopRepository(1)

        n, v, f = grp.getNameVersionFlavor()
        s = stack(repos_alive, repos_dead)
        self.assertRaises(errors.OpenError, s.createChangeSet,
                [(n, (None, None), (v, f), True)],
                recurse=True, withFiles=False)


class ChangeSetJobSourceTest(rephelp.RepositoryHelper):
    def testBugFindTroves(self):
        # Tests a bug in findTroves (referring to self.allTroves)
        db = database.Database(':memory:', ':memory:')
        csjs = ChangeSetJobSource(None, db)
        self.assertEqual(csjs.findTroves('a', []), {})

    def testBugMergeDepSuggestions(self):
        # Tests a bug in mergeDepSuggestions (referring to r)
        db = database.Database(':memory:', ':memory:')
        csjs = ChangeSetJobSource(None, db)
        allSuggs = {'a' : [[]]}
        newSuggs = {'a' : [[2]]}
        csjs.mergeDepSuggestions(allSuggs, newSuggs)
        self.assertEqual(allSuggs, newSuggs)

    def testTwoChangeSetsWithSameTroveAndDiffConfig(self):
        trv = self.addComponent('foo:run=1', [('/etc/config', 'v1\n')])
        trv2 = self.addComponent('foo:run=2', [('/etc/config', 'v2\n')])
        repos = self.openRepository()
        csPath = self.workDir + '/foo.ccs'
        csPath2 = self.workDir + '/foo.ccs'
        self.changeset(repos, 'foo:run=1--2', csPath)
        self.changeset(repos, 'foo:run=1--2', csPath2)
        cs1 = changeset.ChangeSetFromFile(csPath)
        cs2 = changeset.ChangeSetFromFile(csPath2)
        self.updatePkg('foo:run=1')
        db = self.openDatabase()
        source = ChangesetFilesTroveSource(db)
        source.addChangeSets([cs1, cs2], True)
        n, v, f = trv.getNameVersionFlavor()
        v2, f2 = trv2.getNameVersionFlavor()[1:]
        # this used to traceback when there were two changesets that both 
        # provided the same trove in the same source, (if the changesets
        # were relative and involved a config file)
        newCs, remainder = source.createChangeSet([(n, (v, f), (v2, f2), False)],
                                         withFiles=True, withFileContents=True,
                                         useDatabase=True)
        assert(not remainder)
