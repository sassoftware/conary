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

from conary_test import rephelp

from conary.local import database

from conary.deps import deps
from conary.deps.deps import Flavor
from conary.deps.deps import parseDep
from conary import trove
from conary import versions
from conary.repository import changeset
from conary.repository import trovesource


class DepTableTest(testhelp.TestCase):

    new = versions.NewVersion()
    flv = Flavor()

    def _fixupVersion(self, version):
        if version is None:
            return versions.NewVersion()
        else:
            assert(isinstance(version, str))
            return versions.ThawVersion("/localhost@foo:bar/" + "1.0:" +
                    version)

    def reqTrove(self, troveName, depSet, version=None):
        version = self._fixupVersion(version)
        trv = trove.Trove(troveName, version, Flavor(), None)
        trv.setRequires(depSet)
        return trv

    def prvTrove(self, troveName, depSet, version=None):
        version = self._fixupVersion(version)
        trv = trove.Trove(troveName, version, Flavor(), None)
        trv.setProvides(depSet)
        return trv

    def prvReqTrove(self, troveName, prvDepSet, reqDepSet, version=None):
        trv = self.prvTrove(troveName, prvDepSet, version=version)
        trv.setRequires(reqDepSet)
        return trv

    def createJobInfo(self, db, *troves):
        cs = changeset.ChangeSet()
        jobs = []

        for trvInfo in troves:
            if isinstance(trvInfo, tuple):
                oldTrv, newTrv = trvInfo
            else:
                newTrv = trvInfo
                oldTrv = None

            if newTrv is not None:
                trvCs = newTrv.diff(oldTrv, absolute=False)[0]
                cs.newTrove(trvCs)
                jobs.append((trvCs.getName(),
                             (trvCs.getOldVersion(), trvCs.getOldFlavor()),
                             (trvCs.getNewVersion(), trvCs.getNewFlavor()),
                             False))
            else:
                cs.oldTrove(oldTrv.getName(), oldTrv.getVersion(),
                            oldTrv.getFlavor())
                jobs.append((oldTrv.getName(),
                             (oldTrv.getVersion(), oldTrv.getFlavor()),
                             (None, None),
                             False))

        src = trovesource.ChangesetFilesTroveSource(db)
        src.addChangeSet(cs)

        return (db.db, jobs, src)

    def init(self):
        db = database.Database(None, ':memory:')
        dt = db.db.depTables
        # commit the schema changes
        db.commit()

        cu = db.db.db.cursor()

        return dt, db, cu

    def check(self, db, jobSet, troveSource, findOrdering=False):
        checker = db.dependencyChecker(troveSource, findOrdering=findOrdering)
        checker.addJobs(jobSet)
        result = checker.check()

        checker.done()
        return (result.unsatisfiedList, result.unresolveableList,
                result.getChangeSetList())

    def testSimpleResolution(self):
        dt, db, cu = self.init()

        dep = parseDep("soname: ELF32/libc.so.6(GLIBC_2.0)")
        jobInfo = self.createJobInfo(db, self.reqTrove("test", dep))
        assert(self.check(*jobInfo)[0:2] == ([
                    ( ("test", self.new, self.flv), dep )], [] ))
        dt.add(cu, self.prvTrove("test-prov", dep), 1)
        db.commit()
        assert(self.check(*jobInfo)[0:2] == ([], []))

        dt, db, cu = self.init()
        jobInfo = self.createJobInfo(db, self.reqTrove("test", dep),
                           self.prvTrove("test-prov", dep))
        assert(self.check(*jobInfo)[0:2] == ([], []))

    def testFlagSets(self):
        dt, db, cu = self.init()

        dep = parseDep(
                "soname: ELF32/libc.so.6(GLIBC_2.0 GLIBC_2.1 GLIBC_2.2)")
        jobInfo = self.createJobInfo(db, self.reqTrove("test", dep))
        assert(self.check(*jobInfo)[0:2] == ([
                    ( ("test", self.new, self.flv), dep )], [] ))

        # make sure that having separate troves provide each flag doesn't
        # yield resolved dependencies
        prv1 = parseDep("soname: ELF32/libc.so.6(GLIBC_2.0)")
        prv2 = parseDep("soname: ELF32/libc.so.6(GLIBC_2.1)")
        prv3 = parseDep("soname: ELF32/libc.so.6(GLIBC_2.2)")

        dt.add(cu, self.prvTrove("test-prov", prv1), 1)
        dt.add(cu, self.prvTrove("test-prov", prv2), 2)
        dt.add(cu, self.prvTrove("test-prov", prv3), 3)
        db.commit()
        assert(self.check(*jobInfo)[0:2] == ([
                    ( ("test", self.new, self.flv), dep )], [] ))

        # now set up a trove that provides all of the flags; this trove
        # should resolve the dependency
        prv = parseDep(
                "soname: ELF32/libc.so.6(GLIBC_2.0 GLIBC_2.1 GLIBC_2.2)")

        dt.add(cu, self.prvTrove("test-prov", prv), 4)
        db.commit()
        assert(self.check(*jobInfo)[0:2] == ([], []))

    def testInfoOrdering(self):
        dt, db, cu = self.init()

        # info-user:foo needs info-group:foo
        # test needs info-user:foo
        jobInfo = self.createJobInfo(db,
                           self.prvReqTrove("info-user:user",
                             parseDep("userinfo: user"),
                             parseDep("groupinfo: group")),
                           self.prvTrove("info-group:group",
                             parseDep("groupinfo: group")),
                           self.reqTrove("test",
                             parseDep("userinfo: user"))
        )

        resolvedOrder = self.check(findOrdering=True, *jobInfo)[2]
        order = [ [ y[0] for y in x ] for x in resolvedOrder ]
        assert(order == [['info-group:group'], ['info-user:user'], ['test']])

    def testIterativeOrdering(self):
        """Jobs order correctly when the graph is built over several check()
        calls.

        @tests: CNY-3654
        """
        dt, db, cu = self.init()

        # Create a cycle between two troves, as that's easy to verify in the
        # final ordering. One of them also requires a third trove, which will
        # be added in a separate dep check cycle.
        dep1 = parseDep('python: dep1')
        dep2 = parseDep('python: dep2')
        dep3 = parseDep('python: dep3')

        # trv1 requires dep2 + dep3
        trv1reqs = deps.DependencySet()
        trv1reqs.union(dep2)
        trv1reqs.union(dep3)
        trv1 = self.prvReqTrove('trv1:runtime', dep1, trv1reqs)
        trv2 = self.prvReqTrove('trv2:runtime', dep2, dep1)
        trv3 = self.prvTrove('trv3:runtime', dep3)

        # The first job has just the cycle in it, and is missing trv3 to
        # complete the graph.
        _, job12, src = self.createJobInfo(db, trv1, trv2)
        # The second job includes the needed trove
        _, job3, src3 = self.createJobInfo(db, trv3)
        # Merge the second job's changeset into the first so the trove source
        # is complete.
        src.addChangeSets(src3.csList)

        checker = db.db.dependencyChecker(src, findOrdering=True)
        # First pass: missing one dep
        checker.addJobs(job12)
        result = checker.check()
        self.assertEqual(result.unsatisfiedList,
                [(('trv1:runtime', self.new, self.flv), dep3)])
        # Second pass: add provider
        checker.addJobs(job3)
        result = checker.check()
        self.assertEqual(result.unsatisfiedList, [])
        # trv1 and trv2 require each other and so constitute a single job, trv3
        # is not part of the cycle so it is a separate job. The original bug
        # would have all three troves as separate jobs since it forgot about
        # the deps from the first check.
        checker.done()
        self.assertEqual(result.getChangeSetList(), [job3, job12])

    def testSelf(self):
        dt, db, cu = self.init()

        dep = parseDep("trove: test")
        jobInfo = self.createJobInfo(db, self.reqTrove("test", dep))
        assert(self.check(*jobInfo)[1:4] == ([], []))

    def testOldNeedsNew(self):
        dt, db, cu = self.init()

        prv1 = parseDep("soname: ELF32/libtest.so.1(foo)")
        prv2 = parseDep("soname: ELF32/libtest.so.2(foo)")

        prvTrv1 = self.prvTrove("test-prov", prv1, version="1.0-1-1")
        reqTrv1 = self.reqTrove("test-req", prv1, version="1.0-1-1")

        troveInfo = db.addTrove(prvTrv1)
        db.addTroveDone(troveInfo)
        troveInfo = db.addTrove(reqTrv1)
        db.addTroveDone(troveInfo)
        db.commit()

        prvTrv2 = self.prvTrove("test-prov", prv2, version="2.0-1-1")
        reqTrv2 = self.reqTrove("test-req", prv2, version="2.0-1-1")

        jobInfo = self.createJobInfo(db,
                (prvTrv1, prvTrv2), (reqTrv1, reqTrv2))
        order = self.check(findOrdering=True, *jobInfo)[2]
        assert(len(order) == 1)

    def testOutsiderNeedsOldAndNew(self):
        dt, db, cu = self.init()
        dep = parseDep("soname: ELF32/libtest.so.1(flag)")
        reqTrv1 = self.reqTrove("test-req", dep, version="1.0-1-1")
        prvTrv1 = self.prvTrove("test-prov", dep, version="1.0-1-1")
        prvTrv2 = self.prvTrove("test-prov2", dep, version="1.0-1-1")

        troveInfo = db.addTrove(prvTrv1)
        db.addTroveDone(troveInfo)
        troveInfo = db.addTrove(reqTrv1)
        db.addTroveDone(troveInfo)
        db.commit()

        jobInfo = self.createJobInfo(db, (prvTrv1, None), (None, prvTrv2))
        (broken, byErase, order) = self.check(findOrdering=True, *jobInfo)
        assert(not broken and not byErase)
        assert(len(order) == 1)


class DepTableTestWithHelper(rephelp.RepositoryHelper):
    def testGetLocalProvides(self):
        db = self.openDatabase()
        baz = self.addDbComponent(db, 'baz:run', '1', '',
                            provides=parseDep('trove:foo:run'))
        foo2 = self.addDbComponent(db, 'foo:run', '2', '',
                            provides=parseDep('trove:foo:run'),
                            requires=parseDep('trove:baz:run'))
        foo1 = self.addDbComponent(db, 'foo:run', '1', '',
                            provides=parseDep('trove:foo:run'),
                            requires=parseDep('trove:baz:run'))
        bar = self.addDbComponent(db, 'bar:run', '1', '',
                            provides=parseDep('trove:bar:run'),
                            requires=parseDep('trove:foo:run'))
        bam = self.addDbComponent(db, 'bam:run', '1', '',
                            provides=parseDep('trove:bam:run'),
                            requires=parseDep('trove:foo:run'))
        depSet = parseDep('trove:bam:run trove:bar:run')
        sols = db.getTrovesWithProvides([depSet], True)
        assert(sols[depSet] == [[bam.getNameVersionFlavor()],
                                [bar.getNameVersionFlavor()]])

    def testUnknownDepTag(self):
        db = self.openDatabase()
        intTag = 65535
        stringTag = "yet-to-be-defined"

        class YetToBeDefinedDependency(deps.DependencyClass):
            tag = intTag
            tagName = stringTag
            justOne = False
            depClass = deps.Dependency

        ds = deps.DependencySet()
        depName = "some"
        depFlag = "flag1"
        ds.addDep(YetToBeDefinedDependency, deps.Dependency(depName,
            [ (depFlag, deps.FLAG_SENSE_REQUIRED) ]))

        bam = self.addDbComponent(db, 'bam:run', '1', '',
                                  provides=ds,
                                  requires=ds)
        bam2 = db.getTrove(bam.name(), bam.version(), bam.flavor())
        self.assertEqual(bam.requires.freeze(), bam2.requires.freeze())
        self.assertEqual(bam.provides.freeze(), bam2.provides.freeze())
