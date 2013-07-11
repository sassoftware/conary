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


"""
Tests for functions in the cml module
"""

from testutils import mock
from testrunner.testhelp import context

from conary_test import rephelp

from conary import conaryclient, versions
from conary.conaryclient import cml
from conary.deps import deps
from conary.repository.errors import TroveNotFound


class CMCoreTest(rephelp.RepositoryHelper):
    @context('sysmodel')
    def testCMOperation(self):
        self.assertRaises(AssertionError, cml._CMOperation)
        self.assertRaises(NotImplementedError, cml._CMOperation,
            text='foo')

        item = ('a', '1', None)
        sm = cml._CMOperation(item=item, modified=False)
        self.assertEquals(sm.item, item)
        self.assertEquals(sm.modified, False)
        item2 = ('b', '2', None)
        self.assertRaises(NotImplementedError, sm.update, item2)

    @context('sysmodel')
    def testCMLocation(self):
        loc = cml.CMLocation(line=1, context='foo', op=None)
        self.assertEquals(str(loc), 'foo:1')
        self.assertEquals(loc.asString(), 'foo:1')
        self.assertEquals(repr(loc),
            "CMLocation(line=1, context='foo', op=None, spec=None)")
        loc = cml.CMLocation(line=None, context='foo', op=None)
        self.assertEquals(str(loc), 'foo:new-line')
        self.assertEquals(loc.asString(), 'foo:new-line')
        self.assertEquals(repr(loc),
            "CMLocation(line=None, context='foo', op=None, spec=None)")
        loc = cml.CMLocation(line=1, context=None, op=None)
        self.assertEquals(str(loc), '1')
        self.assertEquals(loc.asString(), '1')
        self.assertEquals(repr(loc),
            "CMLocation(line=1, context=None, op=None, spec=None)")
        loc = cml.CMLocation(line=None, context=None, op=None)
        self.assertEquals(str(loc), 'new-line')
        self.assertEquals(loc.asString(), 'new-line')
        self.assertEquals(repr(loc),
            "CMLocation(line=None, context=None, op=None, spec=None)")

    @context('sysmodel')
    def testCMTroveSpec(self):
        ts = cml.CMTroveSpec('foo', 'a@b:c', 'baz')
        self.assertEquals(str(ts), 'foo=a@b:c[baz]')
        self.assertEquals(ts.format(), 'foo=a@b:c[baz]')
        self.assertEquals(ts.asString(), 'foo=a@b:c[baz]')
        self.assertEquals(ts.pinned, False)

        ts = cml.CMTroveSpec('foo=a@b:c[baz]')
        self.assertEquals(str(ts), 'foo=a@b:c[baz]')
        self.assertEquals(ts.format(), 'foo=a@b:c[baz]')

        ts = cml.CMTroveSpec('foo==a@b:c[baz]')
        self.assertEquals(str(ts), 'foo==a@b:c[baz]')
        self.assertEquals(ts.format(), 'foo==a@b:c[baz]')
        self.assertEquals(ts.asString(), 'foo==a@b:c[baz]')
        self.assertEquals(ts.pinned, True)
        self.assertEquals(ts._has_branch, False)
        self.assertEquals(ts.snapshot, False)

        ts = cml.CMTroveSpec('foo=/a@b:c[baz]')
        self.assertEquals(str(ts), 'foo=/a@b:c[baz]')
        self.assertEquals(ts.format(), 'foo=/a@b:c[baz]')
        self.assertEquals(ts.asString(), 'foo=/a@b:c[baz]')
        self.assertEquals(ts.pinned, False)
        self.assertEquals(ts._has_branch, False)
        self.assertEquals(ts.snapshot, False)

        ts = cml.CMTroveSpec('foo=a@b:c/1.2-1-1[baz]')
        self.assertEquals(str(ts), 'foo=a@b:c/1.2-1-1[baz]')
        self.assertEquals(ts.format(), 'foo=a@b:c/1.2-1-1[baz]')
        self.assertEquals(ts.asString(), 'foo=a@b:c/1.2-1-1[baz]')
        self.assertEquals(ts.pinned, False)
        self.assertEquals(ts._has_branch, True)
        self.assertEquals(ts.snapshot, True)


    @context('sysmodel')
    def testSearchOperation(self):
        self.assertRaises(NotImplementedError, cml.SearchOperation,
            text='foo@bar:baz')

    @context('sysmodel')
    def testSearchTrove(self):
        s1 = cml.SearchTrove(text='foo=foo@bar:baz[~blah]')
        self.assertEquals(repr(s1),
            "SearchTrove(text='foo=foo@bar:baz[~blah]', modified=True, index=None)")
        self.assertEquals(s1.format(), 'search foo=foo@bar:baz[~blah]')
        self.assertEquals(s1.asString(), 'foo=foo@bar:baz[~blah]')
        self.assertEquals(str(s1), 'foo=foo@bar:baz[~blah]')
        self.assertEquals(s1.item, ('foo', 'foo@bar:baz', deps.parseFlavor('~blah')))

        s2 = cml.SearchTrove(item = s1.item, modified=True, index=1)
        self.assertEquals(repr(s2),
            "SearchTrove(text='foo=foo@bar:baz[~blah]', modified=True, index=1)")
        self.assertEquals(s2.format(), 'search foo=foo@bar:baz[~blah]')
        self.assertEquals(s2.asString(), 'foo=foo@bar:baz[~blah]')
        self.assertEquals(str(s2), 'foo=foo@bar:baz[~blah]')

        st = cml.SearchTrove('foo=bar:baz[blah]', modified=False)
        self.assertEquals(st.modified, False)
        item2 = (st.item[0], 'bar:blah', st.item[2])
        st.update(item2)
        self.assertEquals(st.item, item2)
        self.assertEquals(st.modified, True)

    @context('sysmodel')
    def testSearchLabel(self):
        s1 = cml.SearchLabel(text='foo@bar:baz')
        self.assertEquals(repr(s1),
            "SearchLabel(text='foo@bar:baz', modified=True, index=None)")
        self.assertEquals(s1.format(), 'search foo@bar:baz')
        self.assertEquals(s1.asString(), 'foo@bar:baz')

        s2 = cml.SearchLabel(item = s1.item, modified=False, index=1)
        self.assertEquals(repr(s2),
            "SearchLabel(text='foo@bar:baz', modified=False, index=1)")
        self.assertEquals(s2.format(), 'search foo@bar:baz')
        self.assertEquals(s2.asString(), 'foo@bar:baz')
        self.assertEquals(str(s2), 'foo@bar:baz')

    @context('sysmodel')
    def testIncludeOperation(self):
        s1 = cml.IncludeOperation(text='foo=foo@bar:baz[~blah]')
        self.assertEquals(repr(s1),
            "IncludeOperation(text='foo=foo@bar:baz[~blah]', modified=True, index=None)")
        self.assertEquals(s1.format(), 'include foo=foo@bar:baz[~blah]')
        self.assertEquals(s1.asString(), 'foo=foo@bar:baz[~blah]')
        self.assertEquals(str(s1), 'foo=foo@bar:baz[~blah]')
        self.assertEquals(s1.item, ('foo', 'foo@bar:baz', deps.parseFlavor('~blah')))

        s2 = cml.IncludeOperation(item = s1.item, modified=True, index=1)
        self.assertEquals(repr(s2),
            "IncludeOperation(text='foo=foo@bar:baz[~blah]', modified=True, index=1)")
        self.assertEquals(s2.format(), 'include foo=foo@bar:baz[~blah]')
        self.assertEquals(s2.asString(), 'foo=foo@bar:baz[~blah]')
        self.assertEquals(str(s2), 'foo=foo@bar:baz[~blah]')

        st = cml.IncludeOperation('foo=bar:baz[blah]', modified=False)
        self.assertEquals(st.modified, False)
        item2 = (st.item[0], 'bar:blah', st.item[2])
        st.update(item2)
        self.assertEquals(st.item, item2)
        self.assertEquals(st.modified, True)

    @context('sysmodel')
    def testNoOperation(self):
        t1 = cml.NoOperation('')
        t2 = cml.NoOperation(text='')
        t3 = cml.NoOperation(item='')
        self.assertEquals(repr(t1), repr(t2))
        self.assertEquals(repr(t2), repr(t3))
        self.assertEquals(repr(t1), 
            "NoOperation(text='', modified=True, index=None)")
        t4 = cml.NoOperation('foo', modified=False, index=1)
        self.assertEquals(repr(t4), 
            "NoOperation(text='foo', modified=False, index=1)")

    @context('sysmodel')
    def testVersionOperation(self):
        t1 = cml.VersionOperation('1.0')
        self.assertEquals(t1.asString(), '1.0')
        self.assertEquals(str(t1), '1.0')
        self.assertEquals(t1.format(), 'version 1.0')
        self.assertEquals(repr(t1), 
            "VersionOperation(text='1.0', modified=True, index=None)")
        t2 = cml.VersionOperation('foo', modified=False, index=1)
        self.assertEquals(t2.asString(), 'foo')
        self.assertEquals(str(t2), 'foo')
        self.assertEquals(t2.format(), 'version foo')
        self.assertEquals(repr(t2), 
            "VersionOperation(text='foo', modified=False, index=1)")

    @context('sysmodel')
    def testTroveOperation(self):
        t1 = cml.TroveOperation('foo')
        self.assertEquals(t1.item, [('foo', None, None)])
        self.assertEquals(t1.asString(), 'foo')
        self.assertEquals(repr(t1),
            "TroveOperation(text=['foo'], modified=True, index=None)")

        t2 = cml.TroveOperation(['foo'])
        self.assertEquals(t2.item, [('foo', None, None)])
        self.assertEquals(t2.asString(), 'foo')

        t3 = cml.TroveOperation(['foo', 'bar=a@b:c'])
        self.assertEquals(t3.item, [('foo', None, None),
                                    ('bar', 'a@b:c', None)])
        iterList = [x for x in t3]
        self.assertEquals(iterList, [('foo', None, None),
                                     ('bar', 'a@b:c', None)])
        self.assertEquals(t3.asString(), 'foo bar=a@b:c')

    @context('sysmodel')
    def testTroveOperations(self):
        t1 = cml.UpdateTroveOperation('foo', index=1)
        self.assertEquals(str(t1.getLocation()), '1')
        self.assertEquals(repr(t1.getLocation()),
              "CMLocation(line=1, context=None,"
              " op=UpdateTroveOperation(text=['foo'], modified=True, index=1),"
              " spec=None)")
        self.assertEquals(t1.getLocation().op, t1)
        self.assertEquals(t1.item, [('foo', None, None)])
        self.assertEquals(t1.asString(), 'foo')
        self.assertEquals(str(t1), 'foo')
        self.assertEquals(t1.format(), 'update foo')
        self.assertEquals(repr(t1),
            "UpdateTroveOperation(text=['foo'], modified=True, index=1)")

        t2 = cml.EraseTroveOperation(['foo'], index=2, context='foo')
        self.assertEquals(str(t2.getLocation()), 'foo:2')
        self.assertEquals(repr(t2.getLocation()),
              "CMLocation(line=2, context='foo',"
              " op=EraseTroveOperation(text=['foo'], modified=True, index=2),"
              " spec=None)")
        self.assertEquals(t2.getLocation().op, t2)
        self.assertEquals(t2.item, [('foo', None, None)])
        self.assertEquals(t2.asString(), 'foo')
        self.assertEquals(str(t2), 'foo')
        self.assertEquals(t2.format(), 'erase foo')
        self.assertEquals(repr(t2),
            "EraseTroveOperation(text=['foo'], modified=True, index=2)")

        t3 = cml.InstallTroveOperation(['foo', 'bar=a@b:c'])
        self.assertEquals(t3.item, [('foo', None, None),
                                    ('bar', 'a@b:c', None)])
        self.assertEquals(t3.asString(), 'foo bar=a@b:c')
        self.assertEquals(str(t3), 'foo bar=a@b:c')
        self.assertEquals(t3.format(), 'install foo bar=a@b:c')
        self.assertEquals(repr(t3),
            "InstallTroveOperation(text=['foo', 'bar=a@b:c'], modified=True, index=None)")

        t4 = cml.PatchTroveOperation(['foo', 'bar=a@b:c', 'baz[f]'])
        self.assertEquals(t4.item, [('foo', None, None),
                                    ('bar', 'a@b:c', None),
                                    ('baz', None, deps.parseFlavor('f'))])
        self.assertEquals(t4.asString(), 'foo bar=a@b:c baz[f]')
        self.assertEquals(str(t4), 'foo bar=a@b:c baz[f]')
        self.assertEquals(t4.format(), 'patch foo bar=a@b:c baz[f]')
        self.assertEquals(repr(t4),
            "PatchTroveOperation(text=['foo', 'bar=a@b:c', 'baz[f]'], modified=True, index=None)")

        t5 = cml.OfferTroveOperation(['foo', 'bar=a@b:c'])
        self.assertEquals(t5.item, [('foo', None, None),
                                    ('bar', 'a@b:c', None)])
        self.assertEquals(t5.asString(), 'foo bar=a@b:c')
        self.assertEquals(str(t5), 'foo bar=a@b:c')
        self.assertEquals(t5.format(), 'offer foo bar=a@b:c')
        self.assertEquals(repr(t5),
            "OfferTroveOperation(text=['foo', 'bar=a@b:c'], modified=True, index=None)")


class CMTest(rephelp.RepositoryHelper):

    @staticmethod
    def getCM():
        cfg = mock.MockObject()
        cfg._mock.set(installLabelPath = ['a@b:c', 'd@e:f' ])
        cfg._mock.set(flavor = deps.parseFlavor(''))
        cfg._mock.set(modelPath = '/etc/conary/system-model')
        return cml.CM(cfg)

    @context('sysmodel')
    def testCMOperations(self):
        m = self.getCM()
        self.assertEquals(m.SearchTrove,
                cml.SearchTrove)
        self.assertEquals(m.SearchLabel,
                cml.SearchLabel)
        self.assertEquals(m.SearchOperation,
                cml.SearchOperation)
        self.assertEquals(m.IncludeOperation,
                cml.IncludeOperation)
        self.assertEquals(m.NoOperation,
                cml.NoOperation)
        self.assertEquals(m.VersionOperation,
                cml.VersionOperation)
        self.assertEquals(m.UpdateTroveOperation,
                cml.UpdateTroveOperation)
        self.assertEquals(m.EraseTroveOperation,
                cml.EraseTroveOperation)
        self.assertEquals(m.InstallTroveOperation,
                cml.InstallTroveOperation)
        self.assertEquals(m.OfferTroveOperation,
                cml.OfferTroveOperation)
        self.assertEquals(m.PatchTroveOperation,
                cml.PatchTroveOperation)
        
    @context('sysmodel')
    def testCMRepresentation(self):
        m = self.getCM()
        gs = cml.SearchTrove(text='group-foo=g@h:i',
            modified=False, index=1)
        m.appendOp(gs)
        gl = cml.SearchLabel(text='j@k:l', modified=False, index=2)
        m.appendOp(gl)
        nop = cml.NoOperation('# comment', modified=False, index=3)
        m.appendNoOperation(nop)
        at = cml.UpdateTroveOperation(text=['bar', 'blah'],
            modified=False, index=4)
        m.appendOp(at)
        rt = cml.EraseTroveOperation(text='baz',
            modified=False, index=5)
        m.appendOp(rt)
        inc = cml.IncludeOperation(text='cml-foo', modified=False, index=6)
        m.appendOp(inc)

        self.assertEquals(m.getVersion(), None)
        ver = cml.VersionOperation('2.0', modified=False, index=7)
        m.setVersion(ver)
        self.assertEquals(str(m.getVersion()), '2.0')

        self.assertEquals(len(m.modelOps), 5)
        self.assertEquals(m.modelOps[0], gs)
        self.assertEquals(m.modelOps[1], gl)
        self.assertEquals(m.modelOps[2], at)
        self.assertEquals(m.modelOps[3], rt)
        self.assertEquals(m.modelOps[4], inc)
        self.assertEquals(m.noOps[0], nop)
        self.assertEquals(len(m.noOps), 1)
        self.assertEquals(sorted(m.indexes.keys()), [1,2,3,4,5,6,7])
        self.assertEquals(m.indexes[1], [gs])
        self.assertEquals(m.indexes[2], [gl])
        self.assertEquals(m.indexes[3], [nop])
        self.assertEquals(m.indexes[4], [at])
        self.assertEquals(m.indexes[5], [rt])
        self.assertEquals(m.indexes[6], [inc])
        self.assertEquals(m.indexes[7], [ver])
        self.assertEquals(m.modified(), False)

    @context('sysmodel')
    def testAddNoOpByText(self):
        m = self.getCM()
        m.appendNoOpByText('#foo', modified=False, index=1)
        self.assertEquals(len(m.noOps), 1)
        self.assertEquals(str(m.noOps[0]), '#foo')
        self.assertEquals(sorted(m.indexes.keys()), [1])

    @context('sysmodel')
    def testAddOperationsByName(self):
        m = self.getCM()
        m.appendOpByName('install', text=['group-foo'],
            modified=False, index=1)
        m.appendOpByName('update', text=['bar', 'blah'],
            modified=False, index=2)
        m.appendOpByName('patch', text='group-errata-1234',
            modified=False, index=3)
        m.appendOpByName('erase', text='baz',
            modified=False, index=4)
        m.appendOpByName('offer', text='optional',
            modified=False, index=5)
        m.appendOpByName('include', text='cml-foo',
            modified=False, index=6)

        self.assertEquals(len(m.modelOps), 6)
        self.assertEquals(m.modelOps[0].format(), 'install group-foo')
        self.assertEquals(m.modelOps[1].format(), 'update bar blah')
        self.assertEquals(m.modelOps[2].format(), 'patch group-errata-1234')
        self.assertEquals(m.modelOps[3].format(), 'erase baz')
        self.assertEquals(m.modelOps[4].format(), 'offer optional')
        self.assertEquals(m.modelOps[5].format(), 'include cml-foo')
        self.assertEquals(sorted(m.indexes.keys()), [1,2,3,4,5,6])

    @context('sysmodel')
    def testRemoveOp(self):
        m = self.getCM()
        m.appendOpByName('install', text=['group-foo'],
            modified=False, index=1)
        op = m.appendOpByName('update', text=['bar', 'blah'],
            modified=False, index=2)
        m.appendOpByName('patch', text='group-errata-1234',
            modified=False, index=3)

        self.assertEquals(len(m.modelOps), 3)
        m.removeOp(op)
        self.assertEquals(len(m.modelOps), 2)

    @context('sysmodel')
    def testAddEraseOperation(self):
        m = self.getCM()
        # as if from the existing system model
        m.appendOpByName('install', text='group-foo',
            modified=False, index=1)
        m.appendOpByName('erase', text='baz',
            modified=False, index=2)

        self.assertEquals(len(m.modelOps), 2)
        self.assertEquals(str(m.modelOps[0]), 'group-foo')
        self.assertEquals(str(m.modelOps[1]), 'baz')
        self.assertEquals(m.modelOps[0].format(), 'install group-foo')
        self.assertEquals(m.modelOps[1].format(), 'erase baz')

        m.appendOpByName('update', text=['bar', 'blah'],
            modified=False, index=3)
        self.assertEquals(m.modelOps[2].format(), 'update bar blah')

        m.appendOpByName('erase', text='bar')
        self.assertEquals(m.modelOps[2].format(), 'update bar blah')
        self.assertEquals(m.modelOps[3].format(), 'erase bar')

    @context('sysmodel')
    def testRefreshVersionSnapshots(self):
        m = self.getCM()

        mockClient = mock.MockObject()
        self.mock(conaryclient, 'ConaryClient', mockClient)
        repos = mockClient().getRepos()
        repos.findTroves._mock.setDefaultReturn(
            {('group-foo', 'g@h:i', None):
                 [('group-foo', 
                   versions.VersionFromString('/g@h:i/1.0-1-2'),
                   deps.parseFlavor('foo'))],
             ('group-foo', 'g@h:i/1.0-1-1', None):
                 [('group-foo',
                   versions.VersionFromString('/g@h:i/1.0-1-1'),
                   deps.parseFlavor('foo'))],
             ('bar', 'g@h:i', None):
                 [('bar', 
                   versions.VersionFromString('/g@h:i/2.0-1-2'),
                   deps.parseFlavor('foo'))],
             ('bar', 'g@h:i/2.0-1-1', None):
                 [('bar',
                   versions.VersionFromString('/g@h:i/2.0-1-1'),
                   deps.parseFlavor('foo'))],
             ('pinned', 'g@h:i', None):
                 [('pinned', 
                   versions.VersionFromString('/g@h:i/2.0-1-2'),
                   deps.parseFlavor('foo'))],
             ('pinned', 'g@h:i/2.0-1-1', None):
                 [('pinned',
                   versions.VersionFromString('/g@h:i/2.0-1-1'),
                   deps.parseFlavor('foo'))],
             ('cml-inc', 'g@h:i', None):
                 [('cml-inc', 
                   versions.VersionFromString('/g@h:i/1.0-1-2'),
                   deps.parseFlavor('foo'))],
             ('cml-inc', 'g@h:i/1.0-1-1', None):
                 [('cml-inc',
                   versions.VersionFromString('/g@h:i/1.0-1-1'),
                   deps.parseFlavor('foo'))],
            }
        )

        gs = cml.SearchTrove(text='group-foo=g@h:i/1.0-1-1',
            modified=False, index=1)
        m.appendOp(gs)
        gl = cml.SearchLabel(text='j@k:l', modified=False, index=2)
        m.appendOp(gl)
        up = cml.UpdateTroveOperation(text='bar=g@h:i/2.0-1-1',
            modified=False, index=3)
        m.appendOp(up)
        pi = cml.InstallTroveOperation(text='pinned==g@h:i/2.0-1-1',
            modified=False, index=4)
        m.appendOp(pi)
        inc = cml.IncludeOperation(text='cml-inc=g@h:i/1.0-1-1',
            modified=False, index=5)
        m.appendOp(inc)

        self.assertEquals(m.modified(), False)
        self.assertEquals([x.format() for x in m.modelOps],
            ['search group-foo=g@h:i/1.0-1-1',
             'search j@k:l',
             'update bar=g@h:i/2.0-1-1',
             'install pinned==g@h:i/2.0-1-1',
             'include cml-inc=g@h:i/1.0-1-1'])
        m.refreshVersionSnapshots()
        self.assertEquals(m.modified(), True)
        self.assertEquals([x.format() for x in m.modelOps],
            ['search group-foo=g@h:i/1.0-1-2',
             'search j@k:l',
             'update bar=g@h:i/2.0-1-2',
             'install pinned==g@h:i/2.0-1-1',
             'include cml-inc=g@h:i/1.0-1-2'])

        # and now, if it doesn't change:
        repos.findTroves._mock.setDefaultReturn(
            {('group-foo', 'g@h:i', None):
                 [('group-foo', 
                   versions.VersionFromString('/g@h:i/1.0-1-2'),
                   deps.parseFlavor('foo'))],
             ('group-foo', 'g@h:i/1.0-1-2', None):
                 [('group-foo',
                   versions.VersionFromString('/g@h:i/1.0-1-2'),
                   deps.parseFlavor('foo'))],
             ('bar', 'g@h:i', None):
                 [('bar', 
                   versions.VersionFromString('/g@h:i/2.0-1-2'),
                   deps.parseFlavor('foo'))],
             ('bar', 'g@h:i/2.0-1-2', None):
                 [('bar',
                   versions.VersionFromString('/g@h:i/2.0-1-2'),
                   deps.parseFlavor('foo'))],
             ('pinned', 'g@h:i', None):
                 [('pinned', 
                   versions.VersionFromString('/g@h:i/2.0-1-2'),
                   deps.parseFlavor('foo'))],
             ('pinned', 'g@h:i/2.0-1-1', None):
                 [('pinned',
                   versions.VersionFromString('/g@h:i/2.0-1-1'),
                   deps.parseFlavor('foo'))],
             ('cml-inc', 'g@h:i', None):
                 [('cml-inc', 
                   versions.VersionFromString('/g@h:i/1.0-1-2'),
                   deps.parseFlavor('foo'))],
             ('cml-inc', 'g@h:i/1.0-1-2', None):
                 [('cml-inc',
                   versions.VersionFromString('/g@h:i/1.0-1-2'),
                   deps.parseFlavor('foo'))],
            }
        )
        m.refreshVersionSnapshots()
        self.assertEquals(m.modified(), True)
        self.assertEquals([x.format() for x in m.modelOps],
            ['search group-foo=g@h:i/1.0-1-2',
             'search j@k:l',
             'update bar=g@h:i/2.0-1-2',
             'install pinned==g@h:i/2.0-1-1',
             'include cml-inc=g@h:i/1.0-1-2'])

        # Old group is missing
        findtroves = {
             ('group-foo', 'g@h:i', None):
                 [('group-foo', 
                   versions.VersionFromString('/g@h:i/1.0-1-2'),
                   deps.parseFlavor('foo'))],
             #('group-foo', 'g@h:i/1.0-1-2', None):
             ('bar', 'g@h:i', None):
                 [('bar', 
                   versions.VersionFromString('/g@h:i/2.0-1-2'),
                   deps.parseFlavor('foo'))],
             ('bar', 'g@h:i/2.0-1-2', None):
                 [('bar',
                   versions.VersionFromString('/g@h:i/2.0-1-2'),
                   deps.parseFlavor('foo'))],
             ('pinned', 'g@h:i', None):
                 [('pinned', 
                   versions.VersionFromString('/g@h:i/2.0-1-2'),
                   deps.parseFlavor('foo'))],
             ('pinned', 'g@h:i/2.0-1-1', None):
                 [('pinned',
                   versions.VersionFromString('/g@h:i/2.0-1-1'),
                   deps.parseFlavor('foo'))],
             ('cml-inc', 'g@h:i', None):
                 [('cml-inc', 
                   versions.VersionFromString('/g@h:i/1.0-1-2'),
                   deps.parseFlavor('foo'))],
             ('cml-inc', 'g@h:i/1.0-1-2', None):
                 [('cml-inc',
                   versions.VersionFromString('/g@h:i/1.0-1-2'),
                   deps.parseFlavor('foo'))],
            }
        repos.findTroves._mock.setDefaultReturn(findtroves)
        m.refreshVersionSnapshots()
        self.assertEquals(m.modified(), True)
        self.assertEquals([x.format() for x in m.modelOps],
            ['search group-foo=g@h:i/1.0-1-2',
             'search j@k:l',
             'update bar=g@h:i/2.0-1-2',
             'install pinned==g@h:i/2.0-1-1',
             'include cml-inc=g@h:i/1.0-1-2'])

        # New version is missing
        del findtroves[('group-foo', 'g@h:i', None)]
        err = self.assertRaises(TroveNotFound, m.refreshVersionSnapshots)
        self.assertEquals(str(err), "Trove not found: group-foo=g@h:i")
        del findtroves[('bar', 'g@h:i', None)]
        err = self.assertRaises(TroveNotFound, m.refreshVersionSnapshots)
        self.assertEquals(str(err),
                "2 troves not found:\nbar=g@h:i\ngroup-foo=g@h:i")

    def testNoRefreshLocalVersions(self):
        m = self.getCM()

        mockClient = mock.MockObject()
        self.mock(conaryclient, 'ConaryClient', mockClient)
        repos = mockClient().getRepos()

        pi = cml.InstallTroveOperation(text='foo=/local@local:COOK/2.0-1-1',
            modified=False, index=4)
        m.appendOp(pi)

        self.assertEquals(m.modified(), False)
        self.assertEquals([x.format() for x in m.modelOps],
            ['install foo=/local@local:COOK/2.0-1-1'])
        m.refreshVersionSnapshots()
        repos.findTroves._mock.assertNotCalled()
        self.assertEquals([x.format() for x in m.modelOps],
            ['install foo=/local@local:COOK/2.0-1-1'])
        self.assertEquals(m.modified(), False)


class CMLTest(rephelp.RepositoryHelper):

    def getCML(self, *args):
        cfg = mock.MockObject()
        cfg._mock.set(installLabelPath = ['a@b:c', 'd@e:f' ])
        cfg._mock.set(flavor = deps.parseFlavor(''))
        cfg._mock.set(root = self.rootDir)
        cfg._mock.set(modelPath = '/etc/conary/system-model')
        return cml.CML(cfg, *args)

    @context('sysmodel')
    def testInit(self):
        smf = self.getCML()
        self.assertEquals(smf.filedata, [])
        smf.parse(fileData=['# comment\n'])
        self.assertEquals(smf.filedata, ['# comment\n'])
        self.assertEquals(repr(smf.noOps[0]),
            repr(cml.NoOperation('# comment', modified=False, index=1)))
        smf.parse() # does not raise an exception

    @context('sysmodel')
    def testCMReset(self):
        smf = self.getCML()
        smf.parse(fileData=['# comment\n', 'search foo\n', 'install bar\n'])
        self.assertEquals(smf.format(),
            '# comment\n'
            'search foo\n'
            'install bar\n')
        smf.parse(fileData=['# comment\n', 'search foo\n'])
        self.assertEquals(smf.format(),
            '# comment\n'
            'search foo\n')
        smf.reset()
        self.assertEquals(smf.format(), '')
        self.assertEquals(smf.modelOps, [])

    @context('sysmodel')
    def testQuotedData(self):
        "Ensure that data that will be split by shlex is saved quoted"
        smf = self.getCML()
        
        smf.parse(fileData=[
            'search group-foo=bar:baz/1.2[a, b is: x86_64]',
            'search group-foo=bar:baz/1.2[a, b is: x86(i386,i486,i586,cmov)]',
            'install foo',])
        smf.modelOps[0].modified = True
        smf.modelOps[1].modified = True
        smf.appendOpByName('install',
            'blah[~foo, !bar, ~!baz is: x86(cmov)]')
        self.assertEquals(smf.format(), '\n'.join((
            "search 'group-foo=bar:baz/1.2[a,b is: x86_64]'",
            "search 'group-foo=bar:baz/1.2[a,b is: x86(cmov,i386,i486,i586)]'",
            "install foo",
            "install 'blah[!bar,~!baz,~foo is: x86(cmov)]'",
            ""
        )))

    @context('sysmodel')
    def testQuotedDataMultiItemLine(self):
        "Ensure that data that will be split by shlex is saved quoted"
        smf = self.getCML()
        trvspecs = [ 'foo[is: x86 x86_64]', 'bar[is: x86 x86_64]' ]
        smf.appendOpByName('install', text = trvspecs)
        expected = '\n'.join([
            "install %s" % ' '.join("'%s'" % x for x in trvspecs),
            "",
        ])
        self.assertEquals(smf.format(), expected)
        # Parse it again, make sure we get the same results
        smf = self.getCML()
        smf.parse(fileData=expected.strip().split('\n'))
        self.assertEquals(smf.format(), expected)
        self.assertEquals(
                [ len(x.item) for x in smf.modelOps ],
                [ 2 ])

    @context('sysmodel')
    def testStartFromScratch(self):
        smf = self.getCML()

        smf.appendNoOpByText('# an initial comment')
        self.assertEquals(smf.format(),
            '# an initial comment\n')

        smf.appendOpByName('update', 'foo')
        self.assertEquals(smf.format(),
            '# an initial comment\n'
            'update foo\n')

        smf.appendOp(cml.SearchLabel('a@b:c'))
        self.assertEquals(smf.format(),
            '# an initial comment\n'
            'update foo\n'
            'search a@b:c\n')

        smf.setVersion(cml.VersionOperation('1.0'))
        self.assertEquals(smf.format(),
            '# an initial comment\n'
            'version 1.0\n'
            'update foo\n'
            'search a@b:c\n')

        smf.appendOpByName('offer', 'optional')
        self.assertEquals(smf.format(),
            '# an initial comment\n'
            'version 1.0\n'
            'update foo\n'
            'search a@b:c\n'
            'offer optional\n')

        smf.appendOpByName('include', 'cml-inc')
        self.assertEquals(smf.format(),
            '# an initial comment\n'
            'version 1.0\n'
            'update foo\n'
            'search a@b:c\n'
            'offer optional\n'
            'include cml-inc\n')

        smf.appendNoOpByText('# a trailing comment', index=999)
        self.assertEquals(smf.format(),
            '# an initial comment\n'
            'version 1.0\n'
            'update foo\n'
            'search a@b:c\n'
            'offer optional\n'
            'include cml-inc\n'
            '# a trailing comment\n')

        smf.appendNoOpByText('# another trailing comment', index=999)
        self.assertEquals(smf.format(),
            '# an initial comment\n'
            'version 1.0\n'
            'update foo\n'
            'search a@b:c\n'
            'offer optional\n'
            'include cml-inc\n'
            '# a trailing comment\n'
            '# another trailing comment\n')

        smf2 = smf.copy()
        self.assertEquals(smf.format(), smf2.format())

    @context('sysmodel')
    def testParseWrite(self):
        fileData = '\n'.join((
            '# Initial comment',
            'search group-foo=a@b:c/1-1-1',
            '# comment 2',
            'offer optional',
            'update foo #disappearing act',
            '# comment 3',
            'install bar',
            '# comment 4',
            'patch baz',
            '# comment 5',
            'erase blah',
            '# comment 6',
            'include cml-inc',
            '# comment 7',
            'version 1.0 # ensure it does not move',
            '',
        ))
        smf = self.getCML()
        smf.parse(fileData=[x+'\n' for x in fileData.split('\n')][:-1])
        self.assertEquals(smf.format(), fileData)
        self.assertEquals(str(smf.modelOps[3].getLocation(
                                  smf.modelOps[3].item[0])), '7:bar')
        self.assertEquals(smf.modelOps[3].format(), 'install bar')
        smf.modelOps[2].modified=True
        self.assertEquals(smf.modified(), True)
        modFileData = fileData.replace(' #disappearing act', '')
        self.assertEquals(smf.format(), modFileData)
        smf.appendOp(cml.UpdateTroveOperation('newtrove'))
        modFileData = modFileData.replace('include cml-inc\n',
                                          'include cml-inc\nupdate newtrove\n')
        self.assertEquals(smf.format(), modFileData)
        smf.appendOp(cml.SearchLabel('d@e:f'))
        modFileData = modFileData.replace('update newtrove\n',
                                          'update newtrove\nsearch d@e:f\n')
        self.assertEquals(smf.format(), modFileData)

        mockFile = mock.MockObject()
        smf.write(mockFile)
        mockFile.write._mock.assertCalled(modFileData)

        smf.parse(fileData=[x+'\n' for x in fileData.split('\n')][:-1],
                  context='foo')
        self.assertEquals(str(smf.modelOps[3].getLocation(
                                    smf.modelOps[3].item[0])),
                          'foo:7:bar')
        self.assertEquals(smf.modelOps[3].format(), 'install bar')


    @context('sysmodel')
    def testParseFail(self):
        smf = self.getCML()
        e =self.assertRaises(cml.CMError,
            smf.parse, fileData=['badverb noun'], context='/foo')
        self.assertEquals(str(e), '/foo:1: Unrecognized command "badverb"')
        e =self.assertRaises(cml.CMError,
            smf.parse, fileData=['badverb'], context='/foo')
        self.assertEquals(str(e), '/foo:1: Invalid statement "badverb"')
        e =self.assertRaises(cml.CMError,
            smf.parse, fileData=['search foo=bar=baz@blah@blah:1-1-1-1-1'])
        self.assertEquals(str(e),
            '/foo:1: Error with spec "foo=bar=baz@blah@blah:1-1-1-1-1":'
            " Too many ='s")
        e = self.assertRaises(cml.CMError,
            smf.parse, fileData=['install "'], context='/foo')
        self.assertEquals(str(e), '/foo:1: No closing quotation')
        e = self.assertRaises(cml.CMError,
            smf.parse, fileData=['search "'], context='/foo')
        self.assertEquals(str(e), '/foo:1: No closing quotation')

    @context('sysmodel')
    def testEmptyEverything(self):
        smf = self.getCML()
        self.assertEquals(smf.format(), '')

    @context('sysmodel')
    def testImmediateErasureInModelData(self):
        fileData = '\n'.join((
            'search group-foo=a@b:c/1-1-1',
            'install foo',
            'erase foo',
            '',
        ))
        smf = self.getCML()
        smf.parse(fileData=[x+'\n' for x in fileData.split('\n')][:-1])
        # ensure that erasure is not short-circuited -- model should
        # not be truncated to just the search line.
        self.assertEquals(smf.format(), fileData)
