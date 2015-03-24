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

import base64
import os
import time
import tempfile
import urllib2

from conary_test import rephelp
from conary_test import resources

from conary import conaryclient
from conary import dbstore
from conary import trove
from conary import versions
from conary.deps import deps
from conary.lib import util, openpgpfile
from conary.lib.http import request
from conary.repository import changeset, errors, filecontainer, netclient, transport
from conary.repository import datastore
from conary.repository import xmlshims
from conary.repository.netrepos import reposlog, netserver
from conary.server import schema
from conary.build import signtrove

from conary.lib import openpgpkey


class NetServerTest(rephelp.RepositoryHelper):
    @testhelp.context('performance')
    def testMassiveGetFileVersions(self):
        raise testhelp.SkipTestException('testMassiveGetFileVersions is massively slow')
        vers = [ '%d.0' %x for x in xrange(20) ]
        needed = set()
        repos = self.openRepository()
        csFile = open(self.workDir + '/test.ccs', 'w')
        for v in vers:
            merged = None
            for i in xrange(100):
                trv, cs = self.Component('test%d:source' %i, filePrimer=i,
                                         version=v)
                fn = self.workDir + '%d.ccs' %i
                cs.writeToFile(fn)
                newCs = changeset.ChangeSetFromFile(fn)
                if not merged:
                    merged = newCs
                else:
                    merged.merge(newCs)
                del newCs
                os.unlink(fn)

                for pathId, path, fileId, version in trv.iterFileList():
                    needed.add((pathId, fileId, version))

            repos.commitChangeSet(merged)

        start = time.time()
        streams = repos.getFileVersions(list(needed))
        end = time.time()
        # we should be able to fetch all these streams in under 5 seconds
        assert(end - start < 5)

    def testGetMissingTroves(self):
        # CNY-948 - don't return non-present trove objects.
        # They don't actually exist, but are referenced, which means that
        # when returning them you get an object w/ no sha1, e.g.
        trv = self.addCollection('foo', '1', [':run'])
        repos = self.openRepository()

        # foo:run is referenced
        assert(list(repos.getTrove(*trv.getNameVersionFlavor()).iterTroveList(strongRefs=True)))
        try:
            # this trove doesn't actually exist!
            repos.getTrove('foo:run', trv.getVersion(), trv.getFlavor())
            assert(0)
        except errors.TroveMissing, err:
            pass

    def testPublicCallsAccessDecorator(self):
        # CNY-659 - decorate public calls with an access type (ro/rw)
        # Also checks if all public calls are defined and callable.

        def startswithany(s, prefixes):
            # True if s starts with any of the prefixes
            for prefix in prefixes:
                if s.startswith(prefix):
                    return True
            return False

        def checkAccess(fname, meth):
            # Heuristics: getters should be read-only, setters should be
            # read-write.
            # If you want to bypass these heuristics, add your check before
            # the defaults.
            if startswithany(fname, ['get', 'list', 'has', 'check']):
                self.assertEqual(meth._accessType, 'readOnly', fname)
            elif fname in ['troveNames', 'prepareChangeSet', 'commitCheck']:
                self.assertEqual(meth._accessType, 'readOnly', fname)
            elif startswithany(fname, ['add', 'change', 'commit', 'delete',
                    'edit', 'set', 'update', 'presentHidden']):
                self.assertEqual(meth._accessType, 'readWrite', fname)
            else:
                self.fail("Please describe the access method for %s in the test"
                    % fname)

        ns = self.getServer()
        for fname in ns.publicCalls:
            self.assertTrue(hasattr(ns, fname))
            meth = getattr(ns, fname)
            self.assertTrue(meth)
            self.assertTrue(hasattr(meth, '__call__'))
            # Decorated?
            self.assertTrue(hasattr(meth, '_accessType'))
            checkAccess(fname, meth)

    def testReadOnlyRepositoryServerSide(self):
        cfg = self.getServerConfiguration(readOnlyRepository=True)
        ns = self.getServer(cfg=cfg)

        r = errors.ReadOnlyRepositoryError( ns.callWrapper,
            'fake-protocol', 8080, 'addUser', 'fake-auth-token', [])
        # Fail unless we got an exception
        self.assertTrue(r[1])

    def testReadOnlyRepositoryClientSide(self):
        # Make sure we don't have a read-write server cached
        self.stopRepository()
        repos = self.openRepository(readOnlyRepository=True)
        try:
            self.assertRaises(errors.ReadOnlyRepositoryError,
                self.addCollection, 'foo', '1', [':run'])
        finally:
            # Make sure we don't cache the read-only server
            self.stopRepository()

    def getServerConfiguration(self, **kw):
        cfg = netserver.ServerConfig()

        # Set defaults
        cfg.repositoryDB = ('sqlite', os.path.join(self.workDir, 'schema.sqlite'))
        cfg.contentsDir = ('legacy', [os.path.join(self.workDir, 'contentsDir')])
        cfg.readOnlyRepository = False

        # And override the defaults with any values supplied as arguments
        for k, v in kw.iteritems():
            assert(hasattr(cfg, k))
            setattr(cfg, k, v)

        return cfg

    def getServer(self, cfg=None, db=None, basicUrl=''):
        if cfg is None:
            cfg = self.getServerConfiguration()

        if db is None:
            (driver, database) = cfg.repositoryDB
            db = dbstore.connect(database, driver)
            schema.loadSchema(db)

        ns = netserver.NetworkRepositoryServer(cfg, basicUrl, db)
        return ns

    def testGetChangesetNoArgs(self):
        # CNY-1142, make sure that http://conary.example.com/conary/changeset
        # returns a 400 error
        repos = self.openRepository()
        url = repos.c.map['localhost']
        e = self.assertRaises(urllib2.HTTPError,
                urllib2.urlopen, url + 'changeset')
        self.assertIn(e.code, [400, 403])

    def testTroveCacheInvalidation(self):
        repos = self.openRepository()
        netsrv = repos.c[self.defLabel]
        oldProtocolVersion = netsrv.getProtocolVersion()
        try:
            self._testTroveCacheInvalidation(repos)

            if repos.c.proxyMap:
                netsrv.setProtocolVersion(oldProtocolVersion)
                # Test old clients (that don't send a ProxyHost header)
                self.mock(transport.URLOpener, '_sendConaryProxyHostHeader',
                          False)
                self.resetRepository()
                repos = self.openRepository()
                netsrv = repos.c[self.defLabel]
                oldProtocolVersion = netsrv.getProtocolVersion()
                self._testTroveCacheInvalidation(repos)
        finally:
            netsrv.setProtocolVersion(oldProtocolVersion)

    def _testTroveCacheInvalidation(self, repos):

        def _dump(url):
            fd, fname = tempfile.mkstemp(dir=self.workDir)
            uo = urllib2.urlopen(url)
            while 1:
                buf = uo.read(16384)
                if not buf:
                    break
                os.write(fd, buf)
            os.close(fd)
            return fname

        def newTrove(name, version, flavor, changelog=None,
                type=trove.TROVE_TYPE_NORMAL, trvobj=None):
            if trvobj:
                trv = trvobj
            else:
                # Create the trove
                trv = trove.Trove(name, version, flavor, changelog, type=type)

            trv.computeDigests()

            # Create the changeset
            trovecs = trv.diff(None)[0]
            cs = changeset.ChangeSet()
            cs.newTrove(trovecs)

            repos.commitChangeSet(cs)

            return trv

        ver1 = versions.Version([self.defLabel,
            versions.Revision("10:1.2-3", frozen=True)])
        ver2 = versions.Version([self.defLabel,
            versions.Revision("20:1.1-4", frozen=True)])
        x86 = deps.parseFlavor('is:x86')
        x86_64 = deps.parseFlavor('is:x86_64')

        trv1 = newTrove('trvname', ver1, x86)
        trv2 = newTrove(trv1.getName(), ver2, x86)
        trv3 = newTrove(trv1.getName(), ver2, x86, type=trove.TROVE_TYPE_REMOVED)

        trv4 = trove.Trove("trvname2", ver1, x86)
        trv4.addTrove("trvname2:data", ver1, x86)
        trv4.addTrove("trvname2:debuginfo", ver1, x86, byDefault=False)

        trv4 = newTrove(None, None, None, trvobj=trv4)

        # Remove a referenced trove
        trv5 =  newTrove('trvname3:data', ver1, x86)
        trv6 =  newTrove('trvname3:debuginfo', ver1, x86)
        trv7 = trove.Trove("trvname3", ver1, x86)
        trv7.addTrove("trvname3:data", ver1, x86)
        trv7.addTrove("trvname3:debuginfo", ver1, x86, byDefault=False)
        trv7 = newTrove(None, None, None, trvobj=trv7)

        trv5 = newTrove(trv5.getName(), trv5.getVersion(),
                    trv5.getFlavor(), type=trove.TROVE_TYPE_REMOVED)

        trv5d = newTrove(trv5.getName(), ver2, trv5.getFlavor())
        trv6d = newTrove(trv6.getName(), ver2, trv6.getFlavor())
        trv8 = trove.Trove(trv7.getName(), ver2, x86)
        trv8.addTrove("trvname3:data", ver2, x86)
        trv8.addTrove("trvname3:debuginfo", ver2, x86, byDefault=False)
        trv8 = newTrove(None, None, None, trvobj=trv8)

        #recurse, withFiles, withFileContents, excludeAutoSource
        callargs = [True, False, False, False]

        csspec = (trv1.getName(),
                  (0, 0),
                  (trv3.getVersion().asString(), trv3.getFlavor().freeze()),
                  True)

        netsrv = repos.c[self.defLabel]
        netsrv.setProtocolVersion(37)
        self.assertRaises(errors.TroveMissing, netsrv.getChangeSet,
            [csspec], *callargs)

        # Request the removed trove
        item = (trv4.getName(),
                (None, None),
                (trv4.getVersion(), trv4.getFlavor()),
                True)
        csspec = (item[0],
                  (0, 0),
                  (item[2][0].asString(), item[2][1].freeze()),
                  item[3])

        css = []
        tcss = []
        tis = []
        for clientVersion in [37, 41, 37, 41]:
            netsrv.setProtocolVersion(clientVersion)
            ret = netsrv.getChangeSet([csspec], *callargs)
            if clientVersion == 37:
                self.assertEqual(len(ret), 4)
            else:
                self.assertEqual(len(ret), 5)
            url = ret[0]

            fname = _dump(url)

            cs = changeset.ChangeSetFromFile(fname)
            css.append(cs)

            # The package is the same; do our tests based on a component
            tcs = [ x for x in cs.iterNewTroveList() if
                        x.getName() == 'trvname2:data' ][0]
            tcss.append(tcs)

            assert(tcs.troveInfoDiff() == tcs.getFrozenTroveInfo())
            ti = trove.TroveInfo(tcs.troveInfoDiff.freeze())
            tis.append(ti)

        # Same client version should issue same result
        self.assertEqual(tcss[0].troveInfoDiff.freeze(),
                             tcss[2].troveInfoDiff.freeze())
        self.assertEqual(tcss[1].troveInfoDiff.freeze(),
                             tcss[3].troveInfoDiff.freeze())
        # This should be where the difference between old clients and new
        # clients is
        self.assertFalse(tcss[0].troveInfoDiff.freeze() ==
                    tcss[1].troveInfoDiff.freeze())

        # For old clients, we've synthesized a real trove instead of a trove
        # that is missing
        self.assertFalse(tis[0].flags.isMissing())
        self.assertTrue(tis[1].flags.isMissing())

        # Request the removed trove
        item = (trv5.getName(),
                (trv5.getVersion(), trv5.getFlavor()),
                (trv8.getVersion(), trv8.getFlavor()),
                False)
        csspec = (item[0],
                  (item[1][0].asString(), item[1][1].freeze()),
                  (item[2][0].asString(), item[2][1].freeze()),
                  item[3])

        del tis

        css = []
        fnames = []
        fconts = []

        # Fetch changeset, both with old client and with new client(s)
        for clientVersion in [37, 41, 43]:
            netsrv.setProtocolVersion(clientVersion)
            ret = netsrv.getChangeSet([csspec], *callargs)
            if clientVersion == 37:
                self.assertEqual(len(ret), 4)
            else:
                self.assertEqual(len(ret), 5)
            url = ret[0]

            fname = _dump(url)
            fnames.append(fname)

            cs = changeset.ChangeSetFromFile(fname)
            css.append(cs)

            fcont = filecontainer.FileContainer(util.ExtendedFile(fname, buffering=False))
            fconts.append(fcont)

        # No caching across versions
        self.assertFalse(fnames[0] == fnames[1])
        self.assertFalse(fnames[1] == fnames[2])
        self.assertFalse(fnames[0] == fnames[2])

        self.assertEqual(list(css[0].iterNewTroveList()), [])
        self.assertEqual(list(css[1].iterNewTroveList()), [])
        self.assertEqual(list(css[2].iterNewTroveList()), [])
        self.assertEqual(list(css[0].getOldTroveList()), [])
        self.assertEqual(list(css[1].getOldTroveList()), [])
        self.assertEqual(list(css[2].getOldTroveList()), [])

        assert(fconts[0].version == filecontainer.FILE_CONTAINER_VERSION_NO_REMOVES)
        assert(fconts[1].version == filecontainer.FILE_CONTAINER_VERSION_WITH_REMOVES)
        assert(fconts[2].version == filecontainer.FILE_CONTAINER_VERSION_FILEID_IDX)

        # New-style client
        netsrv.setProtocolVersion(48)
        # Test that the netclient does have all the file container versions as
        # members
        csVers = [repos.FILE_CONTAINER_VERSION_NO_REMOVES,
                  repos.FILE_CONTAINER_VERSION_WITH_REMOVES,
                  repos.FILE_CONTAINER_VERSION_FILEID_IDX]
        fconts = []
        for csVer in csVers:
            # Explicitly request a changeset version
            cargs = callargs + [csVer]
            ret = netsrv.getChangeSet([csspec], *cargs)
            self.assertEqual(len(ret), 5)
            url = ret[0]

            fname = _dump(url)
            fnames.append(fname)

            cs = changeset.ChangeSetFromFile(fname)
            css.append(cs)

            fcont = filecontainer.FileContainer(util.ExtendedFile(fname, buffering=False))
            fconts.append(fcont)

        assert(fconts[0].version == filecontainer.FILE_CONTAINER_VERSION_NO_REMOVES)
        assert(fconts[1].version == filecontainer.FILE_CONTAINER_VERSION_WITH_REMOVES)
        assert(fconts[2].version == filecontainer.FILE_CONTAINER_VERSION_FILEID_IDX)


    def testChunked(self):
        repos = self.openRepository(0)
        # make a 4.5 GB (sparse) file
        huge = self.workDir + '/hugefile'
        f = open(huge, 'w')
        #f.seek(long(4.5 * 1024 * 1024 * 1024))
        f.seek(long(4.5 * 1024))
        f.write('1')
        f.close()
        sb = os.stat(huge)
        url = self.cfg.repositoryMap['localhost'] + '?huge'
        # make the repository think it is expecting this file
        tmpDir = self.servers.getServer(0).reposDir
        f = open(tmpDir + '/huge-in', 'w')
        f.close()
        # now put the file
        rc = netclient.httpPutFile(url, open(huge), sb.st_size,
                                   chunked=True)
        self.assertEqual(rc, (200, 'OK'))
        # compare the results, just to be sure
        f1 = open(tmpDir + '/huge-in', 'r')
        f2 = open(huge)
        bufSize = 4 * 1024 * 1024
        while 1:
            buf1 = f1.read(bufSize)
            buf2 = f2.read(bufSize)
            self.assertEqual(buf1, buf2)
            if not buf1 and not buf2:
                break

    def testHiddenTroves(self):

        def _hasTrove(repos, trv, hidden = False):
            return repos.hasTroves([ trv.getNameVersionFlavor() ],
                                   hidden = hidden)[trv.getNameVersionFlavor()]

        repos = self.openRepository()
        label = versions.Label("localhost@foo:bar")
        self.addUserAndRole(repos, label, "normal", "n")
        repos.addAcl(label, "normal", None, None, write=True)
        normRepos = self.getRepositoryClient(user = 'normal', password = 'n')

        self.addUserAndRole(repos, label, "mirror", "m")
        repos.addAcl(label, "mirror", None, None, write=True, remove=True)
        repos.setRoleIsAdmin(label, "mirror", True)
        repos.setRoleCanMirror(label, "mirror", True)
        mirrorRepos = self.getRepositoryClient(user = 'mirror', password = 'm')

        anonRepos = self.getRepositoryClient(user = 'anonymous',
                                             password = 'anonymous')

        # normal user doesn't have mirror permission, so cannot commit hidden
        # troves
        self.assertRaises(errors.InsufficientPermission,
            self.addComponent, 'foo:runtime', '1', hidden = True,
                               repos = normRepos)

        trv1 = self.addComponent('foo:runtime', '1', repos = normRepos)

        # admin user can add a hidden trove though
        self.addComponent('foo:runtime', '2', hidden = True, repos = repos)

        # as can a mirror user
        trv3 = self.addComponent('foo:runtime', '3', hidden = True,
                                repos = mirrorRepos)

        # we shouldn't normally see this trove existints
        assert(not _hasTrove(repos, trv3, hidden = False))
        # anyone with write perms can ask though
        assert(    _hasTrove(normRepos, trv3, hidden = True))
        # read-only uers can also see it (new behavior)
        assert(    _hasTrove(anonRepos, trv3, hidden = True))

        # the latest should look like version 1
        assert(repos.getTroveLatestVersion(trv3.getName(),
                                          trv3.getVersion().branch())
                        == trv1.getVersion() )

        # the hidden trove is fetchable if the exact NVF is known
        anonRepos.getTrove(*trv3.getNameVersionFlavor())

        # until we publish the hidden versions, which normal users can't do
        self.assertRaises(errors.InsufficientPermission,
                          normRepos.presentHiddenTroves, 'localhost')

        # but mirror users can
        mirrorRepos.presentHiddenTroves('localhost')

        # which lets everyone see the trove
        assert(_hasTrove(anonRepos, trv3))

        # and makes it appear as the latest one
        assert(repos.getTroveLatestVersion(trv3.getName(),
                                          trv3.getVersion().branch())
                        == trv3.getVersion() )

    def testGetNewTroveInfo(self):
        # set up users
        repos = self.openRepository()
        self.addUserAndRole(repos, self.cfg.buildLabel, "mirror", "m")
        repos.addAcl(self.cfg.buildLabel, "mirror", None, None,
                     write=True, remove=True)
        repos.setRoleIsAdmin(self.cfg.buildLabel, "mirror", True)
        repos.setRoleCanMirror(self.cfg.buildLabel, "mirror", True)
        mirrorRepos = self.getRepositoryClient(user = 'mirror', password = 'm')
        anonRepos = self.getRepositoryClient(user = 'anonymous',
                                             password = 'anonymous')

        trv1 = self.addComponent('foo:runtime', '1')
        mark = mirrorRepos.getNewTroveList('localhost', 0)[0][0]
        info = mirrorRepos.getNewTroveInfo('localhost', mark - 1)
        self.assertEqual(info, [])

        # sleep for a second so we're sure to get a different changed
        # column
        self.sleep(1.2)

        # sign that test trove
        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        self.cfg.signatureKey = fingerprint
        signtrove.signTroves(self.cfg, ['foo:runtime'])

        tup = (trv1.getName(), trv1.getVersion(), trv1.getFlavor())
        # get the new stuff -- this should include everything from
        # the trove
        info = mirrorRepos.getNewTroveInfo('localhost', mark, thaw=True)
        self.assertEqual(len(info), 1)
        self.assertEqual(info[0][1], tup)
        # check to see that the troveInfo matches
        i2 = info[0][2]
        self.assertTrue(isinstance(i2, trove.TroveInfo))
        t = repos.getTrove(trv1.getName(), trv1.getVersion(), trv1.getFlavor())
        self.assertEqual(i2, t.troveInfo)

        # get the new stuff as of one second past the mark -- this
        # should only be the new sig
        info = mirrorRepos.getNewTroveInfo('localhost', mark + 1, thaw=False)
        self.assertEqual(len(info), 1)
        self.assertEqual(info[0][1], tup)
        self.assertTrue(isinstance(info[0][2], str))
        # check to see that the troveInfo only as the signature
        i1 = trove.TroveInfo()
        i1.sigs = t.troveInfo.sigs
        i2 = trove.TroveInfo(base64.b64decode(info[0][2]))
        # sort the sigs to avoid hash differences
        self.assertEqual(sorted(i1.sigs), sorted(i2.sigs))
        i1.sigs = i1.sigs.__class__()
        i2.sigs = i2.sigs.__class__()
        self.assertEqual(i1.freeze(), i2.freeze())

        # make sure normal users see nothing
        info = repos.getNewTroveInfo('localhost', mark)
        self.assertEqual(info, [])

        # and same for anonymous
        info = anonRepos.getNewTroveInfo('localhost', mark)
        self.assertEqual(info, [])

        # now check limiting the troveInfo type
        info = mirrorRepos.getNewTroveInfo('localhost', mark, thaw=True,
                                           infoTypes=[trove._TROVEINFO_TAG_SIZE])
        self.assertEqual(len(info), 1)
        self.assertEqual(info[0][1], tup)
        # check to see that the troveInfo matches
        i1 = trove.TroveInfo()
        i1.size = t.troveInfo.size
        self.assertEqual(i1, info[0][2])

        # add another test component
        info1 = mirrorRepos.getNewTroveInfo('localhost', mark)
        trv1 = self.addComponent('bar:runtime', '1')
        trv2 = self.addComponent('baz:runtime', '/localhost@excluded:branch/1-1-1')
        info2 = mirrorRepos.getNewTroveInfo('localhost', mark)
        self.assertEqual(info1, info2)
        mark = max([x[0] for x in mirrorRepos.getNewTroveList('localhost', mark)])

        # sign with a different key
        fingerprint = 'F7440D78FE813C882212C2BF8AC2828190B1E477'
        keyRing = open(resources.get_archive() + '/pubring.gpg')
        keyData = openpgpfile.exportKey(fingerprint, keyRing)
        keyData.seek(0)
        keyData = keyData.read()

        keyCache = openpgpkey.getKeyCache()
        repos.addNewPGPKey(self.cfg.buildLabel, 'test', keyData)
        keyCache.getPrivateKey(fingerprint, '111111')
        self.cfg.signatureKey = fingerprint
        # make sure these sigs show up after mark + 1
        self.sleep(1.2)
        signtrove.signTroves(self.cfg, [ 'foo:runtime', 'bar:runtime',
                                         'baz:runtime=/localhost@excluded:branch/1-1-1'])
        info = mirrorRepos.getNewTroveInfo('localhost', mark + 1,
                                           labels=[self.cfg.buildLabel.asString()],
                                           thaw=False)
        self.assertEqual(len(info), 2)
        self.assertTrue(tup in [x[1] for x in info])
        bartup = ('bar:runtime', tup[1], tup[2])
        self.assertTrue(bartup in [x[1] for x in info])
        # now include all labels
        info = mirrorRepos.getNewTroveInfo('localhost', mark + 1,
                                           thaw=False)
        self.assertEqual(len(info), 3)
        self.assertTrue(tup in [x[1] for x in info])
        self.assertTrue(bartup in [x[1] for x in info])
        newtup = (trv2.getName(), trv2.getVersion(), trv2.getFlavor())
        self.assertTrue(newtup in [x[1] for x in info])

        # check empty short circuit
        info = mirrorRepos.getNewTroveInfo('localhost', mark + 1,
                                           labels=['garblygook@rpl:1'],
                                           thaw=False)
        self.assertEqual(len(info), 0)

        # check for badly formed label
        self.assertRaises(errors.InsufficientPermission,
                          mirrorRepos.getNewTroveInfo, 'localhost', mark + 1,
                          labels=['garblygook'],
                          thaw=False)

    def testSetTroveInfo(self):
        repos = self.openRepository()
        trv1 = self.addComponent('foo:runtime', '1')
        i = trove.TroveInfo()
        mi = trove.MetadataItem()
        mi.shortDesc.set('This is the short description')
        i.metadata.addItem(mi)
        info = [ ((trv1.getName(), trv1.getVersion(), trv1.getFlavor()),
                  base64.b64encode(i.freeze())) ]

        repos = self.openRepository()
        label = versions.Label("localhost@foo:bar")
        self.addUserAndRole(repos, label, "normal", "n")
        repos.addAcl(label, "normal", None, None, write=True)
        normRepos = self.getRepositoryClient(user = 'normal', password = 'n')
        # make sure a normal user can't access the method
        self.assertRaises(errors.InsufficientPermission,
                              normRepos.setTroveInfo, info, freeze=False)
        # set a metadata troveinfo item for one that did not exist before
        rc = repos.setTroveInfo(info, freeze=False)
        self.assertEqual(rc, 1)
        t = repos.getTrove(trv1.getName(), trv1.getVersion(), trv1.getFlavor())
        description = [ x for x in t.troveInfo.metadata ][0].shortDesc()
        self.assertEqual(description, 'This is the short description')

        # now update the metadata troveinfo item
        mi.shortDesc.set('This is the updated short description')
        mi.clearDigitalSignatures()
        info = [ ((trv1.getName(), trv1.getVersion(), trv1.getFlavor()),
                  base64.b64encode(i.freeze())) ]
        rc = repos.setTroveInfo(info, freeze=False)
        self.assertEqual(rc, 1)
        t = repos.getTrove(trv1.getName(), trv1.getVersion(), trv1.getFlavor())
        description = [ x for x in t.troveInfo.metadata ][0].shortDesc()
        self.assertEqual(description, 'This is the updated short description')

        # now lets do more than one troveInfo element for one instance.
        self.cfg.signatureKey = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        t.addDigitalSignature(self.cfg.signatureKey)
        mi.shortDesc.set('This is the updated again short description')
        mi.clearDigitalSignatures()
        i = trove.TroveInfo()
        i.sigs = t.troveInfo.sigs
        i.metadata.addItem(mi)
        info = [ ((trv1.getName(), trv1.getVersion(), trv1.getFlavor()),
                  base64.b64encode(i.freeze())) ]
        t = repos.getTrove(trv1.getName(), trv1.getVersion(), trv1.getFlavor())
        # make sure it isn't currently signed
        try:
            t.troveInfo.sigs.digitalSigs.getSignature('95B457D16843B21EA3FC73BBC7C32FC1F94E405E')
        except:
            pass
        else:
            self.fail('expected exception')
        # set the signature and new description in one go
        rc = repos.setTroveInfo(info, freeze=False)
        self.assertEqual(rc, 2)
        t = repos.getTrove(trv1.getName(), trv1.getVersion(), trv1.getFlavor())
        description = [ x for x in t.troveInfo.metadata ][0].shortDesc()
        self.assertEqual(description, 'This is the updated again short description')
        t.troveInfo.sigs.digitalSigs.getSignature('95B457D16843B21EA3FC73BBC7C32FC1F94E405E')

        # now we'll work on two instances at once
        trv2 = self.addComponent('bar:runtime', '1')
        i1 = trove.TroveInfo()
        mi1 = trove.MetadataItem()
        mi1.shortDesc.set('This is the short description for foo:runtime')
        i1.metadata.addItem(mi1)
        i2 = trove.TroveInfo()
        mi2 = trove.MetadataItem()
        mi2.shortDesc.set('This is the short description for bar:runtime')
        i2.metadata.addItem(mi2)
        i2.size.set(123)

        info = [ ((trv1.getName(), trv1.getVersion(), trv1.getFlavor()), i1),
                 ((trv2.getName(), trv2.getVersion(), trv1.getFlavor()), i2) ]
        rc = repos.setTroveInfo(info)
        self.assertEqual(rc, 2)
        for trv, mi in ((trv1, mi1), (trv2, mi2)):
            t = repos.getTrove(trv.getName(), trv.getVersion(), trv.getFlavor())
            description = [ x for x in t.troveInfo.metadata ][0].shortDesc()
            self.assertEqual(description, mi.shortDesc())
        self.assertTrue(t.troveInfo.size() != 123)

    def testSetTroveInfoOnOldRepositories(self):
        # setTroveInfo must not store new metadata information in 
        # repositories that can't handle it.
        i = trove.TroveInfo()
        mi = trove.MetadataItem()
        mi.shortDesc.set('shortdesc')
        mi.keyValue['foo'] = 'bar'
        i.metadata.addItem(mi)
        trv = self.addComponent('foo:runtime')
        info = [ (trv.getNameVersionFlavor(), i)]
        repos = self.openRepository()
        repos.c['localhost'].setProtocolVersion(64)
        rc = repos.setTroveInfo(info)
        trv = repos.getTrove(*trv.getNameVersionFlavor())
        assert(trv.troveInfo.metadata.get()['keyValue'] is None)
        repos.c['localhost'].setProtocolVersion(65)
        rc = repos.setTroveInfo(info)
        trv = repos.getTrove(*trv.getNameVersionFlavor())
        assert(dict(trv.troveInfo.metadata.get()['keyValue'])['foo'] == 'bar')

    def testSetTroveInfoOnMissingTroves(self):
        repos = self.openRepository()
        def _getinfo(trv=None):
            i = trove.TroveInfo()
            mi = trove.MetadataItem()
            if trv:
                mi.shortDesc.set('This is the short description for %s=%s[%s]' % trv.getNameVersionFlavor())
            else:
                mi.shortDesc.set('This is the short description')
            i.metadata.addItem(mi)
            info = [ ((trv.getName(), trv.getVersion(), trv.getFlavor()),
                      base64.b64encode(i.freeze())) ]
            return info
        # set a metadata troveinfo item
        trv1 = self.addComponent('foo:runtime', '1')
        info = _getinfo(trv1)
        rc = repos.setTroveInfo(info, freeze=False)
        self.assertEqual(rc, 1)
        # now transform this trove in a "not present" one
        self.markRemoved(trv1.getName())
        self.assertRaises(errors.TroveMissing, repos.setTroveInfo, info, freeze=False)
        # try updating for a trove that has isPresent=0
        trv2 = self.addComponent("bar:lib", "2")
        info = _getinfo(trv2)
        # this will create a bar:runtime trove that is not present
        self.addCollection("group-bar", "1", [
            (trv2.getName(), trv2.getVersion(), trv2.getFlavor()),
            ("bar:runtime", trv2.getVersion(), trv2.getFlavor())
            ])
        # setting troveinfo on bar:lib should work
        self.assertEqual(repos.setTroveInfo(info, freeze=False), 1)
        # setting troveinfo on bar:runtime should fail
        info1 = [ (("bar:runtime", trv2.getVersion(), trv2.getFlavor()), info[0][1]) ]
        self.assertRaises(errors.InsufficientPermission, repos.setTroveInfo, info1, freeze=False)

    def testAddMetadataItems(self):
        repos = self.openRepository()
        trv1 = self.addComponent('foo:runtime', '1')
        mi = trove.MetadataItem()
        mi.shortDesc.set('This is the short description')
        repos.addMetadataItems([((trv1.getName(), trv1.getVersion(),
                                 trv1.getFlavor()), mi)])
        t = repos.getTrove(*trv1.getNameVersionFlavor())
        self.assertEqual(t.getMetadata()['shortDesc'],
                             'This is the short description')
        # now test adding some more...
        mi = trove.MetadataItem()
        mi.url.set("http://localhost/")
        mi.longDesc.set("This is supposed to be a long description, but it isn't")
        repos.addMetadataItems([((trv1.getName(), trv1.getVersion(),
                                 trv1.getFlavor()), mi)])
        t = repos.getTrove(*trv1.getNameVersionFlavor())
        self.assertEqual(t.getMetadata()['url'],
                             'http://localhost/')
        self.assertTrue(t.getMetadata()['longDesc'])
        self.assertTrue(t.getMetadata()['shortDesc'])

        # now test adding some stuff to more than one trove at once
        trv2 = self.addComponent('bar:runtime', '2')
        mi = trove.MetadataItem()
        mi.url.set("http://localhost2/")
        mi2 = trove.MetadataItem()
        mi2.url.set("http://localhost3/")
        mi2.keyValue['key'] = 'lock'
        repos.addMetadataItems([((trv1.getName(), trv1.getVersion(),
                                 trv1.getFlavor()), mi),
                                ((trv2.getName(), trv2.getVersion(),
                                  trv2.getFlavor()), mi2)])
        t = repos.getTrove(*trv1.getNameVersionFlavor())
        self.assertEqual(t.getMetadata()['url'], 'http://localhost2/')
        t = repos.getTrove(*trv2.getNameVersionFlavor())
        self.assertEqual(t.getMetadata()['url'], 'http://localhost3/')
        self.assertEqual(t.getMetadata()['keyValue']['key'], 'lock')
        # test adding to a non-existing trove
        tup = (trv1.getName(), trv2.getVersion(), trv1.getFlavor())
        self.assertRaises(errors.TroveMissing, repos.addMetadataItems,
                              [ (tup, mi) ])
        chL = [ (trv2.getName(), (None, None),
                (trv2.getVersion(), trv2.getFlavor()), True) ]
        fpList = repos.getChangeSetFingerprints(chL, False, False, False,
                False, False)
        # This will replace the previous key/value metadata
        mi3 = trove.MetadataItem()
        mi3.keyValue['key1'] = 'val1'
        mi3.keyValue['key2'] = 'val2'
        repos.addMetadataItems([(trv2.getNameVersionFlavor(), mi3)])
        t = repos.getTrove(*trv2.getNameVersionFlavor())
        self.assertEqual(t.getMetadata()['url'], 'http://localhost3/')
        # XXX CNY-3811: key/value metadata should be additive
        self.assertEqual(sorted(t.getMetadata()['keyValue'].items()),
                # correct values after CNY-3811 gets fixed
                # [('key', 'lock'), ('key1', 'val1'), ('key2', 'val2')])
                [('key1', 'val1'), ('key2', 'val2')])
        fpList2 = repos.getChangeSetFingerprints(chL, False, False, False,
                False, False)
        self.assertNotEqual(fpList2, fpList)

    def testCreateChangesetOptimizations(self):
        # ensure that if you call createChangeSet on a trove with
        # distributed file contents, the client doesn't have to
        # assemble the changeset by calling getFileContents
        try:
            for test in (0, 1):
                self.stopRepository(0)
                self.stopRepository(1)
                if test == 0:
                    self.openRepository(0)
                    repo = self.openRepository(1)
                else:
                    self.servers.stopServer(0)
                    repo = self.openRepository(0, serverName=['localhost', 'localhost1'])
                self.addComponent('foo:runtime',
                                  repos=repo,
                                  fileContents = [ ( '/a', 'a' ),
                                                   ( '/b', 'b') ] )
                t = self.addComponent('foo:runtime',
                                      '/localhost@rpl:devel//localhost1@rpl:1/1-0.1-1',
                                      repos=repo,
                                      fileContents = [ ( '/a', 'a', '/localhost@rpl:devel/1-1-1' ),
                                                       ( '/b', 'b rev 2', '/localhost@rpl:devel//localhost1@rpl:1/1-0.1-1') ] )
                class Checker:
                    def __init__(self, repo):
                        self.hit = False
                        self._getFileContents = repo.getFileContents

                    def getFileContents(self, *args, **kw):
                        self.hit = True
                        return self._getFileContents(*args, **kw)
                if test == 1:
                    # remove anonymous access to make sure that
                    # an update service can grant access to distributed files
                    # because they are on the same server
                    label = t.getVersion().trailingLabel()
                    self.addUserAndRole(repo, label, 'limited', 'bar')
                    # grant access only to the localhost1@rpl:1 label
                    repo.addAcl(label, 'limited', 'ALL', label)
                    repo.deleteUserByName('localhost', 'anonymous')
                    repo.deleteUserByName('localhost', 'test')
                    repo = self.getRepositoryClient(user='limited',
                                                    password='bar',
                                                    serverIdx=0)
                checker = Checker(repo)
                self.mock(repo, "getFileContents", checker.getFileContents)
                cs = repo.createChangeSet([('foo:runtime',
                                            (None, None),
                                            (t.getVersion(), t.getFlavor()),
                                            False)])
                if test == 0:
                    if not checker.hit:
                        self.fail('Expected createChangeSet to call getFileContents')
                else:
                    if checker.hit:
                        self.fail('Did not expect createChangeSet to call getFileContents')
        finally:
            self.resetRepository(0)
            self.stopRepository(0)
            self.resetRepository(1)
            self.stopRepository(1)

    def testUsingProxy(self):
        if os.environ.get('CONARY_PROXY', None):
            repos = self.openRepository()
            assert(repos.c.proxyMap)

    def testInfoOnly(self):

        def _check(repos, rawRepos, jobList, recurse = False,
                   csVersion = filecontainer.FILE_CONTAINER_VERSION_LATEST,
                   expectCachedAtProxy = False):
            fn = self.workDir + '/tmp.ccs'

            l = []
            for (name, (oldVersion, oldFlavor),
                       (newVersion, newFlavor), abs) in jobList:
                if oldVersion is None:
                    oldVersion = 0
                    oldFlavor = 0
                else:
                    oldVersion = str(oldVersion)
                    oldFlavor = oldFlavor.freeze()

                l.append( (name, (oldVersion, oldFlavor),
                                 (str(newVersion), newFlavor.freeze()), abs ) )

            fpList = rawRepos.getChangeSetFingerprints(l, recurse, True, True,
                                                   False, False)

            rc = rawRepos.getChangeSet(l, recurse, True, True, False,
                                       csVersion, False, True)
            size = sum([ int(x[0]) for x in rc[1] ])
            assert(rc[0] == '')

            if self.proxy:
                # the changeset shouldn't be cached
                contents = datastore.ShallowDataStore(self.proxy.reposDir +
                        '/cscache')
                for fp in fpList:
                    path = fp + '-%s.1' % \
                            (filecontainer.FILE_CONTAINER_VERSION_LATEST)
                    if expectCachedAtProxy:
                        assert(contents.hasFile(path))
                    else:
                        assert(not contents.hasFile(path))

            server = self.servers.getCachedServer()
            if server.cache:
                # the server should be cached on the server immediately,
                # whether or not it's cached on the proxy
                contents = datastore.ShallowDataStore(server.cache.getPath())
                for fp in fpList:
                    path = fp + '-%s.1' % \
                            (filecontainer.FILE_CONTAINER_VERSION_LATEST)
                    assert(contents.hasFile(path))

            repos.createChangeSetFile(jobList, fn, recurse = recurse,
                                      changesetVersion = csVersion)
            assert(os.stat(fn).st_size == size)

            fc = filecontainer.FileContainer(
                        util.ExtendedFile(fn, buffering = False))
            assert(fc.version == csVersion)

            if self.proxy:
                # it gets cached when it's retrieved for real
                for fp in fpList:
                    assert(contents.hasFile(path))

        # we can't use a client here because if doesn't expose the raw
        # getChangeSet call
        fn = self.workDir + '/tmp.ccs'
        trv1 = self.addComponent('foo:runtime', '1')
        repos = self.openRepository()
        rawRepos = repos.c['localhost']

        _check(repos, rawRepos, [ (trv1.getName(), (None, None),
                     (trv1.getVersion() , trv1.getFlavor() ), True) ] )

        # in order to get size info on old changeset formats, the proxy
        # actually has to do a conversion step.  We might as well cache
        # the changeset while we're at it.
        _check(repos, rawRepos, [ (trv1.getName(), (None, None),
                     (trv1.getVersion() , trv1.getFlavor() ), True) ],
               csVersion = filecontainer.FILE_CONTAINER_VERSION_NO_REMOVES,
               expectCachedAtProxy = True)

    def testRepositoryMismatch(self):
        # test the RepositoryMismatch exception by forcing a commit to
        # the wrong repository.
        repos = self.openRepository()
        self.addComponent('foo:runtime', '1.0')
        csPath = self.workDir + '/foo.ccs'
        self.changeset(repos, ['foo:runtime'], csPath)

        self.stopRepository()

        repos = self.openRepository(1)
        repos.c.map.append(('localhost', repos.c.map['localhost1']))
        try:
            repos.commitChangeSetFile(csPath)
        except errors.RepositoryMismatch, e:
            assert(e.wrong == 'localhost')
            assert(e.right == [ 'localhost1' ])
        else:
            assert 0, 'RepositoryMismatch exception expected'

    def testProxyLog(self):
        def _reset(path):
            # zero out the file
            os.close(os.open(path, os.O_WRONLY | os.O_TRUNC))

        def _checkLog(path, expected):
            cl = reposlog.RepositoryCallLogger(path, None)
            got = [ x.methodName for x in cl ]
            self.assertEquals(expected, got)

        repos = self.openRepository()
        comp = self.addComponent('foo:runtime', '1.0')

        if not self.proxy: return

        proxyLog = self.proxy.reposDir + '/proxy.log'

        _reset(proxyLog)

        # we should see getChangeSet followed by change set creation
        repos.getTrove(*comp.getNameVersionFlavor())
        _checkLog(proxyLog, [ '+getChangeSet', '__createChangeSets' ])

        _reset(proxyLog)
        # we should see getChangeSet, but no change set creation
        # because it's already cached
        repos.getTrove(*comp.getNameVersionFlavor())
        _checkLog(proxyLog, [ '+getChangeSet' ])

    def testSystemIdThroughProxy(self):
        def _reset(path):
            # zero out the file
            os.close(os.open(path, os.O_WRONLY | os.O_TRUNC))

        def _checkLog(path, systemId=None):
            cl = reposlog.RepositoryCallLogger(path, None)
            got = set((x.systemId for x in cl))
            if isinstance(systemId, str):
                systemId = set([systemId ])
            elif not systemId:
                systemId = set()
            self.assertEquals(systemId, got)

        repos = self.openRepository()
        server = self.servers.getServer()
        systemId = repos.c.systemId
        comp = self.addComponent('foo:runtime', '1.0')

        if self.proxy:
            proxyLog = os.path.join(self.proxy.reposDir, 'proxy.log')
            _reset(proxyLog)

        repoLog = os.path.join(server.reposDir, 'repos.log')
        _reset(repoLog)

        # we should see getChangeSet followed by change set creation
        repos.getTrove(*comp.getNameVersionFlavor())

        if self.proxy:
            _checkLog(proxyLog, systemId)
            _reset(proxyLog)

        _checkLog(repoLog, systemId)
        _reset(repoLog)

        # we should see getChangeSet, but no change set creation
        # because it's already cached
        repos.getTrove(*comp.getNameVersionFlavor())

        if self.proxy:
            _checkLog(proxyLog, systemId)
            # I don't think the repository log should get an entry since the
            # proxy should already have a cached copy.
            _checkLog(repoLog)

        if not self.proxy:
            _checkLog(repoLog, systemId)

    def testDatabaseLocked(self):
        raise testhelp.SkipTestException("Devise a more realistic test that "
                "doesn't fail in early setup")
        # CNY-1596
        if os.environ.get('CONARY_REPOS_DB', 'sqlite') != 'sqlite':
            return

        self.stopRepository()
        repos = self.openRepository(deadlockRetry = 0)

        trv = self.addComponent('foo:runtime', '1.0')
        server = self.servers.getServer(0)
        db = dbstore.connect(server.reposDB.path,
                             driver = server.reposDB.driver)
        cursor = db.cursor()
        # place a lock
        cursor.execute('BEGIN IMMEDIATE')

        # commits have a different code path than other methods
        self.assertRaises(errors.RepositoryLocked, self.addComponent,
                          'foo:runtime', '2.0')

        self.assertRaises(errors.RepositoryLocked, repos.addUser,
                          self.cfg.buildLabel, 'user', 'password')
        self.stopRepository()

    def testServerURL(self):
        # CNY-2034
        repos = self.openRepository()
        comp = self.addComponent('foo:runtime', '1.0')

        # If there is a proxy, rewrite localhost to 127.0.0.1
        if repos.c.proxyMap:
            p = repos.c.proxyMap.__class__()
            for filter, targets in repos.c.proxyMap.items():
                p.addStrategy(filter, [
                    request.URL(str(v).replace('localhost', '127.0.0.1'))
                    for v in targets])
            self.mock(repos.c, 'proxyMap', p)

        # force calls to connect to 127.0.0.1 instead of a host and make
        # sure the url which comes back matches that
        del repos.c['localhost']
        repos.c.map.append(('localhost', 'http://127.0.0.1:%s/' %
                repos.c.map['localhost'].split(':')[-1][:-1]))
        serverProxy = repos.c['localhost']
        self.disableInlining(serverProxy)

        rc = serverProxy.getChangeSet(
                [ (comp.getName(), (0, 0),
                   (str(comp.getVersion()), str(comp.getFlavor())), False) ],
                False, False, False, False)

        assert('127.0.0.1' in rc[0])
        assert('localhost' not in rc[0])

        rc = serverProxy.prepareChangeSet(
                [ (comp.getName(), (0, 0),
                   (str(comp.getVersion()), str(comp.getFlavor())), False) ] )

        assert('127.0.0.1' in rc[0])
        assert('localhost' not in rc[0])

        rc = serverProxy.getFileContents([])
        assert('127.0.0.1' in rc[0])
        assert('localhost' not in rc[0])

    def testCommitAction(self):
        self.stopRepository()

        scriptPath = self.workDir + '/action'
        resultsPath = self.workDir + '/results'

        f = file(scriptPath, "w")
        f.write('#!/bin/bash\n')
        f.write('echo $* > %s\n' % resultsPath)
        f.write('cat >> %s\n' % resultsPath)
        f.close()
        os.chmod(scriptPath, 0755)

        repos = self.openRepository(commitAction = scriptPath)
        comp = self.addComponent('foo:runtime', '1.0')

        self.verifyFile(resultsPath, '''\

foo:runtime
/localhost@rpl:linux/1.0-1-1

''')

        self.stopRepository()

    def testForceSSL(self):
        self.servers.stopServer(1)
        try:
            # this works because the server is ssl
            self.openRepository(1, useSSL = True, forceSSL = True)

            self.servers.stopServer(1)
            # this fails because the server is not ssl; this call fails because
            # it validates being able to call into the server
            self.assertRaises(errors.InsufficientPermission,
                    self.openRepository, 1, useSSL=False, forceSSL=True)
        finally:
            self.servers.stopServer(1)

    @staticmethod
    def disableInlining(serverProxy):
        origOpen = serverProxy._transport.opener.open
        def open(req, *args, **kwargs):
            # Prevent inlining of changeset
            del req.headers['Accept']
            return origOpen(req, *args, **kwargs)
        serverProxy._transport.opener.open = open

    def testHTTPProxyUrlOverride(self):
        # CNY-2117
        comp = self.addComponent('foo:runtime', '1.0')

        h = self.getHTTPProxy()

        h.updateConfig(self.cfg)

        try:
            repos = self.openRepository()
            serverProxy = repos.c['localhost']
            self.disableInlining(serverProxy)

            rc = serverProxy.getChangeSet(
                    [ (comp.getName(), (0, 0),
                       (str(comp.getVersion()), str(comp.getFlavor())), False) ],
                    False, False, False, False)

            self.assertFalse(rc[0].startswith('/'), "Not a full URL: %s" % rc[0])

            rc = serverProxy.prepareChangeSet(
                    [ (comp.getName(), (0, 0),
                       (str(comp.getVersion()), str(comp.getFlavor())), False) ] )

            self.assertFalse(rc[0].startswith('/'), "Not a full URL: %s" % rc)

            rc = serverProxy.getFileContents([])
            self.assertFalse(rc[0].startswith('/'), "Not a full URL: %s" % rc[0])
        finally:
            h.stop()

    def testServerNameWildcard(self):
        # CNY-2293
        repo = self.openRepository(0, serverName=['*'])
        first = self.addComponent('foo:runtime', '1.0',
                                  fileContents = [ ('/foo', '1.0') ] )
        second = self.addComponent('bar:runtime', '/localhost1@rpl:foo/2.0',
                                   fileContents = [ ('/bar', '2.0') ] )
        self.updatePkg('foo:runtime', 'bar:runtime=localhost1@rpl:foo')
        self.stopRepository()

    def testGetTroveInfo(self):
        repos = self.openRepository()
        trv = self.addComponent('foo:runtime', '1.0',
                                fileContents = [ ('/foo', '1.0') ] )
        result = repos.getTroveInfo(trove._TROVEINFO_TAG_SOURCENAME,
                                    [ trv.getNameVersionFlavor() ])
        assert(result[0]() == 'foo:source')

        r = repos.c['localhost']
        r.setProtocolVersion(63)
        self.assertRaises(errors.RepositoryError, r.getTroveInfo,
                    trove._TROVEINFO_TAG_LAST + 100,
                    [ (trv.getName(), repos.fromVersion(trv.getVersion()),
                       repos.fromFlavor(trv.getFlavor())) ])

    def testExtendedMetadata(self):
        repos = self.openRepository()
        repos.setRoleCanMirror(self.cfg.buildLabel, 'test', True)

        # protocol < 65 filters out extended metadata from a couple of calls
        trv = self.addComponent('foo:runtime', '1.0',
                                fileContents = [ ('/foo', '1.0') ] )
        mdi = trove.MetadataItem()
        mdi.keyValue['key'] = 'value';
        mdi.shortDesc.set('short description')
        md = trove.Metadata()
        md.addItem(mdi)
        mark = repos.getNewTroveList('localhost', '-1')[0][0]
        repos.addMetadataItems([(trv.getNameVersionFlavor(), mdi)])

        mdFromRepo = repos.getTroveInfo(trove._TROVEINFO_TAG_METADATA,
                                [ trv.getNameVersionFlavor() ])[0]
        assert(mdFromRepo.freeze() == md.freeze())
        tiList = repos.getNewTroveInfo('localhost', mark,
                                   [ trove._TROVEINFO_TAG_METADATA ])
        assert(tiList[0][2].metadata.freeze() == md.freeze())

        repos.c['localhost'].setProtocolVersion(64)
        mdFromRepo = repos.getTroveInfo(trove._TROVEINFO_TAG_METADATA,
                                [ trv.getNameVersionFlavor() ])[0]
        mdi = mdFromRepo.flatten()[0]
        assert(mdi.shortDesc() == 'short description')
        assert(mdi.keyValue.items() == [])
        tiList = repos.getNewTroveInfo('localhost', mark,
                                   [ trove._TROVEINFO_TAG_METADATA ])
        mdi = tiList[0][2].metadata.flatten()[0]
        assert(mdi.shortDesc() == 'short description')
        assert(mdi.keyValue.items() == [])

    def testListTrovesMultiLabelsSameServer(self):
        # CNY-3187
        # Listing all troves from multiple labels that share the same
        # hostname.
        repos = self.openRepository()
        self.addComponent('foo:runtime', 'localhost@test:label1')
        self.addComponent('foo:runtime', 'localhost@test:label2')
        self.addComponent('foo:runtime', 'localhost@test:label3')

        self.cfg.searchPath = [ "localhost@test:label1",
                                "localhost@test:label2" ]
        client = conaryclient.ConaryClient(self.cfg)
        searchSource = client.getSearchSource()
        for source in searchSource.iterSources():
            res = source.findTroves([ (None, None, None) ],
                    allowMissing = True, acrossLabels = True)
            self.assertEqual([ (k, sorted([ (j[0], str(j[1]), str(j[2])) for j in v]))
                                    for k, v in res.items()],
                [((None, None, None), [
                    ('foo:runtime', '/localhost@test:label1/1-1-1', ''),
                    ('foo:runtime', '/localhost@test:label2/1-1-1', ''),
                    ])])

        # Similar deal, using getTroveLatestByLabel directly
        troveSpecs = [
            (None, versions.Label(self.cfg.searchPath[0]), None),
            (None, versions.Label(self.cfg.searchPath[1]), None),
        ]
        res = repos.getTroveLatestByLabel(troveSpecs, bestFlavor = True,
            troveTypes = 1)
        self.assertEqual([[sorted([ (j[0], str(j[1]), str(j[2])) for j in v ])
                                for v in x ] for x in res ],
            [[
              [('foo:runtime', '/localhost@test:label1/1-1-1', '')],
              [('foo:runtime', '/localhost@test:label2/1-1-1', '')]
             ],
             [[], []],
            ],
        )

    @testhelp.context('rpm')
    def testContentCapsuleAndNormal(self):
        archivePath = resources.get_archive()

        # this commits a capsule with a payload reference to another
        # repository; the stream is None on this repository because
        # of the version number
        fl = [ ('/normal', rephelp.RegularFile(
                                               contents = 'normal\n',
                                               owner = 'root', group = 'root',
                                               version = '/localhost1@foo:bar',
                                               mode = 0644)) ]
        other = self.addComponent("foo:rpm=1.0",
                            fileContents = fl,
                            capsule = archivePath + '/simple-1.0-1.i386.rpm')

        # now commit the same thing, but with the file on this repository.
        # the stream should be filled in on this repository, but the sha1
        # should not be (since the contents for the file are in the capsule,
        # and not stored separately)
        fl = [ ('/normal', rephelp.RegularFile(
                                               contents = 'normal\n',
                                               owner = 'root', group = 'root',
                                               mode = 0644)) ]
        other = self.addComponent("foo:rpm=2.0",
                            fileContents = fl,
                            capsule = archivePath + '/simple-1.0-1.i386.rpm')

        # now commit a normal changeset which needs the sha1 from previously.
        # CNY-3332 happened because the sha1 was stored on the commit
        # previous to this, so the proper contents were not added to the
        # content store during this commit
        fl = [ ('/normal', rephelp.RegularFile(
                                               contents = 'normal\n',
                                               owner = 'root', group = 'root',
                                               mode = 0644)) ]
        other = self.addComponent("foo:runtime=1.0",
                            fileContents = fl)

        # CNY-3332 had this blow up
        self.updatePkg('foo:runtime')

    def testTimestamps(self):
        comp = self.addComponent('foo:runtime=1.0')
        pkg = self.addCollection('foo=1.0', [ ':runtime' ] )
        # this gets rid of the timestamps
        verStr = versions.VersionFromString(str(pkg.getVersion()))
        repos = self.openRepository()

        # get the final packages as committed; the server resets timestamps
        # so the ones from addComponent(), addCollection() aren't correct
        [ comp, pkg ] = repos.getTroves( [ comp.getNameVersionFlavor(),
                                           pkg.getNameVersionFlavor() ] )

        tsList = repos.getTimestamps([
                (comp.getName(), verStr),
                (pkg.getName(), verStr),
                (pkg.getName(),
                 versions.VersionFromString('/localhost@foo:bar/1.0-1-1') ),
            ])

        assert(tsList[0].freeze() == comp.getVersion().freeze())
        assert(tsList[1].freeze() == pkg.getVersion().freeze())
        assert(tsList[2] is None)

    def testEntClassRolesSQLInjection(self):
        """Regression test for a previous privileged SQL injection vector.

        @tests: CNY-3529
        """
        repos = self.openRepository()
        self.assertRaises(errors.RoleNotFound,
                repos.getEntitlementClassesRoles, 'localhost',
                ["'); kaboom!"])
        self.assertRaises(errors.RoleNotFound,
                repos.setEntitlementClassesRoles, 'localhost',
                {"'); kaboom!": ['admin']})

    def testAddAclAdminOnly(self):
        """Regression test for a previous unprivileged permission creation
        vulnerability.

        @tests: CNY-3528
        """
        self.openRepository()
        repos = self.getRepositoryClient(
                user='anonymous', password='anonymous')
        self.assertRaises(errors.InsufficientPermission, repos.addAcl,
                'localhost', 'anonymous', '.*', 'ALL', False, False)

    def testWriteRestrictedMirrorOps(self):
        """Regression test for several mirror operations that require write
        privileges.

        @tests: CNY-3527
        """
        repos = self.openRepository()

        # Create a read-only mirror user.
        self.addUserAndRole(repos, 'localhost', 'mirror', 'mpass')
        repos.addAcl('localhost', 'mirror', None, None, write=False)
        repos.setRoleCanMirror('localhost', 'mirror', True)
        mirror = self.getRepositoryClient(user='mirror', password='mpass')

        self.assertRaises(errors.InsufficientPermission,
                mirror.setMirrorMark, 'localhost', '1234')
        self.assertRaises(errors.InsufficientPermission,
                mirror.presentHiddenTroves, 'localhost')
        self.assertRaises(errors.InsufficientPermission,
                mirror.addPGPKeyList, 'localhost', [])

    def testDeletingUserKeepsKey(self):
        """Deleting a user should not delete the PGP keys they uploaded.

        @tests: CNY-3710
        """
        repos = self.openRepository()
        fingerprint = 'F7440D78FE813C882212C2BF8AC2828190B1E477'
        keyRing = open(resources.get_archive() + '/pubring.gpg')
        keyData = openpgpfile.exportKey(fingerprint, keyRing)
        keyData.seek(0)
        keyData = keyData.read()
        armored = openpgpfile.exportKey(fingerprint, keyRing, armored=True)
        armored.seek(0)
        armored = armored.read()

        repos.addNewPGPKey(self.cfg.buildLabel, 'test', keyData)
        self.assertEquals(repos.getAsciiOpenPGPKey(self.cfg.buildLabel,
            fingerprint), armored)
        repos.deleteUserByName(self.cfg.buildLabel, 'test')
        self.assertEquals(repos.getAsciiOpenPGPKey(self.cfg.buildLabel,
            fingerprint), armored)

    def testMultiVersionChangeset(self):
        """Changeset with multiple versions of a trove"""
        # Chronologically, troves are timestamped in this order:
        # b:x86 a:x86 a:x86_64 b:x86_64
        # The result should be a:* then b:*
        cs = changeset.ReadOnlyChangeSet()
        for v, f, t in [
                ('b', 'is: x86',    1000000000),
                ('a', 'is: x86',    1000000001),
                ('a', 'is: x86_64', 1000000002),
                ('b', 'is: x86_64', 1000000003),
                ]:
            trv, cs1 = self.Component('foo:runtime', v, f)
            trv.version().trailingRevision().timeStamp = t
            cs.merge(cs1)
        repos = self.openRepository()
        repos.commitChangeSet(cs)
        matches = sorted(repos.findTrove(None,
            ('foo:runtime', 'localhost@rpl:linux', None), getLeaves=False))
        VFS = versions.VersionFromString
        Flavor = deps.parseFlavor
        self.assertEqual(matches, [
            ('foo:runtime', VFS('/localhost@rpl:linux/a-1-1'), Flavor('is: x86')),
            ('foo:runtime', VFS('/localhost@rpl:linux/a-1-1'), Flavor('is: x86_64')),
            ('foo:runtime', VFS('/localhost@rpl:linux/b-1-1'), Flavor('is: x86')),
            ('foo:runtime', VFS('/localhost@rpl:linux/b-1-1'), Flavor('is: x86_64')),
            ])
        ts = [x[1].trailingRevision().timeStamp for x in matches]
        # Timestamps must have been reset
        assert ts[0] > 1000000003
        # Timestamps must match between flavors, and be different between versions
        assert ts[0] == ts[1]
        assert ts[0] < ts[2]
        assert ts[2] == ts[3]

    def testResumeOffset(self):
        repos = self.openRepository()
        trv1 = self.addComponent('foo:runtime', '1.0')
        trv2 = self.addComponent('foo:python', '1.0')
        path1 = os.path.join(self.workDir, 'expected1.ccs')
        path2 = os.path.join(self.workDir, 'expected2.ccs')
        job = [trv1.getNameVersionFlavor().asJob(),
                trv2.getNameVersionFlavor().asJob()]
        repos.createChangeSetFile([job[0]], path1)
        repos.createChangeSetFile([job[1]], path2)
        expected1 = open(path1).read()
        expected2 = open(path2).read()
        expected = expected1 + expected2
        size1 = len(expected1)
        total = size1 + len(expected2)

        sp = repos.c[trv1.getVersion().getHost()]
        job = xmlshims.NetworkConvertors().fromJobList(job)
        # This is too slow to run for every possible byte offset and
        # ChangeSetTest.testChangeSetDumpOffset is already doing an exhaustive
        # test, so just test the boundary conditions for the ChangesetProducer
        # part.
        offsets = [0, size1 - 1, size1, size1 + 1, total - 2, total - 1, total]
        for offset in offsets:
            rc = sp.getChangeSet(job, recurse=True, withFiles=True,
                    withFileContents=True, excludeAutoSource=False,
                    resumeOffset=offset)
            actual = rc[0].read()
            self.assertEqual(actual, expected[offset:])
