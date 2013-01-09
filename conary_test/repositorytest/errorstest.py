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


from conary_test import rephelp

from conary.deps import deps
from conary.repository import changeset, errors
from conary.repository.netrepos import reposlog
from conary import trove
from conary.versions import VersionFromString as VFS
from conary.versions import ThawVersion

class ErrorOutputTest(rephelp.RepositoryHelper):
    def testIntegrityError(self):
        try:
            raise errors.TroveIntegrityError('foo', VFS('/localhost@rpl:1/1.0-1-1'), deps.parseFlavor('~!foo'))
        except Exception, err:
            assert(str(err) == 'Trove Integrity Error: foo=/localhost@rpl:1/1.0-1-1[~!foo] checksum does not match precalculated value')
        try:
            raise errors.TroveIntegrityError(error='')
        except Exception, err:
            assert(not str(err))

    def testTroveSchemaError(self):
        try:
            raise errors.TroveSchemaError('foo', VFS('/localhost@rpl:1/1.0-1-1'), deps.parseFlavor('~!foo'), 10, 5)
        except Exception, err:
            assert(str(err) == 'Trove Schema Error: attempted to commit foo=/localhost@rpl:1/1.0-1-1[~!foo] with version 10, but repository only supports 5')

    def testChecksumMissingError(self):
        try:
            raise errors.TroveChecksumMissing('foo', VFS('/localhost@rpl:1/1.0-1-1'), deps.parseFlavor('~!foo'))
        except Exception, err:
            assert(str(err) == 'Checksum Missing Error: Trove foo=/localhost@rpl:1/1.0-1-1[~!foo] has no sha1 checksum calculated, so it was rejected.  Please upgrade conary.')

    def testRepositoryMismatch(self):
        try:
            raise errors.RepositoryMismatch('right', 'wrong')
        except Exception, err:
            assert(str(err) ==
                   'Repository name mismatch.  The correct repository name '
                   'is "right", but it was accessed as "wrong".  Check for '
                   'incorrect repositoryMap configuration entries.')

    def testFailedPut(self):
        # CNY-1182
        repos = self.openRepository()
        url = repos.c['localhost'].prepareChangeSet()[0]
        self.assertRaises(errors.CommitError,
                          repos.c['localhost'].commitChangeSet, url)
        log = reposlog.RepositoryCallLogger(self.reposDir + '/repos.log', None,
                                            readOnly = True)
        for lastEntry in log:
            pass
        assert(lastEntry.exceptionStr.endswith(
                    'is not a valid conary changeset.'))

    def testDuplicateCommit(self):
        for flavor in ('', 'is: x86'):
            self.addComponent("test:doc", "1.0-1-1", flavor)
            try:
                self.addComponent("test:doc", "1.0-1-1", flavor)
            except Exception, err:
                assert(isinstance(err, errors.CommitError))
                assert(str(err) ==
                       'version /localhost@rpl:linux/1.0-1-1 '
                       'of test:doc already exists')

    def testServerErrors(self):
        # try to get the repository to raise the errors from this class
        # by doing Bad Things.
        repos = self.openRepository()

        # add a pkg diff

        t = trove.Trove('foo', ThawVersion('/localhost@rpl:1/1.0:1.0-1-1'), 
                         deps.parseFlavor('~!foo'), None)

        # create an absolute changeset
        cs = changeset.ChangeSet()
        cs.newTrove(t.diff(None)[0])
        try:
            repos.commitChangeSet(cs)
        except errors.TroveChecksumMissing, err:
             assert(str(err) == 'Checksum Missing Error: Trove foo=/localhost@rpl:1/1.0-1-1[~!foo] has no sha1 checksum calculated, so it was rejected.  Please upgrade conary.')
        else:
            assert(0)

        t.computeDigests() # should be renamed computeChecksum
        t.setSize(1) # now modify the trove after computing the sums
        cs = changeset.ChangeSet()
        cs.newTrove(t.diff(None)[0])
        try:
            repos.commitChangeSet(cs)
        except errors.TroveIntegrityError, err:
            assert(str(err) == 'Trove Integrity Error: foo=/localhost@rpl:1/1.0-1-1[~!foo] checksum does not match precalculated value')
        else:
            assert(0)

        t.troveInfo.troveVersion.set(100000)
        t.computeDigests()
        cs = changeset.ChangeSet()
        cs.newTrove(t.diff(None)[0])
        try:
            repos.commitChangeSet(cs)
        except errors.TroveSchemaError, err:
            assert(str(err) == 'Trove Schema Error: attempted to commit foo=/localhost@rpl:1/1.0-1-1[~!foo] with version 100000, but repository only supports %s' % trove.TROVE_VERSION)
        else:
            assert(0)

        t.troveInfo.troveVersion.set(trove.TROVE_VERSION)
        t.computeDigests()
        cs = changeset.ChangeSet()
        cs.newTrove(t.diff(None)[0])
        # let's make sure that there are no other problems with this
        # changeset
        repos.commitChangeSet(cs)

        # access the server with a bad name
        repos.c.map['badserver'] = repos.c.map.values()[0]
        try:
            repos.createChangeSet([('foo', (None, None),
                                    (VFS('/badserver@rpl:devel/1.0-1-1'), deps.parseFlavor('')), 0 )] )
        except:
            pass
        # FIXME: Proxies report error differently from direct connections to 
        # the server, and it's hard to fix :-(
        #except errors.RepositoryMismatch, err:
        #    assert(str(err) ==
        #           'Repository name mismatch.  The correct repository name '
        #           'is "localhost", but it was accessed as "badserver".  Check for '
        #           'incorrect repositoryMap configuration entries.')
