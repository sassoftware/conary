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
import itertools
import os
import base64
import tempfile

#testsuite
from conary_test import rephelp

#conary
from conary import checkin
from conary import conaryclient
from conary.conaryclient import cmdline
from conary import conarycfg, repository, trove, versions
from conary.deps.deps import parseDep
from conary.deps.deps import parseFlavor
from conary.lib import util
from conary.local import database
from conary.repository import changeset
from conary.versions import VersionFromString


class ConaryClientTest(rephelp.RepositoryHelper):
    def testCommitChangesetByDefaultExcluded(self):
        # this test creates two changesets.  The first includes
        # test=1.0-1-1, test:foo=1.0-1-1 and a reference to 
        # test:bar-1.0-1-1 (but not included).  This gets committed
        # to the database.
        # The next version of the test package no longer has a test:bar
        # component.  A changeset is generated between test=1.0-1-1 and
        # test=1.1-1-1.  The resulting changeset is committed to the
        # database

        cl = conaryclient.ConaryClient(self.cfg)

        versionPrefix = '/' + self.cfg.buildLabel.asString() + '/'
        flavor = parseFlavor('')

        # create an absolute changeset
        cs = changeset.ChangeSet()
        # add a pkg diff
        v1 = VersionFromString(versionPrefix + '1.0-1-1',
                              timeStamps=[1.000])
        old = trove.Trove('test', v1, flavor, None)
        old.addTrove('test:foo', v1, flavor, byDefault=True)
        old.addTrove('test:bar', v1, flavor, byDefault=False)
        old.setIsCollection(True)
        old.computeDigests()

        # add the 'test' package
        diff = old.diff(None)[0]
        cs.newTrove(diff)
        cs.addPrimaryTrove('test', v1, flavor)

        # add the test:foo component
        oldfoo = trove.Trove('test:foo', v1, flavor, None)
        oldfoo.computeDigests()
        diff = oldfoo.diff(None)[0]
        cs.newTrove(diff)

        # note that we do not add a test:bar diff to the
        # changeset.  this is because it's not included by default
        # (byDefault=False)

        # commit the first changeset to the database
        cs.writeToFile(self.workDir + '/first.ccs')
        cs = changeset.ChangeSetFromFile(self.workDir + '/first.ccs')
        cs = cl.updateChangeSet([ ("test", (None, None), (v1, flavor), True) ], 
                                fromChangesets = [cs])[0]
        cl.applyUpdate(cs)

        # create a relative changeset
        cs = changeset.ChangeSet()
        v2 = VersionFromString(versionPrefix + '1.1-1-1',
                              timeStamps=[2.000])
        new = trove.Trove('test', v2, flavor, None)
        new.addTrove('test:foo', v2, flavor, byDefault=True)
        new.setIsCollection(True)
        new.computeDigests()

        # add the new 'test' package
        diff = new.diff(old)[0]
        cs.newTrove(diff)
        cs.addPrimaryTrove('test', v2, flavor)

        # add new test:foo component
        newfoo = trove.Trove('test:foo', v2, flavor, None)
        newfoo.computeDigests()
        diff = newfoo.diff(oldfoo)[0]
        cs.newTrove(diff)

        # mark test:bar as being removed
        cs.oldTrove('test:bar', v1, flavor)
        
        cs.writeToFile(self.workDir + '/second.ccs')
        cs = changeset.ChangeSetFromFile(self.workDir + '/second.ccs')
        cs = cl.updateChangeSet([ ('test', (None, None), (v2, flavor), True) ],
                                fromChangesets = [cs])[0]

        # commit the second changeset to the database
        cl.applyUpdate(cs)
        db = cl.db
        e = [('test', v2, flavor)]
        assert(db.findTrove(None, ('test', '1.1-1-1', None)) == e)
        e = [('test:foo', v2, flavor)]
        assert(db.findTrove(None, ('test:foo', '1.1-1-1', None)) == e)

    def testFullUpdateItemList(self):
        def _check(client, checkDict):
            updList = client.fullUpdateItemList()

            for item in updList:
                (name, branch, flavor) = item

                matches = checkDict[name]
                matched = False
                for i, info in enumerate(matches):
                    if len(info):
                        mBranch = info[0]
                    else:
                        mBranch = None

                    if len(info) > 1:
                        mFlavor = info[1]
                    else:
                        mFlavor = None

                    verMatch = ((not mBranch and not branch) or 
                                (branch.asString() == mBranch))

                    flvMatch = False
                    if flavor is None and mFlavor is None:
                        flvMatch = True
                    elif (flavor is not None) and flavor.satisfies(parseFlavor(mFlavor)):
                        flvMatch = True

                    if flvMatch and verMatch:
                        matched = True
                        del matches[i]
                        break

                assert(matched)

            assert(not sum([ len(x) for x in checkDict.values() ]))

        self.addQuickTestComponent('test1:runtime', '1.0-1-1',
                                   fileContents = [ ('/file1', 'hello') ] )
        self.addQuickTestComponent('test1:runtime', 
                                   '/localhost@rpl:branch/1.0-1-1',
                                   fileContents = [ ('/file2', 'hello') ] )

        self.addQuickTestComponent('test2:runtime', '1.0-1-1', 'flag',
                                   fileContents = [ ('/file3', 'hello') ] )
        self.addQuickTestComponent('test2:runtime', '1.0-1-1', flavor = '!flag',
                                   fileContents = [ ('/file4', 'hello') ] )

        self.addQuickTestComponent('test3:runtime', '1.0-1-1',
                                   fileContents = [ ('/file5', 'hello') ] )
        self.addQuickTestComponent('test3:runtime', 
                                   '/localhost@rpl:branch/1.0-1-1',
                                   flavor = 'flag',
                                   fileContents = [ ('/file6', 'hello') ] )
        self.addQuickTestComponent('test3:runtime', 
                                   '/localhost@rpl:branch/1.0-1-1',
                                   flavor = '!flag',
                                   fileContents = [ ('/file7', 'hello') ] )
        
        self.updatePkg(self.rootDir, 'test1:runtime')
        self.updatePkg(self.rootDir, 'test1:runtime',
                       version = 'localhost@rpl:branch',
                       keepExisting = True)

        self.updatePkg(self.rootDir, 'test2:runtime', flavor = 'flag')
        self.updatePkg(self.rootDir, 'test2:runtime', flavor = '!flag',
                       keepExisting = True)

        self.updatePkg(self.rootDir, 'test3:runtime')
        self.updatePkg(self.rootDir, 'test3:runtime', flavor = 'flag',
                       version = 'localhost@rpl:branch',
                       keepExisting = True)
        self.updatePkg(self.rootDir, 'test3:runtime', flavor = '!flag',
                       version = 'localhost@rpl:branch',
                       keepExisting = True)

        client = conaryclient.ConaryClient(self.cfg)

        _check(client,
                    { "test1:runtime" : [ 
                            ( "/localhost@rpl:branch", ),
                            ( "/localhost@rpl:linux", ),
                      ],
                      "test2:runtime" : [
                            ( None, "flag" ),
                            ( None, "!flag" ),
                      ],
                      "test3:runtime" : [ 
                            ( "/localhost@rpl:branch", "!flag" ),
                            ( "/localhost@rpl:branch",  "flag" ),
                            ( "/localhost@rpl:linux", ),
                      ],
                    }
               )

        # Piggy-back on this test for CNY-1390 (testing iterRollbacksList)
        cl = conaryclient.ConaryClient(self.cfg)
        # Get the last 3 rollbacks
        rblist = itertools.islice(cl.iterRollbacksList(), 3)
        names = [ x[0] for x in rblist ]
        self.assertEqual(names, ['r.6', 'r.5', 'r.4'])

    def testFormerlyOverlapping(self):
        self.addQuickTestComponent("test:runtime", "1.0-1-1")
        self.addQuickTestCollection("group-foo", "1.0-1-1",
                                    [ ("test:runtime", "1.0-1-1") ])
        self.addQuickTestCollection("group-uber", "1.0-1-1",
                                    [ ("group-foo", "1.0-1-1"),
                                      ("test:runtime", "1.0-1-1") ])
        self.updatePkg(self.rootDir, 'group-uber')

        self.addQuickTestComponent("test:runtime", "2.0-1-1")
        self.addQuickTestCollection("group-foo", "2.0-1-1",
                                    [ ("test:runtime", "2.0-1-1") ])
        self.addQuickTestCollection("group-uber", "2.0-1-1",
                                    [ ("group-foo", "2.0-1-1") ])
        client = conaryclient.ConaryClient(self.cfg)
        job = client.updateChangeSet([('group-uber', (None, None),
                                                     (None, None), True)])[0]
        jobs = job.getJobs()
        # this makes sure nothing is being removed
        assert(len([x for x in itertools.chain(*jobs) if x[1][0] is None]) == 0)

    def testBranchedSubcomponent(self):
        def _buildPackage(version, filename):
            self.addQuickTestComponent("test:doc", version,
                                       fileContents =  [ (filename, "foo") ] )
            self.addQuickTestComponent("test:runtime", version)
            self.addQuickTestCollection("test", version,
                                        [ ("test:doc", version),
                                          ("test:runtime", version) ])

        origVersion = "/localhost@rpl:test/1.0-1-1"
        secondVersion = "/localhost@rpl:test/2.0-1-1"
        branchedVersion = "/localhost@rpl:test/1.0-1-1/branch/1.0-1-2"

        _buildPackage(origVersion, "/orig")
        _buildPackage(secondVersion, "second")
        _buildPackage(branchedVersion, "/branched")

        self.updatePkg(self.rootDir, "test", version = origVersion)
        self.updatePkg(self.rootDir, 
                            [ "test=%s" % secondVersion,
                              "test:doc=%s" % branchedVersion ] )

    def testUpdateTroveFromChangeset(self):
        self.addQuickTestComponent("test:runtime", '1.0-1-1')
        self.addQuickTestComponent("test:lib", '1.0-1-1', filePrimer = 1)
        pkg = self.addQuickTestCollection("test", '1.0-1-1',
                                    [ ("test:lib", '1.0-1-1'),
                                      ("test:runtime", '1.0-1-1') ])

        self.addQuickTestComponent("test2:runtime", '1.0-1-1', filePrimer=3)

        self.addQuickTestComponent("test:lib", '2.0-1-1', filePrimer=2)
        repos = self.openRepository()
        (fd, path) = tempfile.mkstemp()
        os.close(fd)
        repos.createChangeSetFile(
            [('test', (None, None), (pkg.getVersion(), pkg.getFlavor()), 
                        True)], path, recurse = True)

        self.updatePkg(self.rootDir, ["test:lib=2.0"])
        self.updatePkg(self.rootDir, ["test:lib=1.0"], fromFiles = [ path ],
                        keepExisting=True)
        self.updatePkg(self.rootDir, ["test:runtime", 'test2:runtime'], 
                        fromFiles = [ path ],
                        keepExisting=True)
        os.unlink(path)

        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(sorted(db.iterAllTroveNames()) == ['test2:runtime', 'test:lib', 
                                                  'test:runtime'])
        assert(len(db.trovesByName('test:lib')) == 2)

    def testUpdateNonRecursedCollectionFromChangeset(self):
        self.addQuickTestComponent("test:runtime", '1.0')
        self.addQuickTestCollection("test", '1.0', [':runtime'])

        repos = self.openRepository()
        path = self.workDir + '/test.ccs'
        self.changeset(repos, ['test'], path, recurse=False)
        cs = changeset.ChangeSetFromFile(path)
        self.checkUpdate(['test'], 
                         ['test', 'test:runtime'], 
                         fromChangesets=[cs])

    def testResolveTroveFromChangeset(self):
        # resolve a needed dependency from a changeset, not from the repository
        # (we can be sure it's using the changeset because the repository 
        #  would return version 2.0, the latest)

        self.addQuickTestComponent("test:runtime", ':branch/1.0-1-1',
                                    requires='trove: test:lib')
        self.addQuickTestComponent("test:lib", ':branch/1.0-1-1', 
                                   filePrimer = 1)
        self.addQuickTestComponent("test:lib", ':branch/2.0-1-1', 
                                   filePrimer = 1)
        pkg = self.addQuickTestCollection("test", ':branch/1.0-1-1',
                                    [ "test:lib", "test:runtime" ])

        repos = self.openRepository()
        (fd, path) = tempfile.mkstemp()
        os.close(fd)
        cs = repos.createChangeSet(
            [('test', (None, None), (pkg.getVersion(), pkg.getFlavor()), 
                        True)], recurse = True)

        # clear out the repository, we shouldn't be searching it
        self.resetRepository()
        self.checkUpdate(["test:runtime=:branch"], 
                        expectedJob=['test', 
                                     'test:runtime', 
                                     'test:lib=1.0'],
                        fromChangesets = [ cs ], resolve=True)
        os.unlink(path)

    def testResolveTroveFromChangeset2(self):
        # resolve a needed dependency from a changeset, not from the repository
        # (we can be sure it's using the changeset because the repository 
        #  would return version 2.0, the latest)  
        # in this example the trove we're installing is in the repos

        self.addQuickTestComponent("test2:runtime", '1.0-1-1',
                                    requires='trove: test:lib')
        self.addQuickTestComponent("test:lib", '1.0-1-1', 
                                   filePrimer = 1)
        self.addQuickTestComponent("test:lib", '2.0-1-1', 
                                   filePrimer = 1)
        pkg = self.addQuickTestCollection("test", '1.0-1-1',
                                    [ "test:lib" ])

        repos = self.openRepository()
        (fd, path) = tempfile.mkstemp()
        os.close(fd)
        cs = repos.createChangeSet(
            [('test', (None, None), (pkg.getVersion(), pkg.getFlavor()), 
                        True)], recurse = True)

        self.checkUpdate(["test2:runtime"], 
                        expectedJob=['test=1.0',
                                     'test2:runtime', 
                                     'test:lib=1.0'],
                        fromChangesets = [ cs ], resolve=True)
        os.unlink(path)

    def testPathConflicts(self):
        self.addQuickTestComponent('test:runtime', '1.0-1-1',
                                   provides = parseDep('file: /bin/foo'))
        self.addQuickTestComponent('test:runtime', '2.0-1-1',
                                   filePrimer = 2)
        self.addQuickTestComponent('req:runtime', '2.0-1-1',
                                   requires = parseDep('file: /bin/foo'),
                                   filePrimer = 3)
        self.addQuickTestCollection("group-a", '1.0-1-1',
                                    [ ("test:runtime", '2.0-1-1'),
                                      ("req:runtime", '2.0-1-1') ])

        self.updatePkg(self.rootDir, 'test:runtime', version = '1.0-1-1')
        self.logCheck(self.updatePkg, [self.rootDir, 'group-a'],
        ['warning: keeping test:runtime - required by at least req:runtime'],
                      kwargs = { 'keepRequired' : True })
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(len(db.trovesByName('test:runtime')) == 2)

        self.resetRoot()
        self.updatePkg(self.rootDir, 'test:runtime', version = '1.0-1-1')
        self.updatePkg(self.rootDir, 'req:runtime', version = '2.0-1-1')
        self.logCheck(self.updatePkg,
                      [self.rootDir, ['test:runtime=2.0-1-1']],
                      ['warning: keeping test:runtime - required by at least'
                       ' req:runtime'], kwargs = { 'keepRequired' : True })
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(len(db.trovesByName('test:runtime')) == 2)

        # an explicit remove shouldn't be overridden by install bucket
        # handling
        self.resetRoot()
        self.updatePkg(self.rootDir, 'test:runtime', version='1.0-1-1')
        self.updatePkg(self.rootDir, 'req:runtime', version='2.0-1-1')
        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       ['-test:runtime=1.0-1-1',
                                        '+test:runtime=2.0-1-1'])
        expectedStr = """\
The following dependencies would not be met after this update:

  req:runtime=2.0-1-1 (Already installed) requires:
    file: /bin/foo
  which is provided by:
    test:runtime=1.0-1-1 (Would be erased)
"""
        assert(str == expectedStr)

        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(len(db.trovesByName('test:runtime')) == 1)

        self.resetRoot()
        self.updatePkg(self.rootDir, 'test:runtime', version = '1.0-1-1')
        self.updatePkg(self.rootDir, 'test:runtime', version = '2.0-1-1')
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(len(db.trovesByName('test:runtime')) == 1)

        self.resetRoot()
        self.updatePkg(self.rootDir, 'test:runtime', version = '1.0-1-1')
        self.pin("test:runtime")
        self.logFilter.add()
        self.updatePkg(self.rootDir, 'test:runtime', version = '2.0-1-1')
        self.logFilter.clear()
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(len(db.trovesByName('test:runtime')) == 2)

        self.addQuickTestCollection("test", '1.0-1-1', ("test:runtime", ))
        self.addQuickTestCollection("test", '2.0-1-1', ("test:runtime", ))

        self.resetRoot()
        self.updatePkg(self.rootDir, 'test', version = '1.0-1-1')
        self.pin("test")
        self.logFilter.add()
        self.updatePkg(self.rootDir, 'test', version = '2.0-1-1')
        self.logFilter.clear()
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(len(db.trovesByName('test')) == 2)
        assert(len(db.trovesByName('test:runtime')) == 2)

        self.resetRoot()
        self.updatePkg(self.rootDir, 'test:runtime', version = '1.0-1-1')
        self.pin("test:runtime")
        self.updatePkg(self.rootDir, 'test', version = '2.0-1-1')
        db = database.Database(self.rootDir, self.cfg.dbPath)
        assert(len(db.trovesByName('test')) == 1)
        assert(len(db.trovesByName('test:runtime')) == 2)

        self.resetRoot()
        self.updatePkg(self.rootDir, 'test', version = '1.0-1-1')
        self.erasePkg(self.rootDir, 'test:runtime')
        self.pin("test")
        self.logFilter.add()
        self.updatePkg(self.rootDir, 'test', version = '2.0-1-1')
        self.logFilter.clear()

    @testhelp.context('splitting')
    def testInfoSplitting(self):
        # info packages must be in their own job, not combined with
        # random other packages, because we expect their tag scripts
        # to have been run before other troves are installed.
        self.addComponent('info-foo:user', '1.0', filePrimer=1)
        self.addComponent('foo:runtime', '1.0', filePrimer=2)
        self.addComponent('info-foo:group', '1.0', filePrimer=3)
        self.addComponent('info-zoo:group', '1.0', filePrimer=4)
        client = conaryclient.ConaryClient(self.cfg)
        items = cmdline.parseChangeList(['info-foo:user', 'foo:runtime',
                                         'info-foo:group', 'info-zoo:group'])
        uJob = client.updateChangeSet(items, split=True)[0]
        # grab the name of each job while keeping the job groups the same
        jobs = [[y[0] for y in x] for x in uJob.getJobs() ]
        assert(set(jobs[0]) == set([ 'info-foo:user', 'info-foo:group' ]))
        assert(jobs[1] == [ 'info-zoo:group' ])
        assert(jobs[2] == [ 'foo:runtime' ])

    def _testPasswordQuery(self, getCallback, userList):
        class CallCount:

            def inc(self, host):
                assert(not self.locked)
                self.counts.setdefault(host, 0)
                self.counts[host] += 1

            def check(self, val):
                assert(self.counts == val)

            def lock(self):
                self.locked = True

            def __init__(self):
                self.counts = {}
                self.locked = False

        def getClient(counter = None, threaded = False):
            newcfg = copy.deepcopy(self.cfg)
            newcfg.user = conarycfg.UserInformation()
            for userInfo in userList:
                newcfg.user.addServerGlob(*userInfo)
            newcfg.threaded = threaded

            cb = getCallback(counter)
            cl = conaryclient.ConaryClient(cfg = newcfg, passwordPrompter = cb)
            return (cl, cl.getRepos())

        repos = self.openRepository()
        repos.deleteUserByName(versions.Label('localhost@foo:bar'), 
                               'anonymous')
        repos1 = self.openRepository(1)
        repos1.deleteUserByName(versions.Label('localhost1@foo:bar'), 
                                'anonymous')

        self.addComponent('test1:runtime', '1.0-1-1')
        self.addCollection("test1", "1.0-1-1", [ 'test1:runtime' ])
        self.addComponent('test2:runtime', '1.0-1-1', filePrimer=2)
        self.addCollection("test2", "1.0-1-1", [ 'test2:runtime' ])

        cl, repos = getClient()
        self.assertRaises(repository.errors.InsufficientPermission, repos.getAllTroveLeaves,
                          'localhost', { None : None } )

        count = CallCount()
        cl, repos = getClient(count)
        repos.getAllTroveLeaves('localhost', { None : None } )
        count.check({ 'localhost' : 1 })

        # make sure the client has access to the password
        count = CallCount()
        cl, repos = getClient(count, threaded = True)
        uJob = cl.updateChangeSet(
                [ ('test1', (None, None), (None, None), True),
                  ('test2', (None, None), (None, None), True) ],
                  split=True)[0]
        count.lock()
        assert(len(uJob.getJobs()) == 1)
        cl.applyUpdate(uJob)

        # if the download thread is building a distributed changeset, it might
        # not have the needed password cached; make sure it knows how to
        # prompt for it. This has the added benfit of making sure that the
        # callback is based on the servername, not the mapped hostname (it
        # does this by making sure a prompt is made for localhost1)
        self.resetRoot()
        self.addComponent('test:runtime', '/localhost1@ns:br/1.0-1-1')
        self.addCollection('test', '/localhost1@ns:br/1.0-1-1',
                           [ 'test:runtime' ])
        self.mkbranch([ 'test=localhost1@ns:br' ], self.defLabel, shadow = True,
                      binaryOnly = True)

        count = CallCount()
        cl, repos = getClient(count, threaded = False)
        uJob = cl.updateChangeSet(
                [ ('test', (None, None), (None, None), True),
                  ('test2', (None, None), (None, None), True) ],
                  split=True)[0]
        count.check({ 'localhost' : 1 })
        cl.applyUpdate(uJob)
        count.check({ 'localhost' : 1 , 'localhost1' : 1 })
        self.resetRoot()
        self.resetRepository()
        self.resetRepository(1)

    def testPasswordQuery(self):

        def callback(host, user, counter):
            assert(user == 'test')
            counter.inc(host)
            return user, 'foo'

        def getCallback(counter):
            if counter:
                return lambda host, user: callback(host, user, counter)
            else:
                return lambda host, user: (None, None)

        self._testPasswordQuery(getCallback, [('*', 'test', None)])

    def testGetOverlappingPaths(self):
        # because they are modifying the same file.
        tst1 = self.addComponent('test:run', '1.0', '',
                                 ['/tmp/foo', '/tmp/fooz', '/tmp/bam'])
        tst2 = self.addComponent('test:run', '2.0', '',
                                 ['/tmp/foo2', '/tmp/fooz2'])
        foo = self.addComponent('foo:run', '1.0', '', ['/tmp/foo', '/tmp/fooz'])
        bam = self.addComponent('bam:run', '1.0', '', ['/tmp/bam'])
        bar = self.addComponent('bar:run', '1.0', '', ['/tmp/bar'])
        baz = self.addComponent('baz:run', '1.0', '', ['/tmp/baz'])
        baz2 = self.addComponent('baz2:run', '1.0', '', ['/tmp/baz'])
        baz3 = self.addComponent('baz3:run', '1.0', '', ['/tmp/baz'])
        baz4 = self.addComponent('baz4:run', '1.0', '', ['/tmp/baz'])

        self.updatePkg('test:run=1.0')

        job = [(tst1.getName(), (tst1.getVersion(), tst1.getFlavor()),
                                (tst2.getVersion(), tst2.getFlavor()), False)]
        for trv in foo, bam, bar, baz, baz2, baz3, baz4:
            job.append((trv.getName(), (None, None),
                        (trv.getVersion(), trv.getFlavor()), False))

        cl = conaryclient.ConaryClient(self.cfg)
        uJob = database.UpdateJob(cl.db)
        repos = self.openRepository()

        cs = repos.createChangeSet(job, withFiles=False)
        uJob.getTroveSource().addChangeSet(cs)

        overlapping = cl._findOverlappingJobs(job, uJob.getTroveSource())
        overlappingNames = sorted([ sorted([y[0] for y in x]) for x in  overlapping])
        assert(overlappingNames == [['bam:run', 'foo:run', 'test:run'], ['baz2:run', 'baz3:run', 'baz4:run', 'baz:run']])

    def testDisconnectRepos(self):
        flv = parseFlavor('is: x86')
        self.addComponent('foo:run', '1', flv, fileContents='foo 1\n')
        client = conaryclient.ConaryClient(self.cfg)

        ver = versions.Version([self.defLabel, versions.Revision('1-1-1')])
        troveSpec = [ ('foo:run', (None, None), (ver, flv), True)]
        cs = client.createChangeSet(troveSpec)

        client.disconnectRepos()
        self.assertRaises(conaryclient.errors.RepositoryError,
            client.createChangeSet, troveSpec)

    def testChangeSetFromFileAPI(self):
        # CNY-1578
        # Test that ChangeSetFromFile is accessible through conaryclient
        self.assertTrue(hasattr(conaryclient, 'ChangeSetFromFile'))

    def testFlavorPreferencesOverride(self):
        # CNY-1710
        self.cfg.flavorPreferences = [parseFlavor('a'), parseFlavor('b')]
        client = conaryclient.ConaryClient(self.cfg)
        self.assertEqual([ str(x) for x in client.repos._flavorPreferences],
            ['a', 'b'])

    def testGetClient(self):
        self.cfg.configLine('[1]')
        self.cfg.contact = 'foo'
        cfg = copy.deepcopy(self.cfg)
        client = conaryclient.getClient(context='1', cfg=cfg)
        assert(client.cfg.context == '1')
        assert(client.cfg.contact == 'foo')
        cfg = copy.deepcopy(self.cfg)
        client = conaryclient.getClient(environ={'CONARY_CONTEXT' : '1'}, 
                                        cfg=cfg)
        assert(client.cfg.context == '1')
        assert(client.cfg.contact == 'foo')
        os.chdir(self.workDir)
        checkin.setContext(self.cfg, '1')
        cfg = copy.deepcopy(self.cfg)
        client = conaryclient.getClient(cfg=cfg)
        assert(not client.cfg.context)
        client = conaryclient.getClient(cfg=cfg, searchCurrentDir=True)

        assert(client.cfg.context == '1')
        assert(client.cfg.contact == 'foo')

    def testGetDatabase(self):
        # CNY-2316
        client = conaryclient.ConaryClient(self.cfg)
        db = client.getDatabase()
        self.assertEqual(db, client.db)

    def testreposOverride(self):
        client = conaryclient.ConaryClient(self.cfg, repos = 1234)
        assert(client.repos == 1234)

    def testHasSystemModel(self):
        modelPath = util.joinPaths(self.cfg.root, self.cfg.modelPath)
        util.removeIfExists(modelPath)
        client = conaryclient.ConaryClient(self.cfg)
        self.assertEquals(client.hasSystemModel(), False)
        self.assertEquals(client.getSystemModel(), None)

        # Create file now
        util.mkdirChain(os.path.dirname(modelPath))
        file(modelPath, "w").write("install group-me\n")
        self.assertEquals(client.hasSystemModel(), True)
        sysmodel = client.getSystemModel()
        self.assertEquals(sysmodel.model.filedata, ["install group-me\n"])
        self.assertEquals(sysmodel.fileFullName, modelPath)
        self.assertEquals(sysmodel.mtime, os.stat(modelPath).st_mtime)

    def testSystemIdScript(self):
        systemId = 'foobar'
        script = os.path.join(self.workDir, 'script.sh')
        fh = open(script, 'w')
        fh.write("""\
#!/bin/bash
echo -n "%s"
exit 0
""" % systemId)
        fh.flush()
        fh.close()
        os.chmod(script, 0755)

        self.cfg.systemIdScript = script
        client = conaryclient.ConaryClient(self.cfg)

        self.assertEquals(base64.b64encode(systemId), client.repos.c.systemId)
