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


from testrunner import testhelp
import os
import shutil
import sys
import tempfile

from conary_test import recipes
from conary_test import rephelp

from conary import conaryclient, errors, trove
from conary.conaryclient import cml, systemmodel, update
from conary.cmds import rollbacks
from conary.cmds import updatecmd
from conary.build import use
from conary.lib import util
from conary.local import database

class RollbackTest(rephelp.RepositoryHelper):

    @testhelp.context('rollback')
    def testRollbackPerms(self):
        rbPath = self.rootDir + self.cfg.dbPath + '/rollbacks'

        self.addComponent('foo:runtime', '1.0')
        self.updatePkg('foo:runtime')
        assert(os.stat(rbPath).st_mode
                    & 0777 == 0700)
        assert(os.stat(rbPath + '/status').st_mode
                    & 0777 == 0600)
        assert(os.stat(rbPath + '/0').st_mode
                    & 0777 == 0700)
        assert(os.stat(rbPath + '/0/count').st_mode
                    & 0777 == 0600)
        assert(os.stat(rbPath + '/0/local.0').st_mode
                    & 0777 == 0600)
        assert(os.stat(rbPath + '/0/repos.0').st_mode
                    & 0777 == 0600)

        # ensure we can open the database even though we can't read the
        # rollback status
        try:
            os.chmod(rbPath + '/status', 000)
            db = database.Database(self.rootDir, self.cfg.dbPath)
            # And make sure we get an exception when trying to read the
            # rollback
            try:
                db.getRollbackStack().getList()
                self.fail("ConaryError not raised")
            except errors.ConaryError, e:
                self.assertEqual(str(e),
                    "Unable to open rollback directory")
            except:
                self.fail("Wrong exception raised: %s" % sys.exc_info()[1])

            # Same deal with hasRollback() since it's a different code path
            # than getList()

            try:
                db.rollbackStack.hasRollback('r.1')
                self.fail("ConaryError not raised")
            except errors.ConaryError, e:
                self.assertEqual(str(e),
                    "Unable to open rollback directory")
            except:
                self.fail("Wrong exception raised: %s" % sys.exc_info()[1])

        finally:
            os.chmod(rbPath + '/status', 600)

    @testhelp.context('rollback')
    def testConflictOnRollback(self):
        # test that a applying a rollback when there is a conflict in the
        # repository part errors correctly when --replace-files is not
        # used, and succeeds when --replace-files is used
        (built, d) = self.buildRecipe(recipes.testRecipe1, 'TestRecipe1')
        version = built[0][1]

        self.updatePkg(self.rootDir, 'testcase', version)

        path = os.path.join(self.rootDir, 'usr/share/changed')
        assert(os.path.exists(path))
        self.writeFile(path, 'new text')
        self.erasePkg(self.rootDir, 'testcase', version)
        os.mkdir(os.path.join(self.rootDir, 'etc'))
        conflictPath = os.path.join(self.rootDir, 'etc/changedconfig')
        self.writeFile(conflictPath, 'conflict')
        if use.Arch.x86:
            flavor = 'is: x86'
        elif use.Arch.x86_64:
            flavor = 'is: x86_64'
        else:
            raise NotImplementedError, 'edit test for this arch'
        errstr = ('rollback r.1 cannot be applied:\n'
                  'applying update would cause errors:\n'
                  '%s is in the way of a newly created file in '
                  'testcase:runtime=/localhost@rpl:linux/1.0-1-1[%s]'
            % (conflictPath, flavor))
        self.logFilter.add()
        try:
            self.rollback(self.rootDir, 1)
        except database.RollbackError, e:
            self.assertEqual(str(e), errstr)
            self.logFilter.compare('error: ' + errstr)
        self.logFilter.clear()
        self.logCheck(self.rollback, (self.rootDir, 1, True), ())
        assert(open(path).read() == 'new text')

    @testhelp.context('rollback')
    def testReturnOnFailureInterface(self):
        client = conaryclient.ConaryClient(self.cfg)
        repos = client.getRepos()

        self.addComponent('foo:runtime', '1.0-1-1')
        self.updatePkg(self.rootDir, [ 'foo:runtime' ])

        self.logFilter.add()
        ret = rollbacks.applyRollback(client, 'r.2', returnOnError = True)
        self.assertEqual(ret, 1)
        self.logFilter.compare("error: rollback 'r.2' not present")
        self.logFilter.add()
        try:
            rollbacks.applyRollback(client, 'r.2')
        except database.RollbackDoesNotExist, e:
            self.assertEqual(str(e), 'rollback r.2 does not exist')
        else:
            self.fail("Should have raised exception")
        self.logFilter.compare("error: rollback 'r.2' not present")

        self.logFilter.add()
        ret = rollbacks.applyRollback(client, 'r.r', returnOnError = True)
        self.assertEqual(ret, 1)
        self.logFilter.compare("error: rollback 'r.r' not present")
        self.logFilter.add()
        try:
            rollbacks.applyRollback(client, 'r.r')
        except database.RollbackDoesNotExist, e:
            self.assertEqual(str(e), 'rollback r.r does not exist')
        else:
            self.fail("Should have raised exception")
        self.logFilter.compare("error: rollback 'r.r' not present")

        self.logFilter.add()
        ret = rollbacks.applyRollback(client, 'abc', returnOnError = True)
        self.assertEqual(ret, 1)
        self.logFilter.compare("error: integer rollback count expected instead of 'abc'")
        self.logFilter.add()
        try:
            rollbacks.applyRollback(client, 'abc')
        except database.RollbackDoesNotExist, e:
            self.assertEqual(str(e), 'rollback abc does not exist')
        else:
            self.fail("Should have raised exception")
        self.logFilter.compare("error: integer rollback count expected instead of 'abc'")

        self.logFilter.add()
        ret = rollbacks.applyRollback(client, '-1', returnOnError = True)
        self.assertEqual(ret, 1)
        self.logFilter.compare("error: rollback count must be positive")
        self.logFilter.add()
        try:
            rollbacks.applyRollback(client, '-1')
        except database.RollbackDoesNotExist, e:
            self.assertEqual(str(e), 'rollback -1 does not exist')
        else:
            self.fail("Should have raised exception")
        self.logFilter.compare("error: rollback count must be positive")

        self.logFilter.add()
        ret = rollbacks.applyRollback(client, '2', returnOnError = True)
        self.assertEqual(ret, 1)
        self.logFilter.compare("error: rollback count higher then number of rollbacks available")
        self.logFilter.add()
        try:
            rollbacks.applyRollback(client, '2')
        except database.RollbackDoesNotExist, e:
            self.assertEqual(str(e), 'rollback 2 does not exist')
        else:
            self.fail("Should have raised exception")
        self.logFilter.compare("error: rollback count higher then number of rollbacks available")
        client.close()


    @testhelp.context('rollback')
    def testFailedUpdateRollback(self):
        self.addComponent('foo:runtime', '1.0-1-1')
        self.addComponent('bar:runtime', '1.0-1-1', '', ('contents0', 'bam!'))
        self.updatePkg(self.rootDir, [ 'foo:runtime' ])
        rblist = self.rollbackList(self.rootDir)
        # bar:runtime will have a file conflict with foo:runtime
        self.logCheck(self.updatePkg, (self.rootDir, ['bar:runtime']),
            ('error: changeset cannot be applied:\n'
             'applying update would cause errors:\n'
             '%s/contents0 conflicts with a file '
                    'owned by foo:runtime=/localhost@rpl:linux/1.0-1-1[]')
            % self.rootDir)
        newrblist = self.rollbackList(self.rootDir)
        assert(rblist == newrblist)
        rbdir = util.joinPaths(self.rootDir, self.cfg.dbPath, 'rollbacks')
        l = os.listdir(rbdir)
        # make sure that the rollback got cleaned up
        assert(sorted(l) == ['0', 'status'])

    @testhelp.context('rollback')
    def testIncompleteRollbacks(self):
        repos = self.openRepository()
        self.addComponent('foo:runtime', '1.0')
        self.addComponent('foo:runtime', '2.0')

        OLD_TROVE_VERSION = trove.TROVE_VERSION
        trove.TROVE_VERSION = 1

        self.logFilter.add()

        self.updatePkg('foo:runtime=1.0')
        self.updatePkg('foo:runtime=2.0')
        client = conaryclient.ConaryClient(self.cfg)
        client.applyRollback("r.1", replaceFiles=True)

        trove.TROVE_VERSION = OLD_TROVE_VERSION

        self.logFilter.remove()
        client.close()

    @testhelp.context('rollback', 'fileoverlap')
    def testReplacedFileRollbacks(self):
        # same component
        self.addComponent('foo:runtime', '1.0',
                          fileContents = [ ('/foo', 'foo1.0') ])
        self.addComponent('foo:runtime', '2.0',
                          fileContents = [ ('/foo', 'foo2.0') ])

        self.updatePkg('foo:runtime=1.0')
        self.updatePkg('foo:runtime=2.0', keepExisting = True, 
                       replaceFiles = True)
        self.verifyFile(util.joinPaths(self.rootDir, '/foo'), 'foo2.0')
        self.rollback(self.rootDir, 1)
        self.verifyFile(util.joinPaths(self.rootDir, '/foo'), 'foo1.0')

        # different component
        self.addComponent('bar:runtime', '1.0',
                          fileContents = [ ('/foo', 'bar1.0') ])
        self.updatePkg('bar:runtime=1.0', keepExisting = True, 
                       replaceFiles = True)
        self.verifyFile(util.joinPaths(self.rootDir, '/foo'), 'bar1.0')
        self.rollback(self.rootDir, 1)
        self.verifyFile(util.joinPaths(self.rootDir, '/foo'), 'foo1.0')

        # now a different component, but agree on the file. we don't need
        # replaceFiles anymore
        self.addComponent('fooish:runtime', '1.0',
                          fileContents = [ ('/foo', 'foo1.0') ])
        self.updatePkg('fooish:runtime=1.0', keepExisting = True)
        self.verifyFile(util.joinPaths(self.rootDir, '/foo'), 'foo1.0')
        self.rollback(self.rootDir, 1)
        self.verifyFile(util.joinPaths(self.rootDir, '/foo'), 'foo1.0')

    @testhelp.context('rollback', 'fileoverlap')
    def testConfigAndRemoveRollback(self):
        self.addComponent('foo:runtime', '1.0',
                          fileContents = [ ('/foo', 'foo1.0'),
                                           ('/etc/foo', 'foo1.0\n') ])
        self.addComponent('foo:runtime', '1.1',
                          fileContents = [ ('/foo', 'foo1.0'),
                                           ('/etc/foo', 'foo2.1\n') ])
        self.addComponent('bar:runtime', '1.0',
                          fileContents = [ ('/foo', 'bar1.0') ])

        self.updatePkg('foo:runtime=1.0')
        self.verifyFile(util.joinPaths(self.rootDir, '/foo'), 'foo1.0')
        self.writeFile(util.joinPaths(self.rootDir, "/etc/foo"),
                       "hmm\nfoo1.0\n")
        self.updatePkg([ 'foo:runtime', 'bar:runtime' ], replaceFiles = True)

        self.rollback(self.rootDir, 1)
        self.verifyFile(util.joinPaths(self.rootDir, "/etc/foo"),
                        "hmm\nfoo1.0\n")
        self.verifyFile(util.joinPaths(self.rootDir, '/foo'), 'foo1.0')

    @testhelp.context('rollback', 'localchangeset', 'bydefault')
    def testRollbackLocalChangeWithByDefaultFalse(self):
        recipe1 = """
class testRecipe(PackageRecipe):
    name = "kid"
    version = "2"
    clearBuildReqs()

    def setup(self):
        self.Create("/bin/ls", mode=0755)
        self.Create("%(debugsrcdir)s/%(name)s-%(version)s/foo")
"""


        self.addComponent('kid:runtime', '1')
        self.addComponent('kid:debuginfo', '1')
        trv = self.addCollection('kid', '1', [ (':runtime', True),
                                               (':debuginfo', False)])
        self.updatePkg('kid')

        os.chdir(self.workDir)
        self.writeFile('kid.recipe', recipe1)
        self.discardOutput(self.cookItem, self.openRepository(), self.cfg, 
                           'kid.recipe')
        self.updatePkg('kid-2.ccs')
        self.rollback(self.rootDir, 1) # this used to cause a trove integrity 
                                       # error because teh byDefault setting
                                       # for all components was being set to
                                       # true.
        db = self.openDatabase()
        trv = db.getTrove(*trv.getNameVersionFlavor())
        assert(not trv.includeTroveByDefault('kid:debuginfo', trv.getVersion(),
                                              trv.getFlavor()))


    @testhelp.context('rollback', 'localchangeset')
    def testRollbackChangeThatUserRemoved(self):
        # Flexes conary bug CNY-604
        self.addComponent('foo:run', '1', '', 
                         [('/data/foo', 'contents1\n'),
                          ('/usr/foo', 'contents1\n')])
        trv, cs = self.Component('foo:run', '/local@local:COOK/2.0-1-1', '', 
                                 [('/data/foo', 'contents2\n'),
                                  ('/usr/foo', 'contents2\n')])
        cs.writeToFile(self.workDir + '/foo.ccs')

        self.updatePkg('foo:run')
        shutil.rmtree(self.rootDir + '/data')

        self.logFilter.add()
        self.updatePkg(self.rootDir, self.workDir + '/foo.ccs')
        self.logFilter.compare('warning: /data/foo is missing (use remove if this is intentional)')
        self.rollback(self.rootDir, 1)
        assert(not os.path.exists(self.rootDir + '/data/foo'))

    @testhelp.context('rollback')
    def testRollbackFromLocalOnly(self):
        # Test rolling back from something which is not in the repository,
        # but which conary thinks ought to be. This really can't happen...
        csRuntime = self.workDir + "/foo:runtime-1.1.ccs"
        csPackage = self.workDir + "/foo-1.1.ccs"
        self.addComponent('normal:runtime', '1.0', filePrimer = 1)

        self.addComponent('foo:runtime', '1.0')
        self.addCollection('foo', '1.0', [ ':runtime' ])
        self.updatePkg([ 'foo', 'normal:runtime' ])

        self.addComponent('normal:runtime', '1.1', filePrimer = 1)
        self.addComponent('foo:runtime', '1.1', changeSetFile = csRuntime)
        self.addCollection('foo', '1.1', [ ':runtime' ],
                           changeSetFile = csPackage)
        self.updatePkg(self.rootDir, [ csRuntime, csPackage, 'normal:runtime' ])
        self.rollback(self.rootDir, 1)

        db = database.Database(self.rootDir, self.cfg.dbPath)
        s = set( (x[0], x[1].trailingRevision().getVersion())
                    for x in db.iterAllTroves() )
        assert(s == set([ ('foo:runtime', '1.0'), ('foo', '1.0'),
                          ('normal:runtime', '1.0') ]))

    @testhelp.context('rollback')
    def testJustDb(self):
        self.addComponent('foo:runtime', '1.0',
                          fileContents = [ ('/foo', 'first') ] )
        self.updatePkg('foo:runtime')
        assert(self.rollbackCount() == 0)
        self.rollback(self.rootDir, 0, justDatabase = True)
        self.verifyFile(self.rootDir + "/foo", "first")
        assert(self.rollbackCount() == -1)

    @testhelp.context('rollback')
    def testLocalRollback(self):
        self.addComponent('foo:runtime', '1.0',
                          fileContents = [ ('/foo', 'first'),
                                           ('/etc/foo', 'foo') ] )
        self.updatePkg('foo:runtime')
        self.addComponent('foo:runtime', '1.1',
                          fileContents = [ ('/foo', 'second') ] )
        self.writeFile(self.rootDir + "/foo", "new text")

        self.cfg.localRollbacks = True
        try:
            self.updatePkg('foo:runtime', replaceFiles = True)
            self.stopRepository(0)
            self.rollback(self.rootDir, 1)
            self.verifyFile(self.rootDir + "/foo", "new text")
            self.erasePkg(self.rootDir, 'foo:runtime')
            self.stopRepository(0)
            self.rollback(self.rootDir, 1)
            self.verifyFile(self.rootDir + "/foo", "new text")
        finally:
            self.cfg.localRollbacks = False

    @testhelp.context('rollback')
    def testRollbackInvalidation(self):
        self.addComponent('foo:runtime', '1')
        self.addComponent('foo:runtime', '2')
        self.addComponent('foo:runtime', '3')
        self.addComponent('foo:runtime', '4')
        self.addComponent('foo:runtime', '5')

        client = conaryclient.ConaryClient(self.cfg)
        db = client.getDatabase()
        self.assertEqual(db.getRollbackStack().getList(), [])

        # Invalidating the rollback stack should be a noop now
        db.rollbackStack.invalidate()
        self.assertEqual(db.getRollbackStack().getList(), [])

        self.updatePkg('foo:runtime=1')
        db.rollbackStack._readStatus()
        self.assertEqual(db.getRollbackStack().getList(), ['r.0'])

        db.rollbackStack.invalidate()
        self.assertEqual(db.getRollbackStack().getList(), [])

        # Test some of the API functions
        self.assertFalse(db.rollbackStack.hasRollback('r.0'))

        self.updatePkg('foo:runtime=2')
        self.updatePkg('foo:runtime=3')
        db.rollbackStack._readStatus()
        self.assertEqual(db.getRollbackStack().getList(), ['r.1', 'r.2'])

        # Test some of the API functions
        self.assertFalse(db.rollbackStack.hasRollback('r.0'))

        self.assertRaises(database.RollbackDoesNotExist,
            db.applyRollbackList, client.repos, ['r.0'],
                transactionCounter = db.getTransactionCounter())
        self.assertRaises(database.RollbackDoesNotExist,
            db.applyRollbackList, client.repos, ['r.2', 'r.1', 'r.0'],
                transactionCounter = db.getTransactionCounter())
        self.assertRaises(database.RollbackOrderError,
            db.applyRollbackList, client.repos, ['r.1', 'r.0'],
                transactionCounter = db.getTransactionCounter())

        # Successful rollback
        db.applyRollbackList(client.repos, ['r.2', 'r.1'],
            transactionCounter = db.getTransactionCounter())
        db.rollbackStack._readStatus()
        self.assertFalse(db.rollbackStack.hasRollback('r.0'))

        # Reinstall, invalidate, install some more just to make sure the
        # counters are still good
        self.updatePkg('foo:runtime=2')
        self.updatePkg('foo:runtime=3')
        db.rollbackStack._readStatus()
        self.assertEqual(db.getRollbackStack().getList(), ['r.1', 'r.2'])

        db.rollbackStack.invalidate()
        self.updatePkg('foo:runtime=4')
        self.updatePkg('foo:runtime=5')
        db.rollbackStack._readStatus()
        self.assertEqual(db.getRollbackStack().getList(), ['r.3', 'r.4'])
        for i in range(3):
            self.assertFalse(db.rollbackStack.hasRollback('r.%s' % i))

    @testhelp.context('rollback')
    def testRollbackOutput(self):
        # Verify that what we print is what we expect

        name1 = 'coolpackage'
        name2 = 'extrapackage'
        name3 = 'poorpackage'
        name4 = 'specialpackage'

        names = [name1, name2, name3, name4]
        nameslen = len(names)

        # Carefully construct the content to make it mergeable
        content = "content is %03d\n"
        content += 30 * 'abc\n'
        for v in range(2):
            sv = str(v + 1)

            for j, name in enumerate(names):
                primeval = v * nameslen + j
                fcontent = content % primeval + 'Ignore\n'
                self.addComponent('%s:runtime' % name, sv, 
                     [('/etc/%s/foo' % name, fcontent),
                      ('/usr/%s/foo' % name, fcontent), ])
                self.addCollection(name, sv, [':runtime'])

        # Install name2, name4
        pset = [ "%s=1" % x for x in [ name2, name4 ] ]
        self.updatePkg(pset)

        # Install name1, upgrade name2
        pset = [ "%s=1" % name1 ]
        pset.append("%s=2" % name2)
        self.updatePkg(pset)

        # Upgrade name1, upgrade name4
        pset = [ "%s=2" % name1 ]
        pset.append("%s=2" % name4)
        self.updatePkg(pset)

        # Install name3, erase name1, downgrade name2
        pset = [ "%s=1" % name3 ]
        pset.append("-%s=2" % name1)
        pset.append("%s=1" % name2)
        self.updatePkg(pset)

        # Modify one of the files from name3 (drop last line)
        fname = os.path.join(self.rootDir, "etc", name3, "foo")
        open(fname, "w+").write(content % (0 * nameslen + 2))

        # Install name1, erase name2, downgrade name4, upgrade name3
        pset = [ "%s=1" % name1 ]
        pset.append("-%s=1" % name2)
        pset.append("%s=1" % name4)
        pset.append("%s=2" % name3)
        self.updatePkg(pset)

        # Erase name3
        pset = [ "-%s=2" % name3 ]
        self.updatePkg(pset)

        rblist = self.rollbackList(self.rootDir)
        self.assertEqual(rblist, expected_testRollbackOutput1)

        # Roll back, see if the output still matches
        self.rollback(self.rootDir, 4)

        rblist = self.rollbackList(self.rootDir)
        self.assertEqual(rblist, expected_testRollbackOutput2)

        oldval = self.cfg.showLabels
        self.cfg.showLabels = True
        try:
            rblist = self.rollbackList(self.rootDir)
        finally:
            self.cfg.showLabels = oldval
        self.assertEqual(rblist, expected_testRollbackOutputShowLabels)

        oldvval = self.cfg.fullVersions
        self.cfg.fullVersions = True
        try:
            rblist = self.rollbackList(self.rootDir)
        finally:
            self.cfg.fullVersions = oldvval
        self.assertEqual(rblist, expected_testRollbackOutputFullVersions)

        oldfval = self.cfg.fullFlavors
        self.cfg.fullFlavors = True
        self.cfg.fullVersions = True
        try:
            rblist = self.rollbackList(self.rootDir)
        finally:
            self.cfg.fullFlavors = oldfval
            self.cfg.fullVersions = oldvval
        self.assertEqual(rblist, expected_testRollbackOutputFullVersionsFlavors)

    @testhelp.context('trovescripts', 'rollback')
    def testRollbackInvalidationStatus(self):
        # this tests, among other things, that rollbacks don't invalidate
        # rollbacks(!) CNY-1587
        util.mkdirChain(self.rootDir + '/tmp')
        self.addComponent('foo:runtime', '1.0')

        self.addCollection('group-foo', '1.0', [ ('foo:runtime', '1.0' ) ],
                           compatClass = 1)
        self.addCollection('group-foo', '2.0', [ ('foo:runtime', '1.0' ) ],
                           compatClass = 2)
        self.addCollection('group-foo', '3.0', [ ('foo:runtime', '1.0' ) ],
                           compatClass = 3,
                           postRollbackScript =
                                rephelp.RollbackScript(script = 'postrollback',
                                                       conversions = [ 2 ] ) )
        self.updatePkg('group-foo=1.0')
        self.updatePkg('group-foo=2.0')
        self.updatePkg('group-foo=3.0')
        self.rollback(self.rootDir, 2, tagScript = self.workDir + '/script')
        self.updatePkg('group-foo=3.0')
        self.rollback(self.rootDir, 2, tagScript = self.workDir + '/script')
        rblist = self.rollbackList(self.rootDir)
        assert(not rblist)

    @testhelp.context('rollback')
    def testLocalRollbackWithWeak(self):
        # CNY-1590
        self.addComponent('strong:run', '1.0')
        self.addComponent('weak:run', '1.0', filePrimer = 1)
        self.addComponent('strong:run', '2.0')
        self.addComponent('weak:run', '2.0', filePrimer = 1)
        self.addCollection('group-foo', '1.0', [ 'strong:run' ],
                           weakRefList = [ 'weak:run' ])
        self.addCollection('group-foo', '2.0', [ 'strong:run' ],
                           weakRefList = [ 'weak:run' ])

        self.cfg.localRollbacks = True
        self.updatePkg('group-foo=1.0')
        self.updatePkg('group-foo=2.0')
        self.rollback(self.rootDir, 0)

    @testhelp.context('rollback')
    def testLocalRollbackWithWeakByDefaultChanges(self):
        # CNY-1796
        self.addComponent('weak:run', '1.0')
        self.addComponent('strong:run', '1.0')
        self.addComponent('strong:run', '2.0')
        self.addCollection('group-foo', '1.0', [ 'strong:run' ],
                           weakRefList = [ ('weak:run', '1.0', None, True) ])
        self.addCollection('group-foo', '2.0', [ 'strong:run' ],
                           weakRefList = [ ('weak:run', '1.0', None, False) ])

        self.cfg.localRollbacks = True
        self.updatePkg('group-foo=1.0')
        self.updatePkg('group-foo=2.0')
        self.rollback(self.rootDir, 0)

    @testhelp.context('rollback')
    def testLocalRollbackWithLocalChanges(self):
        # CNY-1444 -- /dir1 wouldn't get created on rollback
        self.addComponent('foo:run', '1.0',
                          fileContents = [ ('/dir1/file1', 'contents1' ) ] )
        self.addComponent('foo:run', '2.0',
                          fileContents = [ ('/dir2/file2', 'contents2' ) ] )

        self.cfg.localRollbacks = True
        self.updatePkg('foo:run=1.0')
        os.chmod(self.rootDir + '/dir1/file1', 0100)
        self.updatePkg('foo:run=2.0')
        self.rollback(1)
        assert(os.stat(self.rootDir + '/dir1/file1').st_mode & 0777 == 0100)
        os.chmod(self.rootDir + '/dir1/file1', 0400)
        self.verifyFile(self.rootDir + '/dir1/file1', 'contents1')

    @testhelp.context('rollback')
    def testRollbackTransactionCounter(self):
        # CNY-1624
        client = conaryclient.ConaryClient(self.cfg)
        # Make sure we get an assertion error if not passing the transaction
        # counter
        self.assertRaises(AssertionError, client.db.applyRollbackList,
            'junk', 'junk')
        # Bogus transaction counter
        try:
            client.db.applyRollbackList('junk', 'junk',
                transactionCounter = 1000)
        except database.RollbackError, e:
            self.assertEqual(str(e), 'rollback junk cannot be applied:\n'
                'Database state has changed, please run the rollback '
                'command again')
        else:
            self.fail("RollbackError not raised")

    @testhelp.context('rollback')
    def testRollbackRemoval(self):
        # CNY-2061
        def _setup():
            self.resetRoot()
            self.updatePkg('foo:runtime=1')
            self.updatePkg('foo:runtime=2')
            self.updatePkg('foo:runtime=3')

            client = conaryclient.ConaryClient(self.cfg)
            return client.getDatabase()

        self.addComponent('foo:runtime', '1')
        self.addComponent('foo:runtime', '2')
        self.addComponent('foo:runtime', '3')

        db = _setup()
        rbStack = db.getRollbackStack()

        assert(rbStack.getList() == [ 'r.0', 'r.1', 'r.2' ] )
        assert(os.path.exists('%s/0' % rbStack.dir))
        assert(os.path.exists('%s/1' % rbStack.dir))
        assert(os.path.exists('%s/2' % rbStack.dir))

        # can't remove from the middle
        self.assertRaises(AssertionError, rbStack.remove, 'r.1')
        rbStack.removeFirst()
        assert(rbStack.getList() == [ 'r.1', 'r.2' ] )
        assert(not rbStack.hasRollback('r.0'))
        assert(not os.path.exists('%s/0' % rbStack.dir))

        rbStack.removeLast()
        assert(rbStack.getList() == [ 'r.1' ] )
        assert(not os.path.exists('%s/2' % rbStack.dir))

        rbStack.removeFirst()
        assert(rbStack.getList() == [ ] )
        assert(os.listdir(rbStack.dir) == [ 'status' ])

        db = _setup()
        rollbacks.removeRollbacks(db, '3')
        assert(db.getRollbackStack().getList() == [ ] )

        db = _setup()
        rollbacks.removeRollbacks(db, '2')
        assert(db.getRollbackStack().getList() == [ 'r.2' ] )

        db = _setup()
        rollbacks.removeRollbacks(db, 'r.1')
        assert(db.getRollbackStack().getList() == [ 'r.2' ] )

        db = _setup()
        self.logCheck(rollbacks.removeRollbacks, (db, 'r.7'),
                      "error: rollback 'r.7' not present")
        self.logCheck(rollbacks.removeRollbacks, (db, 'foo'),
                      "error: integer rollback count expected instead of 'foo'")
        self.logCheck(rollbacks.removeRollbacks, (db, '-1'),
                      "error: rollback count must be positive")
        self.logCheck(rollbacks.removeRollbacks, (db, '4'),
                      "error: rollback count higher then number of rollbacks "
                            "available")

    @testhelp.context('rollback')
    def testLocalRollbackCheck(self):
        self.addComponent('foo:runtime', '1')
        self.addComponent('foo:runtime', '2')

        self.updatePkg('foo:runtime=1')
        self.updatePkg('foo:runtime=2')
        self.erasePkg(self.rootDir, 'foo:runtime')

        self.cfg.localRollbacks = True
        self.updatePkg('foo:runtime=1')
        self.updatePkg('foo:runtime=2')
        self.erasePkg(self.rootDir, 'foo:runtime')

        try:
            client = conaryclient.ConaryClient(self.cfg)
            rbStack = client.getDatabase().getRollbackStack()

            assert(rbStack.getRollback('r.0').isLocal())
            assert(not rbStack.getRollback('r.1').isLocal())
            assert(not rbStack.getRollback('r.2').isLocal())

            # these operations were done with localRollbacks True
            assert(rbStack.getRollback('r.3').isLocal())
            assert(rbStack.getRollback('r.4').isLocal())
            assert(rbStack.getRollback('r.5').isLocal())
        finally:
            self.cfg.localRollbacks = False

    @testhelp.context('rollback')
    def testRollbackChangedConfigFiles(self):
        # CNY-2350
        self.addComponent('foo:run', '1.0',
                          fileContents = ('/etc/foo', 'initial\n') )

        for lrb in (True, False):
            self.resetRoot()
            self.cfg.localRollbacks = lrb
            self.updatePkg('foo:run')
            self.writeFile(self.rootDir + '/etc/foo', 'new contents\n')
            self.erasePkg(self.rootDir, 'foo:run')
            self.rollback(1)
            self.verifyFile(self.rootDir + '/etc/foo', 'new contents\n')

    @testhelp.context('rollback')
    def testLocalRollbackInteractive(self):
        import StringIO
        
        #install foo, bar
        self.addComponent('foo:runtime=1.0', filePrimer=1)
        self.addComponent('bar:runtime=1.0', filePrimer=2)
        self.updatePkg( ['foo:runtime', 'bar:runtime'] )
        #update foo, erase bar
        self.addComponent('foo:runtime=2.0', filePrimer=3)
        self.updatePkg( ['foo:runtime', '-bar:runtime'] )
        self.cfg.interactive = True
        
        oldsysstdin = sys.stdin 
        try:
            stdin = StringIO.StringIO('y')
            sys.stdin = stdin
            (retVal, output) = self.captureOutput(self.rollback, self.rootDir, 1)
            self.assertEqual(output, expected_testRollbackInteractive1)
            (retVal, output) = self.captureOutput(self.rollback, self.rootDir, 0)
            self.assertEqual(output, expected_testRollbackInteractive2)
        finally:
            sys.stdin = oldsysstdin
        return 0
    
    @testhelp.context('rollback')
    def testLocalRollbackInfo(self):
        #install foo, bar
        self.addComponent('foo:runtime=1.0', filePrimer=1)
        self.addComponent('bar:runtime=1.0', filePrimer=2)
        self.updatePkg( ['foo:runtime', 'bar:runtime'] )
        #update foo, erase bar
        self.addComponent('foo:runtime=2.0', filePrimer=3)
        self.updatePkg( ['foo:runtime', '-bar:runtime'] )
        self.cfg.showInfoOnly = True
        
        (retVal, output) = self.captureOutput(self.rollback, self.rootDir, 0, showInfoOnly = True)
        self.assertEqual(output, expected_testRollbackShowInfo)
        return 0

    @testhelp.context('rollback')
    def testRollbackFileTypeChange(self):
        a = self.addComponent('foo:run=1',
              fileContents = [
                  ('/foo', rephelp.RegularFile(contents = 'contents 1',
                                               initialContents = True) ) ] )
        self.addComponent('foo:run=2',
              fileContents = [
                  ('/foo', rephelp.RegularFile(contents = 'contents 1',
                                               initialContents = True,
                                               version = a.getVersion())) ])

        # install version 1 of the trove
        self.updatePkg('foo:run=1')
        # replace the file with a symlink (changing the file type)
        os.unlink(self.rootDir + '/foo')
        os.symlink('bar', self.rootDir + '/foo')
        # install version 2, and then put the symlink back
        self.updatePkg('foo:run=2', replaceFiles = True)
        os.unlink(self.rootDir + '/foo')
        os.symlink('bar', self.rootDir + '/foo')

        # the repository portion of this rollback works fine, because the file
        # hasn't changed so nothing happens. the local portion used to break
        # because the file type changed. since the local rollback wants to
        # make the file the same as what's already on disk it ought to work
        # just fine though (CNY-3340)
        self.rollback(self.rootDir, 1)

expected_testRollbackInteractive1 = """\
The following actions will be performed:
Job 1 of 1
    Install bar:runtime=1.0-1-1
    Update  foo:runtime (2.0-1-1 -> 1.0-1-1)
continue with rollback? [y/N] """

expected_testRollbackInteractive2 = """\
The following actions will be performed:
Job 1 of 1
    Erase   bar:runtime=1.0-1-1
    Erase   foo:runtime=1.0-1-1
continue with rollback? [y/N] """

expected_testRollbackShowInfo = """\
The following actions will be performed:
Job 1 of 2
    Install bar:runtime=1.0-1-1
    Update  foo:runtime (2.0-1-1 -> 1.0-1-1)
Job 2 of 2
    Erase   bar:runtime=1.0-1-1
    Erase   foo:runtime=1.0-1-1
"""

expected_testRollbackOutput1 = """\
r.5:
\t   erased: poorpackage(:runtime) 2-1-1

r.4:
\t   erased: extrapackage(:runtime) 1-1-1
\t  updated: poorpackage(:runtime) 1-1-1 -> 2-1-1
\t  updated: specialpackage(:runtime) 2-1-1 -> 1-1-1
\tinstalled: coolpackage(:runtime) 1-1-1

"""
expected_testRollbackOutput2 = """\
r.3:
\t   erased: coolpackage(:runtime) 2-1-1
\t  updated: extrapackage(:runtime) 2-1-1 -> 1-1-1
\tinstalled: poorpackage(:runtime) 1-1-1

r.2:
\t  updated: coolpackage(:runtime) 1-1-1 -> 2-1-1
\t  updated: specialpackage(:runtime) 1-1-1 -> 2-1-1

r.1:
\t  updated: extrapackage(:runtime) 1-1-1 -> 2-1-1
\tinstalled: coolpackage(:runtime) 1-1-1

r.0:
\tinstalled: extrapackage(:runtime) 1-1-1
\tinstalled: specialpackage(:runtime) 1-1-1

"""

expected_testRollbackOutput1 += expected_testRollbackOutput2

expected_testRollbackOutputShowLabels = """\
r.3:
\t   erased: coolpackage(:runtime) localhost@rpl:linux/2-1-1
\t  updated: extrapackage(:runtime) localhost@rpl:linux/2-1-1 -> localhost@rpl:linux/1-1-1
\tinstalled: poorpackage(:runtime) localhost@rpl:linux/1-1-1

r.2:
\t  updated: coolpackage(:runtime) localhost@rpl:linux/1-1-1 -> localhost@rpl:linux/2-1-1
\t  updated: specialpackage(:runtime) localhost@rpl:linux/1-1-1 -> localhost@rpl:linux/2-1-1

r.1:
\t  updated: extrapackage(:runtime) localhost@rpl:linux/1-1-1 -> localhost@rpl:linux/2-1-1
\tinstalled: coolpackage(:runtime) localhost@rpl:linux/1-1-1

r.0:
\tinstalled: extrapackage(:runtime) localhost@rpl:linux/1-1-1
\tinstalled: specialpackage(:runtime) localhost@rpl:linux/1-1-1

"""

expected_testRollbackOutputFullVersions = """\
r.3:
\t   erased: coolpackage(:runtime) /localhost@rpl:linux/2-1-1
\t  updated: extrapackage(:runtime) /localhost@rpl:linux/2-1-1 -> /localhost@rpl:linux/1-1-1
\tinstalled: poorpackage(:runtime) /localhost@rpl:linux/1-1-1

r.2:
\t  updated: coolpackage(:runtime) /localhost@rpl:linux/1-1-1 -> /localhost@rpl:linux/2-1-1
\t  updated: specialpackage(:runtime) /localhost@rpl:linux/1-1-1 -> /localhost@rpl:linux/2-1-1

r.1:
\t  updated: extrapackage(:runtime) /localhost@rpl:linux/1-1-1 -> /localhost@rpl:linux/2-1-1
\tinstalled: coolpackage(:runtime) /localhost@rpl:linux/1-1-1

r.0:
\tinstalled: extrapackage(:runtime) /localhost@rpl:linux/1-1-1
\tinstalled: specialpackage(:runtime) /localhost@rpl:linux/1-1-1

"""

expected_testRollbackOutputFullVersionsFlavors = """\
r.3:
\t   erased: coolpackage(:runtime) /localhost@rpl:linux/2-1-1[]
\t  updated: extrapackage(:runtime) /localhost@rpl:linux/2-1-1[] -> /localhost@rpl:linux/1-1-1[]
\tinstalled: poorpackage(:runtime) /localhost@rpl:linux/1-1-1[]

r.2:
\t  updated: coolpackage(:runtime) /localhost@rpl:linux/1-1-1[] -> /localhost@rpl:linux/2-1-1[]
\t  updated: specialpackage(:runtime) /localhost@rpl:linux/1-1-1[] -> /localhost@rpl:linux/2-1-1[]

r.1:
\t  updated: extrapackage(:runtime) /localhost@rpl:linux/1-1-1[] -> /localhost@rpl:linux/2-1-1[]
\tinstalled: coolpackage(:runtime) /localhost@rpl:linux/1-1-1[]

r.0:
\tinstalled: extrapackage(:runtime) /localhost@rpl:linux/1-1-1[]
\tinstalled: specialpackage(:runtime) /localhost@rpl:linux/1-1-1[]

"""

class RollbackScriptsTest(rephelp.RepositoryHelper):
    "Serialize rollback scripts"

    @testhelp.context('rollback')
    def testFreezeThaw(self):
        job = ('trv', (None, ''), ('/a@b:c/1.1-1', 'is: x86'), True)
        scriptData = "script data"
        oldCompatClass = 0
        newCompatClass = 1
        data = rollbacks._RollbackScripts._serializeMeta(101, job,
            oldCompatClass, newCompatClass)
        self.assertEqual(data, [
            'index: 101',
            'job: trv=--/a@b:c/1.1-1[is: x86]',
            'oldCompatibilityClass: 0',
            'newCompatibilityClass: 1',
        ])

        from conary import deps
        job2 = ('trv', (None, None),
                ('/a@b:c/1.1-1', deps.deps.parseFlavor('is: x86')), False)
        ret = rollbacks._RollbackScripts._parseMeta(data)
        self.assertEqual(ret[0], 101)
        self.assertEqual(ret[1], (job2, oldCompatClass, newCompatClass))

        data = rollbacks._RollbackScripts._serializeMeta(101, job2,
            oldCompatClass, newCompatClass)
        self.assertEqual(ret[0], 101)
        self.assertEqual(ret[1], (job2, oldCompatClass, newCompatClass))

        # Pathological cases
        ret = rollbacks._RollbackScripts._parseMeta(data[:-1])
        self.assertEqual(ret, None)

        # A non-integer value in a compatibility class will not trip the code
        data2 = data[:-1]
        data2.append('newCompatibilityClass: a')
        ret = rollbacks._RollbackScripts._parseMeta(data2)
        self.assertEqual(ret[0], 101)
        self.assertEqual(ret[1], (job2, oldCompatClass, None))

        # same with None, which is the right value (rolling back an install)
        data2 = data[:-1]
        data2.append('newCompatibilityClass: None')
        ret = rollbacks._RollbackScripts._parseMeta(data2)
        self.assertEqual(ret[0], 101)
        self.assertEqual(ret[1], (job2, oldCompatClass, None))


        data2 = data[:]
        data2[0] = 'index: not a number'
        ret = rollbacks._RollbackScripts._parseMeta(data2)
        self.assertEqual(ret, None)

        data2 = data[:]
        data2.append('')
        data2.append('key, no value')
        data2.append('key, no value, missing space:')
        data2.append('unknown line: ')
        ret = rollbacks._RollbackScripts._parseMeta(data2)
        self.assertEqual(ret[0], 101)
        self.assertEqual(ret[1], (job2, oldCompatClass, newCompatClass))

    @testhelp.context('rollback')
    def testLoadSave(self):
        from conary import deps
        _F = deps.deps.parseFlavor
        items = [
            (('trv1', (None, None), ('/a@b:c/1-1', _F('is: x86')), False),
                "script data 1", 0, 1),
            (('trv2', ('/a@b:c/1-1', _F('is: x86_64')),
                ('/a@b:c/1-1', _F('is: x86')), False),
                "script data 2", 1, 0),
        ]

        rbs = rollbacks._RollbackScripts()
        for item in items:
            rbs.add(*item)

        tdir = os.path.join(self.workDir, "rb")

        expectedFiles = ['post-scripts.meta', 'post-script.1', 'post-script.0']
        self.assertEqual(rbs.getCreatedFiles(tdir),
            set([os.path.join(tdir, x) for x in expectedFiles]))

        # Missing dest dir
        e = self.assertRaises(rollbacks.RollbackScriptsError,
            rbs.save, tdir)
        self.assertEqual(str(e),
            "Open error: 2: %s/post-scripts.meta: No such file or directory"
                % tdir)

        os.mkdir(tdir)
        rbs.save(tdir)

        self.assertEqual(set(os.listdir(tdir)), set(expectedFiles))

        rbs2 = rollbacks._RollbackScripts.load(tdir)

        self.assertEqual(rbs._items, rbs2._items)

        # Get rid of one of the scripts
        os.unlink(os.path.join(tdir, "post-script.1"))

        rbs2 = rollbacks._RollbackScripts.load(tdir)
        self.assertEqual(rbs._items[:-1], rbs2._items)

        # Get rid of the metadata file
        os.unlink(os.path.join(tdir, "post-scripts.meta"))

        e = self.assertRaises(rollbacks.RollbackScriptsError,
            rollbacks._RollbackScripts.load, tdir)
        self.assertEqual(str(e),
            "Open error: 2: %s/post-scripts.meta: No such file or directory"
                % tdir)

    @testhelp.context('trovescripts', 'rollback')
    def testAbortOnErrorInPreScript(self):
        """ This makes sure that the abortOnError option for rollbacks works.
        (CNY-3327).
        """
        failScript = """#!/bin/sh
touch %(root)s/tmp/failed;
exit 5
""" % dict(root=self.rootDir)
        succeedScript = """#!/bin/sh
touch %(root)s/tmp/succeeded
exit 0
"""% dict(root=self.rootDir)
        self.mimicRoot()

        try:
            util.mkdirChain(self.rootDir + '/tmp')

            self.addComponent('foo:runtime', '1.0', filePrimer=1)
            self.addCollection('group-foo', '1.0', [ ('foo:runtime', '1.0' ) ])

            self.addComponent('foo:runtime', '2.0', filePrimer=2)
            self.addCollection('group-foo', '2.0', [ ('foo:runtime', '2.0' ) ],
                               preRollbackScript = rephelp.RollbackScript(
                    script=failScript) )

            self.addComponent('foo:runtime', '3.0', filePrimer=3)
            self.addCollection('group-foo', '3.0', [ ('foo:runtime', '3.0' ) ],
                               preRollbackScript = rephelp.RollbackScript(
                    script=succeedScript) )

            self.updatePkg('group-foo=1.0')
            self.updatePkg('group-foo=2.0')
            self.updatePkg('group-foo=3.0')

            # this one should succeed without problems
            self.rollback(self.rootDir, 2, abortOnError=True)
            # this one should fail with a preScriptError
            self.assertRaises(database.PreScriptError, self.rollback,
                                  self.rootDir, 1, abortOnError=True)

            # now we do it the old way and everything should succeed
            self.addComponent('foo:runtime', '4.0', filePrimer=4)
            self.addCollection('group-foo', '4.0', [ ('foo:runtime', '4.0' ) ],
                               preRollbackScript = rephelp.RollbackScript(
                    script=failScript) )

            self.addComponent('foo:runtime', '5.0', filePrimer=5)
            self.addCollection('group-foo', '5.0', [ ('foo:runtime', '5.0' ) ],
                               preRollbackScript = rephelp.RollbackScript(
                    script=succeedScript ) )

            self.updatePkg('group-foo=4.0')
            self.updatePkg('group-foo=5.0')

            # these should succeed
            self.rollback(self.rootDir, 2)
            self.rollback(self.rootDir, 1)
            self.rollback(self.rootDir, 0)
        finally:
            self.realRoot()


    @testhelp.context('rollback', 'sysmodel')
    def testSystemModelRollbacks(self):
        sm = self.cfg.root + '/etc/conary/system-model'
        foo = self.cfg.root + '/foo'
        util.mkdirChain(self.cfg.root + '/etc/conary')
        self.addComponent('foo:runtime', '1',
                          fileContents = [ ( '/foo', '1') ])
        self.addComponent('foo:runtime', '2',
                          fileContents = [ ( '/foo', '2') ])
        self.addComponent('foo:runtime', '3',
                          fileContents = [ ( '/foo', '3') ])

        model = cml.CML(self.cfg)
        modelFile = systemmodel.SystemModelFile(model)

        file(sm, 'w').write('')
        self.updatePkg('foo:runtime=1', modelFile=modelFile)
        file(sm, 'a').write('install foo:runtime=1\n')
        self.updatePkg('foo:runtime=2', modelFile=modelFile)
        file(sm, 'a').write('install foo:runtime=2\n')
        self.updatePkg('foo:runtime=3', modelFile=modelFile)
        file(sm, 'a').write('install foo:runtime=3\n')


        rbsm = self.cfg.root + '/var/lib/conarydb/rollbacks/%d/system-model'
        self.verifyFile(rbsm % 0, '')
        self.verifyFile(rbsm % 1,
            'install foo:runtime=1\n'
        )
        self.verifyFile(rbsm % 2,
            'install foo:runtime=1\n'
            'install foo:runtime=2\n',
        )

        self.verifyFile(sm,
            'install foo:runtime=1\n'
            'install foo:runtime=2\n'
            'install foo:runtime=3\n',
        )
        self.verifyFile(foo, '3')

        self.rollback(self.rootDir, 2)
        self.verifyFile(sm,
            'install foo:runtime=1\n'
            'install foo:runtime=2\n',
        )
        self.verifyFile(foo, '2')

        self.rollback(self.rootDir, 1)
        self.verifyFile(sm,
            'install foo:runtime=1\n'
        )
        self.verifyFile(foo, '1')

        self.rollback(self.rootDir, 0)
        self.verifyFile(sm, '')
        assert(not os.path.exists(foo))

    @testhelp.context('rollback', 'sysmodel')
    def testSystemModelRollbackRace(self):
        util.mkdirChain(self.cfg.root + '/etc/conary')
        sm = self.cfg.root + '/etc/conary/system-model'
        open(sm, 'w')

        self.addComponent('foo:runtime=1',
                          fileContents = [ ( '/foo', '1') ])
        self.addCollection('foo=1', [ ':runtime' ])
        # does not conflict
        self.addComponent('aaa:runtime=1',
                          fileContents = [ ( '/aaa', '1') ])
        self.addCollection('aaa=1', [ ':runtime' ])
        # intentional file conflict with foo:runtime
        self.addComponent('bar:runtime=1',
                          fileContents = [ ( '/foo', 'conflict!!!') ])
        self.addCollection('bar=1', [ ':runtime' ])


        model = cml.CML(self.cfg)
        modelFile = systemmodel.SystemModelFile(model)

        updatecmd.doModelUpdate(self.cfg, model, modelFile,
            [ '+foo:runtime=localhost@rpl:linux/1' ], updateByDefault = False)
        self.assertRaises(update.UpdateError,
            updatecmd.doModelUpdate, self.cfg, model, modelFile,
            [ '+bar:runtime=localhost@rpl:linux/1' ], updateByDefault = False)
        self.verifyNoFile(sm+'.next')

        # force aaa (no conflict) and bar (conflict) into separate jobs
        self.cfg.updateThreshold = 1
        self.assertRaises(update.UpdateError,
            updatecmd.doModelUpdate, self.cfg, model, modelFile,
            [ '+aaa:runtime=localhost@rpl:linux/1',
              '+bar:runtime=localhost@rpl:linux/1' ], updateByDefault = False)
        self.verifyFile(sm+'.next',
            'install foo:runtime=localhost@rpl:linux/1\n'
            'install bar:runtime=localhost@rpl:linux/1\n'
            'install aaa:runtime=localhost@rpl:linux/1'
            ' bar:runtime=localhost@rpl:linux/1\n')
