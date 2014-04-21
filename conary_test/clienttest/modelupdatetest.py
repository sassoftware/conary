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

import os

from conary import conaryclient
from conary.conaryclient import cml, modelupdate, troveset, update
from conary.errors import TroveSpecsNotFound
from conary.deps import deps
from conary.repository import changeset

class ModelUpdateTest(rephelp.RepositoryHelper):

    def _applyModel(self, modelList, addSearchLabel=True, apply=True,
            useCache=None):
        cachePath = os.path.join(self.workDir, 'modelcache')
        from conary import callbacks
        client = conaryclient.ConaryClient(self.cfg,
                                updateCallback = callbacks.UpdateCallback())

        model = cml.CML(self.cfg)
        cache = modelupdate.CMLTroveCache(
                                    client.getDatabase(), client.getRepos())
        if useCache and os.path.exists(cachePath):
            cache.load(cachePath)

        if addSearchLabel:
            updatedModel = list(modelList)
            updatedModel.insert(0, 'search localhost@rpl:linux')
        else:
            updatedModel = modelList

        model.parse(updatedModel)

        updJob = client.newUpdateJob()
        ts = client.cmlGraph(model)
        suggMap = client._updateFromTroveSetGraph(updJob, ts, cache)
        if useCache:
            cache.save(cachePath)

        if not apply:
            return ts

        client.applyUpdateJob(updJob)

        return updJob, suggMap

    @testhelp.context('sysmodel')
    def testSearchPath(self):
        self.addComponent('foo:runtime=1.0', fileContents = [ ('/foo', '1.0') ])
        self.addComponent('foo:optional=1.0',
                          fileContents = [ ('/opt', '1.0') ])
        self.addCollection("foo=1.0", [ ":runtime", ( ":optional", False) ])
        self.addCollection("group-foo=1.0", [ "foo" ],
                           weakRefList = [ "foo:runtime" ] )

        self.addComponent('foo:runtime=2.0', fileContents = [ ('/foo', '2.0') ])
        self.addComponent('foo:optional=2.0',
                          fileContents = [ ('/opt', '2.0') ])
        self.addCollection("foo=2.0", [ ":runtime", ( ":optional", False) ])
        self.addCollection("group-foo=2.0", [ "foo" ],
                           weakRefList = [ "foo:runtime" ] )

        self._applyModel([ 'search group-foo=localhost@rpl:linux/1.0',
                           'install foo' ])
        self.verifyFile(self.rootDir + '/foo', '1.0')

        self.resetRoot()
        self._applyModel([ 'install foo=1.0', 'install foo:optional' ])
        self.verifyFile(self.rootDir + '/opt', '1.0')

        self.resetRoot()
        self._applyModel([ 'update foo=1.0', 'update foo:optional' ])
        self.verifyFile(self.rootDir + '/opt', '1.0')

        self.resetRoot()
        self._applyModel([ 'offer foo=1.0', 'install foo' ])
        self.verifyFile(self.rootDir + '/foo', '1.0')
        self.verifyNoFile(self.rootDir + '/opt')

        self.resetRoot()
        self._applyModel([ 'offer foo=1.0', 'install foo:optional' ])
        self.verifyNoFile(self.rootDir + '/foo')
        self.verifyFile(self.rootDir + '/opt', '1.0')

        self.resetRoot()
        self._applyModel([ 'offer group-foo=1.0', 'install foo' ])
        self.verifyFile(self.rootDir + '/foo', '1.0')
        self.verifyNoFile(self.rootDir + '/opt')

    @testhelp.context('sysmodel')
    def testIntermediateSearchPath(self):
        self.addComponent('foo:runtime=1.0', fileContents = [ ('/foo', '1.0') ])
        self.addCollection("foo=1.0", [ ":runtime" ])
        self.addComponent('bar:runtime=1.0', fileContents = [ ('/bar', '1.0') ])
        self.addCollection("bar=1.0", [ ":runtime" ])
        self.addCollection("group-foo=1.0", [ "foo", "bar" ],
                           weakRefList = [ "foo:runtime", "bar:runtime" ] )
        self.addComponent('another:runtime=1.0',
                          fileContents = [ ('/another', '1.0') ])
        self.addCollection("another=1.0", [ ":runtime" ])

        self.addComponent('foo:runtime=2.0', fileContents = [ ('/foo', '2.0') ])
        self.addCollection("foo=2.0", [ ":runtime" ] )
        self.addComponent('bar:runtime=2.0', fileContents = [ ('/bar', '2.0') ])
        self.addCollection("bar=2.0", [ ":runtime", ( ":optional", False) ])
        self.addCollection("group-foo=2.0", [ "foo", "bar" ],
                           weakRefList = [ "foo:runtime", "bar:runtime" ] )

        self._applyModel([ 'search group-foo=localhost@rpl:linux/1.0',
                           'install foo',
                           'search group-foo=localhost@rpl:linux/2.0',
                           'install bar',
                           'search another=localhost@rpl:linux/1.0',
                           'install another'],
                         addSearchLabel=False)
        self.verifyFile(self.rootDir + '/foo', '1.0')
        self.verifyFile(self.rootDir + '/bar', '2.0')
        self.verifyFile(self.rootDir + '/another', '1.0')

        self.resetRoot()
        self._applyModel([ 'search group-foo=localhost@rpl:linux/1.0',
                           'search group-foo=localhost@rpl:linux/2.0',
                           'install foo',
                           'install bar'],
                         addSearchLabel=False)
        self.verifyFile(self.rootDir + '/foo', '2.0')
        self.verifyFile(self.rootDir + '/bar', '2.0')

        self.resetRoot()
        self.assertRaises(TroveSpecsNotFound, self._applyModel, [
                          'search group-foo=localhost@rpl:linux/1.0',
                          'install foo',
                          'search group-foo=localhost@rpl:linux/2.0',
                          'install bar',
                          'install another'],
                          addSearchLabel=False)

    @testhelp.context('sysmodel')
    def testInstallWinsOverSearchPath(self):
        self.addComponent('foo:runtime=1.0', fileContents = [ ('/foo', '1.0') ])
        self.addComponent('foo:ignore=1.0',
                          fileContents = [ ('/foo', '1.0') ],
                          provides = 'trove: wanted:this')
        self.addCollection("foo=1.0", [ ":runtime", ( ":ignore", False) ])
        self.addCollection("group-foo=1.0", [ "foo" ],
                           weakRefList = [ ("foo:runtime", True),
                                           ("foo:ignore", False) ] )
        self.addComponent('provider:runtime=1.0',
                          fileContents = [ ('/provider', '1.0') ],
                          provides = 'trove: wanted:this')
        self.addComponent('provider:lib=1.0')
        self.addCollection("provider=1.0", [ ":lib", (":runtime", False) ])
        self.addComponent('requirer:runtime=1.0',
                          fileContents = [ ('/requirer', '1.0') ],
                          requires = 'trove: wanted:this')
        self.addCollection("requirer=1.0", [ ":runtime" ])

        # Here, foo should win via group because provider isn't mentioned
        self._applyModel([ 'search group-foo=localhost@rpl:linux/1.0',
                           'install requirer'])
        self.verifyNoFile(self.rootDir + '/provider')
        self.verifyFile(self.rootDir + '/foo', '1.0')

        # Here, provider should win because provider is installed
        self.resetRoot()
        self._applyModel([ 'search group-foo=localhost@rpl:linux/1.0',
                           'install provider',
                           'install requirer'])
        self.verifyFile(self.rootDir + '/provider', '1.0')
        self.verifyNoFile(self.rootDir + '/foo')

    @testhelp.context('sysmodel')
    def testModelImmediateSearchPath(self):
        '''
        Ensure that things named in search path can be found,
        not just the things that they contain
        '''
        self.addComponent('foo:runtime=/localhost@other:label/1.0-1-1')
        self.addCollection('foo=/localhost@other:label/1.0-1-1', [ ':runtime' ])
        self._applyModel([ 'search foo=localhost@other:label/1.0-1-1',
                           'install foo'], addSearchLabel=False)

    @testhelp.context('sysmodel')
    def testSimple(self):
        # install
        self.addComponent('foo:runtime=1.0', fileContents = [ ('/foo', '1.0') ])
        self.addCollection('foo=1.0', [ ':runtime' ])
        self._applyModel(['install foo'])
        self.verifyFile(self.rootDir + '/foo', '1.0')

        # this should cause an update to 2.0
        self.addComponent('foo:runtime=2.0', fileContents = [ ('/foo', '2.0') ])
        self.addCollection('foo=2.0', [ ':runtime' ])
        self._applyModel(['install foo'])
        self.verifyFile(self.rootDir + '/foo', '2.0')

        # this should cause an update to 3.0, with bar dragged in by deps
        self.addComponent('bar:runtime=3.0', fileContents = [ ('/bar', '3.0') ])
        self.addCollection('bar=3.0', [ ':runtime' ])
        self.addComponent('foo:runtime=3.0', fileContents = [ ('/foo', '3.0') ],
                          requires = deps.parseDep('trove: bar:runtime'))
        self.addCollection('foo=3.0', [ ':runtime' ])
        self._applyModel(['install foo'])
        self.verifyFile(self.rootDir + '/foo', '3.0')
        self.verifyFile(self.rootDir + '/bar', '3.0')

    @testhelp.context('sysmodel')
    def testSplit(self):
        # make sure splitting jobs works
        self.addComponent('foo:runtime=1.0', fileContents = [ ('/foo', '1.0') ])
        self.addCollection('foo=1.0', [ ':runtime' ])

        self.addComponent('bar:runtime=1.0', fileContents = [ ('/bar', '1.0') ])
        self.addCollection('bar=1.0', [ ':runtime' ])

        oldThreshold = self.cfg.updateThreshold
        self.cfg.updateThreshold = 2
        try:
            updJob, suggMap = self._applyModel(['install foo bar'])
            jobs = updJob.getJobs()
            assert(len(jobs) == 2)
            # make sure each job is a collection and a component. we had a
            # bug which put collections before components during ordering
            for job in jobs:
                assert( set([ x[0][3:] for x in job ]) ==
                        set( [ '', ':runtime' ]) )
        finally:
            self.cfg.updateThreshold = oldThreshold

    @testhelp.context('sysmodel')
    def testDependencyFailure(self):
        self.addComponent('foo:runtime=1.0',
                          requires = deps.parseDep('trove: bar:runtime'))
        self.addCollection('foo=1.0', [ ':runtime' ] )
        self.assertRaises(conaryclient.DepResolutionFailure,
                                self._applyModel, ['install foo:runtime'])

    @testhelp.context('sysmodel')
    def testSearchPathBadTrove(self):
        e = self.assertRaises(TroveSpecsNotFound,
            self._applyModel, ['search foo'])
        txt = 'No troves found matching: foo'
        self.assertEquals(str(e), txt)

    @testhelp.context('sysmodel')
    def testInstallBadTrove(self):
        self.openRepository()
        e = self.assertRaises(TroveSpecsNotFound,
            self._applyModel, ['install foo bar'])
        txt = 'No troves found matching: bar foo'
        self.assertEquals(str(e), txt)

    @testhelp.context('sysmodel')
    def testErase(self):
        self.addComponent('foo:runtime=1.0',
                          fileContents = [ ('/runtime', '1.0') ] )
        self.addComponent('foo:lib=1.0',
                          fileContents = [ ('/lib', '1.0') ] )
        self.addCollection('foo=1.0', [ ':lib', ':runtime' ] )
        self._applyModel([ 'install foo', 'erase foo:runtime'] )

        self.verifyFile(self.rootDir + '/lib', '1.0')
        assert(not os.path.exists(self.rootDir + '/runtime'))

        self.addComponent('foo:runtime=/localhost@foo:bar/2.0')
        self.addCollection('foo=/localhost@foo:bar/2.0', [ ':runtime' ])

        self.resetRoot()
        self._applyModel( [ 'install foo', 'search localhost@foo:bar',
                            'erase foo' ] )

        self.addCollection('group-foo=1.0', [ "foo" ])

        self._applyModel( [ 'install group-foo', 'erase foo' ] )


    @testhelp.context('sysmodel')
    def testExcludeTroves(self):
        self.addComponent('foo:runtime=1.0', [ ( '/runtime', '1.0' ) ])
        self.addComponent('foo:doc=1.0', [ ( '/doc', '1.0' ) ])
        self.addCollection('foo', [ ':runtime', ':doc' ] )

        oldExclude = self.cfg.excludeTroves
        try:
            self.cfg.configLine('excludeTroves .*:doc')
            self._applyModel([ 'install foo' ])
            self.verifyFile(self.rootDir + '/runtime', '1.0')
            assert(not os.path.exists(self.rootDir + '/doc'))

            self._applyModel([ 'install foo', 'install foo:doc' ])
            self.verifyFile(self.rootDir + '/runtime', '1.0')
            self.verifyFile(self.rootDir + '/doc', '1.0')
        finally:
            self.cfg.excludeTroves = oldExclude

    @testhelp.context('sysmodel')
    def testInclude(self):
        self.addComponent("foo:runtime=1.0",
                          fileContents = [ ( '/foo', '1.0')  ])
        self.addCollection("foo=1.0", [ ":runtime" ])

        self.addComponent("inctrove:cml", fileContents = [
                    ( "/some/path/to.cml", "install foo\n" ) ] )
        self.addCollection("inctrove=1.0", [ ":cml" ])

        self._applyModel(
                [ 'search %s'% self.cfg.buildLabel,
                  'include inctrove:cml' ],
                addSearchLabel = False)
        self.verifyFile(self.rootDir + '/foo', '1.0')

        self.resetRoot()

        self._applyModel(
                [ 'search %s'% self.cfg.buildLabel,
                  'include inctrove' ],
                addSearchLabel = False)
        self.verifyFile(self.rootDir + '/foo', '1.0')

        self.addComponent("inctrove:source", fileContents = [
                    ( "some.cml", "install foo\n" ),
                    ( "some.recipe", "\n") ] )
        self._applyModel(
                [ 'search %s'% self.cfg.buildLabel,
                  'include inctrove:source' ],
                addSearchLabel = False)
        self.verifyFile(self.rootDir + '/foo', '1.0')

    @testhelp.context('sysmodel')
    def testIncludeDeps(self):
        self.addComponent("foo:runtime=localhost@other:label/1.0",
                          fileContents = [ ( '/foo', '1.0')  ])
        self.addCollection("foo=localhost@other:label/1.0", [ ':runtime' ] )
        self.addComponent("inctrove:cml", fileContents = [
                    ( "/some/path/to.cml",
                      "search foo=localhost@other:label\n" ) ] )

        self.addComponent('bar:runtime', '1.0',
                          requires = deps.parseDep('trove: foo:runtime'))
        self.addCollection("bar", [ ":runtime" ])

        self._applyModel(
                [ 'search %s'% self.cfg.buildLabel,
                  'include inctrove:cml',
                  'install bar' ],
                addSearchLabel = False)
        self.verifyFile(self.rootDir + '/foo', '1.0')

    @testhelp.context('sysmodel')
    def testIncludeErrors(self):
        self.addComponent("inctrove:cml", fileContents = [
                    ( "/some/path/to.cml",
                      "search foo=localhost@other:label\n" ),
                    ( "/some/more.cml", "\n") ] )

        e = self.assertRaises(troveset.IncludeException,
                          self._applyModel, [ 'include inctrove:cml' ])
        self.assertEquals(str(e),
                    'Too many cml files found in '
                    'inctrove:cml=/localhost@rpl:linux/1.0-1-1[]: '
                    '/some/more.cml /some/path/to.cml')

        self.addComponent("inctrove:cml=2.0", fileContents = [ ])
        e = self.assertRaises(troveset.IncludeException,
                          self._applyModel, [ 'include inctrove:cml' ])
        self.assertEquals(str(e),
                          'No cml files found in '
                          'inctrove:cml=/localhost@rpl:linux/2.0-1-1[]')

        self.addComponent('inctrove:runtime=3.0')
        self.addCollection('inctrove=3.0', [ ':runtime' ])
        e = self.assertRaises(troveset.IncludeException,
                          self._applyModel, [ 'include inctrove' ])
        self.assertEquals(str(e),
            'Package inctrove=/localhost@rpl:linux/3.0-1-1[] does not '
            'contain a cml component for inclusion')

        e = self.assertRaises(troveset.IncludeException,
                          self._applyModel, [ 'include inctrove:runtime' ])
        self.assertEquals(str(e), 'Include only supports source and cml '
                          'components')

    @testhelp.context('sysmodel')
    def testIncludeLoops(self):
        self.addComponent("inctrove:cml=1.0",
                          fileContents = [ ("foo.cml", "include inctrove") ])
        self.addCollection("inctrove=1.0", [ ":cml" ])
        e = self.assertRaises(troveset.IncludeException,
                          self._applyModel, [ 'include inctrove:cml' ])
        self.assertEquals(str(e), 'Include loop detected involving '
                            'inctrove:cml=/localhost@rpl:linux/1.0-1-1[]')

    @testhelp.context('sysmodel')
    def testKeepDepsDeps(self):
        self.addComponent("foo:runtime=1.0",
                          requires = deps.parseDep('trove: dep:runtime'))
        self.addCollection("foo=1.0", [ ":runtime" ])

        self.addComponent("dep:runtime=1.0",
                          requires = deps.parseDep('trove: dep2:runtime'))
        self.addCollection("dep=1.0", [ ":runtime" ])

        self.addComponent("dep2:runtime=1.0", fileContents = [ ('/dep2', '1') ])
        self.addCollection("dep2=1.0", [ ":runtime" ])

        updJob, suggMap = self._applyModel(['install foo'])
        self.assertEquals(len(suggMap), 2)
        self.verifyFile(self.rootDir + '/dep2', '1')
        updJob, suggMap = self._applyModel(['install foo'])
        self.assertEquals(len(suggMap), 0)
        self.verifyFile(self.rootDir + '/dep2', '1')

    @testhelp.context('sysmodel')
    def testLocalLabelDeps(self):
        csPath = self.workDir + '/foo.ccs'
        csPath1 = self.workDir + '/foo1.ccs'
        csPath2 = self.workDir + '/foo2.ccs'

        self.addComponent('local:runtime', 'local@local:COOK',
                          changeSetFile = csPath)
        self.addComponent('local:debug', 'local@local:COOK',
                          changeSetFile = csPath1)
        pkg = self.addCollection('local=local@local:COOK',
                                 [ ':runtime', (':debug', False) ],
                                 changeSetFile = csPath2)

        cs = changeset.ChangeSetFromFile(csPath)
        os.unlink(csPath)
        cs.merge(changeset.ChangeSetFromFile(csPath1))
        cs.merge(changeset.ChangeSetFromFile(csPath2))
        cs.setPrimaryTroveList( [ pkg.getNameVersionFlavor() ] )
        cs.writeToFile(csPath)

        self.updatePkg(self.rootDir, csPath)

        self.addComponent('foo:runtime=1.0',
                          requires = 'trove: bar')
        self.addCollection('foo=1.0', [ ':foo' ])
        self.assertRaises(update.DepResolutionFailure,
              self._applyModel,
                    [ 'install local=%s' % pkg.getVersion(),
                      'install foo:runtime=%s' % self.cfg.buildLabel, ],
                      addSearchLabel = True)

    @testhelp.context('sysmodel')
    def testPin(self):
        self.addComponent('foo:runtime=1.0',
                          fileContents = [ ('/runtime-1.0', '1.0') ] )
        self.addCollection('foo=1.0', [ ':runtime' ])
        self.addComponent('foo:runtime=2.0',
                          fileContents = [ ('/runtime-2.0', '2.0') ] )
        self.addCollection('foo=2.0', [ ':runtime' ])

        self._applyModel([ 'install foo=1.0' ])
        self.pin('foo')
        self._applyModel([ 'install foo=2.0' ])
        self.verifyFile(self.rootDir + '/runtime-1.0', '1.0')
        self.verifyFile(self.rootDir + '/runtime-2.0', '2.0')

    @testhelp.context('sysmodel')
    def testRedirect(self):
        self.addComponent('bar:runtime=1.0', fileContents = [ '/bar:runtime'] )
        self.addCollection('bar=1.0', [ ':runtime'] )

        self.addComponent('redirect:runtime=1.0')
        self.addCollection('redirect=1.0', [ ':runtime'] )

        self.addComponent('foo:runtime=1.0', fileContents = [ '/foo:runtime'] )
        self.addCollection('foo=1.0', [ ':runtime' ])

        redirectRecipe = "\n".join([
                    'class testRedirect(RedirectRecipe):',
                    '    name = "redirect"',
                    '    version = "2.0"',
                    '    clearBuildReqs()',
                    '',
                    '    def setup(r):',
                    '        r.addRedirect("foo", "localhost@rpl:linux")' ])

        built, d = self.buildRecipe(redirectRecipe, "testRedirect")

        self._applyModel([ 'install redirect=2.0' ])
        self.verifyFile(self.rootDir + '/foo:runtime')

        redirectRecipe = "\n".join([
                    'class testRedirect(RedirectRecipe):',
                    '    name = "foo"',
                    '    version = "2.0"',
                    '    clearBuildReqs()',
                    '',
                    '    def setup(r):',
                    '        r.addRedirect("bar", "localhost@rpl:linux")' ])

        built, d = self.buildRecipe(redirectRecipe, "testRedirect")
        self._applyModel([ 'install redirect=2.0' ])
        self.verifyFile(self.rootDir + '/bar:runtime')

        redirectRecipe = "\n".join([
                    'class testRedirect(RedirectRecipe):',
                    '    name = "bar"',
                    '    version = "2.0"',
                    '    clearBuildReqs()',
                    '',
                    '    def setup(r):',
                    '        r.addRedirect("redirect","localhost@rpl:linux")' ])
        built, d = self.buildRecipe(redirectRecipe, "testRedirect")

        self.assertRaisesRegexp(conaryclient.UpdateError,
                '^Redirect loop found which includes troves redirect, foo$',
                self._applyModel, [ 'install redirect=2.0' ])

    @testhelp.context('sysmodel')
    def testReferences(self):
        self.addComponent('foo:runtime=1.0')
        self.addCollection('foo=1.0', [ ':runtime' ])
        self.addComponent('bar:runtime=/localhost@other:label/1.0',
                          fileContents = [ ('/bar-runtime', '1.0') ] )
        self.addCollection('bar=/localhost@other:label/1.0', [ ':runtime' ])
        self.addCollection('group-foo=1.0',
                           [ 'foo',
                             ('bar=/localhost@other:label/1.0-1-1', False) ])

        self._applyModel([ 'install group-foo', 'install bar' ])
        self.verifyFile(self.rootDir + '/bar-runtime', '1.0')

    @testhelp.context('sysmodel')
    def testRemoveRecursion(self):
        self.addComponent('foo:runtime=1.0',
                          fileContents = [ ('/runtime', '1.0') ] )
        self.addCollection('foo=1.0', [ ':runtime' ])
        self.addComponent('foo:runtime=2.0',
                          fileContents = [ ('/runtime', '2.0') ] )
        self.addCollection('foo=2.0', [ ':runtime' ])
        self._applyModel([ 'install foo=1.0', 'update foo', 'erase foo' ])
        self.verifyNoFile(self.rootDir + '/runtime')

    @testhelp.context('sysmodel')
    def testPatch(self):
        self.addComponent('foo:runtime=1.0',
                          fileContents = [ ('/runtime', '1.0') ] )
        self.addComponent('foo:lib=1.0',
                          fileContents = [ ('/lib', '1.0') ] )
        self.addCollection('foo=1.0', [ ':runtime', ':lib' ])

        self.addComponent('foo:runtime=/localhost@foo:bar/1.0',
                          fileContents = [ ('/runtime', 'foo/1.0') ] )
        self.addCollection('foo=/localhost@foo:bar/1.0', [ ':runtime' ])

        self._applyModel([ 'install foo',
                           'patch foo:runtime=localhost@foo:bar' ])
        self.verifyFile(self.rootDir + '/lib', '1.0')
        self.verifyFile(self.rootDir + '/runtime', 'foo/1.0')

        self.addComponent('bar:runtime', '1.0',
                          fileContents = [ '/bar' ])
        self.addCollection('bar=1.0', [ ':runtime' ])

        self.addCollection('group-foo-not-bar=1.0',
                           [ 'foo', ('bar', False) ],
                           weakRefList = [ 'foo:runtime', 'foo:lib',
                                           ('bar:runtime', False) ] )
        self._applyModel([ 'install group-foo-not-bar', 'patch bar' ])
        self.verifyFile(self.rootDir + '/lib', '1.0')
        self.verifyFile(self.rootDir + '/runtime', '1.0')
        assert(not os.path.exists(self.rootDir + '/bar'))

        # going backwards in time should be a nop
        self._applyModel([ 'install foo=localhost@foo:bar', 'patch foo'])
        self.verifyFile(self.rootDir + '/runtime', 'foo/1.0')
        assert(not os.path.exists(self.rootDir + '/lib'))
        assert(not os.path.exists(self.rootDir + '/bar'))



    @testhelp.context('sysmodel')
    def testUpdate(self):
        self.addComponent('foo:runtime=1.0',
                          fileContents = [ ('/foo-runtime', '1.0') ] )
        self.addComponent('foo:lib=1.0',
                          fileContents = [ ('/foo-lib', '1.0') ] )
        self.addCollection('foo=1.0', [ ':runtime', ':lib' ])
        self.addComponent('bar:runtime=1.0',
                          fileContents = [ ('/bar-runtime', '1.0') ] )
        self.addCollection('bar=1.0', [ ':runtime' ])
        self.addCollection('group-foobar=1.0', [ 'foo', 'bar' ])

        self.addComponent('foo:runtime=2.0',
                          fileContents = [ ('/foo-runtime', '2.0') ] )
        self.addCollection('foo=2.0', [ ':runtime' ])

        self._applyModel([ 'install group-foobar=1.0',
                           'update foo=2.0'
                           ])

        assert(not os.path.exists(self.rootDir + '/foo-lib'))
        self.verifyFile(self.rootDir + '/foo-runtime', '2.0')
        self.verifyFile(self.rootDir + '/bar-runtime', '1.0')

        self.resetRoot()

        self._applyModel([ 'update group-foobar=1.0',
                           'update foo=2.0'
                           ])
        assert(not os.path.exists(self.rootDir + '/foo-lib'))
        self.verifyFile(self.rootDir + '/foo-runtime', '2.0')
        self.verifyFile(self.rootDir + '/bar-runtime', '1.0')

        self.addCollection('group-optbar=1.0',
                           [ 'foo', ( 'bar', False) ],
                           weakRefList = [ 'foo:runtime=', 'foo:lib',
                                           ('bar:runtime', False) ] )

        self.resetRoot()
        self._applyModel([ 'update group-optbar=1.0' ])
        self.verifyFile(self.rootDir + '/foo-lib', '1.0')
        self.verifyFile(self.rootDir + '/foo-runtime', '1.0')
        assert(not os.path.exists(self.rootDir + '/bar-runtime'))

        self._applyModel([ 'update group-optbar=1.0', 'update bar' ])
        self.verifyFile(self.rootDir + '/foo-lib', '1.0')
        self.verifyFile(self.rootDir + '/foo-runtime', '1.0')
        self.verifyFile(self.rootDir + '/bar-runtime', '1.0')

    @testhelp.context('sysmodel')
    def testUpdateOptionalPackages(self):
        '''
        CNY-3555
        '''
        self.addComponent('foo:runtime', '1.0-1-1', filePrimer=1)
        self.addCollection('foo', '1.0-1-1', [ ':runtime' ])
        self.addComponent('foo:runtime', '2.0-1-1', filePrimer=2)
        self.addCollection('foo', '2.0-1-1', [ ':runtime' ])

        self.addCollection('group-test', '1.0-1-1',
                           [ ('foo', False) ],
                           weakRefList = [ ('foo:runtime', False) ])

        self._applyModel([
            'search group-test=localhost@rpl:linux/1.0-1-1',
            'install group-test',
            'update foo=localhost@rpl:linux/2.0-1-1' ])

        self.verifyNoFile(self.rootDir + '/contents1')
        self.verifyFile(self.rootDir + '/contents2', 'hello, world!\n')

    @testhelp.context('sysmodel')
    def testWalk(self):
        self.addComponent('foo:runtime=1.0',
                          fileContents = [ ('/foo-runtime', '1.0') ] )
        self.addComponent('foo:lib=1.0',
                          fileContents = [ ('/foo-runtime', '1.0') ] )
        self.addCollection('foo=1.0', [ ':lib', ':runtime' ])

        self.addComponent('bar:runtime=1.0',
                          fileContents = [ ('/bar-runtime', '1.0') ] )
        self.addCollection('bar=1.0', [ ':runtime' ])

        self.addCollection('group-foobar=1.0', [ 'foo', 'bar' ],
                           weakRefList = [ ("foo:runtime", False),
                                            "foo:lib",
                                            "bar:runtime" ] )

        # foo:runtime ought to be installed because foo says so; group-foobar
        # isn't relevant
        self._applyModel([ 'install foo:lib',
                           'install foo',
                           'install bar',
                           'install group-foobar',
                           ])

    @testhelp.context('sysmodel')
    def testBranchUpdate(self):
        "CNY-3645"
        self.addComponent('foo:runtime', '1.0-1-1')
        self.addCollection('foo', '1.0-1-1', [':runtime'])
        self._applyModel([ 'install foo=/localhost@rpl:linux' ])

    @testhelp.context('sysmodel')
    def testCacheMergeBug(self):
        "@tests: CNY-3770"
        self.addComponent('foo:runtime', provides='file: /bin/foo')
        self.addComponent('bar:runtime', provides='file: /bin/bar')
        self.addComponent('foo2:runtime', requires='file: /bin/foo')
        self.addComponent('bar2:runtime', requires='file: /bin/bar')
        for name in ['foo', 'foo2', 'bar', 'bar2']:
            self.addCollection(name, [':runtime'])
        model = [
                'search bar=localhost@rpl:linux',
                'search foo=localhost@rpl:linux',
                'install foo2:runtime=localhost@rpl:linux',
                ]
        # First pass caches a hit for /bin/foo on the foo= search line.
        self._applyModel(model, addSearchLabel=False, useCache=True)
        # Second pass caches a miss for /bin/bar on the foo= search line.
        # The bug is that it would also cache a miss for all other foo=
        # solutions including the /bin/foo one.
        model.append('install bar2:runtime=localhost@rpl:linux')
        self._applyModel(model, addSearchLabel=False, useCache=True)
        # The third operation would always fail now that the cache is poisoned.
        self._applyModel(model, addSearchLabel=False, useCache=True)

    @testhelp.context('sysmodel')
    def testImplicitDepUpdate(self):
        "@tests: CNY-3841"
        t1 = self.addComponent('foo:runtime=1.0', provides='file: /bin/foo')
        t2 = self.addComponent('foo:runtime=2.0', provides='file: /bin/foo')
        self.addComponent('bar:runtime', requires='file: /bin/foo')
        self.addCollection('foo=1.0', [':runtime'])
        self.addCollection('foo=2.0', [':runtime'])
        self.addCollection('bar', [':runtime'])
        self._applyModel([
                'search foo=localhost@rpl:linux/1.0',
                'install bar=localhost@rpl:linux',
                ])
        jobs, suggMap = self._applyModel([
                'search foo=localhost@rpl:linux/2.0',
                'install bar=localhost@rpl:linux',
                ])
        jobs = sorted(sum(jobs.jobs, []))
        self.assertEqual(jobs, [
            ('foo',         (t1.getVersion(), t1.getFlavor()), (t2.getVersion(), t2.getFlavor()), False),
            ('foo:runtime', (t1.getVersion(), t1.getFlavor()), (t2.getVersion(), t2.getFlavor()), False),
            ])
