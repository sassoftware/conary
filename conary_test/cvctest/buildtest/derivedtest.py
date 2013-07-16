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

import os
import itertools
import stat
import textwrap

import conary_test
from conary_test import rephelp
from conary_test import resources
from conary_test.rephelp import RegularFile

from conary import files, versions
from conary.deps import deps
from conary.build import errors as builderrors
from conary.build import defaultrecipes
from conary.conaryclient import filetypes

basicDerivedRecipe = '''\
class %(classname)sRecipe(DerivedPackageRecipe):

    clearBuildReqs()

    name = '%(name)s'
    version = '%(version)s'
    %(parentVersion)s

    def setup(r):
        %(rules)s
'''

class DerivedPackageTest(rephelp.RepositoryHelper):

    shadowLabel = versions.Label('localhost@rpl:shadow')

    def tearDown(self):
        self.cfg.buildLabel = self.defLabel
        rephelp.RepositoryHelper.tearDown(self)

    def addCompAndPackage(self, compName, version, *args, **kwargs):
        pkgName = compName.split(':')[0]
        compName = ':' + compName.split(':')[1]
        return self.addCompsAndPackage(pkgName, version,
                        [ self.singleComp(compName, *args, **kwargs) ] )

    def addCompsAndPackage(self, pkgName, version, compList):
        srcName = pkgName + ':source'

        self.cfg.buildLabel = self.defLabel
        comps = []
        for compName, compArgs, compKwArgs in compList:
            fullCompName = pkgName + compName
            byDefault = compKwArgs.pop('byDefault', True)
            comp = self.addComponent(fullCompName, version, *compArgs,
                                     **compKwArgs)
            comps.append((comp, byDefault))

        binVersion = comp.getVersion()
        srcVersion = binVersion.getSourceVersion()
        self.addCollection(pkgName, binVersion,
                    [ (comp.getName(), comp.getVersion(), comp.getFlavor(),
                       byDefault) for (comp, byDefault) in comps ])

        return srcVersion.createShadow(self.shadowLabel)

    @staticmethod
    def getRecipe(pkgName, sourceVersion, rules, parentVersion = None):
        d = {}
        d['name'] = pkgName
        d['classname'] = pkgName.replace('-', '_')
        d['version'] = sourceVersion.trailingRevision().getVersion()
        d['rules'] = "\n        ".join(rules)

        if parentVersion is not None:
            d['parentVersion'] = 'parentVersion = "%s"' % parentVersion
        else:
            d['parentVersion'] = ''

        return basicDerivedRecipe % d

    def buildDerivation(self, name, sourceVersion, *rules, **kwargs):
        if ':' in name:
            pkgName = name.split(':')[0]
        else:
            pkgName = name

        recipe = self.getRecipe(pkgName, sourceVersion, rules,
                                kwargs.get('parentVersion', None))

        returnTrove = kwargs.pop('returnTrove', name)
        buildFlavor = kwargs.pop('buildFlavor', None)
        prep = kwargs.pop('prep', False)

        oldBuildFlavor = self.cfg.buildFlavor
        if buildFlavor is not None:
            self.cfg.buildFlavor = deps.parseFlavor(buildFlavor)
        try:
            self._printOnError, oldval = True, self._printOnError
            rc = self.build(recipe, "%sRecipe" % pkgName.replace('-', '_'),
                              sourceVersion = sourceVersion,
                              returnTrove = returnTrove,
                              prep = prep)
        finally:
            self.cfg.buildFlavor = oldBuildFlavor
            self._printOnError = oldval

        return rc

    @staticmethod
    def checkLoaded(trv, *l):
        s = set()
        for nameList, version, flavor in l:
            version = versions.VersionFromString(version)
            flavor = deps.parseFlavor(flavor)
            if type(nameList) == list or type(nameList) == tuple:
                s.update((x, version, flavor) for x in nameList)
            else:
                s.add( (nameList, version, flavor) )
        assert(set(trv.getDerivedFrom()) == s)

    def testSimpleDerivation(self):
        srcVersion  = self.addCompAndPackage('foo:runtime', '1.0-1-1',
                      fileContents = [ '/bin/bar',
                           '/bin/foo',
                           ('/bin/blah', rephelp.RegularFile(
                                            contents = 'contents',
                                            perms = 0700) ),
                                            ] )

        trv = self.buildDerivation('foo:runtime', srcVersion, "pass")
        assert(sorted([ x[1] for x in trv.iterFileList() ]) ==
                    [ '/bin/bar', '/bin/blah', '/bin/foo' ] )

        pkgTrv, runtimeTrv = self.buildDerivation('foo', srcVersion,
                                   "r.Remove('/bin/foo')",
                                   "r.SetModes('/bin/blah', 0755)",
                                   "r.Replace('contents', 'new', '/bin/blah')",
                                   returnTrove = [ 'foo', ':runtime' ]
                                   )
        self.updatePkg('%s=%s' % (pkgTrv.getName(), pkgTrv.getVersion()))
        self.verifyFile(self.rootDir+'/bin/blah', 'new', perms=0755)
        self.verifyNoFile(self.rootDir+'/bin/foo')
        assert(sorted([ x[1] for x in runtimeTrv.iterFileList() ]) ==
                    [ '/bin/bar', '/bin/blah' ] )
        assert(runtimeTrv.getSourceName() == 'foo:source')
        assert(runtimeTrv.isDerived())
        assert(not runtimeTrv.isCollection())

        assert(pkgTrv.getSourceName() == 'foo:source')
        assert(pkgTrv.isDerived())
        assert(pkgTrv.isCollection())
        self.checkLoaded(pkgTrv,
            ( ("foo", "foo:runtime"), "/localhost@rpl:linux/1.0-1-1", "") )

    def testDerivedRecipeDeps(self):
        srcVersion  = self.addCompAndPackage('foo:runtime', '1.0-1-1',
                      fileContents = [ '/bin/bar',
                           '/bin/foo',
                           ('/bin/blah', rephelp.RegularFile(
                                            contents = 'contents',
                                            perms = 0700) ),
                                            ] )
        def ThisGetRecipe(*args, **kwargs):
            # for this test, we don't want to clear the buildRequires, since
            # that's what we're testing
            return """class fooRecipe(DerivedPackageRecipe):
                          name = 'foo'
                          version = '1.0'

                          def setup(r):
                              pass"""
        self.mock(self, 'getRecipe', ThisGetRecipe)
        baseReqRecipe = """class BaseRequiresRecipe(AbstractPackageRecipe):
            name = 'baserequires'
            version = '1'
            abstractBaseClass = 1

            buildRequires = ['bar:devel']"""

        derivedPackage = defaultrecipes.DerivedPackageRecipe.replace(\
                'internalAbstractBaseClass', 'abstractBaseClass')
        derivedPackage += "\n    version = '1'"

        self.addComponent('baserequires:recipe',
                fileContents = [('/baserequires.recipe',
                    filetypes.RegularFile(contents = baseReqRecipe))])
        self.addCollection('baserequires',
                strongList = ['baserequires:recipe'])

        self.addComponent('derivedpackage:recipe',
                fileContents = [('/derivedpackage.recipe',
                    filetypes.RegularFile(contents = derivedPackage))])
        self.addCollection('derivedpackage',
                strongList = ['derivedpackage:recipe'])

        self.cfg.autoLoadRecipes = ['baserequires', 'derivedpackage']

        # prove that exactly bar:runtime is by go/no-go
        err = self.assertRaises(builderrors.RecipeDependencyError,
                self.buildDerivation, 'foo:runtime', srcVersion, "pass")

        self.addComponent('bar:devel')
        self.updatePkg('bar:devel')
        trv = self.buildDerivation('foo:runtime', srcVersion, "pass")

    @staticmethod
    def singleComp(compName, *args, **kwargs):
        return ( compName, args, kwargs )

    def testFlavorRequiresProvides(self):
        srcVersion  = self.addCompAndPackage('foo:runtime', '1.0-1-1',
                      fileContents = [
                        ('/etc/mmx', RegularFile(flavor = 'is: x86(mmx)',
                                                 requires = 'perl: r-mmx',
                                                 provides = 'perl: p-mmx') ),
                        ('/etc/sse', RegularFile(flavor = 'is: x86(sse)',
                                                 requires = 'perl: r-sse',
                                                 provides = 'perl: p-sse') ) ],
                      flavor = 'use: gtk',
                      requires = 'trove: r-gtk',
                      provides = 'perl: perl-gtk')

        runtimeTrv = self.buildDerivation('foo:runtime', srcVersion,
                                      "r.Remove('/etc/mmx')",
                                      buildFlavor = 'gtk is:x86(mmx,sse)')
        assert(runtimeTrv.getFlavor() ==
               deps.parseFlavor('gtk is: x86(sse)'))
        # /etc/mmx removal implies that its deps disappear from the trove
        assert(runtimeTrv.getRequires() ==
               deps.parseDep('trove: r-gtk perl: r-sse'))
        assert(runtimeTrv.getProvides() ==
               deps.parseDep('perl: perl-gtk trove: foo:runtime perl: p-sse'))

    def testComponentSplit(self):
        srcVersion  = self.addCompsAndPackage('foo', '1.0-1-1',
              [ self.singleComp(':runtime', fileContents = [ '/bin/bar']) ,
                self.singleComp(':lib', fileContents = [ '/usr/lib/bar']) ] )

        # test basic component splitting
        runtimeTrv, libTrv = self.buildDerivation('foo', srcVersion, "pass",
                                        returnTrove = [ ':runtime', ':lib' ])

        assert(sorted([ x[1] for x in runtimeTrv.iterFileList() ]) ==
                    [ '/bin/bar' ] )
        assert(sorted([ x[1] for x in libTrv.iterFileList() ]) ==
                    [ '/usr/lib/bar' ] )

        # /bin/new follows default policy and shows up in :runtime
        runtimeTrv, libTrv = self.buildDerivation('foo', srcVersion,
                "r.Create('/bin/new', mode=0755)",
                returnTrove = [ ':runtime', ':lib' ])

        assert(sorted([ x[1] for x in runtimeTrv.iterFileList() ]) ==
                    [ '/bin/bar', '/bin/new' ] )
        assert(sorted([ x[1] for x in libTrv.iterFileList() ]) ==
                    [ '/usr/lib/bar' ] )

        # create a new file and move it into :new. while we're at it, make
        # sure new is owned by root.root
        runtimeTrv, libTrv, newTrv = self.buildDerivation('foo', srcVersion,
                "r.Create('/bin/new', mode=0755)",
                "r.ComponentSpec(':new', '/bin/new')",
                returnTrove = [ ':runtime', ':lib', ':new' ])

        assert(sorted([ x[1] for x in runtimeTrv.iterFileList() ]) ==
                    [ '/bin/bar' ] )
        assert(sorted([ x[1] for x in libTrv.iterFileList() ]) ==
                    [ '/usr/lib/bar' ] )
        assert(len(list(newTrv.iterFileList())) == 1)
        pathId, path, fileId, fileVersion = list(newTrv.iterFileList())[0]
        assert(path == '/bin/new')
        repos = self.openRepository()
        fObj = repos.getFileVersion(pathId, fileId, fileVersion)
        assert(fObj.inode.owner() == 'root')
        assert(fObj.inode.group() == 'root')

        # move both the new file and the old /bin/bar into :new, which causes
        # :runtime to disappear
        trv, libTrv, newTrv = self.buildDerivation('foo', srcVersion,
                "r.Create('/bin/new', mode=0755)",
                "r.ComponentSpec(':new', '/bin/.*')",
                returnTrove = [ 'foo', ':lib', ':new' ])

        assert(len([ x for x in trv.iterTroveList(strongRefs=  True) ]) == 2)
        assert(sorted([ x[1] for x in libTrv.iterFileList() ]) ==
                    [ '/usr/lib/bar' ] )
        assert(sorted([ x[1] for x in newTrv.iterFileList() ]) ==
                    [ '/bin/bar', '/bin/new' ] )

    def testParentVersion(self):
        srcVersion =  self.addCompAndPackage('foo:runtime', '1.0-1-1',
                      fileContents = [ '/bin/bar', '/bin/1' ] )
        srcVersion2 = self.addCompAndPackage('foo:runtime', '1.0-1-2',
                      fileContents = [ '/bin/bar', '/bin/1', '/bin/2' ] )
        assert(srcVersion == srcVersion2)

        trv = self.buildDerivation('foo:runtime', srcVersion,
                                   "r.Remove('/bin/1')")
        assert(sorted([ x[1] for x in trv.iterFileList() ]) ==
                    [ '/bin/2', '/bin/bar' ] )

        trv = self.buildDerivation('foo:runtime', srcVersion,
                                   "r.Remove('/bin/1')",
                                   parentVersion = '1.0-1-1')
        assert(sorted([ x[1] for x in trv.iterFileList() ]) ==
                    [ '/bin/bar' ] )

        try:
            trv = self.buildDerivation('foo:runtime', srcVersion,
                                       "pass",
                                       parentVersion = '1.1-1-1')
        except builderrors.RecipeFileError, e:
            assert(str(e) == 'parentRevision must have the same upstream '
                             'version as the derived package recipe')

        badParentVersion = 'chunky-bacon@my:house'
        try:
            trv = self.buildDerivation('foo:runtime', srcVersion, 'pass',
                    parentVersion = badParentVersion)
        except builderrors.RecipeFileError, e:
            assert(str(e).startswith('Cannot parse parentVersion %s' % \
                    badParentVersion))
        else:
            self.fail("Expected builderrors.RecipeFileError")

    def getFiles(self, trv):
        fileInfoList = list(trv.iterFileList())
        repos = self.openRepository()
        fileObjs = repos.getFileVersions([ (x[0], x[2], x[3]) for x in
                                                fileInfoList ])
        fileInfo = dict( (x[0][1], x[1]) for x in
                        itertools.izip(fileInfoList, fileObjs) )

        return fileInfo

    def testOwnership(self):
        srcVersion  = self.addCompAndPackage('foo:runtime', '1.0-1-1',
                      fileContents = [ '/bin/bar',
                           ('/bin/foo',
                              rephelp.RegularFile(owner = 'owner',
                                                  group = 'group')),
                           ('/bin/save',
                              rephelp.RegularFile(owner = 'owner',
                                                  group = 'group'))
                                     ])

        runtimeTrv = self.buildDerivation('foo:runtime', srcVersion,
               "r.Create('/bin/new', mode=0755)",
               "r.Ownership('newowner', 'newgroup', '/bin/new')",
               "r.Ownership('fooowner', 'foogroup', '/bin/foo')")

        fileInfo = self.getFiles(runtimeTrv)

        assert(fileInfo['/bin/save'].inode.owner() == 'owner')
        assert(fileInfo['/bin/save'].inode.group() == 'group')
        assert(fileInfo['/bin/save'].requires() ==
               deps.parseDep('userinfo: owner groupinfo: group'))
        assert(fileInfo['/bin/bar'].inode.owner() == 'root')
        assert(fileInfo['/bin/bar'].inode.group() == 'root')
        assert('userinfo' not in str(fileInfo['/bin/bar'].requires()))
        assert(fileInfo['/bin/foo'].inode.owner() == 'fooowner')
        assert(fileInfo['/bin/foo'].inode.group() == 'foogroup')
        assert(fileInfo['/bin/foo'].requires() ==
               deps.parseDep('userinfo: fooowner groupinfo: foogroup'))
        assert(fileInfo['/bin/new'].inode.owner() == 'newowner')
        assert(fileInfo['/bin/new'].inode.group() == 'newgroup')
        assert(fileInfo['/bin/new'].requires() ==
               deps.parseDep('userinfo: newowner groupinfo: newgroup'))


    def testDevices(self):
        srcVersion  = self.addCompAndPackage('foo:runtime', '1.0-1-1',
              fileContents = [ ( '/dev/tty', rephelp.CharacterDevice(5, 0) ) ] )
        trv = self.buildDerivation('foo:runtime', srcVersion,
               "r.MakeDevices('/dev/hda1', 'b', 3, 1, 'root', 'root', 0600)" )

        fileInfo = self.getFiles(trv)

        f = fileInfo['/dev/tty']
        assert(isinstance(f, files.CharacterDevice))
        assert(f.devt.major() == 5 and f.devt.minor() == 0)

        f = fileInfo['/dev/hda1']
        assert(isinstance(f, files.BlockDevice))
        assert(f.devt.major() == 3 and f.devt.minor() == 1)

    def testDirectories(self):
        srcVersion  = self.addCompAndPackage('foo:runtime', '1.0-1-1',
              fileContents = [ ( '/old', rephelp.Directory() ) ] )
        trv = self.buildDerivation('foo:runtime', srcVersion,
               "r.MakeDirs('/new', '/skipped')",
               "r.ExcludeDirectories(exceptions = '/new')" )

        assert(len(list(trv.iterFileList())) == 2)
        fileInfo = self.getFiles(trv)

        assert(isinstance(fileInfo['/old'], files.Directory))
        assert(isinstance(fileInfo['/new'], files.Directory))

    def testComponentRequires(self):
        srcVersion  = self.addCompsAndPackage('foo', '1.0-1-1',
              [ self.singleComp(':runtime', fileContents = [ '/bin/bar'],
                                requires = 'trove: foo:config' ) ,
                self.singleComp(':lib', fileContents = [ '/usr/lib/bar'] ),
                self.singleComp(':config', fileContents = [ ('/etc/bar', '')] ) ] )

        runtimeTrv, libTrv = self.buildDerivation('foo', srcVersion,
               "r.Create('/usr/share/foo')",
               returnTrove = [ ':runtime', ':lib' ] )

        # :runtime requires :data because :data is new
        # :runtime does not require :lib, because :lib is old and it wasn't
        #     required in the parent
        # :runtime requires :config because it did in the parent
        # :lib requires newly created :data
        assert(runtimeTrv.getRequires() ==
                        deps.parseDep('trove: foo:config trove: foo:data'))
        assert(libTrv.getRequires() ==
                        deps.parseDep('trove: foo:data'))

        runtimeTrv = self.buildDerivation('foo', srcVersion,
               "r.Remove('/etc/bar')",
               returnTrove = ':runtime' )
        # :runtime requires nothing, because there is no :data this time,
        #   :lib wasn't required in the parent, and :config disappeared
        assert(runtimeTrv.getRequires().isEmpty())

    def testConfigExclusion(self):
        srcVersion  = self.addCompsAndPackage('foo', '1.0-1-1',
              [ self.singleComp(':runtime', fileContents = [ '/bin/bar'],
                                requires = 'trove: foo:config' ) ,
                self.singleComp(':lib', fileContents = [ '/usr/lib/bar'] ),
                self.singleComp(':config', fileContents = [ ('/etc/bar', '')] ) ] )
        # test that removing /etc/bar doesn't trigger a Config inclusion error
        runtimeTrv = self.buildDerivation('foo', srcVersion,
               "r.Remove('/etc/bar')",
               returnTrove = ':runtime' )

        # test that removing and readding doesn't trigger an exclusion error
        runtimeTrv = self.buildDerivation('foo', srcVersion,
               "r.Remove('/etc/bar')",
               "r.Create('/etc/bar')",
               "r.Config(exceptions = '/etc/bar')",
               returnTrove = ':config' )

        self.assertEquals([x[1] for x in runtimeTrv.iterFileList()],
                ['/etc/bar'])

    def testMerge(self):
        oldDir = os.getcwd()
        # FIXME: silence warnings about experimental nature
        self.logFilter.add()
        try:
            self.addComponent('foo:source', '1.0-1',
                    fileContents = [ ( 'foo.recipe',
                            'class FooRecipe(PackageRecipe):\n'
                            '    name = "foo"\n'
                            '    version = "1.0"\n' ) ] )

            self.mkbranch("1.0-1", '@rpl:shadow', "foo:source", shadow = True)
            os.chdir(self.workDir)
            self.checkout("foo", "@rpl:shadow")
            os.chdir('foo')

            self.writeFile('foo.recipe',
                            'class FooRecipe(DerivedPackageRecipe):\n'
                            '    name = "foo"\n'
                            '    version = "1.0"\n' )
            self.commit()

            self.addComponent('foo:source', '2.0-1',
                    fileContents = [ ( 'foo.recipe',
                            'class FooRecipe(PackageRecipe):\n'
                            '    name = "foo"\n'
                            '    version = "2.0"\n' ) ] )

            self.merge()

            self.verifyFile('foo.recipe',
                            'class FooRecipe(DerivedPackageRecipe):\n'
                            '    name = "foo"\n'
                            '    version = "2.0"\n' )

            self.commit()
        finally:
            os.chdir(oldDir)

    def testComponentProvides(self):
        srcVersion  = self.addCompsAndPackage('foo', '1.0-1-1',
              [ self.singleComp(':runtime', fileContents = [ '/asdf/bar'],
                                provides = 'trove: foo:runtime(CAPABILITY)' ) ,
              ])

        runtimeTrv, dataTrv = self.buildDerivation('foo', srcVersion,
               "r.Create('/usr/share/data')",
               returnTrove = [ ':runtime', ':data' ] )

        # existing :runtime should preserve provides foo:runtime(CAPABILITY)
        # new :data should newly provide foo:data(CAPABILITY)
        assert(runtimeTrv.getProvides() ==
                   deps.parseDep('trove: foo:runtime(CAPABILITY)'))
        assert(dataTrv.getProvides() ==
                   deps.parseDep('trove: foo:data(CAPABILITY)'))

        runtimeTrv, dataTrv = self.buildDerivation('foo', srcVersion,
               "r.Create('/usr/share/data')",
               "r.ComponentProvides('NEW')",
               returnTrove = [ ':runtime', ':data' ] )
        assert(runtimeTrv.getProvides() ==
                   deps.parseDep('trove: foo:runtime(CAPABILITY NEW)'))
        assert(dataTrv.getProvides() ==
                   deps.parseDep('trove: foo:data(CAPABILITY NEW)'))

        runtimeTrv, dataTrv = self.buildDerivation('foo', srcVersion,
               "r.Create('/usr/share/data')",
               "r.ComponentProvides(exceptions='CAPABILITY')",
               returnTrove = [ ':runtime', ':data' ] )
        assert(runtimeTrv.getProvides() ==
                   deps.parseDep('trove: foo:runtime'))
        assert(dataTrv.getProvides() ==
                   deps.parseDep('trove: foo:data'))


    def testLinkGroups(self):
        srcVersion  = self.addCompAndPackage('foo:runtime', '1.0-1-1',
                      fileContents = [
                           ('/bin/onefile', rephelp.RegularFile(
                                            linkGroup = '\1'*16,
                                            contents = 'contents',
                                            perms = 0700) ),
                           ('/bin/anotherfile', rephelp.RegularFile(
                                            linkGroup = '\1'*16,
                                            contents = 'contents',
                                            perms = 0700) ),
                                            ] )

        pkgTrv, runtimeTrv = self.buildDerivation('foo', srcVersion,
               "r.Link('yetanother', '/bin/onefile')",
               returnTrove = [ 'foo', ':runtime' ])
        self.updatePkg('%s=%s' % (pkgTrv.getName(), pkgTrv.getVersion()))
        inode1 = os.stat(self.rootDir+'/bin/onefile')[stat.ST_INO]
        inode2 = os.stat(self.rootDir+'/bin/anotherfile')[stat.ST_INO]
        inode3 = os.stat(self.rootDir+'/bin/yetanother')[stat.ST_INO]
        self.assertEqual(inode1, inode2)
        self.assertEqual(inode1, inode3)

    def testSetUGid(self):
        srcVersion  = self.addCompAndPackage('foo:runtime', '1.0-1-1',
                      fileContents = [
                           ('/bin/setuid', rephelp.RegularFile(
                                            perms = 04755) ),
                           ('/bin/setgid', rephelp.RegularFile(
                                            perms = 02755) ),
                                            ] )

        runtimeTrv = self.buildDerivation('foo', srcVersion,
               "r.Create('/bin/new', mode = 07555)", returnTrove = ':runtime' )

        fileInfo = self.getFiles(runtimeTrv)

        self.assertEquals(fileInfo['/bin/setuid'].inode.perms(), 04755)
        self.assertEquals(fileInfo['/bin/setgid'].inode.perms(), 02755)
        self.assertEquals(fileInfo['/bin/new'].inode.perms(), 07555)

        runtimeTrv2 = self.buildDerivation('foo', srcVersion,
               "r.SetModes('/bin/setuid', 0755)",
               "r.SetModes('/bin/setgid', 0755)",
               returnTrove = ':runtime' )
        fileInfo2 = self.getFiles(runtimeTrv2)
        self.assertEquals(fileInfo2['/bin/setuid'].inode.perms(), 0755)
        self.assertEquals(fileInfo2['/bin/setgid'].inode.perms(), 0755)

    @testhelp.context('initialcontents')
    def testConfig(self):
        srcVersion  = self.addCompAndPackage('foo:runtime', '1.0-1-1',
            fileContents = [
                ( '/etc/cfg', rephelp.RegularFile() ),
                ( '/etc/cfg.ic', rephelp.RegularFile() ),
                ( '/etc/cfg.t', rephelp.RegularFile() ),
            ] )
        trv = self.buildDerivation('foo:runtime', srcVersion,
               "r.Create('/etc/alsoconfig')",
               "r.Create('/etc/notaconfig')",
               "r.Create('/usr/share/transient')",
               "r.Transient('/usr/share/transient')",
               "r.InitialContents('/etc/cfg.ic')",
               "r.Transient('/etc/cfg.t')",
               "r.Config(exceptions = '/etc/(notaconfig|cfg.(ic|t))')",
               "r.ComponentSpec('runtime', '.*')")

        assert(len(list(trv.iterFileList())) == 6)
        fileInfo = self.getFiles(trv)

        self.assertEqual(fileInfo['/etc/cfg'].flags(),
                             files._FILE_FLAG_CONFIG)
        self.assertEqual(fileInfo['/etc/cfg.ic'].flags(),
                             files._FILE_FLAG_INITIAL_CONTENTS)
        self.assertEqual(fileInfo['/etc/cfg.t'].flags(),
                             files._FILE_FLAG_TRANSIENT)
        self.assertEqual(fileInfo['/etc/alsoconfig'].flags(),
                             files._FILE_FLAG_CONFIG)
        self.assertEqual(fileInfo['/etc/notaconfig'].flags(), 0)
        self.assertEqual(fileInfo['/usr/share/transient'].flags(),
                             files._FILE_FLAG_TRANSIENT)


    def testByDefault(self):
        srcVersion  = self.addCompsAndPackage('foo', '1.0-1-1',
              [ self.singleComp(':runtime', fileContents = [ '/bin/bar']) ,
                self.singleComp(':switchon',
                                fileContents = [ '/usr/share/switchon' ],
                                byDefault = False),
                self.singleComp(':switchoff',
                                fileContents = [ '/usr/share/switchoff' ],
                                byDefault = True),
                self.singleComp(':debuginfo',
                                fileContents = [ '/lib/debuginfo/bar'],
                                byDefault = False)
              ] )
        trv = self.buildDerivation('foo', srcVersion,
            "r.ByDefault('foo:switchon')",
            "r.ByDefault(exceptions='foo:switchoff')",
            )
        #d = dict([(x[0][0], x[1]) for x in trv.iterTroveListInfo()])
        #self.assertEqual(d['foo:runtime'], True)
        #self.assertEqual(d['foo:debuginfo'], False)
        ver = trv.getVersion()
        flv = trv.getFlavor()
        self.assertEqual(
            trv.includeTroveByDefault('foo:runtime', ver, flv), True)
        self.assertEqual(
            trv.includeTroveByDefault('foo:debuginfo', ver, flv), False)
        self.assertEqual(
            trv.includeTroveByDefault('foo:switchon', ver, flv), True)
        self.assertEqual(
            trv.includeTroveByDefault('foo:switchoff', ver, flv), False)


    def testUtilizeUserGroup(self):
        srcVersion  = self.addCompAndPackage('foo:runtime', '1.0-1-1',
                      fileContents = [
                        ('/bin/user',
                         RegularFile(requires = 'userinfo: foo',)),
                        ('/bin/group',
                         RegularFile(requires = 'groupinfo: bar',)),
                      ])

        trv = self.buildDerivation('foo:runtime', srcVersion,
            "r.Create('/bin/newuser', mode=0755)",
            "r.UtilizeUser('newuser', '/bin/newuser')",
            "r.Create('/bin/newgroup', mode=0755)",
            "r.UtilizeGroup('newgroup', '/bin/newgroup')",
        )
        self.assertEqual(
            trv.getRequires(),
            deps.parseDep('userinfo: foo userinfo: newuser'
                          ' groupinfo: bar groupinfo: newgroup'))


    def testTags(self):
        srcVersion  = self.addCompAndPackage('foo:runtime', '1.0-1-1',
                      fileContents = [
                           ('/bin/foo', rephelp.RegularFile(
                                tags = set(('foo',)))),
                           ('/bin/bar', rephelp.RegularFile(
                                tags = set(('foo', 'bar')))),
                           ('/bin/update', rephelp.RegularFile(
                                tags = set(('foo', 'bar')))),
                           '/bin/baz',
                      ])

        runtimeTrv = self.buildDerivation('foo:runtime', srcVersion,
               "r.Create('/bin/new', mode=0755)",
               "r.TagSpec('asdf', '/bin/new')",
               "r.TagSpec('asdf', '/bin/update')"
        )

        fileInfo = self.getFiles(runtimeTrv)

        self.assertEqual(list(fileInfo['/bin/foo'].tags), ['foo'])
        self.assertEqual(sorted(fileInfo['/bin/bar'].tags),
                             ['bar', 'foo'])
        self.assertEqual(list(fileInfo['/bin/baz'].tags), [])
        self.assertEqual(sorted(fileInfo['/bin/update'].tags),
                             ['asdf', 'bar', 'foo'])
        self.assertEqual(list(fileInfo['/bin/new'].tags), ['asdf'])


    def testFixDirModes(self):
        srcVersion  = self.addCompAndPackage('foo:runtime', '1.0-1-1',
                      fileContents = [ '/bin/foo' ])
        runtimeTrv = self.buildDerivation('foo:runtime', srcVersion,
               "r.Create('/lib/new')",
               "r.Run('chmod 000 %(destdir)s/lib')",
        )
        # without FixDirModes, this test is known to fail


    def testArchive(self):
        # A wing and a prayer to make sure that basic build functionality
        # works -- we don't try to re-test all the build and source
        # actions because they just aren't different for derived
        # packages the way that policy is.
        srcVersion  = self.addCompAndPackage('foo:runtime', '1.0-1-1',
                      fileContents = [ '/bin/foo' ])
        runtimeTrv = self.buildDerivation('foo:runtime', srcVersion,
               "r.addArchive('logrotate-3.7.1.tar.gz')",
               "r.Make()",
               "r.MakeInstall('MANDIR=%(mandir)s', rootVar='PREFIX')",
        )
        self.assertEqual(
            sorted([x[1] for x in runtimeTrv.iterFileList()]),
            ['/bin/foo', '/usr/sbin/logrotate'])


    def testMultipleBinaryPackages(self):
        recipe = """
class TestMultiPackage(PackageRecipe):
    name = "%(pkgname)s"
    version = "0.1"

    clearBuildReqs()

    def setup(r):
        r.Create("/usr/foo", contents="Contents for foo")
        r.Create("/usr/bar", contents="Contents for bar")
        r.Create('/usr/migrate', contents='Contents for migrate')

        r.PackageSpec("%(pkgname)s-secondary", "/usr/(bar|migrate)")
"""

        pkgname = "test-multi-package"
        repos = self.openRepository()

        os.chdir(self.workDir)
        self.newpkg(pkgname)
        os.chdir(pkgname)
        self.writeFile(pkgname + '.recipe', recipe % dict(pkgname=pkgname))
        self.addfile(pkgname + '.recipe')
        self.commit()
        # Cook twice
        self.cookFromRepository(pkgname)
        self.cookFromRepository(pkgname)

        # CNY-1423: add an extra flavor
        otherflv = deps.parseFlavor("is: x86_128")
        self.addComponent('%s:runtime' % pkgname, '0.1-1-2', otherflv,
                fileContents = [ ( '/128-bit', '128-bit content' ) ],)
        self.addCollection(pkgname, '0.1-1-2', [':runtime'],
                           defaultFlavor=otherflv)

        sver = versions.Version([ self.defLabel, versions.Revision('0.1-1')])
        flv = deps.parseFlavor('')
        srctrv = repos.getTrove(pkgname + ":source", sver, flv)

        bver = sver.copy()
        bver.incrementBuildCount()
        bver.incrementBuildCount()

        trvspec = [ (n, bver, flv) for n in [ pkgname, pkgname + ':runtime',
                    pkgname + '-secondary', pkgname + '-secondary:runtime' ] ]
        troves = repos.getTroves(trvspec, withFiles=False)
        self.assertEqual(len(troves), 4)

        # Shadow
        self.mkbranch(self.defLabel, self.shadowLabel, pkgname + ":source",
                      shadow=True)

        os.chdir(self.workDir)
        self.checkout(pkgname, self.shadowLabel.asString(),
                      dir=pkgname + '-derived')
        os.chdir(pkgname + '-derived')

        recipe = """
class TestMultiPackage(DerivedPackageRecipe):
    name = "%(pkgname)s"
    version = "0.1"

    clearBuildReqs()

    def setup(r):
        r.Create("/usr/baz", contents="Contents for baz")
        # override assignment
        r.PackageSpec('%(pkgname)s', '/usr/migrate')
"""
        self.writeFile(pkgname + '.recipe', recipe % dict(pkgname = pkgname))
        self.commit()

        self.cookFromRepository(pkgname, buildLabel=self.shadowLabel)

        sver = versions.Version([ self.defLabel, self.shadowLabel,
                                  versions.Revision('0.1-1.1')])
        bver = sver.copy()
        bver.incrementBuildCount()

        srctrv = repos.getTrove(pkgname + ":source", sver, flv)

        # Fetch :runtime, it should not contain /usr/bar or /128-bit
        trv = repos.getTrove(pkgname + ':runtime', bver, flv)
        filenames = set(x[1] for x in trv.iterFileList())

        self.assertEqual(sorted(list(filenames)),
                             ['/usr/baz', '/usr/foo', '/usr/migrate'])


    def testExcludeDirectories(self):
        # CNY-1506
        srcVersion  = self.addCompAndPackage('foo:runtime', '1.0-1-1',
                      flavor = deps.parseFlavor(''),
                      fileContents = [
                           # We keep this file
                           '/usr/lib/python2.4/foo.py',
                           # We will remove this file
                           '/usr/share/togoaway/togoaway.txt',
                           # We keep this empty directory
                           ('/usr/share/blip',
                              rephelp.Directory()),
                                     ])

        inclDir = '/usr/share/included-dir'
        runtimeTrv = self.buildDerivation('foo:runtime', srcVersion,
               r"r.Remove('/usr/share/togoaway/togoaway.txt')",
               r"r.MakeDirs('%s', component='runtime')" % inclDir,
               r"r.ExcludeDirectories(exceptions='%s')" % inclDir,)
        self.assertEqual(str(runtimeTrv.flavor()), '')
        fnames = set(x[1] for x in runtimeTrv.iterFileList())
        exp = set(['/usr/share/blip', '/usr/lib/python2.4/foo.py', inclDir])
        self.assertEqual(fnames, exp)

    def testDuplicateContentsAndLinks(self):
        srcVersion  = self.addCompAndPackage('foo:runtime', '1.0-1-1',
                      flavor = deps.parseFlavor(''),
                      fileContents = [
                           ('/bin/a1', rephelp.RegularFile(
                                       linkGroup = '\1'*16,
                                       contents = 'contents') ),
                           ('/bin/a2', rephelp.RegularFile(
                                       linkGroup = '\1'*16,
                                       contents = 'contents') ),
                           ('/bin/b1', rephelp.RegularFile(
                                       linkGroup = '\2'*16,
                                       contents = 'contents') ),
                           ('/bin/b2', rephelp.RegularFile(
                                       linkGroup = '\2'*16,
                                       contents = 'contents') ),
                       ] )

        runtimeTrv = self.buildDerivation('foo:runtime', srcVersion, 'pass')
        self.updatePkg('foo:runtime=localhost@rpl:shadow')
        assert(os.stat(self.rootDir + '/bin/a1').st_ino ==
               os.stat(self.rootDir + '/bin/a2').st_ino)

        assert(os.stat(self.rootDir + '/bin/b1').st_ino ==
               os.stat(self.rootDir + '/bin/b2').st_ino)

        assert(os.stat(self.rootDir + '/bin/a1').st_ino !=
               os.stat(self.rootDir + '/bin/b2').st_ino)

    def testDuplicateContents(self):
        # CNY-2157
        srcVersion  = self.addCompAndPackage('foo:runtime', '1.0-1-1',
                      flavor = deps.parseFlavor(''),
                      fileContents = [
                           ('/bin/a1', rephelp.RegularFile(
                                       contents = 'cont1') ),
                           ('/bin/a2', rephelp.RegularFile(
                                       contents = 'cont1') ),
                           ('/bin/b1', rephelp.RegularFile(
                                       contents = 'contents22') ),
                           ('/bin/b2', rephelp.RegularFile(
                                       contents = 'contents22') ),
                       ] )

        runtimeTrv = self.buildDerivation('foo:runtime', srcVersion, 'pass')
        self.updatePkg('foo:runtime=localhost@rpl:shadow')
        assteq = self.assertEqual
        assteq(os.stat(self.rootDir + '/bin/a1').st_size,
               os.stat(self.rootDir + '/bin/a2').st_size)

        assteq(os.stat(self.rootDir + '/bin/b1').st_size,
               os.stat(self.rootDir + '/bin/b2').st_size)

        assert(os.stat(self.rootDir + '/bin/a1').st_size !=
               os.stat(self.rootDir + '/bin/b2').st_size)


    def testDanglingSymlinks(self):
        srcVersion  = self.addCompAndPackage('foo:runtime', '1.0-1-1',
                      flavor = deps.parseFlavor(''),
                      fileContents = [
                           ('/bin/a1', rephelp.Symlink("../bloo")),
                           ('/bin/a2', rephelp.Symlink("/foo/bar/bloo")),
                      ])
        runtimeTrv = self.buildDerivation('foo:runtime', srcVersion, 'pass')

    def testRemoveNonPackageFiles(self):
        srcVersion  = self.addCompAndPackage('foo:runtime', '1.0-1-1',
                      flavor = deps.parseFlavor(''),
                      fileContents = [
                               ('/etc/init.d/blah', rephelp.RegularFile(
                                       contents = 'contents\n') ),
                               ('/etc/rc0.d/S00blah',
                               rephelp.Symlink("../init.d/blah") ),
                      ])
        runtimeTrv = self.buildDerivation('foo:runtime', srcVersion, 'pass')
        self.assertEqual(2, len([ x for x in runtimeTrv.iterFileList() ]))

    def testFlavorUnmodifiedFile(self):
        # CNY-1954
        socont = open(os.path.join(resources.get_archive(), 'java-libjava.so')).read()
        self.cfg.buildFlavor = deps.parseFlavor('is: x86_64')
        repos = self.openRepository()
        flv = deps.parseFlavor('')
        # The file we include is a 32-bit library. If the Flavor policy kicks
        # in (as part of the derived package cook), the original empty flavor
        # will be replaced with is: x86; but since the file hasn't changed, it
        # shouldn't.
        srcVersion = self.addCompAndPackage('foo:lib', '1.0-1-1',
                      flv,
                      fileContents = [
                                ('/usr/share/java/fubar',
                                    rephelp.RegularFile(contents = socont,
                                        flavor = flv))
                      ])
        runtimeTrv = self.buildDerivation('foo:lib', srcVersion, 'pass')
        pathId, path, fileId, fver = runtimeTrv.iterFileList().next()
        fileObj = repos.getFileVersion(pathId, fileId, fver)
        self.assertEqual(str(fileObj.flavor()), '')

    def testFindTroveToDerive(self):
        srcVersion = self.addCompAndPackage('foo:runtime', '1.0-1-1', 'is:x86')
        srcVersion = self.addCompAndPackage('foo:runtime', '1.0-1-1',
                                            'is:x86_64')
        self.cfg.buildFlavor = deps.parseFlavor('is: x86 x86_64')
        self.buildDerivation('foo:runtime', srcVersion, 'pass', prep=True)
        self.cfg.buildFlavor = deps.parseFlavor('')
        try:
            self.buildDerivation('foo:runtime', srcVersion, 'pass', prep=True)
            assert(0)
        except Exception, err:
            self.assertEquals(str(err), 'Could not find package to derive from for this flavor: version /localhost@rpl:linux/1.0-1-1 of foo was not found (Closest alternate flavors found: [is: x86], [is: x86_64])')

    def testDeriveFromOldPackage(self):
        srcVersion = self.addCompAndPackage('foo:runtime', '1.0-1-1',
                               flavor = deps.parseFlavor(''),
                               fileContents = [
                                    ('/bin/one', 'one\n') ] )
        self.addComponent('foo:source', '1.0-1',
                          fileContents = [ ( 'foo.recipe', 'pass\n') ] )

        self.mkbranch("1.0-1-1", '@rpl:shadow', "foo", shadow = True)

        self.addCompAndPackage('foo:runtime', '1.0-2-1',
                               flavor = deps.parseFlavor(''),
                               fileContents = [
                                    ('/bin/two', 'two\n') ] )

        os.chdir(self.workDir)
        self.checkout("foo", "@rpl:shadow")
        os.chdir('foo')
        self.writeFile('foo.recipe',
                       self.getRecipe('foo', srcVersion, [ 'pass' ]))

        self.commit()
        self.cookFromRepository('foo', buildLabel=self.shadowLabel)


    #def getRecipe(pkgName, sourceVersion, rules, parentVersion = None):

    #    newVersion  = self.addCompAndPackage('foo:runtime', '2.0-2-2',
    #                  flavor = deps.parseFlavor(''),
    #                  fileContents = [
    #                       ('/bin/two', 'two\n') ] )

    def testUserInfoGroupProvides(self):
        recipe = """
class TestMultiPackage(PackageRecipe):
    name = "%(pkgname)s"
    version = "0.1"

    clearBuildReqs()

    def setup(r):
        # this particular test is aimed at provideGroup, because this piece
        # of information is not tracked in destdir. set the value to the
        # non-default setting and observe that the correct value propogates to
        # derived binary troves.
        r.User('foo', 500, provideGroup = False)
        r.Create('/opt/junk')
"""

        pkgname = "test-user-package"
        repos = self.openRepository()

        os.chdir(self.workDir)
        self.newpkg(pkgname)
        os.chdir(pkgname)
        self.writeFile(pkgname + '.recipe', recipe % dict(pkgname=pkgname))
        self.addfile(pkgname + '.recipe')
        self.commit()
        self.cookFromRepository(pkgname)

        sver = versions.Version([ self.defLabel, versions.Revision('0.1-1')])
        flv = deps.parseFlavor('')
        srctrv = repos.getTrove(pkgname + ":source", sver, flv)

        # Shadow
        self.mkbranch(self.defLabel, self.shadowLabel, pkgname + ":source",
                      shadow=True)

        os.chdir(self.workDir)
        self.checkout(pkgname, self.shadowLabel.asString(),
                      dir=pkgname + '-derived')
        os.chdir(pkgname + '-derived')

        recipe = """
class TestMultiPackage(DerivedPackageRecipe):
    name = "%(pkgname)s"
    version = "0.1"

    clearBuildReqs()

    def setup(r):
        pass
"""
        self.writeFile(pkgname + '.recipe', recipe % dict(pkgname = pkgname))
        self.commit()

        res = self.cookFromRepository(pkgname, buildLabel=self.shadowLabel)

        nvfs = repos.findTrove(self.shadowLabel, ('info-foo:user', None, None))
        nvf = nvfs[0]
        trv = repos.getTrove(*nvf)
        self.assertEquals(trv.provides(),
                deps.ThawDependencySet('4#info-foo::user|7#foo'))
        self.assertEquals(trv.requires(), deps.ThawDependencySet('8#foo'))


    def testAdditionalRequires(self):
        phpReq = deps.ThawDependencySet( \
                '4#php-mysql::lib|4#php::lib|4#php::runtime')
        srcVersion  = self.addCompAndPackage('foo:runtime', '1.0-1-1',
                      fileContents = [ '/bin/bar',
                           '/bin/foo',
                           ('/splat/index.php', rephelp.RegularFile(
                                            contents = 'contents',
                                            perms = 0700,
                                            requires = phpReq) )] )
        def ThisGetRecipe(*args, **kwargs):
            return """
class fooRecipe(DerivedPackageRecipe):
    name = 'foo'
    version = '1.0'

    clearBuildReqs()
    def setup(r):
        r.macros.mwdir = '/splat'
        r.Requires(exceptDeps='trove: php(-mysql)?:.*')
        r.Requires('php5:lib', '%(mwdir)s/index.php')
        r.Requires('php5:runtime', '%(mwdir)s/index.php')
        r.Requires('php5-mysql:lib', '%(mwdir)s/index.php')"""
        self.mock(self, 'getRecipe', ThisGetRecipe)

        self.addComponent('php:runtime', filePrimer = 1)
        self.addComponent('php:lib', filePrimer = 2)
        self.addCollection('php', strongList = ['php:runtime', 'php:lib'])
        self.updatePkg('php')
        self.addComponent('php-mysql:lib', filePrimer = 3)
        self.addCollection('php-mysql', strongList = ['php-mysql:lib'])
        self.updatePkg('php-mysql')

        self.addComponent('php5:runtime', filePrimer = 4)
        self.addComponent('php5:lib', filePrimer = 5)
        self.addCollection('php5', strongList = ['php5:runtime', 'php5:lib'])
        self.updatePkg('php5')
        self.addComponent('php5-mysql:lib', filePrimer = 6)
        self.addCollection('php5-mysql', strongList = ['php5-mysql:lib'])
        self.updatePkg('php5-mysql')

        trv = self.buildDerivation('foo:runtime', srcVersion, "pass")
        newPhpReq = deps.ThawDependencySet( \
                '4#php5-mysql::lib|4#php5::lib|4#php5::runtime')
        self.assertEquals(trv.getRequires(), newPhpReq)


    def testRequiresExceptions(self):
        '''
        Make sure that exceptions= is honored for files inherited
        unchanged from the parent.
        '''
        phpReq = deps.ThawDependencySet( \
                '4#php-mysql::lib|4#php::lib|4#php::runtime')
        srcVersion  = self.addCompAndPackage('foo:runtime', '1.0-1-1',
                      fileContents = [ '/bin/bar',
                           '/bin/foo',
                           ('/splat/index.php', rephelp.RegularFile(
                                            contents = 'contents',
                                            perms = 0700,
                                            requires = phpReq) )] )
        def ThisGetRecipe(*args, **kwargs):
            return """
class fooRecipe(DerivedPackageRecipe):
    name = 'foo'
    version = '1.0'

    clearBuildReqs()
    def setup(r):
        r.macros.mwdir = '/splat'
        r.Requires(exceptions='%(mwdir)s/index.php')"""
        self.mock(self, 'getRecipe', ThisGetRecipe)

        trv = self.buildDerivation('foo:runtime', srcVersion, "pass")
        noPhpReq = deps.ThawDependencySet('')
        self.assertEquals(trv.getRequires(), noPhpReq)

    @conary_test.rpm
    def testCapsuleDerivationFails(self):
        pkgname = "tmpwatch"
        pkgversion = "0.1"
        rpmname = 'tmpwatch-2.9.1-1.i386.rpm'
        recipe = """
class TestRecipe(CapsuleRecipe):
    name = '%(pkgName)s'
    version = '%(pkgVersion)s'

    clearBuildReqs()

    def setup(r):
        r.addCapsule('%(rpmName)s')
""" % dict(pkgName=pkgname, pkgVersion=pkgversion, rpmName=rpmname)

        os.chdir(self.workDir)
        self.newpkg(pkgname)
        os.chdir(pkgname)
        self.writeFile(pkgname + '.recipe', recipe % dict(pkgname=pkgname))
        self.addfile(pkgname + '.recipe')
        self.discardOutput(self.commit)
        trvs = self.cookFromRepository(pkgname)
        self.assertEqual(str(trvs),
            "(('tmpwatch:rpm', '/localhost@rpl:linux/0.1-1-1', "
            "Flavor('is: x86')),)")

        # Shadow
        self.mkbranch(self.defLabel, self.shadowLabel, pkgname + ":source",
                      shadow=True)

        os.chdir(self.workDir)
        self.checkout(pkgname, self.shadowLabel.asString(),
                      dir=pkgname + '-derived')
        os.chdir(pkgname + '-derived')

        recipe = """
class TestRecipe(DerivedPackageRecipe):
    name = "tmpwatch"
    version = '0.1'

    clearBuildReqs()

    def setup(r):
        # add a new file
        r.Create("/usr/baz", contents="Contents for baz")
        # change a an existing one
        r.Create("/etc/cron.daily/tmpwatch", contents='Some Contents')
"""
        self.writeFile(pkgname + '.recipe', recipe)
        self.commit()
        self.assertRaises(builderrors.RecipeFileError,
                               self.cookFromRepository, pkgname,
                               buildLabel=self.shadowLabel)

    def testMixedFlavor(self):
        """File flavors of wrong arch shouldn't bubble up to built package

        @tests: CNY-3807
        """
        recipe = textwrap.dedent("""
            class TestFlavors(PackageRecipe):
                name = "%(pkgname)s"
                version = "0.1"

                clearBuildReqs()

                def setup(r):
                    r.addSource('sparc-libelf-0.97.so', dir='/usr/lib')
            """)
        pkgname = "test-flavors"
        repos = self.openRepository()
        os.chdir(self.workDir)
        self.newpkg(pkgname)
        os.chdir(pkgname)
        self.writeFile(pkgname + '.recipe', recipe % dict(pkgname=pkgname))
        self.addfile(pkgname + '.recipe')
        self.commit()
        built = self.cookFromRepository(pkgname)
        self.assertEquals(built[0][2], deps.Flavor())
        v = versions.VersionFromString(built[0][1])

        self.mkbranch(self.defLabel, self.shadowLabel, pkgname + ":source",
                      shadow=True)
        os.chdir(self.workDir)
        self.checkout(pkgname, self.shadowLabel.asString(),
                      dir=pkgname + '-derived')
        os.chdir(pkgname + '-derived')
        self.writeFile(pkgname + '.recipe',
                self.getRecipe(pkgname, v, ['pass']))
        self.commit()
        built = self.cookFromRepository(pkgname, buildLabel=self.shadowLabel)
        self.assertEquals(built[0][2], deps.Flavor())
