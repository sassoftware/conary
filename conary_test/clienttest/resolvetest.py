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

import socket

#testsuite
from conary_test import rephelp

#conary
from conary.conaryclient import update
from conary.deps import deps
from conary import conarycfg, versions

class ClientResolveTest(rephelp.RepositoryHelper):

    def testAutoResolveShouldInstallNewPackage(self):
        # if we're keeping the old component (bc its needed for deps), 
        # we should keep its package too. 
        dep1 = 'trove: prov:lib(1)'
        dep2 = 'trove: prov:lib(2)'

        self.addComponent('prov:lib', '1.0', provides=dep1)
        self.addComponent('prov:lib', '2.0', provides=dep2, filePrimer=1)
        self.addComponent('req:lib', '1.0', requires=dep1, filePrimer=2)
        self.addComponent('req:lib', '2.0', requires=dep2, filePrimer=3)

        self.addCollection('prov', '1.0', [':lib'])
        self.addCollection('prov', '2.0', [':lib'])
        self.addCollection('req', '1.0', [':lib'])
        self.addCollection('req', '2.0', [':lib'])


        self.updatePkg(['req=1.0', 'prov=1.0'])
        self.logFilter.add()
        self.checkUpdate('req=--2.0', ['req=--2.0', 'req:lib=--2.0',
                                       'prov=--2.0', 'prov:lib=2.0'],
                                       resolve=True, keepRequired = True)

    def testGroupRemovesRequiredComponent(self):
        # in this scenario, you have a component "req:runtime" that
        # requires "prov:runtime".  group-test installed "prov:runtime"
        # on the system.  When moving to a new version of "group-test"
        # that does not include "prov:runtime", we expect prov:runtime to
        # be left behind since it satisfies a dependency
        b1 = '/localhost@rpl:branch/'
        b2 = '/localhost@rpl:compat/'
        myDep = deps.parseDep('trove: prov:runtime file:/usr/bin/prov')

        # create initial components
        # we create 2 versions of req:runtime to trigger bugs related
        # to sorting on untimestamped versions.
        self.addComponent('req:runtime', '1.0-1-1', requires=myDep,
                                   filePrimer=1)
        self.addComponent('req:runtime', '1.0-1-2', 
                          requires='file:/usr/bin/prov',
                          filePrimer=2)
        self.addComponent('prov:runtime', '1.0-1-1', provides=myDep,
                                   filePrimer=3)

        self.addComponent('test:runtime', '1.0-1-1',
                                   filePrimer=4)
        # add prov:runtime and test:runtime to group-test (we have
        # test:runtime so we won't have an empty group later on)
        self.addCollection('group-test', '1.0-1-1', ['prov:runtime',
                                                              'test:runtime'])

        # install group-test and req:runtime.
        self.updatePkg(self.rootDir, 'group-test', '1.0-1-1')
        self.updatePkg(self.rootDir, 'req:runtime', '1.0-1-1')
        self.updatePkg(self.rootDir, 'req:runtime', '1.0-1-2',
                       keepExisting=True)

        # now, add the trove that provides our dep into the :compat branch
        self.addComponent('prov:runtime', b2+'1.0-1-1',
                                   provides=myDep, filePrimer=2)
        # make a group-test which only has test:runtime in it
        self.addComponent('test:runtime', b1+'1.0-1-1',
                                   filePrimer=3)
        self.addCollection('group-test', b1+'1.0-1-1',
                                    ['test:runtime'])
        # update to the group-test on the new branch
        # set the installLabelPath to include the new branch
        # and  the compat branch.  Use resolve=True to get prov:runtime from
        # the :compat branch
        self.cfg.installLabelPath = conarycfg.CfgLabelList(
                        [ versions.Label('localhost@rpl:branch'),
                          versions.Label('localhost@rpl:compat') ] )

        # this should leave prov installed
        self.logFilter.add()
        self.checkUpdate('group-test=%s1.0-1-1' % b1,
                         ['group-test=:linux--:branch',
                          'test:runtime=:linux--:branch'], resolve=True,
                         keepRequired = True)
        self.logFilter.compare('warning: keeping prov:runtime - required by at least req:runtime')
        self.logFilter.remove()

    def testGroupDoesOneThingDepsDoesAnother(self):
        # create foo:lib and group-a
        # group-a=2.0-1-1
        #  `- foo:lib=2.1-1-1
        self.addComponent('foo:lib', '2.1-1-1')
        self.addCollection('group-a', '2.1-1-1', [ 'foo:lib' ])
        # update to group-a
        self.updatePkg(self.rootDir, 'group-a')

        # group-a=1.0-1-1
        #  `- foo:lib=2.0-1-1
        self.addComponent('foo:lib', '2.0-1-1')
        self.addCollection('group-a', '2.0-1-1',
                                    [ ('foo:lib', '2.0-1-1') ])

        # create bar:runtime which requires foo:lib from 1.0-1-1
        # (which does not conflict with foo:lib 2.0-1-1)
        dep = deps.parseDep('soname: ELF32/libfoo.so.1(SysV x86)')
        self.addComponent('foo:lib', '1.0-1-1',
                                   provides=dep, filePrimer=1)
        self.addComponent('bar:runtime', '1.0-1-1',
                                   requires=dep)

        # now try to downgrade group-a and install bar:runtime with
        # dependency solving at the same time.  We should get
        # a job that updates foo:lib 2.1-1-1 to 2.0-1-1, and a
        # new install of foo:lib=1.0-1-1
        self.checkUpdate(['group-a=2.0-1-1', 'bar:runtime'],
                         ['foo:lib=2.1-1-1--2.0-1-1',
                          'foo:lib=--1.0-1-1',
                          'bar:runtime=1.0-1-1',
                          'group-a=2.1-1-1--2.0-1-1'], resolve=True)


    def testExistingDepResolution(self):
        # something which is recursively included from the update, but
        # normally wouldn't be installed, is needed to resolve a dependency
        self.addQuickTestComponent("test:runtime", '1.0-1-1')
        self.addQuickTestComponent("test:lib", '1.0-1-1', filePrimer = 1)
        self.addQuickTestCollection("test", '1.0-1-1',
                                    [ ("test:lib", '1.0-1-1'),
                                      ("test:runtime", '1.0-1-1') ])
        self.updatePkg(self.rootDir, "test")
        self.erasePkg(self.rootDir, "test:lib")
        
        self.addQuickTestComponent("test:runtime", '2.0-1-1',
                          requires = deps.parseDep('trove: test:lib'))
        self.addQuickTestComponent("test:lib", '2.0-1-1', filePrimer = 1)
        self.addQuickTestCollection("test", '2.0-1-1',
                                    [ ("test:lib", '2.0-1-1'),
                                      ("test:runtime", '2.0-1-1') ])
        (rc, str) = self.captureOutput(self.updatePkg, self.rootDir,
                                       "test", resolve = True)
        assert(str == 'Including extra troves to resolve dependencies:\n'
                      '    test:lib=2.0-1-1\n')

    def testDepResolutionWouldSwitchBranches(self):
        # we shouldn't switch branches due to dep resolution
        self.addComponent('prov:lib', '1.0')
        trv = self.addComponent('prov:lib', ':branch/2.0', 
                                provides='trove: prov:lib(2)')
        self.addComponent('req:lib', '1.0', requires='trove:prov:lib', 
                         filePrimer=2)
        self.addComponent('req:lib', '2.0', requires='trove:prov:lib(2)',
                          filePrimer=2)
        self.updatePkg(['req:lib=1.0', 'prov:lib=1.0'])
        self.cfg.installLabelPath.append(trv.getVersion().trailingLabel())

        try:
            self.checkUpdate('req:lib', ['req:lib=1.0--2.0',
                                         'prov:lib=1.0--:branch/2.0'],
                              resolve=True)
            raise RuntimeError
        except update.DepResolutionFailure:
            pass

        trv = self.addComponent('prov:lib', ':branch/2.0.1', 
                                provides='trove: prov:lib(2)',
                                filePrimer=3)
        self.checkUpdate('req:lib', ['req:lib=1.0--2.0',
                                     'prov:lib=--:branch/2.0.1'],
                         resolve=True)

    def testResolveErasureFailure(self):
        for v in '1.0', '1.1':
            self.addComponent('foo:python', v,
                              provides='trove: foo:python(%s)' % v,
                              filePrimer=1)

            self.addComponent('foo:runtime', v,
                              requires='trove: foo:python(%s)' % v,
                              filePrimer=2)

            self.addCollection('foo', v, [':python', ':runtime'])

            self.addComponent('foo-build:python', v, 
                              requires='trove: foo:python(%s)' % v,
                              filePrimer=3)
            self.addComponent('foo-build:devel', v, filePrimer=4)

            if v == '1.0':
                self.addCollection('foo-build', v, [':python', ':devel'])
            else:
                self.addCollection('foo-build', v, [':python'])

        self.updatePkg(['foo=1.0', 'foo-build=1.0'])

        self.checkUpdate(['foo=1.1'], ['foo=1.0--1.1',
                                          'foo:runtime=1.0--1.1',
                                          'foo:python=1.0--1.1',
                                          'foo-build=1.0--1.1',
                                          'foo-build:python=1.0--1.1',
                                          'foo-build:devel=1.0--',
                                          ])

    def testResolveErasureFailure2(self):
        # we are updating foo from 1 -- 2
        # bar=1 requires foo=1.  So, we attempt to update bar.
        # but bam requires bar=1.  We don't allow that sort of recursion.
        for v in '1', '2':
            self.addComponent('foo:run', v, provides='trove:foo:run(%s)' % v,
                              filePrimer=1)
            self.addComponent('bar:run', v, provides='trove:bar:run(%s)' % v,
                              requires='trove:foo:run(%s)' % v,
                              filePrimer=2)
            self.addComponent('bam:run', v, provides='trove:bam:run(%s)' % v,
                              requires='trove:bar:run(%s)' % v,
                              filePrimer=3)
            self.addCollection('foo', v, [':run'])
            self.addCollection('bar', v, [':run'])
            self.addCollection('bam', v, [':run'])

        self.updatePkg(['foo=1', 'bar=1', 'bam=1'])
        try:
            self.checkUpdate('foo', [])
        except update.EraseDepFailure, err:
            # this will give some lame message
            # about erasing bar:runtime=1 
            # cause bam:runtime=1 to fail.
            pass
        else:
            assert(0)

        v = 2
        # make a copy of bar that will install side-by-side
        self.addComponent('bar:run', '2.1', provides='trove:bar:run(%s)' % v,
                          requires='trove:foo:run(%s)' % v,
                          filePrimer=4)
        self.addCollection('bar', '2.1', [':run'])

        self.logFilter.add()
        try:
            self.checkUpdate('foo', ['foo=1--2', 'foo:run=1--2', 
                                     'bar=--2', 'bar:run=--2'],
                             keepRequired = True)
        except update.EraseDepFailure, err:
            # this gives a message about bar:runtime=1 requiring
            # foo:runtime=1, since at some time we attempt to resolve
            # a situation by leaving old bar in place and updating 
            # new bar.
            pass
        else:
            assert(0)

        self.logFilter.compare('warning: keeping bar:run - required by at least bam:run')


    def testResolveErasureNeedsResolution(self):
        # we are updating foo from 1 -- 2
        # bar=1 requires foo=1.  So, we attempt to update bar.
        # bar needs bam to be installed.
        for v in '1', '2':
            self.addComponent('foo:run', v, provides='trove:foo:run(%s)' % v,
                              filePrimer=1)
            self.addComponent('bar:run', v, provides='trove:bar:run(%s)' % v,
                      requires='trove:foo:run(%s) trove:bam:run(%s)' % (v, v),
                      filePrimer=2)
            self.addComponent('bam:run', v, provides='trove:bam:run(%s)' % v,
                              filePrimer=3)
            self.addCollection('foo', v, [':run'])
            self.addCollection('bar', v, [':run'])
            self.addCollection('bam', v, [':run'])
        self.updatePkg(['foo=1', 'bar=1', 'bam=1'])
        self.checkUpdate('foo', ['foo=1--2', 'foo:run=1--2',
                                 'bar=1--2', 'bar:run=1--2',
                                 'bam=1--2', 'bam:run=1--2'], resolve=True)

    def testResolveAgainstDownRepository(self):
        try:
            socket.gethostbyname('www.rpath.com')
        except:
            raise testhelp.SkipTestException('Test requires networking')

        trv, cs = self.Component('foo:run', requires='trove:bar:run')
        self.addComponent('bar:run')

        oldILP = self.cfg.installLabelPath
        try:
            self.cfg.installLabelPath = [versions.Label('doesnotexist@rpl:devel')] + self.cfg.installLabelPath
            self.logFilter.add()
            self.checkUpdate(['foo:run'], ['foo:run', 'bar:run'],
                             fromChangesets=[cs], resolve=True)
            if self.cfg.proxy:
                proxyPort =  self.cfg.proxy['http'].split(':')[-1][:-1]
                msg = ('warning: Could not access doesnotexist@rpl:devel'
                       ' for dependency resolution: Error occurred'
                       ' opening repository'
                       ' https://test:<PASSWD>@doesnotexist/conary/:'
                       ' Error talking to HTTP proxy localhost:%s:'
                       ' 404 (Not Found)' % proxyPort)
            else:
                msg = ('warning: Could not access doesnotexist@rpl:devel'
                       ' for dependency resolution: Error occurred'
                       ' opening repository'
                       ' https://test:<PASSWD>@doesnotexist/conary/:'
                       ' Name or service not known')
            self.logFilter.compare(msg)
        finally:
            self.cfg.installLabelPath = oldILP

    def testResolveLevel2UpdatesNew(self):
        # There's a new version of foo, but we've explicitly updated foo
        # to a broken version - we shouldn't try to override the user's
        # decision on what to update that package to.
        for v in '1', '2':
            self.addComponent('foo:run', v, requires="trove:gcc(1)")
            self.addCollection('foo', v, [':run'])
            self.addComponent('gcc:run',  v, provides="trove:gcc(%s)" % v,
                              filePrimer=1)
            self.addCollection('gcc', v, [':run'])

        self.addComponent('foo:run', '3', requires="trove:gcc(2)")
        self.addCollection('foo', '3', [':run'])

        self.updatePkg(['foo=1', 'gcc=1'])
        try:
            self.updatePkg(['foo=2', 'gcc'], raiseError=True)
        except update.EraseDepFailure, err:
            expectedStr = """\
The following dependencies would not be met after this update:

  foo:run=2-1-1 (Would be updated from 1-1-1) requires:
    trove: gcc(1)
  which is provided by:
    gcc:run=1-1-1 (Would be updated to 2-1-1)"""
            assert(str(err) == expectedStr)
        else:
            assert(0)

    def testResolveLevel2UpdatesTwoFromSameReq(self):
        # There's a new version of foo, but we've explicitly updated foo
        # to a broken version - we shouldn't try to override the user's
        # decision on what to update that package to.
        for v in '1', '2':
            self.addComponent('foo:run', v, requires="trove:gcc(%s)" % v)
            self.addCollection('foo', v, [':run'])
            self.addComponent('bar:run', v, requires="trove:gcc(%s)" % v,
                               filePrimer=1)
            self.addCollection('bar', v, [':run'])
            self.addComponent('gcc:run',  v, provides="trove:gcc(%s)" % v,
                              filePrimer=2)
            self.addCollection('gcc', v, [':run'])

        self.updatePkg(['foo=1', 'bar=1', 'gcc=1'])
        self.updatePkg(['gcc'], raiseError=True)

    def testPullInX86WhenWeHaveX86_64(self):
        # pull in an x86 flavor of a lib when the x86_64 flavor is already
        # installed
        self.addComponent('foo:lib=1[is:x86]', provides='trove:foo:lib(x86)')
        self.addComponent('foo:lib=1[is:x86_64]', provides='trove:foo:lib(x86_64)', filePrimer=1)

        self.addComponent('bar:lib', requires='trove:foo:lib(x86)')
        self.updatePkg('foo:lib[is:x86_64]')
        self.cfg.flavor = [ deps.parseFlavor('is:x86_64'),
                            deps.parseFlavor('is: x86 x86_64') ]
        self.checkUpdate('bar:lib', ['foo:lib[is:x86]', 'bar:lib'],
                         resolve=True)

    def testNeverAddAnArch(self):
        Flavor = deps.parseFlavor
        repos = self.openRepository()
        self.cfg.flavorPreferences = [ Flavor('is:x86_64'), Flavor('is:x86')]

        self.cfg.flavor = [Flavor('is: x86 x86_64')]
        self.addComponent('foo:lib=1[is:x86]')
        self.addComponent('foo:lib=2[is:x86 x86_64]',
                          provides='trove:foo:lib(2)')
        self.addComponent('bar:lib', requires='trove:foo:lib(2)')
        self.updatePkg('foo:lib=1[is:x86]')
        self.assertRaises(update.NoNewTrovesError, self.checkUpdate,
                          'foo:lib', [])
        self.assertRaises(update.DepResolutionFailure,
                          self.checkUpdate, 'bar:lib', [], resolve=True)

        # this will install side-by-side so should work
        self.addComponent('foo:lib=3[is:x86 x86_64]', 
                          provides='trove:foo:lib(2)',
                          filePrimer=3)
        self.checkUpdate('bar:lib', ['foo:lib=--3', 'bar:lib'], resolve=True)

    def testPickLatestByLabel(self):
        self.addComponent('foo:lib=/localhost@rpl:branch//linux/1:1[ssl]')
        self.addComponent('foo:lib=2:2')
        self.addComponent('bar:lib', requires='trove:foo:lib')
        self.checkUpdate('bar:lib', ['foo:lib=--2', 'bar:lib'], resolve=True)

    def testResolveFlavorPreferences(self):
        Flavor = deps.parseFlavor
        self.cfg.flavor = [Flavor('ssl is:x86 x86_64')]
        self.cfg.flavorPreferences = [Flavor('is:x86_64'), Flavor('is:x86')]
        self.addComponent('foo:lib=1-1-1[is:x86_64]')
        self.addComponent('foo:lib=2-1-1[is:x86]')
        self.addComponent('bar:lib', requires='trove:foo:lib')
        # updates to the x86_64 even though there's an x86 available.
        self.checkUpdate('bar:lib', ['foo:lib=--1', 'bar:lib'], resolve=True)

    def testResolveFailsDueToErase(self):
        self.addComponent('foo:lib', provides='trove:foo:lib(1)')
        self.addCollection('group-foo', ['foo:lib'])
        self.addComponent('bar:lib', requires='trove:foo:lib(1)', filePrimer=1)
        self.updatePkg(['group-foo', 'bar:lib'])

        self.addComponent('foo:lib=2', provides='trove:foo:lib(2)')
        self.addCollection('group-foo=2', ['foo:lib'])
        try:
            self.updatePkg(['group-foo'], raiseError=True)
        except Exception, e:
            self.assertEquals(str(e), '''\
The following dependencies would not be met after this update:

  bar:lib=1.0-1-1 (Already installed) requires:
    trove: foo:lib(1)
  which is provided by:
    foo:lib=1.0-1-1 (Would be updated to 2-1-1)''')

    def testResolveLevel3UpdatesNew(self):
        # There's a new version of foo, but we've explicitly updated foo
        # to a broken version - we shouldn't try to override the user's
        # decision on what to update that package to.
        for v in '1', '2':
            self.addComponent('foo:run', v, requires="trove:gcc(1)")
            self.addCollection('foo', v, [':run'])
            self.addComponent('gcc:run',  v, provides="trove:gcc(%s)" % v,
                              filePrimer=1)
            self.addCollection('gcc', v, [':run'])

        self.addComponent('foo:run', '3', requires="trove:gcc(2)")
        self.addCollection('foo', '3', [':run'])

        self.updatePkg(['foo=1', 'gcc=1'])
        try:
            self.updatePkg(['foo=2', 'gcc'], raiseError=True)
            assert(0)
        except update.EraseDepFailure, err:
            assert(str(err) == '''\
The following dependencies would not be met after this update:

  foo:run=2-1-1 (Would be updated from 1-1-1) requires:
    trove: gcc(1)
  which is provided by:
    gcc:run=1-1-1 (Would be updated to 2-1-1)''')
        try:
            self.cfg.fullVersions = True
            self.cfg.fullFlavors = True
            self.updatePkg(['foo=2', 'gcc'], raiseError=True)
            assert(0)
        except update.EraseDepFailure, err:
            expectedStr = '''\
The following dependencies would not be met after this update:

  foo:run=/localhost@rpl:linux/2-1-1[] (Would be updated from \
/localhost@rpl:linux/1-1-1[]) requires:
    trove: gcc(1)
  which is provided by:
    gcc:run=/localhost@rpl:linux/1-1-1[] (Would be updated to \
/localhost@rpl:linux/2-1-1[])'''
            assert(str(err) == expectedStr)

    def testResolveEncountersErased(self):
        # CNY-2996
        self.addComponent('foo:lib')
        self.addComponent('foo:devellib',
                           requires='trove: foo:lib', filePrimer=1)
        self.addComponent('foo:lib=:branch')
        self.addComponent('foo:devellib=:branch',
                          requires='trove: foo:lib', filePrimer=1)
        self.addComponent('bar:devellib',
                          requires='trove: foo:devellib trove:bam:runtime(1)',
                          filePrimer=2)
        self.addComponent('bam:runtime=1',
                          requires='trove:bam:runtime(2)',
                          provides='trove:bam:runtime(1)', filePrimer=3)
        self.addComponent('bam:runtime=2',
                          provides='trove:bam:runtime(2)', filePrimer=4)
        self.updatePkg(['foo:lib=:branch', 'foo:devellib=:branch'],
                       raiseError=True)
        err = self.assertRaises(update.EraseDepFailure,
            self.checkUpdate,
            ['-foo:lib', '-foo:devellib', 'bar:devellib'],  [], resolve=True)
        self.assertEquals(str(err),  '''\
The following dependencies would not be met after this update:

  bar:devellib=1.0-1-1 (Would be newly installed) requires:
    trove: foo:devellib
  which is provided by:
    foo:devellib=1-1-1 (Would be erased)''')
        self.addComponent('baz:devellib', requires='trove:bar:devellib',
                          filePrimer=5)

        err = self.assertRaises(update.EraseDepFailure,
            self.checkUpdate,
            ['-foo:lib', '-foo:devellib', 'baz:devellib'],  [], resolve=True)
        self.assertEquals(str(err), '''\
The following dependencies would not be met after this update:

  bar:devellib=1.0-1-1 (Would be added due to resolution) requires:
    trove: foo:devellib
  which is provided by:
    foo:devellib=1-1-1 (Would be erased)''')

    def testResatisfiedUsergroup(self):
        """
        Info dep is resatisfied during a migrate, e.g. because of splitting
        :user to :user and :group, while also being depended on by both a trove
        being updated in the same operation, and by another trove not in the
        operation.
        @tests: CNY-3685
        """
        d = 'groupinfo: nobody'
        self.addComponent('info-nobody:user', provides=d, filePrimer=1)
        self.addComponent('info-nobody:group', provides=d, filePrimer=1)
        self.addComponent('updated:runtime', '1.0', requires=d, filePrimer=2)
        self.addComponent('updated:runtime', '2.0', requires=d, filePrimer=2)
        self.addComponent('leftalone:runtime', requires=d, filePrimer=3)
        self.addCollection('group-foo', '1', [
            'info-nobody:user=1.0',
            'updated:runtime=1.0',
            'leftalone:runtime=1.0',
            ])
        self.addCollection('group-foo', '2', [
            'info-nobody:group=1.0',
            'updated:runtime=2.0',
            'leftalone:runtime=1.0',
            ])

        self.updatePkg(['group-foo=1'], raiseError=True)
        self.updatePkg(['group-foo=2'], raiseError=True)
