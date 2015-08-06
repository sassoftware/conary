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

from conary_test import rephelp

import conary
from conary.build import errors
from conary.deps import deps
from conary.repository import changeset
from conary.local import database
from conary import trove
from conary import versions

packageRecipe = """
class testRecipe(PackageRecipe):
    name = "test"
    version = "1.0"
    clearBuildReqs()

    def setup(self):
        self.Create("/etc/file")
        self.Create("/usr/share/file")
        self.ComponentSpec('runtime', '/etc/')
        self.ComponentRequires({'data': set()})
"""

packageRecipe2 = """
class testRecipe(PackageRecipe):
    name = "test"
    version = "2.0"
    clearBuildReqs()

    def setup(self):
        self.Create("/etc/file")
        self.Create("/usr/share/file")
        self.Create("%(thisdocdir)s/foo")
        self.ComponentSpec('runtime', '/etc/')
        self.ComponentRequires({'data': set()})
"""

redirectBaseRecipe = """
class testRedirect(PackageRecipe):
    name = "redirect"
    version = "0.1"
    clearBuildReqs()

    def setup(self):
        self.Create("/etc/base")
        self.Create("/usr/share/base")
        self.ComponentSpec('runtime', '/etc/')
        self.ComponentRequires({'data': set()})
"""

def _redirectRecipe(rules):
    rulesStr = "\n        ".join( "r.addRedirect(%s)" % x for x in rules)
    return """class testRedirect(RedirectRecipe):
    name = 'redirect'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        l = "localhost@rpl:linux"
        %s
""" % rulesStr

redirectRecipe = """
class testRedirect(RedirectRecipe):
    name = 'redirect'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        l = "localhost@rpl:linux"
        r.addRedirect("test", l)
"""

redirectRemoveRecipe = """
class testRedirect(RedirectRecipe):
    name = 'redirect'
    version = '2.0'
    clearBuildReqs()

    def setup(r):
        r.addRemoveRedirect()
"""

redirectBranchRecipe = """
class testRedirect(RedirectRecipe):
    name = 'redirect'
    version = '0.0'
    clearBuildReqs()

    def setup(r):
        l = "localhost@rpl:linux"
        r.addRedirect("redirect", l)
"""


flavorRedirectRecipe = """
class testRedirect(RedirectRecipe):
    name = 'redirect'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        l = "localhost@rpl:linux"
        # this actually creates a redirect to another redirect, and that
        # redirect winds points to 'test'. gross.
        r.addRedirect("redirect", l, sourceFlavor='!readline', targetFlavor='readline')
        r.addRedirect("test", l)
"""


skipCheckRedirectRecipe = """
class testRedirect(RedirectRecipe):
    name = 'redirect'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        l = "localhost@rpl:linux"
        r.addRedirect("test", l, skipTargetMatching=True)
"""




chainedRedirectRecipe = """
class testChainedRedirect(RedirectRecipe):
    name = 'redirect-chain'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        l = "localhost@rpl:linux"
        r.addRedirect("redirect", l)
"""

loopedRedirectRecipe = """
class testRedirect(RedirectRecipe):
    name = 'redirect'
    version = '2.0'
    clearBuildReqs()

    def setup(r):
        l = "localhost@rpl:linux"
        r.addRedirect("redirect-chain", l)
"""

redirectWithPkgRecipe = """
class testRedirectWithPkg(RedirectRecipe):
    name = 'redirect'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        l = "localhost@rpl:linux"
        r.addRedirect("test", l)
        r.addRedirect("test-foo", l, fromTrove='redirect-foo')
"""

groupRecipe = """
class testGroup(GroupRecipe):
    name = "group-test"
    version = '1.0'
    clearBuildRequires()

    def setup(r):
        r.addTrove('redirect')
"""

groupRecipe2 = """
class testGroup(GroupRecipe):
    name = "group-test"
    version = '1.0'
    clearBuildRequires()

    def setup(r):
        r.addTrove('redirect')
        r.addTrove('test')
"""

groupRecipe3 = """
class testGroup(GroupRecipe):
    name = "group-test"
    version = '1.0'
    autoResolve = True
    clearBuildRequires()

    def setup(r):
        r.addTrove('redirect')
        r.addTrove('test')
"""

redirectTemplate = """
class testRedirect(RedirectRecipe):
    name = '%(name)s'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addRedirect(%(redirect)s)
"""


class RedirectTest(rephelp.RepositoryHelper):

    def _getLeaves(self, name, version, flavor):
        repos = self.openRepository()
        return repos.getTroves(repos.findTrove(self.cfg.installLabelPath,
                                (name, version, flavor),
                                None, bestFlavor=False))

    @staticmethod
    def _redirStrs(trv):
        l = []
        for (name, branch, flavor) in trv.iterRedirects():
            if flavor is None:
                l.append((name, str(branch), None))
            else:
                l.append((name, str(branch), str(flavor)))

        return l

    @testhelp.context('redirect')
    def testSimple(self):
        repos = self.openRepository()
        trv = self.buildRecipe(packageRecipe, "testRecipe")
        # building this second package recipe with more components 
        # does all sorts of bad things to the redirect alg.
        #self.buildRecipe(packageRecipe2, "testRecipe")
        built = self.buildRecipe(redirectBaseRecipe, "testRedirect")

        v, f = (versions.VersionFromString(built[0][0][1]), built[0][0][2])
        mi = self.createMetadataItem(shortDesc='redir')
        mi2 = self.createMetadataItem(shortDesc='redir:runtime')
        repos.addMetadataItems([(('redirect', v, f), mi),
                                (('redirect:runtime', v, f), mi2)])
        built, d = self.buildRecipe(redirectRecipe, "testRedirect")
        v, f = (versions.VersionFromString(built[0][1]), built[0][2])
        trv, runtimeTrv = repos.getTroves((('redirect', v, f), 
                                           ('redirect:runtime', v, f)))
        assert(not trv.isCollection())
        assert(not runtimeTrv.isCollection())
        assert(runtimeTrv.getProvides().isEmpty())
        assert(trv.getProvides().isEmpty())
        assert(trv.getMetadata()['shortDesc'] == 'redir')
        assert(runtimeTrv.getMetadata()['shortDesc'] == 'redir:runtime')

        self.updatePkg(self.cfg.root, "redirect", version = '0.1-1-1')
        assert(os.path.exists(self.cfg.root + "/etc/base"))
        assert(os.path.exists(self.cfg.root + "/usr/share/base"))
        self.updatePkg(self.cfg.root, "redirect", "localhost@rpl:linux")
        assert(os.path.exists(self.cfg.root + "/etc/file"))
        assert(os.path.exists(self.cfg.root + "/usr/share/file"))
        assert(not os.path.exists(self.cfg.root + "/etc/base"))
        assert(not os.path.exists(self.cfg.root + "/usr/share/base"))

        self.resetRoot()
        self.updatePkg(self.cfg.root, "redirect", version = '0.1-1-1')
        self.erasePkg(self.cfg.root, "redirect:data")
        assert(os.path.exists(self.cfg.root + "/etc/base"))
        assert(not os.path.exists(self.cfg.root + "/usr/share/base"))
        self.updatePkg(self.cfg.root, "redirect", "localhost@rpl:linux")
        assert(os.path.exists(self.cfg.root + "/etc/file"))
        assert(not os.path.exists(self.cfg.root + "/usr/share/file"))
        assert(not os.path.exists(self.cfg.root + "/etc/base"))
        assert(not os.path.exists(self.cfg.root + "/usr/share/base"))

        self.resetRoot()
        self.updatePkg(self.cfg.root, "redirect:runtime", version = '0.1-1-1')
        assert(os.path.exists(self.cfg.root + "/etc/base"))
        # XXX remove the "del self.ComponentRequires" lines, do all updates
        # and erases with depCheck=False, and this line spins forever in a
        # tight loop.  I have not investigated this.
        self.updatePkg(self.cfg.root, "redirect:runtime", "localhost@rpl:linux")
        assert(os.path.exists(self.cfg.root + "/etc/file"))
        assert(not os.path.exists(self.cfg.root + "/etc/base"))

        # this now cooks
        self.buildRecipe(groupRecipe, 'testGroup')
        # group2 contains the redirect target, so it should cook
        self.buildRecipe(groupRecipe2, 'testGroup')
        # group3 contains the redirect target and attempts to autoresolve
        self.buildRecipe(groupRecipe3, 'testGroup')
        self.resetRoot()
        self.updatePkg(self.rootDir, 'test:data')
        self.checkUpdate('redirect', ['test', 'test:runtime'])

    @testhelp.context('redirect')
    def testRedirectOverlap(self):
        initialRecipe = """
class testRecipe(PackageRecipe):
    name = "test"
    version = "1.0"
    clearBuildReqs()

    def setup(r):
        r.Create("/foo")
        r.Create("/bar")
        r.PackageSpec("testbar", "/bar")
"""

        mergedRecipe = """
class testRecipe(PackageRecipe):
    name = "test"
    version = "1.1"
    clearBuildReqs()

    def setup(r):
        r.Create("/foo")
        r.Create("/bar")
"""

        redirectToMergedRecipe = """
class testRedirect(RedirectRecipe):
    name = 'testbar'
    version = '0'
    clearBuildReqs()

    def setup(r):
        l = "localhost@rpl:linux"
        r.addRedirect("test", l)
"""
        self.buildRecipe(initialRecipe, "testRecipe")
        self.buildRecipe(mergedRecipe, "testRecipe")
        self.buildRecipe(redirectToMergedRecipe, "testRedirect")
        self.updatePkg(self.cfg.root, "test", version = '1.0-1-1')
        self.updatePkg(self.cfg.root, "testbar", version = '1.0-1-1')
        self.updatePkg(self.cfg.root, "testbar", version = '0-1-1')
        db = database.Database(self.cfg.root, self.cfg.dbPath)
        assert (db.iterTrovesByPath('/bar')[0].getName() == 'test:runtime')

    @testhelp.context('redirect')
    def testRedirectUpdateWithManyComponents(self):
        for v in '1.0-1-1', '2.0-1-1':
            self.addQuickTestComponent("test:runtime", v)
            self.addQuickTestComponent("test:lib", v)
            self.addQuickTestComponent("test:data", v)
            self.addQuickTestCollection("test", v,
                                    [ "test:runtime", "test:lib", "test:data"])

        self.buildRecipe(redirectBaseRecipe, "testRedirect")
        self.buildRecipe(redirectRecipe, "testRedirect")

        self.updatePkg(self.cfg.root, ["test:runtime=1.0"])
        # It might be odd to install the target of a redirect when the 
        # original component of the redirect was not installed, but it is 
        # less odd than installing a package
        # and none of its components as a response to a 'conary update'
        # command, when none of the package is installed.  
        self.checkUpdate('redirect', ['test=2.0', 
                                      'test:runtime=2.0',
                                      'test:lib', 
                                      'test:data'])

        # note if we install the package, then the fact we didn't install
        # the other components becomes relevant.
        self.updatePkg(self.cfg.root, ["test=1.0"], recurse=False)
        self.checkUpdate('redirect', ['test=2.0', 
                                      'test:runtime=2.0'])

    @testhelp.context('redirect')
    def testRemoveRedirect(self):
        self.addComponent('redirect:runtime', '0.1')
        self.addCollection('redirect', '0.1', [':runtime' ])
        self.updatePkg('redirect')

        self.buildRecipe(redirectRemoveRecipe, "testRedirect")

        self.checkUpdate('redirect', [ 'redirect=0.1--',
                                       'redirect:runtime=0.1--' ])

        self.erasePkg(self.rootDir, 'redirect', recurse=False)
        self.checkUpdate('redirect', [ 'redirect:runtime=0.1--' ])
        self.checkUpdate('redirect:runtime', [ 'redirect:runtime=0.1--' ])

    @testhelp.context('redirect')
    def testComponentDisappears(self):
        self.addQuickTestComponent("redirect:runtime", '0.1-1-1')
        self.addQuickTestComponent("redirect:lib", '0.1-1-1', filePrimer = 1)
        self.addQuickTestComponent("redirect:data", '0.1-1-1', filePrimer = 2)
        self.addQuickTestCollection("redirect", '0.1-1-1',
                                    [ "redirect:runtime",
                                      "redirect:lib", "redirect:data"])

        self.addQuickTestComponent("test:runtime", '1.0-1-1')
        self.addQuickTestComponent("test:data", '1.0-1-1', filePrimer = 2)
        self.addQuickTestCollection("test", '1.0-1-1',
                                    [ "test:runtime", "test:data"])

        built, d = self.buildRecipe(redirectRecipe, "testRedirect")
        self.updatePkg(self.cfg.root, 'redirect', version = '0.1-1-1')
        self.updatePkg(self.cfg.root, 'redirect')
        db = database.Database(self.cfg.root, self.cfg.dbPath)
        assert([ x for x in sorted(db.iterAllTroveNames()) ] == 
                                ['test', 'test:data', 'test:runtime'] )

        self.resetRoot()
        self.checkUpdate('redirect:lib=0.1-1-1', ['redirect:lib'], apply=True)
        # the redirect for redirect:lib redirects to nothing (so is an erase)
        self.checkUpdate('redirect:lib', ['redirect:lib=0.1--'])

        # test should appear and redirect:lib should
        # go away
        self.checkUpdate('redirect', ['redirect:lib=0.1--', 
                        'test=--1.0',
                        'test:runtime=--1.0',
                        'test:data=--1.0'])

    @testhelp.context('redirect')
    def testRedirectFromTrove(self):
        self.addComponent('test:runtime', '1.0')
        self.addComponent('test-foo:runtime', '1.0')
        self.addCollection('test', '1.0', [':runtime'])
        self.addCollection('test-foo', '1.0', [':runtime'])

        redirectBranch = '/%s//branch' % self.cfg.buildLabel


        self.addComponent('redirect:runtime', '%s/0.1' % redirectBranch)
        self.addComponent('redirect-foo:runtime', '%s/0.1' % redirectBranch)
        self.addCollection('redirect', '%s/0.1' % redirectBranch, [':runtime'])
        self.addCollection('redirect-foo', '%s/0.1' % redirectBranch, 
                           [':runtime'])

        v1 = versions.VersionFromString('/%s//branch/1.0-1' % 
                                                self.cfg.buildLabel)
        built, d = self.buildRecipe(redirectWithPkgRecipe, 
                    "testRedirectWithPkg", sourceVersion=v1)
        self.checkUpdate(['redirect=:branch', 'redirect-foo=:branch'], 
                         ['test=--1.0',
                          'test:runtime=--1.0',
                          'test-foo=--1.0',
                          'test-foo:runtime=--1.0'])

        # make sure you can't shadow a redirect
        try:
            self.mkbranch("/localhost@rpl:linux//branch/1.0-1-0.1",
                          versions.Label("localhost@foo:bar"),
                          "redirect", shadow = True)
        except conary.errors.ShadowRedirect, e:
            assert(str(e) == 'cannot create a shadow of redirect=/localhost@rpl:linux//branch/1.0-1-0.1[] because it is a redirect')
        else:
            assert(0)

    @testhelp.context('redirect')
    def testDoubleRedirect(self):
        self.addComponent('test:runtime', '1.0')
        self.addCollection('test', '1.0', [':runtime' ])
        self.addComponent('redirect:runtime', '0.1')
        self.addCollection('redirect', '0.1', [':runtime' ])
        self.addComponent('redirect-chain:runtime', '0.1')
        self.addCollection('redirect-chain', '0.1', [':runtime' ])

        self.updatePkg(self.rootDir, "redirect-chain")

        self.buildRecipe(redirectRecipe, "testRedirect")
        self.buildRecipe(chainedRedirectRecipe, "testChainedRedirect")

        self.checkUpdate('redirect-chain',
                         ['test=--1.0',
                          'test:runtime=--1.0',
                          'redirect-chain=0.1--',
                          'redirect-chain:runtime=0.1--' ])

        self.buildRecipe(loopedRedirectRecipe, "testRedirect")
        rc = self.logCheck(self.updatePkg, (self.rootDir, "redirect-chain"),
                           "error: Redirect loop found which includes troves "
                           "redirect, redirect-chain")

    @testhelp.context('redirect')
    def testUpdateRedirectAndTarget(self):
        
        for v in '1.0-1-1', '2.0-1-1':
            self.addQuickTestComponent("test:runtime", v)
            self.addQuickTestComponent("test:lib", v)
            self.addQuickTestComponent("test:data", v)
            self.addQuickTestCollection("test", v,
                                    [ "test:runtime", "test:lib", "test:data"])

        self.buildRecipe(redirectBaseRecipe, "testRedirect")
        self.buildRecipe(redirectRecipe, "testRedirect")
        self.updatePkg("redirect:runtime=0.1-1-1")
        self.checkUpdate(["redirect:runtime", "test:runtime"], 
                         ['test:runtime=2.0',
                          'redirect:runtime=0.1--'])
        self.checkUpdate(["redirect:runtime", "test:runtime=--2.0"], 
                         ['test:runtime=2.0',
                          'redirect:runtime=0.1--'])

    @testhelp.context('redirect')
    def testRedirectWithMultipleTargetFlavors(self):
        for version, flavor in (('1.0', '~readline'), 
                                ('2.0', '~!readline,ssl')):
            self.addComponent('test:runtime', version, flavor)
            self.addCollection("test", version, [':runtime'], 
                                defaultFlavor=flavor)

        self.buildRecipe(redirectBaseRecipe, "testRedirect")
        self.buildRecipe(redirectRecipe, "testRedirect")

    @testhelp.context('redirect')
    def testRedirectWithMultipleSourceFlavors(self):
        for version, flavor in (('1.0', 'readline'), 
                                ('2.0', '!readline'),
                                ('3.0', '~!readline')):
            self.addComponent('redirect:runtime', version, flavor)
            self.addCollection("redirect", version, [':runtime'],
                                defaultFlavor=flavor)

        self.addComponent('test:runtime', '1.0', 'readline')
        self.addCollection('test', '1.0', [':runtime'],
                           defaultFlavor='readline')

        # This builds redirects from the 'redirect' trove to 'test' for each
        # flavor which has a compatible flavor to redirect to. This means
        # we can build redirects from redirect[~!readline] and
        # redirect[readline] to test[readline]. redirect[!readline] cannot
        # redirect to test[readline] though (as the flavors are incompatible).
        # This means two redirects get built, and we check to make sure the
        # latest redirect[!readline] is still the original trove.
        self.buildRecipe(redirectRecipe, "testRedirect")

        trvs = self._getLeaves('redirect', None, None)
        assert(len(trvs) == 3)

        norl = [ x for x in trvs if str(x.getFlavor()) == '!readline']
        others = [ x for x in trvs if str(x.getFlavor()) != '!readline']

        assert(len(norl) == 1)
        assert(not norl[0].isRedirect())
        assert(str(norl[0].getVersion().trailingRevision()) == '2.0-1-1')

        assert(len(others) == 2)
        for trv in others:
            # the redirect overrides the previous 1.0 version; it's -1 because
            # we didn't use a :source component to generate the original 1.0
            # package
            assert(trv.isRedirect())
            assert(str(trv.getVersion().trailingRevision()) == '1.0-1-2')

        self.buildRecipe(flavorRedirectRecipe, "testRedirect")

        trvs = self._getLeaves('redirect', None, None)
        trvsByFlavor = dict( (str(x.getFlavor()), self._redirStrs(x) )
                                for x in trvs )
        # redirect[!readline] redirecting to redirect[readline] is how the
        # recipe is written
        assert(trvsByFlavor ==
           { 'readline':   [('test',     '/localhost@rpl:linux', None)],
             '~!readline': [('test',     '/localhost@rpl:linux', None)],
             '!readline':  [('redirect', '/localhost@rpl:linux', 'readline')] })

        trvs = self._getLeaves('redirect:runtime', None, None)
        trvsByFlavor = dict( (str(x.getFlavor()), self._redirStrs(x) )
                                for x in trvs )
        assert(trvsByFlavor ==
           { 'readline':   [('test:runtime', '/localhost@rpl:linux', None)],
             '~!readline': [('test:runtime', '/localhost@rpl:linux', None)],
             '!readline':  [('redirect:runtime', '/localhost@rpl:linux',
                                                                 'readline')] })

        self.checkUpdate('redirect[!readline]', 
                         ['test[readline]', 'test:runtime[readline]'])

    @testhelp.context('redirect')
    def testRedirectWithSkipTargetMatching(self):
        self.addComponent('redirect:runtime', '1.0')
        self.addCollection('redirect', '1.0', [':runtime'])
        self.addComponent('test:runtime', '1.0', 'foo')
        self.addCollection('test', '1.0', [':runtime'], 
                            defaultFlavor='foo')
        try:
           self.buildRecipe(redirectRecipe, 'testRedirect')
        except errors.CookError, msg:
            assert(str(msg) ==
                    'Could not find target with satisfying flavor for '
                    'redirect redirect - either create a redirect with '
                    'targetFlavor and sourceFlavor set, or create a redirect '
                    'with skipTargetMatching = True')
            pass
        else:
            assert(0)

        self.buildRecipe(skipCheckRedirectRecipe, "testRedirect")

        try:
            self.checkUpdate('redirect', ['test[foo]', 'test:runtime[foo]'])
        except Exception, err:
            # FIXME: We tried to follow a redirect, but failed.
            # should we give a better error in this case?
            assert(str(err) == 'test was not found on path localhost@rpl:linux (Closest alternate flavors found: [~foo])')
        else:
            assert(0)

        self.cfg.flavor[0] = deps.overrideFlavor(self.cfg.flavor[0],
                                                 deps.parseFlavor('foo'))
        self.checkUpdate('redirect', 
                         ['test[foo]', 'test:runtime[foo]'])


    @testhelp.context('redirect')
    def testRedirectWithFewerComponents(self):
        # we've installed a version of trove redirect with :runtime and :data
        # a redirect has been built from the trove "redirect" to "test",
        # but only the :runtime component is redirected, because the latest
        # version of the "redirect" trove only has one component!
        # therefore, currently, the redirect:data component gets left behind.k
        self.addComponent('test:runtime', '1')
        self.addCollection('test', '1', [':runtime'])

        self.addComponent('redirect:runtime', '1')
        self.addComponent('redirect:data', '1', filePrimer=1)
        self.addCollection('redirect', '1', [':runtime', ':data'])

        self.addComponent('redirect:runtime', '2')
        self.addCollection('redirect', '2', [':runtime'])

        self.updatePkg('redirect=1')

        self.buildRecipe(redirectRecipe, 'testRedirect')
        self.checkUpdate('redirect', ['test', 'test:runtime', 
                                      'redirect=1--', 
                                      'redirect:runtime=1--',
                                      'redirect:data=1--'])

    @testhelp.context('redirect')
    def testRedirectErrors(self):
        redirectTemplate = """
class testRedirect(RedirectRecipe):
    name = '%(name)s'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addRedirect(%(redirect)s)
        %(extra)s
"""

        for v in ('/localhost@rpl:branch1//linux/1.0-1-1',
                  '/localhost@rpl:branch2//linux/1.0-1-1'):
            self.addComponent('test:runtime', v)
            self.addCollection('test', v, [':runtime'])

        self.buildRecipe(redirectBaseRecipe, "testRedirect")
        try:
            self.buildRecipe(redirectRecipe, "testRedirect")
        except errors.RecipeFileError, err:
            assert(str(err) == 'Label localhost@rpl:linux matched multiple branches.')

        self.resetRepository()
        self.addComponent('test:runtime', '1.0')
        self.addCollection('group-foo', '1.0', ['test:runtime'])
        # MISSING:
        # test redirecting a group
        # test redirecting a component
        # test redirecting to a flavor that doesn't exist/invalid
        # test redirecting from a flavor that doesn't exist/invalid
        redirect = redirectTemplate % dict(name='group-foo', extra = '',
                    redirect='("test:runtime", "%s")' % self.cfg.buildLabel)

        try:
            self.buildRecipe(redirect, "testRedirect")
        except errors.CookError, msg:
            assert('groups cannot be redirected' in str(msg))


        redirect = redirectTemplate % dict(name='redirect', extra = '',
                    redirect='"test:runtime", "%s", '
                             'fromTrove="redirect-test:runtime"' \
                             % self.cfg.buildLabel)

        try:
            self.buildRecipe(redirect, "testRedirect")
        except errors.CookError, msg:
            assert('components cannot be individually redirected' in str(msg))

        redirect = redirectTemplate % dict(name='redirect', extra = '',
                    redirect='"blah", "%s"' % self.cfg.buildLabel)
        try:
            self.buildRecipe(redirect, "testRedirect")
        except errors.RecipeFileError, msg:
            assert(str(msg) == 'No troves found with name(s) redirect')

        self.addComponent('redirect:runtime', '1.0')
        self.addCollection('redirect', '1.0', [':runtime'])

        try:
            self.buildRecipe(redirect, "testRedirect")
        except errors.RecipeFileError, msg:
            assert(str(msg) == 'Trove blah does not exist')

        self.resetRepository()
        self.addComponent('test:runtime', '1.0', 'foo')
        self.addCollection('test', '1.0', [':runtime'], defaultFlavor='foo')
        self.addComponent('redirect:runtime', '1.0')
        self.addCollection('redirect', '1.0', [':runtime'])

        redirect = redirectTemplate % dict(name='redirect', extra = '',
                    redirect='"test", "%s"' % self.cfg.buildLabel)

        try:
            self.buildRecipe(redirect, "testRedirect")
        except errors.CookError, msg:
            assert(str(msg) == 'Could not find target with satisfying flavor for redirect redirect - either create a redirect with targetFlavor and sourceFlavor set, or create a redirect with skipTargetMatching = True')

        self.addComponent('test:runtime', '1.1')
        self.addComponent('test2:runtime', '1.1')
        self.addCollection('test', '1.1', [':runtime'])
        self.addCollection('test2', '1.1', [':runtime'])

        redirect = redirectTemplate % dict(name='redirect',
                redirect='"test", "%s"' % self.cfg.buildLabel,
                extra = 'r.addRedirect("test2", "%s")' % self.cfg.buildLabel
            )

        try:
            self.buildRecipe(redirect, "testRedirect")
        except errors.RecipeFileError, err:
            assert(str(err) == 'Multiple redirect targets specified '
                               'from trove redirect[]')

    @testhelp.context('redirect')
    def testRedirectToFile(self):
        repos = self.openRepository()
        os.chdir(self.workDir)
        self.addComponent('redirect:runtime', '1.0')
        self.addCollection('redirect', '1.0', [':runtime'])
        self.addComponent('test:runtime', '1.0')
        self.addCollection('test', '1.0', [':runtime'])
        self.writeFile('redirect.recipe', redirectRecipe)
        built, str = self.captureOutput(self.cookItem, repos, self.cfg,
                                        'redirect.recipe')
        cs = changeset.ChangeSetFromFile('redirect-1.0.ccs')
        assert(len(cs.getPrimaryTroveList()) == 1)

    @testhelp.context('redirect')
    def testRedirectSwitchesBranchNotName(self):
        # when a redirect switches branches but not name, we don't
        # need to remove it from the job as an erase
        self.addComponent('redirect:runtime', '1.0') # target of redirect
        self.addCollection('redirect', '1.0', [':runtime']) 
                                                    # target of redirect

        # source of redirect
        redirectBranch = '/localhost@rpl:branch'
        self.addComponent('redirect:runtime', '%s/1.0' % redirectBranch)
        self.addCollection('redirect', '%s/1.0' % redirectBranch, [':runtime']) 

        # actual redirect = redirect=:branch -> redirect=:linux
        oldBuildLabel = self.cfg.buildLabel
        self.cfg.buildLabel = versions.Label('localhost@rpl:branch')
        tst = self.buildRecipe(redirectBranchRecipe, "testRedirect")
        self.cfg.buildLabel = oldBuildLabel

        self.updatePkg('redirect=:branch/1.0')
        self.checkUpdate(['redirect'], ['redirect=:branch--:linux',
                                        'redirect:runtime=:branch--:linux'])

        self.addCollection('group-redirect', '1.0', ['redirect'])
        self.updatePkg('group-redirect', recurse=False)
        self.checkUpdate(['redirect'], ['redirect=:branch--:linux',
                                        'redirect:runtime=:branch--:linux'])

    @testhelp.context('redirect')
    def testGroupRedirects(self):
        redirectGroupTemplate = """
class testRedirect(RedirectRecipe):
    name = 'group-foo'
    version = '2'
    clearBuildReqs()

    def setup(r):
        r.addRedirect(%(redirect)s)
"""

        self.addComponent('foo:lib', '1', filePrimer=1)
        self.addComponent('foo:debuginfo', '1', filePrimer=2)
        self.addCollection('foo', '1', [':lib', (':debuginfo', False)])
        self.addComponent('bam:lib', '1', filePrimer=3)
        self.addComponent('bam:debuginfo', '1', filePrimer=4)
        self.addCollection('bam', '1', [':lib', (':debuginfo', False)])

        self.addCollection('group-foo', '1', ['foo', 'bam'])

        self.addComponent('foo:lib', '2', filePrimer=6)
        self.addComponent('foo:debuginfo', '2', filePrimer=7)
        self.addCollection('foo', '2', [':lib', (':debuginfo', False)])
        self.addComponent('bar:lib', '2', filePrimer=8)
        self.addComponent('bar:debuginfo', '2', filePrimer=9)
        self.addCollection('bar', '2', [':lib', (':debuginfo', False)])
        self.addCollection('group-bar', '2', ['bar', 'foo'])

        redirect='"group-bar", "/%s"' % self.cfg.buildLabel

        self.makeSourceTrove('group-foo',
                              redirectGroupTemplate % dict(redirect=redirect))
        repos = self.openRepository()
        n,v,f = self.cookItem(repos, self.cfg, 'group-foo')[0][0]
        v = versions.VersionFromString(v)
        trv = repos.getTrove(n,v,f)
        assert(trv.troveInfo.troveVersion() == trove.TROVE_VERSION_1_1)

        self.updatePkg('group-foo=1')
        self.checkUpdate('group-foo', 
                        ['group-foo=1--', 'group-bar=--2',
                         'bar=--2', 'bar:lib=--2', 'foo=1--2', 'foo:lib=1--2',
                         'bam=1--', 'bam:lib=1--'])

    @testhelp.context('redirect')
    def testRedirectFlavorConflicts(self):
        for version, flavor in (('1.0', '!readline'), 
                                ('1.0', 'readline')):
            self.addComponent('redirect:runtime', version, flavor)
            self.addCollection("redirect", version, [':runtime'],
                                defaultFlavor=flavor)

        self.addComponent('test:runtime', '1.0', 'readline')
        self.addCollection('test', '1.0', [':runtime' ],
                           defaultFlavor = 'readline')

        # this should redirect redirect[!readline] to test[readline] and
        # redirect[readline] to test (unflavored)
        self.buildRecipe(_redirectRecipe([
           '"test", l, sourceFlavor="!readline", targetFlavor="readline"',
           '"test", l' ]), 'testRedirect' )

        trvs = self._getLeaves('redirect', None, None)
        trvsByFlavor = dict( (str(x.getFlavor()), self._redirStrs(x) )
                                for x in trvs )

        assert(trvsByFlavor ==
           { 'readline':   [('test', '/localhost@rpl:linux', None )],
             '!readline':  [('test', '/localhost@rpl:linux', 'readline')] })

    @testhelp.context('redirect')
    def testRedirectOnBranch(self):
        # CNY-1181
        # create test and test-foo on localhost@rpl:linux
        self.addComponent('test:source', '1.0')
        self.addComponent('test:runtime', '1.0')
        self.addComponent('test-foo:runtime', '1.0')
        self.addCollection('test', '1.0', [':runtime'])
        self.addCollection('test-foo', '1.0', [':runtime'])

        # branch test to /localhost@rpl:linux/:branch
        self.mkbranch("/localhost@rpl:linux",
                      versions.Label("localhost@foo:branch"),
                      "test")
        b = '/localhost@rpl:linux/1.0-1-0/branch/2.0-1-1'
        # add a new version
        self.addComponent('test:runtime', b)
        self.addCollection('test', b, [':runtime'])

        r = """
class testRedirect(RedirectRecipe):
    name = 'test'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addRedirect("test-foo", "localhost@rpl:linux")
"""
        # use the source version that would match :source on the branch
        v = versions.ThawVersion('/localhost@rpl:linux/123.0:1.0-1/branch/456.0:2.0-1')
        built, str = self.captureOutput(self.buildRecipe, r, 'testRedirect',
                                        sourceVersion=v)

        self.checkUpdate(['test=:branch'],
                         ['test-foo=--1.0',
                          'test-foo:runtime=--1.0'])

    @testhelp.context('redirect')
    def testIncompleteRedirects(self):
        self.logFilter.add()
        self.setTroveVersion(10)
        self.addComponent('b:runtime', '1.0')
        self.addComponent('a:runtime', '1.0', filePrimer=1)
        self.addComponent('a:runtime', '2.0', redirect=('b:runtime',))
        self.addComponent('b:runtime', '2.0', filePrimer=2)
        db = self.openDatabase()

        # install an incomplete version of b
        self.setTroveVersion(1)
        self.updatePkg('b:runtime=1.0')
        tup = db.findTrove(None, ('b:runtime', None, None))[0]
        assert(db.troveIsIncomplete(*tup))

        self.setTroveVersion(10)
        # install a complete version of a and then update
        self.updatePkg('a:runtime=1.0')
        # be should be moved from incomplete to complete as a part of
        # this redirect update.
        self.updatePkg('a:runtime')
        tup = db.findTrove(None, ('b:runtime', None, None))[0]
        assert(not db.troveIsIncomplete(*tup))
        assert(str(tup[1]) == '/localhost@rpl:linux/2.0-1-1')

    @testhelp.context('redirect')
    def testMultiTargetRedirect(self):
        for i, name in enumerate(('redirect', 'test1', 'test2')):
            self.addComponent(name + ':runtime', '1.0', filePrimer = i)
            self.addCollection(name, '1.0', [':runtime'])

        self.buildRecipe(_redirectRecipe([
           '"test1", l',
           '"test2", l, allowMultipleTargets = True']), 'testRedirect' )

        self.updatePkg('redirect=1.0-1-1')
        self.updatePkg('redirect')

    @testhelp.context('redirect')
    def testLabelFollowing(self):
        self.addComponent('target:runtime', 'localhost@test:1//shadow/1.0')
        self.addCollection('target', 'localhost@test:1//shadow/1.0',
                           [':runtime'])

        self.addComponent('redirect:runtime', '0.1')
        self.addCollection('redirect', '0.1', [':runtime'])
        self.updatePkg('redirect=0.1')

        self.buildRecipe(_redirectRecipe([
           '"target", "localhost@test:shadow"' ]), 'testRedirect')
        self.checkUpdate('redirect=1.0',
                        [ 'target=/localhost@test:1//shadow',
                          'target:runtime=/localhost@test:1//shadow'],
                         exactMatch = False)

        self.addComponent('target:runtime', 'localhost@test:2//shadow/1.0')
        self.addCollection('target', 'localhost@test:2//shadow/1.0',
                           [':runtime'])

        self.checkUpdate('redirect=1.0',
                        [ 'target=/localhost@test:2//shadow',
                          'target:runtime=/localhost@test:2//shadow'],
                         exactMatch = False)

    @testhelp.context('redirect')
    def testRedirectOnSameLabel(self):
        foo = self.addComponent('foo:run=/localhost@rpl:linux/1-1')
        self.addComponent('foo:run=/localhost@rpl:branch//linux/1-1', 
                          redirect=[('foo:run', '/localhost@rpl:linux', None)])
        self.checkUpdate('foo:run', ['foo:run=/localhost@rpl:linux'])

    @testhelp.context('redirect')
    def testRedirectWithBuildReqs(self):
        self.addComponent('foo:runtime')
        self.updatePkg('foo:runtime')

        self.addComponent('redirect:runtime=0-0-0')
        self.addCollection('redirect', '0-0-0', [':runtime' ])

        self.addComponent('redirect:source=1-1',
                fileContents = [ ('redirect.recipe', """
class testRedirect(RedirectRecipe):
    name = 'redirect'
    version = '1.0'
    clearBuildReqs()
    buildRequires = [ 'foo:runtime' ]

    def setup(r):
        r.addRedirect("target")
""") ] )

        self.cookFromRepository('redirect')
