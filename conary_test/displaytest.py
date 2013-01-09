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


import os
import re

from conary_test import recipes
from conary_test import redirecttest
from conary_test import rephelp
from conary_test import resources

from conary import display
from conary.cmds import query
from conary.cmds import queryrep
from conary.deps import deps
from conary.deps.deps import parseFlavor as Flavor
from conary.versions import VersionFromString as VFS


class DisplayTest(rephelp.RepositoryHelper):

    def testDisplay(self):
        (built, d) = self.buildRecipe(recipes.testRecipe1, "TestRecipe1")
        pkgname, version = built[0][:2]
        self.updatePkg(self.rootDir, pkgname, version)

        db = self.openDatabase()

        # test --ls output
        # grab the (assumed) correct output from the repository
        (rc, s1) = self.captureOutput(queryrep.displayTroves, self.cfg, 
                                       [ "%s=%s" % (pkgname, version) ],
                                       ls = True)
        # compare to the database 
        (rc, s2) = self.captureOutput(query.displayTroves, db, self.cfg,
                                      ['%s=%s' %(pkgname, version)],
                                      ls=True)
        assert(s1 == s2)
        # make sure that querying by path works too
        (rc, s2) = self.captureOutput(query.displayTroves, db, self.cfg,
                                     [], pathList=['/etc/changedconfig'],
                                     ls=True)
        assert(s1 == s2)

        # path query which ends in / queries a directory
        (rc, s2) = self.captureOutput(query.displayTroves, db, self.cfg,
                                     [], pathList=['/etc/'])
        self.assertEquals(s2, 'testcase:runtime=1.0-1-1\n'
                              'testcase:runtime=1.0-1-1\n')

    def testTroveDisplayFormat(self):
        db = self.openDatabase()
        self.addDbComponent(db, 'test:runtime', '1.0', 'readline,!ssl')
        rs, s = self.captureOutput(query.displayTroves, db, self.cfg, [])
        assert(s == 'test:runtime=1.0-1-1[!ssl]\n')
        self.cfg.showLabels = True
        rs, s = self.captureOutput(query.displayTroves, db, self.cfg, [])
        assert(s == 'test:runtime=localhost@rpl:linux/1.0-1-1[!ssl]\n')
        self.cfg.fullVersions = True
        rs, s = self.captureOutput(query.displayTroves, db, self.cfg, [])
        assert(s == 'test:runtime=/localhost@rpl:linux/1.0-1-1[!ssl]\n')
        self.cfg.fullFlavors = True
        rs, s = self.captureOutput(query.displayTroves, db, self.cfg, [])
        assert(s == 'test:runtime=/localhost@rpl:linux/1.0-1-1[readline,!ssl]\n')
    def testFlavorDisplayed(self):
        repos = self.openRepository()
        db = self.openDatabase()

        foobar = Flavor('~foo,~bar')
        nofoobar = Flavor('~!foo,~!bar')
        v1 = VFS('/localhost@rpl:devel/1.0-1-1')

        dcfg = display.DisplayConfig(repos, db)
        dcfg.setTroveDisplay(baseFlavors=[Flavor('~foo')])
        formatter =  display.TroveTupFormatter(dcfg)
        formatter.prepareTuples([('foo', v1, foobar),
                                 ('foo', v1, nofoobar)])

        vStr, flv = formatter.getTupleStrings('foo', v1, foobar)
        assert(str(flv) == '~bar,~foo')

        vStr, flv = formatter.getTupleStrings('foo', v1, nofoobar)
        assert(str(flv) == '~!bar,~!foo')

    def testTroveDisplayInfo(self):
        trv = self.addComponent('test1:source=1.0-1')
        mi = self.createMetadataItem(categories=['cat1', 'cat2'],
                                     licenses=['GPL', 'GPLv3'],
                                     shortDesc='short',
                                     longDesc='long',
                                     crypto=['foo', 'foo2'],
                                     url=['url1'])


        self.addComponent('test1:run=1.0-1-1', metadata=mi, 
                          sourceName='test1:source')
        repos = self.openRepository()
        rs, s = self.captureOutput(queryrep.displayTroves, self.cfg,
                                   ['test1:run'], info=True)
        s = re.sub('Build time: .*', 'Build time: <TIME>', s)
        info = '''\
Name      : test1:run          Build time: <TIME>
Version   : 1.0-1-1            Label     : localhost@rpl:linux
Size      : 14                
Flavor    :                   
License   : GPL
License   : GPLv3
Crypto    : foo
Crypto    : foo2
Category  : cat1
Category  : cat2
Summary   : short
Url       : url1
Description: 
    long
Source    : test1:source
Change log:  ()
'''
        assert(s == info)

        # test to make sure that we can display some info if :source
        # is not available to us
        noSourceInfo = '''\
Name      : test1:run          Build time: <TIME>
Version   : 1.0-1-1            Label     : localhost@rpl:linux
Size      : 14                
Flavor    :                   
License   : GPL
License   : GPLv3
Crypto    : foo
Crypto    : foo2
Category  : cat1
Category  : cat2
Summary   : short
Url       : url1
Description: 
    long
'''
        repos.deleteUserByName(self.cfg.buildLabel, 'anonymous')
        limitedRepos = self.setupUser(repos, self.cfg.buildLabel,
                                      'limited', 'bar',
                                      '[^:]+(:(?!debuginfo$|source$).*|$)',
                                      self.cfg.buildLabel)
        self.cfg.user.addServerGlob('*', 'limited', 'bar')
        rs, s = self.captureOutput(queryrep.displayTroves, self.cfg, 
                                   ['test1:run'],
                                   info=True)
        s = re.sub('Build time: .*', 'Build time: <TIME>', s)

        self.assertEqual(s, noSourceInfo)

    def testTroveDisplayDeps(self):
        self.addComponent('foo:runtime', '1.0', requires='trove:bar:runtime')
        self.addCollection('foo', '1.0', [':runtime'])
        rs, txt = self.captureOutput(queryrep.displayTroves, self.cfg, 
                                     ['foo'],
                                     showDeps=True, 
                                     )
        assert(txt == '''\
foo=1.0-1-1
  Provides:
    trove: foo

  Requires:
     None

  foo:runtime=1.0-1-1
    Provides:
      trove: foo:runtime

    Requires:
      trove: bar:runtime

''')


    def testTroveDisplaySigs(self):
        # go through a lot of trouble to sign this trove.
        # copied from sigtest.
        self.addComponent('foo:runtime', '1.0')
        from conary.lib import openpgpfile, openpgpkey
        from conary.build import signtrove
        fingerprint = 'F7440D78FE813C882212C2BF8AC2828190B1E477'
        keyCache = openpgpkey.getKeyCache()
        keyCache.getPrivateKey(fingerprint, '111111')
        keyRing = open(resources.get_archive() + '/pubring.gpg')
        keyData = openpgpfile.exportKey(fingerprint, keyRing)
        keyData.seek(0)
        keyData = keyData.read()

        # upload the public key
        repos = self.openRepository()
        repos.addNewPGPKey(self.cfg.buildLabel, 'test', keyData)
        self.cfg.signatureKey = fingerprint
        self.cfg.quiet = True
        signtrove.signTroves(self.cfg, [ "foo:runtime"])

        rs, txt = self.captureOutput(queryrep.displayTroves, self.cfg, 
                                     ['foo:runtime'],
                                     digSigs=True)
        assert(txt.startswith('''\
foo:runtime=1.0-1-1
  Digital Signature:
      F7440D78FE813C882212C2BF8AC2828190B1E477:'''))

    def testTroveDisplayImageGroup(self):
        self.addComponent('foo:runtime', '1.0')
        self.addCollection('foo', strongList = ['foo:runtime'])
        self.addCollection('group-foo', strongList = ['foo'],
                imageGroup = True)

        timestamp = 1238075164
        import time
        timestr = time.ctime(timestamp)
        rs, txt = self.captureOutput(queryrep.displayTroves, self.cfg,
                                     ['group-foo'], info = True)
        ref = 'Name      : group-foo          Build time: %s\nVersion   : 1.0-1-1            Label     : localhost@rpl:linux\nSize      : 0                 \nFlavor    :                   \nImage Group: True\n' % timestr
        self.assertEquals(txt, ref)

    def testTroveDisplayBuildReqs(self):
        db = self.openDatabase()
        self.addDbComponent(db, 'foo:runtime', '1.0')
        self.addDbComponent(db, 'foo:runtime', ':branch/1.0')
        recipestr1 = """
class TestBuildReqs(PackageRecipe):
    name = 'bar'
    version = '1'
    clearBuildReqs()
    buildRequires = ['foo:runtime=:linux', 'foo:runtime=:branch']
    def setup(r):
        r.Create('/asdf/bar')
"""
        self.buildRecipe(recipestr1, "TestBuildReqs")
        self.cfg.showLabels = True
        (rc, txt) = self.captureOutput(queryrep.displayTroves, self.cfg, 
                                       [ "bar" ], showBuildReqs=True)
        assert(txt == '''\
foo:runtime=localhost@rpl:linux/1.0-1-1
foo:runtime=localhost@rpl:branch/1.0-1-1
''')
        self.cfg.showLabels = False
        (rc, txt) = self.captureOutput(queryrep.displayTroves, self.cfg, 
                                       [ "bar" ], showBuildReqs=True,
                                       alwaysDisplayHeaders=True)

        assert(txt == '''\
bar=1-1-1
  foo:runtime=1.0-1-1
  foo:runtime=1.0-1-1
''')

    def testChildTroveDisplayFormat(self):
        db = self.openDatabase()
        f = 'readline,!ssl'
        self.addComponent('test:runtime', '1.0', f,
                          fileContents=[('/foo', 'hello', '0.9', 
                                        deps.parseDep('soname: ELF32/libfoo.so.2(SysV x86)'))])
        self.addComponent('test:debuginfo', '1.0', f, filePrimer=2)
        self.addComponent('test:debuginfo2', '1.0', f, filePrimer=3)
        self.addCollection('test', '1.0', [(':runtime', '1.0', f),
                                           (':debuginfo', '1.0', f, False),
                                           (':debuginfo2', '1.0', f, False)])
        self.addComponent('foo:runtime', '1.0', 'ssl', filePrimer=4)

        self.addCollection( 'foo', '1.0', [('foo:runtime', '1.0', 'ssl') ])
        self.addCollection('group-dist', '1.0', 
                            [('test', '1.0', 'readline,!ssl'),
                             ('foo', '1.0', 'ssl', False)])
        self.updatePkg(['group-dist', 'test:debuginfo2[readline,!ssl]'], depCheck=False)
        rs, s = self.captureOutput(query.displayTroves, db, self.cfg,
                                   ['group-dist'], showTroves=True)
        assert(s == '''\
group-dist=1.0-1-1
  test=1.0-1-1[!ssl]
''')
        rs, s = self.captureOutput(query.displayTroves, db, self.cfg,
                                   ['group-dist'], recurse=True)
        assert(s == '''\
group-dist=1.0-1-1
  test=1.0-1-1[!ssl]
''')

        self.cfg.showComponents = True
        rs, s = self.captureOutput(query.displayTroves, db, self.cfg,
                                   ['group-dist'], recurse=True)

        assert(s == '''\
group-dist=1.0-1-1
  test=1.0-1-1[!ssl]
    test:debuginfo2=1.0-1-1[!ssl]
    test:runtime=1.0-1-1[!ssl]
''')
        self.cfg.showComponents = False

        rs, s = self.captureOutput(query.displayTroves, db, self.cfg,
                                   ['group-dist'], showAllTroves=True)
        assert(s == '''\
group-dist=1.0-1-1
  foo=1.0-1-1
  test=1.0-1-1[!ssl]
''')

        self.cfg.showComponents = True
        rs, s = self.captureOutput(query.displayTroves, db, self.cfg,
                                   ['group-dist'], showAllTroves=True,
                                   recurse=True)
        assert(s == '''\
group-dist=1.0-1-1
  foo=1.0-1-1
  test=1.0-1-1[!ssl]
    test:debuginfo=1.0-1-1[!ssl]
    test:debuginfo2=1.0-1-1[!ssl]
    test:runtime=1.0-1-1[!ssl]
''')
        rs, s = self.captureOutput(query.displayTroves, db, self.cfg,
                                   ['group-dist'], showAllTroves=True,
                                   recurse=True, showTroveFlags=True)
        assert(s == '''\
group-dist=1.0-1-1
  foo=1.0-1-1 [Missing,NotByDefault]
  test=1.0-1-1[!ssl]
    test:debuginfo=1.0-1-1[!ssl] [Missing,NotByDefault]
    test:debuginfo2=1.0-1-1[!ssl] [NotByDefault]
    test:runtime=1.0-1-1[!ssl]
''')
        rs, s = self.captureOutput(query.displayTroves, db, self.cfg,
                                   ['group-dist'], showAllTroves=True,
                                   weakRefs=True, showTroveFlags=True)
        assert(s == '''\
group-dist=1.0-1-1
  foo=1.0-1-1 [Missing,NotByDefault]
  foo:runtime=1.0-1-1 [Missing,NotByDefault,Weak]
  test=1.0-1-1[!ssl]
  test:debuginfo=1.0-1-1[!ssl] [Missing,NotByDefault,Weak]
  test:debuginfo2=1.0-1-1[!ssl] [NotByDefault,Weak]
  test:runtime=1.0-1-1[!ssl] [Weak]
''')

        # test to make sure files and file deps are indented appropriately
        # when you trove headers are on
        rs, s = self.captureOutput(query.displayTroves, db, self.cfg,
                                   ['test'], fileDeps=True, fileVersions=True,
                                   alwaysDisplayHeaders=True) 
        assert(s == '''\
test=1.0-1-1[!ssl]
  test:debuginfo2=1.0-1-1[!ssl]
    /contents3    1.0-1-1
  test:runtime=1.0-1-1[!ssl]
    /foo    0.9-1-1
      Requires:
        soname: ELF32/libfoo.so.2(SysV x86)

''')

        # test weak ref display
        rs, s = self.captureOutput(query.displayTroves, db, self.cfg,
                           ['group-dist'], weakRefs=True, showTroveFlags=True)

        assert(s == '''\
group-dist=1.0-1-1
  test=1.0-1-1[!ssl]
  test:debuginfo2=1.0-1-1[!ssl] [NotByDefault,Weak]
  test:runtime=1.0-1-1[!ssl] [Weak]
''')
        rs, s = self.captureOutput(query.displayTroves, db, self.cfg,
                           ['group-dist'], weakRefs=True, showTroveFlags=True,
                                           showAllTroves=True)
        assert(s == '''\
group-dist=1.0-1-1
  foo=1.0-1-1 [Missing,NotByDefault]
  foo:runtime=1.0-1-1 [Missing,NotByDefault,Weak]
  test=1.0-1-1[!ssl]
  test:debuginfo=1.0-1-1[!ssl] [Missing,NotByDefault,Weak]
  test:debuginfo2=1.0-1-1[!ssl] [NotByDefault,Weak]
  test:runtime=1.0-1-1[!ssl] [Weak]
''')

        rs, s = self.captureOutput(query.displayTroves, db, self.cfg, [])
        # test just --components
        assert(s == '''\
group-dist=1.0-1-1
test=1.0-1-1[!ssl]
  test:debuginfo2=1.0-1-1[!ssl]
  test:runtime=1.0-1-1[!ssl]
''')

        self.cfg.showComponents = False
        rs, s = self.captureOutput(query.displayTroves, db, self.cfg,
                                   ['test:runtime'], showAllTroves=True,
                                   recurse=True)
        assert(s == '''\
test:runtime=1.0-1-1[!ssl]
''')

    def testFileDisplay(self):
        # test file display options
        self.addComponent('foo:runtime', '1.0', '', 
                          [ ('/foo', 'contents', '0.9-2-3', 
                                     ('soname: ELF32/libm.so.6(x86)', 
                                      'soname: ELF32/libfoo.so.1(x86)')),
                            ('/bar.py', 'contents2', '0.5.8',
                                     ('python: itertools',
                                      'python: foo.bar'))])

        (rc, txt) = self.captureOutput(queryrep.displayTroves, self.cfg, 
                                       [], ls = True)
        assert(txt == '/bar.py\n/foo\n')

        (rc, txt) = self.captureOutput(queryrep.displayTroves, self.cfg, 
                                       [], lsl = True)
        lines = [ x.split() for x in txt.split('\n')]
        assert(lines[0][0] == '-rw-r--r--')
        assert(lines[0][4] == '9')
        assert(lines[0][-1] == '/bar.py')
        assert(lines[1][0] == '-rw-r--r--')
        assert(lines[1][4] == '8')
        assert(lines[1][-1] == '/foo')

        (rc, txt) = self.captureOutput(queryrep.displayTroves, self.cfg, 
                                       [], ids=True, sha1s=True,
                                       fileDeps=True, fileVersions=True)
        assert(txt == '''acb00835c9660cc43e44ceb80ad1ee8b 5e6ef1425063a0441b2cddbabac603374a983fac, 14de86e007f14bc0c6bc9a84d21cb9da908495ae /bar.py    0.5.8-1-1
  Provides:
    python: foo.bar

  Requires:
    python: itertools

1effb2475fcfba4f9e8b8a1dbc8f3caf 7d02899494ab2aabf0fe5296c776ad209ab9de3e, 4a756ca07e9487f482465a99e8286abc86ba4dc7 /foo    0.9-2-3
  Provides:
    soname: ELF32/libfoo.so.1(x86)

  Requires:
    soname: ELF32/libm.so.6(x86)

''')

    def testFileDisplayTags(self):
        firstUser1 = self.build(recipes.firstTagUserRecipe1, "FirstTagUser")
        (rc, txt) = self.captureOutput(queryrep.displayTroves, self.cfg, 
                                       [], tags=True, fileVersions=True)
        assert(txt == '''\
/etc/testfirst.1 {testtag config}    0-1-1
/etc/testfirst.2 {testtag config}    0-1-1
''')
        os.chdir(self.workDir)
        self.newpkg('autosource')
        os.chdir('autosource')
        self.writeFile('localfile', 'foo\n')
        self.addfile('localfile', text=True)
        self.writeFile('autosource.recipe', recipes.autoSource1)
        self.addfile('autosource.recipe')
        self.commit()
        (rc, txt) = self.captureOutput(queryrep.displayTroves, self.cfg,
                                       ['autosource:source'], tags=True,
                                       fileVersions=True)
        assert(txt == '''\
autosource.recipe {config}    1.0-1
distcc-2.9.tar.bz2 {autosource}    1.0-1
localfile {config}    1.0-1
''')

    def testFileDisplayFlavor(self):
        self.addComponent('foo:runtime', '1', 'is:x86', 
                          [('/foo', rephelp.RegularFile(flavor='is:x86'))])
        (rc, txt) = self.captureOutput(queryrep.displayTroves, self.cfg,
                                       ['foo:runtime'],
                                       fileFlavors=True)
        assert(txt == '/foo[is: x86]\n')


    def testRedirectAndRemoveDisplay(self):
        repos = self.openRepository()

        self.addComponent('redirect:runtime', '1.0')
        self.addCollection('redirect', '1.0', [':runtime'])
        self.addComponent('test:runtime', '1.0')
        self.addCollection('test', '1.0', [':runtime'])
        self.addComponent('foo:runtime', redirect=[])
        self.addCollection('foo', [':runtime'], redirect=[])

        firstUser1 = self.build(redirecttest.redirectRecipe, "testRedirect")

        (rc, txt) = self.captureOutput(queryrep.displayTroves, self.cfg,
                                       ['redirect'], showTroveFlags = True)
        assert(txt ==
               'redirect=1.0-1-2 [Redirect -> test=/localhost@rpl:linux]\n')
        self.markRemoved('redirect')
        (rc, txt) = self.captureOutput(queryrep.displayTroves, self.cfg,
                                       ['redirect'], showTroveFlags = True,
                                       troveTypes=repos.TROVE_QUERY_ALL)
        assert(txt == 'redirect=1.0-1-2 [Removed]\n')
        (rc, txt) = self.captureOutput(queryrep.displayTroves, self.cfg,
                                       ['foo'], showTroveFlags = True)
        assert(txt == 'foo=1.0-1-1 [Redirect -> Nothing]\n')

    def testRemovedDisplay(self):
        self.addComponent('foo:runtime', '1')
        self.addCollection('foo', '1', [':runtime'])
        self.markRemoved('foo')
        repos = self.openRepository()
        (rc, txt) = self.captureOutput(queryrep.displayTroves, self.cfg,
                                       ['foo'], showTroveFlags = True,
                                       troveTypes=repos.TROVE_QUERY_ALL)
        assert(txt == 'foo=1-1-1 [Removed]\n')

    def testRecurseDisplay(self):
        db = self.openDatabase()
        self.addComponent('foo:runtime', '1')
        self.addCollection('foo', '1', [':runtime'])
        (rc, txt) = self.captureOutput(queryrep.displayTroves, self.cfg,
                                       ['foo'], recurse = True)
        assert(txt == 'foo=1-1-1\n')

    def testJobTupFormatter(self):
        # CNY-1742
        repos = self.openRepository()
        db = self.openDatabase()
        dcfg = display.JobDisplayConfig(repos, db)
        formatter = display.JobTupFormatter(dcfg)
        v1 = VFS('/localhost@rpl:devel/1.0-1-1')
        v2 = VFS('/localhost@rpl:devel/1.0-1-2')
        f = Flavor('')
        jobs = [('foo', (None, None), (v1, f), False),
                ('foo', (None, None), (v2, f), False)]
        formatter.prepareJobs(jobs)
        result = ''.join(x for x in formatter.formatJobTups(jobs))
        self.assertEqual(result, 'Install foo=1.0-1-1Install foo=1.0-1-2')

        formatter = display.JobFormatter(dcfg)
        (rc, txt) = self.captureOutput(display.displayJobs, dcfg,
                                       formatter, jobs)
        self.assertEqual(txt, 'Install foo=1.0-1-1\nInstall foo=1.0-1-2\n')

    def testTroveDisplayInfoWithZeroSize(self):
        self.addComponent('test1:source=1.0-1')
        mi = self.createMetadataItem(categories=['cat1', 'cat2'],
                                     licenses=['GPL', 'GPLv3'],
                                     shortDesc='short',
                                     longDesc='long',
                                     crypto=['foo', 'foo2'],
                                     url=['url1'],
                                     sizeOverride=0,)

        self.addComponent('test1:run=1.0-1-1', metadata=mi,
                          sourceName='test1:source')
        self.openRepository()
        rs, s = self.captureOutput(queryrep.displayTroves, self.cfg,
                                   ['test1:run'], info=True)
        s = re.sub('Build time: .*', 'Build time: <TIME>', s)
        info = '''\
Name      : test1:run          Build time: <TIME>
Version   : 1.0-1-1            Label     : localhost@rpl:linux
Size      : 0                 
Flavor    :                   
License   : GPL
License   : GPLv3
Crypto    : foo
Crypto    : foo2
Category  : cat1
Category  : cat2
Summary   : short
Url       : url1
Description: 
    long
Source    : test1:source
Change log:  ()
'''
        assert(s == info)
