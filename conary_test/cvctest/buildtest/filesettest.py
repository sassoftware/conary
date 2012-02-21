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


import os

from conary_test import rephelp
from conary.build import errors


packageRecipe = """
class testRecipe(PackageRecipe):
    name = "test"
    version = "1.0"
    clearBuildReqs()

    def setup(self):
        self.Create("/bin/ls")
        self.Create("/bin/cat")
        self.SetModes("/bin/{ls,cat}", 0755)
        self.Create("/bin/dd")
        self.NonBinariesInBindirs(exceptions='/bin/dd')
        self.Create("/usr/bin/vi")
        self.Create("/usr/bin/vim")
        self.Create("/usr/bin/vile")
        self.Create("/usr/bin/emacs")
        self.Create("/usr/bin/pico")
        self.Create("/usr/bin/nano")
        self.NonBinariesInBindirs(exceptions='/usr/bin/')
        self.ExcludeDirectories(exceptions='/usr/bin')
"""

basicFileset = """
class basicFileset(FilesetRecipe):
    name = "fileset-test"
    version = "1.0"
    branch = "@rpl:linux"
    clearBuildRequires()

    def setup(self):
        self.addFile("/bin/ls", "test:runtime", self.branch)
        self.addFile("/usr/bin/vi", "test:runtime", self.branch)
"""

globFileset1 = """
class globFileset1(FilesetRecipe):
    name = "fileset-glob-test"
    version = "1.0"
    clearBuildRequires()

    def setup(self):
        self.addFile("/bin/*", "test:runtime", "@rpl:linux")
        self.addFile("/usr/bin/v*", "test:runtime", "@rpl:linux")
"""

globFileset2 = """
class globFileset2(FilesetRecipe):
    name = "fileset-glob-test"
    version = "1.1"
    recurse = True
    clearBuildRequires()

    def setup(self):
        self.addFile("/usr/bin", "test", "@rpl:linux", recurse = 
                     self.recurse)
"""

errorFileset = """
class errorFileset(FilesetRecipe):
    name = "fileset-error-test"
    version = "1.0"
    case = 1
    clearBuildRequires()

    def setup(self):
        if self.case == 1:
            self.addFile("/usr/bin/vi", "test:runtime")
            self.addFile("/usr/bin", "test:runtime")
        if self.case == 2:
            self.addFile("/usr/bin/vi", "test:run")
        if self.case == 3:
            self.addFile("/usr/bin/v", "test:runtime")
"""

remapFileset = """
class remapFileset(FilesetRecipe):
    name = "fileset-remap-test"
    version = "1.0"
    clearBuildRequires()

    def setup(self):
        self.addFile("/bin/*", "test:runtime", "@rpl:linux", 
                     remap = ("/bin", "/usr/local/bin"))
"""

class FilesetTest(rephelp.RepositoryHelper):

    #topDir = "/tmp/test"
    #cleanupDir = 0

    def basicTest(self):
        pkg = self.build(basicFileset, "basicFileset")
        self.verifyPackageFileList(pkg, [ "/bin/ls", "/usr/bin/vi" ])
        # make sure it provides itself
        assert(str(pkg.getProvides()) == 'trove: fileset-test')

        pkg = self.build(basicFileset, "basicFileset",
                             { "version" : "1.1", "branch" : None } )
        self.verifyPackageFileList(pkg, [ "/bin/ls", "/usr/bin/vi" ])

    def globTest(self):
        pkg = self.build(globFileset1, "globFileset1")
        fl = [ "/bin/ls", "/bin/cat", "/bin/dd", "/usr/bin/vi", 
               "/usr/bin/vile", "/usr/bin/vim"]
        self.verifyPackageFileList(pkg,  fl)
        self.updatePkg(self.rootDir, pkg.getName(), pkg.getVersion().asString())
        self.verifyInstalledFileList(self.rootDir, fl)

        pkg = self.build(globFileset2, "globFileset2")
        fl = [ "/usr/bin/emacs", "/usr/bin/nano", "/usr/bin/vi", 
               "/usr/bin/vile", "/usr/bin/vim", "/usr/bin", "/usr/bin/pico"] 
        self.verifyPackageFileList(pkg, [ "/usr/bin/emacs", "/usr/bin/nano",
                                          "/usr/bin/vi", "/usr/bin/vile",
                                          "/usr/bin/vim", "/usr/bin",
                                          "/usr/bin/pico"] )
        self.updatePkg(self.rootDir, pkg.getName(), pkg.getVersion().asString())
        fl.remove("/usr/bin")
        self.verifyInstalledFileList(self.rootDir, fl)

        pkg = self.build(globFileset2, "globFileset2", 
                         { "version" : "1.2", "recurse" : False } )
        self.verifyPackageFileList(pkg, [ "/usr/bin" ] )


    def checkFailure(self, one, two, case, msg):
        try:
            self.build(one, two, { 'case' : case } )
        except errors.RecipeFileError, e:
            self.assertFalse(str(e) != msg, "incorrect exception: %s" % str(e))
        else:
            self.fail("exception expected")

    def errorTest(self):
        self.checkFailure(errorFileset, "errorFileset", 1,
                          "/usr/bin/vi has been included multiple times")
        self.checkFailure(errorFileset, "errorFileset", 2,
              'test:run was not found on path localhost@rpl:linux')
        self.checkFailure(errorFileset, "errorFileset", 3,
                      "/usr/bin/v does not exist in version "
                      "/localhost@rpl:linux/1.0-1-1 of "
                      "test:runtime")

    def remapTest(self):
        pkg = self.build(remapFileset, "remapFileset")
        self.verifyPackageFileList(pkg, [ "/usr/local/bin/ls", 
                                          "/usr/local/bin/cat", 
                                          "/usr/local/bin/dd" ])

    def test1(self):
        self.buildRecipe(packageRecipe, "testRecipe")

        self.basicTest()
        self.globTest()
        self.errorTest()
        self.remapTest()

    def testLocalCook(self):
        self.buildRecipe(packageRecipe, "testRecipe")
        origDir = os.getcwd()
        try:
            os.chdir(self.workDir)
            self.writeFile('fileset-test.recipe', basicFileset)
            pkg = self.cookFromRepository('fileset-test.recipe')
            self.updatePkg(self.rootDir, 'fileset-test-1.0.ccs')
            self.verifyInstalledFileList(self.rootDir, ['/bin/ls', '/usr/bin/vi'])
        finally:
            os.chdir(origDir)

    def testFlavoredFileSet(self):
        # Make sure build flavor is used for filesets. (CNY-1127)
        self.addComponent('foo:run', '1', 'is:x86',
                          [('/bam', rephelp.RegularFile(flavor='is:x86', 
                                                        contents='x86\n'))])
        self.addComponent('foo:run', '1', 'is:x86_64', 
                          [('/bam', rephelp.RegularFile(flavor='is:x86_64',
                                                         contents='x86_64\n'))])
        fileSetRecipe = """\
class FilesetFoo(FilesetRecipe):
    name = 'fileset-foo'
    version = '0.1'
    clearBuildRequires()

    def setup(r):
        r.addFile('/bam', 'foo:run', '%(buildlabel)s')
"""
        self.overrideBuildFlavor('is:x86')
        pkg = self.build(fileSetRecipe, "FilesetFoo")

        self.assertTrue(str(pkg.getBuildFlavor()))

        self.overrideBuildFlavor('is:x86_64')
        pkg2 = self.build(fileSetRecipe, "FilesetFoo")
        assert(str(pkg.getFlavor()) == 'is: x86')
        self.updatePkg('fileset-foo[is:x86]')
        self.verifyFile(self.cfg.root + '/bam', 'x86\n')
        assert(str(pkg2.getFlavor()) == 'is: x86_64')
        self.updatePkg('fileset-foo[is:x86_64]')
        self.verifyFile(self.cfg.root + '/bam', 'x86_64\n')

    def testMacrosInFileSet(self):
        # Make sure build flavor is used for filesets. (CNY-1127)
        self.addComponent('foo:run', '1', 
                          [('/bam', 'foo\n')])
        fileSetRecipe = """\
class FilesetFoo(FilesetRecipe):
    name = 'fileset-foo'
    version = '0.1'
    clearBuildRequires()

    def setup(r):
        r.macros.troveName = 'foo:run'
        r.macros.fileName = '/bam'
        r.macros.newFileName = '/bar'
        r.addFile('%(fileName)s', '%(troveName)s', '%(buildlabel)s',
                  remap=[('%(fileName)s', '%(newFileName)s')])
"""
        pkg = self.build(fileSetRecipe, "FilesetFoo")
        self.updatePkg('fileset-foo')
        self.verifyFile(self.cfg.root + '/bar', 'foo\n')
