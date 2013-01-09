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

from conary_test import recipes
from conary_test import rephelp

from conary import versions
from conary.build import derive


class DeriveTest(rephelp.RepositoryHelper):
    def testDerivePackage(self):
        self.addComponent('simple:source=1-1',
                          [('simple.recipe', recipes.simpleRecipe)])
        self.addComponent('simple:runtime=1-1-1', [('/foo', 'contents\n')])
        self.addCollection('simple=1-1-1', [':runtime'])
        self.addComponent('simple:source=1-2', 
                          [('simple.recipe', recipes.simpleRecipe + '\n')])

        derive.derive(self.openRepository(), self.cfg,
                      versions.Label('localhost@rpl:branch'),
                      'simple=localhost@rpl:linux',
                      checkoutDir = self.workDir + '/foo',
                      extract = True)

        recipe = open(self.workDir  + '/foo/simple.recipe').read()
        self.assertEquals(recipe, """
class SimpleRecipe(DerivedPackageRecipe):
    name = 'simple'
    version = '1'

    def setup(r):
        '''
        In this recipe, you can make modifications to the package.

        Examples:

        # This appliance has high-memory-use PHP scripts
        r.Replace('memory_limit = 8M', 'memory_limit = 32M', '/etc/php.ini')

        # This appliance uses PHP as a command interpreter but does
        # not include a web server, so remove the file that creates
        # a dependency on the web server
        r.Remove('/etc/httpd/conf.d/php.conf')

        # This appliance requires that a few binaries be replaced
        # with binaries built from a custom archive that includes
        # a Makefile that honors the DESTDIR variable for its
        # install target.
        r.addArchive('foo.tar.gz')
        r.Make()
        r.MakeInstall()

        # This appliance requires an extra configuration file
        r.Create('/etc/myconfigfile', contents='some data')
        '''
""")
        self.verifyFile(self.workDir + '/foo/_ROOT_/foo', 'contents\n')
        self.verifyFile(self.workDir + '/foo/_OLD_ROOT_/foo', 'contents\n')
        self.assertEquals(
        sorted(os.listdir(os.path.join(self.workDir, 'foo'))),
        sorted(['CONARY', '_ROOT_', 'simple.recipe', '_OLD_ROOT_']))
