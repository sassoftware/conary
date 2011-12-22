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
import socket

from conary_test import recipes
from conary_test import rephelp
from conary_test import resources

from conary import versions
from conary.cmds import metadata as metadata_mod


testXML = """<trove>
    <troveName>testcase</troveName>
    <shortDesc>A metadata testcase.</shortDesc>
    <longDesc>Just a testcase for the metadata.</longDesc>
    <source>testsource</source>
    <language>C</language>
    <url>http://url1/</url>
    <license>GPL</license>
    <category>Test Category</category>
    <category>Test Category 2</category>
</trove>
"""

testSimpleXML = """<trove>
    <troveName>testcase</troveName>
    <shortDesc>A metadata testcase.</shortDesc>
</trove>
"""


class MetadataTest(rephelp.RepositoryHelper):
    def testMetadata(self):
        troveVersion = self.repo.findTrove(self.cfg.buildLabel, 
                                           ("testcase:source", None, None), 
                                           self.cfg.flavor)[0][1]
        branch = troveVersion.branch()

        shortDesc = "Short Description"
        longDesc = "Long Description"
        urls = ["http://www.url.com/"]
        licenses = ["GPL"]
        categories = ["CAT1", "CAT2"]
        
        self.repo.updateMetadata("testcase:source", branch, 
                                 shortDesc, longDesc,
                                 urls, licenses, categories,
                                 source="local", language="C")

        metadata = self.repo.getMetadata(["testcase:source", branch], branch.label())
        metadata = metadata["testcase:source"]

        assert(metadata.getShortDesc() == shortDesc)
        assert(metadata.getLongDesc() == longDesc)
        assert(metadata.getUrls() == urls)
        assert(metadata.getLicenses() == licenses)
        assert(metadata.getCategories() == categories)
        assert(metadata.getSource() == "local")
        assert(metadata.getLanguage() == "C")
        assert(metadata.getVersion() == '/localhost@rpl:linux/1-1')

        self.repo.updateMetadata("testcase:source", branch,
                                 "Translated Short Description",
                                 "Translated Long Description",
                                 language = "fr")

        metadata = self.repo.getMetadata(["testcase:source", branch], branch.label(), language="fr")
        metadata = metadata["testcase:source"]

        assert(metadata.getShortDesc() == "Translated Short Description")
        assert(metadata.getLongDesc() == "Translated Long Description")
        assert(metadata.getLanguage() == "fr")
        assert(metadata.getVersion() == '/localhost@rpl:linux/1-1')
       
        

    def testMetadataXML(self):
        troveVersion = self.repo.findTrove(self.cfg.buildLabel, 
                            ("testcase:source", None, None), 
                            self.cfg.flavor)[0][1]
        branch = troveVersion.branch()

        self.repo.updateMetadataFromXML('testcase:source', branch, testXML)

        metadata = self.repo.getMetadata(["testcase:source", branch], branch.label())
        metadata = metadata["testcase:source"]
        
        assert(metadata.getShortDesc() == "A metadata testcase.")
        assert(metadata.getLongDesc() == "Just a testcase for the metadata.")
        assert(metadata.getSource() == "testsource")
        assert(metadata.getLanguage() == "C")
        assert(metadata.getUrls() == ["http://url1/"])
        assert(metadata.getLicenses() == ["GPL"])
        assert(metadata.getCategories() == ["Test Category", "Test Category 2"])

        self.repo.updateMetadataFromXML('testcase:source', branch, testSimpleXML)
        
        metadata = self.repo.getMetadata(["testcase:source", branch], branch.label())
        metadata = metadata["testcase:source"]
        
        assert(metadata.getShortDesc() == "A metadata testcase.")
        assert(metadata.getLongDesc() == "")
        assert(metadata.getSource() == "local")
        assert(metadata.getLanguage() == "C")
        assert(metadata.getUrls() == [])
        assert(metadata.getLicenses() == [])
        assert(metadata.getCategories() == [])

    def testFollowBranch(self):
        troveVersion = self.repo.findTrove(self.cfg.buildLabel, 
                            ("testcase:source", None, None), 
                            self.cfg.flavor)[0][1]
        branch = troveVersion.branch()

        self.mkbranch(self.cfg.buildLabel, "@rpl:branch1", "testcase:source")

        self.repo.updateMetadataFromXML('testcase:source', branch, testXML)

        newLabel = versions.Label("localhost@rpl:branch1")
        troveVersion = self.repo.findTrove(newLabel, ("testcase:source", None, 
                                                      None), 
                                                      self.cfg.flavor)[0][1]
        branch1 = troveVersion.branch()

        metadata = self.repo.getMetadata(["testcase:source", branch1], newLabel)
        metadata = metadata["testcase:source"]

        assert(metadata.getShortDesc() == "A metadata testcase.")
        assert(metadata.getLongDesc() == "Just a testcase for the metadata.")
        assert(metadata.getSource() == "testsource")
        assert(metadata.getLanguage() == "C")
        assert(metadata.getUrls() == ["http://url1/"])
        assert(metadata.getLicenses() == ["GPL"])
        assert(metadata.getCategories() == ["Test Category", "Test Category 2"])

    def testFetchFreshmeat(self):
        """Tests metadata.fetchFreshmeat"""
        filename = os.path.join(resources.get_archive(), 'tora.xml')
        f = open(filename)

        try:
            m = metadata_mod.fetchFreshmeat('tora', xmlDocStream=f)
        except socket.gaierror:
            raise testhelp.SkipTestException('requires network access')
        self.failUnlessEqual(m.getShortDesc(), "A tool for administrating or developing for Oracle databases.")

    def setUp(self):
        rephelp.RepositoryHelper.setUp(self)
        self.resetRepository()
        self.resetWork()
        self.repo = self.openRepository()
        self.newpkg("testcase")
        os.chdir("testcase")
        self.writeFile('testcase.recipe', recipes.testRecipe1)
        self.addfile('testcase.recipe')
        self.commit()
    
    def tearDown(self):
        os.chdir("..")
        shutil.rmtree("testcase")
        rephelp.RepositoryHelper.tearDown(self)
