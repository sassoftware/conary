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


from conary_test import rephelp

from conary.versions import VersionFromString as VFS
from conary.build import nextversion

from conary.deps import deps

class NextVersionTest(rephelp.RepositoryHelper):
    def testNextVersions(self):
        trv = self.addComponent('foo:source', '1')
        self.addComponent('foo:run', '1')
        self.addCollection('foo', '1', [':run'])
        localVersion = VFS('/local@local:COOK/1.0-1')
        sourceList = [(trv.getVersion(), ['foo'], deps.Flavor()),
                      (localVersion, ['bar'], deps.Flavor())]
        repos = self.openRepository()
        nextVersions = nextversion.nextVersions(repos, self.openDatabase(),
                                                sourceList)
        assert(nextVersions == [VFS('/localhost@rpl:linux/1-1-2'),
                                VFS('/local@local:COOK/1.0-1-1')])

    def testNextVersionMultipleBranchesWithDepth(self):
        # this has depth 3, but the trove we're building has depth 2.
        # so we can't consider it the "latest" and just increment its source
        # count.
        self.addCollection('foo=/localhost@rpl:1//2//3/2:1-2-0.0.1', [':run'])

        repos = self.openRepository()
        sourceList = [(VFS('/localhost@rpl:2//3/1-2'), ['foo'], deps.Flavor())]
        nextVersions = nextversion.nextVersions(repos, self.openDatabase(),
                                                sourceList)
        assert(nextVersions == [VFS('/localhost@rpl:2//3/1-2-0.1')])

    def testNextVersionMultipleBranchesWithDepth2(self):
        self.addCollection('foo=/localhost@rpl:1//2//3/2:1-2-0.1', [':run'])

        repos = self.openRepository()
        sourceList = [(VFS('/localhost@rpl:2//3/1-2'), ['foo'], deps.Flavor())]
        nextVersions = nextversion.nextVersions(repos, self.openDatabase(),
                                                sourceList)
        assert(nextVersions == [VFS('/localhost@rpl:2//3/1-2-0.2')])

    def testNextVersionLatestDevelOnOtherBranch(self):
        # depth 2 but not latest
        self.addCollection('foo=/localhost@rpl:1//3/1:1-2-0.1[is:x86]', 
                           [':run'])
        # depth 3
        self.addCollection('foo=/localhost@rpl:1//2//3/2:1-2-0.0.1', [':run'])

        sourceList = [(VFS('/localhost@rpl:1//3/1-2'), ['foo'], deps.Flavor())]
        nextVersions = nextversion.nextVersions(self.openRepository(),
                                                self.openDatabase(), sourceList)
        assert(nextVersions == [VFS('/localhost@rpl:1//3/1-2-0.2')])

    def testNextVersionLatestDevelOnThisBranch(self):
        # depth 3
        self.addCollection('foo=/localhost@rpl:1//2//3/1:1-2-0.0.1', [':run'])
        # depth 2 and latest
        self.addCollection('foo=/localhost@rpl:1//3/2:1-2-0.1[is:x86]', 
                           [':run'])
        sourceList = [(VFS('/localhost@rpl:1//3/1-2'), ['foo'], deps.Flavor())]
        nextVersions = nextversion.nextVersions(self.openRepository(),
                                                self.openDatabase(), sourceList)
        assert(nextVersions == [VFS('/localhost@rpl:1//3/1-2-0.1')])
