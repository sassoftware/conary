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

class HarnessTest(rephelp.RepositoryHelper):
    def testOpenRepository1(self):
        # close all repositories
        self.servers.stopAllServers()
        # the fd leaks in this test are intentional
        self.openRepository()
        self.openRepository(1)

        repos = self.getRepositoryClient()
        foo = self.addComponent("foo:lib", "localhost@rpl:linux/1-1", repos = repos)
        bar = self.addComponent("bar:lib", "localhost1@rpl:linux/1-1", repos = repos)

        ret = repos.getTroveVersionList("localhost", { None : None })
        ret1 = repos.getTroveVersionList("localhost1", { None : None })
        self.assertEqual(ret, {foo.getName():{foo.getVersion():[foo.getFlavor()]}})
        self.assertEqual(ret1, {bar.getName():{bar.getVersion():[bar.getFlavor()]}})

    def testOpenRepository2(self):
        # this test is meant to check that repository databases get
        # correctly reset when we do not call stopRepository()
        repos = self.getRepositoryClient()
        # make sure both servers are still running
        self.assertEqual(set(repos.getUserMap()),
                             set([('localhost', 'test', 'foo'),
                                  ('localhost1', 'test', 'foo')]))
        ret = repos.getTroveVersionList("localhost", { None : None })
        ret1 = repos.getTroveVersionList("localhost1", { None : None })
        # databases should have been emptied, though
        self.assertEqual(ret.keys(), [])
        self.assertEqual(ret1.keys(), [])
        self.stopRepository(1)
        self.stopRepository()
        repos = self.getRepositoryClient()
        self.assertEqual(repos.getUserMap(), [])
