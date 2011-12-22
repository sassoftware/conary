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
        self.failUnlessEqual(ret, {foo.getName():{foo.getVersion():[foo.getFlavor()]}})
        self.failUnlessEqual(ret1, {bar.getName():{bar.getVersion():[bar.getFlavor()]}})

    def testOpenRepository2(self):
        # this test is meant to check that repository databases get
        # correctly reset when we do not call stopRepository()
        repos = self.getRepositoryClient()
        # make sure both servers are still running
        self.failUnlessEqual(set(repos.getUserMap()),
                             set([('localhost', 'test', 'foo'),
                                  ('localhost1', 'test', 'foo')]))
        ret = repos.getTroveVersionList("localhost", { None : None })
        ret1 = repos.getTroveVersionList("localhost1", { None : None })
        # databases should have been emptied, though
        self.failUnlessEqual(ret.keys(), [])
        self.failUnlessEqual(ret1.keys(), [])
        self.stopRepository(1)
        self.stopRepository()
        repos = self.getRepositoryClient()
        self.failUnlessEqual(repos.getUserMap(), [])
