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
import tempfile
import shutil

from conary.build import recipe, policy, macros, buildinfo, lookaside
from conary_test import rephelp

class DummyRecipe(recipe.Recipe):
    # there is an awful lot of stuff to make a stub recipe.  this makes
    # me sad.
    buildRequires = []
    def __init__(self, cfg):
        recipe.Recipe.__init__(self, cfg)
        self.macros = macros.Macros()
        self.theMainDir = 'dummy-1.0'
        self.macros.builddir = tempfile.mkdtemp()
        self.macros.destdir = tempfile.mkdtemp()
        self.macros.maindir = self.theMainDir
        self.srcdirs = [ cfg.sourceSearchDir ]
        self.buildinfo = buildinfo.BuildInfo(self.macros.builddir)
        self.buildinfo.begin()
        self.laReposCache = lookaside.RepositoryCache(None, cfg=cfg)
        self.fileFinder = lookaside.FileFinder('dummy', self.laReposCache,
                                            self.srcdirs, {}, cfg.mirrorDirs)
        self.name = 'dummy'
        self.version = '1.0'
        self.explicitMainDir = False
        self._derivedFiles = {}

    def mainDir(self, *args, **kw):
        return self.theMainDir

    def sourceMap(self, *args, **kw):
        pass

    def reportExcessBuildRequires(self, *args, **kw):
        pass

    def reportMissingBuildRequires(self, *args, **kw):
        pass

    def  _getTransitiveBuildRequiresNames(self, *args, **kw):
        return set()

    def isatty(self):
        return False

    def __del__(self):
        shutil.rmtree(self.macros.builddir)
        shutil.rmtree(self.macros.destdir)

class SubtreeGlobPolicy(policy.Policy):
    invariantsubtrees = [ '/blah' ]
    def __init__(self, *args, **kw):
        self.traversed = []
        policy.Policy.__init__(self, *args, **kw)

    def doFile(self, filename):
        self.traversed.append(filename)

class PolicyTest(rephelp.RepositoryHelper):
    def testSubtreeGlob(self):
        r = DummyRecipe(self.cfg)
        for x in ('1', '2'):
            path = r.macros.destdir + '/foo-' + x
            os.mkdir(path)
            f = open(path + '/file-' + x, 'w')
            f.close()
        p = SubtreeGlobPolicy(r, subtrees=['/foo-*'])
        p.doProcess(r)
        assert(sorted(p.traversed) == ['/foo-1/file-1', '/foo-2/file-2'])

        # reset the class
        SubtreeGlobPolicy.invariantsubtrees = [ '/blah' ]
        p2 = SubtreeGlobPolicy(r, subtrees=['/foo-{1,2}'])
        assert(p2.invariantsubtrees == [ '/blah' ])
        p2.doProcess(r)
        assert(sorted(p2.traversed) == ['/foo-1/file-1', '/foo-2/file-2'])
