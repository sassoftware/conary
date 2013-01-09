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
from conary.lib import util
from conary.build.filter import PathSet

class Manifest:

    def __init__(self, package, recipe):
        self.recipe = recipe
        self.package = package
        if package is not None:
            self.prepareManifestFile()

    def prepareManifestFile(self, package=None):
        # separate from __init__ for the sake of delayed instantiation
        # where package is derived from data not available at __init__ time
        if package is None:
            package = self.package

        self.manifestsDir = '%s/%s/_MANIFESTS_' \
            % (util.normpath(self.recipe.cfg.buildPath), self.recipe.name)

        component = None

        if ':' in package:
            (package, component) = package.split(':')
        if package:
            self.recipe.packages[package] = True

        i = 0
        while True:
            manifestName = '%s.%d' % (package, i)
            if manifestName not in self.recipe.manifests:
                break
            i += 1

        self.name = manifestName
        self.manifestFile = '%s/%s.manifest' % (self.manifestsDir, manifestName)
        self.recipe.manifests.add(manifestName)

        if component:
            self.recipe.ComponentSpec(component, self.load)
        if package:
            self.recipe.PackageSpec(package, self.load)

    def walk(self, init=True):

        fileSet = set()
        destDir = self.recipe.macros.destdir

        skip=len(destDir)
        for root, dirs, files in os.walk(destDir):
            topdir = root[skip:]
            if not topdir:
                topdir = '/'
            for name in dirs+files:
                fileSet.add(os.path.join(topdir, name))

        if init:
            self.fileSet = fileSet
        else:
            self.fileSet = fileSet - self.fileSet

    def create(self):

        self.walk(init=False)

        if not os.path.exists(self.manifestsDir):
            util.mkdirChain(self.manifestsDir)

        manifest = open(self.manifestFile, 'a')
        for file in sorted(list(self.fileSet)):
            manifest.write('%s\n' % file)
        manifest.close()

    def translatePath(self, path):
        for oldPath, newPath in self.recipe._pathTranslations:
            if path == oldPath:
                path = newPath
        return path

    def load(self):
        return PathSet(self.translatePath(x[:-1])
                       for x in open(self.manifestFile).readlines())

class ExplicitManifest(Manifest):
    """This class is used when an exact effect on destdir is known.
        No walking of the destdir will be performed. Instead each path in the
        manifest must be explicitly recorded."""
    def __init__(self, package, recipe, paths = []):
        self.manifestPaths = set(paths)
        Manifest.__init__(self, package, recipe)

    def recordRelativePaths(self, paths):
        if not isinstance(paths, (list, tuple, set)):
            paths = [paths]
        self.manifestPaths.update(paths)

    def recordPaths(self, paths):
        if not isinstance(paths, (list, tuple, set)):
            paths = [paths]
        destdir = util.normpath(self.recipe.macros.destdir)
        def _removeDestDir(p):
            p = util.normpath(p)
            if p[:len(destdir)] == destdir:
                return p[len(destdir):]
            else:
                return p
        paths = [_removeDestDir(x % self.recipe.macros) for x in paths]
        self.manifestPaths.update(paths)

    def walk(self, init = False):
        self.fileSet = set(self.manifestPaths)
