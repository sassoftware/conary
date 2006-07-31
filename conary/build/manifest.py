#
# Copyright (c) 2006 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import os, re
from conary.lib import util

class Manifest:

    def __init__(self, package, recipe):

        self.recipe = recipe
        self.manifestsDir = '%s/%s/_MANIFESTS_' \
            % (util.normpath(recipe.cfg.buildPath), recipe.name)

        component = None

        if ':' in package:
            (package, component) = package.split(':')

        recipe.packages[package] = True
        i = 0
        while True:
            manifestName = '%s.%d' % (package, i)
            if manifestName not in recipe.manifests:
                break
            i += 1

        self.name = manifestName
        self.manifestFile = '%s/%s.manifest' % (self.manifestsDir, manifestName)
        recipe.manifests.add(manifestName)

        if component:
            recipe.ComponentSpec(component, self.load)
        if package:
            recipe.PackageSpec(package, self.load)

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

    def load(self):

        fileList = [ re.escape(x[:-1]) \
                     for x in open(self.manifestFile).readlines() ]

        regexp = '^('+'|'.join(fileList)+')$'
        regexp = re.compile(regexp)

        return regexp

