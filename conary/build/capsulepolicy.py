#
# Copyright (c) 2009 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
"""
FIXME: write doc string
"""
import codecs
import imp
import itertools
import os
import re
import site
import sre_constants
import stat
import sys

from conary import files, trove, rpmhelper
from conary.build import buildpackage, filter, policy, packagepolicy
from conary.build import tags, use
from conary.deps import deps
from conary.lib import elf, magic, util, pydeps, fixedglob, graph
from conary.local import database

class ComponentSpec(packagepolicy.ComponentSpec):
    # normal packages need Config before ComponentSpec to enable the
    # automatic :config component, but capsule packages require
    # Config to follow ComponentSpec and PackageSpec so that hardlink
    # groups in the capsule do not get marked as config files
    requires = (x for x in packagepolicy.ComponentSpec.requires
                if x[0] != 'Config')


class Config(packagepolicy.Config):
    # Descends from packagepolicy.Config to inherit _fileIsBinary and
    # requires, but is used only for files marked in the capsule as
    # a config file and therefore should have no invariants
    invariantinclusions = None
    invariantexceptions = [ ]
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR), # for hardlink detection
        ('LinkType', policy.CONDITIONAL_SUBSEQUENT),
        ('InitialContents', policy.REQUIRED_SUBSEQUENT),
    )

    def doFile(self, filename):
        m = self.recipe.magic[filename]

        fullpath = self.macros.destdir + filename
        hardlinkMap = self.recipe.autopkg.findComponent(filename).hardlinkMap

        if (os.path.isfile(fullpath) and util.isregular(fullpath) and
            not self._fileIsBinary(fullpath, maxsize=20*1024) and
            not filename in hardlinkMap):
            self.info(filename)
            self.recipe.autopkg.pathMap[filename].flags.isConfig(True)
        else:
            # RPM config files are handled more like initialcontents,
            # so for for files that conary can't be sure it can display
            # diffs on, we should make them be initialcontents for
            # conary verify purposes
            self.recipe.InitialContents(filename)


class InitialContents(packagepolicy.InitialContents):
    # Descends from packagepolicy.InitialContents to remove invariants
    # and avoid errors when importing RPMs
    invariantinclusions = None
    invariantexceptions = [ ]

    def updateArgs(self, *args, **keywords):
        policy.Policy.updateArgs(self, *args, **keywords)

    def doFile(self, filename):
	fullpath = self.macros.destdir + filename
        recipe = self.recipe
        if not os.path.isdir(fullpath) or os.path.islink(fullpath):
            f = recipe.autopkg.pathMap[filename]
            # config wins; initialContents is only for verify in capsules
            if not f.flags.isConfig():
                self.info(filename)
                f.flags.isInitialContents(True)


class Transient(packagepolicy.Transient):
    # Descends from packagepolicy.Transient to remove invariants
    # and avoid errors when importing RPMs
    invariantinclusions = None

    def doFile(self, filename):
	fullpath = self.macros.destdir + filename
	if os.path.isfile(fullpath) and util.isregular(fullpath):
            recipe = self.recipe
            f = recipe.autopkg.pathMap[filename]
            # config or initialContents wins in capsule packages
            if not (f.flags.isConfig() or f.flags.isInitialContents()):
                self.info(filename)
                f.flags.isTransient(True)


class Payload(policy.Policy):
    """
    FIXME: write docs
    """
    bucket = policy.PACKAGE_CREATION
    filetree = policy.PACKAGE

    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('Config', policy.REQUIRED_PRIOR),
    )

    def doFile(self, filename):
        f = self.recipe.autopkg.pathMap[filename]

        # every regular file is payload if it is not a config file,
        # not an empty initialContents file and is inside a capsule
        if self.recipe._getCapsulePathsForFile(filename) and isinstance(f, files.RegularFile) \
                and not f.flags.isConfig() and not ( f.flags.isInitialContents() and not f.contents.size() ):
            f.flags.isPayload(True)


class RPMProvides(policy.Policy):
    """
    FIXME: Write docs
    """
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )

    keywords = {
        'provisions': {}
    }

    provisionRe = re.compile('(.*?):(.*?)\((.*?)\)')

    def updateArgs(self, *args, **keywords):
        if len(args) is 2:
            name = args[1]
            if ':' not in name:
                name = name + ':rpm'

            if not self.provisions.get(name):
                self.provisions[name] = deps.DependencySet()

            reMatch = self.provisionRe.match(args[0])

            depClass = reMatch.group(1).strip()
            dep = reMatch.group(2).strip()
            flags = reMatch.group(3).strip().split()
            flags = [ (x, deps.FLAG_SENSE_REQUIRED) for x in flags if x ]

            self.provisions[name].addDep(
                deps.dependencyClassesByName[depClass.lower()],
                deps.Dependency(dep, flags))
            policy.Policy.updateArgs(self, **keywords)


    def do(self):
        for comp in self.recipe.autopkg.components.items():
            capsule =  self.recipe._getCapsule(comp[0])

            if capsule:
                if capsule[0] == 'rpm':
                    path = capsule[1]
                    h = rpmhelper.readHeader(file(path))

                    prov = h._getDepsetFromHeader(rpmhelper.PROVIDENAME)
                    comp[1].provides.union(prov)

                    if self.provisions:
                        userProvs = self.provisions.get(comp[0])
                        if userProvs:
                            comp[1].provides.union(userProvs)


class RPMRequires(policy.Policy):
    """
    FIXME: Write docs
    """
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )

    keywords = {
        'requirements': {}
    }

    requirementRe = re.compile('(.*?):(.*?)\((.*?)\)')

    def updateArgs(self, *args, **keywords):
        if len(args) is 2:
            name = args[1]
            if ':' not in name:
                name = name + ':rpm'

            if not self.requirements.get(name):
                self.requirements[name] = deps.DependencySet()

            reMatch = self.requirementRe.match(args[0])

            depClass = reMatch.group(1).strip()
            dep = reMatch.group(2).strip()
            flags = reMatch.group(3).strip().split()
            flags = [ (x, deps.FLAG_SENSE_REQUIRED) for x in flags if x ]

            self.requirements[name].addDep(
                deps.dependencyClassesByName[depClass.lower()],
                deps.Dependency(dep, flags))
            policy.Policy.updateArgs(self, **keywords)


    def do(self):
        for comp in self.recipe.autopkg.components.items():
            capsule =  self.recipe._getCapsule(comp[0])

            if capsule:
                if capsule[0] == 'rpm':
                    path = capsule[1]
                    h = rpmhelper.readHeader(file(path))

                    req = h._getDepsetFromHeader(rpmhelper.REQUIRENAME)
                    comp[1].requires.union(req)

                    if self.requirements:
                        userReqs = self.requirements.get(comp[0])
                        if userReqs:
                            comp[1].requires.union(userReqs)

