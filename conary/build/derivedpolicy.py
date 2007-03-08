#
# Copyright (c) 2007 rPath, Inc.
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

import re, os

from conary import files, trove
from conary.build import buildpackage, destdirpolicy, filter, packagepolicy, policy
from conary.deps import deps

FixDirModes = destdirpolicy.FixDirModes

Config = packagepolicy.Config
InitialContents = packagepolicy.InitialContents
Transient = packagepolicy.Transient

class ComponentSpec(packagepolicy.ComponentSpec):
    processUnmodified = True

    requires = (
        ('PackageSpec', policy.REQUIRED_SUBSEQUENT),
    )

    def doProcess(self, recipe):
        # map paths into the correct components
        for trvCs in self.recipe.cs.iterNewTroveList():
            trv = trove.Trove(trvCs)

            if not trv.isCollection():
                regexs = [ re.escape(x[1]) for x in trv.iterFileList() ]
                f = filter.Filter(regexs, self.recipe.macros,
                                  name = trv.getName().split(':')[1])
                self.derivedFilters.append(f)

        packagepolicy.ComponentSpec.doProcess(self, recipe)

class PackageSpec(packagepolicy.PackageSpec):
    processUnmodified = True

    def doProcess(self, recipe):
        self.pathObjs = {}

        for trvCs in self.recipe.cs.iterNewTroveList():
            trv = trove.Trove(trvCs)

            for (pathId, path, fileId, version) in trv.iterFileList():
                fileCs = self.recipe.cs.getFileChange(None, fileId)
                self.pathObjs[path] = files.ThawFile(fileCs, pathId)

        packagepolicy.PackageSpec.doProcess(self, recipe)

    def doFile(self, path):
        destdir = self.recipe.macros.destdir

        if path not in self.pathObjs:
            return packagepolicy.PackageSpec.doFile(self, path)

        self.recipe.autopkg.addFile(path, destdir + path)
        component = self.recipe.autopkg.componentMap[path]
        pkgFile = self.recipe.autopkg.pathMap[path]
        fileObj = self.pathObjs[path]
        # these three flags can be changed in policy
        fileObj.flags.isConfig(False)
        fileObj.flags.isInitialContents(False)
        fileObj.flags.isTransient(False)
        pkgFile.inode.owner.set(fileObj.inode.owner())
        pkgFile.inode.group.set(fileObj.inode.group())
        pkgFile.tags.thaw(fileObj.tags.freeze())
        pkgFile.flavor.thaw(fileObj.flavor.freeze())
        pkgFile.flags.thaw(fileObj.flags.freeze())

        component.requiresMap[path] = fileObj.requires()
        component.providesMap[path] = fileObj.provides()

    def postProcess(self):
        packagepolicy.PackageSpec.postProcess(self)
        fileProvides = deps.DependencySet()
        fileRequires = deps.DependencySet()
        for fileObj in self.pathObjs.values():
            fileProvides.union(fileObj.provides())
            fileRequires.union(fileObj.requires())

        for comp in self.recipe.autopkg.components.values():
            if comp.name in self.recipe._componentReqs:
                # copy component dependencies for components which came
                # from derived packages, only for dependencies that are
                # not expressed in the file dependencies
                comp.requires.union(
                    self.recipe._componentReqs[comp.name] - fileRequires)
                # copy only the provisions that won't be handled through
                # ComponentProvides, which may remove capability flags
                depSet = deps.DependencySet()
                for dep in self.recipe._componentProvs[comp.name].iterDeps():
                    if (dep[0] is deps.TroveDependencies and
                        dep[1].getName()[0] in self.recipe._componentReqs):
                        continue
                    depSet.addDep(*dep)
                comp.provides.union(depSet - fileProvides)

class Flavor(packagepolicy.Flavor):
    processUnmodified = True

    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )

    def preProcess(self):
        packagepolicy.Flavor.preProcess(self)

        for comp in self.recipe.autopkg.components.values():
            comp.flavor.union(self.recipe.useFlags)

    def doFile(self, path):
        componentMap = self.recipe.autopkg.componentMap
        if path not in componentMap:
            return
        pkg = componentMap[path]
        f = pkg.getFile(path)

        if f.flavor().isEmpty():
            packagepolicy.Flavor.doFile(self, path)
        else:
            self.packageFlavor.union(f.flavor())

class Requires(packagepolicy.Requires):
    processUnmodified = True
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('Provides', policy.REQUIRED_PRIOR),
    )
    filetree = policy.PACKAGE

    def doFile(self, path):
        pkg = self.recipe.autopkg.componentMap[path]
        f = pkg.getFile(path)
        self.whiteOut(path, pkg)
        self.unionDeps(path, pkg, f)

class Provides(packagepolicy.Provides):
    processUnmodified = True

    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('Requires', policy.REQUIRED_SUBSEQUENT),
    )

    def doFile(self, path):
        pkg = self.recipe.autopkg.componentMap[path]
        f = pkg.getFile(path)

        m = self.recipe.magic[path]
        macros = self.recipe.macros

        fullpath = macros.destdir + path
        dirpath = os.path.dirname(path)

        self.addExplicitProvides(path, fullpath, pkg, macros, m, f)
        self.addPathDeps(path, dirpath, pkg, f)
        self.unionDeps(path, pkg, f)

class ComponentRequires(packagepolicy.ComponentRequires):
    processUnmodified = True

    def do(self):
        packagepolicy.ComponentRequires.do(self)

        # Remove any intercomponent dependencies which point to troves which
        # are now empty.  We wouldn't have created any, but we could have
        # inherited some during PackageSpec
        components = self.recipe.autopkg.components
        packageMap = self.recipe.autopkg.packageMap
        mainSet = set([main.name for main in packageMap])
        for comp in components.values():
            removeDeps = deps.DependencySet()
            for dep in comp.requires.iterDepsByClass(deps.TroveDependencies):
                name = dep.getName()[0]
                if ':' in name:
                    main = name.split(':', 1)[0]
                    if (main in mainSet and
                        name not in components or not components[name]):
                        removeDeps.addDep(deps.TroveDependencies, dep)

            comp.requires -= removeDeps

class ComponentProvides(packagepolicy.ComponentProvides):
    processUnmodified = True
    def do(self):
        # pick up parent component flags
        for depSet in self.recipe._componentProvs.values():
            for dep in depSet.iterDepsByClass(deps.TroveDependencies):
                self.flags.update(dep.flags.keys())
        packagepolicy.ComponentProvides.do(self)


class ByDefault(packagepolicy.ByDefault):
    # Because this variant honors existing settings, overrides must
    # be of the package:component variety.  ":component" will only
    # work for components added in this derived package
    def doProcess(self, recipe):
        originalInclusions = recipe.byDefaultIncludeSet
        originalExceptions = recipe.byDefaultExcludeSet
        if not self.inclusions:
            self.inclusions = []
        if not self.exceptions:
            self.exceptions = []
        inclusions = set(originalInclusions.union(set(self.inclusions))
             - set(self.exceptions).union(set(self.invariantexceptions)))
        exceptions = set(originalExceptions.union(set(self.exceptions))
             - set(self.inclusions))
        recipe.setByDefaultOn(inclusions)
        recipe.setByDefaultOff(exceptions)
    

class TagSpec(packagepolicy.TagSpec):
    # do not load the system-defined tags for derived packages
    processUnmodified = True
    def doProcess(self, recipe):
	self.tagList = []
        self.suggestBuildRequires = set()
        self.db = None
        self.fullReqs = set()
        packagepolicy._addInfo.doProcess(self, recipe)


ExcludeDirectories = packagepolicy.ExcludeDirectories
MakeDevices = packagepolicy.MakeDevices
Ownership = packagepolicy.Ownership
UtilizeUser = packagepolicy.UtilizeUser
UtilizeGroup = packagepolicy.UtilizeGroup
TagDescription = packagepolicy.TagDescription
TagHandler = packagepolicy.TagHandler

setModes = packagepolicy.setModes
LinkType = packagepolicy.LinkType
LinkCount = packagepolicy.LinkCount
reportMissingBuildRequires = packagepolicy.reportMissingBuildRequires
reportErrors = packagepolicy.reportErrors
