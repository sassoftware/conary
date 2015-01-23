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


"""
Module used to override and augment packagepolicy specifically for Capsule
Recipes
"""

import os
import re
import stat
import sre_constants

from conary import files
from conary.lib import util
from conary import rpmhelper
from conary.deps import deps
from conary.build import filter
from conary.build import policy
from conary.build import packagepolicy

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
    processUnmodified = True
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR), # for hardlink detection
        ('LinkType', policy.CONDITIONAL_SUBSEQUENT),
        ('InitialContents', policy.REQUIRED_SUBSEQUENT),
    )

    def doFile(self, filename):
        fullpath = self.macros.destdir + filename
        hardlinkMap = self.recipe.autopkg.findComponent(filename).hardlinkMap

        if (os.path.isfile(fullpath) and util.isregular(fullpath) and
            not self._fileIsBinary(filename, fullpath, maxsize=20*1024) and
            not filename in hardlinkMap):
            self.info(filename)
            for pkg in self.recipe.autopkg.findComponents(filename):
                f = pkg.getFile(filename)
                f.flags.isConfig(True)
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
    processUnmodified = True

    def updateArgs(self, *args, **keywords):
        # override packagepolicy.InitialContents to avoid invoking Config
        policy.Policy.updateArgs(self, *args, **keywords)

    def doFile(self, filename):
        fullpath = self.macros.destdir + filename
        recipe = self.recipe
        if not os.path.isdir(fullpath) or os.path.islink(fullpath):
            for pkg in self.recipe.autopkg.findComponents(filename):
                f = pkg.getFile(filename)
                # config wins; initialContents is only for verify in capsules
                if f.flags.isConfig():
                    return
            # OK, not set to config:
            self.info(filename)
            for pkg in self.recipe.autopkg.findComponents(filename):
                f = pkg.getFile(filename)
                f.flags.isInitialContents(True)


class Transient(packagepolicy.Transient):
    # Descends from packagepolicy.Transient to remove invariants
    # and avoid errors when importing RPMs
    invariantinclusions = None
    processUnmodified = True

    def doFile(self, filename):
        fullpath = self.macros.destdir + filename
        if os.path.isfile(fullpath) and util.isregular(fullpath):
            recipe = self.recipe
            for pkg in self.recipe.autopkg.findComponents(filename):
                f = pkg.getFile(filename)
                # config or initialContents wins in capsule packages
                if f.flags.isConfig() or f.flags.isInitialContents():
                    return
            self.info(filename)
            for pkg in self.recipe.autopkg.findComponents(filename):
                f = pkg.getFile(filename)
                f.flags.isTransient(True)


class MissingOkay(policy.Policy):
    """
    NAME
    ====
    B{C{r.MissingOkay()}} - Mark as "missing Okay" only capsule-provided files
    so marked in their respective capsule.

    DESCRIPTION
    ===========
    This policy should not be called from recipes.
    """
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )
    bucket = policy.PACKAGE_CREATION
    filetree = policy.PACKAGE
    processUnmodified = True
    invariantinclusions = None
    invariantexceptions = [ ]

    def doFile(self, filename):
        if not self.recipe._getCapsulePathsForFile(filename):
            # applies only to encapsulated files
            return
        self.info(filename)
        for pkg in self.recipe.autopkg.findComponents(filename):
            f = pkg.getFile(filename)
            f.flags.isMissingOkay(True)


class setModes(packagepolicy.setModes):
    # descends from packagepolicy.setModes to honor varying modes
    # between different capsules sharing a path; also to set mtime
    # on capsules
    def do(self):
        for filename, package, _, _, mode, mtime in self.recipe._iterCapsulePathData():
            mode = stat.S_IMODE(mode)
            for pkg in self.recipe.autopkg.findComponents(filename):
                if pkg.getName() == package:
                    f = pkg.getFile(filename)
                    f.inode.perms.set(mode)
                    if f.inode.mtime() != mtime:
                        f.inode.mtime.set(mtime)
        # For any other paths, fall through to superclass
        packagepolicy.setModes.do(self)

class Ownership(packagepolicy.Ownership):
    # descends from packagepolicy.Ownership to honor varying Ownership
    # between different capsules sharing a path
    def do(self):
        for filename, package, user, group, _, _ in self.recipe._iterCapsulePathData():
            for pkg in self.recipe.autopkg.findComponents(filename):
                if pkg.getName() == package:
                    f = pkg.getFile(filename)
                    f.inode.owner.set(user)
                    f.inode.group.set(group)
        # For any other paths, fall through to superclass
        packagepolicy.Ownership.do(self)


class Payload(policy.Policy):
    """
    This policy is used to mark files which have their contents stored  inside
    a capsule rather than in the trove.
    Do not call it directly; it is for internal use only.
    """
    bucket = policy.PACKAGE_CREATION
    filetree = policy.PACKAGE
    processUnmodified = True

    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('Config', policy.REQUIRED_PRIOR),
    )

    def doFile(self, filename):

        # every regular file is payload if it is not a config file,
        # not an empty initialContents file and is inside a capsule
        if self.recipe._getCapsulePathsForFile(filename):
            for pkg in self.recipe.autopkg.findComponents(filename):
                f = pkg.getFile(filename)
                if (isinstance(f, files.RegularFile)
                    and not f.flags.isConfig()
                    and not (f.flags.isInitialContents()
                             and not f.contents.size())):
                    f.flags.isEncapsulatedContent(True)


class RPMProvides(policy.Policy):
    """
    NAME
    ====
    B{C{r.RPMProvides()}} - Creates dependency provision for an RPM capsule

    SYNOPSIS
    ========
    C{r.RPMProvides([I{provision}, I{package} | I{package:component})}

    DESCRIPTION
    ===========
    The C{r.RPMProvides()} policy marks an rpm capsule as providing certain
    features or characteristics, and can be called to explicitly provide things
    that cannot be automatically discovered and are not provided by the RPM
    header.

    For unusual cases where you want to remove a provision Conary
    automatically finds in the encapsulated RPM, you can specify
    C{r.RPMProvides(exceptDeps='regexp')} to override all provisions matching
    a regular expression, C{r.RPMProvides(exceptDeps=('filterexp', 'regexp'))}
    to override provisions matching a regular expression only for components
    matching filterexp, or
    C{r.RPMProvides(exceptDeps=(('filterexp', 'regexp'), ...))} to specify
    multiple overrides.

    A C{I{provision}} can only specify an rpm provision in the form of I{rpm:
    dependency(FLAG1...)}

    EXAMPLES
    ========
    C{r.RPMProvides('rpm: bar(FLAG1 FLAG2)', 'foo:rpm')}

    C{r.RPMProvides(exceptDeps='rpm: libstdc++.*')}
    """
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('Provides', policy.REQUIRED_PRIOR),
    )

    keywords = {
        'provisions': {}
    }

    provisionRe = re.compile('(.+?):([^()]+)\(?([^()]*)\)?')

    def __init__(self, *args, **keywords):
        policy.Policy.__init__(self, *args, **keywords)
        self.mergeKmodSymbols = False
        self.exceptDeps = []

    def updateArgs(self, *args, **keywords):
        if len(args) is 2:
            name = args[1]
            if ':' not in name:
                name = name + ':rpm'

            if not self.provisions.get(name):
                self.provisions[name] = deps.DependencySet()

            reMatch = self.provisionRe.match(args[0])
            if not reMatch or len(reMatch.groups()) != 3:
                return

            depClass = reMatch.group(1).strip().lower()
            if depClass != 'rpm' and depClass != 'rpmlib':
                raise policy.PolicyError, "RPMProvides cannot be used to " \
                    "provide the non-rpm dependency: '%s'" % args[0]
            dep = reMatch.group(2).strip()
            flags = reMatch.group(3).strip().split()
            flags = [(x, deps.FLAG_SENSE_REQUIRED) for x in flags if x]

            if not self.provisions.get(name):
                self.provisions[name] = deps.DependencySet()
            self.provisions[name].addDep(
                deps.dependencyClassesByName[depClass],
                deps.Dependency(dep, flags))

        exceptDeps = keywords.pop('exceptDeps', None)
        if exceptDeps:
            if type(exceptDeps) is str:
                exceptDeps = ('.*', exceptDeps)
            assert(type(exceptDeps) == tuple)
            if type(exceptDeps[0]) is tuple:
                self.exceptDeps.extend(exceptDeps)
            else:
                self.exceptDeps.append(exceptDeps)

        # CNY-3518: set the default for whether to merge modules --
        # this should be passed in only from RPMRequires
        if '_mergeKmodSymbols' in keywords:
            self.mergeKmodSymbols = keywords.pop('_mergeKmodSymbols')

        policy.Policy.updateArgs(self, **keywords)

    def preProcess(self):
        exceptDeps = []
        for fE, rE in self.exceptDeps:
            try:
                exceptDeps.append((filter.Filter(fE, self.macros),
                                   re.compile(rE % self.macros)))
            except sre_constants.error, e:
                self.error('Bad regular expression %s for file spec %s: %s',
                    rE, fE, e)
        self.exceptDeps = exceptDeps

    def do(self):
        for comp in self.recipe.autopkg.components.items():
            capsule = self.recipe._getCapsule(comp[0])

            if capsule and capsule[0] == 'rpm':
                path = capsule[1]
                h = rpmhelper.readHeader(file(path))
                prov = h.getProvides(mergeKmodSymbols=self.mergeKmodSymbols,
                    enableRPMVersionDeps=self.recipe.cfg.enableRPMVersionDeps)

                fltrprov = self._filterProvides(comp[0], prov)

                comp[1].provides.union(fltrprov)

                if self.provisions:
                    userProvs = self.provisions.get(comp[0])
                    if userProvs:
                        comp[1].provides.union(userProvs)

    def _filterProvides(self, compName, provides):
        removeDeps = deps.DependencySet()

        for depClass, dep in provides.iterDeps():
            for compRe, depRe in self.exceptDeps:
                if not compRe.match(compName):
                    continue
                depName = '%s: %s' % (depClass.tagName, str(dep))
                if depRe.match(depName):
                    removeDeps.addDep(depClass, dep)
                    break

        return provides - removeDeps


class RPMRequires(policy.Policy):
    """
    NAME
    ====
    B{C{r.RPMRequires()}} - Creates dependency requirement for an RPM capsule

    SYNOPSIS
    ========
    C{r.RPMRequires([I{requirement}, I{package} || I{package:component} || I{exceptions=filterexp}])}

    DESCRIPTION
    ===========
    The C{r.RPMRequires()} policy marks an rpm capsule as requiring certain
    features or characteristics, and can be called to explicitly provide things
    that cannot be automatically discovered and are not provided by the RPM
    header. You can pass in exceptions that should not have automatic
    requirement discovery done.

    A C{I{requirement}} can only specify an rpm requirement in the form of
    I{rpm: dependency(FLAG1...)}

    For unusual cases where Conary finds a false or misleading dependency,
    or in which you need to override a true dependency, you can specify
    C{r.RPMRequires(exceptDeps='regexp')} to override all dependencies matching
    a regular expression, C{r.RPMRequires(exceptDeps=('filterexp', 'regexp'))}
    to override dependencies matching a regular expression only for files
    matching filterexp, or
    C{r.RPMRequires(exceptDeps=(('filterexp', 'regexp'), ...))} to specify
    multiple overrides.


    EXAMPLES
    ========
    C{r.RPMRequires('rpm: bar(FLAG1 FLAG2)', 'foo:rpm')}
    """

    bucket = policy.PACKAGE_CREATION
    requires = (
        ('Requires', policy.REQUIRED_PRIOR),
        ('RPMProvides', policy.REQUIRED_PRIOR),
        ('RemoveSelfProvidedRequires', policy.REQUIRED_SUBSEQUENT),
        )

    keywords = {
        'exceptions': {},
        'exceptDeps' : []
        }

    requirementRe = re.compile('(.+?):([^()]+)\(?([^()]*)\)?')
    rpmStringRe = re.compile('(.*?)\[(.*?)\]')

    def __init__(self, *args, **keywords):
        policy.Policy.__init__(self, *args, **keywords)
        self.excepts = set()
        self.requirements = {}
        self.filters = []
        self.mergeKmodSymbols = False

    def updateArgs(self, *args, **keywords):
        if len(args) is 2:
            name = args[1]
            if ':' not in name:
                name = name + ':rpm'

            reMatch = self.requirementRe.match(args[0])
            if not reMatch or len(reMatch.groups()) != 3:
                return

            depClass = reMatch.group(1).strip().lower()
            if depClass != 'rpm' and depClass != 'rpmlib':
                raise policy.PolicyError, "RPMRequires cannot be used to " \
                    "provide the non-rpm dependency: '%s'" % args[0]
            dep = reMatch.group(2).strip()
            flags = reMatch.group(3).strip().split()
            flags = [(x, deps.FLAG_SENSE_REQUIRED) for x in flags if x]

            if not self.requirements.get(name):
                self.requirements[name] = deps.DependencySet()
            self.requirements[name].addDep(
                deps.dependencyClassesByName[depClass],
                deps.Dependency(dep, flags))

        allowUnusedFilters = keywords.pop('allowUnusedFilters', False) or \
            self.allowUnusedFilters

        exceptions = keywords.pop('exceptions', None)
        if exceptions:
            if type(exceptions) is str:
                self.excepts.add(exceptions)
                if not allowUnusedFilters:
                    self.unusedFilters['exceptions'].add(exceptions)
            elif type(exceptions) in (tuple, list):
                self.excepts.update(exceptions)
                if not allowUnusedFilters:
                    self.unusedFilters['exceptions'].update(exceptions)

        exceptDeps = keywords.pop('exceptDeps', None)
        if exceptDeps:
            if type(exceptDeps) is str:
                exceptDeps = ('.*', exceptDeps)
            assert(type(exceptDeps) == tuple)
            if type(exceptDeps[0]) is tuple:
                self.exceptDeps.extend(exceptDeps)
            else:
                self.exceptDeps.append(exceptDeps)

        # CNY-3518: set the default for whether to merge modules
        if 'mergeKmodSymbols' in keywords:
            self.mergeKmodSymbols = keywords.pop('mergeKmodSymbols')
            self.recipe.RPMProvides(_mergeKmodSymbols=self.mergeKmodSymbols)

        policy.Policy.updateArgs(self, **keywords)

    def preProcess(self):
        exceptDeps = []
        for fE, rE in self.exceptDeps:
            try:
                exceptDeps.append((filter.Filter(fE, self.macros),
                                   re.compile(rE % self.macros)))
            except sre_constants.error, e:
                self.error('Bad regular expression %s for file spec %s: %s',
                           rE, fE, e)
        self.exceptDeps=exceptDeps

    def do(self):
        for comp in self.recipe.autopkg.components.items():
            capsule =  self.recipe._getCapsule(comp[0])

            if capsule and capsule[0] == 'rpm':
                if not self.filters:
                    self.filters = [(x, filter.Filter(x, self.macros))
                               for x in self.excepts]

                path = capsule[1]
                matchFound = False
                for regexp, f in self.filters:
                    if f.match(path):
                        self.unusedFilters['exceptions'].discard(regexp)
                        matchFound=True
                if matchFound:
                    continue

                h = rpmhelper.readHeader(file(path))
                rReqs, rProv = h.getDeps(mergeKmodSymbols=self.mergeKmodSymbols,
                    enableRPMVersionDeps=self.recipe.cfg.enableRPMVersionDeps)

                # integrate user specified requirements
                if self.requirements:
                    userReqs = self.requirements.get(comp[0])
                    if userReqs:
                        rReqs.union(userReqs)

                # remove rpm provisions from the requirements
                rReqs = rReqs.difference(rProv)

                # cull duplicate rpm reqs that have a standard conary
                # representations
                # currently we only handle perl and sonames
                culledReqs = deps.DependencySet()
                cnyReqs = comp[1].requires
                cnyProv = comp[1].provides
                if rReqs.hasDepClass(deps.RpmDependencies):
                    soDeps = deps.DependencySet()
                    soDeps.addDeps(deps.SonameDependencies, \
                        list(cnyReqs.iterDepsByClass(deps.SonameDependencies))+\
                        list(cnyProv.iterDepsByClass(deps.SonameDependencies)))

                    for r in list(rReqs.iterDepsByClass(deps.RpmDependencies)):
                        reMatch = self.rpmStringRe.match(r.name)
                        if reMatch and reMatch.groups():
                            rpmFile = reMatch.group(1)
                            rpmFlags = reMatch.group(2).strip()
                        else:
                            rpmFile = r.name
                            rpmFlags = ''
                        if rpmFile == 'perl' and rpmFlags:
                            ds = deps.DependencySet()
                            dep = deps.Dependency(rpmFlags)
                            dep.flags = r.flags
                            ds.addDep(deps.PerlDependencies, dep)
                            if cnyReqs.satisfies(ds) or \
                                    cnyProv.satisfies(ds):
                                culledReqs.addDep(
                                    deps.RpmDependencies,r)
                        elif '.so' in rpmFile:
                            ds = deps.DependencySet()
                            if rpmFlags == '64bit':
                                elfPrefix = 'ELF64/'
                            else:
                                elfPrefix = 'ELF32/'
                            dep = deps.Dependency(elfPrefix + rpmFile)
                            dep.flags = r.flags
                            ds.addDep(deps.SonameDependencies, dep)
                            if soDeps.satisfies(ds):
                                culledReqs.addDep(deps.RpmDependencies,r)
                rReqs = rReqs.difference(culledReqs)

                # remove any excepted deps
                for filt, exceptRe in self.exceptDeps:
                    if filt.match(path):
                        for depClass, dep in list(rReqs.iterDeps()):
                            matchName = '%s: %s' %(depClass.tagName, str(dep))
                            if exceptRe.match(matchName):
                                rReqs.removeDeps(depClass, [ dep ])
                cnyReqs.union(rReqs)

class PureCapsuleComponents(policy.Policy):
    """
    NAME
    ====
    B{C{r.PureCapsuleComponents()}} - Ensure that components which contain a
    capsule do not contain any other files.

    SYNOPSIS
    ========
    C{r.PureCapsuleComponents(I{exceptions=filterexp}])}

    DESCRIPTION
    ===========
    This policy is used to ensure that if a component contains a capsule that
    it only contains files defined within that capsule. It should not normally
    be called.  It is, however, sometimes necessary to provide an exception to
    add an additional file to a capsule package.


    EXAMPLES
    ========
    C{r.PureCapsuleComponents( exceptions='foo.*conf')}
    """
    bucket = policy.ENFORCEMENT
    requires = (
        ('ExcludeDirectories', policy.REQUIRED_PRIOR),
    )

    def doFile(self,path):
        inCapsule = bool(self.recipe._getCapsulePathsForFile(path))
        if inCapsule:
            return
        comp = self.recipe.autopkg.componentMap.get(path,None)
        if comp:
            compHasCapsule = bool(self.recipe._hasCapsulePackage(comp.name))
            if compHasCapsule:
                self.error("Component %s contains both a "
                           "capsule and the non-capsule file: %s. Use an "
                           "exception to add this file to this component."
                           % (comp.name,path) )

class CapsuleModifications(policy.Policy):
    """
    This policy is used to mark files which are modified in or added to a
    capsule component.
    Do not call it directly; it is for internal use only.
    """
    bucket = policy.PACKAGE_CREATION
    filetree = policy.PACKAGE

    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('ExcludeDirectories', policy.REQUIRED_PRIOR),
    )

    def doFile(self, filename):
        changeState = self.fileChanged(filename)
        if changeState:
            for pkg in self.recipe.autopkg.findComponents(filename):
                capPath = self.recipe._getCapsulePathsForFile(filename)
                hasCapsule = bool(self.recipe._hasCapsulePackage(pkg.name))
                if hasCapsule:
                    if capPath:
                        # CNY-3590: we won't allow users to change contents or
                        # permissions of encapsulated files
                        continue
                    f = pkg.getFile(filename)
                    f.flags.isCapsuleAddition(True)
                    # Setting the override flag is unnecessary, but
                    # historically that's what the code did, prior to CNY-3577
                    f.flags.isCapsuleOverride(True)
                    f.flags.isEncapsulatedContent(False)


class RemoveCapsuleFiles(packagepolicy._filterSpec):
    """
    NAME
    ====
    B{C{r.RemoveCapsuleFiles()}} - Remove a encapsulated file from the Conary
    package manifest.

    SYNOPSIS
    ========
    C{r.RemoveCapsuleFiles(I{packagename, I{filterexp})}

    DESCRIPTION
    ===========
    The C{r.RemoveCapsuleFiles()} policy removes encapsulated files from the
    specified package that match the specified regular expression. This policy
    is meant to be used in the case that Conary incorrectly handles an RPM
    update due to path conflict checking.

    NOTE: The excluded file will still be installed and only managed by RPM.

    EXAMPLES
    ========
    C{r.RemoveCapsuleFiles('foo:rpm', '/opt')}

    Specifies that the directory C{/opt} should be removed from the Conary
    package manifest.
    """

    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )

    def do(self):
        """
        Remove files from the Conary package manifest that match any specified
        filters.
        """

        # Compile the set of filters.
        filters = {}
        for name, regex in self.extraFilters:
            name %= self.macros
            filters.setdefault(name, []).append(
                filter.Filter(regex, self.macros, name=name))

        # Get a mapping of component name to component object
        components = dict((x.name, x) for x in
            self.recipe.autopkg.getComponents())

        for name, fltrs in filters.iteritems():
            if name not in components:
                self.error("Component %s does not exist" % name);
                continue

            provides = components[name].provides

            # make a copy of the files list since it will be modified in place.
            files = components[name].keys()
            for fn in files:
                for fltr in fltrs:
                    if fltr.match(fn):
                        self.recipe.autopkg.delFile(fn)
                        self.recipe._capsulePathMap.pop(fn)
                        self.recipe._capsuleDataMap.pop(fn)
                        provides.removeDeps(deps.FileDependencies,
                            [ deps.Dependency(fn) ], missingOkay=True)
                        break
