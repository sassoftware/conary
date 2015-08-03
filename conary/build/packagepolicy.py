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
Module used after C{%(destdir)s} has been finalized to create the
initial packaging.  Also contains error reporting.
"""
import codecs
import imp
import itertools
import os
import re
import site
import sre_constants
import stat
import subprocess
import sys

from conary import files, trove
from conary.build import buildpackage, filter, policy, recipe, tags, use
from conary.build import smartform
from conary.deps import deps
from conary.lib import elf, magic, util, pydeps, fixedglob, graph

from conary.build.action import TARGET_LINUX
from conary.build.action import TARGET_WINDOWS

try:
    from xml.etree import ElementTree
except ImportError:
    try:
        from elementtree import ElementTree
    except ImportError:
        ElementTree = None


# Helper class
class _DatabaseDepCache(object):
    __slots__ = ['db', 'cache']
    def __init__(self, db):
        self.db = db
        self.cache = {}

    def getProvides(self, depSetList):
        ret = {}
        missing = []
        for depSet in depSetList:
            if depSet in self.cache:
                ret[depSet] = self.cache[depSet]
            else:
                missing.append(depSet)
        newresults = self.db.getTrovesWithProvides(missing)
        ret.update(newresults)
        self.cache.update(newresults)
        return ret


class _filterSpec(policy.Policy):
    """
    Pure virtual base class from which C{ComponentSpec} and C{PackageSpec}
    are derived.
    """
    bucket = policy.PACKAGE_CREATION
    processUnmodified = False
    supported_targets = (TARGET_LINUX, TARGET_WINDOWS)
    def __init__(self, *args, **keywords):
        self.extraFilters = []
        policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
        """
        Call derived classes (C{ComponentSpec} or C{PackageSpec}) as::
            ThisClass('<name>', 'filterexp1', 'filterexp2')
        where C{filterexp} is either a regular expression or a
        tuple of C{(regexp[, setmodes[, unsetmodes]])}
        """
        if args:
            theName = args[0]
            for filterexp in args[1:]:
                self.extraFilters.append((theName, filterexp))
        policy.Policy.updateArgs(self, **keywords)


class _addInfo(policy.Policy):
    """
    Pure virtual class for policies that add information such as tags,
    requirements, and provision, to files.
    """
    bucket = policy.PACKAGE_CREATION
    processUnmodified = False
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )
    keywords = {
        'included': {},
        'excluded': {}
    }
    supported_targets = (TARGET_LINUX, TARGET_WINDOWS)

    def updateArgs(self, *args, **keywords):
        """
        Call as::
            C{I{ClassName}(I{info}, I{filterexp})}
        or::
            C{I{ClassName}(I{info}, exceptions=I{filterexp})}
        where C{I{filterexp}} is either a regular expression or a
        tuple of C{(regexp[, setmodes[, unsetmodes]])}
        """
        if args:
            args = list(args)
            info = args.pop(0)
            if args:
                if not self.included:
                    self.included = {}
                if info not in self.included:
                    self.included[info] = []
                self.included[info].extend(args)
            elif 'exceptions' in keywords:
                # not the usual exception handling, this is an exception
                if not self.excluded:
                    self.excluded = {}
                if info not in self.excluded:
                    self.excluded[info] = []
                self.excluded[info].append(keywords.pop('exceptions'))
            else:
                raise TypeError, 'no paths provided'
        policy.Policy.updateArgs(self, **keywords)

    def doProcess(self, recipe):
        # for filters
        self.rootdir = self.rootdir % recipe.macros

        # instantiate filters
        d = {}
        for info in self.included:
            newinfo = info % recipe.macros
            l = []
            for item in self.included[info]:
                l.append(filter.Filter(item, recipe.macros))
            d[newinfo] = l
        self.included = d

        d = {}
        for info in self.excluded:
            newinfo = info % recipe.macros
            l = []
            for item in self.excluded[info]:
                l.append(filter.Filter(item, recipe.macros))
            d[newinfo] = l
        self.excluded = d

        policy.Policy.doProcess(self, recipe)

    def doFile(self, path):
        fullpath = self.recipe.macros.destdir+path
        if not util.isregular(fullpath) and not os.path.islink(fullpath):
            return
        self.runInfo(path)

    def runInfo(self, path):
        'pure virtual'
        pass


class Config(policy.Policy):
    """
    NAME
    ====
    B{C{r.Config()}} - Mark files as configuration files

    SYNOPSIS
    ========
    C{r.Config([I{filterexp}] || [I{exceptions=filterexp}])}

    DESCRIPTION
    ===========
    The C{r.Config} policy marks all files below C{%(sysconfdir)s}
    (that is, C{/etc}) and C{%(taghandlerdir)s} (that is,
    C{/usr/libexec/conary/tags/}), and any other files explicitly
    mentioned, as configuration files.

        - To mark files as exceptions, use
          C{r.Config(exceptions='I{filterexp}')}.
        - To mark explicit inclusions as configuration files, use:
          C{r.Config('I{filterexp}')}

    A file marked as a Config file cannot also be marked as a
    Transient file or an InitialContents file.  Conary enforces this
    requirement.

    EXAMPLES
    ========
    C{r.Config(exceptions='%(sysconfdir)s/X11/xkb/xkbcomp')}

    The file C{/etc/X11/xkb/xkbcomp} is marked as an exception, since it is
    not actually a configuration file even though it is within the C{/etc}
    (C{%(sysconfdir)s}) directory hierarchy and would be marked as a
    configuration file by default.

    C{r.Config('%(mmdir)s/Mailman/mm_cfg.py')}

    Marks the file C{%(mmdir)s/Mailman/mm_cfg.py} as a configuration file;
    it would not be automatically marked as a configuration file otherwise.
    """
    bucket = policy.PACKAGE_CREATION
    processUnmodified = True
    requires = (
        # for :config component, ComponentSpec must run after Config
        # Otherwise, this policy would follow PackageSpec and just set isConfig
        # on each config file
        ('ComponentSpec', policy.REQUIRED_SUBSEQUENT),
    )
    invariantinclusions = [ '%(sysconfdir)s/', '%(taghandlerdir)s/']
    invariantexceptions = [ '%(userinfodir)s/', '%(groupinfodir)s' ]

    def doFile(self, filename):
        m = self.recipe.magic[filename]
        if m and m.name == "ELF":
            # an ELF file cannot be a config file, some programs put
            # ELF files under /etc (X, for example), and tag handlers
            # can be ELF or shell scripts; we just want tag handlers
            # to be config files if they are shell scripts.
            # Just in case it was not intentional, warn...
            if self.macros.sysconfdir in filename:
                self.info('ELF file %s found in config directory', filename)
            return
        fullpath = self.macros.destdir + filename
        if os.path.isfile(fullpath) and util.isregular(fullpath):
            if self._fileIsBinary(filename, fullpath):
                self.error("binary file '%s' is marked as config" % \
                        filename)
            self._markConfig(filename, fullpath)

    def _fileIsBinary(self, path, fn, maxsize=None, decodeFailIsError=True):
        limit = os.stat(fn)[stat.ST_SIZE]
        if maxsize is not None and limit > maxsize:
            self.warn('%s: file size %d longer than max %d',
                path, limit, maxsize)
            return True

        # we'll consider file to be binary file if we don't find any
        # good reason to mark it as text, or if we find a good reason
        # to mark it as binary
        foundFF = False
        foundNL = False
        f = open(fn, 'r')
        try:
            while f.tell() < limit:
                buf = f.read(65536)
                if chr(0) in buf:
                    self.warn('%s: file contains NULL byte', path)
                    return True
                if '\xff\xff' in buf:
                    self.warn('%s: file contains 0xFFFF sequence', path)
                    return True
                if '\xff' in buf:
                    foundFF = True
                if '\n' in buf:
                    foundNL = True
        finally:
            f.close()

        if foundFF and not foundNL:
            self.error('%s: found 0xFF without newline', path)

        utf8 = codecs.open(fn, 'r', 'utf-8')
        win1252 = codecs.open(fn, 'r', 'windows-1252')
        try:
            try:
                while utf8.tell() < limit:
                    utf8.read(65536)
            except UnicodeDecodeError, e:
                # Still want to print a warning if it is not unicode;
                # Note that Code Page 1252 is considered a legacy
                # encoding on Windows
                self.warn('%s: %s', path, str(e))
                try:
                    while win1252.tell() < limit:
                        win1252.read(65536)
                except UnicodeDecodeError, e:
                    self.warn('%s: %s', path, str(e))
                    return decodeFailIsError
        finally:
            utf8.close()
            win1252.close()

        return False

    def _addTrailingNewline(self, filename, fullpath):
        # FIXME: This exists only for stability; there is no longer
        # any need to add trailing newlines to config files.  This
        # also violates the rule that no files are modified after
        # destdir modification has been completed.
        self.warn("adding trailing newline to config file '%s'" % \
                filename)
        mode = os.lstat(fullpath)[stat.ST_MODE]
        oldmode = None
        if mode & 0600 != 0600:
            # need to be able to read and write the file to fix it
            oldmode = mode
            os.chmod(fullpath, mode|0600)

        f = open(fullpath, 'a')
        f.seek(0, 2)
        f.write('\n')
        f.close()
        if oldmode is not None:
            os.chmod(fullpath, oldmode)

    def _markConfig(self, filename, fullpath):
        self.info(filename)
        f = file(fullpath)
        f.seek(0, 2)
        if f.tell():
            # file has contents
            f.seek(-1, 2)
            lastchar = f.read(1)
            f.close()
            if lastchar != '\n':
                self._addTrailingNewline(filename, fullpath)

        f.close()
        self.recipe.ComponentSpec(_config=filename)


class ComponentSpec(_filterSpec):
    """
    NAME
    ====
    B{C{r.ComponentSpec()}} - Determines which component each file is in

    SYNOPSIS
    ========
    C{r.ComponentSpec([I{componentname}, I{filterexp}] || [I{packagename}:I{componentname}, I{filterexp}])}

    DESCRIPTION
    ===========
    The C{r.ComponentSpec} policy includes the filter expressions that specify
    the default assignment of files to components.  The expressions are
    considered in the order in which they are evaluated in the recipe, and the
    first match wins.  After all the recipe-provided expressions are
    evaluated, the default expressions are evaluated.  If no expression
    matches, then the file is assigned to the C{catchall} component.
    Note that in the C{I{packagename}:I{componentname}} form, the C{:}
    must be literal, it cannot be part of a macro.

    KEYWORDS
    ========
    B{catchall} : Specify the  component name which gets all otherwise
    unassigned files. Default: C{runtime}

    EXAMPLES
    ========
    C{r.ComponentSpec('manual', '%(contentdir)s/manual/')}

    Uses C{r.ComponentSpec} to specify that all files below the
    C{%(contentdir)s/manual/} directory are part of the C{:manual} component.

    C{r.ComponentSpec('foo:bar', '%(sharedir)s/foo/')}

    Uses C{r.ComponentSpec} to specify that all files below the
    C{%(sharedir)s/foo/} directory are part of the C{:bar} component
    of the C{foo} package, avoiding the need to invoke both the
    C{ComponentSpec} and C{PackageSpec} policies.

    C{r.ComponentSpec(catchall='data')}

    Uses C{r.ComponentSpec} to specify that all files not otherwise specified
    go into the C{:data} component instead of the default {:runtime}
    component.
    """
    requires = (
        ('Config', policy.REQUIRED_PRIOR),
        ('PackageSpec', policy.REQUIRED_SUBSEQUENT),
    )
    keywords = { 'catchall': 'runtime' }

    def __init__(self, *args, **keywords):
        """
        @keyword catchall: The component name which gets all otherwise
        unassigned files.  Default: C{runtime}
        """
        _filterSpec.__init__(self, *args, **keywords)
        self.configFilters = []
        self.derivedFilters = []

    def updateArgs(self, *args, **keywords):
        if '_config' in keywords:
            configPath=keywords.pop('_config')
            self.recipe.PackageSpec(_config=configPath)

        if args:
            name = args[0]
            if ':' in name:
                package, name = name.split(':')
                args = list(itertools.chain([name], args[1:]))
                if package:
                    # we've got a package as well as a component, pass it on
                    pkgargs = list(itertools.chain((package,), args[1:]))
                    self.recipe.PackageSpec(*pkgargs)

        _filterSpec.updateArgs(self, *args, **keywords)

    def doProcess(self, recipe):
        compFilters = []
        self.macros = recipe.macros
        self.rootdir = self.rootdir % recipe.macros

        self.loadFilterDirs()

        # The extras need to come before base in order to override decisions
        # in the base subfilters; invariants come first for those very few
        # specs that absolutely should not be overridden in recipes.
        for filteritem in itertools.chain(self.invariantFilters,
                                          self.extraFilters,
                                          self.derivedFilters,
                                          self.configFilters,
                                          self.baseFilters):
            if not isinstance(filteritem, (filter.Filter, filter.PathSet)):
                name = filteritem[0] % self.macros
                assert(name != 'source')
                args, kwargs = self.filterExpArgs(filteritem[1:], name=name)
                filteritem = filter.Filter(*args, **kwargs)

            compFilters.append(filteritem)

        # by default, everything that hasn't matched a filter pattern yet
        # goes in the catchall component ('runtime' by default)
        compFilters.append(filter.Filter('.*', self.macros, name=self.catchall))

        # pass these down to PackageSpec for building the package
        recipe.PackageSpec(compFilters=compFilters)


    def loadFilterDirs(self):
        invariantFilterMap = {}
        baseFilterMap = {}
        self.invariantFilters = []
        self.baseFilters = []

        # Load all component python files
        for componentDir in self.recipe.cfg.componentDirs:
            for filterType, map in (('invariant', invariantFilterMap),
                                    ('base', baseFilterMap)):
                oneDir = os.sep.join((componentDir, filterType))
                if not os.path.isdir(oneDir):
                    continue
                for filename in os.listdir(oneDir):
                    fullpath = os.sep.join((oneDir, filename))
                    if (not filename.endswith('.py') or
                        not util.isregular(fullpath)):
                        continue
                    self.loadFilter(filterType, map, filename, fullpath)

        # populate the lists with dependency-sorted information
        for filterType, map, filterList in (
            ('invariant', invariantFilterMap, self.invariantFilters),
            ('base', baseFilterMap, self.baseFilters)):
            dg = graph.DirectedGraph()
            for filterName in map.keys():
                dg.addNode(filterName)
                filter, follows, precedes  = map[filterName]

                def warnMissing(missing):
                    self.error('%s depends on missing %s', filterName, missing)

                for prior in follows:
                    if not prior in map:
                        warnMissing(prior)
                    dg.addEdge(prior, filterName)
                for subsequent in precedes:
                    if not subsequent in map:
                        warnMissing(subsequent)
                    dg.addEdge(filterName, subsequent)

            # test for dependency loops
            depLoops = [x for x in dg.getStronglyConnectedComponents()
                        if len(x) > 1]
            if depLoops:
                self.error('dependency loop(s) in component filters: %s',
                           ' '.join(sorted(':'.join(x)
                                           for x in sorted(list(depLoops)))))
                return

            # Create a stably-sorted list of config filters where
            # the filter is not empty.  (An empty filter with both
            # follows and precedes specified can be used to induce
            # ordering between otherwise unrelated components.)
            #for name in dg.getTotalOrdering(nodeSort=lambda a, b: cmp(a,b)):
            for name in dg.getTotalOrdering():
                filters = map[name][0]
                if not filters:
                    continue

                componentName = filters[0]
                for filterExp in filters[1]:
                    filterList.append((componentName, filterExp))


    def loadFilter(self, filterType, map, filename, fullpath):
        # do not load shared libraries
        desc = [x for x in imp.get_suffixes() if x[0] == '.py'][0]
        f = file(fullpath)
        modname = filename[:-3]
        m = imp.load_module(modname, f, fullpath, desc)
        f.close()

        if not 'filters' in m.__dict__:
            self.warn('%s missing "filters"; not a valid component'
                      ' specification file', fullpath)
            return
        filters = m.__dict__['filters']

        if filters and len(filters) > 1 and type(filters[1]) not in (list,
                                                                     tuple):
            self.error('invalid expression in %s: filters specification'
                       " must be ('name', ('expression', ...))", fullpath)

        follows = ()
        if 'follows' in m.__dict__:
            follows = m.__dict__['follows']

        precedes = ()
        if 'precedes' in m.__dict__:
            precedes = m.__dict__['precedes']

        map[modname] = (filters, follows, precedes)



class PackageSpec(_filterSpec):
    """
    NAME
    ====
    B{C{r.PackageSpec()}} - Determines which package each file is in

    SYNOPSIS
    ========
    C{r.PackageSpec(I{packagename}, I{filterexp})}

    DESCRIPTION
    ===========
    The C{r.PackageSpec()} policy determines which package each file
    is in. (Use C{r.ComponentSpec()} to specify the component without
    specifying the package, or to specify C{I{package}:I{component}}
    in one invocation.)

    EXAMPLES
    ========
    C{r.PackageSpec('openssh-server', '%(sysconfdir)s/pam.d/sshd')}

    Specifies that the file C{%(sysconfdir)s/pam.d/sshd} is in the package
    C{openssh-server} rather than the default (which in this case would have
    been C{openssh} because this example was provided by C{openssh.recipe}).
    """
    requires = (
        ('ComponentSpec', policy.REQUIRED_PRIOR),
    )
    keywords = { 'compFilters': None }

    def __init__(self, *args, **keywords):
        """
        @keyword compFilters: reserved for C{ComponentSpec} to pass information
        needed by C{PackageSpec}.
        """
        _filterSpec.__init__(self, *args, **keywords)
        self.configFiles = []
        self.derivedFilters = []

    def updateArgs(self, *args, **keywords):
        if '_config' in keywords:
            self.configFiles.append(keywords.pop('_config'))
        # keep a list of packages filtered for in PackageSpec in the recipe
        if args:
            newTrove = args[0] % self.recipe.macros

            self.recipe.packages[newTrove] = True
        _filterSpec.updateArgs(self, *args, **keywords)

    def preProcess(self):
        self.pkgFilters = []
        recipe = self.recipe
        self.destdir = recipe.macros.destdir
        if self.exceptions:
            self.warn('PackageSpec does not honor exceptions')
            self.exceptions = None
        if self.inclusions:
            # would have an effect only with exceptions listed, so no warning...
            self.inclusions = None

        # userinfo and groupinfo are invariant filters, so they must come first
        for infoType in ('user', 'group'):
            infoDir = '%%(%sinfodir)s' % infoType % self.macros
            realDir = util.joinPaths(self.destdir, infoDir)
            if not os.path.isdir(realDir):
                continue
            for infoPkgName in os.listdir(realDir):
                pkgPath = util.joinPaths(infoDir, infoPkgName)
                self.pkgFilters.append( \
                        filter.Filter(pkgPath, self.macros,
                                      name = 'info-%s' % infoPkgName))
        # extras need to come before derived so that derived packages
        # can change the package to which a file is assigned
        for filteritem in itertools.chain(self.extraFilters,
                                          self.derivedFilters):
            if not isinstance(filteritem, (filter.Filter, filter.PathSet)):
                name = filteritem[0] % self.macros
                if not trove.troveNameIsValid(name):
                    self.error('%s is not a valid package name', name)

                args, kwargs = self.filterExpArgs(filteritem[1:], name=name)
                self.pkgFilters.append(filter.Filter(*args, **kwargs))
            else:
                self.pkgFilters.append(filteritem)

        # by default, everything that hasn't matched a pattern in the
        # main package filter goes in the package named recipe.name
        self.pkgFilters.append(filter.Filter('.*', self.macros, name=recipe.name))

        # OK, all the filters exist, build an autopackage object that
        # knows about them
        recipe.autopkg = buildpackage.AutoBuildPackage(
            self.pkgFilters, self.compFilters, recipe)
        self.autopkg = recipe.autopkg

    def do(self):
        # Walk capsule contents ignored by doFile
        for filePath, _, componentName in self.recipe._iterCapsulePaths():
            realPath = self.destdir + filePath
            if util.exists(realPath):
                # Files that do not exist on the filesystem (devices)
                # are handled separately
                self.autopkg.addFile(filePath, realPath, componentName)
        # Walk normal files
        _filterSpec.do(self)

    def doFile(self, path):
        # all policy classes after this require that the initial tree is built
        if not self.recipe._getCapsulePathsForFile(path):
            realPath = self.destdir + path
            self.autopkg.addFile(path, realPath)

    def postProcess(self):
        # flag all config files
        for confname in self.configFiles:
            self.recipe.autopkg.pathMap[confname].flags.isConfig(True)




class InitialContents(policy.Policy):
    """
    NAME
    ====
    B{C{r.InitialContents()}} - Mark only explicit inclusions as initial
    contents files

    SYNOPSIS
    ========
    C{InitialContents([I{filterexp}])}

    DESCRIPTION
    ===========
    By default, C{r.InitialContents()} does not apply to any files.
    It is used to specify all files that Conary needs to mark as
    providing only initial contents.  When Conary installs or
    updates one of these files, it will never replace existing
    contents; it uses the provided contents only if the file does
    not yet exist at the time Conary is creating it.

    A file marked as an InitialContents file cannot also be marked
    as a Transient file or a Config file.  Conary enforces this
    requirement.

    EXAMPLES
    ========
    C{r.InitialContents('%(sysconfdir)s/conary/.*gpg')}

    The files C{%(sysconfdir)s/conary/.*gpg} are being marked as initial
    contents files.  Conary will use those contents when creating the files
    the first time, but will never overwrite existing contents in those files.
    """
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('Config', policy.REQUIRED_PRIOR),
    )
    bucket = policy.PACKAGE_CREATION
    processUnmodified = True

    invariantexceptions = [ '%(userinfodir)s/', '%(groupinfodir)s' ]
    invariantinclusions = ['%(localstatedir)s/run/',
                           '%(localstatedir)s/log/',
                           '%(cachedir)s/']

    def postInit(self, *args, **kwargs):
        self.recipe.Config(exceptions = self.invariantinclusions,
                allowUnusedFilters = True)

    def updateArgs(self, *args, **keywords):
        policy.Policy.updateArgs(self, *args, **keywords)
        self.recipe.Config(exceptions=args, allowUnusedFilters = True)

    def doFile(self, filename):
        fullpath = self.macros.destdir + filename
        recipe = self.recipe
        if os.path.isfile(fullpath) and util.isregular(fullpath):
            self.info(filename)
            f = recipe.autopkg.pathMap[filename]
            f.flags.isInitialContents(True)
            if f.flags.isConfig():
                self.error(
                    '%s is marked as both a configuration file and'
                    ' an initial contents file', filename)


class Transient(policy.Policy):
    """
    NAME
    ====
    B{C{r.Transient()}} - Mark files that have transient contents

    SYNOPSIS
    ========
    C{r.Transient([I{filterexp}])}

    DESCRIPTION
    ===========
    The C{r.Transient()} policy marks files as containing transient
    contents. It automatically marks the two most common uses of transient
    contents: python and emacs byte-compiled files
    (C{.pyc}, C{.pyo}, and C{.elc} files).

    Files containing transient contents are almost the opposite of
    configuration files: their contents should be overwritten by
    the new contents without question at update time, even if the
    contents in the filesystem have changed.  (Conary raises an
    error if file contents have changed in the filesystem for normal
    files.)

    A file marked as a Transient file cannot also be marked as an
    InitialContents file or a Config file.  Conary enforces this
    requirement.

    EXAMPLES
    ========
    C{r.Transient('%(libdir)s/firefox/extensions/')}

    Marks all the files in the directory C{%(libdir)s/firefox/extensions/} as
    having transient contents.
    """
    bucket = policy.PACKAGE_CREATION
    filetree = policy.PACKAGE
    processUnmodified = True
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('Config', policy.REQUIRED_PRIOR),
        ('InitialContents', policy.REQUIRED_PRIOR),
    )

    invariantinclusions = [
        r'..*\.py(c|o)$',
        r'..*\.elc$',
        r'%(userinfodir)s/',
        r'%(groupinfodir)s'
    ]

    def doFile(self, filename):
        fullpath = self.macros.destdir + filename
        if os.path.isfile(fullpath) and util.isregular(fullpath):
            recipe = self.recipe
            f = recipe.autopkg.pathMap[filename]
            f.flags.isTransient(True)
            if f.flags.isConfig() or f.flags.isInitialContents():
                self.error(
                    '%s is marked as both a transient file and'
                    ' a configuration or initial contents file', filename)


class TagDescription(policy.Policy):
    """
    NAME
    ====
    B{C{r.TagDescription()}} - Marks tag description files

    SYNOPSIS
    ========
    C{r.TagDescription([I{filterexp}])}

    DESCRIPTION
    ===========
    The C{r.TagDescription} class marks tag description files as
    such so that conary handles them correctly. Every file in
    C{%(tagdescriptiondir)s/} is marked as a tag description file by default.

    No file outside of C{%(tagdescriptiondir)s/} will be considered by this
    policy.

    EXAMPLES
    ========
    This policy is not called explicitly.
    """
    bucket = policy.PACKAGE_CREATION
    processUnmodified = False
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )

    invariantsubtrees = [ '%(tagdescriptiondir)s/' ]

    def doFile(self, path):
        if self.recipe._getCapsulePathsForFile(path):
            return
        fullpath = self.macros.destdir + path
        if os.path.isfile(fullpath) and util.isregular(fullpath):
            self.info('conary tag file: %s', path)
            self.recipe.autopkg.pathMap[path].tags.set("tagdescription")


class TagHandler(policy.Policy):
    """
    NAME
    ====
    B{C{r.TagHandler()}} - Mark tag handler files

    SYNOPSIS
    ========
    C{r.TagHandler([I{filterexp}])}

    DESCRIPTION
    ===========
    All files in C{%(taghandlerdir)s/} are marked as a tag
    handler files.

    EXAMPLES
    ========
    This policy is not called explicitly.
    """
    bucket = policy.PACKAGE_CREATION
    processUnmodified = False
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )
    invariantsubtrees = [ '%(taghandlerdir)s/' ]

    def doFile(self, path):
        if self.recipe._getCapsulePathsForFile(path):
            return
        fullpath = self.macros.destdir + path
        if os.path.isfile(fullpath) and util.isregular(fullpath):
            self.info('conary tag handler: %s', path)
            self.recipe.autopkg.pathMap[path].tags.set("taghandler")


class TagSpec(_addInfo):
    """
    NAME
    ====
    B{C{r.TagSpec()}} - Apply tags defined by tag descriptions

    SYNOPSIS
    ========
    C{r.TagSpec([I{tagname}, I{filterexp}] || [I{tagname}, I{exceptions=filterexp}])}

    DESCRIPTION
    ===========
    The C{r.TagSpec()} policy automatically applies tags defined by tag
    descriptions in both the current system and C{%(destdir)s} to all
    files in C{%(destdir)s}.

    To apply tags manually (removing a dependency on the tag description
    file existing when the packages is cooked), use the syntax:
    C{r.TagSpec(I{tagname}, I{filterexp})}.
    To set an exception to this policy, use:
    C{r.TagSpec(I{tagname}, I{exceptions=filterexp})}.

    EXAMPLES
    ========
    C{r.TagSpec('initscript', '%(initdir)s/')}

    Applies the C{initscript} tag to all files in the directory
    C{%(initdir)s/}.
    """
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )
    def doProcess(self, recipe):
        self.tagList = []
        self.buildReqsComputedForTags = set()
        self.suggestBuildRequires = set()
        # read the system and %(destdir)s tag databases
        for directory in (recipe.macros.destdir+'/etc/conary/tags/',
                          '/etc/conary/tags/'):
            if os.path.isdir(directory):
                for filename in os.listdir(directory):
                    path = util.joinPaths(directory, filename)
                    self.tagList.append(tags.TagFile(path, recipe.macros, True))
        self.fullReqs = self.recipe._getTransitiveBuildRequiresNames()
        _addInfo.doProcess(self, recipe)

    def markTag(self, name, tag, path, tagFile=None):
        # commonly, a tagdescription will nominate a file to be
        # tagged, but it will also be set explicitly in the recipe,
        # and therefore markTag will be called twice.
        if (len(tag.split()) > 1 or
            not tag.replace('-', '').replace('_', '').isalnum()):
            # handlers for multiple tags require strict tag names:
            # no whitespace, only alphanumeric plus - and _ characters
            self.error('illegal tag name %s for file %s' %(tag, path))
            return
        tags = self.recipe.autopkg.pathMap[path].tags
        if tag not in tags:
            self.info('%s: %s', name, path)
            tags.set(tag)
            if tagFile and tag not in self.buildReqsComputedForTags:
                self.buildReqsComputedForTags.add(tag)
                db = self._getDb()
                for trove in db.iterTrovesByPath(tagFile.tagFile):
                    troveName = trove.getName()
                    if troveName not in self.fullReqs:
                        # XXX should be error, change after bootstrap
                        self.warn("%s assigned by %s to file %s, so add '%s'"
                                   ' to buildRequires or call r.TagSpec()'
                                   %(tag, tagFile.tagFile, path, troveName))
                        self.suggestBuildRequires.add(troveName)

    def runInfo(self, path):
        if self.recipe._getCapsulePathsForFile(path):
            # capsules do not participate in the tag protocol
            return
        excludedTags = {}
        for tag in self.included:
            for filt in self.included[tag]:
                if filt.match(path):
                    isExcluded = False
                    if tag in self.excluded:
                        for filt in self.excluded[tag]:
                            if filt.match(path):
                                s = excludedTags.setdefault(tag, set())
                                s.add(path)
                                isExcluded = True
                                break
                    if not isExcluded:
                        self.markTag(tag, tag, path)

        for tag in self.tagList:
            if tag.match(path):
                if tag.name:
                    name = tag.name
                else:
                    name = tag.tag
                isExcluded = False
                if tag.tag in self.excluded:
                    for filt in self.excluded[tag.tag]:
                        # exception handling is per-tag, so handled specially
                        if filt.match(path):
                            s = excludedTags.setdefault(name, set())
                            s.add(path)
                            isExcluded = True
                            break
                if not isExcluded:
                    self.markTag(name, tag.tag, path, tag)
        if excludedTags:
            for tag in excludedTags:
                self.info('ignoring tag match for %s: %s',
                          tag, ', '.join(sorted(excludedTags[tag])))

    def postProcess(self):
        if self.suggestBuildRequires:
            self.info('possibly add to buildRequires: %s',
                      str(sorted(list(self.suggestBuildRequires))))
            self.recipe.reportMissingBuildRequires(self.suggestBuildRequires)


class Properties(policy.Policy):
    """
    NAME
    ====
    B{C{r.Properties()}} - Read property definition files

    SYNOPSIS
    ========
    C{r.Properties(I{exceptions=filterexp} || [I{contents=xml},
                   I{package=pkg:component}] ||
                   [I{/path/to/file}, I{filterexp}], I{contents=ipropcontents})}

    DESCRIPTION
    ===========
    The C{r.Properties()} policy automatically parses iconfig property
    definition files, making the properties available for configuration
    management with iconfig.

    To add configuration properties manually, use the syntax:
    C{r.Properties(I{contents=ipropcontents}, I{package=pkg:component}}
    Where contents is the xml string that would normally be stored in the iprop
    file and package is the component where to attach the config metadata.
    (NOTE: This component must exist)

    or

    C{r.Properties([I{/path/to/file}, I{filterexp}], I{contents=ipropcontents})
    Where contents is the xml string that would normally be stored in the iprop
    file and the path or filterexp matches the files that represent the
    conponent that the property should be attached to.
    """
    supported_targets = (TARGET_LINUX, TARGET_WINDOWS)
    bucket = policy.PACKAGE_CREATION
    processUnmodified = True
    _supports_file_properties = True
    requires = (
        # We need to know what component files have been assigned to
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )

    def __init__(self, *args, **kwargs):
        policy.Policy.__init__(self, *args, **kwargs)

        self.ipropFilters = []
        self.ipropPaths = [ r'%(prefix)s/lib/iconfig/properties/.*\.iprop' ]
        self.contents = []
        self.paths = []
        self.fileFilters = []
        self.propMap = {}

    def updateArgs(self, *args, **kwargs):
        if 'contents' in kwargs:
            contents = kwargs.pop('contents')
            pkg = kwargs.pop('package', None)

            if pkg is None and args:
                for arg in args:
                    self.paths.append((arg, contents))
            else:
                self.contents.append((pkg, contents))

        policy.Policy.updateArgs(self, *args, **kwargs)

    def doProcess(self, recipe):
        for filterSpec, iprop in self.paths:
            self.fileFilters.append((
                filter.Filter(filterSpec, recipe.macros),
                iprop,
            ))
        for ipropPath in self.ipropPaths:
            self.ipropFilters.append(
                filter.Filter(ipropPath, recipe.macros))
        policy.Policy.doProcess(self, recipe)

    def _getComponent(self, path):
        componentMap = self.recipe.autopkg.componentMap
        if path not in componentMap:
            return
        main, comp = componentMap[path].getName().split(':')
        return main, comp

    def doFile(self, path):
        if path not in self.recipe.autopkg.pathMap:
            return

        for fltr, iprop in self.fileFilters:
            if fltr.match(path):
                main, comp = self._getComponent(path)
                self._parsePropertyData(iprop, main, comp)

        # Make sure any remaining files are actually in the root.
        fullpath = self.recipe.macros.destdir + path
        if not os.path.isfile(fullpath) or not util.isregular(fullpath):
            return

        # Check to see if this is an iprop file locaiton that we know about.
        for fltr in self.ipropFilters:
            if fltr.match(path):
                break
        else:
            return

        main, comp = self._getComponent(path)
        xml = open(fullpath).read()
        self._parsePropertyData(xml, main, comp)

    def postProcess(self):
        for pkg, content in self.contents:
            pkg = pkg % self.macros
            pkgName, compName = pkg.split(':')
            self._parsePropertyData(content, pkgName, compName)

    def _parsePropertyData(self, xml, pkgName, compName):
        pkgSet = self.propMap.setdefault(xml, set())
        if (pkgName, compName) in pkgSet:
            return

        pkgSet.add((pkgName, compName))
        self.recipe._addProperty(trove._PROPERTY_TYPE_SMARTFORM, pkgName,
            compName, xml)


class MakeDevices(policy.Policy):
    """
    NAME
    ====
    B{C{r.MakeDevices()}} - Make device nodes

    SYNOPSIS
    ========
    C{MakeDevices([I{path},] [I{type},] [I{major},] [I{minor},] [I{owner},] [I{groups},] [I{mode}])}

    DESCRIPTION
    ===========
    The C{r.MakeDevices()} policy creates device nodes.  Conary's
    policy of non-root builds requires that these nodes exist only in the
    package, and not in the filesystem, as only root may actually create
    device nodes.


    EXAMPLES
    ========
    C{r.MakeDevices(I{'/dev/tty', 'c', 5, 0, 'root', 'root', mode=0666, package=':dev'})}

    Creates the device node C{/dev/tty}, as type 'c' (character, as opposed to
    type 'b', or block) with a major number of '5', minor number of '0',
    owner, and group are both the root user, and permissions are 0666.
    """
    bucket = policy.PACKAGE_CREATION
    processUnmodified = True
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('Ownership', policy.REQUIRED_SUBSEQUENT),
    )

    def __init__(self, *args, **keywords):
        self.devices = []
        policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
        """
        MakeDevices(path, devtype, major, minor, owner, group, mode=0400)
        """
        if args:
            args = list(args)
            l = len(args)
            if not ((l > 5) and (l < 9)):
                self.recipe.error('MakeDevices: incorrect arguments: %r %r'
                    %(args, keywords))
            mode = keywords.pop('mode', None)
            package = keywords.pop('package', None)
            if l > 6 and mode is None:
                mode = args[6]
            if mode is None:
                mode = 0400
            if l > 7 and package is None:
                package = args[7]
            self.devices.append(
                (args[0:6], {'perms': mode, 'package': package}))
        policy.Policy.updateArgs(self, **keywords)

    def do(self):
        for device, kwargs in self.devices:
            r = self.recipe
            filename = device[0]
            owner = device[4]
            group = device[5]
            r.Ownership(owner, group, filename)
            device[0] = device[0] % r.macros
            r.autopkg.addDevice(*device, **kwargs)


class setModes(policy.Policy):
    """
    Do not call from recipes; this is used internally by C{r.SetModes},
    C{r.ParseManifest}, and unpacking derived packages.  This policy
    modified modes relative to the mode on the file in the filesystem.
    It adds setuid/setgid bits not otherwise set/honored on files on the
    filesystem, and sets user r/w/x bits if they were altered for the
    purposes of accessing the files during packaging.  Otherwise,
    it honors the bits found on the filesystem.  It does not modify
    bits in capsules.
    """
    bucket = policy.PACKAGE_CREATION
    processUnmodified = True
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('WarnWriteable', policy.REQUIRED_SUBSEQUENT),
        ('ExcludeDirectories', policy.CONDITIONAL_SUBSEQUENT),
    )
    def __init__(self, *args, **keywords):
        self.sidbits = {}
        self.userbits = {}
        policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
        """
        setModes(path(s), [sidbits=int], [userbits=int])
        """
        sidbits = keywords.pop('sidbits', None)
        userbits = keywords.pop('userbits', None)
        for path in args:
            if sidbits is not None:
                self.sidbits[path] = sidbits
            if userbits is not None:
                self.userbits[path] = userbits
                self.recipe.WarnWriteable(
                    exceptions=re.escape(path).replace('%', '%%'),
                    allowUnusedFilters = True)
        policy.Policy.updateArgs(self, **keywords)

    def doFile(self, path):
        # Don't set modes on capsule files
        if self.recipe._getCapsulePathsForFile(path):
            return
        # Skip files that aren't part of the package
        if path not in self.recipe.autopkg.pathMap:
            return
        newmode = oldmode = self.recipe.autopkg.pathMap[path].inode.perms()
        if path in self.userbits:
            newmode = (newmode & 077077) | self.userbits[path]
        if path in self.sidbits and self.sidbits[path]:
            newmode |= self.sidbits[path]
            self.info('suid/sgid: %s mode 0%o', path, newmode & 07777)
        if newmode != oldmode:
            self.recipe.autopkg.pathMap[path].inode.perms.set(newmode)


class LinkType(policy.Policy):
    """
    NAME
    ====
    B{C{r.LinkType()}} - Ensures only regular, non-configuration files are hardlinked

    SYNOPSIS
    ========
    C{r.LinkType([I{filterexp}])}

    DESCRIPTION
    ===========
    The C{r.LinkType()} policy ensures that only regular, non-configuration
    files are hardlinked.


    EXAMPLES
    ========
    This policy is not called explicitly.
    """
    bucket = policy.PACKAGE_CREATION
    processUnmodified = True
    requires = (
        ('Config', policy.REQUIRED_PRIOR),
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )
    def do(self):
        for component in self.recipe.autopkg.getComponents():
            for path in sorted(component.hardlinkMap.keys()):
                if self.recipe.autopkg.pathMap[path].flags.isConfig():
                    self.error("Config file %s has illegal hard links", path)
            for path in component.badhardlinks:
                self.error("Special file %s has illegal hard links", path)


class LinkCount(policy.Policy):
    """
    NAME
    ====
    B{C{r.LinkCount()}} - Restricts hardlinks across directories.

    SYNOPSIS
    ========
    C{LinkCount([I{filterexp}] | [I{exceptions=filterexp}])}

    DESCRIPTION
    ===========
    The C{r.LinkCount()} policy restricts hardlinks across directories.

    It is generally an error to have hardlinks across directories, except when
    the packager knows that there is no reasonable chance that they will be on
    separate filesystems.

    In cases where the packager is certain hardlinks will not cross
    filesystems, a list of regular expressions specifying files
    which are excepted from this rule may be passed to C{r.LinkCount}.

    EXAMPLES
    ========
    C{r.LinkCount(exceptions='/usr/share/zoneinfo/')}

    Uses C{r.LinkCount} to except zoneinfo files, located in
    C{/usr/share/zoneinfo/}, from the policy against cross-directory
    hardlinks.
    """
    bucket = policy.PACKAGE_CREATION
    processUnmodified = False
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )
    def __init__(self, *args, **keywords):
        policy.Policy.__init__(self, *args, **keywords)
        self.excepts = set()

    def updateArgs(self, *args, **keywords):
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
        # FIXME: we may want to have another keyword argument
        # that passes information down to the buildpackage
        # that causes link groups to be broken for some
        # directories but not others.  We need to research
        # first whether this is useful; it may not be.

    def do(self):
        if self.recipe.getType() == recipe.RECIPE_TYPE_CAPSULE:
            return
        filters = [(x, filter.Filter(x, self.macros)) for x in self.excepts]
        for component in self.recipe.autopkg.getComponents():
            for inode in component.linkGroups:
                # ensure all in same directory, except for directories
                # matching regexps that have been passed in

                allPaths = [x for x in component.linkGroups[inode]]
                for path in allPaths[:]:
                    for regexp, f in filters:
                        if f.match(path):
                            self.unusedFilters['exceptions'].discard(regexp)
                            allPaths.remove(path)
                dirSet = set(os.path.dirname(x) + '/' for x in allPaths)

                if len(dirSet) > 1:
                    self.error('files %s are hard links across directories %s',
                               ', '.join(sorted(component.linkGroups[inode])),
                               ', '.join(sorted(list(dirSet))))
                    self.error('If these directories cannot reasonably be'
                               ' on different filesystems, disable this'
                               ' warning by calling'
                               " r.LinkCount(exceptions=('%s')) or"
                               " equivalent"
                               % "', '".join(sorted(list(dirSet))))


class ExcludeDirectories(policy.Policy):
    """
    NAME
    ====
    B{C{r.ExcludeDirectories()}} - Exclude directories from package

    SYNOPSIS
    ========
    C{r.ExcludeDirectories([I{filterexp}] | [I{exceptions=filterexp}])}

    DESCRIPTION
    ===========
    The C{r.ExcludeDirectories} policy causes directories to be
    excluded from the package by default.  Use
    C{r.ExcludeDirectories(exceptions=I{filterexp})} to set exceptions to this
    policy, which will cause directories matching the regular expression
    C{filterexp} to be included in the package.  Remember that Conary
    packages cannot share files, including directories, so only one
    package installed on a system at any one time can own the same
    directory.

    There are only three reasons to explicitly package a directory: the
    directory needs permissions other than 0755, it needs non-root owner
    or group, or it must exist even if it is empty.

    Therefore, it should generally not be necessary to invoke this policy
    directly.  If your directory requires permissions other than 0755, simply
    use C{r.SetMode} to specify the permissions, and the directory will be
    automatically included.  Similarly, if you wish to include an empty
    directory with owner or group information, call C{r.Ownership} on that
    empty directory,

    Because C{r.Ownership} can reasonably be called on an entire
    subdirectory tree and indiscriminately applied to files and
    directories alike, non-empty directories with owner or group
    set will be excluded from packaging unless an exception is
    explicitly provided.

    If you call C{r.Ownership} with a filter that applies to an
    empty directory, but you do not want to package that directory,
    you will have to remove the directory with C{r.Remove}.

    Packages do not need to explicitly include directories to ensure
    existence of a target to place a file in. Conary will appropriately
    create the directory, and delete it later if the directory becomes empty.

    EXAMPLES
    ========
    C{r.ExcludeDirectories(exceptions='/tftpboot')}

    Sets the directory C{/tftboot} as an exception to the
    C{r.ExcludeDirectories} policy, so that the C{/tftpboot}
    directory will be included in the package.
    """
    bucket = policy.PACKAGE_CREATION
    processUnmodified = True
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('Ownership', policy.REQUIRED_PRIOR),
        ('MakeDevices', policy.CONDITIONAL_PRIOR),
    )
    invariantinclusions = [ ('.*', stat.S_IFDIR) ]
    supported_targets = (TARGET_LINUX, TARGET_WINDOWS)

    def doFile(self, path):
        # temporarily do nothing for capsules, we might do something later
        if self.recipe._getCapsulePathsForFile(path):
            return
        fullpath = self.recipe.macros.destdir + os.sep + path
        s = os.lstat(fullpath)
        mode = s[stat.ST_MODE]

        if mode & 0777 != 0755:
            self.info('excluding directory %s with mode %o', path, mode&0777)
        elif not os.listdir(fullpath):
            d = self.recipe.autopkg.pathMap[path]
            if d.inode.owner.freeze() != 'root':
                self.info('not excluding empty directory %s'
                          ' because of non-root owner', path)
                return
            elif d.inode.group.freeze() != 'root':
                self.info('not excluding empty directory %s'
                          ' because of non-root group', path)
                return
            self.info('excluding empty directory %s', path)
            # if its empty and we're not packaging it, there's no need for it
            # to continue to exist on the filesystem to potentially confuse
            # other policy actions... see CNP-18
            os.rmdir(fullpath)
        self.recipe.autopkg.delFile(path)


class ByDefault(policy.Policy):
    """
    NAME
    ====
    B{C{r.ByDefault()}} - Determines components to be installed by default

    SYNOPSIS
    ========
    C{r.ByDefault([I{inclusions} || C{exceptions}=I{exceptions}])}

    DESCRIPTION
    ===========
    The C{r.ByDefault()} policy determines which components should
    be installed by default at the time the package is installed on the
    system.  The default setting for the C{ByDefault} policy is that the
    C{:debug}, and C{:test} packages are not installed with the package.

    The inclusions and exceptions do B{not} specify filenames.  They are
    either C{I{package}:I{component}} or C{:I{component}}.  Inclusions
    are considered before exceptions, and inclusions and exceptions are
    considered in the order provided in the recipe, and first match wins.

    EXAMPLES
    ========
    C{r.ByDefault(exceptions=[':manual'])}

    Uses C{r.ByDefault} to ignore C{:manual} components when enforcing the
    policy.

    C{r.ByDefault(exceptions=[':manual'])}
    C{r.ByDefault('foo:manual')}

    If these lines are in the C{bar} package, and there is both a
    C{foo:manual} and a C{bar:manual} component, then the C{foo:manual}
    component will be installed by default when the C{foo} package is
    installed, but the C{bar:manual} component will not be installed by
    default when the C{bar} package is installed.
    """
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )
    filetree = policy.NO_FILES
    supported_targets = (TARGET_LINUX, TARGET_WINDOWS)

    invariantexceptions = [':test', ':debuginfo']

    allowUnusedFilters = True

    def doProcess(self, recipe):
        if not self.inclusions:
            self.inclusions = []
        if not self.exceptions:
            self.exceptions = []
        recipe.setByDefaultOn(frozenset(self.inclusions))
        recipe.setByDefaultOff(frozenset(self.exceptions +
                                         self.invariantexceptions))


class _UserGroup(policy.Policy):
    """
    Abstract base class that implements marking owner/group dependencies.
    """
    bucket = policy.PACKAGE_CREATION
    # All classes that descend from _UserGroup must run before the
    # Requires policy, as they implicitly depend on it to set the
    # file requirements and union the requirements up to the package.
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('Requires', policy.REQUIRED_SUBSEQUENT),
    )
    filetree = policy.PACKAGE
    processUnmodified = True

    def setUserGroupDep(self, path, info, depClass):
        componentMap = self.recipe.autopkg.componentMap
        if path not in componentMap:
            return
        pkg = componentMap[path]
        f = pkg.getFile(path)
        if path not in pkg.requiresMap:
            pkg.requiresMap[path] = deps.DependencySet()
        pkg.requiresMap[path].addDep(depClass, deps.Dependency(info, []))


class Ownership(_UserGroup):
    """
    NAME
    ====
    B{C{r.Ownership()}} - Set file ownership

    SYNOPSIS
    ========
    C{r.Ownership([I{username},] [I{groupname},] [I{filterexp}])}

    DESCRIPTION
    ===========
    The C{r.Ownership()} policy sets user and group ownership of files when
    the default of C{root:root} is not appropriate.

    List the ownerships in order, most specific first, ending with least
    specific. The filespecs will be matched in the order that you provide them.

    KEYWORDS
    ========
    None.

    EXAMPLES
    ========
    C{r.Ownership('apache', 'apache', '%(localstatedir)s/lib/php/session')}

    Sets ownership of C{%(localstatedir)s/lib/php/session} to owner
    C{apache}, and group C{apache}.
    """

    def __init__(self, *args, **keywords):
        self.filespecs = []
        self.systemusers = ('root',)
        self.systemgroups = ('root',)
        policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
        if args:
            for filespec in args[2:]:
                self.filespecs.append((filespec, args[0], args[1]))
        policy.Policy.updateArgs(self, **keywords)

    def doProcess(self, recipe):
        # we must NEVER take ownership from the filesystem
        assert(not self.exceptions)
        self.rootdir = self.rootdir % recipe.macros
        self.fileFilters = []
        for (filespec, user, group) in self.filespecs:
            self.fileFilters.append(
                (filter.Filter(filespec, recipe.macros),
                 user %recipe.macros,
                 group %recipe.macros))
        del self.filespecs
        policy.Policy.doProcess(self, recipe)

    def doFile(self, path):
        if self.recipe._getCapsulePathsForFile(path):
            return

        pkgfile = self.recipe.autopkg.pathMap[path]
        pkgOwner = pkgfile.inode.owner()
        pkgGroup = pkgfile.inode.group()
        bestOwner = pkgOwner
        bestGroup = pkgGroup
        for (f, owner, group) in self.fileFilters:
            if f.match(path):
                bestOwner, bestGroup = owner, group
                break

        if bestOwner != pkgOwner:
            pkgfile.inode.owner.set(bestOwner)
        if bestGroup != pkgGroup:
            pkgfile.inode.group.set(bestGroup)

        if bestOwner and bestOwner not in self.systemusers:
            self.setUserGroupDep(path, bestOwner, deps.UserInfoDependencies)
        if bestGroup and bestGroup not in self.systemgroups:
            self.setUserGroupDep(path, bestGroup, deps.GroupInfoDependencies)

class _Utilize(_UserGroup):
    """
    Pure virtual base class for C{UtilizeUser} and C{UtilizeGroup}
    """
    def __init__(self, *args, **keywords):
        self.filespecs = []
        policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
        """
        call as::
          UtilizeFoo(item, filespec(s)...)
        List them in order, most specific first, ending with most
        general; the filespecs will be matched in the order that
        you provide them.
        """
        item = args[0] % self.recipe.macros
        if args:
            for filespec in args[1:]:
                self.filespecs.append((filespec, item))
        policy.Policy.updateArgs(self, **keywords)

    def doProcess(self, recipe):
        self.rootdir = self.rootdir % recipe.macros
        self.fileFilters = []
        for (filespec, item) in self.filespecs:
            self.fileFilters.append(
                (filter.Filter(filespec, recipe.macros), item))
        del self.filespecs
        policy.Policy.doProcess(self, recipe)

    def doFile(self, path):
        for (f, item) in self.fileFilters:
            if f.match(path):
                self._markItem(path, item)
        return

    def _markItem(self, path, item):
        # pure virtual
        assert(False)


class UtilizeUser(_Utilize):
    """
    NAME
    ====
    B{C{r.UtilizeUser()}} - Marks files as requiring a user definition to exist

    SYNOPSIS
    ========
    C{r.UtilizeUser([I{username}, I{filterexp}])}

    DESCRIPTION
    ===========
    The C{r.UtilizeUser} policy marks files as requiring a user definition
    to exist even though the file is not owned by that user.

    This is particularly useful for daemons that are setuid root
    ant change their user id to a user id with no filesystem permissions
    after they start.

    EXAMPLES
    ========
    C{r.UtilizeUser('sshd', '%(sbindir)s/sshd')}

    Marks the file C{%(sbindir)s/sshd} as requiring the user definition
    'sshd' although the file is not owned by the 'sshd' user.
    """
    def _markItem(self, path, user):
        if not self.recipe._getCapsulePathsForFile(path):
            self.info('user %s: %s' % (user, path))
            self.setUserGroupDep(path, user, deps.UserInfoDependencies)


class UtilizeGroup(_Utilize):
    """
    NAME
    ====
    B{C{r.UtilizeGroup()}} - Marks files as requiring a user definition to
    exist

    SYNOPSIS
    ========
    C{r.UtilizeGroup([groupname, filterexp])}

    DESCRIPTION
    ===========
    The C{r.UtilizeGroup} policy marks files as requiring a group definition
    to exist even though the file is not owned by that group.

    This is particularly useful for daemons that are setuid root
    ant change their user id to a group id with no filesystem permissions
    after they start.

    EXAMPLES
    ========
    C{r.UtilizeGroup('users', '%(sysconfdir)s/default/useradd')}

    Marks the file C{%(sysconfdir)s/default/useradd} as requiring the group
    definition 'users' although the file is not owned by the 'users' group.
    """
    def _markItem(self, path, group):
        if not self.recipe._getCapsulePathsForFile(path):
            self.info('group %s: %s' % (group, path))
            self.setUserGroupDep(path, group, deps.GroupInfoDependencies)


class ComponentRequires(policy.Policy):
    """
    NAME
    ====
    B{C{r.ComponentRequires()}} - Create automatic intra-package,
    inter-component dependencies

    SYNOPSIS
    ========
    C{r.ComponentRequires([{'I{componentname}': I{requiringComponentSet}}] |
    [{'I{packagename}': {'I{componentname}': I{requiringComponentSet}}}])}

    DESCRIPTION
    ===========
    The C{r.ComponentRequires()} policy creates automatic,
    intra-package, inter-component dependencies, such as a corresponding
    dependency between C{:lib} and C{:data} components.

    Changes are passed in using dictionaries, both for additions that
    are specific to a specific package, and additions that apply
    generally to all binary packages being cooked from one recipe.
    For general changes that are not specific to a package, use this syntax:
    C{r.ComponentRequires({'I{componentname}': I{requiringComponentSet}})}.
    For package-specific changes, you need to specify packages as well
    as components:
    C{r.ComponentRequires({'I{packagename}': 'I{componentname}': I{requiringComponentSet}})}.

    By default, both C{:lib} and C{:runtime} components (if they exist)
    require the C{:data} component (if it exists).  If you call
    C{r.ComponentRequires({'data': set(('lib',))})}, you limit it
    so that C{:runtime} components will not require C{:data} components
    for this recipe.

    In recipes that create more than one binary package, you may need
    to limit your changes to a single binary package.  To do so, use
    the package-specific syntax.  For example, to remove the C{:runtime}
    requirement on C{:data} only for the C{foo} package, call:
    C{r.ComponentRequires({'foo': 'data': set(('lib',))})}.

    Note that C{r.ComponentRequires} cannot require capability flags; use
    C{r.Requires} if you need to specify requirements, including capability
    flags.


    EXAMPLES
    ========
    C{r.ComponentRequires({'openssl': {'config': set(('runtime', 'lib'))}})}

    Uses C{r.ComponentRequires} to create dependencies in a top-level manner
    for the C{:runtime} and C{:lib} component sets to require the
    C{:config} component for the C{openssl} package.
    """
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('ExcludeDirectories', policy.CONDITIONAL_PRIOR),
    )
    supported_targets = (TARGET_LINUX, TARGET_WINDOWS)

    def __init__(self, *args, **keywords):
        self.depMap = {
            # component: components that require it if they both exist
            'data': frozenset(('lib', 'runtime', 'devellib', 'cil', 'java',
                'perl', 'python', 'ruby')),
            'devellib': frozenset(('devel',)),
            'lib': frozenset(('devel', 'devellib', 'runtime')),
            'config': frozenset(('runtime', 'lib', 'devellib', 'devel')),
        }
        self.overridesMap = {}
        policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
        d = args[0]
        if isinstance(d[d.keys()[0]], dict): # dict of dicts
            for packageName in d:
                if packageName not in self.overridesMap:
                    # start with defaults, then override them individually
                    o = {}
                    o.update(self.depMap)
                    self.overridesMap[packageName] = o
                self.overridesMap[packageName].update(d[packageName])
        else: # dict of sets
            self.depMap.update(d)

    def do(self):
        flags = []
        if self.recipe.isCrossCompileTool():
            flags.append((_getTargetDepFlag(self.macros), deps.FLAG_SENSE_REQUIRED))
        components = self.recipe.autopkg.components
        for packageName in [x.name for x in self.recipe.autopkg.packageMap]:
            if packageName in self.overridesMap:
                d = self.overridesMap[packageName]
            else:
                d = self.depMap
            for requiredComponent in d:
                for requiringComponent in d[requiredComponent]:
                    reqName = ':'.join((packageName, requiredComponent))
                    wantName = ':'.join((packageName, requiringComponent))
                    if (reqName in components and wantName in components and
                        components[reqName] and components[wantName]):
                        if (d == self.depMap and
                            reqName in self.recipe._componentReqs and
                            wantName in self.recipe._componentReqs):
                            # this is an automatically generated dependency
                            # which was not in the parent of a derived
                            # pacakge. don't add it here either
                            continue

                        # Note: this does not add dependencies to files;
                        # these dependencies are insufficiently specific
                        # to attach to files.
                        ds = deps.DependencySet()
                        depClass = deps.TroveDependencies

                        ds.addDep(depClass, deps.Dependency(reqName, flags))
                        p = components[wantName]
                        p.requires.union(ds)


class ComponentProvides(policy.Policy):
    """
    NAME
    ====
    B{C{r.ComponentProvides()}} - Causes each trove to explicitly provide
    itself.

    SYNOPSIS
    ========
    C{r.ComponentProvides(I{flags})}

    DESCRIPTION
    ===========
    The C{r.ComponentProvides()} policy causes each trove to explicitly
    provide its name.  Call it to provide optional capability flags
    consisting of a single string, or a list, tuple, or set of strings,
    It is impossible to provide a capability flag for one component but
    not another within a single package.

    EXAMPLES
    ========
    C{r.ComponentProvides("addcolumn")}

    Uses C{r.ComponentProvides} in the context of the sqlite recipe, and
    causes sqlite to provide itself explicitly with the capability flag
    C{addcolumn}.
    """
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('ExcludeDirectories', policy.CONDITIONAL_PRIOR),
    )
    supported_targets = (TARGET_LINUX, TARGET_WINDOWS)

    def __init__(self, *args, **keywords):
        self.flags = set()
        self.excepts = set()
        policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
        if 'exceptions' in keywords:
            exceptions = keywords.pop('exceptions')
            if type(exceptions) is str:
                self.excepts.add(exceptions)
            elif type(exceptions) in (tuple, list):
                self.excepts.update(set(exceptions))

        if not args:
            return
        if len(args) >= 2:
            # update the documentation if we ever support the
            # pkgname, flags calling convention
            #pkgname = args[0]
            flags = args[1]
        else:
            flags = args[0]
        if not isinstance(flags, (list, tuple, set)):
            flags=(flags,)
        self.flags |= set(flags)

    def do(self):
        self.excepts = set(re.compile(x) for x in self.excepts)
        self.flags = set(x for x in self.flags
                         if not [y.match(x) for y in self.excepts])

        if self.flags:
            flags = [ (x % self.macros, deps.FLAG_SENSE_REQUIRED)
                      for x in self.flags ]
        else:
            flags = []
        if self.recipe.isCrossCompileTool():
            flags.append(('target-%s' % self.macros.target,
                          deps.FLAG_SENSE_REQUIRED))

        for component in self.recipe.autopkg.components.values():
            component.provides.addDep(deps.TroveDependencies,
                deps.Dependency(component.name, flags))


def _getTargetDepFlag(macros):
    return 'target-%s' % macros.target

class _dependency(policy.Policy):
    """
    Internal class for shared code between Provides and Requires
    """

    def __init__(self, *args, **kwargs):
        # bootstrap keeping only one copy of these around
        self.bootstrapPythonFlags = None
        self.bootstrapSysPath = []
        self.bootstrapPerlIncPath = []
        self.bootstrapRubyLibs = []
        self.cachedProviders = {}
        self.pythonFlagNamespace = None
        self.removeFlagsByDependencyClass = None # pre-transform
        self.removeFlagsByDependencyClassMap = {}

    def updateArgs(self, *args, **keywords):
        removeFlagsByDependencyClass = keywords.pop(
            'removeFlagsByDependencyClass', None)
        if removeFlagsByDependencyClass is not None:
            clsName, ignoreFlags = removeFlagsByDependencyClass
            cls = deps.dependencyClassesByName[clsName]
            l = self.removeFlagsByDependencyClassMap.setdefault(cls, [])
            if isinstance(ignoreFlags, (list, set, tuple)):
                l.append(set(ignoreFlags))
            else:
                l.append(re.compile(ignoreFlags))
        policy.Policy.updateArgs(self, **keywords)

    def preProcess(self):
        self.CILPolicyRE = re.compile(r'.*mono/.*/policy.*/policy.*\.config$')
        self.legalCharsRE = re.compile('[.0-9A-Za-z_+-/]')
        self.pythonInterpRE = re.compile(r'\.[a-z]+-\d\dm?')

        # interpolate macros, using canonical path form with no trailing /
        self.sonameSubtrees = set(os.path.normpath(x % self.macros)
                                  for x in self.sonameSubtrees)
        self.pythonFlagCache = {}
        self.pythonTroveFlagCache = {}
        self.pythonVersionCache = {}

    def _hasContents(self, m, contents):
        """
        Return False if contents is set and m does not have that contents
        """
        if contents and (contents not in m.contents or not m.contents[contents]):
            return False
        return True

    def _isELF(self, m, contents=None):
        "Test whether is ELF file and optionally has certain contents"
        # Note: for provides, check for 'abi' not 'provides' because we
        # can provide the filename even if there is no provides list
        # as long as a DT_NEEDED entry has been present to set the abi
        return m and m.name == 'ELF' and self._hasContents(m, contents)

    def _isPython(self, path):
        return path.endswith('.py') or path.endswith('.pyc')

    def _isPythonModuleCandidate(self, path):
        return path.endswith('.so') or self._isPython(path)

    def _runPythonScript(self, binPath, destdir, libdir, scriptLines):
        script = '\n'.join(scriptLines)
        environ = {}
        if binPath.startswith(destdir):
            environ['LD_LIBRARY_PATH'] = destdir + libdir
        proc = subprocess.Popen([binPath, '-Ec', script],
                executable=binPath,
                stdout=subprocess.PIPE,
                shell=False,
                env=environ,
                )
        stdout, _ = proc.communicate()
        if proc.returncode:
            raise RuntimeError("Process exited with status %s" %
                    (proc.returncode,))
        return stdout

    def _getPythonVersion(self, pythonPath, destdir, libdir):
        if pythonPath not in self.pythonVersionCache:
            try:
                stdout = self._runPythonScript(pythonPath, destdir, libdir,
                        ["import sys", "print('%d.%d' % sys.version_info[:2])"])
                self.pythonVersionCache[pythonPath] = stdout.strip()
            except (OSError, RuntimeError):
                self.warn("Unable to determine Python version directly; "
                        "guessing based on path.")
                self.pythonVersionCache[pythonPath] = self._getPythonVersionFromPath(pythonPath, destdir)
        return self.pythonVersionCache[pythonPath]

    def _getPythonSysPath(self, pythonPath, destdir, libdir, useDestDir=False):
        """Return the system path for the python interpreter at C{pythonPath}

        @param pythonPath: Path to the target python interpreter
        @param destdir: Destination root, in case of a python bootstrap
        @param libdir: Destination libdir, in case of a python bootstrap
        @param useDestDir: If True, look in the destdir instead.
        """
        script = ["import sys, site"]
        if useDestDir:
            # Repoint site.py at the destdir so it picks up .pth files there.
            script.extend([
                    "sys.path = []",
                    "sys.prefix = %r + sys.prefix" % (destdir,),
                    "sys.exec_prefix = %r + sys.exec_prefix" % (destdir,),
                    "site.PREFIXES = [sys.prefix, sys.exec_prefix]",
                    "site.addsitepackages(None)",
                    ])
        script.append(r"print('\0'.join(sys.path))")

        try:
            stdout = self._runPythonScript(pythonPath, destdir, libdir, script)
        except (OSError, RuntimeError):
            # something went wrong, don't trust any output
            self.info('Could not run system python "%s", guessing sys.path...',
                      pythonPath)
            sysPath = []
        else:
            sysPath = [x.strip() for x in stdout.split('\0') if x.strip()]

        if not sysPath and not useDestDir:
            # probably a cross-build -- let's try a decent assumption
            # for the syspath.
            self.info("Failed to detect system python path, using fallback")
            pyVer = self._getPythonVersionFromPath(pythonPath, destdir)
            if not pyVer and self.bootstrapPythonFlags is not None:
                pyVer = self._getPythonVersionFromFlags(
                    self.bootstrapPythonFlags)
            if pyVer and self.bootstrapSysPath is not None:
                lib = self.recipe.macros.lib
                # this list needs to include all sys.path elements that
                # might be needed for python per se -- note that
                # bootstrapPythonFlags and bootstrapSysPath go
                # together
                sysPath = self.bootstrapSysPath + [
                    '/usr/%s/%s' %(lib, pyVer),
                    '/usr/%s/%s/plat-linux2' %(lib, pyVer),
                    '/usr/%s/%s/lib-tk' %(lib, pyVer),
                    '/usr/%s/%s/lib-dynload' %(lib, pyVer),
                    '/usr/%s/%s/site-packages' %(lib, pyVer),
                    # for purelib python on x86_64
                    '/usr/lib/%s/site-packages' %pyVer,
                ]
        return sysPath

    def _warnPythonPathNotInDB(self, pathName):
        self.warn('%s found on system but not provided by'
                  ' system database; python requirements'
                  ' may be generated incorrectly as a result', pathName)
        return set([])

    def _getPythonTroveFlags(self, pathName):
        if pathName in self.pythonTroveFlagCache:
            return self.pythonTroveFlagCache[pathName]
        db = self._getDb()
        foundPath = False
        pythonFlags = set()
        pythonTroveList = db.iterTrovesByPath(pathName)
        if pythonTroveList:
            depContainer = pythonTroveList[0]
            assert(depContainer.getName())
            foundPath = True
            for dep in depContainer.getRequires().iterDepsByClass(
                    deps.PythonDependencies):
                flagNames = [x[0] for x in dep.getFlags()[0]]
                pythonFlags.update(flagNames)
            self.pythonTroveFlagCache[pathName] = pythonFlags

        if not foundPath:
            self.pythonTroveFlagCache[pathName] = self._warnPythonPathNotInDB(
                pathName)

        return self.pythonTroveFlagCache[pathName]

    def _getPythonFlags(self, pathName, bootstrapPythonFlags=None):
        if pathName in self.pythonFlagCache:
            return self.pythonFlagCache[pathName]

        if bootstrapPythonFlags:
            self.pythonFlagCache[pathName] = bootstrapPythonFlags
            return self.pythonFlagCache[pathName]

        db = self._getDb()
        foundPath = False

        # FIXME: This should be iterFilesByPath when implemented (CNY-1833)
        # For now, cache all the python deps in all the files in the
        # trove(s) so that we iterate over each trove only once
        containingTroveList = db.iterTrovesByPath(pathName)
        for containerTrove in containingTroveList:
            for pathid, p, fileid, v in containerTrove.iterFileList():
                if pathName == p:
                    foundPath = True
                pythonFlags = set()
                f = files.ThawFile(db.getFileStream(fileid), pathid)
                for dep in f.provides().iterDepsByClass(
                        deps.PythonDependencies):
                    flagNames = [x[0] for x in dep.getFlags()[0]]
                    pythonFlags.update(flagNames)
                self.pythonFlagCache[p] = pythonFlags

        if not foundPath:
            self.pythonFlagCache[pathName] = self._warnPythonPathNotInDB(
                pathName)

        return self.pythonFlagCache[pathName]

    def _getPythonFlagsFromPath(self, pathName):
        pathList = pathName.split('/')
        foundLib = False
        foundVer = False
        flags = set()
        for dirName in pathList:
            if not foundVer and not foundLib and dirName.startswith('lib'):
                # lib will always come before ver
                foundLib = True
                flags.add(dirName)
            elif not foundVer and dirName.startswith('python'):
                foundVer = True
                flags.add(dirName[6:])
            if foundLib and foundVer:
                break
        if self.pythonFlagNamespace:
            flags = set('%s:%s' %(self.pythonFlagNamespace, x) for x in flags)

        return flags

    def _stringIsPythonVersion(self, s):
        return not set(s).difference(set('.0123456789'))

    def _getPythonVersionFromFlags(self, flags):
        nameSpace = self.pythonFlagNamespace
        for flag in flags:
            if nameSpace and flag.startswith(nameSpace):
                flag = flag[len(nameSpace):]
            if self._stringIsPythonVersion(flag):
                return 'python'+flag

    def _getPythonVersionFromPath(self, pathName, destdir):
        if destdir and pathName.startswith(destdir):
            pathName = pathName[len(destdir):]

        pathList = pathName.split('/')
        for dirName in pathList:
            if dirName.startswith('python') and self._stringIsPythonVersion(
                    dirName[6:]):
                # python2.4 or python2.5 or python3.9 but not python.so
                return dirName
        return ''

    def _isCIL(self, m):
        return m and m.name == 'CIL'

    def _isJava(self, m, contents=None):
        return m and isinstance(m, (magic.jar, magic.java)) and self._hasContents(m, contents)

    def _isPerlModule(self, path):
        return (path.endswith('.pm') or
                path.endswith('.pl') or
                path.endswith('.ph'))

    def _isPerl(self, path, m, f):
        return self._isPerlModule(path) or (
            f.inode.perms() & 0111 and m and m.name == 'script'
            and 'interpreter' in m.contents
            and '/bin/perl' in m.contents['interpreter'])


    def _createELFDepSet(self, m, elfinfo, recipe=None, basedir=None,
                         soname=None, soflags=None,
                         libPathMap={}, getRPATH=None, path=None,
                         isProvides=None):
        """
        Add dependencies from ELF information.

        @param m: magic.ELF object
        @param elfinfo: requires or provides from magic.ELF.contents
        @param recipe: recipe object for calling Requires if basedir is not None
        @param basedir: directory to add into dependency
        @param soname: alternative soname to use
        @param libPathMap: mapping from base dependency name to new dependency name
        @param isProvides: whether the dependency being created is a provides
        """
        abi = m.contents['abi']
        elfClass = abi[0]
        nameMap = {}
        usesLinuxAbi = False

        depSet = deps.DependencySet()
        for depClass, main, flags in elfinfo:
            if soflags:
                flags = itertools.chain(*(flags, soflags))
            flags = [ (x, deps.FLAG_SENSE_REQUIRED) for x in flags ]
            if depClass == 'soname':
                if '/' in main:
                    main = os.path.basename(main)

                if getRPATH:
                    rpath = getRPATH(main)
                    if rpath:
                        # change the name to follow the rpath
                        main = '/'.join((rpath, main))
                elif soname:
                    main = soname

                if basedir:
                    oldname = os.path.normpath('/'.join((elfClass, main)))
                    main = '/'.join((basedir, main))

                main = os.path.normpath('/'.join((elfClass, main)))

                if basedir:
                    nameMap[main] = oldname

                if libPathMap and main in libPathMap:
                    # if we have a mapping to a provided library that would be
                    # satisfied, then we modify the requirement to match the
                    # provision
                    provided = libPathMap[main]
                    requiredSet = set(x[0] for x in flags)
                    providedSet = set(provided.flags.keys())
                    if requiredSet.issubset(providedSet):
                        main = provided.getName()[0]
                    else:
                        pathString = ''
                        if path:
                            pathString = 'for path %s' %path
                        self.warn('Not replacing %s with %s because of missing %s%s',
                                  main, provided.getName()[0],
                                  sorted(list(requiredSet-providedSet)),
                                  pathString)

                curClass = deps.SonameDependencies
                for flag in abi[1]:
                    if flag == 'Linux':
                        usesLinuxAbi = True
                        flags.append(('SysV', deps.FLAG_SENSE_REQUIRED))
                    else:
                        flags.append((flag, deps.FLAG_SENSE_REQUIRED))

                dep = deps.Dependency(main, flags)

            elif depClass == 'abi':
                curClass = deps.AbiDependency
                dep = deps.Dependency(main, flags)
            else:
                assert(0)

            depSet.addDep(curClass, dep)

            # This loop has to happen late so that the soname
            # flag merging from multiple flag instances has happened
            if nameMap:
                for soDep in depSet.iterDepsByClass(deps.SonameDependencies):
                    newName = soDep.getName()[0]
                    if newName in nameMap:
                        oldName = nameMap[newName]
                        recipe.Requires(_privateDepMap=(oldname, soDep))

        if usesLinuxAbi and not isProvides:
            isnset = m.contents.get('isnset', None)
            if elfClass == 'ELF32' and isnset == 'x86':
                main = 'ELF32/ld-linux.so.2'
            elif elfClass == 'ELF64' and isnset == 'x86_64':
                main = 'ELF64/ld-linux-x86-64.so.2'
            else:
                self.error('%s: unknown ELF class %s or instruction set %s',
                           path, elfClass, isnset)
                return depSet
            flags = [('Linux', deps.FLAG_SENSE_REQUIRED),
                     ('SysV', deps.FLAG_SENSE_REQUIRED),
                     (isnset, deps.FLAG_SENSE_REQUIRED)]
            dep = deps.Dependency(main, flags)
            depSet.addDep(curClass, dep)

        return depSet

    def _addDepToMap(self, path, depMap, depType, dep):
        "Add a single dependency to a map, regardless of whether path was listed before"
        if path not in depMap:
            depMap[path] = deps.DependencySet()
        depMap[path].addDep(depType, dep)

    def _addDepSetToMap(self, path, depMap, depSet):
        "Add a dependency set to a map, regardless of whether path was listed before"
        if path in depMap:
            depMap[path].union(depSet)
        else:
            depMap[path] = depSet

    @staticmethod
    def _recurseSymlink(path, destdir, fullpath=None):
        """
        Recurse through symlinks in destdir and get the final path and fullpath.
        If initial fullpath (or destdir+path if fullpath not specified)
        does not exist, return path.
        """
        if fullpath is None:
            fullpath = destdir + path
        while os.path.islink(fullpath):
            contents = os.readlink(fullpath)
            if contents.startswith('/'):
                fullpath = os.path.normpath(contents)
            else:
                fullpath = os.path.normpath(
                    os.path.dirname(fullpath)+'/'+contents)
        return fullpath[len(destdir):], fullpath

    def _symlinkMagic(self, path, fullpath, macros, m=None):
        "Recurse through symlinks and get the final path and magic"
        path, _ = self._recurseSymlink(path, macros.destdir, fullpath=fullpath)
        m = self.recipe.magic[path]
        return m, path

    def _enforceProvidedPath(self, path, fileType='interpreter',
                             unmanagedError=False):
        key = path, fileType
        if key in self.cachedProviders:
            return self.cachedProviders[key]
        db = self._getDb()
        troveNames = [ x.getName() for x in db.iterTrovesByPath(path) ]
        if not troveNames:
            talk = {True: self.error, False: self.warn}[bool(unmanagedError)]
            talk('%s file %s not managed by conary' %(fileType, path))
            return None
        troveName = sorted(troveNames)[0]

        # prefer corresponding :devel to :devellib if it exists
        package, component = troveName.split(':', 1)
        if component in ('devellib', 'lib'):
            for preferredComponent in ('devel', 'devellib'):
                troveSpec = (
                    ':'.join((package, preferredComponent)),
                    None, None
                )
                results = db.findTroves(None, [troveSpec],
                                             allowMissing = True)
                if troveSpec in results:
                    troveName = results[troveSpec][0][0]
                    break

        if troveName not in self.recipe._getTransitiveBuildRequiresNames():
            self.recipe.reportMissingBuildRequires(troveName)

        self.cachedProviders[key] = troveName
        return troveName

    def _getRuby(self, macros, path):
        # For bootstrapping purposes, prefer the just-built version if
        # it exists
        # Returns tuple: (pathToRubyInterpreter, bootstrap)
        ruby = '%(ruby)s' %macros
        if os.access('%(destdir)s/%(ruby)s' %macros, os.X_OK):
            return '%(destdir)s/%(ruby)s' %macros, True
        elif os.access(ruby, os.X_OK):
            # Enforce the build requirement, since it is not in the package
            self._enforceProvidedPath(ruby)
            return ruby, False
        else:
            self.warn('%s not available for Ruby dependency discovery'
                      ' for path %s' %(ruby, path))
        return False, None

    def _getRubyLoadPath(self, macros, rubyInvocation, bootstrap):
        # Returns tuple of (invocationString, loadPathList)
        destdir = macros.destdir
        if bootstrap:
            rubyLibPath = [destdir + x for x in self.bootstrapRubyLibs]
            rubyInvocation = (('LD_LIBRARY_PATH=%(destdir)s%(libdir)s '
                               'RUBYLIB="'+':'.join(rubyLibPath)+'" '
                               +rubyInvocation)%macros)
        rubyLoadPath = util.popen(
            "%s -e 'puts $:'" %
            rubyInvocation).readlines()
        # get gem dir if rubygems is installed
        if os.access('%(bindir)s/gem' %macros, os.X_OK):
            rubyLoadPath.extend(
                util.popen("%s -rubygems -e 'puts Gem.default_dir'" %
                        rubyInvocation).readlines())
        rubyLoadPath = [ x.strip() for x in rubyLoadPath if x.startswith('/') ]
        loadPathList = rubyLoadPath[:]
        if bootstrap:
            rubyLoadPath = [ destdir+x for x in rubyLoadPath ]
            rubyInvocation = ('LD_LIBRARY_PATH=%(destdir)s%(libdir)s'
                    ' RUBYLIB="'+':'.join(rubyLoadPath)+'"'
                    ' %(destdir)s/%(ruby)s') % macros
        return (rubyInvocation, loadPathList)

    def _getRubyVersion(self, macros):
        cmd = self.rubyInvocation + (" -e 'puts RUBY_VERSION'" % macros)
        rubyVersion = util.popen(cmd).read()
        rubyVersion = '.'.join(rubyVersion.split('.')[0:2])
        return rubyVersion

    def _getRubyFlagsFromPath(self, pathName, rubyVersion):
        pathList = pathName.split('/')
        pathList = [ x for x in pathList if x ]
        foundLib = False
        foundVer = False
        flags = set()
        for dirName in pathList:
            if not foundLib and dirName.startswith('lib'):
                foundLib = True
                flags.add(dirName)
            elif not foundVer and dirName.split('.')[:1] == rubyVersion.split('.')[:1]:
                # we only compare major and minor versions due to
                # ruby api version (dirName) differing from programs
                # version (rubyVersion)
                foundVer = True
                flags.add(dirName)
            if foundLib and foundVer:
                break
        return flags


    def _getmonodis(self, macros, path):
        # For bootstrapping purposes, prefer the just-built version if
        # it exists
        monodis = '%(monodis)s' %macros
        if os.access('%(destdir)s/%(monodis)s' %macros, os.X_OK):
            return ('MONO_PATH=%(destdir)s%(prefix)s/lib'
                    ' LD_LIBRARY_PATH=%(destdir)s%(libdir)s'
                    ' %(destdir)s/%(monodis)s' %macros)
        elif os.access(monodis, os.X_OK):
            # Enforce the build requirement, since it is not in the package
            self._enforceProvidedPath(monodis)
            return monodis
        else:
            self.warn('%s not available for CIL dependency discovery'
                      ' for path %s' %(monodis, path))
        return None


    def _getperlincpath(self, perl, destdir):
        """
        Fetch the perl @INC path, falling back to bootstrapPerlIncPath
        only if perl cannot be run.  All elements of the search path
        will be resolved against symlinks in destdir if they exist. (CNY-2949)
        """
        if not perl:
            return []
        p = util.popen(r"""%s -e 'print join("\n", @INC)'""" %perl)
        perlIncPath = p.readlines()
        # make sure that the command completed successfully
        try:
            rc = p.close()
            perlIncPath = [x.strip() for x in perlIncPath if not x.startswith('.')]
            return [self._recurseSymlink(x, destdir)[0] for x in perlIncPath]
        except RuntimeError:
            return [self._recurseSymlink(x, destdir)[0]
                    for x in self.bootstrapPerlIncPath]

    def _getperl(self, macros, recipe):
        """
        Find the preferred instance of perl to use, including setting
        any environment variables necessary to use that perl.
        Returns string for running it, the C{@INC} path, and a separate
        string, if necessary, for adding to @INC.
        """
        perlDestPath = '%(destdir)s%(bindir)s/perl' %macros
        # not %(bindir)s so that package modifications do not affect
        # the search for system perl
        perlPath = '/usr/bin/perl'
        destdir = macros.destdir

        def _perlDestInc(destdir, perlDestInc):
            return ' '.join(['-I' + destdir + x for x in perlDestInc])

        if os.access(perlDestPath, os.X_OK):
            # must use packaged perl if it exists
            m = recipe.magic[perlDestPath[len(destdir):]] # not perlPath
            if m and 'RPATH' in m.contents and m.contents['RPATH']:
                # we need to prepend the destdir to each element of the RPATH
                # in order to run perl in the destdir
                perl = ''.join((
                    'export LD_LIBRARY_PATH=',
                    '%s%s:' %(destdir, macros.libdir),
                    ':'.join([destdir+x
                              for x in m.contents['RPATH'].split(':')]),
                    ';',
                    perlDestPath
                ))
                perlIncPath = self._getperlincpath(perl, destdir)
                perlDestInc = _perlDestInc(destdir, perlIncPath)
                return [perl, perlIncPath, perlDestInc]
            else:
                # perl that does not use/need rpath
                perl = 'LD_LIBRARY_PATH=%s%s %s' %(
                    destdir, macros.libdir, perlDestPath)
                perlIncPath = self._getperlincpath(perl, destdir)
                perlDestInc = _perlDestInc(destdir, perlIncPath)
                return [perl, perlIncPath, perlDestInc]
        elif os.access(perlPath, os.X_OK):
            # system perl if no packaged perl, needs no @INC mangling
            self._enforceProvidedPath(perlPath)
            perlIncPath = self._getperlincpath(perlPath, destdir)
            return [perlPath, perlIncPath, '']

        # must be no perl at all
        return ['', [], '']


    def _getPython(self, macros, path):
        """
        Takes a path
        Returns, for that path, a tuple of
            - the preferred instance of python to use
            - whether that instance is in the destdir
        """
        m = self.recipe.magic[path]
        if m and m.name == 'script' and 'python' in m.contents['interpreter']:
            pythonPath = [m.contents['interpreter']]
        else:
            pythonVersion = self._getPythonVersionFromPath(path, None)
            # After PATH, fall back to %(bindir)s.  If %(bindir)s should be
            # preferred, it needs to be earlier in the PATH.  Include
            # unversioned python as a last resort for confusing cases.
            shellPath = os.environ.get('PATH', '').split(':') + [ '%(bindir)s' ]
            pythonPath = []
            if pythonVersion:
                pythonPath = [ os.path.join(x, pythonVersion) for x in shellPath ]
            pythonPath.extend([ os.path.join(x, 'python') for x in shellPath ])

        for pathElement in pythonPath:
            pythonDestPath = ('%(destdir)s'+pathElement) %macros
            if os.access(pythonDestPath, os.X_OK):
                return (pythonDestPath, True)
        for pathElement in pythonPath:
            pythonDestPath = pathElement %macros
            if os.access(pythonDestPath, os.X_OK):
                self._enforceProvidedPath(pythonDestPath)
                return (pythonDestPath, False)

        # Specified python not found on system (usually because of
        # bad interpreter path -- CNY-2050)
        if len(pythonPath) == 1:
            missingPythonPath = '%s ' % pythonPath[0]
        else:
            missingPythonPath = ''
        self.warn('Python interpreter %snot found for %s',
                  missingPythonPath, path)
        return (None, None)


    def _stripDestDir(self, pathList, destdir):
        destDirLen = len(destdir)
        pathElementList = []
        for pathElement in pathList:
            if pathElement.startswith(destdir):
                pathElementList.append(pathElement[destDirLen:])
            else:
                pathElementList.append(pathElement)
        return pathElementList



class Provides(_dependency):
    """
    NAME
    ====
    B{C{r.Provides()}} - Creates dependency provision

    SYNOPSIS
    ========
    C{r.Provides([I{provision}, I{filterexp}] || [I{exceptions=filterexp}])}

    DESCRIPTION
    ===========
    The C{r.Provides()} policy marks files as providing certain features
    or characteristics, and can be called to explicitly provide things
    that cannot be automatically discovered. C{r.Provides} can also override
    automatic discovery, and prevent marking a file as providing things, such
    as for package-private plugin modules installed in system library
    directories.

    A C{I{provision}} may be C{'file'} to mark a file as providing its
    filename, or a dependency type.  You can create a file, soname or
    ABI C{I{provision}} manually; all other types are only automatically
    discovered.  Provisions that begin with C{file} are files, those that
    start with C{soname:} are sonames, and those that start with C{abi:}
    are ABIs.  Other prefixes are reserved.

    Soname provisions are normally discovered automatically; they need
    to be provided manually only in two cases:
      - If a shared library was not built with a soname at all.
      - If a symbolic link to a shared library needs to provide its name
        as a soname.

    Note: Use {Cr.ComponentProvides} rather than C{r.Provides} to add
    capability flags to components.

    For unusual cases where you want to remove a provision Conary
    automatically finds, you can specify C{r.Provides(exceptDeps='regexp')}
    to override all provisions matching a regular expression,
    C{r.Provides(exceptDeps=('filterexp', 'regexp'))}
    to override provisions matching a regular expression only for files
    matching filterexp, or
    C{r.Provides(exceptDeps=(('filterexp', 'regexp'), ...))} to specify
    multiple overrides.

    EXAMPLES
    ========
    C{r.Provides('file', '/usr/share/dict/words')}

    Demonstrates using C{r.Provides} to specify the file provision
    C{/usr/share/dict/words}, so that other files can now require that file.

    C{r.Provides('soname: libperl.so', '%(libdir)s/perl5/.*/CORE/libperl.so')}

    Demonstrates synthesizing a shared library provision for all the
    libperl.so symlinks.

    C{r.Provides(exceptDeps = 'java: .*')}

    Demonstrates removing all java provisions.
    """
    bucket = policy.PACKAGE_CREATION

    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('SharedLibrary', policy.REQUIRED),
        # _ELFPathProvide calls Requires to pass in discovered info
        # _addCILPolicyProvides does likewise
        ('Requires', policy.REQUIRED_SUBSEQUENT),
    )
    filetree = policy.PACKAGE

    invariantexceptions = (
        '%(docdir)s/',
    )

    dbDepCacheClass = _DatabaseDepCache

    def __init__(self, *args, **keywords):
        _dependency.__init__(self, *args, **keywords)
        self.provisions = []
        self.sonameSubtrees = set()
        self.sysPath = None
        self.monodisPath = None
        self.rubyInterpreter = None
        self.rubyVersion = None
        self.rubyInvocation = None
        self.rubyLoadPath = None
        self.perlIncPath = None
        self.pythonSysPathMap = {}
        self.exceptDeps = []
        policy.Policy.__init__(self, *args, **keywords)
        self.depCache = self.dbDepCacheClass(self._getDb())

    def updateArgs(self, *args, **keywords):
        if args:
            for filespec in args[1:]:
                self.provisions.append((filespec, args[0]))
        sonameSubtrees = keywords.pop('sonameSubtrees', None)
        if sonameSubtrees:
            if type(sonameSubtrees) in (list, tuple):
                self.sonameSubtrees.update(set(sonameSubtrees))
            else:
                self.sonameSubtrees.add(sonameSubtrees)
        exceptDeps = keywords.pop('exceptDeps', None)
        if exceptDeps:
            if type(exceptDeps) is str:
                exceptDeps = ('.*', exceptDeps)
            assert(type(exceptDeps) == tuple)
            if type(exceptDeps[0]) is tuple:
                self.exceptDeps.extend(exceptDeps)
            else:
                self.exceptDeps.append(exceptDeps)
        # The next three are called only from Requires and should override
        # completely to make sure the policies are in sync
        pythonFlagNamespace = keywords.pop('_pythonFlagNamespace', None)
        if pythonFlagNamespace is not None:
            self.pythonFlagNamespace = pythonFlagNamespace
        bootstrapPythonFlags = keywords.pop('_bootstrapPythonFlags', None)
        if bootstrapPythonFlags is not None:
            self.bootstrapPythonFlags = bootstrapPythonFlags
        bootstrapSysPath = keywords.pop('_bootstrapSysPath', None)
        if bootstrapSysPath is not None:
            self.bootstrapSysPath = bootstrapSysPath
        bootstrapPerlIncPath = keywords.pop('_bootstrapPerlIncPath', None)
        if bootstrapPerlIncPath is not None:
            self.bootstrapPerlIncPath = bootstrapPerlIncPath
        bootstrapRubyLibs = keywords.pop('_bootstrapRubyLibs', None)
        if bootstrapRubyLibs is not None:
            self.bootstrapRubyLibs = bootstrapRubyLibs
        if keywords.get('removeFlagsByDependencyClass', None):
            self.error('removeFlagsByDependencyClass not currently implemented for Provides (CNY-3443)')

        _dependency.updateArgs(self, **keywords)

    def preProcess(self):
        macros = self.macros
        if self.bootstrapPythonFlags is not None:
            self.bootstrapPythonFlags = set(x % macros
                                            for x in self.bootstrapPythonFlags)
        if self.bootstrapSysPath:
            self.bootstrapSysPath = [x % macros for x in self.bootstrapSysPath]
        if self.pythonFlagNamespace is not None:
            self.pythonFlagNamespace = self.pythonFlagNamespace % macros
        if self.bootstrapPerlIncPath:
            self.bootstrapPerlIncPath = [x % macros for x in self.bootstrapPerlIncPath]
        self.rootdir = self.rootdir % macros
        self.fileFilters = []
        self.binDirs = frozenset(
            x % macros for x in [
            '%(bindir)s', '%(sbindir)s',
            '%(essentialbindir)s', '%(essentialsbindir)s',
            '%(libexecdir)s', ])
        self.noProvDirs = frozenset(
            x % macros for x in [
            '%(testdir)s',
            '%(debuglibdir)s',
            ]).union(self.binDirs)
        exceptDeps = []
        for fE, rE in self.exceptDeps:
            try:
                exceptDeps.append((filter.Filter(fE, macros),
                                   re.compile(rE % self.macros)))
            except sre_constants.error, e:
                self.error('Bad regular expression %s for file spec %s: %s', rE, fE, e)
        self.exceptDeps= exceptDeps
        for filespec, provision in self.provisions:
            self.fileFilters.append(
                (filter.Filter(filespec, macros), provision % macros))
        del self.provisions
        _dependency.preProcess(self)


    def doFile(self, path):
        pkgs = self.recipe.autopkg.findComponents(path)
        if not pkgs:
            return
        pkgFiles = [(x, x.getFile(path)) for x in pkgs]
        macros = self.recipe.macros
        m = self.recipe.magic[path]

        fullpath = macros.destdir + path
        basepath = os.path.basename(path)
        dirpath = os.path.dirname(path)

        if os.path.exists(fullpath):
            mode = os.lstat(fullpath)[stat.ST_MODE]

        # First, add in the manual provisions
        self.addExplicitProvides(path, fullpath, pkgFiles, macros, m)

        # Next, discover all automatically-discoverable provisions
        if os.path.exists(fullpath):
            if (self._isELF(m, 'abi')
                and m.contents['Type'] != elf.ET_EXEC
                and not [ x for x in self.noProvDirs if path.startswith(x) ]):
                # we do not add elf provides for programs that won't be linked to
                self._ELFAddProvide(path, m, pkgFiles, basedir=dirpath)
            if dirpath in self.sonameSubtrees:
                # only export filename as soname if is shlib
                sm, finalpath = self._symlinkMagic(path, fullpath, macros, m)
                if sm and self._isELF(sm, 'abi') and sm.contents['Type'] != elf.ET_EXEC:
                    # add the filename as a soname provision (CNY-699)
                    # note: no provides necessary
                    self._ELFAddProvide(path, sm, pkgFiles, soname=basepath, basedir=dirpath)

            if self._isPythonModuleCandidate(path):
                self._addPythonProvides(path, m, pkgFiles, macros)

            rubyProv = self._isRubyModule(path, macros, fullpath)
            if rubyProv:
                self._addRubyProvides(path, m, pkgFiles, macros, rubyProv)

            elif self._isCIL(m):
                self._addCILProvides(path, m, pkgFiles, macros)

            elif self.CILPolicyRE.match(path):
                self._addCILPolicyProvides(path, pkgFiles, macros)

            elif self._isJava(m, 'provides'):
                # Cache the internal provides
                if not hasattr(self.recipe, '_internalJavaDepMap'):
                    self.recipe._internalJavaDepMap = None
                self._addJavaProvides(path, m, pkgFiles)

            elif self._isPerlModule(path):
                self._addPerlProvides(path, m, pkgFiles)

        self.addPathDeps(path, dirpath, pkgFiles)
        self.whiteOut(path, pkgFiles)
        self.unionDeps(path, pkgFiles)

    def whiteOut(self, path, pkgFiles):
        # remove intentionally discarded provides
        for pkg, f in pkgFiles:
            if self.exceptDeps and path in pkg.providesMap:
                depSet = deps.DependencySet()
                for depClass, dep in pkg.providesMap[path].iterDeps():
                    for filt, exceptRe in self.exceptDeps:
                        if filt.match(path):
                            matchName = '%s: %s' %(depClass.tagName, str(dep))
                            if exceptRe.match(matchName):
                                # found one to not copy
                                dep = None
                                break
                    if dep is not None:
                        depSet.addDep(depClass, dep)
                pkg.providesMap[path] = depSet

    def addExplicitProvides(self, path, fullpath, pkgFiles, macros, m):
        for (filter, provision) in self.fileFilters:
            if filter.match(path):
                self._markProvides(path, fullpath, provision, pkgFiles, macros, m)

    def addPathDeps(self, path, dirpath, pkgFiles):
        # Because paths can change, individual files do not provide their
        # paths.  However, within a trove, a file does provide its name.
        # Furthermore, non-regular files can be path dependency targets
        # Therefore, we have to handle this case a bit differently.
        for pkg, f  in pkgFiles:
            if dirpath in self.binDirs and not isinstance(f, files.Directory):
                # CNY-930: automatically export paths in bindirs
                # CNY-1721: but not directories in bindirs
                f.flags.isPathDependencyTarget(True)

            if f.flags.isPathDependencyTarget():
                pkg.provides.addDep(deps.FileDependencies, deps.Dependency(path))

    def unionDeps(self, path, pkgFiles):
        for pkg, f in pkgFiles:
            if path in pkg.providesMap:
                f.provides.set(pkg.providesMap[path])
                pkg.provides.union(f.provides())



    def _getELFinfo(self, m, soname):
        if 'provides' in m.contents and m.contents['provides']:
            return m.contents['provides']
        else:
            # we need to synthesize some provides information
            return [('soname', soname, ())]

    def _ELFAddProvide(self, path, m, pkgFiles, soname=None, soflags=None, basedir=None):
        if basedir is None:
            basedir = os.path.dirname(path)
        if basedir in self.sonameSubtrees:
            # do not record the basedir
            basedir = None
        else:
            # path needs to be in the dependency, since the
            # provides is too broad otherwise, so add it.
            # We can only add characters from the path that are legal
            # in a dependency name
            basedir = ''.join(x for x in basedir if self.legalCharsRE.match(x))

        elfinfo = self._getELFinfo(m, os.path.basename(path))
        depSet = self._createELFDepSet(m, elfinfo,
                                       recipe=self.recipe, basedir=basedir,
                                       soname=soname, soflags=soflags,
                                       path=path, isProvides=True)
        for pkg, _ in pkgFiles:
            self._addDepSetToMap(path, pkg.providesMap, depSet)


    def _getPythonProvidesSysPath(self, path):
        """Generate an ordered list of python paths for the target package.

        This includes the current system path, plus any paths added by the new
        package in the destdir through .pth files or a newly built python.

        @return: (sysPath, pythonVersion)
        """
        pythonPath, bootstrapPython = self._getPython(self.macros, path)
        if not pythonPath:
            # Most likely bad interpreter path in a .py file
            return (None, None)
        if pythonPath in self.pythonSysPathMap:
            return self.pythonSysPathMap[pythonPath]
        destdir = self.macros.destdir
        libdir = self.macros.libdir
        pythonVersion = self._getPythonVersion(pythonPath, destdir, libdir)

        # Get default sys.path from python interpreter, either the one just
        # built (in the case of a python bootstrap) or from the system.
        systemPaths = set(self._getPythonSysPath(pythonPath, destdir, libdir,
            useDestDir=False))
        # Now add paths from the destdir's site-packages, typically due to
        # newly installed .pth files.
        systemPaths.update(self._getPythonSysPath(pythonPath, destdir, libdir,
            useDestDir=True))
        # Sort in descending order so that the longest path matches first.
        sysPath = sorted(self._stripDestDir(systemPaths, destdir), reverse=True)

        self.pythonSysPathMap[pythonPath] = (sysPath, pythonVersion)
        return self.pythonSysPathMap[pythonPath]

    def _fetchPerlIncPath(self):
        """
        Cache the perl @INC path, sorted longest first
        """
        if self.perlIncPath is not None:
            return

        _, self.perlIncPath, _ = self._getperl(
            self.recipe.macros, self.recipe)
        self.perlIncPath.sort(key=len, reverse=True)

    def _addPythonProvides(self, path, m, pkgFiles, macros):

        if not self._isPythonModuleCandidate(path):
            return

        sysPath, pythonVersion = self._getPythonProvidesSysPath(path)
        if not sysPath:
            return

        # Add provides for every match in sys.path. For example, PIL.Imaging
        # and Imaging should both be provided since they are both reachable
        # names.
        for sysPathEntry in sysPath:
            if not path.startswith(sysPathEntry):
                continue
            newDepPath = path[len(sysPathEntry)+1:]
            if newDepPath.split('.')[0] == '__init__':
                # we don't allow bare __init__ as a python import
                # hopefully we'll find this init as a deeper import at some
                # other point in the sysPath
                continue
            elif ('site-packages' in newDepPath
                    or 'lib-dynload' in newDepPath
                    or 'plat-linux' in newDepPath
                    ):
                # site-packages should be specifically excluded since both it
                # and its parent are always in sys.path. However, invalid
                # python package names in general are allowed due to certain
                # cases where relative imports happen inside a hyphenated
                # directory and the requires detector picks up on that.
                continue
            # Note that it's possible to have a false positive here. For
            # example, in the PIL case if PIL/__init__.py did not exist,
            # PIL.Imaging would still be provided. The odds of this causing
            # problems are so small that it is not checked for here.
            self._addPythonProvidesSingle(path, m, pkgFiles, macros,
                    newDepPath)

    def _addPythonProvidesSingle(self, path, m, pkgFiles, macros, depPath):
        # remove extension
        depPath, extn = depPath.rsplit('.', 1)

        if depPath == '__future__':
            return

        # remove python3 __pycache__ directory from dep
        if '__pycache__/' in depPath:
            depPath = depPath.replace('__pycache__/', '')

        # PEP 3147 adds the interperter and version to the pyc file
        depPath = self.pythonInterpRE.sub('', depPath)

        if depPath.endswith('/__init__'):
            depPath = depPath.replace('/__init__', '')

        depPath = depPath.replace('/', '.')

        depPaths = [ depPath ]

        if extn == 'so':
            fname = util.joinPaths(macros.destdir, path)
            try:
                syms = elf.getDynSym(fname)
                # Does this module have an init<blah> function?
                initfuncs = [ x[4:] for x in syms if x.startswith('init') ]
                # This is the equivalent of dirname()
                comps = depPath.rsplit('.', 1)
                dpPrefix = comps[0]
                if len(comps) == 1:
                    # Top-level python module
                    depPaths.extend(initfuncs)
                else:
                    for initfunc in initfuncs:
                        depPaths.append('.'.join([dpPrefix, initfunc]))
            except elf.error:
                pass

        flags = self._getPythonFlagsFromPath(path)
        flags = [(x, deps.FLAG_SENSE_REQUIRED) for x in sorted(list(flags))]
        for dpath in depPaths:
            dep = deps.Dependency(dpath, flags)
            for pkg, _ in pkgFiles:
                self._addDepToMap(path, pkg.providesMap, deps.PythonDependencies, dep)

    def _addOneCILProvide(self, pkgFiles, path, name, ver):
        for pkg, _ in pkgFiles:
            self._addDepToMap(path, pkg.providesMap, deps.CILDependencies,
                    deps.Dependency(name, [(ver, deps.FLAG_SENSE_REQUIRED)]))

    def _addCILPolicyProvides(self, path, pkgFiles, macros):
        if ElementTree is None:
            return
        try:
            keys = {'urn': '{urn:schemas-microsoft-com:asm.v1}'}
            fullpath = macros.destdir + path
            tree = ElementTree.parse(fullpath)
            root = tree.getroot()
            identity, redirect = root.find('runtime/%(urn)sassemblyBinding/%(urn)sdependentAssembly' % keys).getchildren()
            assembly = identity.get('name')
            self._addOneCILProvide(pkgFiles, path, assembly,
                redirect.get('oldVersion'))
            self.recipe.Requires(_CILPolicyProvides={
                path: (assembly, redirect.get('newVersion'))})
        except:
            return

    def _addCILProvides(self, path, m, pkgFiles, macros):
        if not m or m.name != 'CIL':
            return
        fullpath = macros.destdir + path
        if not self.monodisPath:
            self.monodisPath = self._getmonodis(macros, path)
            if not self.monodisPath:
                return
        p = util.popen('%s --assembly %s' %(
                       self.monodisPath, fullpath))
        name = None
        ver = None
        for line in [ x.strip() for x in p.readlines() ]:
            if 'Name:' in line:
                name = line.split()[1]
            elif 'Version:' in line:
                ver = line.split()[1]
        p.close()
        # monodis did not give us any info
        if not name or not ver:
            return
        self._addOneCILProvide(pkgFiles, path, name, ver)

    def _isRubyModule(self, path, macros, fullpath):
        if not util.isregular(fullpath) or os.path.islink(fullpath):
            return False
        if '/ruby/' in path:
            # load up ruby opportunistically; this is our first chance
            if self.rubyInterpreter is None:
                self.rubyInterpreter, bootstrap = self._getRuby(macros, path)
                if not self.rubyInterpreter:
                    return False
                self.rubyInvocation, self.rubyLoadPath = self._getRubyLoadPath(
                    macros, self.rubyInterpreter, bootstrap)
                self.rubyVersion = self._getRubyVersion(macros)
                # we need to look deep first
                self.rubyLoadPath = sorted(list(self.rubyLoadPath),
                                           key=len, reverse=True)
            elif self.rubyInterpreter is False:
                return False

            for pathElement in self.rubyLoadPath:
                if path.startswith(pathElement) \
                        and (path.endswith('.rb') or path.endswith('.so')):
                    if '/gems/' in path:
                        path = path.partition("/gems/")[-1]
                        if '/lib/' in path:
                            return path.partition('/lib/')[-1].rsplit('.', 1)[0]
                    else:
                        return path[len(pathElement)+1:].rsplit('.', 1)[0]
        return False

    def _addRubyProvides(self, path, m, pkgFiles, macros, prov):
        flags = self._getRubyFlagsFromPath(path, self.rubyVersion)
        flags = [(x, deps.FLAG_SENSE_REQUIRED) for x in sorted(list(flags))]
        dep = deps.Dependency(prov, flags)
        for pkg, _ in pkgFiles:
            self._addDepToMap(path, pkg.providesMap, deps.RubyDependencies, dep)

    def _addJavaProvides(self, path, m, pkgFiles):
        if 'provides' not in m.contents or not m.contents['provides']:
            return
        if not hasattr(self.recipe, '_reqExceptDeps'):
            self.recipe._reqExceptDeps = []
        # Compile requires exceptDeps (and persist them)
        if not hasattr(self.recipe, '_compiledReqExceptDeps'):
            self.recipe._compiledReqExceptDeps = exceptDeps = []
            macros = self.recipe.macros
            for fE, rE in self.recipe._reqExceptDeps:
                try:
                    exceptDeps.append((filter.Filter(fE, macros),
                                       re.compile(rE % macros)))
                except sre_constants.error, e:
                    self.error('Bad regular expression %s for file spec %s: %s',
                        rE, fE, e)
            # We will no longer need this, we have the compiled version now
            self.recipe._reqExceptDeps = []

        if self.recipe._internalJavaDepMap is None:
            # Instantiate the dictionary of provides from this package
            self.recipe._internalJavaDepMap = internalJavaDepMap = {}
            componentMap = self.recipe.autopkg.componentMap
            for opath in componentMap:
                om = self.recipe.magic[opath]
                if not self._isJava(om, 'provides'):
                    continue
                # The file could be a .jar, in which case it contains multiple
                # classes. contents['files'] is a dict, keyed on the file name
                # within the jar and with a provide and a set of requires as
                # value.
                internalJavaDepMap.setdefault(opath, {}).update(
                                                        om.contents['files'])
        else:
            internalJavaDepMap = self.recipe._internalJavaDepMap

        if hasattr(self.recipe, '_internalJavaProvides'):
            internalProvides = self.recipe._internalJavaProvides
        else:
            # We need to cache the internal java provides, otherwise we do too
            # much work for each file (CNY-3372)
            self.recipe._internalJavaProvides = internalProvides = set()
            for opath, ofiles in internalJavaDepMap.items():
                internalProvides.update(x[0] for x in ofiles.values()
                    if x[0] is not None)
            # Now drop internal provides from individual class requires

            for opath, ofiles in internalJavaDepMap.items():
                for oclassName, (oclassProv, oclassReqSet) in ofiles.items():
                    if oclassReqSet is None:
                        continue
                    oclassReqSet.difference_update(internalProvides)

        reqs = set()
        if self._isJava(m, 'requires'):
            # Extract this file's requires
            reqs.update(m.contents['requires'])
            # Remove the ones that are satisfied internally
            reqs.difference_update(internalProvides)

        # For now, we are only trimming the provides (and requires) for
        # classes for which the requires are not satisfied, neither internally
        # nor from the system Conary database. In the future we may need to
        # build a dependency tree between internal classes, such that we do
        # the removal transitively (class A requires class B which doesn't
        # have its deps satisfied should make class A unusable). This can come
        # at a later time
        # CNY-3362: we don't drop provides for classes which had requires on
        # classes that had their dependencies pruned. (at least not yet)

        if reqs:
            # Try to resolve these deps against the Conary database
            depSetList = []
            depSetMap = {}
            for req in reqs:
                depSet = deps.DependencySet()
                depSet.addDep(deps.JavaDependencies, deps.Dependency(req, []))
                depSetList.append(depSet)
                depSetMap[depSet] = req
            troves = self.depCache.getProvides(depSetList)
            missingDepSets = set(depSetList) - set(troves)
            missingReqs = set(depSetMap[x] for x in missingDepSets)

            # White out the missing requires if exceptDeps for them are found
            rExceptDeps = self.recipe._compiledReqExceptDeps
            if missingReqs and rExceptDeps:
                depClass = deps.JavaDependencies
                filteredMissingDeps = set()
                for dep in list(missingReqs):
                    for filt, exceptRe in rExceptDeps:
                        if not filt.match(path):
                            continue
                        matchName = '%s: %s' %(depClass.tagName, str(dep))
                        if exceptRe.match(matchName):
                            # found one to not copy
                            missingReqs.remove(dep)
                            filteredMissingDeps.add(dep)
                            break
                if filteredMissingDeps:
                    # We need to take them out of the per-file requires
                    ofiles = internalJavaDepMap[path]
                    for _, (oclassProv, oclassReqSet) in ofiles.items():
                        if oclassProv is not None:
                            oclassReqSet.difference_update(filteredMissingDeps)

            if missingReqs:
                fileDeps = internalJavaDepMap[path]
                # This file has unsatisfied dependencies.
                # Walk its list of classes to determine which ones are not
                # satisfied.
                satisfiedClasses = dict((fpath, (fprov, freqs))
                    for (fpath, (fprov, freqs)) in fileDeps.iteritems()
                        if freqs is not None
                            and not freqs.intersection(missingReqs))
                internalJavaDepMap[path] = satisfiedClasses

                self.warn('Provides and requirements for file %s are disabled '
                          'because of unsatisfied dependencies. To re-enable '
                          'them, add to the recipe\'s buildRequires the '
                          'packages that provide the following '
                          'requirements: %s' %
                            (path, " ".join(sorted(missingReqs))))

        # Add the remaining provides
        fileDeps = internalJavaDepMap[path]
        provs = set(fprov for fpath, (fprov, freqs) in fileDeps.iteritems()
                        if fprov is not None)
        for prov in provs:
            dep = deps.Dependency(prov, [])
            for pkg, _ in pkgFiles:
                self._addDepToMap(path, pkg.providesMap, deps.JavaDependencies, dep)


    def _addPerlProvides(self, path, m, pkgFiles):
        # do not call perl to get @INC unless we have something to do for perl
        self._fetchPerlIncPath()

        # It is possible that we'll want to allow user-specified
        # additions to the perl search path, but if so, we need
        # to path-encode those files, so we can't just prepend
        # those elements to perlIncPath.  We would need to end up
        # with something like "perl: /path/to/foo::bar" because
        # for perl scripts that don't modify @INC, they could not
        # find those scripts.  It is not clear that we need this
        # at all, because most if not all of those cases would be
        # intra-package dependencies that we do not want to export.

        depPath = None
        for pathPrefix in self.perlIncPath:
            if path.startswith(pathPrefix):
                depPath = path[len(pathPrefix)+1:]
                break
        if depPath is None:
            return

        # foo/bar/baz.pm -> foo::bar::baz
        prov = '::'.join(depPath.split('/')).rsplit('.', 1)[0]
        dep = deps.Dependency(prov, [])
        for pkg, _ in pkgFiles:
            self._addDepToMap(path, pkg.providesMap, deps.PerlDependencies, dep)

    def _markProvides(self, path, fullpath, provision, pkgFiles, macros, m):
        if provision.startswith("file"):
            # can't actually specify what to provide, just that it provides...
            for _, f in pkgFiles:
                f.flags.isPathDependencyTarget(True)

        elif provision.startswith("abi:"):
            abistring = provision[4:].strip()
            op = abistring.index('(')
            abi = abistring[:op]
            flags = abistring[op+1:-1].split()
            flags = [ (x, deps.FLAG_SENSE_REQUIRED) for x in flags ]
            dep = deps.Dependency(abi, flags)
            for pkg, _ in pkgFiles:
                self._addDepToMap(path, pkg.providesMap, deps.AbiDependency, dep)

        elif provision.startswith("soname:"):
            sm, finalpath = self._symlinkMagic(path, fullpath, macros, m)
            if self._isELF(sm, 'abi'):
                # Only ELF files can provide sonames.
                # This is for libraries that don't really include a soname,
                # but programs linked against them require a soname.
                # For this reason, we do not pass 'provides' to _isELF
                soname = provision[7:].strip()
                soflags = []
                if '(' in soname:
                    # get list of arbitrary flags
                    soname, rest = soname.split('(')
                    soflags.extend(rest[:-1].split())
                basedir = None
                if '/' in soname:
                    basedir, soname = soname.rsplit('/', 1)
                self._ELFAddProvide(path, sm, pkgFiles, soname=soname, soflags=soflags,
                                    basedir=basedir)
        else:
            self.error('Provides %s for file %s does not start with one of'
                       ' "file", "abi:", or "soname"',
                       provision, path)


class Requires(_addInfo, _dependency):
    """
    NAME
    ====
    B{C{r.Requires()}} - Creates dependency requirements

    SYNOPSIS
    ========
    C{r.Requires([I{/path/to/file}, I{filterexp}] || [I{packagename:component[(FLAGS)]}, I{filterexp}] || [I{exceptions=filterexp)}])}

    DESCRIPTION
    ===========
    The C{r.Requires()} policy adds requirements for a file.
    You can pass in exceptions that should not have automatic requirement
    discovery done, such as example shell scripts outside of C{%(docdir)s}.

    Note: Components are the only troves which can be required.

    For executables executed only through wrappers that
    use C{LD_LIBRARY_PATH} to find the libraries instead of
    embedding an RPATH in the binary, you will need to provide
    a synthetic RPATH using C{r.Requires(rpath='I{RPATH}')}
    or C{r.Requires(rpath=('I{filterExp}', 'I{RPATH}'))} calls,
    which are tested in the order provided.

    The RPATH is a standard Unix-style path string containing one or more
    directory names, separated only by colon characters, except for one
    significant change: Each path component is interpreted using shell-style
    globs, which are checked first in the C{%(destdir)s} and then on the
    installed system. (The globs are useful for cases like perl where
    statically determining the entire content of the path is difficult. Use
    globs only for variable parts of paths; be as specific as you can without
    using the glob feature any more than necessary.)

    Executables that use C{dlopen()} to open a shared library will not
    automatically have a dependency on that shared library. If the program
    unconditionally requires that it be able to C{dlopen()} the shared
    library, encode that requirement by manually creating the requirement
    by calling C{r.Requires('soname: libfoo.so', 'filterexp')} or
    C{r.Requires('soname: /path/to/libfoo.so', 'filterexp')} depending on
    whether the library is in a system library directory or not. (It should be
    the same as how the soname dependency is expressed by the providing
    package.)

    For unusual cases where a system library is not listed in C{ld.so.conf}
    but is instead found through a search through special subdirectories with
    architecture-specific names (such as C{i686} and C{tls}), you can pass in
    a string or list of strings specifying the directory or list of
    directories. with C{r.Requires(sonameSubtrees='/directoryname')}
    or C{r.Requires(sonameSubtrees=['/list', '/of', '/dirs'])}

    Note: These are B{not} regular expressions. They will have macro
    expansion expansion performed on them.

    For unusual cases where Conary finds a false or misleading dependency,
    or in which you need to override a true dependency, you can specify
    C{r.Requires(exceptDeps='regexp')} to override all dependencies matching
    a regular expression, C{r.Requires(exceptDeps=('filterexp', 'regexp'))}
    to override dependencies matching a regular expression only for files
    matching filterexp, or
    C{r.Requires(exceptDeps=(('filterexp', 'regexp'), ...))} to specify
    multiple overrides.


    EXAMPLES
    ========
    C{r.Requires('mailbase:runtime', '%(sbindir)s/sendmail')}

    Demonstrates using C{r.Requires} to specify a manual requirement of the
    file C{%(sbindir)s/sendmail} to the  C{:runtime} component of package
    C{mailbase}.

    C{r.Requires('file: %(sbindir)s/sendmail', '%(datadir)s/squirrelmail/index.php')}

    Specifies that conary should require the file C{%(sbindir)s/sendmail} to
    be present when trying to install C{%(datadir)s/squirrelmail/index.php}.

    C{r.Requires('soname: %(libdir)/kde3/kgreet_classic.so', '%(bindir)/kdm')}

    Demonstrates using C{r.Requires} to specify a manual soname requirement
    of the file C{%(bindir)s/kdm} to the soname
    C{%(libdir)/kde3/kgreet_classic.so}.

    C{r.Requires(exceptions='/usr/share/vim/.*/doc/')}

    Demonstrates using C{r.Requires} to specify that files in the
    subdirectory C{/usr/share/vim/.*/doc} are excepted from being marked as
    requirements.

    C{r.Requires(exceptDeps='trove:$trovename')}

    Uses C{r.Requires} to specify that the trove C{trovename} is excluded
    from the dependencies for the package.
    """

    bucket = policy.PACKAGE_CREATION
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('SharedLibrary', policy.REQUIRED_PRIOR),
        # Requires depends on ELF dep path discovery previously done in Provides
        ('Provides', policy.REQUIRED_PRIOR),
    )
    filetree = policy.PACKAGE

    invariantexceptions = (
        '%(docdir)s/',
    )

    dbDepCacheClass = _DatabaseDepCache

    def __init__(self, *args, **keywords):
        _dependency.__init__(self, *args, **keywords)
        self.bootstrapPythonFlags = set()
        self.bootstrapSysPath = []
        self.bootstrapPerlIncPath = []
        self.bootstrapRubyLibs = []
        self.pythonFlagNamespace = None
        self.sonameSubtrees = set()
        self._privateDepMap = {}
        self.rpathFixup = []
        self.exceptDeps = []
        self.sysPath = None
        self.monodisPath = None
        self.rubyInterpreter = None
        self.rubyVersion = None
        self.rubyInvocation = None
        self.rubyLoadPath = None
        self.perlReqs = None
        self.perlPath = None
        self.perlIncArgs = None
        self._CILPolicyProvides = {}
        self.pythonSysPathMap = {}
        self.pythonModuleFinderMap = {}
        self.troveDeps = {}
        policy.Policy.__init__(self, *args, **keywords)
        self.depCache = self.dbDepCacheClass(self._getDb())

        ISD = deps.InstructionSetDependency
        TISD = deps.TargetInstructionSetDependency
        instructionDeps = list(self.recipe._buildFlavor.iterDepsByClass(ISD))
        instructionDeps += list(self.recipe._buildFlavor.iterDepsByClass(TISD))
        self.allowableIsnSets = [ x.name for x in instructionDeps ]

    def updateArgs(self, *args, **keywords):
        # _privateDepMap is used only for Provides to talk to Requires
        privateDepMap = keywords.pop('_privateDepMap', None)
        if privateDepMap:
            self._privateDepMap.update([privateDepMap])
        sonameSubtrees = keywords.pop('sonameSubtrees', None)
        if sonameSubtrees:
            if type(sonameSubtrees) in (list, tuple):
                self.sonameSubtrees.update(set(sonameSubtrees))
            else:
                self.sonameSubtrees.add(sonameSubtrees)
        bootstrapPythonFlags = keywords.pop('bootstrapPythonFlags', None)
        if bootstrapPythonFlags:
            if type(bootstrapPythonFlags) in (list, tuple):
                self.bootstrapPythonFlags.update(set(bootstrapPythonFlags))
            else:
                self.bootstrapPythonFlags.add(bootstrapPythonFlags)
            # pass full set to Provides to share the exact same data
            self.recipe.Provides(
                _bootstrapPythonFlags=self.bootstrapPythonFlags)
        bootstrapSysPath = keywords.pop('bootstrapSysPath', None)
        if bootstrapSysPath:
            if type(bootstrapSysPath) in (list, tuple):
                self.bootstrapSysPath.extend(bootstrapSysPath)
            else:
                self.error('bootstrapSysPath must be list or tuple')
            # pass full set to Provides to share the exact same data
            self.recipe.Provides(
                _bootstrapSysPath=self.bootstrapSysPath)
        pythonFlagNamespace = keywords.pop('pythonFlagNamespace', None)
        if pythonFlagNamespace is not None:
            self.pythonFlagNamespace = pythonFlagNamespace
            self.recipe.Provides(_pythonFlagNamespace=pythonFlagNamespace)
        bootstrapPerlIncPath = keywords.pop('bootstrapPerlIncPath', None)
        if bootstrapPerlIncPath:
            if type(bootstrapPerlIncPath) in (list, tuple):
                self.bootstrapPerlIncPath.extend(bootstrapPerlIncPath)
            else:
                self.error('bootstrapPerlIncPath must be list or tuple')
            # pass full set to Provides to share the exact same data
            self.recipe.Provides(
                _bootstrapPerlIncPath=self.bootstrapPerlIncPath)
        bootstrapRubyLibs = keywords.pop('bootstrapRubyLibs', None)
        if bootstrapRubyLibs is not None:
            if type(bootstrapRubyLibs) in (list, tuple):
                self.bootstrapRubyLibs.extend(bootstrapRubyLibs)
            else:
                self.error('bootstrapRubyLibs must be list or tuple')
            # pass full set to Provides to share the exact same data
            self.recipe.Provides(
                _bootstrapRubyLibs=self.bootstrapRubyLibs)
        _CILPolicyProvides = keywords.pop('_CILPolicyProvides', None)
        if _CILPolicyProvides:
            self._CILPolicyProvides.update(_CILPolicyProvides)
        rpath = keywords.pop('rpath', None)
        if rpath:
            if type(rpath) is str:
                rpath = ('.*', rpath)
            assert(type(rpath) == tuple)
            self.rpathFixup.append(rpath)
        exceptDeps = keywords.pop('exceptDeps', None)
        if exceptDeps:
            if type(exceptDeps) is str:
                exceptDeps = ('.*', exceptDeps)
            assert(type(exceptDeps) == tuple)
            if type(exceptDeps[0]) is tuple:
                self.exceptDeps.extend(exceptDeps)
            else:
                self.exceptDeps.append(exceptDeps)
        if not hasattr(self.recipe, '_reqExceptDeps'):
            self.recipe._reqExceptDeps = []
        self.recipe._reqExceptDeps.extend(self.exceptDeps)

        # Filter out trove deps that are not associated with a file.
        if len(args) >= 2:
            troves = []
            component = re.compile('^[-a-zA-Z0-9]*:[a-zA-Z]+$')
            for arg in args[1:]:
                arg = arg % self.recipe.macros
                # Make sure arg looks like a component
                if not component.match(arg):
                    break
                troves.append(arg.lstrip(':'))
            else:
                self.troveDeps[args[0]] = troves
                args = ()

        _dependency.updateArgs(self, *args, **keywords)
        _addInfo.updateArgs(self, *args, **keywords)

    def preProcess(self):
        macros = self.macros
        self.systemLibPaths = set(os.path.normpath(x % macros)
                                  for x in self.sonameSubtrees)
        self.bootstrapPythonFlags = set(x % macros
                                        for x in self.bootstrapPythonFlags)
        self.bootstrapSysPath = [x % macros for x in self.bootstrapSysPath]
        if self.pythonFlagNamespace is not None:
            self.pythonFlagNamespace = self.pythonFlagNamespace % macros
        self.bootstrapPerlIncPath = [x % macros for x in self.bootstrapPerlIncPath]

        # anything that any buildreqs have caused to go into ld.so.conf
        # or ld.so.conf.d/*.conf is a system library by definition,
        # but only look at paths, not (for example) "include" lines
        if os.path.exists('/etc/ld.so.conf'):
            self.systemLibPaths |= set(os.path.normpath(x.strip())
                for x in file('/etc/ld.so.conf').readlines()
                if x.startswith('/'))
        for fileName in fixedglob.glob('/etc/ld.so.conf.d/*.conf'):
            self.systemLibPaths |= set(os.path.normpath(x.strip())
                for x in file(fileName).readlines()
                if x.startswith('/'))
        self.rpathFixup = [(filter.Filter(x, macros), y % macros)
                           for x, y in self.rpathFixup]
        exceptDeps = []
        for fE, rE in self.exceptDeps:
            try:
                exceptDeps.append((filter.Filter(fE, macros), re.compile(rE % macros)))
            except sre_constants.error, e:
                self.error('Bad regular expression %s for file spec %s: %s', rE, fE, e)
        self.exceptDeps= exceptDeps
        _dependency.preProcess(self)

    def postProcess(self):
        self._delPythonRequiresModuleFinder()

        components = {}
        for comp in self.recipe.autopkg.getComponents():
            components[comp.getName()] = comp
            shortName = comp.getName().split(':')[1]

            # Mark copmonent names with duplicates
            if shortName in components:
                components[shortName] = None
            else:
                components[shortName] = comp

        # r.Requires('foo:runtime', 'msi')
        # r.Requires('foo:runtime', ':msi')
        # r.Requires('foo:runtime', 'bar:msi')
        depClass = deps.TroveDependencies
        for info, troves in self.troveDeps.iteritems():
            # Sanity check inputs.
            if ':' not in info:
                self.error('package dependency %s not allowed', info)
                return
            for trove in troves:
                if trove not in components:
                    self.error('no component named %s', trove)
                    return
                if components[trove] is None:
                    self.error('specified component name matches multiple '
                        'components %s', trove)
                    return

            # Add the trove dependency.
            dep = deps.Dependency(info)
            for trove in troves:
                components[trove].requires.addDep(depClass, dep)

    def doFile(self, path):
        pkgs = self.recipe.autopkg.findComponents(path)
        if not pkgs:
            return
        pkgFiles = [(x, x.getFile(path)) for x in pkgs]
        # this file object used only for tests, not for doing packaging
        f = pkgFiles[0][1]
        macros = self.recipe.macros
        fullpath = macros.destdir + path
        m = self.recipe.magic[path]

        if self._isELF(m, 'requires'):
            isnset = m.contents['isnset']
            if isnset in self.allowableIsnSets:
                # only add requirements for architectures
                # that we are actually building for (this may include
                # major and minor architectures)
                self._addELFRequirements(path, m, pkgFiles)

        # now go through explicit requirements
        for info in self.included:
            for filt in self.included[info]:
                if filt.match(path):
                    self._markManualRequirement(info, path, pkgFiles, m)

        # now check for automatic dependencies besides ELF
        if f.inode.perms() & 0111 and m and m.name == 'script':
            interp = m.contents['interpreter']
            if interp.strip().startswith('/') and self._checkInclusion(interp,
                                                                       path):
                # no interpreter string warning is in BadInterpreterPaths
                if not (os.path.exists(interp) or
                        os.path.exists(macros.destdir+interp)):
                    # this interpreter not on system, warn
                    # cannot be an error to prevent buildReq loops
                    self.warn('interpreter "%s" (referenced in %s) missing',
                        interp, path)
                    # N.B. no special handling for /{,usr/}bin/env here;
                    # if there has been an exception to
                    # NormalizeInterpreterPaths, then it is a
                    # real dependency on the env binary
                self._addRequirement(path, interp, [], pkgFiles,
                                     deps.FileDependencies)

        if (f.inode.perms() & 0111 and m and m.name == 'script' and
            os.path.basename(m.contents['interpreter']).startswith('python')):
            self._addPythonRequirements(path, fullpath, pkgFiles)
        elif self._isPython(path):
            self._addPythonRequirements(path, fullpath, pkgFiles)

        if (f.inode.perms() & 0111 and m and m.name == 'script' and
            os.path.basename(m.contents['interpreter']).startswith('ruby')):
            self._addRubyRequirements(path, fullpath, pkgFiles, script=True)
        elif '/ruby/' in path and path.endswith('.rb'):
            self._addRubyRequirements(path, fullpath, pkgFiles, script=False)

        if self._isCIL(m):
            if not self.monodisPath:
                self.monodisPath = self._getmonodis(macros, path)
                if not self.monodisPath:
                    return
            p = util.popen('%s --assemblyref %s' %(
                           self.monodisPath, fullpath))
            for line in [ x.strip() for x in p.readlines() ]:
                if ': Version=' in line:
                    ver = line.split('=')[1]
                elif 'Name=' in line:
                    name = line.split('=')[1]
                    self._addRequirement(path, name, [ver], pkgFiles,
                                         deps.CILDependencies)
            p.close()

        elif self.CILPolicyRE.match(path):
            name, ver = self._CILPolicyProvides[path]
            self._addRequirement(path, name, [ver], pkgFiles, deps.CILDependencies)

        if self._isJava(m, 'requires'):
            self._addJavaRequirements(path, m, pkgFiles)

        db = self._getDb()
        if self._isPerl(path, m, f):
            perlReqs = self._getPerlReqs(path, fullpath)
            for req in perlReqs:
                thisReq = deps.parseDep('perl: ' + req)
                if db.getTrovesWithProvides([thisReq]) or [
                        x for x in self.recipe.autopkg.getComponents()
                        if x.provides.satisfies(thisReq)]:
                    self._addRequirement(path, req, [], pkgFiles,
                                         deps.PerlDependencies)

        self.whiteOut(path, pkgFiles)
        self.unionDeps(path, pkgFiles)

    def _addJavaRequirements(self, path, m, pkgFiles):
        if not hasattr(self.recipe, '_internalJavaDepMap'):
            self.recipe._internalJavaDepMap = {}
        fileDeps = self.recipe._internalJavaDepMap.get(path, {})
        reqs = set()
        for fpath, (fprov, freq) in fileDeps.items():
            if freq is not None:
                reqs.update(freq)
        for req in reqs:
            self._addRequirement(path, req, [], pkgFiles,
                                 deps.JavaDependencies)


    def whiteOut(self, path, pkgFiles):
        # remove intentionally discarded dependencies
        for pkg, _ in pkgFiles:
            if self.exceptDeps and path in pkg.requiresMap:
                depSet = deps.DependencySet()
                for depClass, dep in pkg.requiresMap[path].iterDeps():
                    for filt, exceptRe in self.exceptDeps:
                        if filt.match(path):
                            matchName = '%s: %s' %(depClass.tagName, str(dep))
                            if exceptRe.match(matchName):
                                # found one to not copy
                                dep = None
                                break
                    if dep is not None:
                        depSet.addDep(depClass, dep)
                pkg.requiresMap[path] = depSet

    def unionDeps(self, path, pkgFiles):
        # finally, package the dependencies up
        for pkg, f in pkgFiles:
            if path in pkg.requiresMap:
                # files should not require items they provide directly. CNY-2177
                f.requires.set(pkg.requiresMap[path] - f.provides())
                pkg.requires.union(f.requires())

    def _addELFRequirements(self, path, m, pkgFiles):
        """
        Add ELF and abi dependencies, including paths when not shlibs
        """

        def appendUnique(ul, items):
            for item in items:
                if item not in ul:
                    ul.append(item)

        def _canonicalRPATH(rpath, glob=False):
            # normalize all elements of RPATH
            l = [ util.normpath(x) for x in rpath.split(':') ] # CNY-3425
            # prune system paths and relative paths from RPATH
            l = [ x for x in l
                  if x not in self.systemLibPaths and x.startswith('/') ]
            if glob:
                destdir = self.macros.destdir
                dlen = len(destdir)
                gl = []
                for item in l:
                    # prefer destdir elements
                    paths = util.braceGlob(destdir + item)
                    paths = [ os.path.normpath(x[dlen:]) for x in paths ]
                    appendUnique(gl, paths)
                    # then look on system
                    paths = util.braceGlob(item)
                    paths = [ os.path.normpath(x) for x in paths ]
                    appendUnique(gl, paths)
                l = gl
            return l

        rpathList = []
        def _findSonameInRpath(soname):
            for rpath in rpathList:
                destpath = '/'.join((self.macros.destdir, rpath, soname))
                if os.path.exists(destpath):
                    return rpath
                destpath = '/'.join((rpath, soname))
                if os.path.exists(destpath):
                    return rpath
            # didn't find anything
            return None

        # fixup should come first so that its path elements can override
        # the included RPATH if necessary
        if self.rpathFixup:
            for f, rpath in self.rpathFixup:
                if f.match(path):
                    # synthetic RPATH items are globbed
                    rpathList = _canonicalRPATH(rpath, glob=True)
                    break

        if m and 'RPATH' in m.contents and m.contents['RPATH']:
            rpathList += _canonicalRPATH(m.contents['RPATH'])

        depSet = self._createELFDepSet(m, m.contents['requires'],
                                       libPathMap=self._privateDepMap,
                                       getRPATH=_findSonameInRpath,
                                       path=path, isProvides=False)
        for pkg, _ in pkgFiles:
            self._addDepSetToMap(path, pkg.requiresMap, depSet)


    def _getPythonRequiresSysPath(self, pathName):
        # Generate the correct sys.path for finding the required modules.
        # we use the built in site.py to generate a sys.path for the
        # current system and another one where destdir is the root.
        # note the below code is similar to code in Provides,
        # but it creates an ordered path list with and without destdir prefix,
        # while provides only needs a complete list without destdir prefix.
        # Returns tuple:
        #  (sysPath, pythonModuleFinder, pythonVersion)

        pythonPath, bootstrapPython = self._getPython(self.macros, pathName)
        if not pythonPath:
            return (None, None, None)
        if pythonPath in self.pythonSysPathMap:
            return self.pythonSysPathMap[pythonPath]
        destdir = self.macros.destdir
        libdir = self.macros.libdir
        pythonVersion = self._getPythonVersion(pythonPath, destdir, libdir)

        # Start with paths inside the destdir so that imports within a package
        # are discovered correctly.
        systemPaths = self._getPythonSysPath(pythonPath, destdir, libdir,
                useDestDir=True)
        # Now add paths from the system (or bootstrap python)
        systemPaths += self._getPythonSysPath(pythonPath, destdir, libdir,
                useDestDir=False)
        if not bootstrapPython:
            # update pythonTroveFlagCache to require correct flags
            self._getPythonTroveFlags(pythonPath)
        # Keep original order for use with the module finder.
        sysPathForModuleFinder = list(systemPaths)
        # Strip destdir and sort in descending order for converting paths to
        # qualified python module names.
        sysPath = sorted(set(self._stripDestDir(systemPaths, destdir)),
                reverse=True)

        # load module finder after sys.path is restored
        # in case delayed importer is installed.
        pythonModuleFinder = self._getPythonRequiresModuleFinder(
            pythonPath, destdir, libdir, sysPathForModuleFinder,
            bootstrapPython)

        self.pythonSysPathMap[pythonPath] = (
            sysPath, pythonModuleFinder, pythonVersion)
        return self.pythonSysPathMap[pythonPath]

    def _getPythonRequiresModuleFinder(self, pythonPath, destdir, libdir, sysPath, bootstrapPython):

        if self.recipe.isCrossCompiling():
            return None
        if pythonPath not in self.pythonModuleFinderMap:
            try:
                self.pythonModuleFinderMap[pythonPath] = pydeps.moduleFinderProxy(pythonPath, destdir, libdir, sysPath, self.error)
            except pydeps.ModuleFinderInitializationError, e:
                if bootstrapPython:
                    # another case, like isCrossCompiling, where we cannot
                    # run pythonPath -- ModuleFinderInitializationError
                    # is raised before looking at any path, so should
                    # be consistent for any pythonPath
                    self.pythonModuleFinderMap[pythonPath] = None
                else:
                    raise
        return self.pythonModuleFinderMap[pythonPath]

    def _delPythonRequiresModuleFinder(self):
        for finder in self.pythonModuleFinderMap.values():
            if finder is not None:
                finder.close()


    def _addPythonRequirements(self, path, fullpath, pkgFiles):
        destdir = self.recipe.macros.destdir
        destDirLen = len(destdir)

        (sysPath, pythonModuleFinder, pythonVersion
        )= self._getPythonRequiresSysPath(path)

        if not sysPath:
            # Probably a bad interpreter path
            return

        if not pythonModuleFinder:
            # We cannot (reliably) determine runtime python requirements
            # in the cross-compile case, so don't even try (for
            # consistency).
            return

        pythonModuleFinder.load_file(fullpath)
        data = pythonModuleFinder.getDepsForPath(fullpath)
        if data['result'] != 'ok':
            self.info('File %s is not a valid python file', path)
            return

        for depPath in data['paths']:
            if not depPath:
                continue
            flags = None
            absPath = None
            if depPath.startswith(destdir):
                depPath = depPath[destDirLen:]
                flags = self._getPythonFlagsFromPath(depPath)

                # The file providing this dependency is part of this package.
                absPath = depPath
            for sysPathEntry in sysPath:
                if depPath.startswith(sysPathEntry):
                    newDepPath = depPath[len(sysPathEntry)+1:]
                    if newDepPath not in ('__init__', '__init__.py'):
                        # we don't allow bare __init__'s as dependencies.
                        # hopefully we'll find this at deeper level in
                        # in the sysPath
                        if flags is None:
                            # this is provided by the system, so we have
                            # to see with which flags it is provided with
                            flags = self._getPythonFlags(depPath,
                                self.bootstrapPythonFlags)
                        depPath = newDepPath
                        break

            if depPath.startswith('/'):
                # a python file not found in sys.path will not have been
                # provided, so we must not depend on it either
                return
            if not (depPath.endswith('.py') or depPath.endswith('.pyc') or
                    depPath.endswith('.so')):
                # Not something we provide, so not something we can
                # require either.  Drop it and go on.  We have seen
                # this when a script in /usr/bin has ended up in the
                # requires list.
                continue

            if depPath.endswith('module.so'):
                # Strip 'module.so' from the end, make it a candidate
                cands = [ depPath[:-9] + '.so', depPath ]
                cands = [ self._normalizePythonDep(x) for x in cands ]
                if absPath:
                    depName = self._checkPackagePythonDeps(pkgFiles, absPath,
                                                           cands, flags)
                else:
                    depName = self._checkSystemPythonDeps(cands, flags)
            else:
                depName = self._normalizePythonDep(depPath)
                if depName == '__future__':
                    continue
            self._addRequirement(path, depName, flags, pkgFiles,
                                 deps.PythonDependencies)

        #if data['missing']:
        #    self.warn("Python file %s is missing requirements: %s" % (
        #        path, ', '.join(data['missing'])))

    def _checkPackagePythonDeps(self, pkgFiles, depPath, depNames, flags):
        # Try to match depNames against all current packages
        # Use the last value in depNames as the fault value
        assert depNames, "No dependencies passed"
        for pkg, _ in pkgFiles:
            if depPath in pkg:
                fileProvides = pkg[depPath][1].provides()

                if flags:
                    flags = [ (x, deps.FLAG_SENSE_REQUIRED) for x in flags ]

                # Walk the depNames list in order, pick the first dependency
                # available.
                for dp in depNames:
                    depSet = deps.DependencySet()
                    depSet.addDep(deps.PythonDependencies,
                                  deps.Dependency(dp, flags))
                    if fileProvides.intersection(depSet):
                        # this dep is provided
                        return dp

        # If we got here, the file doesn't provide this dep. Return the last
        # candidate and hope for the best
        return depNames[-1]

    def _checkSystemPythonDeps(self, depNames, flags):
        if flags:
            flags = [ (x, deps.FLAG_SENSE_REQUIRED) for x in flags ]

        for dp in depNames:
            depSet = deps.DependencySet()
            depSet.addDep(deps.PythonDependencies, deps.Dependency(dp, flags))
            troves = self.depCache.getProvides([depSet])
            if troves:
                return dp
        return depNames[-1]

    def _normalizePythonDep(self, depName):
        # remove extension
        depName = depName.rsplit('.', 1)[0]
        depName = depName.replace('/', '.')
        depName = depName.replace('.__init__', '')
        depName = self.pythonInterpRE.sub('', depName)
        return depName

    def _addRubyRequirements(self, path, fullpath, pkgFiles, script=False):
        macros = self.recipe.macros
        destdir = macros.destdir
        destDirLen = len(destdir)

        if self.rubyInterpreter is None:
            self.rubyInterpreter, bootstrap = self._getRuby(macros, path)
            if not self.rubyInterpreter:
                return
            self.rubyInvocation, self.rubyLoadPath = self._getRubyLoadPath(
                macros, self.rubyInterpreter, bootstrap)
            self.rubyVersion = self._getRubyVersion(macros)
        elif self.rubyInterpreter is False:
            return

        if not script:
            if not util.isregular(fullpath) or os.path.islink(fullpath):
                return
            foundInLoadPath = False
            for pathElement in self.rubyLoadPath:
                if path.startswith(pathElement):
                    foundInLoadPath = True
                    break
            if not foundInLoadPath:
                return

        # This is a very limited hack, but will work for the 90% case
        # better parsing may be written later
        # Note that we only honor "require" at the beginning of
        # the line and only requirements enclosed in single quotes
        # to avoid conditional requirements and requirements that
        # do any sort of substitution.  Because most ruby packages
        # contain multiple ruby modules, getting 90% of the ruby
        # dependencies will find most of the required packages in
        # practice
        depEntries = [x.strip() for x in file(fullpath)
                      if x.startswith('require ') or
                         x.startswith('require(')]
        depEntries = (x.split() for x in depEntries)
        depEntries = (x[1].strip("\"'") for x in depEntries
                      if len(x) == 2 and x[1].startswith("'") and
                                         x[1].endswith("'"))
        depEntries = set(depEntries)

        # I know of no way to ask ruby to report deps from scripts
        # Unfortunately, so far it seems that there are too many
        # Ruby modules which have code that runs in the body; this
        # code runs slowly, has not been useful in practice for
        # filtering out bogus dependencies, and has been hanging
        # and causing other unintended side effects from modules
        # that have code in the main body.
        #if not script:
        #    depClosure = util.popen(r'''%s -e "require '%s'; puts $\""'''
        #        %(self.rubyInvocation%macros, fullpath)).readlines()
        #    depClosure = set([x.split('.')[0] for x in depClosure])
        #    # remove any entries from the guessed immediate requirements
        #    # that are not in the closure
        #    depEntries = set(x for x in depEntries if x in depClosure)

        def _getDepEntryPath(depEntry):
            for prefix in (destdir, ''):
                for pathElement in self.rubyLoadPath:
                    for suffix in ('.rb', '.so'):
                        candidate = util.searchPath(
                            os.path.basename(depEntry) + suffix,
                            prefix + pathElement,
                            )
                        if candidate:
                            return candidate
            return None

        for depEntry in depEntries:
            depEntryPath = _getDepEntryPath(depEntry)
            if depEntryPath is None:
                continue
            if depEntryPath.startswith(destdir):
                depPath = depEntryPath[destDirLen:]
            else:
                depPath = depEntryPath
            flags = self._getRubyFlagsFromPath(depPath, self.rubyVersion)
            self._addRequirement(path, depEntry, flags, pkgFiles,
                                 deps.RubyDependencies)

    def _fetchPerl(self):
        """
        Cache the perl path and @INC path with -I%(destdir)s prepended to
        each element if necessary
        """
        if self.perlPath is not None:
            return

        macros = self.recipe.macros
        self.perlPath, perlIncPath, perlDestInc = self._getperl(macros, self.recipe)
        if perlDestInc:
            self.perlIncArgs = perlDestInc
        else:
            self.perlIncArgs = ' '.join('-I'+x for x in perlIncPath)

    def _getPerlReqs(self, path, fullpath):
        if self.perlReqs is None:
            self._fetchPerl()
            if not self.perlPath:
                # no perl == bootstrap, but print warning
                self.info('Unable to find perl interpreter,'
                           ' disabling perl: requirements')
                self.perlReqs = False
                return []
            # get the base directory where conary lives.  In a checked
            # out version, this would be .../conary/conary/build/package.py
            # chop off the last 3 directories to find where
            # .../conary/Scandeps and .../conary/scripts/perlreqs.pl live
            basedir = '/'.join(sys.modules[__name__].__file__.split('/')[:-3])
            scandeps = '/'.join((basedir, 'conary/ScanDeps'))
            if (os.path.exists(scandeps) and
                os.path.exists('%s/scripts/perlreqs.pl' % basedir)):
                perlreqs = '%s/scripts/perlreqs.pl' % basedir
            else:
                # we assume that conary is installed in
                # $prefix/$libdir/python?.?/site-packages.  Use this
                # assumption to find the prefix for
                # /usr/lib/conary and /usr/libexec/conary
                regexp = re.compile(r'(.*)/lib(64){0,1}/python[1-9].[0-9]/site-packages')
                match = regexp.match(basedir)
                if not match:
                    # our regexp didn't work.  fall back to hardcoded
                    # paths
                    prefix = '/usr'
                else:
                    prefix = match.group(1)
                # ScanDeps is not architecture specific
                scandeps = '%s/lib/conary/ScanDeps' %prefix
                if not os.path.exists(scandeps):
                    # but it might have been moved to lib64 for multilib
                    scandeps = '%s/lib64/conary/ScanDeps' %prefix
                perlreqs = '%s/libexec/conary/perlreqs.pl' %prefix
            self.perlReqs = '%s -I%s %s %s' %(
                self.perlPath, scandeps, self.perlIncArgs, perlreqs)
        if self.perlReqs is False:
            return []

        cwd = os.getcwd()
        os.chdir(os.path.dirname(fullpath))
        try:
            p = os.popen('%s %s' %(self.perlReqs, fullpath))
        finally:
            try:
                os.chdir(cwd)
            except:
                pass
        reqlist = [x.strip().split('//') for x in p.readlines()]
        # make sure that the command completed successfully
        rc = p.close()
        if rc:
            # make sure that perl didn't blow up
            assert(os.WIFEXITED(rc))
            # Apparantly ScanDeps could not handle this input
            return []

        # we care only about modules right now
        # throwing away the filenames for now, but we might choose
        # to change that later
        reqlist = [x[2] for x in reqlist if x[0] == 'module']
        # foo/bar/baz.pm -> foo::bar::baz
        reqlist = ['::'.join(x.split('/')).rsplit('.', 1)[0] for x in reqlist]

        return reqlist

    def _markManualRequirement(self, info, path, pkgFiles, m):
        flags = []
        if self._checkInclusion(info, path):
            if info[0] == '/':
                depClass = deps.FileDependencies
            elif info.startswith('file:') and info[5:].strip()[0] == '/':
                info = info[5:].strip()
                depClass = deps.FileDependencies
            elif info.startswith('soname:'):
                if not m or m.name != 'ELF':
                    # only an ELF file can have a soname requirement
                    return
                # we need to synthesize a dependency that encodes the
                # same ABI as this binary
                depClass = deps.SonameDependencies
                for depType, dep, f in m.contents['requires']:
                    if depType == 'abi':
                        flags = tuple(x == 'Linux' and 'SysV' or x
                                      for x in f) # CNY-3604
                        info = '%s/%s' %(dep, info.split(None, 1)[1])
                        info = os.path.normpath(info)
            else: # by process of elimination, must be a trove
                if info.startswith('group-'):
                    self.error('group dependency %s not allowed', info)
                    return
                if info.startswith('fileset-'):
                    self.error('fileset dependency %s not allowed', info)
                    return
                if ':' not in info:
                    self.error('package dependency %s not allowed', info)
                    return
                depClass = deps.TroveDependencies
            self._addRequirement(path, info, flags, pkgFiles, depClass)

    def _checkInclusion(self, info, path):
        if info in self.excluded:
            for filt in self.excluded[info]:
                # exception handling is per-requirement,
                # so handled specially
                if filt.match(path):
                    self.info('ignoring requirement match for %s: %s',
                              path, info)
                    return False
        return True

    def _addRequirement(self, path, info, flags, pkgFiles, depClass):
        if depClass == deps.FileDependencies:
            pathMap = self.recipe.autopkg.pathMap
            componentMap = self.recipe.autopkg.componentMap
            if (info in pathMap and not
                componentMap[info][info][1].flags.isPathDependencyTarget()):
                # if a package requires a file, includes that file,
                # and does not provide that file, it should error out
                self.error('%s requires %s, which is included but not'
                           ' provided; use'
                           " r.Provides('file', '%s')", path, info, info)
                return

        # in some cases, we get literal "(flags)" from the recipe
        if '(' in info:
            flagindex = info.index('(')
            flags = set(info[flagindex+1:-1].split() + list(flags))
            info = info.split('(')[0]

        # CNY-3443
        if depClass in self.removeFlagsByDependencyClassMap:
            flags = set(flags)
            for ignoreItem in self.removeFlagsByDependencyClassMap[depClass]:
                if isinstance(ignoreItem, set):
                    ignoreFlags = ignoreItem
                else:
                    ignoreFlags = set(f for f in flags if ignoreItem.match(f))
                flags -= ignoreFlags

        if flags:
            flags = [ (x, deps.FLAG_SENSE_REQUIRED) for x in flags ]

        for pkg, _ in pkgFiles:
            # we may need to create a few more DependencySets.
            if path not in pkg.requiresMap:
                pkg.requiresMap[path] = deps.DependencySet()
            pkg.requiresMap[path].addDep(depClass, deps.Dependency(info, flags))

class _basePluggableRequires(Requires):
    """
    Base class for pluggable Requires policies.
    """

    # This set of policies get executed before the Requires policy,
    # and inherits the Requires' ordering constraints
    requires = list(Requires.requires) + [
        ('Requires', policy.REQUIRED_SUBSEQUENT),
    ]

    def preProcess(self):
        # We want to inherit the exceptions from the Requires class, so we
        # need to peek into the Required policy object. We can still pass
        # explicit exceptions into the pluggable sub-policies, and they will
        # only apply to the sub-policy.
        exceptions = self.recipe._policyMap['Requires'].exceptions
        if exceptions:
            Requires.updateArgs(self, exceptions=exceptions,
                    allowUnusedFilters = True)
        Requires.preProcess(self)

    def reportErrors(self, *args, **kwargs):
        return self.recipe._policyMap['Requires'].reportErrors(*args, **kwargs)

    def error(self, *args, **kwargs):
        return self.recipe._policyMap['Requires'].error(*args, **kwargs)

    def warn(self, *args, **kwargs):
        return self.recipe._policyMap['Requires'].warn(*args, **kwargs)

    def info(self, *args, **kwargs):
        return self.recipe._policyMap['Requires'].info(*args, **kwargs)

    def _addClassName(self, *args, **kwargs):
        return self.recipe._policyMap['Requires']._addClassName(*args, **kwargs)

    def doFile(self, path):
        pkgs = self.recipe.autopkg.findComponents(path)
        if not pkgs:
            return
        pkgFiles = [(x, x.getFile(path)) for x in pkgs]
        macros = self.recipe.macros
        fullpath = macros.destdir + path

        self.addPluggableRequirements(path, fullpath, pkgFiles, macros)

        self.whiteOut(path, pkgFiles)
        self.unionDeps(path, pkgFiles)

    def addPluggableRequirements(self, path, fullpath, pkgFiles, macros):
        """Override in subclasses"""
        pass

class RemoveSelfProvidedRequires(policy.Policy):
    """
    This policy is used to remove component requirements when they are provided
    by the component itself.
    Do not call it directly; it is for internal use only.
    """
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('Requires', policy.REQUIRED_PRIOR),
    )
    supported_targets = (TARGET_LINUX, TARGET_WINDOWS)

    def do(self):
        if use.Use.bootstrap._get():
            return

        for comp in self.recipe.autopkg.getComponents():
            comp.requires -= comp.provides

class Flavor(policy.Policy):
    """
    NAME
    ====
    B{C{r.Flavor()}} - Controls the Flavor mechanism

    SYNOPSIS
    ========
    C{r.Flavor([I{filterexp}] | [I{exceptions=filterexp}])}

    DESCRIPTION
    ===========
    The C{r.Flavor} policy marks files with the appropriate Flavor.
    To except a file's flavor from being marked, use:
    C{r.Flavor(exceptions='I{filterexp}')}.

    EXAMPLES
    ========
    C{r.Flavor(exceptions='%(crossprefix)s/lib/gcc-lib/.*')}

    Files in the directory C{%(crossprefix)s/lib/gcc-lib} are being excepted
    from having their Flavor marked, because they are not flavored for
    the system on which the trove is being installed.
    """
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('Requires', policy.REQUIRED_PRIOR),
        # For example: :lib component contains only a single packaged empty
        # directory, which must be artificially flavored for multilib
        ('ExcludeDirectories', policy.REQUIRED_PRIOR),
    )
    filetree = policy.PACKAGE
    supported_targets = (TARGET_LINUX, TARGET_WINDOWS)

    def preProcess(self):
        self.libRe = re.compile(
            '^(%(libdir)s'
            '|/%(lib)s'
            '|%(x11prefix)s/%(lib)s'
            '|%(krbprefix)s/%(lib)s)(/|$)' %self.recipe.macros)
        self.libReException = re.compile('^/usr/(lib|%(lib)s)/(python|ruby).*$')
        self.baseIsnset = use.Arch.getCurrentArch()._name
        self.baseArchFlavor = use.Arch.getCurrentArch()._toDependency()
        self.archFlavor = use.createFlavor(None, use.Arch._iterUsed())
        self.packageFlavor = deps.Flavor()
        self.troveMarked = False
        self.componentMap = self.recipe.autopkg.componentMap
        ISD = deps.InstructionSetDependency
        TISD = deps.TargetInstructionSetDependency
        instructionDeps = list(self.recipe._buildFlavor.iterDepsByClass(ISD))
        instructionDeps += list(self.recipe._buildFlavor.iterDepsByClass(TISD))
        self.allowableIsnSets = [ x.name for x in instructionDeps ]

    def postProcess(self):
        # If this is a Windows package, include the flavor from the windows
        # helper.
        if (self._getTarget() == TARGET_WINDOWS and
            hasattr(self.recipe, 'winHelper')):

            flavorStr = self.recipe.winHelper.flavor
            if flavorStr:
                self.packageFlavor.union(deps.parseFlavor(flavorStr))

        # all troves need to share the same flavor so that we can
        # distinguish them later
        for pkg in self.recipe.autopkg.components.values():
            pkg.flavor.union(self.packageFlavor)

    def hasLibInPath(self, path):
        return self.libRe.match(path) and not self.libReException.match(path)

    def hasLibInDependencyFlag(self, path, f):
        for depType in (deps.PythonDependencies, deps.RubyDependencies):
            for dep in ([x for x in f.requires.deps.iterDepsByClass(depType)] +
                        [x for x in f.provides.deps.iterDepsByClass(depType)]):
                flagNames = [x[0] for x in dep.getFlags()[0]]
                flagNames = [x for x in flagNames if x.startswith('lib')]
                if flagNames:
                    return True
        return False

    def doFile(self, path):
        autopkg = self.recipe.autopkg
        pkg = autopkg.findComponent(path)
        if pkg is None:
            return
        f = pkg.getFile(path)
        m = self.recipe.magic[path]
        if m and m.name == 'ELF' and 'isnset' in m.contents:
            isnset = m.contents['isnset']
        elif self.hasLibInPath(path) or self.hasLibInDependencyFlag(path, f):
            # all possible paths in a %(lib)s-derived path get default
            # instruction set assigned if they don't have one already
            if f.hasContents:
                isnset = self.baseIsnset
            else:
                # this file can't be marked by arch, but the troves
                # and package must be.  (e.g. symlinks and empty directories)
                # we don't need to union in the base arch flavor more
                # than once.
                if self.troveMarked:
                    return
                self.packageFlavor.union(self.baseArchFlavor)
                self.troveMarked = True
                return
        else:
            return

        flv = deps.Flavor()
        flv.addDep(deps.InstructionSetDependency, deps.Dependency(isnset, []))
        # get the Arch.* dependencies
        # set the flavor for the file to match that discovered in the
        # magic - but do not let that propagate up to the flavor of
        # the package - instead the package will have the flavor that
        # it was cooked with.  This is to avoid unnecessary or extra files
        # causing the entire package from being flavored inappropriately.
        # Such flavoring requires a bunch of Flavor exclusions to fix.
        # Note that we need to set all shared paths between containers
        # to share flavors and ensure that fileIds are the same
        for pkg in autopkg.findComponents(path):
            f = pkg.getFile(path)
            f.flavor.set(flv)

        # get the Arch.* dependencies
        flv.union(self.archFlavor)
        if isnset in self.allowableIsnSets:
            self.packageFlavor.union(flv)


class _ProcessInfoPackage(policy.UserGroupBasePolicy):
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('ComponentSpec', policy.REQUIRED_PRIOR),
        ('Provides', policy.CONDITIONAL_PRIOR),
        ('Requires', policy.CONDITIONAL_PRIOR),
        ('Config', policy.CONDITIONAL_PRIOR),
        ('InitialContents', policy.CONDITIONAL_PRIOR)
    )

    def preProcess(self):
        if self.exceptions:
            self.error('%s does not honor exceptions' % self.__class__.__name__)
            self.exceptions = None
        if self.inclusions:
            self.inclusions = None

    def doFile(self, path):
        expectedName = 'info-%s:%s' % (os.path.basename(path), self.component)
        comp = self.recipe.autopkg.componentMap[path]
        compName = comp.name
        if not isinstance(comp.getFile(path), files.RegularFile):
            self.error("Only regular files may appear in '%s'" % expectedName)
            return
        if len(comp) > 1:
            badPaths = [x for x in comp if x != path]
            self.error("The following files are not allowed in '%s': '%s'" % \
                    (compName, "', '".join(badPaths)))
        else:
            fileObj = comp[path][1]
            for tag in fileObj.tags():
                self.error("TagSpec '%s' is not allowed for %s" % \
                        (tag, expectedName))

            fileObj.tags.set('%s-info' % self.component)
            fileObj.flags.isTransient(True)
            self.parseError = False
            self.addProvides(path)
            if not self.parseError:
                self.addRequires(path)

    def parseInfoFile(self, path):
        infoname = "info-%s:%s" % (os.path.basename(path), self.component)
        data = {}
        try:
            data = dict([x.strip().split('=', 1) \
                    for x in open(path).readlines()])
            extraKeys = set(data.keys()).difference(self.legalKeys)
            if extraKeys:
                for key in extraKeys:
                    self.error("%s is not is not a valid value for %s" % \
                            (key, infoname))
                    self.parseError = True
        except ValueError:
            self.error("Unable to parse info file for '%s'" % infoname)
            self.parseError = True
        return data

    def addProvides(self, path):
        realpath, fileObj = self.recipe.autopkg.findComponent(path)[path]
        data = self.parseInfoFile(realpath)
        pkg = self.recipe.autopkg.componentMap[path]
        infoname = os.path.basename(path)
        if path in pkg.providesMap:
            # only deps related to userinfo/troveinfo are allowed
            self.error("Illegal provision for 'info-%s:%s': '%s'" % \
                    (infoname, self.component, str(pkg.providesMap[path])))

        pkg.providesMap[path] = deps.DependencySet()
        depSet = self.getProvides(infoname, data)

        fileObj.provides.set(depSet)
        pkg.providesMap[path].union(depSet)
        pkg.provides.union(depSet)

    def addRequires(self, path):
        realpath, fileObj = self.recipe.autopkg.findComponent(path)[path]
        data = self.parseInfoFile(realpath)
        pkg = self.recipe.autopkg.componentMap[path]
        infoname = os.path.basename(path)
        if path in pkg.requiresMap:
            # only deps related to userinfo/troveinfo are allowed
            self.error("Illegal requirement on 'info-%s:%s': '%s'" % \
                    (infoname, self.component, str(pkg.requiresMap[path])))
        pkg.requiresMap[path] = deps.DependencySet()
        depSet = self.getRequires(infoname, data)

        fileObj.requires.set(depSet)
        pkg.requiresMap[path].union(depSet)
        pkg.requires.union(depSet)

class ProcessUserInfoPackage(_ProcessInfoPackage):
    """
    NAME
    ====
    B{C{r.ProcessUserInfoPackage()}} - Set dependencies and tags for User
    info packages

    SYNOPSIS
    ========
    C{r.ProcessUserInfoPackage()}

    DESCRIPTION
    ===========
    The C{r.ProcessUserInfoPackage} policy automatically sets up provides
    and requries, as well as tags for user info files create by the
    C{r.User} build action.

    This policy is not intended to be invoked from recipes. Do not use it.
    """
    invariantsubtrees = ['%(userinfodir)s']
    component = 'user'
    legalKeys = ['PREFERRED_UID', 'GROUP', 'GROUPID', 'HOMEDIR', 'COMMENT',
            'SHELL', 'SUPPLEMENTAL', 'PASSWORD']

    def parseInfoFile(self, path):
        if self.recipe._getCapsulePathsForFile(path):
            return {}
        data = _ProcessInfoPackage.parseInfoFile(self, path)
        if data:
            supplemental = data.get('SUPPLEMENTAL')
            if supplemental is not None:
                data['SUPPLEMENTAL'] = supplemental.split(',')
        return data

    def getProvides(self, infoname, data):
        depSet = deps.DependencySet()
        groupname = data.get('GROUP', infoname)
        depSet.addDep(deps.UserInfoDependencies,
                      deps.Dependency(infoname, []))
        if self.recipe._provideGroup.get(infoname, True):
            depSet.addDep(deps.GroupInfoDependencies,
                    deps.Dependency(groupname, []))
        return depSet

    def getRequires(self, infoname, data):
        groupname = data.get('GROUP', infoname)
        supp = data.get('SUPPLEMENTAL', [])
        depSet = deps.DependencySet()
        for grpDep in supp:
            depSet.addDep(deps.GroupInfoDependencies,
                          deps.Dependency(grpDep, []))
        if not self.recipe._provideGroup.get(infoname):
            depSet.addDep(deps.GroupInfoDependencies,
                    deps.Dependency(groupname, []))
        return depSet

class ProcessGroupInfoPackage(_ProcessInfoPackage):
    """
    NAME
    ====
    B{C{r.ProcessGroupInfoPackage()}} - Set dependencies and tags for Group
    info packages

    SYNOPSIS
    ========
    C{r.ProcessGroupInfoPackage()}

    DESCRIPTION
    ===========
    The C{r.ProcessGroupInfoPackage} policy automatically sets up provides
    and requries, as well as tags for group info files create by the
    C{r.Group}  and C{r.SupplementalGroup} build actions.

    This policy is not intended to be invoked from recipes. Do not use it.
    """
    invariantsubtrees = ['%(groupinfodir)s']
    component = 'group'
    legalKeys = ['PREFERRED_GID', 'USER']

    def getProvides(self, groupname, data):
        depSet = deps.DependencySet()
        depSet.addDep(deps.GroupInfoDependencies,
                      deps.Dependency(groupname, []))
        return depSet

    def getRequires(self, groupname, data):
        infoname = data.get('USER')
        depSet = deps.DependencySet()
        if infoname:
            depSet.addDep(deps.UserInfoDependencies,
                          deps.Dependency(infoname, []))
        return depSet


class reportExcessBuildRequires(policy.Policy):
    """
    NAME
    ====
    B{C{r.reportExcessBuildRequires()}} - suggest items to remove from C{buildRequires} list

    SYNOPSIS
    ========
    C{r.reportExcessBuildRequires('required:component')}
    C{r.reportExcessBuildRequires(['list:of', 'required:components'])}

    DESCRIPTION
    ===========
    The C{r.reportExcessBuildRequires()} policy is used to report
    together all suggestions for possible items to remove from the
    C{buildRequires} list.

    The suggestions provided by this policy are build requirements
    listed in the recipe's C{buildRequires} list for which Conary
    has not specifically discovered a need.  Build requirement
    discovery is not perfect, which means that even though this
    policy prints a warning that a build requirement might not be
    necessary, Conary does not know that it is definitely not needed.
    These are only hints.  If you are not sure whether a component
    should be removed from the C{buildRequires} list, it is safer
    to leave it in the list.  This is because an extra component
    in the C{buildRequires} list is very unlikely to cause trouble,
    but a truly missing component causes failure (by definition).

    Because dependencies on C{:runtime} components are the least
    likely dependencies to be discovered automatically, this policy
    currently does not recommend removing any C{:runtime} components.

    EXAMPLES
    ========
    This policy is normally called only internally by other Conary
    policies.  However, a recipe can report build requirements
    that are known by the recipe maintainer to be required but
    which Conary does not discover automatically by passing a
    list of these components.  For example, if this policy
    says that C{foo:devel} and C{blah:perl} are possible extra
    build requirements, but you know that they are required in
    order to correctly build the included software, you can
    turn off the warnings like this:

    C{r.reportExcessBuildRequires(['foo:devel', 'blah:perl'])}

    This will tell the C{reportExcessBuildRequires} policy that
    C{foo:devel} and C{blah:perl} are known to be required to
    build the package.

    No regular expressions are honored.
    """
    bucket = policy.ERROR_REPORTING
    processUnmodified = True
    filetree = policy.NO_FILES
    supported_targets = (TARGET_LINUX, TARGET_WINDOWS)

    def __init__(self, *args, **keywords):
        self.found = set()
        policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
        for arg in args:
            if type(arg) in (list, tuple, set):
                self.found.update(arg)
            else:
                self.found.add(arg)

    def do(self):
        # If absolutely no buildRequires were found automatically,
        # assume that the buildRequires list has been carefully crafted
        # for some reason that the buildRequires enforcement policy
        # doesn't yet support, and don't warn that all of the listed
        # buildRequires might be excessive.
        if self.found and self.recipe._logFile:
            r = self.recipe
            def getReqNames(key):
                return set(x.split('=')[0] for x in r._recipeRequirements[key])
            recipeReqs = getReqNames('buildRequires')
            superReqs = getReqNames('buildRequiresSuper')
            foundPackages = set(x.split(':')[0] for x in self.found)
            superClosure = r._getTransitiveDepClosure(superReqs)
            foundClosure = r._getTransitiveDepClosure(self.found)

            def removeCore(candidates):
                # conary, python, and setup are always required; gcc
                # is often an implicit requirement, and sqlite:lib is
                # listed explicitly make bootstrapping easier
                return set(x for x in candidates if
                           not x.startswith('conary')
                           and not x.startswith('python:')
                           and not x.startswith('gcc:')
                           and not x in ('libgcc:devellib',
                                         'setup:runtime',
                                         'sqlite:lib'))

            def removeSome(candidates):
                # at this point, we don't have good enough detection
                # of :runtime in particular to recommend getting rid
                # of it
                return set(x for x in removeCore(candidates) if
                           not x.endswith(':runtime'))

            def removeDupComponents(candidates):
                # If any component is required, we don't really need
                # to flag others as excessive in superclass excess
                return set(x for x in candidates
                           if x.split(':')[0] not in foundPackages)

            # for superclass reqs
            excessSuperReqs = superReqs - foundClosure
            if excessSuperReqs:
                # note that as this is for debugging only, we do not
                # remove runtime requirements
                deDupedSuperReqs = sorted(list(
                    removeDupComponents(removeCore(excessSuperReqs))))
                if deDupedSuperReqs:
                    self._reportExcessSuperclassBuildRequires(deDupedSuperReqs)

            excessReqs = recipeReqs - self.found
            redundantReqs = recipeReqs.intersection(superClosure)
            if excessReqs or redundantReqs:
                excessBuildRequires = sorted(list(
                    removeSome(excessReqs.union(redundantReqs))))
                # all potential excess build requires might have
                # been removed by removeSome
                if excessBuildRequires:
                    self._reportExcessBuildRequires(excessBuildRequires)

    def _reportExcessBuildRequires(self, reqList):
        self.recipe._logFile.reportExcessBuildRequires(
            sorted(list(reqList)))

    def _reportExcessSuperclassBuildRequires(self, reqList):
        self.recipe._logFile.reportExcessSuperclassBuildRequires(
            sorted(list(reqList)))


class reportMissingBuildRequires(policy.Policy):
    """
    This policy is used to report together all suggestions for
    additions to the C{buildRequires} list.
    Do not call it directly; it is for internal use only.
    """
    bucket = policy.ERROR_REPORTING
    processUnmodified = True
    filetree = policy.NO_FILES
    supported_targets = (TARGET_LINUX, TARGET_WINDOWS)

    def __init__(self, *args, **keywords):
        self.errors = set()
        policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
        for arg in args:
            if type(arg) in (list, tuple, set):
                self.errors.update(arg)
            else:
                self.errors.add(arg)

    def do(self):
        if self.errors and self.recipe._logFile:
            self.recipe._logFile.reportMissingBuildRequires(
                sorted(list(self.errors)))


class reportErrors(policy.Policy, policy.GroupPolicy):
    """
    This policy is used to report together all package errors.
    Do not call it directly; it is for internal use only.
    """
    bucket = policy.ERROR_REPORTING
    processUnmodified = True
    filetree = policy.NO_FILES
    groupError = False
    supported_targets = (TARGET_LINUX, TARGET_WINDOWS)

    def __init__(self, *args, **keywords):
        self.errors = []
        policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
        """
        Called once, with printf-style arguments, for each warning.
        """
        self.errors.append(args[0] %tuple(args[1:]))
        groupError = keywords.pop('groupError', None)
        if groupError is not None:
            self.groupError = groupError

    def do(self):
        if self.errors:
            msg = self.groupError and 'Group' or 'Package'
            raise policy.PolicyError, ('%s Policy errors found:\n%%s' % msg) \
                    % "\n".join(self.errors)

class _TroveScript(policy.PackagePolicy):
    processUnmodified = False
    keywords = { 'contents' : None }

    _troveScriptName = None

    def __init__(self, *args, **keywords):
        policy.PackagePolicy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
        if args:
            troveNames = args
        else:
            troveNames = [ self.recipe.name ]
        self.troveNames = troveNames
        policy.PackagePolicy.updateArgs(self, **keywords)

    def do(self):
        if not self.contents:
            return

        # Build component map
        availTroveNames = dict((x.name, None) for x in
                                self.recipe.autopkg.getComponents())
        availTroveNames.update(self.recipe.packages)
        troveNames = set(self.troveNames) & set(availTroveNames)

        # We don't support compatibility classes for troves (yet)
        self.recipe._addTroveScript(troveNames, self.contents,
            self._troveScriptName, None)

class ScriptPreUpdate(_TroveScript):
    _troveScriptName = 'preUpdate'

class ScriptPostUpdate(_TroveScript):
    _troveScriptName = 'postUpdate'

class ScriptPreInstall(_TroveScript):
    _troveScriptName = 'preInstall'

class ScriptPostInstall(_TroveScript):
    _troveScriptName = 'postInstall'

class ScriptPreErase(_TroveScript):
    _troveScriptName = 'preErase'

class ScriptPostErase(_TroveScript):
    _troveScriptName = 'postErase'

class ScriptPreRollback(_TroveScript):
    _troveScriptName = 'preRollback'

class ScriptPostRollback(_TroveScript):
    _troveScriptName = 'postRollback'
