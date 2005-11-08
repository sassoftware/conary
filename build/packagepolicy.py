#
# Copyright (c) 2004-2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
"""
Module used after C{%(destdir)s} has been finalized to choose packages
and components; set flags, tags, and dependencies; and enforce policy
requirements on the contents of C{%(destdir)s}.

Classes from this module are not used directly; instead, they are accessed
through eponymous interfaces in recipe.  Most of these policies are rarely
(if ever) invoked.  Examples are presented only for policies that are
expected to be invoked in some recipes.
"""
import itertools
import os
import re
import stat

import buildpackage
from local import database
from deps import deps
import destdirpolicy
import files
import filter
from lib import elf, util, log
import policy
import use
import tags


class NonBinariesInBindirs(policy.Policy):
    """
    Directories that are specifically for binaries should have only
    files that have some executable bit set:
    C{r.NonBinariesInBindirs(exceptions=I{filterexp})}
    """
    invariantexceptions = [ ('.*', stat.S_IFDIR) ]
    invariantsubtrees = [
	'%(bindir)s/',
	'%(essentialbindir)s/',
	'%(krbprefix)s/bin/',
	'%(x11prefix)s/bin/',
	'%(sbindir)s/',
	'%(essentialsbindir)s/',
	'%(initdir)s/',
	'%(libexecdir)s/',
	'%(sysconfdir)s/profile.d/',
	'%(sysconfdir)s/cron.daily/',
	'%(sysconfdir)s/cron.hourly/',
	'%(sysconfdir)s/cron.weekly/',
	'%(sysconfdir)s/cron.monthly/',
    ]

    def doFile(self, file):
	d = self.macros['destdir']
	mode = os.lstat(util.joinPaths(d, file))[stat.ST_MODE]
	if not mode & 0111:
            self.error(
                "%s has mode 0%o with no executable permission in bindir",
                file, mode)
	m = self.recipe.magic[file]
	if m and m.name == 'ltwrapper':
            self.error("%s is a build-only libtool wrapper script", file)


class FilesInMandir(policy.Policy):
    """
    The C{%(mandir)s} directory should normally have only files in it;
    the main cause of files in C{%(mandir)s} is confusion in packages
    about whether "mandir" means /usr/share/man or /usr/share/man/man<n>.
    """
    invariantsubtrees = [
        '%(mandir)s',
        '%(x11prefix)s/man',
        '%(krbprefix)s/man',
    ]
    invariantinclusions = [
	(r'.*', None, stat.S_IFDIR),
    ]
    recursive = False

    def doFile(self, file):
        self.error("%s is non-directory file in mandir", file)


class BadInterpreterPaths(policy.Policy):
    """
    Interpreters must not use relative paths.  There should be no
    exceptions outside of %(thisdocdir)s.
    """
    invariantexceptions = [ '%(thisdocdir.literalRegex)s/', ]

    def doFile(self, path):
	d = self.macros['destdir']
	mode = os.lstat(util.joinPaths(d, path))[stat.ST_MODE]
	if not mode & 0111:
            # we care about interpreter paths only in executable scripts
            return
        m = self.recipe.magic[path]
	if m and m.name == 'script':
            interp = m.contents['interpreter']
            if not interp:
                self.error(
                    'missing interpreter in "%s", missing buildRequires?',
                    path)
            elif interp[0] != '/':
                self.error(
                    "illegal relative interpreter path %s in %s (%s)",
                    interp, path, m.contents['line'])


class BadFilenames(policy.Policy):
    """
    Filenames must not contain newlines because filenames are separated
    by newlines in several conary protocols.  No exceptions are allowed.
    """
    def test(self):
        assert(not self.exceptions)
        return True
    def doFile(self, path):
        if path.find('\n') != -1:
            self.error("path %s has illegal newline character", path)


class NonUTF8Filenames(policy.Policy):
    """
    Filenames should be UTF-8 encoded because that is the standard system
    encoding.
    """
    def doFile(self, path):
        try:
            path.decode('utf-8')
        except UnicodeDecodeError:
            self.error('path "%s" is not valid UTF-8', path)



class NonMultilibComponent(policy.Policy):
    """
    Python and Perl components should generally be under /usr/lib, unless
    they have binaries and are built on a 64-bit platform, in which case
    they should have no files under /usr/lib, so that both the 32-bit abd
    64-bit components can be installed at the same time (that is, they
    should have multilib support).
    """
    invariantsubtrees = [
        '%(libdir)s/',
        '%(prefix)s/lib/',
    ]
    invariantinclusions = [
        '.*/python[^/]*/site-packages/.*',
        '.*/perl[^/]*/vendor-perl/.*',
    ]
    invariantexceptions = [
        '%(debuglibdir)s/',
    ]

    def __init__(self, *args, **keywords):
        self.foundlib = {'python': False, 'perl': False}
        self.foundlib64 = {'python': False, 'perl': False}
        self.reported = {'python': False, 'perl': False}
        self.productMapRe = re.compile(
            '.*/(python|perl)[^/]*/(site-packages|vendor-perl)/.*')
	policy.Policy.__init__(self, *args, **keywords)

    def test(self):
	if self.macros.lib == 'lib':
	    # no need to do anything
	    return False
        return True

    def doFile(self, path):
        if not False in self.reported.values():
            return
        # we've already matched effectively the same regex, so should match...
        p = self.productMapRe.match(path).group(1)
        if self.reported[p]:
            return
        if self.currentsubtree == '%(libdir)s/':
            self.foundlib64[p] = path
        else:
            self.foundlib[p] = path
        if self.foundlib64[p] and self.foundlib[p] and not self.reported[p]:
            self.error(
                '%s packages may install in /usr/lib or /usr/lib64,'
                ' but not both: at least %s and %s both exist',
                p, self.foundlib[p], self.foundlib64[p])
            self.reported[p] = True


class NonMultilibDirectories(policy.Policy):
    """
    Troves for 32-bit platforms should not normally contain
    directories named "lib64".
    """
    invariantinclusions = [ ( '.*/lib64', stat.S_IFDIR ), ]

    def test(self):
	if self.macros.lib == 'lib64':
	    # no need to do anything
	    return False
        return True

    def doFile(self, path):
        self.error('path %s has illegal lib64 component on 32-bit platform',
                   path)



class ImproperlyShared(policy.Policy):
    """
    The C{%(datadir)s} directory (normally /usr/share) is intended for
    data that can be shared between architectures; therefore, no
    ELF files should be there.
    """
    invariantsubtrees = [ '/usr/share/' ]

    def doFile(self, file):
        m = self.recipe.magic[file]
	if m:
	    if m.name == "ELF":
                self.error(
                    "Architecture-specific file %s in shared data directory",
                    file)
	    if m.name == "ar":
                self.error("Possibly architecture-specific file %s in shared data directory", file)


class CheckSonames(policy.Policy):
    """
    Warns about various possible shared library packaging errors:
    C{r.CheckSonames(exceptions=I{filterexp})} for things like directories
    full of plugins.
    """
    invariantsubtrees = destdirpolicy.librarydirs
    invariantinclusions = [
	(r'..*\.so', None, stat.S_IFDIR),
    ]
    recursive = False

    def doFile(self, path):
	d = self.macros.destdir
	destlen = len(d)
	l = util.joinPaths(d, path)
	if not os.path.islink(l):
	    m = self.recipe.magic[path]
	    if m and m.name == 'ELF' and 'soname' in m.contents:
		if os.path.basename(path) == m.contents['soname']:
		    target = m.contents['soname']+'.something'
		else:
		    target = m.contents['soname']
                self.warn(
                    '%s is not a symlink but probably should be a link to %s',
                    path, target)
	    return

	# store initial contents
	sopath = util.joinPaths(os.path.dirname(l), os.readlink(l))
	so = util.normpath(sopath)
	# find final file
	while os.path.islink(l):
	    l = util.normpath(util.joinPaths(os.path.dirname(l),
					     os.readlink(l)))

	p = util.joinPaths(d, path)
	linkpath = l[destlen:]
	m = self.recipe.magic[linkpath]

	if m and m.name == 'ELF' and 'soname' in m.contents:
	    if so == linkpath:
                self.dbg('%s is final path, soname is %s;'
                    ' soname usually is symlink to specific implementation',
                    linkpath, m.contents['soname'])
	    soname = util.normpath(util.joinPaths(
			os.path.dirname(sopath), m.contents['soname']))
	    s = soname[destlen:]
	    try:
		os.stat(soname)
		if not os.path.islink(soname):
                    self.warn('%s has soname %s; therefore should be a symlink',
                              s, m.contents['soname'])
	    except:
                self.warn("%s implies %s, which does not exist --"
                          " use r.Ldconfig('%s')?",
                          path, s, os.path.dirname(path))


class RequireChkconfig(policy.Policy):
    """
    Require that all initscripts provide chkconfig information; the only
    exceptions should be core initscripts like reboot:
    C{r.RequireChkconfig(exceptions=I{filterexp})}
    """
    invariantsubtrees = [ '%(initdir)s' ]
    def doFile(self, path):
	d = self.macros.destdir
        fullpath = util.joinPaths(d, path)
	if not (os.path.isfile(fullpath) and util.isregular(fullpath)):
            return
        f = file(fullpath)
        lines = f.readlines()
        f.close()
        foundChkconfig = False
        for line in lines:
            if not line.startswith('#'):
                # chkconfig tag must come before any uncommented lines
                break
            if line.find('chkconfig:') != -1:
                foundChkconfig = True
                break
        if not foundChkconfig:
            self.error("initscript %s must contain chkconfig information before any uncommented lines", path)


class CheckDestDir(policy.Policy):
    """
    Look for the C{%(destdir)s} path in file paths and symlink contents;
    it should not be there.  Does not check the contents of files, though
    files also should not contain C{%(destdir)s}.
    """
    def doFile(self, file):
	d = self.macros.destdir
        b = self.macros.builddir

	if file.find(d) != -1:
            self.error('Path %s contains destdir %s', file, d)
	fullpath = d+file
	if os.path.islink(fullpath):
	    contents = os.readlink(fullpath)
	    if contents.find(d) != -1:
                self.error('Symlink %s contains destdir %s in contents %s',
                           file, d, contents)
	    if contents.find(b) != -1:
                self.error('Symlink %s contains builddir %s in contents %s',
                           file, b, contents)

        badRPATHS = (d, b, '/tmp', '/var/tmp')
        m = self.recipe.magic[file]
        if m and m.name == "ELF":
            rpaths = m.contents['RPATH'] or ''
            for rpath in rpaths.split(':'):
                for badRPATH in badRPATHS:
                    if rpath.startswith(badRPATH):
                        self.warn('file %s has illegal RPATH %s',
                                    file, rpath)
                        break

# now the packaging classes

class _filterSpec(policy.Policy):
    """
    Pure virtual base class from which C{ComponentSpec} and C{PackageSpec}
    are derived.
    """
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


class ComponentSpec(_filterSpec):
    """
    Determines which component each file is in:
    C{r.ComponentSpec(I{componentname}, I{filterexp}...)}
    or
    C{r.ComponentSpec(I{packagename:component}, I{filterexp}...)}

    This class includes the filter expressions that specify the default
    assignment of files to components.  The expressions are considered
    in the order in which they are evaluated in the recipe, and the
    first match wins.  After all the recipe-provided expressions are
    evaluated, the default expressions are evaluated.  If no expression
    matches, then the file is assigned to the catchall component, which
    is C{runtime} by default but can be changed with the C{catchall=}
    argument.
    """
    invariantFilters = (
        # These must never be overridden; keeping this separate allows for
        # r.ComponentSpec('runtime', '.*')
	('test',      ('%(testdir)s/')),
	('debuginfo', ('%(debugsrcdir)s/',
		       '%(debuglibdir)s/')),
    )
    baseFilters = (
        # development docs go in :devel
        ('devel',     ('%(mandir)s/man(2|3)/')),
	# note that gtk-doc is not well-named; it is a shared system, like info,
	# and is used by unassociated tools (devhelp).  This line needs to
        # come first because "lib" in these paths should not mean :lib
	('doc',       ('%(datadir)s/(gtk-doc|doc|man|info)/')),
	# automatic subpackage names and sets of regexps that define them
	# cannot be a dictionary because it is ordered; first match wins
	('runtime',   ('%(datadir)s/gnome/help/.*/C/')), # help menu stuff
        # python is potentially architecture-specific because of %(lib)
	('python',    ('/usr/(%(lib)s|lib)/python.*/site-packages/')),
        # perl is potentially architecture-specific because of %(lib)
	('perl',      ('/usr/(%(lib)s|lib)/perl.*/vendor_perl/')),
        # devellib is architecture-specific
        ('devellib',  (r'\.so',), stat.S_IFLNK),
	('devellib',  (r'\.a',
                       '%(libdir)s/pkgconfig/')),
        # devel is architecture-generic -- no %(lib)s/%(libdir)s
        ('devel',     (r'.*/include/.*\.h',
		       '%(includedir)s/',
		       '%(datadir)s/aclocal/',
		       '%(bindir)s/..*-config')),
	('locale',    ('%(datadir)s/locale/',
		       '%(datadir)s/gnome/help/.*/')),
	('emacs',     ('%(datadir)s/emacs/site-lisp/',)),
        # Anything else in /usr/share should be architecture-independent
        # data files (thus the "datadir" name)
        ('data',      ('%(datadir)s/',)),
        # Anything in {,/usr}/lib{,64} is architecture-specific
        ('lib',       (r'.*/(%(lib)s|lib)/')),
    )
    keywords = { 'catchall': 'runtime' }

    def __init__(self, *args, **keywords):
        """
        @keyword catchall: The component name which gets all otherwise
        unassigned files.  Default: C{runtime}
        """
        _filterSpec.__init__(self, *args, **keywords)

    def doProcess(self, recipe):
	compFilters = []
	self.macros = recipe.macros
	self.rootdir = self.rootdir % recipe.macros

        # The extras need to come before base in order to override decisions
        # in the base subfilters; invariants come first for those very few
        # specs that absolutely should not be overridden in recipes.
        for filteritem in itertools.chain(self.invariantFilters,
                                          self.extraFilters,
                                          self.baseFilters):
            main = ''
	    name = filteritem[0] % self.macros
            if ':' in name:
                main, name = name.split(':')
	    assert(name != 'source')
	    filterargs = self.filterExpression(filteritem[1:], name=name)
	    compFilters.append(filter.Filter(*filterargs))
            if main:
                # we've got a package as well as a component, pass it on
                recipe.PackageSpec(main, filteritem[1:])
	# by default, everything that hasn't matched a filter pattern yet
	# goes in the catchall component ('runtime' by default)
	compFilters.append(filter.Filter('.*', self.macros, name=self.catchall))

	# pass these down to PackageSpec for building the package
	recipe.PackageSpec(compFilters=compFilters)

class PackageSpec(_filterSpec):
    """
    Determines which package (and optionally also component) each file is in:
    C{r.PackageSpec(I{packagename}, I{filterexp}...)}
    """
    keywords = { 'compFilters': None }

    def __init__(self, *args, **keywords):
        """
        @keyword compFilters: reserved for C{ComponentSpec} to pass information
        needed by C{PackageSpec}.
        """
        _filterSpec.__init__(self, *args, **keywords)
        
    def updateArgs(self, *args, **keywords):
        # keep a list of packages filtered for in PackageSpec in the recipe
        if args:
            newTrove = args[0] % self.recipe.macros
            self.recipe.packages[newTrove] = True
        _filterSpec.updateArgs(self, *args, **keywords)

    def doProcess(self, recipe):
	pkgFilters = []
	self.macros = recipe.macros
	self.rootdir = self.rootdir % self.macros

	for (filteritem) in self.extraFilters:
	    name = filteritem[0] % self.macros
	    filterargs = self.filterExpression(filteritem[1:], name=name)
	    pkgFilters.append(filter.Filter(*filterargs))
	# by default, everything that hasn't matched a pattern in the
	# main package filter goes in the package named recipe.name
	pkgFilters.append(filter.Filter('.*', self.macros, name=recipe.name))

	# OK, all the filters exist, build an autopackage object that
	# knows about them
	recipe.autopkg = buildpackage.AutoBuildPackage(
	    pkgFilters, self.compFilters, recipe)

	# now walk the tree -- all policy classes after this require
	# that the initial tree is built
        recipe.autopkg.walk(self.macros['destdir'])

class InstallBucket(policy.Policy):
    """
    Stub for older recipes
    """
    def updateArgs(self, *args, **keywords):
        self.warn('Install buckets are deprecated')

    def test(self):
        return False

def _markConfig(policy, filename, fullpath):
    policy.dbg('config: %s', filename)
    f = file(fullpath)
    f.seek(0, 2)
    if f.tell():
	# file has contents
	f.seek(-1, 2)
	lastchar = f.read(1)
	f.close()
	if lastchar != '\n':
	    policy.error("config file %s missing trailing newline" %filename)
    f.close()
    policy.recipe.autopkg.pathMap[filename].flags.isConfig(True)

class EtcConfig(policy.Policy):
    """
    Mark all files below /etc as config files:
    C{r.EtcConfig(exceptions=I{filterexp})}
    """
    invariantsubtrees = [ '%(sysconfdir)s', '%(taghandlerdir)s']

    def doFile(self, file):
        m = self.recipe.magic[file]
	if m and m.name == "ELF":
	    # an ELF file cannot be a config file, some programs put
	    # ELF files under /etc (X, for example), and tag handlers
	    # can be ELF or shell scripts; we just want tag handlers
	    # to be config files if they are shell scripts.
	    # Just in case it was not intentional, warn...
            self.dbg('ELF file %s found in config directory', file)
	    return
	fullpath = ('%(destdir)s/'+file) %self.macros
	if os.path.isfile(fullpath) and util.isregular(fullpath):
	    _markConfig(self, file, fullpath)


class Config(policy.Policy):
    """
    Mark only explicit inclusions as config files:
    C{r.Config(I{filterexp})}
    """

    # change inclusions to default to none, instead of all files
    keywords = policy.Policy.keywords.copy()
    keywords['inclusions'] = []

    def doFile(self, filename):
	fullpath = self.macros.destdir + filename
	if os.path.isfile(fullpath) and util.isregular(fullpath):
	    _markConfig(self, filename, fullpath)


class InitialContents(policy.Policy):
    """
    Mark only explicit inclusions as initial contents files, which
    provide their contents only if the file does not yet exist:
    C{r.InitialContents(I{filterexp})}
    """

    # change inclusions to default to none, instead of all files
    keywords = policy.Policy.keywords.copy()
    keywords['inclusions'] = []

    def updateArgs(self, *args, **keywords):
	policy.Policy.updateArgs(self, *args, **keywords)
        self.recipe.EtcConfig(exceptions=args)

    def doFile(self, filename):
	fullpath = self.macros.destdir + filename
        recipe = self.recipe
	if os.path.isfile(fullpath) and util.isregular(fullpath):
            self.dbg(filename)
            f = recipe.autopkg.pathMap[filename]
            f.flags.isInitialContents(True)
            if f.flags.isConfig():
                self.error(
                    '%s is marked as both a configuration file and'
                    ' an initial contents file', filename)



class Transient(policy.Policy):
    """
    Mark files that have transient contents as such:
    C{r.Transient(I{filterexp})}
    
    Transient contents are contents that should be overwritten by a new
    version without question at update time; almost the opposite of
    configuration files.
    """
    invariantinclusions = [
	r'..*\.py(c|o)$',
        r'..*\.elc$',
    ]

    def doFile(self, filename):
	fullpath = self.macros.destdir + filename
	if os.path.isfile(fullpath) and util.isregular(fullpath):
            recipe = self.recipe
            f = recipe.autopkg.pathMap[filename]
            self.dbg(filename)
	    f.flags.isTransient(True)
            if f.flags.isConfig() or f.flags.isInitialContents():
                self.error(
                    '%s is marked as both a transient file and'
                    ' a configuration or initial contents file', filename)


class SharedLibrary(policy.Policy):
    """
    Mark system shared libaries as such so that ldconfig will be run:
    C{r.SharedLibrary(subtrees=I{path})} to mark a path as containing
    shared libraries; C{r.SharedLibrary(I{filterexp})} to mark a file.

    C{r.SharedLibrary} does B{not} walk entire directory trees.  Every
    directory that you want to add must be passed in using the
    C{subtrees} keyword.
    """
    invariantsubtrees = destdirpolicy.librarydirs
    invariantinclusions = [
	(r'..*\.so\..*', None, stat.S_IFDIR),
    ]
    recursive = False

    def updateArgs(self, *args, **keywords):
	policy.Policy.updateArgs(self, *args, **keywords)
        if 'subtrees' in keywords:
            # share with other policies that need to know about shlibs
            d = {'subtrees': keywords['subtrees']}
            self.recipe.ExecutableLibraries(**d)
            self.recipe.CheckSonames(**d)
            self.recipe.NormalizeLibrarySymlinks(**d)
            # Provides and Requires need a different limitation...
            d = {'sonameSubtrees': keywords['subtrees']}
            self.recipe.Provides(**d)
            self.recipe.Requires(**d)

    def doFile(self, filename):
	fullpath = self.macros.destdir + filename
	if os.path.isfile(fullpath) and util.isregular(fullpath):
	    m = self.recipe.magic[filename]
	    if m and m.name == 'ELF' and 'soname' in m.contents:
                self.dbg(filename)
		self.recipe.autopkg.pathMap[filename].tags.set("shlib")


class TagDescription(policy.Policy):
    """
    Mark tag description files as such so that conary handles them
    correctly.  By default, every file in %(tagdescriptiondir)s/
    is marked as a tag description file.  No file outside of
    %(tagdescriptiondir)s/ will be considered by this policy.
    """
    invariantsubtrees = [ '%(tagdescriptiondir)s/' ]

    def doFile(self, file):
	fullpath = self.macros.destdir + file
	if os.path.isfile(fullpath) and util.isregular(fullpath):
            self.dbg('conary tag file: %s', file)
	    self.recipe.autopkg.pathMap[file].tags.set("tagdescription")


class TagHandler(policy.Policy):
    """
    Mark tag handler files as such so that conary handles them
    correctly.  By default, every file in %(taghandlerdir)s/
    is marked as a tag handler file.  No file outside of
    %(taghandlerdir)s/ will be considered by this policy.
    """
    invariantsubtrees = [ '%(taghandlerdir)s/' ]

    def doFile(self, file):
	fullpath = self.macros.destdir + file
	if os.path.isfile(fullpath) and util.isregular(fullpath):
            self.dbg('conary tag handler: %s', file)
	    self.recipe.autopkg.pathMap[file].tags.set("taghandler")


class _addInfo(policy.Policy):
    """
    Pure virtual class for policies that add information such as tags,
    requirements, and provision, to files.
    """
    keywords = {
	'included': {},
	'excluded': {}
    }

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


class TagSpec(_addInfo):
    """
    Apply tags defined by tag descriptions in both the current system
    and C{%(destdir)s} to all the files in C{%(destdir)s}; can also
    be told to apply tags manually:
    C{r.TagSpec(I{tagname}, I{filterexp})} to add manually, or
    C{r.TagSpec(I{tagname}, exceptions=I{filterexp})} to set an exception
    """
    def doProcess(self, recipe):
	self.tagList = []
	# read the system and %(destdir)s tag databases
	for directory in (recipe.macros.destdir+'/etc/conary/tags/',
			  '/etc/conary/tags/'):
	    if os.path.isdir(directory):
		for filename in os.listdir(directory):
		    path = util.joinPaths(directory, filename)
		    self.tagList.append(tags.TagFile(path, recipe.macros, True))
        self.db = database.Database(self.recipe.cfg.root, self.recipe.cfg.dbPath)
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
            self.dbg('%s: %s', name, path)
            tags.set(tag)
            if tagFile:
                for trove in self.db.iterTrovesByPath(tagFile.tagFile):
                    troveName = trove.getName()
                    if troveName not in self.recipe.buildRequires:
                        # XXX should be error, change after bootstrap
                        self.warn("%s assigned by %s to file %s, so add '%s'"
                                   ' to buildRequires or call r.TagSpec()'
                                   %(tag, tagFile.tagFile, path, troveName))

    def runInfo(self, path):
        for tag in self.included:
	    for filt in self.included[tag]:
		if filt.match(path):
                    isExcluded = False
                    if tag in self.excluded:
		        for filt in self.excluded[tag]:
                            if filt.match(path):
                                self.dbg('ignoring tag match for %s: %s',
                                         tag, path)
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
                            self.dbg('ignoring tag match for %s: %s',
                                     name, path)
                            isExcluded = True
			    break
                if not isExcluded:
		    self.markTag(name, tag.tag, path, tag)


class ParseManifest(policy.Policy):
    """
    Parses a file containing a manifest intended for RPM:
    C{r.ParseManifest(I{filename})}
    
    In the manifest, it finds the information that can't be represented by
    pure filesystem status with a non-root built: device files (C{%dev})
    and permissions (C{%attr}); it ignores directory ownership (C{%dir})
    because Conary handled directories very differently from RPM,
    and C{%defattr} because Conary's default ownership is root:root
    and because permissions (except for setuid and setgid files) are
    collected from the filesystem.  It translates each manifest line
    which it handles into the related Conary construct.

    Warning: tested only with MAKEDEV output so far.
    """

    def __init__(self, *args, **keywords):
	self.paths = []
	policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
	"""
	ParseManifest(path(s)...)
	"""
	if args:
	    self.paths.extend(args)
	policy.Policy.updateArgs(self, **keywords)

    def do(self):
	for path in self.paths:
	    self.processPath(path)

    def processPath(self, path):
	if not path.startswith('/'):
	    path = self.macros['builddir'] + os.sep + path
        f = open(path)
        for line in f:
            line = line.strip()
            fields = line.split(')')

            attr = fields[0].lstrip('%attr(').split(',')
            perms = attr[0].strip()
            owner = attr[1].strip()
            group = attr[2].strip()

            fields[1] = fields[1].strip()
            if fields[1].startswith('%dev('):
                dev = fields[1][5:].split(',')
                devtype = dev[0]
                major = dev[1]
                minor = dev[2]
                target = fields[2].strip()
                self.recipe.MakeDevices(target, devtype, int(major), int(minor),
                                        owner, group, int(perms, 0))
            elif fields[1].startswith('%dir '):
		pass
		# ignore -- Conary directory handling is too different
		# to map
            else:
		# XXX is this right?
                target = fields[1].strip()
		if int(perms, 0) & 06000:
		    self.recipe.AddModes(int(perms, 0),
                                         util.literalRegex(target))
		if owner != 'root' or group != 'root':
		    self.recipe.Ownership(owner, group,
                                          util.literalRegex(target))


class MakeDevices(policy.Policy):
    """
    Makes device nodes:
    C{r.MakeDevices(I{path}, I{type}, I{major}, I{minor}, I{owner}, I{group}, I{mode}=0400)}, where C{I{type}} is C{b} or C{c}.

    These nodes are only in the package, not in the filesystem, in order
    to enable Conary's policy of non-root builds (only root can actually
    create device nodes).
    """
    def __init__(self, *args, **keywords):
	self.devices = []
	policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
	"""
	MakeDevices(path, devtype, major, minor, owner, group, mode=0400)
	"""
	if args:
            args = list(args)
            if 'mode' in keywords:
                args.append(keywords.pop('mode'))
	    l = len(args)
	    # mode is optional, all other arguments must be there
	    assert((l > 5) and (l < 8))
	    if l == 6:
		args.append(0400)
	    self.devices.append(args)
	policy.Policy.updateArgs(self, **keywords)

    def do(self):
        for device in self.devices:
            r = self.recipe
            r.autopkg.addDevice(*device)
            filename = device[0]
            owner = device[4]
            group = device[5]
            r.Ownership(owner, group, filename)


class DanglingSymlinks(policy.Policy):
    # This policy must run after all modifications to the packaging
    # are complete because it counts on self.recipe.autopkg.pathMap
    # being final
    """
    Disallow dangling symbolic links (symbolic links which point to
    files which do not exist):
    C{DanglingSymlinks(exceptions=I{filterexp})} for intentionally
    dangling symlinks.
    
    If you know that a dangling symbolic link created by your package
    is fulfilled by another package on which your package depends,
    you may set up an exception for that file.
    """
    invariantexceptions = (
	'%(testdir)s/.*', )
    targetexceptions = [
        # ('filterexp', 'requirement')
	('.*consolehelper', 'usermode:runtime'),
	('/proc(/.*)?', None), # provided by the kernel, no package
    ]
    def doProcess(self, recipe):
	self.rootdir = self.rootdir % recipe.macros
	self.targetFilters = []
	self.macros = recipe.macros # for filterExpression
	for targetitem, requirement in self.targetexceptions:
	    filterargs = self.filterExpression(targetitem)
	    self.targetFilters.append((filter.Filter(*filterargs), requirement))
	policy.Policy.doProcess(self, recipe)

    def doFile(self, path):
	d = self.macros.destdir
	f = util.joinPaths(d, path)
        recipe = self.recipe
	if os.path.islink(f):
	    contents = os.readlink(f)
	    if contents[0] == '/':
                self.warn('Absolute symlink %s points to %s,'
                          ' should probably be relative', path, contents)
		return
	    abscontents = util.joinPaths(os.path.dirname(path), contents)
	    if abscontents in recipe.autopkg.pathMap:
		componentMap = recipe.autopkg.componentMap
		if componentMap[abscontents] != componentMap[path] and \
		   not path.endswith('.so') and \
		   not componentMap[path].getName().endswith(':test'):
		    # warn about suspicious cross-component symlink
                    self.warn('symlink %s points from package %s to %s',
                              path, componentMap[path].getName(),
                              componentMap[abscontents].getName())
	    else:
		for targetFilter, requirement in self.targetFilters:
		    if targetFilter.match(abscontents):
			# contents are an exception
                        self.dbg('allowing special dangling symlink %s -> %s',
                                 path, contents)
                        if requirement:
                            self.dbg('automatically adding requirement'
                                     ' %s for symlink %s', requirement, path)
                            recipe.Requires(requirement,
                                            util.literalRegex(path))
			return
		self.error(
		    "Dangling symlink: %s points to non-existant %s (%s)"
		    %(path, contents, abscontents))


class AddModes(policy.Policy):
    """
    Do not call from recipes; this is used internally by C{r.SetModes}
    and C{r.ParseManifest}
    """
    def __init__(self, *args, **keywords):
	self.fixmodes = {}
	policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
	"""
	AddModes(mode, path(s)...)
	"""
	if args:
	    for path in args[1:]:
		self.fixmodes[path] = args[0]
	policy.Policy.updateArgs(self, **keywords)

    def doFile(self, path):
	if path in self.fixmodes:
	    mode = self.fixmodes[path]
	    # set explicitly, do not warn
	    self.recipe.WarnWriteable(
                exceptions=util.literalRegex(path.replace('%', '%%')))
            self.dbg('suid/sgid: %s mode 0%o', path, mode & 07777)
	    self.recipe.autopkg.pathMap[path].inode.perms.set(mode)


class WarnWriteable(policy.Policy):
    """
    Warns about unexpectedly group- or other-writeable files; rather
    than set exceptions to this policy, use C{r.SetModes} so that the
    open permissions are explicitly and expected.
    """
    # Needs to run after AddModes because AddModes sets exceptions
    def doFile(self, file):
	fullpath = self.macros.destdir + file
	if os.path.islink(fullpath):
	    return
	if file not in self.recipe.autopkg.pathMap:
	    # directory has been deleted
	    return
	mode = os.lstat(fullpath)[stat.ST_MODE]
	if mode & 022:
	    if stat.S_ISDIR(mode):
		type = "directory"
	    else:
		type = "file"
            self.warn('Possibly inappropriately writeable permission'
                      ' 0%o for %s %s', mode & 0777, type, file)


class WorldWriteableExecutables(policy.Policy):
    """
    No executable file should ever be world-writeable.  If you have an
    exception, you can use:
    C{r.NonBinariesInBindirs(exceptions=I{filterexp})}
    But you should never have an exception.  Note that this policy is
    separate from C{WarnWriteable} because calling C{r.SetModes} should
    not override this policy automatically.
    """
    invariantexceptions = [ ('.*', stat.S_IFDIR) ]
    def doFile(self, file):
	d = self.macros['destdir']
	mode = os.lstat(util.joinPaths(d, file))[stat.ST_MODE]
        if mode & 0111 and mode & 02 and not stat.S_ISLNK(mode):
            self.error(
                "%s has mode 0%o with world-writeable permission in bindir",
                file, mode)



class FilesForDirectories(policy.Policy):
    """
    Warn about files where we expect directories, commonly caused
    by bad C{r.Install()} invocations.  Does not honor exceptions!
    """
    # This list represents an attempt to pick the most likely directories
    # to make these mistakes with: directories potentially inhabited by
    # files from multiple packages, with reasonable possibility that they
    # will have files installed by hand rather than by a "make install".
    candidates = (
	'/bin',
	'/sbin',
	'/etc',
	'/etc/X11',
	'/etc/init.d',
	'/etc/sysconfig',
	'/etc/xinetd.d',
	'/lib',
	'/mnt',
	'/opt',
	'/usr',
	'/usr/bin',
	'/usr/sbin',
	'/usr/lib',
	'/usr/libexec',
	'/usr/include',
	'/usr/share',
	'/usr/share/info',
	'/usr/share/man',
	'/usr/share/man/man1',
	'/usr/share/man/man2',
	'/usr/share/man/man3',
	'/usr/share/man/man4',
	'/usr/share/man/man5',
	'/usr/share/man/man6',
	'/usr/share/man/man7',
	'/usr/share/man/man8',
	'/usr/share/man/man9',
	'/usr/share/man/mann',
	'/var/lib',
	'/var/spool',
    )
    def do(self):
	d = self.recipe.macros.destdir
	for path in self.candidates:
	    fullpath = util.joinPaths(d, path)
	    if os.path.exists(fullpath):
		if not os.path.isdir(fullpath):
                    # XXX only report error if directory is included in
                    # the package; if it is merely in the filesystem
                    # only log a warning.  Needs to follow ExcludeDirectories...
                    self.error(
                        'File %s should be a directory; bad r.Install()?', path)


class ObsoletePaths(policy.Policy):
    """
    Warn about paths that used to be considered correct, but now are
    obsolete.  Does not honor exceptions!
    """
    candidates = {
	'/usr/man': '/usr/share/man',
	'/usr/info': '/usr/share/info',
	'/usr/doc': '/usr/share/doc',
    }
    def do(self):
	d = self.recipe.macros.destdir
	for path in self.candidates.keys():
	    fullpath = util.joinPaths(d, path)
	    if os.path.exists(fullpath):
                # XXX only report error if directory is included in
                # the package; if it is merely in the filesystem
                # only log a warning.  Needs to follow ExcludeDirectories...
                self.error('Path %s should not exist, use %s instead',
                           path, self.candidates[path])


class IgnoredSetuid(policy.Policy):
    """
    Files/directories that are setuid/setgid in the filesystem
    but do not have that mode explicitly set in the recipe will
    be packaged without setuid/setgid bits set.  This might be
    a bug, so flag it with a warning.
    """
    def doFile(self, file):
	fullpath = self.macros.destdir + file
	mode = os.lstat(fullpath)[stat.ST_MODE]
	if mode & 06000 and \
	   not self.recipe.autopkg.pathMap[file].inode.perms() & 06000:
	    if stat.S_ISDIR(mode):
		type = "directory"
	    else:
		type = "file"
            self.warn('%s %s has unpackaged set{u,g}id mode 0%o in filesystem',
                      type, file, mode&06777)


class LinkType(policy.Policy):
    """
    Only regular, non-config files may have hardlinks; no exceptions.
    """
    def do(self):
        for component in self.recipe.autopkg.getComponents():
            for path in component.hardlinks:
                if self.recipe.autopkg.pathMap[path].flags.isConfig():
                    self.error("Config file %s has illegal hard links", path)
            for path in component.badhardlinks:
                self.error("Special file %s has illegal hard links", path)


class LinkCount(policy.Policy):
    """
    It is generally an error to have hardlinks across directories,
    except when the packager knows that there is no reasonable
    chance that they will be on separate filesystems; in those
    cases, pass in a list of regexps specifying directory names
    that are exceptions to this rule by calling
    C{r.LinkCount(exceptions=I{regexp}} or
    C{r.LinkCount(exceptions=[I{regexp}, I{regexp}])}
    """
    def __init__(self, *args, **keywords):
        policy.Policy.__init__(self, *args, **keywords)
        self.excepts = set()

    def updateArgs(self, *args, **keywords):
        if 'exceptions' in keywords:
            exceptions = keywords.pop('exceptions')
            if type(exceptions) is str:
                self.excepts.add(exceptions)
            elif type(exceptions) in (tuple, list):
                self.excepts.update(set(exceptions))
        # FIXME: we may want to have another keyword argument
        # that passes information down to the buildpackage
        # that causes link groups to be broken for some
        # directories but not others.  We need to research
        # first whether this is useful; it may not be.

    def do(self):
        filters = [filter.Filter(x, self.macros) for x in self.excepts]
        for component in self.recipe.autopkg.getComponents():
            for inode in component.linkGroups:
                # ensure all in same directory, except for directories
                # matching regexps that have been passed in
                dirSet = set(os.path.dirname(x) + '/'
                             for x in component.linkGroups[inode]
                             if not [y for y in filters if y.match(x)])
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



class User(policy.Policy):
    """
    Stub for older recipes
    """
    def updateArgs(self, *args, **keywords):
        self.warn('User policy is deprecated, create a separate UserInfoRecipe instead')

    def test(self):
        return False


class SupplementalGroup(policy.Policy):
    """
    Stub for older recipes
    """
    def updateArgs(self, *args, **keywords):
        self.warn('SupplementalGroup policy is deprecated, create a separate GroupInfoRecipe instead')

    def test(self):
        return False


class Group(policy.Policy):
    """
    Stub for older recipes
    """
    def updateArgs(self, *args, **keywords):
        self.warn('Group policy is deprecated, create a separate GroupInfoRecipe instead')

    def test(self):
        return False


class ExcludeDirectories(policy.Policy):
    """
    Causes directories to be excluded from the package by default; set
    exceptions to this policy with
    C{ExcludeDirectories(exceptions=I{filterexp})} and the directories
    matching the regular expression will be included in the package.

    There are only two reasons to package a directory: the directory needs
    permissions other than 0755, or it must exist even if it is empty.

    It should generally not be necessary to invoke this policy directly,
    because the most common reason to include a directory in a package
    is that it needs permissions other than 0755, so simply call
    C{r.SetMode(I{path(s)}, I{mode})} where C{I{mode}} is not C{0755},
    and the directory will automatically included.

    Packages do not need to explicitly include a directory just to ensure
    that there is a place to put a file; Conary will appropriately create
    the directory, and delete it later if the directory becomes empty.
    """
    invariantinclusions = [ ('.*', stat.S_IFDIR) ]

    def doFile(self, path):
	fullpath = self.recipe.macros.destdir + os.sep + path
	s = os.lstat(fullpath)
	mode = s[stat.ST_MODE]
	if mode & 0777 != 0755:
            self.dbg('excluding directory %s with mode %o', path, mode&0777)
	elif not os.listdir(fullpath):
            self.dbg('excluding empty directory %s', path)
	self.recipe.autopkg.delFile(path)


class ByDefault(policy.Policy):
    """
    Determines which components should be installed by default when
    the package is installed; set a component as not installed by
    default with C{r.ByDefault(exceptions=':I{comp}')} or
    C{r.ByDefault(exceptions='I{pkgname}:I{comp}')}.
    The default is that :test and :debuginfo packages are not installed
    by default.
    """
    # Must follow ExcludeDirectories as well as PackageSpec

    invariantexceptions = [':test', ':debuginfo']

    def doProcess(self, recipe):
        if not self.inclusions:
            self.inclusions = []
        if not self.exceptions:
            self.exceptions = []
        recipe.setByDefaultOn(frozenset(self.inclusions))
        recipe.setByDefaultOff(frozenset(self.exceptions +
                                         self.invariantexceptions))


class _UserGroup:
    """
    Abstract base class that implements marking owner/group dependencies.
    """
    # All classes that descend from _UserGroup must run before the
    # Requires policy, as they implicitly depend on it to set the
    # file requirements and union the requirements up to the package.
    def setUserGroupDep(self, path, info, depClass):
	componentMap = self.recipe.autopkg.componentMap
	if path not in componentMap:
	    return
	pkg = componentMap[path]
	f = pkg.getFile(path)
        if path not in pkg.requiresMap:
            # BuildPackage only fills in requiresMap for ELF files; we may
            # need to create a few more DependencySets.
            pkg.requiresMap[path] = deps.DependencySet()
        pkg.requiresMap[path].addDep(depClass, deps.Dependency(info, []))


class _BuildPackagePolicy(policy.Policy):
    """
    Abstract base class for policy that walks the buildpackage
    rather than the %(destdir)s
    """
    def do(self):
        pkg = self.recipe.autopkg
        for thispath in sorted(pkg.pathMap):
            if self._pathAllowed(thispath):
                self.doFile(thispath)


class Ownership(_BuildPackagePolicy, _UserGroup):
    """
    Sets user and group ownership of files when the default of
    root:root is not appropriate:
    C{r.Ownership(I{username}, I{groupname}, I{filterexp}...)}

    No exceptions to this policy are permitted.
    """
    def __init__(self, *args, **keywords):
	self.filespecs = []
        self.systemusers = ('root',)
        self.systemgroups = ('root',)
	policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
	"""
	call as::
	  Ownership(user, group, filespec(s)...)
	List them in order, most specific first, ending with most
	general; the filespecs will be matched in the order that
	you provide them.
	"""
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
	for (f, owner, group) in self.fileFilters:
	    if f.match(path):
		self._markOwnership(path, owner, group)
		return
	self._markOwnership(path, 'root', 'root')

    def _markOwnership(self, filename, owner, group):
	pkgfile = self.recipe.autopkg.pathMap[filename]
	if owner:
	    pkgfile.inode.owner.set(owner)
            if owner not in self.systemusers:
                self.setUserGroupDep(filename, owner, deps.UserInfoDependencies)
	if group:
	    pkgfile.inode.group.set(group)
            if group not in self.systemgroups:
                self.setUserGroupDep(filename, group, deps.GroupInfoDependencies)


class _Utilize(_BuildPackagePolicy, _UserGroup):
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
	if args:
	    for filespec in args[1:]:
		self.filespecs.append((filespec, args[0]))
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


class UtilizeUser(_Utilize):
    """
    Marks files as requiring a user definition to exist even though
    the file is not owned by that user:
    C{r.UtilizeUser(I{username}, I{filterexp}...)}
    This is particularily useful for daemons that are setuid root
    but change their user id to a user id with no filesystem permissions
    after they start.
    """
    def _markItem(self, path, user):
        self.info('user %s: %s' % (user, path))
        self.setUserGroupDep(path, user, deps.UserInfoDependencies)


class UtilizeGroup(_Utilize):
    """
    Marks files as requiring a user definition to exist even though
    the file is not owned by that user:
    C{r.UtilizeGroup(I{groupname}, I{filterexp}...)}
    This is particularily useful for daemons that are setuid root
    but change their group id to a group id with no filesystem permissions
    after they start.
    """
    def _markItem(self, path, group):
        self.info('group %s: %s' % (group, path))
        self.setUserGroupDep(path, group, deps.GroupInfoDependencies)


class ComponentRequires(policy.Policy):
    """
    Creates automatic intra-package inter-component dependencies, such
    as C{:lib} components depending on their corresponding C{:data}
    components.  Changes are passed in with dictionaries; general changes with:
    C{r.ComponentRequires({I{componentname}: I{requiringComponentSet}})}
    and top-level package-specific changes with:
    C{r.ComponentRequires({I{packagename}: {I{componentname}: I{requiringComponentSet}}})}
    (i.e.  C{r.ComponentRequires({'data': set(('lib',))})} means that in
    all top-level packages (normally just one), only C{:lib} requires
    C{:data}, whereas by default both C{:lib} and C{:runtime} require C{:data};
    and C{r.ComponentRequires({'foo': {'data': set(('lib',))}})} makes that
    same change, but only for the C{foo} package).  C{ComponentRequires} cannot
    require capability flags; use C{Requires} if you need to specify a
    requirement including a capability flag.
    """
    def __init__(self, *args, **keywords):
        self.depMap = {
            # component: components that require it if they both exist
            'data': frozenset(('lib', 'runtime', 'devellib')),
            'devellib': frozenset(('devel',)),
            'lib': frozenset(('devel', 'devellib', 'runtime')),
            # while config is not an automatic component, its meaning
            # is standardized
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
                        # Note: this does not add dependencies to files;
                        # we could iterate over all the files in the
                        # requiring trove and make them require the
                        # required trove, but that seems slow and useless.
                        # These dependencies really shouldn't be useful
                        # for fileset creation anyway.  And we do have
                        # other places where we attach information to troves
                        # without it bubbling up from files.
                        ds = deps.DependencySet()
                        depClass = deps.TroveDependencies
                        ds.addDep(depClass, deps.Dependency(reqName))
                        p = components[wantName]
                        p.requires.union(ds)


class ComponentProvides(policy.Policy):
    """
    Causes each trove to provide itself explicitly; optionally with
    capability flags provided by
    C{r.ComponentProvides(I{flags})} or
    C{r.ComponentProvides('I{pkgname}', I{flags})}
    where C{I{flags}} may be a single string, or a list, tuple, or set
    of strings.
    At this time, all packages and components have the union of all
    capability flags built from this recipe.  The second form may in
    the future be changed to apply capability flags only to the named
    package.  It is impossible to provide a capability flag for one
    component but not another within a single package.
    """
    # frozenset to make sure we do not modify class data
    flags = frozenset()

    def updateArgs(self, *args, **keywords):
        if len(args) == 2:
            #pkgname = args[0]
            flags = args[1]
        else:
            flags = args[0]
        if not isinstance(flags, (list, tuple, set)):
            flags=(flags,)
        self.flags = frozenset(flags) | self.flags

    def do(self):
        if self.flags:
            flags = [ (x % self.macros, deps.FLAG_SENSE_REQUIRED)
                      for x in self.flags ]
        else:
            flags = []
        for component in self.recipe.autopkg.components.values():
            component.provides.addDep(deps.TroveDependencies,
                deps.Dependency(component.name, flags))



def _getmonodis(macros, recipe, path):
    # For bootstrapping purposes, prefer the just-built version if
    # it exists
    if os.access('%(destdir)s/%(monodis)s' %macros, os.X_OK):
        return ('MONO_PATH=%(destdir)s%(prefix)s/lib'
                ' LD_LIBRARY_PATH=%(destdir)s%(libdir)s'
                ' %(destdir)s/%(monodis)s' %macros)
    elif os.access('%(monodis)s' %macros, os.X_OK):
        return '%(monodis)s' %macros
    else:
        recipe.warn('%s not available for dependency discovery'
                    ' for path %s' %(macros.monodis, path))
    return None


class Provides(_BuildPackagePolicy):
    """
    Drives provides mechanism: to avoid marking a file as providing things,
    such as for package-private plugin modules installed in system library
    directories:
    C{r.Provides(exceptions=I{filterexp})} or
    C{r.Provides(I{provision}, I{filterexp}...)}
    A C{I{provision}} may be a file, soname or an ABI; a C{I{provision}} that
    starts with 'file' is a file, one that starts with 'soname:' is a
    soname, and one that starts with 'abi:' is an ABI.  Other prefixes are
    reserved.  Note: use C{ComponentProvides} to add capability flags to
    components.
    """
    # must come before Requires because Requires depends on _ELFPathProvide
    # having been run
    invariantexceptions = (
	'%(docdir)s/',
    )
    monodisPath = None

    def __init__(self, *args, **keywords):
	self.provisions = []
        self.sonameSubtrees = set(destdirpolicy.librarydirs)
	policy.Policy.__init__(self, *args, **keywords)

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
        policy.Policy.updateArgs(self, **keywords)

    def preProcess(self):
	self.rootdir = self.rootdir % self.macros
	self.fileFilters = []
	for filespec, provision in self.provisions:
	    self.fileFilters.append(
		(filter.Filter(filespec, self.macros), provision % self.macros))
	del self.provisions
        self.legalCharsRE = re.compile('[.0-9A-Za-z_+-/]')

        # interpolate macros, using canonical path form with no trailing /
        self.sonameSubtrees = set(os.path.normpath(x % self.macros)
                                  for x in self.sonameSubtrees)

    def _ELFPathProvide(self, path, m, pkg):
        basedir = os.path.dirname(path)
        # thanks to _ELFNonProvide, we know that this is a reasonable library
        if basedir not in self.sonameSubtrees and path in pkg.providesMap:
            # path needs to be in the dependency, since the
            # provides is too broad otherwise, so add it.
            # We can only add characters from the path that are legal
            # in a dependency name
            basedir = ''.join(x for x in basedir if self.legalCharsRE.match(x))
            depSet = deps.DependencySet()
            for depClass, dep in pkg.providesMap[path].iterDeps():
                if depClass is deps.SonameDependencies:
                    oldname = dep.getName()[0]
                    elfclass, soname = oldname.split('/', 1)
                    if '/' not in soname:
                        # add it
                        name = '%s%s/%s' % (elfclass, basedir, soname)
                        newdep = deps.Dependency(name, dep.flags)
                        # tell Requires about this change
                        self.recipe.Requires(_privateDepMap=(oldname, newdep))
                        dep = newdep
                depSet.addDep(depClass, dep)
            pkg.providesMap[path] = depSet

    def _AddCILDeps(self, path, m, pkg, macros):
        if not m or m.name != 'CIL':
            return
        fullpath = macros.destdir + path
        if not self.monodisPath:
            self.monodisPath = _getmonodis(macros, self, path)
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
        if path not in pkg.providesMap:
            pkg.providesMap[path] = deps.DependencySet()
        pkg.providesMap[path].addDep(deps.CILDependencies,
                deps.Dependency(name, [(ver, deps.FLAG_SENSE_REQUIRED)]))

    def doFile(self, path):
	componentMap = self.recipe.autopkg.componentMap
	if path not in componentMap:
	    return
	pkg = componentMap[path]
	f = pkg.getFile(path)
        macros = self.recipe.macros
        m = None

        fullpath = macros.destdir + path

        if os.path.exists(fullpath):
            m = self.recipe.magic[path]
            mode = os.lstat(fullpath)[stat.ST_MODE]

        if os.path.exists(fullpath):
            if (m and m.name == 'ELF'
                and m.contents['provides']
                and m.contents['Type'] == elf.ET_EXEC):
                # unless specified manually, do not export dependencies
                # for ELF executables
                del pkg.providesMap[path]

        # Now add in the manual provisions, which may include sonames
        # that might need to have paths added
        for (filter, provision) in self.fileFilters:
            if filter.match(path):
                m = self._markProvides(path, fullpath, provision, pkg, m, f)

        if os.path.exists(fullpath):
            if m and m.name == 'ELF':
                self._ELFPathProvide(path, m, pkg)
            if m and m.name == 'CIL':
                self._AddCILDeps(path, m, pkg, macros)

        if path not in pkg.providesMap:
            return
        f.provides.set(pkg.providesMap[path])
        pkg.provides.union(f.provides())

        # Because paths can change, individual files do not provide their
        # paths.  However, within a trove, a file does provide its name.
        # Furthermore, non-regular files can be path dependency targets 
        # Therefore, we have to handle this case a bit differently.
        if f.flags.isPathDependencyTarget():
            pkg.provides.addDep(deps.FileDependencies, deps.Dependency(path))

    def _markProvides(self, path, fullpath, provision, pkg, m, f):
        if path not in pkg.providesMap:
            # BuildPackage only fills in providesMap for ELF files; we may
            # need to create a few more DependencySets.
            pkg.providesMap[path] = deps.DependencySet()

        if provision.startswith("file"):
            # can't actually specify what to provide, just that it provides...
            f.flags.isPathDependencyTarget(True)
            return m

        if provision.startswith("abi:"):
            abistring = provision[4:].strip()
            op = abistring.index('(')
            abi = abistring[:op]
            flags = abistring[op+1:-1].split()
            flags = [ (x, deps.FLAG_SENSE_REQUIRED) for x in flags ]
            pkg.providesMap[path].addDep(deps.AbiDependency,
                deps.Dependency(abi, flags))
            return m

        if provision.startswith("soname:"):
            if os.path.islink(fullpath):
                # allow symlinks to provide sonames if necessary and if
                # they point to real files; paths encoded in sonames might
                # require the symlink path
                # FIXME: this requires changes elsewhere to allow
                # symlinks to carry deps
                contents = os.readlink(fullpath)
                if contents.startswith('/'):
                    m = self.recipe.magic[os.path.normpath(contents)]
                else:
                    m = self.recipe.magic[os.path.normpath(
                                          os.path.dirname(path)+'/'+contents)]
            if m and m.name == 'ELF':
                # Only ELF files can provide sonames.
                # This is for libraries that don't really include a soname,
                # but programs linked against them require a soname
                main = provision[7:].strip()
                soflags = []
                if '(' in main:
                    # get list of arbitrary flags
                    main, rest = main.split('(')
                    soflags.extend(rest[:-1].split())
                main = ''.join(x for x in main if self.legalCharsRE.match(x))
                abi = m.contents['abi']
                soflags.extend(abi[1])
                flags = [ (x, deps.FLAG_SENSE_REQUIRED) for x in soflags ]
                # normpath removes extra / characters
                dep = deps.Dependency(
                    os.path.normpath('/'.join((abi[0], main))), flags)
                pkg.providesMap[path].addDep(deps.SonameDependencies, dep)
                if '/' in main:
                    # basename removes path, so that we can map
                    # requirements in Requires
                    plainname = '/'.join((abi[0], os.path.basename(main)))
                    self.recipe.Requires(_privateDepMap=(plainname, dep))
            return m


class Requires(_addInfo, _BuildPackagePolicy):
    # _addInfo must come first to use its updateArgs() member
    # _BuildPackagePolicy provides package-walking do() member
    """
    Drives requirement mechanism: to avoid adding requirements for a file,
    such as example shell scripts outside C{%(docdir)s},
    C{r.Requires(exceptions=I{filterexp})}
    and to add a requirement manually,
    C{r.Requires('I{foo}', I{filterexp})} where C{'I{foo}'} can be
    C{'I{/path/to/file}'} or C{'I{packagename}:I{component[}(I{FLAGS})I{]}'}
    (components are the only troves that can be required).

    For executables that are executed only through wrappers that use
    C{LD_LIBRARY_PATH} to find the libraries instead of embedding an
    C{RPATH} in the binary, you will need to provide a synthetic
    RPATH using the C{r.Requires(rpath=I{RPATH})} or
    C{r.Requires(rpath=(I{filterExp}, I{RPATH}))} calls, which are
    tested in the order provided.  The C{I{RPATH}} is a standard
    Unix-style path string containing one or more directory names,
    separated only by colon characters.

    Executables that use C{dlopen()} to open a shared library will
    not automatically have a dependency on that shared library.
    If the program unconditionally requires that it be able to
    C{dlopen()} the shared library, encode that requirement by
    manually creating the requirement by calling
    C{r.Requires('soname: I{libfoo.so}', 'I{filterexp}')} or
    C{r.Requires('soname: I{/path/to/libfoo.so}', 'I{filterexp}')}
    depending on whether the library is in a system library
    directory or not.  (It needs to be the same as how the
    soname dependency is expressed by the providing package.)
    """
    invariantexceptions = (
	'%(docdir)s/',
    )
    monodisPath = None

    def __init__(self, *args, **keywords):
        self.sonameSubtrees = set(destdirpolicy.librarydirs)
        self._privateDepMap = {}
        self.rpathFixup = []
        policy.Policy.__init__(self, *args, **keywords)

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
        rpath = keywords.pop('rpath', None)
        if rpath:
            if type(rpath) is str:
                rpath = ('.*', rpath)
            assert(type(rpath) == tuple)
            self.rpathFixup.append(rpath)
        _addInfo.updateArgs(self, *args, **keywords)

    def preProcess(self):
        macros = self.macros
        self.systemLibPaths = set(os.path.normpath(x % macros)
                                  for x in self.sonameSubtrees)
        # anything that any buildreqs have caused to go into ld.so.conf
        # is a system library by definition
        self.systemLibPaths |= set(os.path.normpath(x[:-1])
                                   for x in file('/etc/ld.so.conf').readlines())
        self.rpathFixup = [(filter.Filter(x, macros), y % macros)
                           for x, y in self.rpathFixup]

    def _ELFPathFixup(self, path, m, pkg):
        """
        Change requirements to have path in soname if appropriate
        """
        # common case is no likely modification, so make that fastest
        found = False
        rpathList = []
        for depClass, dep in pkg.requiresMap[path].iterDeps():
            if dep.getName()[0] in self._privateDepMap:
                found = True

        def _canonicalRPATH(rpath):
            # normalize all elements of RPATH
            l = [ os.path.normpath(x) for x in rpath.split(':') ]
            # prune system paths from RPATH
            l = [ x for x in l if x not in self.systemLibPaths ]
            return l

        # fixup should come first so that its path elements can override
        # the included RPATH if necessary
        if self.rpathFixup:
            for f, rpath in self.rpathFixup:
                if f.match(path):
                    rpathList = _canonicalRPATH(rpath)
                    break

        if m and 'RPATH' in m.contents and m.contents['RPATH']:
            rpathList += _canonicalRPATH(m.contents['RPATH'])

        if not found and not rpathList:
            return

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

        # found at least one potential replacement, do the real work
        depSet = deps.DependencySet()
        for depClass, dep in pkg.requiresMap[path].iterDeps():
            oldName = dep.getName()[0]
            if oldName in self._privateDepMap:
                # we assume that if it is both provided and required,
                # we fix up the requirement to match the provision
                provided = self._privateDepMap[oldName]
                if set(dep.flags.keys()).issubset(set(provided.flags.keys())):
                    # we need the new name, but we need to keep the old flags
                    dep = deps.Dependency(provided.getName()[0], dep.flags)
            elif depClass.tag == deps.DEP_CLASS_SONAME:
                elfClass, soname = oldName.split('/', 1)
                rpath = _findSonameInRpath(soname)
                if rpath:
                    if rpath.startswith('/'):
                        # we need it without the leading /
                        rpath = rpath[1:]
                    # change the name to follow the rpath
                    newName = '/'.join((elfClass, rpath, soname))
                    dep = deps.Dependency(newName, dep.flags)
            depSet.addDep(depClass, dep)
        pkg.requiresMap[path] = depSet

    def doFile(self, path):
	componentMap = self.recipe.autopkg.componentMap
	if path not in componentMap:
	    return
	pkg = componentMap[path]
	f = pkg.getFile(path)
        macros = self.recipe.macros
        m = self.recipe.magic[path]

        # we may have some dependencies that need converting
        if (m and m.name == 'ELF'
            and path in pkg.requiresMap):
            self._ELFPathFixup(path, m, pkg)

        # now go through explicit requirements
	for info in self.included:
	    for filt in self.included[info]:
		if filt.match(path):
                    self._markManualRequirement(info, path, pkg, m)

        # now check for automatic dependencies besides ELF
        if f.inode.perms() & 0111 and m and m.name == 'script':
            interp = m.contents['interpreter']
            if len(interp.strip()) and self._checkInclusion(interp, path):
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
                self._addRequirement(path, interp, [], pkg,
                                     deps.FileDependencies)

        if m and m.name == 'CIL':
            fullpath = macros.destdir + path
            if not self.monodisPath:
                self.monodisPath = _getmonodis(macros, self, path)
                if not self.monodisPath:
                    return
            p = util.popen('%s --assemblyref %s' %(
                           self.monodisPath, fullpath))
            for line in [ x.strip() for x in p.readlines() ]:
                if ': Version=' in line:
                    ver = line.split('=')[1]
                elif 'Name=' in line:
                    name = line.split('=')[1]
                    self._addRequirement(path, name, [ver], pkg,
                                         deps.CILDependencies)
            p.close()


        # finally, package the dependencies up
        if path not in pkg.requiresMap:
            return
        f.requires.set(pkg.requiresMap[path])
        pkg.requires.union(f.requires())
    
    def _markManualRequirement(self, info, path, pkg, m):
        flags = []
        if self._checkInclusion(info, path):
            if info[0] == '/':
                depClass = deps.FileDependencies
            if info.startswith('soname:'):
                if not m or m.name != 'ELF':
                    # only an ELF file can have a soname requirement
                    return
                # we need to synthesize a dependency that encodes the
                # same ABI as this binary
                depClass = deps.SonameDependencies
                for depType, dep, f in m.contents['requires']:
                    if depType == 'abi':
                        flags = f
                        info = '%s/%s' %(dep, info.split(None, 1)[1])
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
            self._addRequirement(path, info, flags, pkg, depClass)

    def _checkInclusion(self, info, path):
        if info in self.excluded:
            for filt in self.excluded[info]:
                # exception handling is per-requirement,
                # so handled specially
                if filt.match(path):
                    self.dbg('ignoring requirement match for %s: %s',
                             path, info)
                    return False
        return True

    def _addRequirement(self, path, info, flags, pkg, depClass):
        if depClass == deps.FileDependencies:
            pathMap = self.recipe.autopkg.pathMap
            if info in pathMap and info not in pkg.providesMap:
                # if a package requires a file, includes that file,
                # and does not provide that file, it should error out
                self.error('%s requires %s, which is included but not'
                           ' provided; use'
                           " r.Provides('file', '%s')", path, info, info)
                return
        if path not in pkg.requiresMap:
            # BuildPackage only fills in requiresMap for ELF files; we may
            # need to create a few more DependencySets.
            pkg.requiresMap[path] = deps.DependencySet()
        # in some cases, we get literal "(flags)" from the recipe
        if '(' in info:
            flagindex = info.index('(')
            flags = set(info[flagindex+1:-1].split() + list(flags))
            info = info.split('(')[0]
        if flags:
            flags = [ (x, deps.FLAG_SENSE_REQUIRED) for x in flags ]
        pkg.requiresMap[path].addDep(depClass, deps.Dependency(info, flags))


class Flavor(_BuildPackagePolicy):
    """
    Drives flavor mechanism: to avoid marking a file's flavor:
    C{r.Flavor(exceptions=I{filterexp})}
    """
    def doProcess(self, recipe):
	self.libRe = re.compile(
            '^(%(libdir)s'
            '|/%(lib)s'
            '|%(x11prefix)s/%(lib)s'
            '|%(krbprefix)s/%(lib)s)(/|$)' %recipe.macros)
	self.libReException = re.compile('^/usr/(lib|%(lib)s)/python.*$')

        self.baseIsnset = use.Arch.getCurrentArch()._name
        self.troveMarked = False
	policy.Policy.doProcess(self, recipe)

    def hasLib(self, path):
        return self.libRe.match(path) and not self.libReException.match(path)

    def doFile(self, path):
	componentMap = self.recipe.autopkg.componentMap
	if path not in componentMap:
	    return
	pkg = componentMap[path]
	f = pkg.getFile(path)
        if path in pkg.isnsetMap:
            isnset = pkg.isnsetMap[path]
        elif self.hasLib(path):
            # all possible paths in a %(lib)s-derived path get default
            # instruction set assigned if they don't have one already
            if f.hasContents:
                isnset = self.baseIsnset
            else:
                # this file can't be marked by arch, but the troves
                # and package must be
                if self.troveMarked:
                    return
                set = use.Arch.getCurrentArch()._toDependency()
                for pkg in componentMap.values():
                    pkg.flavor.union(set)
                self.troveMarked = True
                return
        else:
            return

	set = deps.DependencySet()
        set.addDep(deps.InstructionSetDependency, deps.Dependency(isnset, []))
        # get the Arch.* dependencies
        set.union(use.createFlavor(None, use.Arch._iterUsed()))
        f.flavor.set(set)
        # all troves need to share the same flavor so that we can
        # distinguish them later
        for pkg in componentMap.values():
            pkg.flavor.union(f.flavor())



class EnforceSonameBuildRequirements(policy.Policy):
    """
    Test to make sure that each requires dependency in the package
    is matched by a suitable element in the C{buildRequires} list;
    any trove names wrongly suggested can be eliminated from the
    list with C{r.EnforceSonameBuildRequirements(exceptions='I{pkg}:I{comp}')}.
    """
    def preProcess(self):
        self.compExceptions = set()
        if self.exceptions:
            for exception in self.exceptions:
                self.compExceptions.add(exception % self.recipe.macros)
        self.exceptions = None

    def do(self):
        missingBuildRequires = set()
        missingBuildRequiresChoices = []
        # right now we do not enforce branches.  This could be
        # done with more work.  There is no way I know of to
        # enforce flavors, so we just remove them from the spec.
        truncatedBuildRequires = set(
            self.recipe.buildReqMap[spec].getName()
            for spec in self.recipe.buildRequires
            if spec in self.recipe.buildReqMap)

	components = self.recipe.autopkg.components
        pathMap = self.recipe.autopkg.pathMap
        pathReqMap = {}

        reqDepSet = deps.DependencySet()
        provDepSet = deps.DependencySet()
        for pkg in components.values():
            reqDepSet.union(pkg.requires)
            provDepSet.union(pkg.provides)
        depSet = deps.DependencySet()
        depSet.union(reqDepSet - provDepSet)

        sonameDeps = depSet.getDepClasses().get(deps.DEP_CLASS_SONAME, None)
        if not sonameDeps:
            return

        depSetList = [ ]
        for dep in sonameDeps.getDeps():
            depSet = deps.DependencySet()
            depSet.addDep(deps.SonameDependencies, dep)
            depSetList.append(depSet)

        db = database.Database(self.recipe.cfg.root, self.recipe.cfg.dbPath)
        localProvides = db.getTrovesWithProvides(depSetList)

        def providesNames(libname):
            # Instead of requiring the :lib component that satisfies
            # the dependency, our first choice, if possible, is to
            # require :devel, because that would include header files;
            # if it does not exist, then :devellib for a soname link;
            # finally if neither of those exists, then :lib (though
            # that is a degenerate case).
            return [name.replace(':lib', ':devel'),
                    name.replace(':lib', ':devellib'),
                    name]
            
        for dep in localProvides:
            provideNameList = [x[0] for x in localProvides[dep]]
            # normally, there is only one name in provideNameList

            foundCandidates = set()
            for name in provideNameList:
                for candidate in providesNames(name):
                    if db.hasTroveByName(candidate):
                        foundCandidates.add(candidate)
                        break
            foundCandidates -= self.compExceptions

            missingCandidates = foundCandidates - truncatedBuildRequires
            if missingCandidates == foundCandidates:
                # None of the troves that provides this requirement is
                # reflected in the buildRequires list.  Add candidates
                # to proper list to print at the end:
                if len(foundCandidates) > 1:
                    found = False
                    for candidateSet in missingBuildRequiresChoices:
                        if candidateSet == foundCandidates:
                            found = True
                    if found == False:
                        missingBuildRequiresChoices.append(foundCandidates)
                else:
                    missingBuildRequires.update(foundCandidates)

                # Now give lots of specific information to help the packager
                # in case things do not look so obvious...
                pathList = []
                for path in pathMap:
                    pkgfile = pathMap[path]
                    if pkgfile.hasContents and (pkgfile.requires() & dep):
                        pathList.append(path)
                        l = pathReqMap.setdefault(path, [])
                        l.append(dep)
                if pathList:
                    self.warn('buildRequires %s needed to satisfy "%s"'
                              ' for files: %s',
                              str(sorted(list(foundCandidates))),
                              str(dep),
                              ', '.join(sorted(pathList)))

        if pathReqMap:
            for path in pathReqMap:
                self.warn('file %s has unsatisfied build requirements "%s"',
                          path, '", "'.join([
                             str(x) for x in
                               sorted(list(set(pathReqMap[path])))]))

        if missingBuildRequires:
            self.warn('add to buildRequires: %s',
                       str(sorted(list(set(missingBuildRequires)))))
            # one special case:
            if list(missingBuildRequires) == [ 'glibc:devel' ]:
                self.warn('consider CPackageRecipe or AutoPackageRecipe')
        if missingBuildRequiresChoices:
            for candidateSet in missingBuildRequiresChoices:
                self.warn('add to buildRequires one of: %s',
                           str(sorted(list(candidateSet))))


class EnforceConfigLogBuildRequirements(policy.Policy):
    """
    This class looks through the builddir for config.log files, and looks
    in them for mention of files that configure found on the system, and
    makes sure that the components that contain them are listed as
    build requirements; pass exceptions in with
    C{r.EnforceConfigLogBuildRequirements(exceptions='I{/path/to/file/found}'}
    or with
    C{r.EnforceConfigLogBuildRequirements(exceptions='I{pkg}:I{comp}')}.
    """
    rootdir = '%(builddir)s'
    invariantinclusions = [ (r'.*/config\.log', 0400, stat.S_IFDIR), ]
    # list of regular expressions (using macros) that cause an
    # entry to be ignored unless a related strings is found in
    # another named file (empty tuple is unconditional blacklist)
    greylist = [
        # config.log string, ((filename, regexp), ...)
        ('%(prefix)s/X11R6/bin/makedepend', ()),
        ('%(bindir)s/g77',
            (('configure.ac', r'\s*AC_PROG_F77'),
             ('configure.in', r'\s*AC_PROG_F77'))),
        ('%(bindir)s/bison',
            (('configure.ac', r'\s*AC_PROC_YACC'),
             ('configure.in', r'\s*(AC_PROG_YACC|YACC=)'))),
    ]

    def test(self):
        return not self.recipe.ignoreDeps

    def preProcess(self):
        self.foundRe = re.compile('^[^ ]+: found (/([^ ]+)?bin/[^ ]+)\n$')
        self.foundPaths = set()
        self.greydict = {}
        # turn list into dictionary, interpolate macros, and compile regexps
        for greyTup in self.greylist:
            self.greydict[greyTup[0] % self.macros] = (
                (x, re.compile(y % self.macros)) for x, y in greyTup[1])
        # process exceptions differently; user can specify either the
        # source (found path) or destination (found component) to ignore
        self.pathExceptions = set()
        self.compExceptions = set()
        if self.exceptions:
            for exception in self.exceptions:
                exception = exception % self.recipe.macros
                if '/' in exception:
                    self.pathExceptions.add(exception)
                else:
                    self.compExceptions.add(exception)
        # never suggest a recursive buildRequires
        self.compExceptions.update(set(self.recipe.autopkg.components.keys()))
        self.exceptions = None

    def foundPath(self, line):
        match = self.foundRe.match(line)
        if match:
            return match.group(1)
        return False

    def doFile(self, path):
        fullpath = self.macros.builddir + path
        # iterator to avoid reading in the whole file at once;
        # nested iterators to avoid matching regexp twice
        foundPaths = set(path for path in 
           (self.foundPath(line) for line in file(fullpath))
           if path and path not in self.pathExceptions)

        # now remove false positives using the greylist
        # copy() for copy because modified
        for foundPath in foundPaths.copy():
            if foundPath in self.greydict:
                foundMatch = False
                for otherFile, testRe in self.greydict[foundPath]:
                    otherFile = fullpath.replace('config.log', otherFile)
                    if not foundMatch and os.path.exists(otherFile):
                        otherFile = file(otherFile)
                        if [line for line in otherFile if testRe.match(line)]:
                            foundMatch = True
                if not foundMatch:
                    # greylist entry has no match, so this is a false
                    # positive and needs to be removed from the set
                    foundPaths.remove(foundPath)
        self.foundPaths.update(foundPaths)

    def postProcess(self):
        # first, get all the trove names in the transitive buildRequires
        # runtime dependency closure
        db = database.Database(self.recipe.cfg.root, self.recipe.cfg.dbPath)
        transitiveBuildRequires = set(
            self.recipe.buildReqMap[spec].getName()
            for spec in self.recipe.buildRequires)
        depSetList = [ self.recipe.buildReqMap[spec].getRequires()
                       for spec in self.recipe.buildRequires ]
        d = db.getTransitiveProvidesClosure(depSetList)
        for depSet in d:
            transitiveBuildRequires.update(set(tup[0] for tup in d[depSet]))

        # next, for each file found, report if it is not in the
        # transitive closure of runtime requirements of buildRequires
        fileReqs = set()
        for path in sorted(self.foundPaths):
            thisFileReqs = set(trove.getName()
                               for trove in db.iterTrovesByPath(path))
            thisFileReqs -= self.compExceptions
            missingReqs = thisFileReqs - transitiveBuildRequires
            if missingReqs:
                self.warn('path %s suggests buildRequires: %s',
                          path, ', '.join((sorted(list(missingReqs)))))
            fileReqs.update(thisFileReqs)

        # finally, give the coalesced suggestion for cut and paste
        # into the recipe if all the individual messages make sense
        missingReqs = fileReqs - transitiveBuildRequires
        if missingReqs:
            self.warn('Probably add to buildRequires: %s',
                      str(sorted(list(missingReqs))))


class reportErrors(policy.Policy):
    """
    This class is used to pull together all package errors in the
    sanity-checking rules that come above it; do not call it
    directly; it is for internal use only.
    """
    # Must come after all the other package classes that report
    # fatal errors, so might as well come last.
    def __init__(self, *args, **keywords):
	self.warnings = []
	policy.Policy.__init__(self, *args, **keywords)
    def updateArgs(self, *args, **keywords):
	"""
	Called once, with printf-style arguments, for each warning.
	"""
	self.warnings.append(args[0] %tuple(args[1:]))
    def do(self):
	if self.warnings:
	    for warning in self.warnings:
		log.error(warning)
	    raise PackagePolicyError, 'Package Policy errors found:\n%s' %"\n".join(self.warnings)



def DefaultPolicy(recipe):
    """
    Return a list of actions that expresses the default policy.
    """
    return [
	NonBinariesInBindirs(recipe),
	FilesInMandir(recipe),
        BadInterpreterPaths(recipe),
        BadFilenames(recipe),
        NonUTF8Filenames(recipe),
        NonMultilibComponent(recipe),
        NonMultilibDirectories(recipe),
	ImproperlyShared(recipe),
	CheckSonames(recipe),
        RequireChkconfig(recipe),
	CheckDestDir(recipe),
	ComponentSpec(recipe),
	PackageSpec(recipe),
        InstallBucket(recipe),
	EtcConfig(recipe),
	Config(recipe),
	InitialContents(recipe),
	Transient(recipe),
	SharedLibrary(recipe),
	TagDescription(recipe),
	TagHandler(recipe),
	TagSpec(recipe),
	ParseManifest(recipe),
	MakeDevices(recipe),
	DanglingSymlinks(recipe),
	AddModes(recipe),
	WarnWriteable(recipe),
        WorldWriteableExecutables(recipe),
	FilesForDirectories(recipe),
	ObsoletePaths(recipe),
	IgnoredSetuid(recipe),
	LinkType(recipe),
	LinkCount(recipe),
        User(recipe),
        SupplementalGroup(recipe),
        Group(recipe),
	ExcludeDirectories(recipe),
        ByDefault(recipe),
	Ownership(recipe),
        UtilizeUser(recipe),
        UtilizeGroup(recipe),
	ComponentRequires(recipe),
        ComponentProvides(recipe),
	Provides(recipe),
	Requires(recipe),
	Flavor(recipe),
        EnforceSonameBuildRequirements(recipe),
        EnforceConfigLogBuildRequirements(recipe),
	reportErrors(recipe),
    ]


class PackagePolicyError(policy.PolicyError):
    pass
