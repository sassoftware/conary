#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import util
import magic
import os
import policy
import log
import stat
import buildpackage
import filter

"""
Module used by recipes to effect packaging policy; things like setting
hints, flags, and dependencies.
Classes from this module are not used directly; instead, they are used
through eponymous interfaces in recipe.
"""

# bail out as soon as possible if there's an unrecoverable error
# so put error classes first

class NonBinariesInBindirs(policy.Policy):
    """
    Directories that are specifically for binaries should have only
    files that have some executable bit set.
    """
    invariantexceptions = [ ('.*', stat.S_IFDIR) ]
    invariantsubtrees = [
	'%(bindir)s/',
	'%(essentialbindir)s/',
	'%(sbindir)s/',
	'%(essentialsbindir)s/',
	'%(initdir)s/',
	'%(libexecdir)s/',
	'%(sysconfdir)s/profile.d/',
    ]

    def doFile(self, file):
	mode = os.lstat(self.macros['destdir'] + os.sep + file)[stat.ST_MODE]
	if not mode & 0111:
	    raise PackagePolicyError(
		"%s has mode 0%o with no executable permission in bindir"
		%(file, mode))


class FilesInMandir(policy.Policy):
    """
    The C{%(mandir)s} directory should only have files in it, normally.
    The main cause of files in C{%(mandir)s} is confusion in packages
    about whether "mandir" means /usr/share/man or /usr/share/man/man<n>.
    """
    invariantinclusions = [ ('%(mandir)s/[^/][^/]*$', None, stat.S_IFDIR) ]

    def doFile(self, file):
	raise PackagePolicyError("%s is non-directory file in mandir" %file)


class ImproperlyShared(policy.Policy):
    """
    The %(datadir)s directory (normally /usr/share) is intended for
    data that can be shared between architectures; therefore, no
    ELF files should be there.
    """
    invariantsubtrees = [ '/usr/share/' ]

    def doFile(self, file):
	destdir = self.macros['destdir']
        m = magic.magic(file, destdir)
	if m and m.name == "ELF":
	    raise PackagePolicyError(
		"Architecture-specific file %s in shared data directory" %file)


# now the packaging classes

class _filterSpec(policy.Policy):
    def __init__(self, *args, **keywords):
	self.extraFilters = []
	policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
	"""
	ThisClass('<name>', 'regex1', 'regex2', [setmodes=stat.??] [unsetmodes=stat.???])
	"""
	if args:
	    # pull setmodes and unsetmodes out of **keywords
	    setmodes = keywords.pop('setmodes', None)
	    unsetmodes = keywords.pop('unsetmodes', None)
	    self.extraFilters.append((args[0], args[1:], setmodes, unsetmodes))
	policy.Policy.updateArgs(self, [], **keywords)


class ComponentSpec(_filterSpec):
    """
    Determines which component each file is in.
    """
    baseFilters = (
	# automatic subpackage names and sets of regexps that define them
	# cannot be a dictionary because it is ordered; first match wins
	('python',    ('%(libdir)s/python.*/site-packages/')),
	('runtime',   ('%(essentiallibdir)s/security/',
		       '/lib/security/')),
	('devel',     ('\.so',), stat.S_IFLNK),
	('devel',     ('\.a',
		       '.*/include/.*\.h',
		       '%(includedir)s/',
		       '%(mandir)s/man(2|3)/',
		       '%(datadir)s/aclocal/',
		       '%(libdir)s/pkgconfig/')),
	('lib',       ('.*/lib/.*\.so.*')),
	# note that gtk-doc is not well-named; it is a shared system, like info,
	# and is used by unassociated tools (devhelp)
	('doc',       ('%(datadir)s/(gtk-doc|doc|man|info)/')),
	('locale',    ('%(datadir)s/locale/')),
	('emacs',     ('%(datadir)s/emacs/site-lisp/.*',)),
	('runtime',   ('.*',)),
    )

    def doProcess(self, recipe):
	compFilters = []
	self.macros = recipe.macros

	# the extras need to come first in order to override decisions
	# in the base subfilters
	for (filteritem) in self.extraFilters + list(self.baseFilters):
	    name = filteritem[0] % self.macros
	    assert(name != 'sources')
	    filterargs = self.filterExpression(filteritem[1:], name=name)
	    compFilters.append(filter.Filter(*filterargs))

	# pass these down to PackageSpec for building the package
	recipe.PackageSpec(compFilters=compFilters)

class PackageSpec(_filterSpec):
    """
    FIXME
    """
    keywords = { 'compFilters': None }

    def __init__(self, *args, **keywords):
        """
        @keyword compFilters: reserved for C{ComponentSpec} to pass information
        needed by C{PackageSpec}.
        """
        _filterSpec.__init__(self, *args, **keywords)
        

    def doProcess(self, recipe):
	pkgFilters = []
	macros = recipe.macros

	for (filteritem) in self.extraFilters:
	    filteritem = list(filteritem)
	    while len(filteritem) < 4:
		filteritem.append(None)
	    name, patterns, setmode, unsetmode = filteritem
	    pkgFilters.append(
		filter.Filter(patterns, macros,
			      setmode=setmode, unsetmode=unsetmode,
			      name=name %macros))
	# by default, everything that hasn't matched a pattern in the
	# main package filter goes in the package named recipe.name
	pkgFilters.append(filter.Filter('.*', macros, name=recipe.name))

	# OK, all the filters exist, build an autopackage object that
	# knows about them
	recipe.autopkg = buildpackage.AutoBuildPackage(
	    recipe.fullVersion,
	    pkgFilters, self.compFilters)

	# now walk the tree -- all policy classes after this require
	# that the initial tree is built
        recipe.autopkg.walk(macros['destdir'])


def _markConfig(recipe, filename):
    log.debug('config: %s', filename)
    recipe.autopkg.pathMap[filename].flags.isConfig(True)

class EtcConfig(policy.Policy):
    """
    Mark all files below /etc as config files
    """
    invariantsubtrees = [ '%(sysconfdir)s' ]

    def doFile(self, file):
	fullpath = ('%(destdir)s/'+file) %self.macros
	if os.path.isfile(fullpath) and not os.path.islink(fullpath):
	    _markConfig(self.recipe, file)


class Config(policy.Policy):
    """
    Mark only explicit inclusions as config files
    """
    keywords = {
	'inclusions': []
    }

    def __init__(self, *args, **keywords):
        """
        @keyword inclusions: regexp(s) specifying files to be included.
        Do not mention files in /etc, which are already covered by the
        EtcConfig class.
        @type inclusions: None, regexp string, sequence of regexp strings.
        """
        policy.Policy.__init__(self, *args, **keywords)
        

    def updateArgs(self, *args, **keywords):
	"""
	Config(pathregex(s)...)
	"""
	if args:
	    self.inclusions.extend(args)
	inclusions = keywords.pop('inclusions', None)
	if inclusions:
	    self.inclusions.append(inclusions)
	policy.Policy.updateArgs(self, [], **keywords)

    def doProcess(self, recipe):
	self.configFilters = []
	if self.inclusions:
	    if not isinstance(self.inclusions, (tuple, list)):
		self.inclusions = (self.inclusions,)
	    for inclusion in self.inclusions:
		self.configFilters.append(
		    filter.Filter(inclusion, recipe.macros))
	policy.Policy.doProcess(self, recipe)

    def doFile(self, file):
	fullpath = ('%(destdir)s/'+file) %self.macros
	if os.path.isfile(fullpath) and not os.path.islink(fullpath):
	    for configFilter in self.configFilters:
		if configFilter.match(file):
		    _markConfig(self.recipe, file)


class InitScript(policy.Policy):
    """
    Mark initscripts as such so that chkconfig will be run.
    By default, every file in %(initdir)s is marked as an initscript.
    """
    invariantinclusions = [ '%(initdir)s/.[^/]*$' ]

    def _markInitScript(self, filename):
	log.debug('initscript: %s', filename)
	self.recipe.autopkg.pathMap[filename].flags.isInitScript(True)

    def doFile(self, file):
	fullpath = ('%(destdir)s/'+file) %self.macros
	if os.path.isfile(fullpath) and not os.path.islink(fullpath):
	    self._markInitScript(file)


class SharedLibrary(policy.Policy):
    """
    Mark system shared libaries as such so that ldconfig will be run.
    """
    invariantinclusions = [
	'(%(essentiallibdir)s|%(libdir)s|%(prefix)s/X11R6/%(lib)s|'
	'%(prefix)s/kerberos/%(lib)s|'
	'%(prefix)s/local/%(lib)s|%(libdir)s/qt.*/lib|'
	'%(libdir)s/(mysql|sane))/..*\.so\..*'
    ]

    def _markSharedLibrary(self, filename):
	log.debug('shared library: %s', filename)
	self.recipe.autopkg.pathMap[filename].flags.isShLib(True)

    def doFile(self, file):
	fullpath = ('%(destdir)s/'+file) %self.macros
	if os.path.isfile(fullpath) and not os.path.islink(fullpath):
	    self._markSharedLibrary(file)


class ParseManifest(policy.Policy):
    """
    Parse a file containing a manifest intended for RPM, finding the
    information that can't be represented by pure filesystem status
    with a non-root built: device files (%dev), directory responsibility
    (%dir), and ownership (%attr).  It translates these into the
    related SRS construct for each.  There is no equivalent to
    %defattr -- our default ownership is root:root, and permissions
    (except for setuid and setgid files) are collected from the filesystem.

    XXX I think this parsing may not be sufficient for all manifests,
    tested only with MAKEDEV output so far.
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
	policy.Policy.updateArgs(self, [], **keywords)

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
                target = fields[1][5:]
		# XXX not sure what we should do here...
                dironly = 1
            else:
		# XXX is this right?
                target = fields[1].strip()
		if int(perms, 0) & 06000:
		    self.recipe.AddModes(int(perms, 0), target)
		if owner != 'root' or group != 'root':
		    self.recipe.Ownership(owner, group, target)


class MakeDevices(policy.Policy):
    """
    Make device nodes
    """
    def __init__(self, *args, **keywords):
	self.devices = []
	policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
	"""
	MakeDevices(path, devtype, major, minor, owner, group, perms=0400)
	"""
	if args:
	    l = len(args)
	    # perms is optional, all other arguments must be there
	    assert((l > 5) and (l < 8))
	    if l == 6:
		args.append(0400)
	    self.devices.append(args)
	policy.Policy.updateArgs(self, [], **keywords)

    def do(self):
        for device in self.devices:
            self.recipe.autopkg.addDevice(*device)


class AddModes(policy.Policy):
    """
    Apply suid/sgid modes -- use SetModes in recipes; this is just the
    combined back end to SetModes and ParseManifest.
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
	policy.Policy.updateArgs(self, [], **keywords)

    def doFile(self, path):
	if path in self.fixmodes:
	    log.debug('suid/sgid: %s', path)
	    mode = self.fixmodes[path]
	    self.recipe.autopkg.pathMap[path].inode.setPerms(mode)


class WarnWriteable(policy.Policy):
    """
    Unless a mode has been set explicitly (i.e. with SetModes), warn
    about group- or other-writeable files.
    """
    # Needs to run after AddModes in order to access the pathMap
    def doFile(self, file):
	fullpath = ('%(destdir)s/'+file) %self.macros
	if os.path.islink(fullpath):
	    return
	mode = os.lstat(self.macros['destdir'] + os.sep + file)[stat.ST_MODE]
	if mode & 022:
	    # do we need to warn?
	    if self.recipe.autopkg.pathMap[file].inode.perms():
		log.warning('Possibly inappropriately writeable permission'
			    ' 0%o for file %s', mode & 0777, file)


class Ownership(policy.Policy):
    """
    Set user and group ownership of files.  The default is
    root:root
    """
    def __init__(self, *args, **keywords):
	self.filespecs = []
	policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
	"""
	Ownership(user, group, filespec(s)...)
	"""
	if args:
	    for filespec in args[2:]:
		self.filespecs.append((filespec, args[0], args[1]))
	policy.Policy.updateArgs(self, [], **keywords)

    def doProcess(self, recipe):
	# we must NEVER take ownership from the filesystem
	assert(not self.exceptions)
	self.fileFilters = []
	for (filespec, user, group) in self.filespecs:
	    self.fileFilters.append(
		(filter.Filter(filespec, recipe.macros), user, group))
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
	    pkgfile.inode.setOwner(owner)
	if group:
	    pkgfile.inode.setGroup(group)


class ExcludeDirectories(policy.Policy):
    """
    In SRS, there are only two reasons to package a directory: the
    directory needs permissions other than 0755, or it must exist
    even if it is empty.

    In order to include a directory in a package, call
    C{self.SetMode(path, mode)} where C{mode} is not C{0755},
    or call C{ExcludeDirectories(exceptions=path)} if the
    directory mode is C{0755}
    """
    invariantinclusions = [ ('.*', stat.S_IFDIR) ]

    def doFile(self, path):
	log.debug('excluding directory: %s', path)
	del self.recipe.autopkg.pkgMap[path][path]
	del self.recipe.autopkg.pkgMap[path]
	del self.recipe.autopkg.pathMap[path]


def DefaultPolicy():
    """
    Return a list of actions that expresses the default policy.
    A recipe can then modify this list if necessary.
    """
    return [
	NonBinariesInBindirs(),
	FilesInMandir(),
	ImproperlyShared(),
	ComponentSpec(),
	PackageSpec(),
	EtcConfig(),
	Config(),
	InitScript(),
	SharedLibrary(),
	ParseManifest(),
	MakeDevices(),
	AddModes(),
	WarnWriteable(),
	Ownership(),
	ExcludeDirectories(),
    ]


class PackagePolicyError(policy.PolicyError):
    pass
